#!/usr/bin/env python3
"""
vancam.py — ring-buffer capture + dump-to-zip + live preview + offline buffer playback + export MP4/PNGs

Key design:
  - Each camera /dev/video* is opened by exactly ONE process (ffmpeg).
  - ffmpeg writes:
      (A) rolling segment ring buffer to disk (MJPEG copied into MKV segments)
      (B) local UDP preview stream on 127.0.0.1:<port> as H.264 in MPEG-TS (reliable for ffplay)
  - Preview windows (ffplay) read UDP (not /dev/video), so you can close/reopen safely.
  - playbuf plays the last ~60s from disk even when capture is stopped.
  - export merges the latest N seconds to MP4 and/or PNGs.

Commands:
  ./vancam.py start --num 2
  ./vancam.py stop [--force]
  ./vancam.py status
  ./vancam.py view --num 2
  ./vancam.py noview
  ./vancam.py playbuf --num 2
  ./vancam.py dump
  ./vancam.py export --num 2 --seconds 60 --mp4 --png
"""

from __future__ import annotations

import argparse
import datetime as _dt
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


APP_ROOT = Path(".").resolve()
BUFFERS_ROOT_DEFAULT = APP_ROOT / "cam_buffers"
DUMPS_ROOT_DEFAULT = APP_ROOT / "dumps"

DEFAULT_MATCH = "HD USB Camera"
DEFAULT_INPUT_FORMAT = "mjpeg"
DEFAULT_SIZE = "1920x1080"
DEFAULT_FPS = 30

DEFAULT_SEGMENT_SECONDS = 1
DEFAULT_WRAP = 60
DEFAULT_EXT = "mkv"

DEFAULT_BASE_PORT = 5500  # cam0->5500, cam1->5501, ...

RE_VIDEO_NODE = re.compile(r"(/dev/video\d+)")


@dataclass(frozen=True)
class CamSpec:
    idx: int
    devnode: str
    name: str


def which_or_die(prog: str) -> str:
    p = shutil.which(prog)
    if not p:
        print(f"ERROR: required program not found in PATH: {prog}", file=sys.stderr)
        sys.exit(2)
    return p


def run_cmd(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)


def parse_v4l2_list_devices(output: str) -> List[Tuple[str, List[str]]]:
    blocks: List[Tuple[str, List[str]]] = []
    current_name: Optional[str] = None
    current_nodes: List[str] = []

    for line in output.splitlines():
        if not line.strip():
            if current_name and current_nodes:
                blocks.append((current_name, current_nodes))
            current_name, current_nodes = None, []
            continue

        if not line.startswith("\t") and line.endswith(":"):
            if current_name and current_nodes:
                blocks.append((current_name, current_nodes))
            current_name = line.rstrip(":").strip()
            current_nodes = []
            continue

        m = RE_VIDEO_NODE.search(line)
        if m and current_name:
            current_nodes.append(m.group(1))

    if current_name and current_nodes:
        blocks.append((current_name, current_nodes))

    return blocks


def discover_cameras(num: int, match: Optional[str]) -> List[CamSpec]:
    which_or_die("v4l2-ctl")
    cp = run_cmd(["v4l2-ctl", "--list-devices"], check=True)
    blocks = parse_v4l2_list_devices(cp.stdout)
    if not blocks:
        print("ERROR: No V4L2 devices found.", file=sys.stderr)
        sys.exit(3)

    cams: List[CamSpec] = []
    used = set()
    for name, nodes in blocks:
        if not nodes:
            continue
        if match and match not in name:
            continue
        dev = nodes[0]
        if dev in used:
            continue
        used.add(dev)
        cams.append(CamSpec(idx=len(cams), devnode=dev, name=name))
        if len(cams) >= num:
            break

    if len(cams) < num:
        print(f"ERROR: Requested {num} cameras but found {len(cams)} matching '{match}'.", file=sys.stderr)
        print("Detected blocks:", file=sys.stderr)
        for n, ns in blocks:
            print(f"  - {n}: {', '.join(ns)}", file=sys.stderr)
        sys.exit(4)

    return cams


def cam_dir(buffers_root: Path, cam_idx: int) -> Path:
    return buffers_root / f"cam{cam_idx}"


def pid_ffmpeg(buffers_root: Path, cam_idx: int) -> Path:
    return cam_dir(buffers_root, cam_idx) / "ffmpeg.pid"


def pid_ffplay(buffers_root: Path, cam_idx: int, kind: str = "view") -> Path:
    return cam_dir(buffers_root, cam_idx) / f"ffplay.{kind}.pid"


def meta_file(buffers_root: Path, cam_idx: int) -> Path:
    return cam_dir(buffers_root, cam_idx) / "cam.meta"


def read_pid(path: Path) -> Optional[int]:
    try:
        return int(path.read_text().strip())
    except Exception:
        return None


def is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def kill_pidfile(path: Path, sig: int, kill_group: bool = True) -> None:
    pid = read_pid(path)
    if not pid:
        return
    if not is_pid_running(pid):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        return
    try:
        if kill_group:
            os.killpg(pid, sig)
        else:
            os.kill(pid, sig)
    except Exception:
        try:
            os.kill(pid, sig)
        except Exception:
            pass


def parse_meta(path: Path) -> Dict[str, str]:
    d: Dict[str, str] = {}
    if not path.exists():
        return d
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        d[k.strip()] = v.strip()
    return d


def start_capture_ffmpeg(
    cam: CamSpec,
    buffers_root: Path,
    input_format: str,
    size: str,
    fps: int,
    segment_seconds: int,
    wrap: int,
    ext: str,
    udp_port: int,
) -> int:
    which_or_die("ffmpeg")

    outdir = cam_dir(buffers_root, cam.idx)
    outdir.mkdir(parents=True, exist_ok=True)

    seg_pattern = str(outdir / f"%03d.{ext}")
    udp_url = f"udp://127.0.0.1:{udp_port}?pkt_size=1316"

    logfile = outdir / "ffmpeg.log"
    logf = open(logfile, "a", buffering=1)

    # Output 0: ring buffer segments (cheap copy)
    # Output 1: UDP preview as H.264 in MPEG-TS (reliable)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-f", "v4l2",
        "-input_format", input_format,
        "-video_size", size,
        "-framerate", str(fps),
        "-i", cam.devnode,

        "-map", "0:v",
        "-c:v:0", "copy",
        "-f", "segment",
        "-segment_time", str(segment_seconds),
        "-segment_wrap", str(wrap),
        "-reset_timestamps", "1",
        seg_pattern,

        "-map", "0:v",
        "-c:v:1", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-pix_fmt", "yuv420p",
        "-g", str(max(1, fps)),
        "-keyint_min", str(max(1, fps)),
        "-bf", "0",
        "-f", "mpegts",
        "-flush_packets", "1",
        udp_url,
    ]

    p = subprocess.Popen(cmd, stdout=logf, stderr=logf, start_new_session=True)

    write_text_atomic(pid_ffmpeg(buffers_root, cam.idx), str(p.pid))
    write_text_atomic(
        meta_file(buffers_root, cam.idx),
        f"idx={cam.idx}\nname={cam.name}\ndevnode={cam.devnode}\n"
        f"input_format={input_format}\nsize={size}\nfps={fps}\n"
        f"segment_seconds={segment_seconds}\nwrap={wrap}\next={ext}\n"
        f"udp_port={udp_port}\n",
    )

    time.sleep(0.35)
    if p.poll() is not None:
        try:
            tail = "\n".join(logfile.read_text().splitlines()[-120:])
        except Exception:
            tail = "(could not read log)"
        print(f"ERROR: ffmpeg exited immediately for cam{cam.idx} ({cam.devnode}). Log tail:\n{tail}", file=sys.stderr)
        sys.exit(5)

    return p.pid


def start_preview_ffplay_udp(cam_idx: int, buffers_root: Path, udp_port: int) -> int:
    which_or_die("ffplay")
    outdir = cam_dir(buffers_root, cam_idx)
    outdir.mkdir(parents=True, exist_ok=True)

    pf = pid_ffplay(buffers_root, cam_idx, kind="view")
    pid = read_pid(pf)
    if pid and is_pid_running(pid):
        return pid

    logfile = outdir / "ffplay.view.log"
    logf = open(logfile, "a", buffering=1)

    title = f"VanCam cam{cam_idx}  udp:{udp_port}"
    url = f"udp://127.0.0.1:{udp_port}?fifo_size=1000000&overrun_nonfatal=1"

    cmd = [
        "ffplay",
        "-hide_banner",
        "-loglevel", "warning",
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-probesize", "256k",
        "-analyzeduration", "300000",
        "-window_title", title,
        "-f", "mpegts",
        url,
    ]

    p = subprocess.Popen(cmd, stdout=logf, stderr=logf, start_new_session=True)
    write_text_atomic(pf, str(p.pid))

    time.sleep(0.35)
    if p.poll() is not None:
        try:
            tail = "\n".join(logfile.read_text().splitlines()[-120:])
        except Exception:
            tail = "(could not read log)"
        print(f"WARNING: ffplay(view) exited for cam{cam_idx} (udp {udp_port}). Log tail:\n{tail}", file=sys.stderr)
        try:
            pf.unlink(missing_ok=True)
        except Exception:
            pass
        return -1

    return p.pid


def build_segments_by_mtime(cam_path: Path, ext: str, wrap: int) -> List[Path]:
    segs: List[Path] = []
    for i in range(wrap):
        f = cam_path / f"{i:03d}.{ext}"
        if f.exists() and f.is_file() and f.stat().st_size > 0:
            segs.append(f)
    segs.sort(key=lambda p: (p.stat().st_mtime, p.name))
    return segs


def start_playbuf_ffplay(cam_idx: int, buffers_root: Path, ext: str, wrap: int) -> int:
    which_or_die("ffplay")
    outdir = cam_dir(buffers_root, cam_idx)
    outdir.mkdir(parents=True, exist_ok=True)

    pf = pid_ffplay(buffers_root, cam_idx, kind="playbuf")
    pid = read_pid(pf)
    if pid and is_pid_running(pid):
        return pid

    segs = build_segments_by_mtime(outdir, ext=ext, wrap=wrap)
    if not segs:
        print(f"WARNING: cam{cam_idx}: no segment files found to play.", file=sys.stderr)
        return -1

    concat_path = outdir / "playbuf.concat.txt"
    write_text_atomic(concat_path, "\n".join([f"file '{s.resolve()}'" for s in segs]) + "\n")

    logfile = outdir / "ffplay.playbuf.log"
    logf = open(logfile, "a", buffering=1)

    title = f"VanCam cam{cam_idx}  playbuf"
    cmd = [
        "ffplay",
        "-hide_banner",
        "-loglevel", "warning",
        "-window_title", title,
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_path),
    ]

    p = subprocess.Popen(cmd, stdout=logf, stderr=logf, start_new_session=True)
    write_text_atomic(pf, str(p.pid))

    time.sleep(0.25)
    if p.poll() is not None:
        try:
            tail = "\n".join(logfile.read_text().splitlines()[-120:])
        except Exception:
            tail = "(could not read log)"
        print(f"WARNING: ffplay(playbuf) exited for cam{cam_idx}. Log tail:\n{tail}", file=sys.stderr)
        try:
            pf.unlink(missing_ok=True)
        except Exception:
            pass
        return -1

    return p.pid


def stop_preview(buffers_root: Path, force: bool) -> None:
    for d in sorted(buffers_root.glob("cam*")):
        kill_pidfile(d / "ffplay.view.pid", sig=signal.SIGTERM, kill_group=True)
        kill_pidfile(d / "ffplay.playbuf.pid", sig=signal.SIGTERM, kill_group=True)

    deadline = time.time() + (0.7 if not force else 0.2)
    while time.time() < deadline:
        alive = False
        for d in sorted(buffers_root.glob("cam*")):
            for pf in (d / "ffplay.view.pid", d / "ffplay.playbuf.pid"):
                pid = read_pid(pf)
                if pid and is_pid_running(pid):
                    alive = True
                    break
            if alive:
                break
        if not alive:
            break
        time.sleep(0.1)

    if force:
        for d in sorted(buffers_root.glob("cam*")):
            kill_pidfile(d / "ffplay.view.pid", sig=signal.SIGKILL, kill_group=True)
            kill_pidfile(d / "ffplay.playbuf.pid", sig=signal.SIGKILL, kill_group=True)

    for d in sorted(buffers_root.glob("cam*")):
        for pf in (d / "ffplay.view.pid", d / "ffplay.playbuf.pid"):
            pid = read_pid(pf)
            if pid and not is_pid_running(pid):
                try:
                    pf.unlink(missing_ok=True)
                except Exception:
                    pass


def stop_capture(buffers_root: Path, force: bool) -> None:
    for d in sorted(buffers_root.glob("cam*")):
        kill_pidfile(d / "ffmpeg.pid", sig=signal.SIGTERM, kill_group=True)

    deadline = time.time() + (0.9 if not force else 0.2)
    while time.time() < deadline:
        alive = False
        for d in sorted(buffers_root.glob("cam*")):
            pid = read_pid(d / "ffmpeg.pid")
            if pid and is_pid_running(pid):
                alive = True
                break
        if not alive:
            break
        time.sleep(0.1)

    if force:
        for d in sorted(buffers_root.glob("cam*")):
            kill_pidfile(d / "ffmpeg.pid", sig=signal.SIGKILL, kill_group=True)

    for d in sorted(buffers_root.glob("cam*")):
        pf = d / "ffmpeg.pid"
        pid = read_pid(pf)
        if pid and not is_pid_running(pid):
            try:
                pf.unlink(missing_ok=True)
            except Exception:
                pass


def status(buffers_root: Path) -> None:
    cam_dirs = sorted([p for p in buffers_root.glob("cam*") if p.is_dir()])
    if not cam_dirs:
        print("No camera state found under:", buffers_root)
        return

    for d in cam_dirs:
        m = re.match(r"cam(\d+)$", d.name)
        if not m:
            continue
        idx = int(m.group(1))

        pid_m = read_pid(d / "ffmpeg.pid")
        pid_v = read_pid(d / "ffplay.view.pid")
        pid_p = read_pid(d / "ffplay.playbuf.pid")

        run_m = bool(pid_m and is_pid_running(pid_m))
        run_v = bool(pid_v and is_pid_running(pid_v))
        run_p = bool(pid_p and is_pid_running(pid_p))

        print(f"{d.name}: capture(pid={pid_m}, running={run_m}) view(pid={pid_v}, running={run_v}) playbuf(pid={pid_p}, running={run_p})")

        mf = d / "cam.meta"
        if mf.exists():
            md = parse_meta(mf)
            for k in ("devnode", "size", "fps", "input_format", "udp_port", "segment_seconds", "wrap", "ext"):
                if k in md:
                    print(f"  {k}={md[k]}")


def dump_buffers(buffers_root: Path, dumps_root: Path, ext: str, wrap: int) -> List[Path]:
    ts = _dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    dumps_root.mkdir(parents=True, exist_ok=True)

    out_zips: List[Path] = []
    cam_dirs = sorted([p for p in buffers_root.glob("cam*") if p.is_dir()])
    if not cam_dirs:
        print("ERROR: No cam buffers found. Did you run `start` first?", file=sys.stderr)
        sys.exit(6)

    for cam_dir_path in cam_dirs:
        m = re.match(r"cam(\d+)$", cam_dir_path.name)
        if not m:
            continue
        cam_idx = int(m.group(1))

        segments = []
        for i in range(wrap):
            f = cam_dir_path / f"{i:03d}.{ext}"
            if f.exists() and f.is_file():
                segments.append(f)

        if not segments:
            print(f"WARNING: No segments found for {cam_dir_path.name}. Skipping.", file=sys.stderr)
            continue

        snap_dir = dumps_root / f"{ts}_buffer_cam_{cam_idx}"
        snap_dir.mkdir(parents=True, exist_ok=True)
        for f in segments:
            try:
                os.link(f, snap_dir / f.name)
            except Exception:
                shutil.copy2(f, snap_dir / f.name)

        zip_path = dumps_root / f"{ts}_buffer_cam_{cam_idx}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(snap_dir.glob(f"*.{ext}")):
                zf.write(f, arcname=f.name)

        shutil.rmtree(snap_dir, ignore_errors=True)
        out_zips.append(zip_path)

    return out_zips


def export_latest(
    buffers_root: Path,
    dumps_root: Path,
    cam_indices: List[int],
    seconds: int,
    mp4: bool,
    png: bool,
    png_every: int,
    crf: int,
    preset: str,
    force_fps: Optional[int],
) -> List[Path]:
    which_or_die("ffmpeg")
    dumps_root.mkdir(parents=True, exist_ok=True)

    ts = _dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    out_paths: List[Path] = []

    for cam_idx in cam_indices:
        cdir = cam_dir(buffers_root, cam_idx)
        if not cdir.exists():
            print(f"WARNING: cam{cam_idx}: missing directory {cdir}, skipping.", file=sys.stderr)
            continue

        md = parse_meta(meta_file(buffers_root, cam_idx))
        ext = md.get("ext", DEFAULT_EXT)
        wrap = int(md.get("wrap", str(DEFAULT_WRAP)))
        seg_s = int(md.get("segment_seconds", str(DEFAULT_SEGMENT_SECONDS)))
        fps = force_fps if force_fps is not None else int(md.get("fps", str(DEFAULT_FPS)))

        segs = build_segments_by_mtime(cdir, ext=ext, wrap=wrap)
        if not segs:
            print(f"WARNING: cam{cam_idx}: no segments found, skipping.", file=sys.stderr)
            continue

        need = max(1, int(math.ceil(seconds / max(1, seg_s))))
        chosen = segs[-need:]  # newest N by mtime
        # concat list must be chronological
        chosen.sort(key=lambda p: (p.stat().st_mtime, p.name))

        concat_list = dumps_root / f"{ts}_cam{cam_idx}_concat.txt"
        write_text_atomic(concat_list, "\n".join([f"file '{p.resolve()}'" for p in chosen]) + "\n")

        mp4_path = dumps_root / f"{ts}_cam{cam_idx}.mp4"
        png_dir = dumps_root / f"{ts}_cam{cam_idx}_png"

        if mp4:
            # Always transcode to H.264 MP4 for compatibility
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-f", "concat",
                "-safe", "0",
                "-i", str(concat_list),
                "-r", str(fps),
                "-c:v", "libx264",
                "-preset", preset,
                "-crf", str(crf),
                "-pix_fmt", "yuv420p",
                str(mp4_path),
            ]
            subprocess.run(cmd, check=True)
            out_paths.append(mp4_path)

        if png:
            png_dir.mkdir(parents=True, exist_ok=True)
            # Source for PNG extraction: prefer mp4 if created, else concat list directly
            if mp4 and mp4_path.exists():
                src = str(mp4_path)
                in_args = ["-i", src]
            else:
                in_args = ["-f", "concat", "-safe", "0", "-i", str(concat_list)]

            vf = []
            if png_every > 1:
                # take every Nth frame
                vf.append(f"select='not(mod(n\\,{png_every}))'")
                # keep timestamps sane
                vf.append("setpts=N/FRAME_RATE/TB")
            vf_arg = []
            if vf:
                vf_arg = ["-vf", ",".join(vf)]

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                *in_args,
                *vf_arg,
                str(png_dir / f"cam{cam_idx}_%06d.png"),
            ]
            subprocess.run(cmd, check=True)
            out_paths.append(png_dir)

        # keep concat list for debugging only if desired; otherwise remove
        try:
            concat_list.unlink(missing_ok=True)
        except Exception:
            pass

    return out_paths


def main() -> int:
    ap = argparse.ArgumentParser(prog="vancam.py", add_help=True)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add_common_cam_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--num", type=int, default=2)
        p.add_argument("--match", default=DEFAULT_MATCH)
        p.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)
        p.add_argument("--input-format", default=DEFAULT_INPUT_FORMAT)
        p.add_argument("--size", default=DEFAULT_SIZE)
        p.add_argument("--fps", type=int, default=DEFAULT_FPS)
        p.add_argument("--base-port", type=int, default=DEFAULT_BASE_PORT)

    p_start = sub.add_parser("start")
    add_common_cam_args(p_start)
    p_start.add_argument("--segment-seconds", type=int, default=DEFAULT_SEGMENT_SECONDS)
    p_start.add_argument("--wrap", type=int, default=DEFAULT_WRAP)
    p_start.add_argument("--ext", default=DEFAULT_EXT)
    p_start.add_argument("--no-view", action="store_true")

    p_stop = sub.add_parser("stop")
    p_stop.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)
    p_stop.add_argument("--force", action="store_true")

    p_status = sub.add_parser("status")
    p_status.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)

    p_view = sub.add_parser("view")
    p_view.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)
    p_view.add_argument("--num", type=int, default=2)
    p_view.add_argument("--base-port", type=int, default=DEFAULT_BASE_PORT)

    p_playbuf = sub.add_parser("playbuf")
    p_playbuf.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)
    p_playbuf.add_argument("--num", type=int, default=2)
    p_playbuf.add_argument("--wrap", type=int, default=DEFAULT_WRAP)
    p_playbuf.add_argument("--ext", default=DEFAULT_EXT)

    p_noview = sub.add_parser("noview")
    p_noview.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)
    p_noview.add_argument("--force", action="store_true")

    p_dump = sub.add_parser("dump")
    p_dump.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)
    p_dump.add_argument("--outdir", type=Path, default=DUMPS_ROOT_DEFAULT)
    p_dump.add_argument("--wrap", type=int, default=DEFAULT_WRAP)
    p_dump.add_argument("--ext", default=DEFAULT_EXT)

    p_export = sub.add_parser("export")
    p_export.add_argument("--buffers", type=Path, default=BUFFERS_ROOT_DEFAULT)
    p_export.add_argument("--outdir", type=Path, default=DUMPS_ROOT_DEFAULT)
    p_export.add_argument("--num", type=int, default=2)
    p_export.add_argument("--seconds", type=int, default=60)
    p_export.add_argument("--mp4", action="store_true", help="produce MP4 per cam")
    p_export.add_argument("--png", action="store_true", help="extract PNG frames per cam")
    p_export.add_argument("--png-every", type=int, default=1, help="keep every Nth frame (default 1=all frames)")
    p_export.add_argument("--crf", type=int, default=23, help="H.264 quality (lower=better, bigger files)")
    p_export.add_argument("--preset", default="veryfast", help="x264 preset (ultrafast..veryslow)")
    p_export.add_argument("--fps", type=int, default=None, help="override output FPS for mp4/png extraction")

    args = ap.parse_args()

    if args.cmd == "stop":
        stop_preview(args.buffers, force=args.force)
        stop_capture(args.buffers, force=args.force)
        return 0

    if args.cmd == "start":
        args.buffers.mkdir(parents=True, exist_ok=True)
        cams = discover_cameras(args.num, match=args.match)
        print("Discovered cameras:")
        for c in cams:
            print(f"  cam{c.idx}: {c.devnode}  ({c.name})")

        for c in cams:
            pf = pid_ffmpeg(args.buffers, c.idx)
            pid = read_pid(pf)
            udp_port = args.base_port + c.idx
            if pid and is_pid_running(pid):
                print(f"cam{c.idx}: capture already running (pid {pid})")
            else:
                pid = start_capture_ffmpeg(
                    c, args.buffers, args.input_format, args.size, args.fps,
                    args.segment_seconds, args.wrap, args.ext, udp_port
                )
                print(f"cam{c.idx}: started capture pid {pid} (udp {udp_port})")

        if not args.no_view:
            for c in cams:
                udp_port = args.base_port + c.idx
                pidp = start_preview_ffplay_udp(c.idx, args.buffers, udp_port)
                if pidp > 0:
                    print(f"cam{c.idx}: started view pid {pidp} (udp {udp_port})")
        return 0

    if args.cmd == "view":
        for i in range(args.num):
            udp_port = args.base_port + i
            pidp = start_preview_ffplay_udp(i, args.buffers, udp_port)
            if pidp > 0:
                print(f"cam{i}: started view pid {pidp} (udp {udp_port})")
        return 0

    if args.cmd == "playbuf":
        for i in range(args.num):
            pidp = start_playbuf_ffplay(i, args.buffers, ext=args.ext, wrap=args.wrap)
            if pidp > 0:
                print(f"cam{i}: started playbuf pid {pidp}")
        return 0

    if args.cmd == "noview":
        stop_preview(args.buffers, force=args.force)
        return 0

    if args.cmd == "status":
        status(args.buffers)
        return 0

    if args.cmd == "dump":
        zips = dump_buffers(args.buffers, args.outdir, ext=args.ext, wrap=args.wrap)
        for z in zips:
            print(str(z))
        return 0

    if args.cmd == "export":
        if not args.mp4 and not args.png:
            print("ERROR: export requires at least one of --mp4 or --png", file=sys.stderr)
            return 2
        outs = export_latest(
            buffers_root=args.buffers,
            dumps_root=args.outdir,
            cam_indices=list(range(args.num)),
            seconds=args.seconds,
            mp4=args.mp4,
            png=args.png,
            png_every=max(1, args.png_every),
            crf=args.crf,
            preset=args.preset,
            force_fps=args.fps,
        )
        for p in outs:
            print(str(p))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
