#!/usr/bin/env python3
"""
vancam_web.py — minimal Flask UI for VanCam

Usage:
  ./vancam_web.py
Then open on phone:
  http://<laptop-ip>:5000/

Assumptions:
  - vancam.py is in the same directory
  - phone + laptop on same LAN (Starlink Wi-Fi)
"""

import subprocess
from flask import Flask, request, jsonify, render_template_string

APP = Flask(__name__)
VANCAM = "./vancam.py"

HTML = """
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>VanCam Control</title>
<style>
  body { font-family: sans-serif; background:#111; color:#eee; text-align:center; }
  button {
    width: 90%;
    max-width: 400px;
    font-size: 1.4em;
    padding: 20px;
    margin: 12px;
    border-radius: 12px;
    border: none;
  }
  .start { background:#2ecc71; }
  .dump  { background:#e67e22; }
  .export{ background:#3498db; }
  .stop  { background:#e74c3c; }
</style>
</head>
<body>
<h2>VanCam</h2>

<button class="start" onclick="post('/start')">Start Capture</button>
<button class="dump"  onclick="post('/dump')">Dump Last 60s</button>
<button class="export" onclick="post('/export')">Export MP4 + PNG</button>
<button class="stop"  onclick="post('/stop')">Stop All</button>

<pre id="out"></pre>

<script>
function post(path) {
  document.getElementById("out").textContent = "Working...";
  fetch(path, {method:"POST"})
    .then(r => r.json())
    .then(j => document.getElementById("out").textContent = j.output)
    .catch(e => document.getElementById("out").textContent = e);
}
</script>
</body>
</html>
"""

def run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.stdout.strip()

@APP.route("/")
def index():
    return render_template_string(HTML)

@APP.route("/start", methods=["POST"])
def start():
    out = run([VANCAM, "start", "--num", "2"])
    return jsonify(output=out)

@APP.route("/dump", methods=["POST"])
def dump():
    out = run([VANCAM, "dump"])
    return jsonify(output=out)

@APP.route("/export", methods=["POST"])
def export():
    out = run([VANCAM, "export", "--num", "2", "--seconds", "60", "--mp4", "--png"])
    return jsonify(output=out)

@APP.route("/stop", methods=["POST"])
def stop():
    out = run([VANCAM, "stop", "--force"])
    return jsonify(output=out)

if __name__ == "__main__":
    # IMPORTANT: bind to all interfaces so phone can connect
    APP.run(host="0.0.0.0", port=5000, debug=False)
