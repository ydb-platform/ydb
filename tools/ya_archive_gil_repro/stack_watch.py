"""Sample main/worker stacks externally: this process is independent of ya's GIL."""

import ctypes
import json
from pathlib import Path
import signal
import sys
import time

marker = Path(sys.argv[1]) / "extracting"
deadline = time.monotonic() + 60
while True:
    try:
        info = json.loads(marker.read_text())
        break
    except (FileNotFoundError, json.JSONDecodeError):
        if time.monotonic() > deadline:
            raise TimeoutError("No extraction marker")
        time.sleep(0.002)
libc = ctypes.CDLL(None, use_errno=True)
time.sleep(0.15)
for tid, sig in [(info["tid"], signal.SIGUSR2), (info["pid"], signal.SIGUSR2)]:
    if libc.tgkill(info["pid"], tid, sig) != 0:
        raise OSError(ctypes.get_errno(), "tgkill")
    time.sleep(0.025)
