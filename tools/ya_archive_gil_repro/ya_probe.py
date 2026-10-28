"""Instrument real ya make. Optionally bypass ONLY its GIL-holding SWIG wrapper.

The A/B adapter calls the same native function in the same binary through ctypes.
This is diagnostic code for an unstripped, non-PIE Linux ya-bin, not a production fix.
"""

import ctypes
import faulthandler
import json
import os
import signal
from pathlib import Path
import struct
import subprocess
import sys
import threading
import time

run_dir = Path(os.environ.pop("GIL_PROBE_DIR"))
mode = os.environ.pop("GIL_PROBE_MODE", "original")
os.environ.pop("Y_PYTHON_ENTRY_POINT", None)
from libarchive import _libarchive

original = _libarchive.archive_read_data_into_fd
if mode == "release":
    with open(sys.executable, "rb") as f:
        elf = f.read(20)
    if (
        elf[:6] != b"\x7fELF\x02\x01"
        or struct.unpack("<H", elf[16:18])[0] != 2
        or struct.unpack("<H", elf[18:20])[0] != 62
    ):
        raise RuntimeError("Diagnostic adapter requires a non-PIE x86_64 ELF binary")
    symbols = subprocess.check_output(["nm", "-P", sys.executable], text=True)
    address = int(
        next(
            line.split()[2]
            for line in symbols.splitlines()
            if line.startswith("archive_read_data_into_fd ")
        ),
        16,
    )
    native = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_int)(address)
elif mode != "original":
    raise ValueError(mode)

calls = []
samples = []
stop = threading.Event()


def heartbeat():
    while not stop.is_set():
        samples.append(time.monotonic())
        time.sleep(0.005)


beat = threading.Thread(target=heartbeat, name="gil-probe-heartbeat", daemon=True)
beat.start()
stack_file = (run_dir / "targeted-stacks.txt").open("w")
watcher = subprocess.Popen(
    ["/usr/bin/python3", str(Path(__file__).with_name("stack_watch.py")), str(run_dir)]
)


def read_data(archive, fd):
    # Ignore any infrastructure resources unpacked during ya initialization.
    if not os.readlink("/proc/self/fd/%d" % fd).endswith("/large.log"):
        return original(archive, fd)
    # Register after ya's startup resets signal handlers; SIGUSR1 belongs to ya.
    faulthandler.register(signal.SIGUSR2, file=stack_file, all_threads=False)
    (run_dir / "extracting").write_text(
        json.dumps({"pid": os.getpid(), "tid": threading.get_native_id()})
    )
    a = time.monotonic()
    try:
        if mode == "release":
            return native(int(archive), fd)
        return original(archive, fd)
    finally:
        b = time.monotonic()
        calls.append({"start": a, "end": b, "seconds": b - a})


_libarchive.archive_read_data_into_fd = read_data

sys.argv = [sys.executable] + sys.argv[1:]
start = time.monotonic()
try:
    from entry.main import main

    main()
finally:
    end = time.monotonic()
    stop.set()
    beat.join(timeout=2)
    try:
        watcher.wait(timeout=3)
    except subprocess.TimeoutExpired:
        watcher.terminate()
        watcher.wait()
    faulthandler.unregister(signal.SIGUSR2)
    stack_file.close()
    (run_dir / "probe.json").write_text(
        json.dumps(
            {
                "mode": mode,
                "binary": sys.executable,
                "start": start,
                "end": end,
                "wall_seconds": end - start,
                "calls": calls,
                "heartbeat": samples,
            },
            indent=2,
        )
    )
