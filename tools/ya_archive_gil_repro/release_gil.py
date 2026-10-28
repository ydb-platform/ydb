"""Diagnostic control: release GIL around the same native archive reader."""

import ctypes
import os
import struct
import subprocess
import sys

from libarchive import _libarchive

with open(sys.executable, "rb") as binary:
    header = binary.read(20)
if header[:6] != b"\x7fELF\x02\x01" or struct.unpack("<HH", header[16:20]) != (2, 62):
    raise RuntimeError("Requires an unstripped non-PIE Linux x86_64 ya-bin")
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
_libarchive.archive_read_data_into_fd = lambda archive, fd: native(int(archive), fd)

os.environ.pop("Y_PYTHON_ENTRY_POINT", None)
sys.argv = [sys.executable] + sys.argv[1:]
from entry.main import main

main()
