#!/usr/bin/env python3
import os
from pathlib import Path
import sys
import socket
import time


# Compose creates network aliases while starting services; avoid caching startup NXDOMAINs in YDB.
deadline = time.monotonic() + 180
while True:
    try:
        for node in range(1, 14):
            socket.getaddrinfo(f"storage-{node}", 19001)
        break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(1)

mode, identity, *extra = sys.argv[1:]
command = ["/opt/ydb/bin/ydbd", "server", "--yaml-config=/fixture/config.yaml",
           "--ic-port=19001", "--mon-port=8765", "--mon-address=0.0.0.0"]
if mode == "storage":
    data = Path("/data")
    data.mkdir(exist_ok=True)
    marker = data / "node-id"
    if marker.exists() and marker.read_text().strip() != identity:
        raise RuntimeError("PDisk volume belongs to another storage node")
    marker.write_text(identity + "\n")
    disk = data / "pdisk.dat"
    if not disk.exists():
        # Sparse real file, not SectorMap. Never resize or erase an existing PDisk.
        with disk.open("xb") as stream:
            stream.truncate(16 * 1024**3)
    command += [f"--node={identity}", "--grpc-port=2135"]
elif mode == "compute":
    port, = extra
    command += [f"--tenant=/Root/block{identity}", "--node-broker=storage-1:2135",
                f"--grpc-port={port}", "--grpc-public-host=localhost", f"--grpc-public-port={port}"]
else:
    raise ValueError(mode)
os.execv(command[0], command)
