"""Run with ya's embedded Python: Y_PYTHON_ENTRY_POINT=:main ./ya path/to/repro.py."""

import argparse
import ctypes
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import time


def task(kind, directory, output):
    directory, output = Path(directory), Path(output)
    if kind == "archive":
        os.link(directory.parent / "payload.tar.zstd", output)
        return
    if kind == "before":
        deadline = time.monotonic() + 60
        while not (directory / "extracting").exists():
            if time.monotonic() > deadline:
                raise TimeoutError("Extraction did not start")
            time.sleep(0.002)
        time.sleep(0.05)
    (directory / kind).write_text(str(time.monotonic()))
    output.write_text("ok")


def run(mode, directory):
    from libarchive import _libarchive

    directory = Path(directory)
    original = _libarchive.archive_read_data_into_fd
    if mode == "release":
        with open(sys.executable, "rb") as binary:
            header = binary.read(20)
        if header[:6] != b"\x7fELF\x02\x01" or struct.unpack("<HH", header[16:20]) != (2, 62):
            raise RuntimeError("Control requires an unstripped non-PIE Linux x86_64 ya-bin")
        symbols = subprocess.check_output(["nm", "-P", sys.executable], text=True)
        address = int(
            next(
                line.split()[2]
                for line in symbols.splitlines()
                if line.startswith("archive_read_data_into_fd ")
            ),
            16,
        )
        # CFUNCTYPE releases GIL around the SAME native function, bypassing SWIG.
        native = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_int)(address)

    durations = []

    def extract(archive, fd):
        if not os.readlink(f"/proc/self/fd/{fd}").endswith("/large.log"):
            return original(archive, fd)
        (directory / "extracting").touch()
        start = time.monotonic()
        try:
            return native(int(archive), fd) if mode == "release" else original(archive, fd)
        finally:
            durations.append(time.monotonic() - start)

    nodes = []
    for kind in ("archive", "before", "after"):
        output = "$(BUILD_ROOT)/probe/" + ("payload.tar.zstd" if kind == "archive" else kind)
        node = {
            "uid": kind,
            "deps": ["before"] if kind == "after" else [],
            "inputs": [],
            "outputs": [output],
            "cache": False,
            "cmds": [
                {
                    "cmd_args": [
                        "/usr/bin/python3",
                        str(Path(__file__).resolve()),
                        "task",
                        kind,
                        str(directory),
                        output,
                    ],
                    "cwd": "$(BUILD_ROOT)",
                    "env": {},
                }
            ],
            "kv": {"p": "PROBE", "pc": "green"},
            "platform": "default-linux-x86_64",
            "target_properties": {"module_type": "file", "module_dir": "probe"},
        }
        if kind == "archive":
            node.update(tared_outputs=[output])
            node["kv"]["tared_kind"] = "dir"
        nodes.append(node)
    (directory / "graph.json").write_text(
        json.dumps(
            {
                "conf": {"resources": []},
                "graph": nodes,
                "result": ["archive", "after"],
            }
        )
    )
    _libarchive.archive_read_data_into_fd = extract
    os.environ.pop("Y_PYTHON_ENTRY_POINT", None)
    sys.argv = [
        sys.executable,
        "make",
        "--build",
        "relwithdebinfo",
        "--build-custom-json",
        str(directory / "graph.json"),
        "--no-src-links",
        "--no-bazel-remote-store",
        "--link-threads",
        "2",
        "--output",
        str(directory / "out"),
    ]
    from entry.main import main

    try:
        main()
    finally:
        (directory / "extraction.json").write_text(json.dumps(durations))


def compare():
    import libarchive

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mib", type=int, default=16384)
    args = parser.parse_args()
    if args.mib <= 0:
        parser.error("--mib must be positive")
    root = Path(tempfile.mkdtemp(prefix="ya-gil-"))
    print(f"Results: {root}", flush=True)
    os.environ.setdefault("YA_CACHE_DIR", str(root / "cache"))
    block = b"Example test log: repeated diagnostic message\n" * 16384
    remaining = args.mib * 1024**2
    with libarchive.Archive(
        str(root / "payload.tar.zstd"), mode="wb", format="gnu", filter="zstd"
    ) as archive:
        with archive.writestream("large.log", size=remaining) as stream:
            while remaining:
                piece = block[: min(remaining, len(block))]
                stream.write(piece)
                remaining -= len(piece)
    for mode in ("original", "release"):
        directory = root / mode
        directory.mkdir()
        with (directory / "ya.log").open("w") as log:
            subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "run", mode, str(directory)],
                check=True,
                env=dict(os.environ, Y_PYTHON_ENTRY_POINT=":main"),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        durations = json.loads((directory / "extraction.json").read_text())
        assert len(durations) == 1
        assert (directory / "out/probe/payload/large.log").stat().st_size == args.mib * 1024**2
        delay = float((directory / "after").read_text()) - float((directory / "before").read_text())
        print(f"{mode}: extraction={durations[0]:.3f}s, next-task delay={delay:.3f}s", flush=True)


if __name__ == "__main__":
    if sys.argv[1:2] == ["task"]:
        task(*sys.argv[2:])
    elif sys.argv[1:2] == ["run"]:
        run(*sys.argv[2:])
    else:
        compare()
