"""Run with ya-bin's embedded Python: a zstd tar containing one regular file."""

import argparse
import hashlib
from pathlib import Path
import libarchive

p = argparse.ArgumentParser()
p.add_argument("output", type=Path)
p.add_argument("--mib", type=int, default=1024)
p.add_argument("--filter", default="zstd")
args = p.parse_args()
block = (
    b"Example test log: a repeated diagnostic message, sequence=00000001\n" * 16384
)[: 1024 * 1024]
remaining = args.mib * 1024 * 1024
digest = hashlib.sha256()
with libarchive.Archive(
    str(args.output), mode="wb", format="gnu", filter=args.filter
) as archive:
    with archive.writestream("large.log", size=remaining) as stream:
        while remaining:
            piece = block[: min(remaining, len(block))]
            stream.write(piece)
            digest.update(piece)
            remaining -= len(piece)
args.output.with_suffix(args.output.suffix + ".sha256").write_text(
    digest.hexdigest() + "\n"
)
print(args.output, args.output.stat().st_size, digest.hexdigest())
