from pathlib import Path
import sys
import time

kind, output, argument = sys.argv[1:4]
output = Path(output)
if kind == "archive":
    block = b"Example test log: repeated diagnostic message\n" * 16384
    remaining = int(argument) * 1024**2
    with output.open("wb") as stream:
        while remaining:
            part = block[: min(remaining, len(block))]
            stream.write(part)
            remaining -= len(part)
elif kind == "before":
    deadline = time.monotonic() + 300
    while not any(Path(argument).rglob("large.log")):
        if time.monotonic() > deadline:
            raise TimeoutError("No extracted large.log")
        time.sleep(0.01)
    time.sleep(0.05)
    output.write_text(str(time.monotonic()))
else:
    delay = time.monotonic() - float(Path(argument).read_text())
    output.write_text(f"Next-task delay: {delay:.3f}s\n")
