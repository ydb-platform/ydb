import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("executable")
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()

    messages = json.loads(args.trace.read_text(encoding="utf-8"))
    frames = []
    for message in messages:
        if "method" in message:
            body = json.dumps(message).encode("utf-8")
            frames.append(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body)

    result = subprocess.run(
        [args.executable, "--stdio"],
        input=b"".join(frames),
        stdout=subprocess.PIPE,
        timeout=10,
        check=True,
    )
    for fragment in (b'"result"', b'"capabilities"', b'"serverInfo"', b'"yql"'):
        assert fragment in result.stdout, f"Missing {fragment!r} in {result.stdout!r}"


if __name__ == "__main__":
    main()
