"""Prepare ignored demo formulas, then serve static files on loopback. No APIs."""

import argparse
import functools
import hashlib
import http.server
import json
from pathlib import Path
import socket
import sys

DIRECTORY = Path(__file__).resolve().parent
sys.path.insert(0, str(DIRECTORY.parents[5]))

from ydb.core.kqp.opt.rbo.verification.inspector.formulas import prepare_formulas


def prepare_demo():
    manifest = json.loads((DIRECTORY / "demo/manifest.json").read_text())
    generated = DIRECTORY / ".generated"
    generated.mkdir(exist_ok=True)
    sources = DIRECTORY.parent / "verification"
    producer = {str(path.relative_to(sources)): hashlib.sha256(path.read_bytes()).hexdigest()
                for directory in ("inspector", "rbo_verifier")
                for path in sorted((sources / directory).glob("*.py"))}
    for case in manifest["cases"]:
        document = prepare_formulas((DIRECTORY / "demo" / case["before"]).read_bytes(),
                                    (DIRECTORY / "demo" / case["after"]).read_bytes(),
                                    json.loads((DIRECTORY / "demo" / case["verdict"]).read_text())["row_bound"]).document
        name = f"{case['id']}.formulas.json"
        data = (json.dumps(document, separators=(",", ":")) + "\n").encode()
        (generated / name).write_bytes(data)
        for role in ("before", "after", "verdict", "trace", "query"):
            if role in case:
                case[role] = "../demo/" + case[role]
        case["hashes"] = {"../demo/" + name: digest for name, digest in case.get("hashes", {}).items()}
        case["formulas"] = name
        case["hashes"][name] = hashlib.sha256(data).hexdigest()
        case["formula_producer"] = {"source_sha256": producer, "note": "Generated from this checkout; no solver run."}
    (generated / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--bind", choices=("127.0.0.1", "::1"), default="127.0.0.1")
    parser.add_argument("--prepare-only", action="store_true")
    options = parser.parse_args()
    prepare_demo()
    if options.prepare_only:
        return
    class Server(http.server.ThreadingHTTPServer):
        address_family = socket.AF_INET6 if options.bind == "::1" else socket.AF_INET
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(DIRECTORY))
    with Server((options.bind, options.port), handler) as server:
        host = "[::1]" if options.bind == "::1" else options.bind
        print(f"RBO explorer: http://{host}:{server.server_port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
