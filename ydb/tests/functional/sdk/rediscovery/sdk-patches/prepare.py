"""Check out pinned public SDK sources and apply the draft patches for cutover tests."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("sdks", nargs="*", help="SDK names; defaults to all entries")
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    with (here / "sources.json").open() as source:
        sources = json.load(source)
    for name in args.sdks or sources:
        if name not in sources:
            parser.error(f"Unknown SDK {name}")
        spec = sources[name]
        target = args.output / name
        target.mkdir(parents=True, exist_ok=False)

        def git(*arguments):
            subprocess.run(["git", "-C", str(target), *arguments], check=True)

        git("init", "--quiet")
        git("fetch", "--quiet", "--depth=1", spec["repository"], spec["base"])
        git("checkout", "--quiet", "--detach", spec["base"])
        git("apply", "--index", "--whitespace=error", str(here / (name + ".patch")))
        print(f"{name} at {spec['base']} with its staged EDS patch", flush=True)


if __name__ == "__main__":
    main()
