import sys
import subprocess


def fix(args):
    for arg in args:
        kind = None

        if arg.endswith(".ptx"):
            kind = "ptx"
        elif arg.endswith(".cubin"):
            kind = "elf"

        if not kind:
            yield arg
            continue

        _, arch, _ = arg.rsplit(".", 2)

        yield f"--image3=kind={kind},sm={arch},file={arg}"


def main():
    cmd = list(fix(sys.argv[1:]))
    rc = subprocess.call(cmd)
    sys.exit(rc)


if __name__ == "__main__":
    main()
