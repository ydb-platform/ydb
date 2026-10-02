import sys
import subprocess


def fix(args):
    prev_module_id = None

    for arg in args:
        kind = None

        if arg.endswith(".ptx"):
            kind = "ptx"
        elif arg.endswith(".cubin"):
            kind = "elf"
        elif arg.endswith(".module_id"):
            module_id = open(arg).read()

            if prev_module_id is not None and module_id != prev_module_id:
                print(f".module_id mismatch: {module_id} vs {prev_module_id}", file=sys.stderr)
                sys.exit(1)

            prev_module_id = module_id
            continue

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
