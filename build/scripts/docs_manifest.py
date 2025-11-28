import argparse
import json
import os
import sys


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--type", type=str, choices=["yfm"], default="yfm")
    parser.add_argument("--version", type=str, default="1.0")
    parser.add_argument("items", nargs='*', default=[])
    return parser.parse_args(argv)


def generate_manifest(args):
    if not args.output or os.path.isdir(args.output):
        raise Exception(f"Output path '{args.output}' is a directory")
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    kvs = {}
    for item in args.items:
        try:
            key, value = item.split("=", 1)
        except Exception:
            raise Exception(f"Invalid data format [{item}], expected `key=value`")
        if not key or not value:
            raise Exception(f"Invalid data format [{item}], both `key` and `value` must be non-empty")
        if key not in ("html", "preprocessed"):
            raise Exception(f"Unexpected `key`: [{item}]")
        if key in kvs:
            raise Exception(f"Duplicate `key` [{key}]")

        kvs[key] = value

    return json.dumps({"version": args.version, "type": args.type, "map": kvs})


def main():
    args = parse_args(sys.argv[1:])
    try:
        content = generate_manifest(args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    with open(args.output, "w") as f:
        f.write(content)


def test_manifest():
    args = parse_args(['--output', 'docs.manifest', 'html=html.tar.gz', 'preprocessed=preprocessed.tar.gz'])
    content = generate_manifest(args)
    assert json.loads(content) == json.loads(
        '{"type": "yfm", "map": {"preprocessed": "preprocessed.tar.gz", "html": "html.tar.gz"}, "version": "1.0"}'
    )


def test_invald_input():
    # invalid output
    args = parse_args(['--output', '', 'html=html.tar.gz', 'preprocessed=preprocessed.tar.gz'])
    try:
        generate_manifest(args)
        assert False
    except Exception:
        pass

    # invalid output
    args = parse_args(['--output', 'oops/', 'html=html.tar.gz', 'preprocessed=preprocessed.tar.gz'])
    try:
        generate_manifest(args)
        assert False
    except Exception:
        pass

    # invalid format
    args = parse_args(['--output', 'docs.manifest', 'html=', 'preprocessed=preprocessed.tar.gz'])
    try:
        generate_manifest(args)
        assert False
    except Exception:
        pass

    # invalid format
    args = parse_args(['--output', 'docs.manifest', 'html=html-docs.tar.gz', 'preprocessed:preprocessed.tar.gz'])
    try:
        generate_manifest(args)
        assert False
    except Exception:
        pass

    # duplicate keys
    args = parse_args(
        ['--output', 'docs.manifest', 'html=html-docs.tar.gz', 'preprocessed=preprocessed.tar.gz', 'html=html.tar.gz']
    )
    try:
        generate_manifest(args)
        assert False
    except Exception:
        pass

    # unexpected key
    args = parse_args(['--output', 'docs.manifest', 'html=html-docs.tar.gz', '_preprocessed=preprocessed.tar.gz'])
    try:
        generate_manifest(args)
        assert False
    except Exception:
        pass


if __name__ == '__main__':
    main()
