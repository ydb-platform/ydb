import argparse
import errno
import hashlib
import os
import platform
import shutil
import sys


class CliArgs:
    def __init__(self, resource_root, uri, out):  # type: (str,str,str) -> None
        self.resource_root = resource_root
        self.uri = uri
        self.out = out


def parse_args():  # type: () -> CliArgs
    parser = argparse.ArgumentParser()
    parser.add_argument("--resource-root", required=True)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--out", required=True)
    return parser.parse_args()


def print_err_msg(msg):  # type: (str) -> None
    print("[[bad]]process_from_https: {}[[rst]]".format(msg), file=sys.stderr)


def link_or_copy(src, dst):  # type: (str,str) -> None
    try:
        if platform.system().lower() == "windows":
            shutil.copy(src, dst)
        else:
            os.link(src, dst)
    except OSError as e:
        if e.errno == errno.EEXIST:
            print_err_msg("destination file already exists: {}".format(dst))
        if e.errno == errno.ENOENT:
            print_err_msg("source file does not exists: {}".format(src))
        raise


def md5_hex(string):  # type: (str) -> str
    return hashlib.md5(string.encode()).hexdigest()


def get_integrity_from_meta(meta_str):  # type: (str) -> str | None
    pairs = meta_str.split("&")
    integrity_prefix = "integrity="
    for pair in pairs:
        if pair.startswith(integrity_prefix):
            return pair[len(integrity_prefix) :]

    return None


def get_path_from_uri(resource_uri):  # type: (str) -> str | None
    if not resource_uri.startswith("https://") and not resource_uri.startswith("http://"):
        print_err_msg("Uri has to start with 'https:' or 'http:', got {}".format(resource_uri))
        return None

    _, meta_str = resource_uri.split("#", 1)
    integrity = get_integrity_from_meta(meta_str)

    if not integrity:
        print_err_msg("Uri mate has to have integrity field, got {}".format(resource_uri))
        return None

    resource_id = md5_hex(integrity)

    return "http/{}/resource".format(resource_id)


def main():
    args = parse_args()
    relative_resource_path = get_path_from_uri(args.uri)
    resource_path = os.path.join(args.resource_root, relative_resource_path)

    if not resource_path:
        print_err_msg("Cannot get filepath from uri")
        return 1

    if not os.path.exists(resource_path):
        print_err_msg("File {} not found in $(RESOURCE_ROOT)".format(relative_resource_path))
        return 1

    our_dirname = os.path.dirname(args.out)
    if our_dirname:
        os.makedirs(our_dirname, exist_ok=True)

    link_or_copy(resource_path, args.out)


if __name__ == "__main__":
    sys.exit(main())
