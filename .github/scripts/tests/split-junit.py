#!/usr/bin/env python3
import os
import sys
import argparse
from pathlib import Path
from xml.etree import ElementTree as ET


def save_suite(suite, path):
    root = ET.Element("testsuites")
    root.append(suite)
    tree = ET.ElementTree(root)
    tree.write(path)


def do_split(fn, out_dir):
    try:
        tree = ET.parse(fn)
    except ET.ParseError as e:
        print(f"Unable to parse {fn}: {e}", file=sys.stderr)
        sys.exit(1)

    root = tree.getroot()

    for n, suite in enumerate(root.iter('testsuite')):
        part_fn = Path(out_dir).joinpath(f"part_{n}.xml")
        print(f"write {part_fn}")
        save_suite(suite, part_fn)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', dest='out_dir', required=True)
    parser.add_argument("in_file", type=argparse.FileType("r"))

    args = parser.parse_args()

    if not os.path.isdir(args.out_dir):
        os.makedirs(args.out_dir)

    do_split(args.in_file, args.out_dir)


if __name__ == '__main__':
    main()