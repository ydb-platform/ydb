#!/usr/bin/env python3

import os
import shutil
import sys


def main(src_dir, dst_dir):
    for root, _, files in os.walk(src_dir):
        for filename in files:
            if '$inlined$sam$' not in filename or not filename.endswith('.class'):
                continue

            src = os.path.join(root, filename)
            dst = os.path.join(dst_dir, os.path.relpath(src, src_dir))
            if os.path.exists(dst):
                shutil.copyfile(src, dst)


if __name__ == '__main__':
    main(*sys.argv[1:])
