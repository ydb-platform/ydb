#!/usr/bin/env python3
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import argparse
import os
import platform
import random
import subprocess
import tempfile
import time

IS_WINDOWS = platform.system() == "Windows"


def generate_k_lines_of_n_files(
    k: int, n: int, compress: bool = False, tmp_dir: str | None = None
) -> str:
    """Creates testing files.

    Notes:
        Returned path has to be deleted by caller.

    Args:
        k: Number of lines per file to generate.
        n: Number of files to generate.
        compress: Whether to compress the files (Default value = False).
        tmp_dir: Location where the files should be generated, if not supplied a temp directory will be created
        (Default value = None).

    Returns:
        Path to parent folder to newly generated files.
    """
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix="data")
    for i in range(n):
        with open(os.path.join(tmp_dir, f"file{i}"), "w", encoding="utf-8") as f:
            for _ in range(k):
                num = int(random.random() * 10000.0)
                tm = time.gmtime(int(random.random() * 30000.0) - 15000)
                dt = time.strftime("%Y-%m-%d", tm)
                tm = time.gmtime(int(random.random() * 30000.0) - 15000)
                ts = time.strftime("%Y-%m-%d %H:%M:%S", tm)
                tm = time.gmtime(int(random.random() * 30000.0) - 15000)
                tsltz = time.strftime("%Y-%m-%d %H:%M:%S", tm)
                tm = time.gmtime(int(random.random() * 30000.0) - 15000)
                tsntz = time.strftime("%Y-%m-%d %H:%M:%S", tm)
                tm = time.gmtime(int(random.random() * 30000.0) - 15000)
                tstz = (
                    time.strftime("%Y-%m-%dT%H:%M:%S", tm)
                    + ("-" if random.random() < 0.5 else "+")
                    + "{:02d}:{:02d}".format(
                        int(random.random() * 12.0), int(random.random() * 60.0)
                    )
                )
                pct = random.random() * 1000.0
                ratio = f"{random.random() * 1000.0:5.2f}"
                rec = "{:d},{:s},{:s},{:s},{:s},{:s},{:f},{:s}".format(
                    num, dt, ts, tsltz, tsntz, tstz, pct, ratio
                )
                f.write(rec + "\n")
        if compress:
            if not IS_WINDOWS:
                subprocess.Popen(
                    ["gzip", os.path.join(tmp_dir, f"file{i}")],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                ).communicate()
            else:
                import gzip
                import shutil

                fname = os.path.join(tmp_dir, f"file{i}")
                with open(fname, "rb") as f_in, gzip.open(fname + ".gz", "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                os.unlink(fname)
    return tmp_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate random testing files for Snowflake"
    )
    parser.add_argument(
        "k", metavar="K", type=int, help="number of lines to generate in each files"
    )
    parser.add_argument("n", metavar="N", type=int, help="number of files to generate")
    parser.add_argument(
        "--dir",
        action="store",
        default=None,
        help="the directory in which to generate files",
    )
    args = vars(parser.parse_args())
    print(generate_k_lines_of_n_files(k=args["k"], n=args["n"], tmp_dir=args["dir"]))
