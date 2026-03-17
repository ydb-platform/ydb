#!/usr/bin/env python

#
# (C) Copyright 2005- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#
from __future__ import print_function

import argparse
import binascii
import os
import re
import sys
import time

parser = argparse.ArgumentParser()

parser.add_argument(
    "-n",
    "--count",
    type=int,
    default=10,
    help="Number of files to generate",
)

parser.add_argument(
    "-C",
    "--chunk",
    type=int,
    default=16,
    help="Chunk size (MB)",
)

parser.add_argument(
    "-o",
    "--output",
    type=str,
    default="memfs_gen",
    help="Name of C file to generate",
)

parser.add_argument(
    "-e",
    "--exclude",
    help="Exclude packages",
)

parser.add_argument(
    "dirs",
    type=str,
    nargs="+",
    help="The list of directories to process",
)

args = parser.parse_args()


start = time.time()
print("MEMFS: Starting")

# Exclude experimental features e.g. GRIB3 and TAF
# The BUFR codetables is not used in the engine
EXCLUDED = ["grib3", "codetables", "taf", "metar", "stations", "grib1_mlgrib2_ieee32"]

EXCLUDE = {
    None: [],
    "bufr": ["bufr"],
    "grib": ["grib1", "grib2"],
}

EXCLUDED.extend(EXCLUDE[args.exclude])


dirs = [os.path.realpath(x) for x in args.dirs]
print("MEMFS: Directories: ", dirs)
print("MEMFS: Excluding: ", EXCLUDED)

FILES = {}
SIZES = {}
NAMES = []
CHUNK = args.chunk * 1024 * 1024  # chunk size in bytes

# Binary to ASCII function. Different in Python 2 and 3
try:
    str(b"\x23\x20", "ascii")
    ascii = lambda x: str(x, "ascii")  # Python 3
except:
    ascii = lambda x: str(x)  # Python 2


def get_outfile_name(base, count):
    return base + "_" + str(count).zfill(3) + ".c"


# The last argument is the base name of the generated C file(s)
output_file_base = args.output

buffer = None
fcount = 0
MAX_FCOUNT = args.count

for directory in dirs:

    # print("MEMFS: directory=", directory)
    dname = os.path.basename(directory)
    NAMES.append(dname)

    for dirpath, dirnames, files in os.walk(directory, followlinks=True):

        # Prune the walk by modifying the dirnames in-place
        dirnames[:] = [dirname for dirname in dirnames if dirname not in EXCLUDED]
        for name in files:

            if buffer is None:
                opath = get_outfile_name(output_file_base, fcount)
                fcount += 1
                print("MEMFS: Generating output:", opath)
                buffer = open(opath, "w")

            full = "%s/%s" % (dirpath, name)
            _, ext = os.path.splitext(full)
            if ext not in [".def", ".table", ".tmpl", ".list", ".txt"]:
                continue
            if name == "CMakeLists.txt":
                continue

            full = full.replace("\\", "/")
            fname = full[full.find("/%s/" % (dname,)) :]
            # print("MEMFS: Add ", fname)
            name = re.sub(r"\W", "_", fname)

            assert name not in FILES
            assert name not in SIZES
            FILES[name] = fname
            SIZES[name] = os.path.getsize(full)

            buffer.write("const unsigned char %s[] = {" % (name,))

            with open(full, "rb") as f:
                i = 0
                # Python 2
                # contents_hex = f.read().encode("hex")

                # Python 2 and 3
                contents_hex = binascii.hexlify(f.read())

                # Read two characters at a time and convert to C hex
                # e.g. 23 -> 0x23
                for n in range(0, len(contents_hex), 2):
                    twoChars = ascii(contents_hex[n : n + 2])
                    buffer.write("0x%s," % (twoChars,))
                    i += 1
                    if (i % 20) == 0:
                        buffer.write("\n")

            buffer.write("};\n")
            if buffer.tell() >= CHUNK:
                buffer.close()
                buffer = None


if buffer is not None:
    buffer.close()

assert fcount <= MAX_FCOUNT, fcount

while fcount < MAX_FCOUNT:
    opath = get_outfile_name(output_file_base, fcount)
    print("MEMFS: Generating output:", opath, "(empty)")
    with open(opath, "w") as f:
        # ISO compilers issue a warning for an empty translation unit
        # so add a dummy declaration to suppress this
        print("struct eccodes_suppress_iso_warning;/* empty */", file=f)
    fcount += 1

# The number of generated C files is hard coded.
# See memfs/CMakeLists.txt
opath = output_file_base + "_final.c"
print("MEMFS: Generating output:", opath)
g = open(opath, "w")

f = open(os.path.join(os.path.dirname(__file__), "src", "memfs.c"))
for line in f.readlines():
    line = line.rstrip()
    if "<MARKER>" in line:
        # Write extern variables with sizes
        for k, v in SIZES.items():
            print("extern const unsigned char %s[%d];" % (k, v), file=g)

        print(
            "static struct entry entries[] = {",
            file=g,
        )
        items = [(v, k) for k, v in FILES.items()]
        for k, v in sorted(items):
            print(
                '{"/MEMFS%s", &%s[0], sizeof(%s) / sizeof(%s[0]) },' % (k, v, v, v),
                file=g,
            )
        print(
            "};",
            file=g,
        )

    else:
        print(line, file=g)


elapsed = time.time() - start
print("MEMFS: Done in %.2f seconds" % elapsed)
