#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import defaultdict
import gzip

"""
Utilities to parse a Debian Contents index file.
These are used by apt-file for instance

See https://wiki.debian.org/DebianRepository/Format#A.22Contents.22_indices
for format details.

For example files see:
 - http://ftp.de.debian.org/debian/dists/Debian10.6/main/Contents-amd64.gz
 - http://archive.ubuntu.com/ubuntu/dists/focal/Contents-i386.gz

See also https://salsa.debian.org/debian-irc-team/judd/-/blob/master/supybot/plugins/Judd/debcontents/contents_file.py
"""


def parse_contents(location, has_header=True):
    """
    Return a mapping of {path: [list of packages]} and a mapping of
    {package: [list of paths]} from parsing a Debian Contents file at
    ``location``.
    The Contents file are typically gzipped but we also accept plain text files.

    If ``has_header`` is True, the file is expected to have a header narrative
    and a FILE/LOCATION columns headers before the table starts in earnest.

    See https://wiki.debian.org/DebianRepository/Format#A.22Contents.22_indices
    for format details.
    """
    if location.endswith(".gz"):
        opener, mode = gzip.GzipFile, "rb"
    else:
        opener, mode = open, "r"

    packages_by_path = defaultdict(list)
    paths_by_package = defaultdict(list)
    with opener(location, mode=mode) as lines:
        if has_header:
            # keep track if we are now in the table proper
            # e.g. after the FILE  LOCATION header
            # this is the case for Ubuntu
            in_table = False
        else:
            # if we have no header (like in Debian) we start right away.
            in_table = True

        for line in lines:
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            left, _, right = line.strip().rpartition(" ")
            left = left.strip()
            right = right.strip()
            if left == "FILE" and right == "LOCATION":
                if not has_header:
                    raise Exception(
                        "Invalid Contents file with a FILE/LOCATION header: "
                        "call with has_header=True."
                    )

                if not in_table:
                    # The first row of the table SHOULD have the columns "FILE"
                    # and "LOCATION": This is the spec and used to be True for
                    # Debian. But nowadays only Ubuntu older do this.
                    in_table = True
            else:
                if not in_table:
                    continue
                path = left
                packages = right
                package_names = packages.split(",")
                for archsec_name in package_names:
                    # "A list of qualified package names, separated by comma. A
                    # qualified package name has the form
                    # [[$AREA/]$SECTION/]$NAME, where $AREA is the archive area,
                    # $SECTION the package section, and $NAME the name of the
                    # package."

                    # NOTE: we ignore the arch and section for now
                    archsec, _, package_name = archsec_name.rpartition("/")
                    arch, _, section = archsec.rpartition("/")
                    packages_by_path[path].append(package_name)
                    paths_by_package[package_name].append(path)

    if not in_table:
        raise Exception("Invalid Content files without FILE/LOCATION header.")
    return packages_by_path, paths_by_package


if __name__ == "__main__":
    import sys
    import time

    try:
        location = sys.argv[1]
        start = time.time()
        packages_by_path, paths_by_package = parse_contents(location, has_header=False)
        duration = time.time() - start
        print(f"Parsing completed in {duration} seconds.")
        names_count = len(paths_by_package)
        paths_count = len(packages_by_path)
        print(f"Found {names_count} package names with {paths_count} paths.")

    except Exception as e:
        print("Parse a Debian Contents files and print stats.")
        print("Usage: contents <path to a Gzipped Debian Contents index>")
        print(
            "For example, download this file: http://ftp.de.debian.org/debian/dists/Debian10.6/main/Contents-amd64.gz"
        )
        raise
