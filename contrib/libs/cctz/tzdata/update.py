#!/usr/bin/env python3

import glob
import hashlib
import io
import os
import pathlib
import re
import shutil
import string
import subprocess
import sys
import tarfile
import tempfile
import urllib.request


def create_cmakelists(zoneinfo_dir):
    zoneinfo_dir = pathlib.Path(zoneinfo_dir)
    with open("ya.make.resources", "wt") as f:
        all_files = []
        for dir, _, files in zoneinfo_dir.walk():
            all_files += [
                (dir / file).relative_to(".")
                for file in files 
            ]
        f.write("RESOURCE(\n")
        for file in sorted(all_files):
            rel_path = file.relative_to(zoneinfo_dir)
            f.write(f"    {str(file): <40} /cctz/tzdata/{rel_path}\n")
        f.write(")")


def get_latest_version():
    index_html = urllib.request.urlopen('http://www.iana.org/time-zones').read()
    version_match = re.search('<a href="[^"]*">tzdata(.*).tar.gz</a>', index_html.decode())
    if not version_match:
        raise Exception('Failed to determine the latest tzdata version')
    return version_match.group(1)


def prepare_tzdata(version):
    temp_dir = "tmp"
    shutil.rmtree(temp_dir, ignore_errors=True)

    EXCLUDE = [
        "iso3166.tab",
        "leapseconds",
        "tzdata.zi",
        "zone.tab",
        "zone1970.tab",
        "zonenow.tab",
    ]

    try:
        for type in ('data', 'code'):
            filename = f'tz{type}{version}.tar.gz'
            url = f'http://www.iana.org/time-zones/repository/releases/{filename}'
            print(f'Downloading {url}')

            bytestream = io.BytesIO(urllib.request.urlopen(url).read())
            print(f'Extracting {filename}')
            with tarfile.open(fileobj=bytestream, mode="r:gz") as f:
                f.extractall(path=temp_dir)
        
        print('Converting tzdata to binary format')
        subprocess.check_call(
            ['make', "--silent", "TOPDIR=.", 'install'],
            cwd=temp_dir,
        )
        
        shutil.rmtree(f"{temp_dir}/usr/share/zoneinfo/etc")
        for path in EXCLUDE:
            os.remove(f"{temp_dir}/usr/share/zoneinfo/{path}")

        # keep posixrules for now
        shutil.copyfile(
            "generated/posixrules",
            f"{temp_dir}/usr/share/zoneinfo/posixrules",
        )

        print('Preparing ya.make.resources')
        shutil.rmtree("generated", ignore_errors=True)
        os.rename(f"{temp_dir}/usr/share/zoneinfo", "generated")
        create_cmakelists("generated")
    finally:
        shutil.rmtree(temp_dir)


def main():
    version = get_latest_version()
    print(f'Importing tzdata {version}')
    prepare_tzdata(version)

if __name__ == '__main__':
    
    main()
