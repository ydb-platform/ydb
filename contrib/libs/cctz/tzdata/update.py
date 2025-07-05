#!/usr/bin/env python3

import glob
import hashlib
import io
import os
import re
import shutil
import string
import subprocess
import sys
import tarfile
import tempfile
import urllib.request


def create_cmakelists(zoneinfo_dir):
    tz_to_hash = {}
    hash_to_content = {}
    total_size = 0
    for dirpath, _, filenames in os.walk(zoneinfo_dir):
        for fn in filenames:
            tz_file_name = os.path.join(dirpath, fn)
            with open(tz_file_name, 'rb') as f:
                tz_content = f.read()
            if not tz_content.startswith(b'TZif'):
                continue
            tz_hash = hashlib.md5(tz_content).hexdigest()
            tz_name = tz_file_name.replace(zoneinfo_dir, '').lstrip('/')
            tz_to_hash[tz_name] = tz_hash
            hash_to_content[tz_hash] = tz_content
            total_size += len(tz_content)
    print('Total data size in bytes:', total_size)

    generated_dir = 'generated'
    if not os.path.isdir(generated_dir):
        os.mkdir(generated_dir)
    for tz_hash, tz_content in hash_to_content.items():
        with open(os.path.join(generated_dir, tz_hash), 'wb') as f:
            f.write(tz_content)

    yamake_template =  (
        'RESOURCE(\n'
        '{}\n'
        ')'
    )
    resources = '\n'.join('    generated/{} /cctz/tzdata/{}'.format(tz_hash, tz_name) for tz_name, tz_hash in sorted(tz_to_hash.items()))

    all_hashes = set(tz_to_hash.values())
    hash_pattern = os.path.join('generated', '[{}]'.format(string.hexdigits) * 32)
    for fn in glob.glob(hash_pattern):
        cmd = 'add' if os.path.basename(fn) in all_hashes else 'rm'
        subprocess.check_call(['arc', cmd, fn])

    with open('ya.make.resources', 'w') as f:
        print(yamake_template.format(resources), file=f)


def get_latest_version():
    # Temporary here for the purposes of reimport
    return "2024a"
    index_html = urllib.request.urlopen('http://www.iana.org/time-zones').read()
    version_match = re.search('<a href="[^"]*">tzdata(.*).tar.gz</a>', index_html.decode())
    if not version_match:
        raise Exception('Failed to determine the latest tzdata version')
    return version_match.group(1)


def get_current_version():
    try:
        with open('VERSION') as f:
            return f.read()
    except:
        return 0


def prepare_tzdata(version):
    temp_dir = "tmp"
    shutil.rmtree(temp_dir, ignore_errors=True)

    EXCLUDE = [
        "iso3166.tab",
        "zone.tab",
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
            "generated/58543f30ac34b6510b552b9b3e82b772",
            f"{temp_dir}/usr/share/zoneinfo/posixrules",
        )

        print('Preparing ya.make.resources')
        create_cmakelists(f"{temp_dir}/usr/share/zoneinfo")
    finally:
        shutil.rmtree(temp_dir)


def main():
    version_current = get_current_version()
    version_latest = get_latest_version()
    print(f'Updating from {version_current} to {version_latest}')
    prepare_tzdata(version_latest)

if __name__ == '__main__':
    
    main()
