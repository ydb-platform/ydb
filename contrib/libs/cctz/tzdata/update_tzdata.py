#!/usr/bin/env python

import glob
import hashlib
import os
import re
import shutil
import string
import subprocess
import sys
import tarfile
import tempfile
import urllib2

def create_cmakelists(zoneinfo_dir):
    tz_to_hash = {}
    hash_to_content = {}
    total_size = 0
    for dirpath, _, filenames in os.walk(zoneinfo_dir):
        for fn in filenames:
            tz_file_name = os.path.join(dirpath, fn)
            with open(tz_file_name) as f:
                tz_content = f.read()
            if not tz_content.startswith('TZif'):
                continue
            tz_hash = hashlib.md5(tz_content).hexdigest()
            tz_name = tz_file_name.replace(zoneinfo_dir, '').lstrip('/')
            tz_to_hash[tz_name] = tz_hash
            hash_to_content[tz_hash] = tz_content
            total_size += len(tz_content)
    print 'Total data size in bytes:', total_size

    generated_dir = 'generated'
    if not os.path.isdir(generated_dir):
        os.mkdir(generated_dir)
    for tz_hash, tz_content in hash_to_content.iteritems():
        with open(os.path.join(generated_dir, tz_hash), 'w') as f:
            f.write(tz_content)

    yamake_template =  (
        'RESOURCE(\n'
        '{}\n'
        ')'
    )
    resources = '\n'.join('    generated/{} /cctz/tzdata/{}'.format(tz_hash, tz_name) for tz_name, tz_hash in sorted(tz_to_hash.iteritems()))

    all_hashes = set(tz_to_hash.values())
    hash_pattern = os.path.join('generated', '[{}]'.format(string.hexdigits) * 32)
    for fn in glob.glob(hash_pattern):
        cmd = 'add' if os.path.basename(fn) in all_hashes else 'rm'
        subprocess.check_call(['arc', cmd, fn])

    with open('ya.make.resources', 'w') as f:
        print >>f, yamake_template.format(resources)

def get_latest_iana_version():
    index_html = urllib2.urlopen('http://www.iana.org/time-zones').read()
    version_match = re.search('<a href="[^"]*">tzdata(.*).tar.gz</a>', index_html)
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
    temp_dir = tempfile.mkdtemp()
    try:
        for file_type in ('data', 'code'):
            file_name = 'tz{}{}.tar.gz'.format(file_type, version)
            full_url = 'http://www.iana.org/time-zones/repository/releases/{}'.format(file_name)
            print 'Downloading {}'.format(full_url)

            local_file_name = os.path.join(temp_dir, file_name)
            with open(local_file_name, 'w') as f:
                f.write(urllib2.urlopen(full_url).read())

            print 'Extracting {}'.format(local_file_name)
            with tarfile.open(local_file_name) as f:
                f.extractall(path=temp_dir)

        print 'Converting tzdata to binary format'
        subprocess.check_call(['make', '-s', '-C', temp_dir, 'TOPDIR={}'.format(temp_dir), 'install'])

        print 'Preparing ya.make.resources'
        zoneinfo_dir = os.path.join(temp_dir, 'usr', 'share', 'zoneinfo')
        create_cmakelists(zoneinfo_dir)
    finally:
        shutil.rmtree(temp_dir)

def main():
    current_version, latest_version = get_current_version(), get_latest_iana_version()
    print 'The current version of tzdata is {}'.format(current_version)
    print 'The latest version of tzdata on the IANA site is {}'.format(latest_version)
    if current_version == latest_version:
        print 'You already have the latest version'
        return
    print 'Updating from {} to {}'.format(current_version, latest_version)
    prepare_tzdata(latest_version)

    with open('VERSION', 'w') as f:
        f.write(latest_version)

    print 'All good! Now make sure the tests pass, and run this:'
    print 'arc add VERSION update_tzdata.py ya.make.resources'
    print 'arc co -b tzdata.{}'.format(latest_version)
    print 'arc ci . -m "Updated tzdata from {} to {}"'.format(current_version, latest_version)

if __name__ == '__main__':
    main()
