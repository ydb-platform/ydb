import os
import os.path
import re
import subprocess

import yatest.common


def test_libc():
    mrjob_dir = yatest.common.binary_path('ydb/library/yql/tools/mrjob')
    mrjob_path = os.path.join(mrjob_dir, 'mrjob')
    tools_path = os.path.dirname(yatest.common.cxx_compiler_path())
    nm_path = os.path.join(tools_path, 'llvm-nm')
    readelf_path = os.path.join(tools_path, 'readelf')
    if os.path.isfile(nm_path):
        result = subprocess.check_output([nm_path, mrjob_path])
    elif os.path.isfile(readelf_path):
        result = subprocess.check_output([readelf_path, '-a', mrjob_path])
    else:
        assert False, 'neither llvm-nm nor readelf found, checked paths: %s' % str((readelf_path, nm_path))

    glibc_tag_count = 0
    for line in result.decode().split('\n'):
        glibc_tag = re.search(r'GLIBC_[0-9\.]+', line)
        if glibc_tag:
            glibc_tag_count += 1
            parts = glibc_tag.group().split('.')
            assert len(parts) > 1
            assert int(parts[1]) <= 11
