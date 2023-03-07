import os
import yatest.common as ya_common
"""
For yatest.common package see file
library/python/testing/yatest_common/yatest/common/__init__.py
"""


class CanonsMeta:
    root = None
    directory = None


canons_meta = CanonsMeta()
is_oss = os.getenv('YDB_OPENSOURCE') is not None


def set_canondata_root(canondata_path):
    canons_meta.root = ya_common.source_path(canondata_path)


def canondata_filepath(filepath):
    filename = filepath.split('/')[-1]
    assert canons_meta.root is not None
    assert canons_meta.directory is not None
    return os.path.join(
        canons_meta.root,
        canons_meta.directory,
        filename
    )
