from __future__ import annotations

import unittest

# picked from test-parse-index2, copied rather than imported
# so that it stays stable even if test-parse-index2 changes or disappears.
data_non_inlined = (
    b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x01D\x19'
    b'\x00\x07e\x12\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff'
    b'\xff\xff\xff\xff\xd1\xf4\xbb\xb0\xbe\xfc\x13\xbd\x8c\xd3\x9d'
    b'\x0f\xcd\xd9;\x8c\x07\x8cJ/\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    b'\x00\x00\x00\x00\x00\x00\x01D\x19\x00\x00\x00\x00\x00\xdf\x00'
    b'\x00\x01q\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\xff'
    b'\xff\xff\xff\xc1\x12\xb9\x04\x96\xa4Z1t\x91\xdfsJ\x90\xf0\x9bh'
    b'\x07l&\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    b'\x00\x01D\xf8\x00\x00\x00\x00\x01\x1b\x00\x00\x01\xb8\x00\x00'
    b'\x00\x01\x00\x00\x00\x02\x00\x00\x00\x01\xff\xff\xff\xff\x02\n'
    b'\x0e\xc6&\xa1\x92\xae6\x0b\x02i\xfe-\xe5\xbao\x05\xd1\xe7\x00'
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01F'
    b'\x13\x00\x00\x00\x00\x01\xec\x00\x00\x03\x06\x00\x00\x00\x01'
    b'\x00\x00\x00\x03\x00\x00\x00\x02\xff\xff\xff\xff\x12\xcb\xeby1'
    b'\xb6\r\x98B\xcb\x07\xbd`\x8f\x92\xd9\xc4\x84\xbdK\x00\x00\x00'
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00'
)


from ..revlogutils.constants import (
    KIND_CHANGELOG,
)
from ..revlogutils import config as revlog_config
from .. import revlog


try:
    from ..cext import parsers as cparsers  # pytype: disable=import-error
except ImportError:
    cparsers = None

try:
    from ..pyo3_rustext import (  # pytype: disable=import-error
        revlog as rust_revlog,
    )

    rust_revlog.__name__  # force actual import
except ImportError:
    rust_revlog = None


@unittest.skipIf(
    cparsers is None,
    (
        'The C version of the "parsers" module is not available. '
        'It is needed for this test.'
    ),
)
class RevlogBasedTestBase(unittest.TestCase):
    def parseindex(self, data=None):
        if data is None:
            data = data_non_inlined
        return cparsers.parse_index2(data, False)[0]


@unittest.skipIf(
    rust_revlog is None,
    'The Rust revlog module is not available. It is needed for this test.',
)
class RustRevlogBasedTestBase(unittest.TestCase):
    # defaults
    revlog_data_config = revlog_config.DataConfig()
    revlog_delta_config = revlog_config.DeltaConfig()
    revlog_feature_config = revlog_config.FeatureConfig()

    @classmethod
    def irl_class(cls):
        return rust_revlog.InnerRevlog

    @classmethod
    def nodetree(cls, idx):
        return rust_revlog.NodeTree(idx)

    def make_inner_revlog(
        self, data=None, vfs_is_readonly=True, kind=KIND_CHANGELOG
    ):
        if data is None:
            data = data_non_inlined

        return self.irl_class()(
            vfs_base=b"Just a path",
            fncache=None,  # might be enough for now
            vfs_is_readonly=vfs_is_readonly,
            index_data=data,
            index_file=b'test.i',
            data_file=b'test.d',
            sidedata_file=None,
            inline=False,
            data_config=self.revlog_data_config,
            delta_config=self.revlog_delta_config,
            feature_config=self.revlog_feature_config,
            chunk_cache=None,
            default_compression_header=None,
            revlog_type=kind,
            use_persistent_nodemap=False,  # until we cook one.
            use_plain_encoding=False,
        )

    def parserustindex(self, data=None):
        return revlog.RustIndexProxy(self.make_inner_revlog(data=data))
