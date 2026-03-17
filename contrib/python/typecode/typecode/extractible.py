#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/aboutcode-org/typecode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import partial
import bz2
import gzip
import os
import tarfile
import zipfile

"""
Utilities to detect is a file is extractible. We prefer using extractcode
support if available, otherwise we use the standard library archive
and compressed files support as available.
"""


def _is_compressed(location, opener):
    if os.path.isfile(location):
        try:
            with opener(location) as comp:
                comp.read()
            return True
        except Exception:
            return False


is_gzipfile = partial(_is_compressed, opener=gzip.open)

is_bz2file = partial(_is_compressed, opener=bz2.BZ2File)

try:
    import lzma

    is_lzmafile = partial(_is_compressed, opener=lzma.open)
except ImportError:
    is_lzmafile = lambda _: False
    lzma = None

# Each function accept a single location argument and return True if this is
# an archive
archive_handlers = [zipfile.is_zipfile, tarfile.is_tarfile, is_gzipfile, is_bz2file, is_lzmafile]


def _can_extract(location):
    """
    Return True if this location is likely to be extractible as some archive
    or compressed file.
    """
    return any(handler(location) for handler in archive_handlers)


try:
    from extractcode.archive import can_extract  # NOQA
except ImportError:
    can_extract = _can_extract
