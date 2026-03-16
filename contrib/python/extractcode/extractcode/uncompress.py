#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/extractcode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import bz2
import gzip
import logging
import os
import shutil

from functools import partial

from commoncode import fileutils

from extractcode import EXTRACT_SUFFIX

DEBUG = False
logger = logging.getLogger(__name__)
# import sys
# logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
# logger.setLevel(logging.DEBUG)


def uncompress(location, target_dir, decompressor, suffix=EXTRACT_SUFFIX):
    """
    Uncompress a compressed file at location in the target_dir using the
    `decompressor` object. The uncompressed file is named after the original
    archive with a `suffix` added.

    Return a list of warning messages. Raise Exceptions on errors.
    """
    # FIXME: do not create a sub-directory and instead strip the "compression"
    # extension such gz, etc. or introspect the archive header to get the file
    # name when present.
    if DEBUG:
        logger.debug('uncompress: ' + location)

    tmp_loc, warnings = uncompress_file(location, decompressor)

    target_location = os.path.join(target_dir, os.path.basename(location) + suffix)
    if os.path.exists(target_location):
        fileutils.delete(target_location)
    shutil.move(tmp_loc, target_location)
    return warnings


def uncompress_file(location, decompressor):
    """
    Uncompress a compressed file at location and return a temporary location of
    the uncompressed file and a list of warning messages. Raise Exceptions on
    errors. Use the `decompressor` object for decompression.
    """
    # FIXME: do not create a sub-directory and instead strip the "compression"
    # extension such gz, etc. or introspect the archive header to get the file
    # name when present.
    assert location
    assert decompressor

    warnings = []
    base_name = fileutils.file_base_name(location)
    target_location = os.path.join(fileutils.get_temp_dir(
        prefix='extractcode-extract-'), base_name)

    with decompressor(location, 'rb') as compressed:
        with open(target_location, 'wb') as uncompressed:
            buffer_size = 32 * 1024 * 1024
            while True:
                chunk = compressed.read(buffer_size)
                if not chunk:
                    break
                uncompressed.write(chunk)

        if getattr(decompressor, 'has_trailing_garbage', False):
            warnings.append(location + ': Trailing garbage found and ignored.')

    return target_location, warnings


def uncompress_bzip2(location, target_dir):
    """
    Uncompress a bzip2 compressed file at location in the target_dir.
    Return a list warnings messages.
    """
    return uncompress(location, target_dir, decompressor=bz2.BZ2File)


def uncompress_gzip(location, target_dir):
    """
    Uncompress a gzip compressed file at location in the target_dir.
    Return a list warnings messages.
    """

    return uncompress(location, target_dir, decompressor=gzip.GzipFile)


def get_compressed_file_content(location, decompressor):
    """
    Uncompress a compressed file at location and return its content as a byte
    string and a list of warning messages. Raise Exceptions on errors. Use the
    `decompressor` object for decompression.
    """
    warnings = []
    with decompressor(location, 'rb') as compressed:
        content = compressed.read()
        if getattr(decompressor, 'has_trailing_garbage', False):
            warnings.append(location + ': Trailing garbage found and ignored.')
    return content, warnings


get_gz_compressed_file_content = partial(
    get_compressed_file_content,
    decompressor=gzip.GzipFile,
)
get_bz2_compressed_file_content = partial(
    get_compressed_file_content,
    decompressor=bz2.BZ2File,
)
