#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import partial
import os
from os import path
import gzip
import tarfile
import zipfile

from commoncode.system import on_windows

"""
Mimimal tar and zip file handling, primarily for testing.
"""


def _extract_tar_raw(test_path, target_dir, to_bytes, *args, **kwargs):
    """
    Raw simplified extract for certain really weird paths and file
    names.
    """
    tar = None
    try:
        tar = tarfile.open(test_path)
        tar.extractall(path=target_dir)
    finally:
        if tar:
            tar.close()


extract_tar_raw = partial(_extract_tar_raw, to_bytes=True)

extract_tar_uni = partial(_extract_tar_raw, to_bytes=False)


def extract_tar(location, target_dir, verbatim=False, *args, **kwargs):
    """
    Extract a tar archive at location in the target_dir directory.
    If `verbatim` is True preserve the permissions.
    """
    # always for using bytes for paths on all OSses... tar seems to use bytes internally
    # and get confused otherwise
    location = os.fsencode(location)
    with open(location, 'rb') as input_tar:
        tar = None
        try:
            tar = tarfile.open(fileobj=input_tar)
            tarinfos = tar.getmembers()
            to_extract = []
            for tarinfo in tarinfos:
                if tar_can_extract(tarinfo, verbatim):
                    if not verbatim:
                        tarinfo.mode = 0o755
                    to_extract.append(tarinfo)
            tar.extractall(target_dir, members=to_extract)
        finally:
            if tar:
                tar.close()


def extract_zip(location, target_dir, *args, **kwargs):
    """
    Extract a zip archive file at location in the target_dir directory.
    """
    if not path.isfile(location) and zipfile.is_zipfile(location):
        raise Exception('Incorrect zip file %(location)r' % locals())

    with zipfile.ZipFile(location) as zipf:
        for info in zipf.infolist():
            name = info.filename
            content = zipf.read(name)
            target = path.join(target_dir, name)
            if not path.exists(path.dirname(target)):
                os.makedirs(path.dirname(target))
            if not content and target.endswith(path.sep):
                if not path.exists(target):
                    os.makedirs(target)
            if not path.exists(target):
                with open(target, 'wb') as f:
                    f.write(content)


def extract_zip_raw(location, target_dir, *args, **kwargs):
    """
    Extract a zip archive file at location in the target_dir directory.
    Use the builtin extractall function
    """
    if not path.isfile(location) and zipfile.is_zipfile(location):
        raise Exception('Incorrect zip file %(location)r' % locals())

    with zipfile.ZipFile(location) as zipf:
        zipf.extractall(path=target_dir)


def tar_can_extract(tarinfo, verbatim):
    """
    Return True if a tar member can be extracted to handle OS specifics.
    If verbatim is True, always return True.
    """
    if tarinfo.ischr():
        # never extract char devices
        return False

    if verbatim:
        # extract all on all OSse
        return True

    # FIXME: not sure hard links are working OK on Windows
    include = tarinfo.type in tarfile.SUPPORTED_TYPES
    exclude = tarinfo.isdev() or (on_windows and tarinfo.issym())

    if include and not exclude:
        return True


def get_gz_compressed_file_content(location):
    """
    Uncompress a compressed file at `location` and return its content as a byte
    string. Raise Exceptions on errors.
    """
    with gzip.GzipFile(location, 'rb') as compressed:
        content = compressed.read()
    return content
