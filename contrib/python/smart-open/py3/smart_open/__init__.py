# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""
Utilities for streaming to/from several file-like data storages: S3 / HDFS / local
filesystem / compressed files, and many more, using a simple, Pythonic API.

The streaming makes heavy use of generators and pipes, to avoid loading
full file contents into memory, allowing work with arbitrarily large files.

The main functions are:

* `open()`, which opens the given file for reading/writing
* `parse_uri()`
* `s3_iter_bucket()`, which goes over all keys in an S3 bucket in parallel
* `register_compressor()`, which registers callbacks for transparent compressor handling

"""

import contextlib
import logging
from importlib.metadata import PackageNotFoundError, version

with contextlib.suppress(PackageNotFoundError):
    __version__ = version("smart_open")
#
# Prevent regression of #474 and #475
#
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

from .smart_open_lib import open, parse_uri, smart_open, register_compressor  # noqa: E402

_WARNING = """smart_open.s3_iter_bucket is deprecated and will stop functioning
in a future version. Please import iter_bucket from the smart_open.s3 module instead:

    from smart_open.s3 import iter_bucket as s3_iter_bucket

"""
_WARNED = False


def s3_iter_bucket(
        bucket_name,
        prefix='',
        accept_key=None,
        key_limit=None,
        workers=16,
        retries=3,
        **session_kwargs
):
    """Deprecated.  Use smart_open.s3.iter_bucket instead."""
    global _WARNED
    from .s3 import iter_bucket
    if not _WARNED:
        logger.warning(_WARNING)
        _WARNED = True
    return iter_bucket(
        bucket_name=bucket_name,
        prefix=prefix,
        accept_key=accept_key,
        key_limit=key_limit,
        workers=workers,
        retries=retries,
        session_kwargs=session_kwargs
    )


__all__ = [
    'open',
    'parse_uri',
    'register_compressor',
    's3_iter_bucket',
    'smart_open',
]
