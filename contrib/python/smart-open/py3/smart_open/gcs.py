# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing to/from GCS."""

import logging
import warnings

try:
    import google.cloud.exceptions
    import google.cloud.storage
    import google.auth.transport.requests
except ImportError:
    MISSING_DEPS = True

import smart_open.bytebuffer
import smart_open.utils

from smart_open import constants

logger = logging.getLogger(__name__)

SCHEME = "gs"
"""Supported scheme for GCS"""

_DEFAULT_MIN_PART_SIZE = 50 * 1024**2
"""Default minimum part size for GCS multipart uploads"""

_DEFAULT_WRITE_OPEN_KWARGS = {'ignore_flush': True}


def parse_uri(uri_as_string):
    sr = smart_open.utils.safe_urlsplit(uri_as_string)
    assert sr.scheme == SCHEME
    bucket_id = sr.netloc
    blob_id = sr.path.lstrip('/')
    return dict(scheme=SCHEME, bucket_id=bucket_id, blob_id=blob_id)


def open_uri(uri, mode, transport_params):
    parsed_uri = parse_uri(uri)
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(parsed_uri['bucket_id'], parsed_uri['blob_id'], mode, **kwargs)


def warn_deprecated(parameter_name):
    message = f"Parameter {parameter_name} is deprecated, this parameter no-longer has any effect"
    warnings.warn(message, UserWarning)


def open(
    bucket_id,
    blob_id,
    mode,
    buffer_size=None,
    min_part_size=_DEFAULT_MIN_PART_SIZE,
    client=None,  # type: google.cloud.storage.Client
    get_blob_kwargs=None,
    blob_properties=None,
    blob_open_kwargs=None,
):
    """Open an GCS blob for reading or writing.

    Parameters
    ----------
    bucket_id: str
        The name of the bucket this object resides in.
    blob_id: str
        The name of the blob within the bucket.
    mode: str
        The mode for opening the object. Must be either "rb" or "wb".
    buffer_size:
        deprecated
    min_part_size: int, optional
        The minimum part size for multipart uploads. For writing only.
    client: google.cloud.storage.Client, optional
        The GCS client to use when working with google-cloud-storage.
    get_blob_kwargs: dict, optional
        Additional keyword arguments to propagate to the bucket.get_blob
        method of the google-cloud-storage library. For reading only.
    blob_properties: dict, optional
        Set properties on blob before writing. For writing only.
    blob_open_kwargs: dict, optional
        Additional keyword arguments to propagate to the blob.open method
        of the google-cloud-storage library.

    """
    if blob_open_kwargs is None:
        blob_open_kwargs = {}

    if buffer_size is not None:
        warn_deprecated('buffer_size')

    if mode in (constants.READ_BINARY, 'r', 'rt'):
        _blob = Reader(bucket=bucket_id,
                       key=blob_id,
                       client=client,
                       get_blob_kwargs=get_blob_kwargs,
                       blob_open_kwargs=blob_open_kwargs)

    elif mode in (constants.WRITE_BINARY, 'w', 'wt'):
        _blob = Writer(bucket=bucket_id,
                       blob=blob_id,
                       min_part_size=min_part_size,
                       client=client,
                       blob_properties=blob_properties,
                       blob_open_kwargs=blob_open_kwargs)

    else:
        raise NotImplementedError(f'GCS support for mode {mode} not implemented')

    return _blob


def Reader(bucket,
           key,
           buffer_size=None,
           line_terminator=None,
           client=None,
           get_blob_kwargs=None,
           blob_open_kwargs=None):

    if get_blob_kwargs is None:
        get_blob_kwargs = {}
    if blob_open_kwargs is None:
        blob_open_kwargs = {}
    if client is None:
        client = google.cloud.storage.Client()
    if buffer_size is not None:
        warn_deprecated('buffer_size')
    if line_terminator is not None:
        warn_deprecated('line_terminator')

    bkt = client.bucket(bucket)
    blob = bkt.get_blob(key, **get_blob_kwargs)

    if blob is None:
        raise google.cloud.exceptions.NotFound(f'blob {key} not found in {bucket}')

    return blob.open('rb', **blob_open_kwargs)


def Writer(bucket,
           blob,
           min_part_size=None,
           client=None,
           blob_properties=None,
           blob_open_kwargs=None):

    if blob_open_kwargs is None:
        blob_open_kwargs = {}
    if blob_properties is None:
        blob_properties = {}
    if client is None:
        client = google.cloud.storage.Client()

    blob_open_kwargs = {**_DEFAULT_WRITE_OPEN_KWARGS, **blob_open_kwargs}

    g_blob = client.bucket(bucket).blob(
        blob,
        chunk_size=min_part_size,
    )

    for k, v in blob_properties.items():
        setattr(g_blob, k, v)

    _blob = g_blob.open('wb', **blob_open_kwargs)

    # backwards-compatiblity, was deprecated upstream https://cloud.google.com/storage/docs/resumable-uploads
    _blob.terminate = lambda: None

    return _blob
