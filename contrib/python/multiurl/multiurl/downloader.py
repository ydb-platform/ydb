# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import logging
import os
from pathlib import Path
from urllib.parse import urlparse

from .file import FullFileDownloader, PartFileDownloader
from .ftp import FullFTPDownloader, PartFTPDownloader
from .heuristics import Part
from .http import FullHTTPDownloader, PartHTTPDownloader, robust
from .multipart import compress_parts

LOG = logging.getLogger(__name__)

__all__ = ["Downloader", "download", "robust"]

DOWNLOADERS = {
    ("ftp", False): FullFTPDownloader,
    ("ftp", True): PartFTPDownloader,
    ("http", False): FullHTTPDownloader,
    ("http", True): PartHTTPDownloader,
    ("https", False): FullHTTPDownloader,
    ("https", True): PartHTTPDownloader,
    ("file", False): FullFileDownloader,
    ("file", True): PartFileDownloader,
}


def _ensure_scheme(url):
    o = urlparse(url)
    if not o.scheme or (len(o.scheme) == 1 and not o.netloc):
        path = Path(url)
        if not path.is_absolute():
            path = Path(os.path.abspath(path))
        url = path.as_uri()
    return url


def _ensure_parts(parts):
    if parts is None:
        return None
    parts = [Part(offset, length) for offset, length in parts]
    if len(parts) == 0:
        return None
    return parts


def _canonicalize(url, **kwargs):
    if not isinstance(url, (list, tuple)):
        url = [url]

    result = []
    if isinstance(url[0], (list, tuple)):
        assert "parts" not in kwargs
        for u, p in url:
            result.append((_ensure_scheme(u), _ensure_parts(p)))
    else:
        p = _ensure_parts(kwargs.pop("parts", None))
        for u in url:
            result.append((_ensure_scheme(u), p))

    urls_and_parts = []
    # Break into ascending order if needed
    for url, parts in result:
        if parts is None:
            urls_and_parts.append((url, None))
            continue

        last = 0
        newparts = []
        for p in parts:
            if p.offset < last:
                if newparts:
                    urls_and_parts.append((url, compress_parts(newparts)))
                    newparts = []
            newparts.append(p)
            last = p.offset
        urls_and_parts.append((url, compress_parts(newparts)))

    return urls_and_parts, kwargs


def Downloader(url, **kwargs):
    urls, kwargs = _canonicalize(url, **kwargs)

    downloaders = []
    for url, parts in urls:
        o = urlparse(url)
        klass = DOWNLOADERS[(o.scheme, parts is not None)]
        downloaders.append(
            klass(url, parts=parts, **kwargs).mutate(url, parts=parts, **kwargs)
        )

    if len(downloaders) == 1:
        return downloaders[0]

    from .multiurl import MultiDownloader

    return MultiDownloader(downloaders)


def download(url, target, **kwargs):
    return Downloader(url, **kwargs).download(target)
