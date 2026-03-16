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
import sys
from urllib.parse import urlparse

from .base import DownloaderBase

LOG = logging.getLogger(__name__)


class FileDownloaderBase(DownloaderBase):
    def __init__(self, url, **kwargs):
        super().__init__(url, **kwargs)
        o = urlparse(self.url)
        path = o.path

        if sys.platform == "win32" and self.url.startswith("file://"):
            # this is because urllib does not decode
            # 'file://C:\Users\name\climetlab\docs\examples\test.nc'
            # as expected.
            path = self.url[len("file://") :]

        if sys.platform == "win32" and path[0] == "/" and path[2] == ":":
            path = path[1:]

        self.path = path


class FullFileDownloader(FileDownloaderBase):
    def local_path(self):
        return self.path

    def __repr__(self):
        return f"FullFileDownloader({self.path})"

    def estimate_size(self, target):
        # TODO: resume transfers
        size = os.path.getsize(self.path)
        return (size, "wb", 0, True)

    def transfer(self, f, pbar):
        total = 0
        with open(self.path, "rb") as g:
            while True:
                chunk = g.read(self.chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                pbar.update(len(chunk))
                total += len(chunk)
        return total


class PartFileDownloader(FileDownloaderBase):
    def __repr__(self):
        return f"PartFileDownloader({self.path, self.parts})"

    def estimate_size(self, target):
        parts = self.parts
        size = sum(p.length for p in parts)
        return (size, "wb", 0, True)

    def transfer(self, f, pbar):
        with open(self.path, "rb") as g:
            total = 0
            for offset, length in self.parts:
                g.seek(offset)
                self.observer()
                while length > 0:
                    chunk = g.read(min(length, self.chunk_size))
                    assert chunk
                    f.write(chunk)
                    length -= len(chunk)
                    total += len(chunk)
                    pbar.update(len(chunk))
        return total
