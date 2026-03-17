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
from ftplib import FTP, error_perm
from urllib.parse import urlparse

from .base import DownloaderBase

LOG = logging.getLogger(__name__)


class FTPDownloaderBase(DownloaderBase):
    def __init__(self, url, **kwargs):
        super().__init__(url, **kwargs)

    def estimate_size(self, target):
        url_object = urlparse(self.url)
        assert url_object.scheme == "ftp"

        user, password = url_object.username, url_object.password

        ftp = FTP(timeout=self.timeout)
        connect_kwargs = {}
        if url_object.port is not None:
            connect_kwargs["port"] = url_object.port
        ftp.connect(host=url_object.hostname, **connect_kwargs)

        ftp.login(user, password)

        ftp.cwd(os.path.dirname(url_object.path))
        ftp.set_pasv(True)
        self.filename = os.path.basename(url_object.path)
        self.ftp = ftp

        try:
            return (ftp.size(self.filename), "wb", 0, True)
        except error_perm:
            return (-1, "wb", True, False)

    def transfer(self, f, pbar):
        total = 0

        def callback(chunk):
            nonlocal total
            self.observer()
            f.write(chunk)
            total += len(chunk)
            pbar.update(len(chunk))

        self.ftp.retrbinary(f"RETR {self.filename}", callback)

    def finalise(self):
        self.ftp.close()


class FullFTPDownloader(FTPDownloaderBase):
    def __repr__(self):
        return f"FullFTPDownloader({self.url})"


class PartFTPDownloader(FTPDownloaderBase):
    def __init__(self, url, **kwargs):
        # If needed, that can be implemented with the PartFilter
        raise NotImplementedError("Part FTPDownloader is not yet implemented")

    def __repr__(self):
        return f"PartFTPDownloader({self.url, self.parts})"
