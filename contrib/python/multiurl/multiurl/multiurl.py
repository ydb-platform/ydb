# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import logging

from .base import DownloaderBase

LOG = logging.getLogger(__name__)


class MultiDownloader(DownloaderBase):
    def __init__(self, downloaders, **kwargs):
        super().__init__("<multiple>", **kwargs)
        self.downloaders = downloaders

    def __repr__(self):
        return f"MultiDownloader({self.downloaders})"

    def estimate_size(self, download):
        total = 0
        trust_size = True
        for downloader in self.downloaders:
            size, _, _, trust = downloader.estimate_size(download)
            if size is not None:
                total += size
            trust_size = trust_size and trust

        return total, "wb", 0, trust_size

    def finalise(self):
        for downloader in self.downloaders:
            downloader.finalise()

    def transfer(self, f, pbar):
        for downloader in self.downloaders:
            downloader.transfer(f, pbar)
