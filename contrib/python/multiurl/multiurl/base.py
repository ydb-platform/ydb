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

LOG = logging.getLogger(__name__)


def _ignore(*args, **kwargs):
    pass


class NoBar:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass


def progress_bar(total, initial=0, desc=None):
    try:
        # There is a bug in tqdm that expects ipywidgets
        # to be installed if running in a notebook
        import ipywidgets  # noqa F401
        from tqdm.auto import tqdm  # noqa F401
    except ImportError:
        try:
            from tqdm import tqdm  # noqa F401
        except ImportError:
            tqdm = NoBar

    return tqdm(
        total=total,
        initial=initial,
        unit_scale=True,
        unit_divisor=1024,
        unit="B",
        disable=False,
        leave=False,
        desc=desc,
    )


class DownloaderBase:
    def __init__(
        self,
        url,
        chunk_size=1024 * 1024,
        timeout=None,
        parts=None,
        observer=_ignore,
        statistics_gatherer=_ignore,
        progress_bar=progress_bar,
        resume_transfers=False,
        override_target_file=True,
        download_file_extension=None,
        auth=None,
        **kwargs,
    ):
        self.url = url
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.parts = parts
        self.observer = observer
        self.statistics_gatherer = statistics_gatherer
        self.progress_bar = progress_bar
        self.resume_transfers = resume_transfers
        self.override_target_file = override_target_file
        self.download_file_extension = download_file_extension
        self.auth = auth

    def mutate(self, *args, **kwargs):
        return self

    def local_path(self):
        return None

    def extension(self, url=None):
        if url is None:
            url = self.url
        url_no_args = url.split("?")[0]
        base = os.path.basename(url_no_args)
        extensions = []
        while True:
            base, ext = os.path.splitext(base)
            if not ext:
                break
            extensions.append(ext)
        if not extensions:
            extensions.append(".unknown")
        return "".join(reversed(extensions))

    def download(self, target):
        if os.path.exists(target) and not self.override_target_file:
            return

        if self.download_file_extension is not None:
            download = target + ".download"
        else:
            download = target

        LOG.info("Downloading %s", self.url)

        size, mode, skip, trust_size = self.estimate_size(download)

        with self.progress_bar(
            total=size,
            initial=skip,
            desc=self.title(),
        ) as pbar:
            with open(download, mode) as f:
                total = self.transfer(f, pbar)

            pbar.close()

        if trust_size and size is not None:
            assert (
                os.path.getsize(download) == size
            ), f"File size mismatch {os.path.getsize(download)} bytes instead of {size}"

        if download != target:
            os.rename(download, target)

        self.finalise()
        return total

    def finalise(self):
        pass

    def title(self):
        return os.path.basename(self.url)

    def cache_data(self):
        return None

    def out_of_date(self, path, cache_data):
        return False
