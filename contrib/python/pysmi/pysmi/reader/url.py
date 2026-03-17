#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import warnings
from urllib import parse as urlparse
from urllib.request import url2pathname

from pysmi import error
from pysmi.reader.httpclient import HttpReader
from pysmi.reader.localfile import FileReader
from pysmi.reader.zipreader import ZipReader


def get_readers_from_urls(*sourceUrls, **options):
    readers = []
    for sourceUrl in sourceUrls:
        mibSource = urlparse.urlparse(sourceUrl)

        if mibSource.scheme in ("", "file", "zip"):
            scheme = mibSource.scheme
            filePath = url2pathname(mibSource.path)
            if scheme != "file" and (
                filePath.endswith(".zip") or filePath.endswith(".ZIP")
            ):
                scheme = "zip"

            else:
                scheme = "file"

            if scheme == "file":
                readers.append(FileReader(filePath).set_options(**options))
            else:
                readers.append(ZipReader(filePath).set_options(**options))

        elif mibSource.scheme in ("http", "https"):
            readers.append(HttpReader(sourceUrl).set_options(**options))

        else:
            raise error.PySmiError(f"Unsupported URL scheme {sourceUrl}")

    return readers


# Compatibility API
deprecated_attributes = {
    "getReadersFromUrls": "get_readers_from_urls",
}


def __getattr__(attr: str):
    """Handle deprecated attributes."""
    if newAttr := deprecated_attributes.get(attr):
        warnings.warn(
            f"{attr} is deprecated. Please use {newAttr} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[newAttr]
    raise AttributeError(f"module {__name__} has no attribute {attr}")
