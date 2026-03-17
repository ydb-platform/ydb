#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import sys
import time

from pysmi import __version__ as pysmi_version
from pysmi import debug
from pysmi import error
from pysmi.compat import decode
from pysmi.mibinfo import MibInfo
from pysmi.reader.base import AbstractReader
from requests import session


class HttpReader(AbstractReader):
    """Fetch ASN.1 MIB text by name from a web site.

    *HttpReader* class instance tries to download ASN.1 MIB files
    by name and return their contents to caller.
    """

    MIB_MAGIC = "@mib@"

    def __init__(self, url):
        """Create an instance of *HttpReader* bound to specific URL.

        Note:
            The `http_proxy` and `https_proxy` environment variables are
            respected by the underlying `urllib` stdlib module.

        Args:
            host (str): domain name or IP address of web server
            port (int): TCP port web server is listening
            locationTemplate (str): location part of the URL optionally containing @mib@ magic placeholder to be replaced with MIB name. If @mib@ magic is not present, MIB name is appended to `locationTemplate`

        Keyword Args:
            timeout (int): response timeout
            ssl (bool): access HTTPS web site
        """
        self._url = url

        self.session = session()

        self._user_agent = f"pysmi-{pysmi_version}; python-{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}; {sys.platform}"

    def __str__(self):
        """Return string representation of the instance."""
        return self._url

    def get_data(self, mibname, **options):
        headers = {"Accept": "text/plain", "User-Agent": self._user_agent}

        mibname = decode(mibname)

        debug.logger & debug.FLAG_READER and debug.logger(f"looking for MIB {mibname}")

        for mibalias, mibfile in self.get_mib_variants(mibname, **options):
            if self.MIB_MAGIC in self._url:
                url = self._url.replace(self.MIB_MAGIC, mibfile)
            else:
                url = self._url + mibfile

            debug.logger & debug.FLAG_READER and debug.logger(
                f"trying to fetch MIB from {url}"
            )

            try:
                response = self.session.get(url, headers=headers)

            except Exception:
                debug.logger & debug.FLAG_READER and debug.logger(
                    f"failed to fetch MIB from {url}: {sys.exc_info()[1]}"
                )
                continue

            debug.logger & debug.FLAG_READER and debug.logger(
                f"HTTP response {response.status_code}"
            )

            if response.status_code == 200:
                try:
                    mtime = time.mktime(
                        time.strptime(
                            response.headers["Last-Modified"],
                            "%a, %d %b %Y %H:%M:%S %Z",
                        )
                    )

                except Exception:
                    debug.logger & debug.FLAG_READER and debug.logger(
                        f"malformed HTTP headers: {sys.exc_info()[1]}"
                    )
                    mtime = time.time()

                debug.logger & debug.FLAG_READER and debug.logger(
                    f"fetching source MIB {url}, mtime {response.headers['Last-Modified']}"
                )

                return MibInfo(
                    path=url, file=mibfile, name=mibalias, mtime=mtime
                ), response.content.decode("utf-8")

        raise error.PySmiReaderFileNotFoundError(
            f"source MIB {mibname} not found", reader=self
        )
