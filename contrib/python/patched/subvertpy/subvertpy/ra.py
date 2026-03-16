# Copyright (C) 2006-2008 Jelmer Vernooij <jelmer@jelmer.uk>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
"""Python bindings for Subversion."""

__author__ = "Jelmer Vernooij <jelmer@jelmer.uk>"

from subvertpy import SubversionException, ERR_BAD_URL

from subvertpy import _ra
from subvertpy._ra import *  # noqa: F403,F401
from subvertpy import ra_svn  # noqa: F401

try:
    from urllib2 import splittype
except ImportError:
    from urllib.parse import splittype

url_handlers = {
        "svn": _ra.RemoteAccess,
        # "svn": ra_svn.Client,
        "svn+ssh": _ra.RemoteAccess,
        # "svn+ssh": ra_svn.Client,
        "http": _ra.RemoteAccess,
        "https": _ra.RemoteAccess,
        "file": _ra.RemoteAccess,
}


def RemoteAccess(url, *args, **kwargs):
    """Connect to a remote Subversion server

    :param url: URL to connect to
    :return: RemoteAccess object
    """
    if isinstance(url, bytes):
        url = url.decode("utf-8")
    (type, opaque) = splittype(url)
    if type not in url_handlers:
        raise SubversionException("Unknown URL type '%s'" % type, ERR_BAD_URL)
    return url_handlers[type](url, *args, **kwargs)
