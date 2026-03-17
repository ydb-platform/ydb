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
__version__ = (0, 10, 1)

NODE_DIR = 2
NODE_FILE = 1
NODE_NONE = 0
NODE_UNKNOWN = 3

ERR_UNSUPPORTED_FEATURE = 200007
ERR_RA_SVN_UNKNOWN_CMD = 210001
ERR_RA_SVN_CONNECTION_CLOSED = 210002
ERR_WC_LOCKED = 155004
ERR_RA_NOT_AUTHORIZED = 170001
ERR_INCOMPLETE_DATA = 200003
ERR_DIR_NOT_EMPTY = 200011
ERR_RA_SVN_MALFORMED_DATA = 210004
ERR_RA_NOT_IMPLEMENTED = 170003
ERR_FS_NO_SUCH_REVISION = 160006
ERR_FS_TXN_OUT_OF_DATE = 160028
ERR_REPOS_DISABLED_FEATURE = 165006
ERR_STREAM_MALFORMED_DATA = 140001
ERR_RA_ILLEGAL_URL = 170000
ERR_RA_LOCAL_REPOS_OPEN_FAILED = 180001
ERR_BAD_FILENAME = 125001
ERR_BAD_URL = 125002
ERR_BAD_DATE = 125003
ERR_RA_DAV_REQUEST_FAILED = 175002
ERR_RA_DAV_PATH_NOT_FOUND = 175007
ERR_FS_NOT_DIRECTORY = 160016
ERR_FS_NOT_FOUND = 160013
ERR_FS_ALREADY_EXISTS = 160020
ERR_RA_SVN_REPOS_NOT_FOUND = 210005
ERR_WC_NOT_WORKING_COPY = ERR_WC_NOT_DIRECTORY = 155007
ERR_ENTRY_EXISTS = 150002
ERR_WC_PATH_NOT_FOUND = 155010
ERR_CANCELLED = 200015
ERR_WC_UNSUPPORTED_FORMAT = 155021
ERR_UNKNOWN_CAPABILITY = 200026
ERR_AUTHN_NO_PROVIDER = 215001
ERR_RA_DAV_RELOCATED = 175011
ERR_FS_NOT_FILE = 160017
ERR_WC_BAD_ADM_LOG = 155009
ERR_WC_BAD_ADM_LOG_START = 155020
ERR_WC_NOT_LOCKED = 155005
ERR_RA_DAV_NOT_VCC = 20014
ERR_REPOS_HOOK_FAILURE = 165001
ERR_XML_MALFORMED = 130003
ERR_MALFORMED_FILE = 200002
ERR_FS_PATH_SYNTAX = 160005
ERR_RA_DAV_FORBIDDEN = 175013
ERR_WC_SCHEDULE_CONFLICT = 155013
ERR_RA_DAV_PROPPATCH_FAILED = 175008
ERR_SVNDIFF_CORRUPT_WINDOW = 185001
ERR_FS_CONFLICT = 160024
ERR_NODE_UNKNOWN_KIND = 145000
ERR_RA_SERF_SSL_CERT_UNTRUSTED = 230001
ERR_ENTRY_NOT_FOUND = 150000
ERR_BAD_PROPERTY_VALUE = 125005
ERR_FS_ROOT_DIR = 160021
ERR_WC_NODE_KIND_CHANGE = 155018
ERR_WC_UPGRADE_REQUIRED = 155036
ERR_RA_CANNOT_CREATE_SESSION = 170013

ERR_APR_OS_START_EAIERR = 670000
ERR_APR_OS_ERRSPACE_SIZE = 50000
ERR_CATEGORY_SIZE = 5000


# These will be removed in the next version of subvertpy
ERR_EAI_NONAME = 670008
ERR_UNKNOWN_HOSTNAME = 670002

AUTH_PARAM_DEFAULT_USERNAME = 'svn:auth:username'
AUTH_PARAM_DEFAULT_PASSWORD = 'svn:auth:password'

SSL_NOTYETVALID = 0x00000001
SSL_EXPIRED = 0x00000002
SSL_CNMISMATCH = 0x00000004
SSL_UNKNOWNCA = 0x00000008
SSL_OTHER = 0x40000000


class SubversionException(Exception):
    """A Subversion exception"""

    def __init__(self, msg, num, child=None, location=None):
        self.args = (msg, num)
        self.child = child
        self.location = location


def _check_mtime(m):
    """Check whether a C extension is out of date.

    :param m: Python module that is a C extension
    """
    import os
    (base, _) = os.path.splitext(m.__file__)
    c_file = "%s.c" % base
    if not os.path.exists(c_file):
        return True
    if os.path.getmtime(m.__file__) < os.path.getmtime(c_file):
        return False
    return True


try:
    from subvertpy import client, _ra, repos, wc
    # (ilezhankin): no need to check mtime in Arcadia build.
    # for x in client, _ra, repos, wc:
    #    if not _check_mtime(x):
    #        from warnings import warn
    #        warn("subvertpy extensions are outdated and need to be rebuilt")
    #        break
except ImportError as e:
    raise ImportError("Unable to load subvertpy extensions: %s" % e)
