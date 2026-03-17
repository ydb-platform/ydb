#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
import errno
import os
from os.path import abspath
from os.path import dirname
from os.path import expanduser
from os.path import join
from os.path import exists
import tempfile

"""
Core configuration globals.

Note: this module MUST import ONLY from the standard library.
"""

# this exception is not available on POSIX
try:
    WindowsError  # NOQA
except NameError:

    class WindowsError(Exception):
        pass


def _create_dir(location):
    """
    Create directory and all sub-directories recursively at `location`.
    Raise Exceptions if it fails to create the directory.
    NOTE: this is essentailly a copy of commoncode.fileutils.create_dir()
    """

    if exists(location):
        if not os.path.isdir(location):
            err = ('Cannot create directory: existing file '
                   'in the way ''%(location)s.')
            raise OSError(err % locals())
        return

    # may fail on win if the path is too long
    # FIXME: consider using UNC ?\\ paths
    try:
        os.makedirs(location)

    # avoid multi-process TOCTOU conditions when creating dirs
    # the directory may have been created since the exist check
    except WindowsError as e:
        # [Error 183] Cannot create a file when that file already exists
        if e and e.winerror == 183:
            if not os.path.isdir(location):
                raise
        else:
            raise
    except (IOError, OSError) as o:
        if o.errno == errno.EEXIST:
            if not os.path.isdir(location):
                raise
        else:
            raise

################################################################################
# INVARIABLE INSTALLATION-SPECIFIC, BUILT-IN LOCATIONS AND FLAGS
################################################################################
# these are guaranteed to be there and are entirely based on and relative to the
# current installation location. This is where the source code and static data
# lives.

# in case package is not installed or we do not have setutools/pkg_resources
# on hand fall back to this version
__version__ = '21.6.7'
try:
    from pkg_resources import get_distribution, DistributionNotFound
    try:
        __version__ = get_distribution('scancode-toolkit').version
    except DistributionNotFound:
        pass
except ImportError:
    pass

system_temp_dir = tempfile.gettempdir()
scancode_src_dir = dirname(__file__)
scancode_root_dir = dirname(scancode_src_dir)

################################################################################
# USAGE MODE FLAGS
################################################################################

# tag file or env var to determined if we are in dev mode
SCANCODE_DEV_MODE = (
    os.getenv('SCANCODE_DEV_MODE')
    or exists(join(scancode_root_dir, 'SCANCODE_DEV_MODE'))
)

################################################################################
# USAGE MODE-, INSTALLATION- and IMPORT- and RUN-SPECIFIC DIRECTORIES
################################################################################
# These vary based on the usage mode: dev or not and based on environamnet
# variables

# - scancode_cache_dir: for long-lived caches which are installation-specific:
# this is for cached data which are infrequently written to and mostly readed,
# such as the license index cache. The same location is used across runs of
# a given version of ScanCode
"""
We set the path to an existing directory where ScanCode can cache files
available across runs from the value of the `SCANCODE_CACHE` environment
variable. If `SCANCODE_CACHE` is not set, a default sub-directory in the user
home directory is used instead.
"""
if SCANCODE_DEV_MODE:
    # in dev mode the cache and temp files are stored execlusively under the
    # scancode_root_dir
    scancode_cache_dir = join(scancode_root_dir, '.cache')
else:
    # In other usage modes (as a CLI or as a library, regardless of how
    # installed) the cache dir goes to the home directory and is different for
    # each version
    user_home = abspath(expanduser('~'))
    __env_cache_dir = os.getenv('SCANCODE_CACHE')
    std_scancode_cache_dir = join(user_home, '.cache', 'scancode-tk', __version__)
    scancode_cache_dir = (__env_cache_dir or std_scancode_cache_dir)

# we pre-build the index and bundle this with the the deployed release
# therefore we use package data
# .... but we accept this to be overriden with and env variable
std_license_cache_dir = join(scancode_src_dir, 'licensedcode', 'data', 'cache')
__env_license_cache_dir = os.getenv('SCANCODE_LICENSE_INDEX_CACHE')
licensedcode_cache_dir = (__env_license_cache_dir or std_license_cache_dir)

_create_dir(licensedcode_cache_dir)
_create_dir(scancode_cache_dir)

# - scancode_temp_dir: for short-lived temporary files which are import- or run-
# specific that may live for the duration of a function call or for the duration
# of a whole scancode run, such as any temp file and the per-run scan results
# cache. A new unique location is used for each run of ScanCode (e.g. for the
# lifetime of the Python interpreter process)

"""
We set the path to an existing directory where ScanCode can create temporary
files from the value of the `SCANCODE_TMP` environment variable if available. If
`SCANCODE_TMP` is not set, a default sub-directory in the system temp directory
is used instead. Each scan run creates its own tempfile subdirectory.
"""
__scancode_temp_base_dir = os.getenv('SCANCODE_TEMP')

if not __scancode_temp_base_dir:
    if SCANCODE_DEV_MODE:
        __scancode_temp_base_dir = join(scancode_root_dir, 'tmp')
    else:
        __scancode_temp_base_dir = system_temp_dir

_create_dir(__scancode_temp_base_dir)
_prefix = 'scancode-tk-' + __version__ + '-'
scancode_temp_dir = tempfile.mkdtemp(prefix=_prefix, dir=__scancode_temp_base_dir)
