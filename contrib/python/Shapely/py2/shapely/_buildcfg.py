"""
Minimal proxy to a GEOS C dynamic library, which is system dependant

Two environment variables influence this module: GEOS_LIBRARY_PATH and/or
GEOS_CONFIG.

If GEOS_LIBRARY_PATH is set to a path to a GEOS C shared library, this is
used. Otherwise GEOS_CONFIG can be set to a path to `geos-config`. If
`geos-config` is already on the PATH environment variable, then it will
be used to help better guess the name for the GEOS C dynamic library.
"""

from ctypes import CDLL, cdll, c_void_p, c_char_p
from ctypes.util import find_library
import os
import logging
import re
import subprocess
import sys

from contrib.libs.geos.capi.ctypes import Geos

lgeos = Geos()

# Add message handler to this module's logger
log = logging.getLogger(__name__)
ch = logging.StreamHandler()
log.addHandler(ch)

if 'all' in sys.warnoptions:
    # show GEOS messages in console with: python -W all
    log.setLevel(logging.DEBUG)


def _geos_version():
    # extern const char GEOS_DLL *GEOSversion();
    GEOSversion = lgeos.GEOSversion
    GEOSversion.restype = c_char_p
    GEOSversion.argtypes = []
    # #define GEOS_CAPI_VERSION "@VERSION@-CAPI-@CAPI_VERSION@"
    geos_version_string = GEOSversion()
    if sys.version_info[0] >= 3:
        geos_version_string = geos_version_string.decode('ascii')

    res = re.findall(r'(\d+)\.(\d+)\.(\d+)', geos_version_string)
    geos_version = tuple(int(x) for x in res[0])
    capi_version = tuple(int(x) for x in res[1])

    return geos_version_string, geos_version, capi_version

geos_version_string, geos_version, geos_capi_version = _geos_version()
