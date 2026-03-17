#
# (C) Copyright 2017- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

# Authors:
#   Alessandro Amici - B-Open - https://bopen.eu
#   Shahram Najm - ECMWF - https://www.ecmwf.int
#

from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os
import pkgutil
import sys

import cffi

__version__ = "2.43.0"

LOG = logging.getLogger(__name__)

_MAP = {
    "grib_api": "eccodes",
    "gribapi": "eccodes",
}

EXTENSIONS = {
    "darwin": ".dylib",
    "win32": ".dll",
}

# convenient way to trace the search for the library
if int(os.environ.get("ECCODES_PYTHON_TRACE_LIB_SEARCH", "0")):
    LOG.setLevel(logging.DEBUG)
    LOG.addHandler(logging.StreamHandler())


def _lookup(name):
    return _MAP.get(name, name)


def find_binary_libs(name):
    name = _lookup(name)
    env_var = "ECCODES_PYTHON_USE_FINDLIBS"
    if int(os.environ.get(env_var, "0")):
        LOG.debug(f"{name} lib search: {env_var} set, so using findlibs")

    elif os.name == "nt":
        LOG.debug(f"{name} lib search: trying to find binary wheel")
        here = os.path.dirname(__file__)
        # eccodes libs are actually in eccodes dir, not gribapi dir
        here = os.path.abspath(os.path.join(here, os.path.pardir, "eccodes"))
        extension = EXTENSIONS.get(sys.platform, ".so")

        for libdir in [here + ".libs", os.path.join(here, ".dylibs"), here]:
            LOG.debug(f"{name} lib search: looking in {libdir}")
            if not name.startswith("lib"):
                libnames = ["lib" + name, name]
            else:
                libnames = [name, name[3:]]

            if os.path.exists(libdir):
                for file in os.listdir(libdir):
                    if file.endswith(extension):
                        for libname in libnames:
                            if libname == file.split("-")[0].split(".")[0]:
                                foundlib = os.path.join(libdir, file)
                                LOG.debug(
                                    f"{name} lib search: returning wheel library from {foundlib}"
                                )
                                # force linking with the C++ 'glue' library
                                try:
                                    from eccodes._eccodes import versions as _versions
                                except ImportError as e:
                                    LOG.warn(str(e))
                                    raise
                                LOG.debug(
                                    f"{name} lib search: versions: %s", _versions()
                                )
                                return foundlib

        LOG.debug(
            f"{name} lib search: did not find library from wheel; try to find as separate lib"
        )

    # if did not find the binary wheel, or the env var is set, fall back to findlibs
    import findlibs

    foundlib = findlibs.find(name)
    LOG.debug(f"{name} lib search: findlibs returned {foundlib}")
    return foundlib


library_path = None

# default encoding for ecCodes strings
ENC = "ascii"

ffi = cffi.FFI()
CDEF = pkgutil.get_data(__name__, "grib_api.h")
CDEF += pkgutil.get_data(__name__, "eccodes.h")
ffi.cdef(CDEF.decode("utf-8").replace("\r", "\n"))


lib = ffi.dlopen(library_path)
