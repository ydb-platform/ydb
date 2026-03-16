import logging
import os
import sys

import cffi

ffibuilder = cffi.FFI()
ffibuilder.set_source(
    "gribapi._bindings",
    "#include <eccodes.h>",
    libraries=["eccodes"],
)
dirname = os.path.dirname(__file__)
grib_api_h_path = os.path.join(dirname, "gribapi/grib_api.h")
eccodes_h_path = os.path.join(dirname, "gribapi/eccodes.h")
ffibuilder.cdef(open(grib_api_h_path).read() + open(eccodes_h_path).read())

if __name__ == "__main__":
    try:
        ffibuilder.compile(verbose=True)
    except Exception:
        logging.exception("can't compile ecCodes bindings")
        sys.exit(1)
