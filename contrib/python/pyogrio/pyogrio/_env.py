# With Python >= 3.8 on Windows directories in PATH are not automatically
# searched for DLL dependencies and must be added manually with
# os.add_dll_directory.
# adapted from Fiona: https://github.com/Toblerity/Fiona/pull/875


import logging
import os
import platform
from contextlib import contextmanager
from pathlib import Path

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


try:
    # set GDAL_CURL_CA_BUNDLE / PROJ_CURL_CA_BUNDLE for GDAL >= 3.2
    import certifi

    ca_bundle = certifi.where()
    #os.environ.setdefault("GDAL_CURL_CA_BUNDLE", ca_bundle)
    #os.environ.setdefault("PROJ_CURL_CA_BUNDLE", ca_bundle)
except ImportError:
    pass


gdal_dll_dir = None

if platform.system() == "Windows":
    # if loading of extension modules fails, search for gdal dll directory
    try:
        import pyogrio._io  # noqa: F401

    except ImportError:
        for path in os.getenv("PATH", "").split(os.pathsep):
            if list(Path(path).glob("gdal*.dll")):
                log.info(f"Found GDAL at {path}")
                gdal_dll_dir = path
                break

        if not gdal_dll_dir:
            raise ImportError(
                "GDAL DLL could not be found.  It must be on the system PATH."
            )


@contextmanager
def GDALEnv():
    dll_dir = None

    if gdal_dll_dir:
        dll_dir = os.add_dll_directory(gdal_dll_dir)

    try:
        yield None
    finally:
        if dll_dir is not None:
            dll_dir.close()
