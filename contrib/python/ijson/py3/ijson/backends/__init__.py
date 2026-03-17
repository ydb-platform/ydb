import os
import warnings

class YAJLImportError(ImportError):
    pass


def require_version(version, required):
    '''
    Asserts that the major component of 'version' is equal to 'required'.
    Raises YAJLImportError otherwise.
    '''
    major, rest = divmod(version, 10000)
    minor, micro = divmod(rest, 100)
    if major != required:
        raise YAJLImportError('YAJL version %s.x required, found %s.%s.%s' % (required, major, minor, micro))

def get_yajl_version(yajl):
    try:
        return yajl.yajl_version()
    except AttributeError:
        warnings.warn('Cannot determine yajl version, assuming <1.0.12')
        return 10000

def find_yajl_ctypes(required):
    '''
    Finds and loads yajl shared object of the required major
    version (1, 2, ...) using ctypes.
    '''
    # Importing ``ctypes`` should be in scope of this function to prevent failure
    # of `backends`` package load in a runtime where ``ctypes`` is not available.
    # Example of such environment is Google App Engine (GAE).
    from ctypes import util, cdll

    so_name = os.getenv('YAJL_DLL') or util.find_library('yajl')
    if so_name is None:
        raise YAJLImportError('YAJL shared object not found.')
    try:
        yajl = cdll.LoadLibrary(so_name)
    except OSError:
        raise YAJLImportError('Unable to load YAJL.')
    require_version(get_yajl_version(yajl), required)
    return yajl

def find_yajl_cffi(ffi, required):
    '''
    Finds and loads yajl shared object of the required major
    version (1, 2, ...) using cffi.
    '''
    try:
        yajl = ffi.dlopen(os.getenv('YAJL_DLL') or 'yajl')
    except OSError:
        raise YAJLImportError('Unable to load YAJL.')
    require_version(get_yajl_version(yajl), required)
    return yajl
