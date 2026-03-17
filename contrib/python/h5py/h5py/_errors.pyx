# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2019 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

# Python-style minor error classes.  If the minor error code matches an entry
# in this dict, the generated exception will be used.

include "config.pxi"

from cpython cimport PyErr_Occurred, PyErr_SetObject
import re


_minor_table = {
    H5E_SEEKERROR:      OSError,    # Seek failed
    H5E_READERROR:      OSError,    # Read failed
    H5E_WRITEERROR:     OSError,    # Write failed
    H5E_CLOSEERROR:     OSError,    # Close failed
    H5E_OVERFLOW:       OSError,    # Address overflowed
    H5E_FCNTL:          OSError,    # File control (fcntl) failed

    H5E_FILEEXISTS:     OSError,    # File already exists
    H5E_FILEOPEN:       OSError,    # File already open
    H5E_CANTCREATE:     OSError,    # Unable to create file
    H5E_CANTOPENFILE:   OSError,    # Unable to open file
    H5E_CANTCLOSEFILE:  OSError,    # Unable to close file
    H5E_NOTHDF5:        OSError,    # Not an HDF5 file
    H5E_BADFILE:        ValueError, # Bad file ID accessed
    H5E_TRUNCATED:      OSError,    # File has been truncated
    H5E_MOUNT:          OSError,    # File mount error

    H5E_NOFILTER:       OSError,    # Requested filter is not available
    H5E_CALLBACK:       OSError,    # Callback failed
    H5E_CANAPPLY:       OSError,    # Error from filter 'can apply' callback
    H5E_SETLOCAL:       OSError,    # Error from filter 'set local' callback
    H5E_NOENCODER:      OSError,    # Filter present but encoding disabled

    H5E_BADGROUP:       ValueError,  # Unable to find ID group information
    H5E_BADSELECT:      ValueError,  # Invalid selection (hyperslabs)
    H5E_UNINITIALIZED:  ValueError,  # Information is uninitialized
    H5E_UNSUPPORTED:    NotImplementedError,    # Feature is unsupported

    H5E_NOTFOUND:       KeyError,    # Object not found
    H5E_CANTINSERT:     ValueError,  # Unable to insert object

    H5E_BADTYPE:        TypeError,   # Inappropriate type
    H5E_BADRANGE:       ValueError,  # Out of range
    H5E_BADVALUE:       ValueError,  # Bad value

    H5E_EXISTS:         ValueError,  # Object already exists
    H5E_ALREADYEXISTS:  ValueError,  # Object already exists, part II
    H5E_CANTCONVERT:    TypeError,   # Can't convert datatypes

    H5E_CANTDELETE:     KeyError,    # Can't delete message

    H5E_CANTOPENOBJ:    KeyError,

    H5E_CANTMOVE:       ValueError,  # Can't move a link
}

# "Fudge" table to accommodate annoying inconsistencies in HDF5's use
# of the minor error codes.  If a (major, minor) entry appears here,
# it will override any entry in the minor error table.
_exact_table = {
    (H5E_CACHE, H5E_BADVALUE):      OSError,    # obj create w/o write intent
    (H5E_RESOURCE, H5E_CANTINIT):   OSError,    # obj create w/o write intent
    (H5E_INTERNAL, H5E_SYSERRSTR):  OSError,    # e.g. wrong file permissions
    (H5E_DATATYPE, H5E_CANTINIT):   TypeError,  # No conversion path
    (H5E_DATASET, H5E_CANTINIT):    ValueError, # bad param for dataset setup
    (H5E_ARGS, H5E_CANTINIT):       TypeError,  # Illegal operation on object
    (H5E_ARGS, H5E_BADTYPE):        ValueError, # Invalid location in file
    (H5E_REFERENCE, H5E_CANTINIT):  ValueError, # Dereferencing invalid ref

    # due to changes to H5F.c:H5Fstart_swmr_write
    (H5E_FILE, H5E_CANTCONVERT):    ValueError, # Invalid file format
}

IF HDF5_VERSION > (1, 12, 0):
    _exact_table[(H5E_DATASET, H5E_CANTCREATE)] = ValueError  # bad param for dataset setup

IF HDF5_VERSION < (1, 13, 0):
    _minor_table[H5E_BADATOM] = ValueError  # Unable to find atom information (already closed?)
    _exact_table[(H5E_SYM, H5E_CANTINIT)] = ValueError  # Object already exists/1.8
ELSE:
    _minor_table[H5E_BADID] = ValueError  # Unable to find ID information
    _exact_table[(H5E_SYM, H5E_CANTCREATE)] = ValueError  # Object already exists

cdef struct err_data_t:
    H5E_error_t err
    int n

cdef herr_t walk_cb(unsigned int n, const H5E_error_t *desc, void *e) noexcept nogil:

    cdef err_data_t *ee = <err_data_t*>e

    ee[0].err.maj_num = desc[0].maj_num
    ee[0].err.min_num = desc[0].min_num
    ee[0].err.desc = desc[0].desc
    ee[0].n = n

cdef int set_exception() except -1:

    cdef err_data_t err
    cdef const char *desc = NULL          # Note: HDF5 forbids freeing these
    cdef const char *desc_bottom = NULL

    if PyErr_Occurred():
        # An exception was already set, e.g. by a Python callback within the
        # HDF5 call. Skip translating the HDF5 error, and let the Python
        # exception propagate.
        return 1

    # First, extract the major & minor error codes from the top of the
    # stack, along with the top-level error description

    err.n = -1

    if H5Ewalk(<hid_t>H5E_DEFAULT, H5E_WALK_UPWARD, walk_cb, &err) < 0:
        raise RuntimeError("Failed to walk error stack")

    if err.n < 0:   # No HDF5 exception information found
        return 0

    eclass = _minor_table.get(err.err.min_num, RuntimeError)
    eclass = _exact_table.get((err.err.maj_num, err.err.min_num), eclass)

    desc = err.err.desc
    if desc is NULL:
        raise RuntimeError("Failed to extract top-level error description")

    # Second, retrieve the bottom-most error description for additional info

    err.n = -1

    if H5Ewalk(<hid_t>H5E_DEFAULT, H5E_WALK_DOWNWARD, walk_cb, &err) < 0:
        raise RuntimeError("Failed to walk error stack")

    desc_bottom = err.err.desc
    if desc_bottom is NULL:
        raise RuntimeError("Failed to extract bottom-level error description")

    msg = b"%b (%b)" % (bytes(desc).capitalize(), bytes(desc_bottom))

    # Finally, set the exception.  We do this with a Python C function
    # so that the traceback doesn't point here.

    m = re.search(b'errno\s*=\s*(\d+)', desc_bottom)
    if m and eclass is OSError:
        # Python can automatically create an appropriate OSError subclass
        # (e.g. FileNotFoundError) given the POSIX errno (e.g. ENOENT)
        errno = int(m.group(1))
        PyErr_SetObject(OSError, (errno, msg.decode('utf-8', 'replace')))
    else:
        PyErr_SetString(eclass, msg)

    return 1


cdef extern from "stdio.h":
    void *stderr

cdef err_cookie _error_handler  # Store error handler used by h5py
_error_handler.func = NULL
_error_handler.data = NULL

cdef void set_default_error_handler() noexcept nogil:
    """Set h5py's current default error handler"""
    H5Eset_auto(<hid_t>H5E_DEFAULT, _error_handler.func, _error_handler.data)

def silence_errors():
    """ Disable HDF5's automatic error printing in this thread """
    _error_handler.func = NULL
    _error_handler.data = NULL
    set_default_error_handler()

def unsilence_errors():
    """ Re-enable HDF5's automatic error printing in this thread """
    _error_handler.func = <H5E_auto_t> H5Eprint
    _error_handler.data = stderr
    set_default_error_handler()


cdef err_cookie set_error_handler(err_cookie handler):
    # Note: exceptions here will be printed instead of raised.

    cdef err_cookie old_handler

    if H5Eget_auto(<hid_t>H5E_DEFAULT, &old_handler.func, &old_handler.data) < 0:
        raise RuntimeError("Failed to retrieve old handler")

    if H5Eset_auto(<hid_t>H5E_DEFAULT, handler.func, handler.data) < 0:
        raise RuntimeError("Failed to install new handler")

    return old_handler
