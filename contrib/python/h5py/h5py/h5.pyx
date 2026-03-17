# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

include "config.pxi"

from warnings import warn
from .defs cimport *
from ._objects import phil, with_phil
from .h5py_warnings import H5pyDeprecationWarning

ITER_INC    = H5_ITER_INC     # Increasing order
ITER_DEC    = H5_ITER_DEC     # Decreasing order
ITER_NATIVE = H5_ITER_NATIVE  # No particular order, whatever is fastest

INDEX_NAME      = H5_INDEX_NAME       # Index on names
INDEX_CRT_ORDER = H5_INDEX_CRT_ORDER  # Index on creation order

HDF5_VERSION_COMPILED_AGAINST = HDF5_VERSION
NUMPY_VERSION_COMPILED_AGAINST = NUMPY_BUILD_VERSION
CYTHON_VERSION_COMPILED_WITH = CYTHON_BUILD_VERSION


class ByteStringContext:

    def __init__(self):
        self._readbytes = False

    def __bool__(self):
        return self._readbytes

    def __nonzero__(self):
        return self.__bool__()

    def __enter__(self):
        self._readbytes = True

    def __exit__(self, *args):
        self._readbytes = False


cdef class H5PYConfig:

    """
        Provides runtime access to global library settings.  You retrieve the
        master copy of this object by calling h5py.get_config().

        complex_names (tuple, r/w)
            Settable 2-tuple controlling how complex numbers are saved.
            Defaults to ('r','i').

        bool_names (tuple, r/w)
            Settable 2-tuple controlling the HDF5 enum names used for boolean
            values.  Defaults to ('FALSE', 'TRUE') for values 0 and 1.
    """

    def __init__(self):
        self._r_name = b'r'
        self._i_name = b'i'
        self._f_name = b'FALSE'
        self._t_name = b'TRUE'
        self._bytestrings = ByteStringContext()
        self._track_order = False

    @property
    def complex_names(self):
        """ Settable 2-tuple controlling how complex numbers are saved.

        Format is (real_name, imag_name), defaulting to ('r','i').
        """
        with phil:
            import sys
            def handle_val(val):
                return val.decode('utf8')
            return (handle_val(self._r_name), handle_val(self._i_name))

    @complex_names.setter
    def complex_names(self, val):
        with phil:
            def handle_val(val):
                if isinstance(val, unicode):
                    return val.encode('utf8')
                elif isinstance(val, bytes):
                    return val
                else:
                    return bytes(val)
            try:
                if len(val) != 2:
                    raise TypeError()
                r = handle_val(val[0])
                i = handle_val(val[1])
            except Exception:
                raise TypeError("complex_names must be a length-2 sequence of strings (real, img)")
            self._r_name = r
            self._i_name = i

    @property
    def bool_names(self):
        """ Settable 2-tuple controlling HDF5 ENUM names for boolean types.

        Format is (false_name, real_name), defaulting to ('FALSE', 'TRUE').
        """

        with phil:
            return (self._f_name, self._t_name)

    @bool_names.setter
    def bool_names(self, val):
        with phil:
            try:
                if len(val) != 2: raise TypeError()
                f = str(val[0])
                t = str(val[1])
            except Exception:
                raise TypeError("bool_names must be a length-2 sequence of of names (false, true)")
            self._f_name = f
            self._t_name = t

    @property
    def read_byte_strings(self):
        """ Returns a context manager which forces all strings to be returned
        as byte strings. """
        with phil:
            return self._bytestrings

    @property
    def mpi(self):
        """ Boolean indicating if Parallel HDF5 is available """
        return MPI

    @property
    def ros3(self):
        """ Boolean indicating if ROS3 VDS is available """
        return ROS3

    @property
    def direct_vfd(self):
        """ Boolean indicating if DIRECT VFD is available """
        return DIRECT_VFD

    @property
    def swmr_min_hdf5_version(self):
        """ Tuple indicating the minimum HDF5 version required for SWMR features"""
        warn(
            "h5py.get_config().swmr_min_hdf5_version is deprecated. "
            "This version of h5py does not support older HDF5 without SWMR.",
            category=H5pyDeprecationWarning,
        )
        return (1, 9, 178)

    @property
    def vds_min_hdf5_version(self):
        """Tuple indicating the minimum HDF5 version required for virtual dataset (VDS) features"""
        warn(
            "h5py.get_config().vds_min_hdf5_version is deprecated. "
            "This version of h5py does not support older HDF5 without VDS.",
            category=H5pyDeprecationWarning,
        )
        return (1, 9, 233)

    @property
    def track_order(self):
        """ Default value for track_order argument of
        File.open()/Group.create_group()/Group.create_dataset() """
        return self._track_order

    @track_order.setter
    def track_order(self, val):
        self._track_order = val

    @property
    def default_file_mode(self):
        """Default mode for h5py.File()"""
        warn(
            "h5py.get_config().default_file_mode is deprecated. "
            "The default mode is now always 'r' (read-only).",
            category=H5pyDeprecationWarning,
        )
        return 'r'

    @default_file_mode.setter
    def default_file_mode(self, val):
        if val == 'r':
            warn(
                "Setting h5py.default_file_mode is deprecated. "
                "'r' (read-only) is the default from h5py 3.0.",
                category=H5pyDeprecationWarning,
            )
        elif val in {'r+', 'x', 'w-', 'w', 'a'}:
            raise ValueError(
                "Using default_file_mode other than 'r' is no longer "
                "supported. Pass the mode to h5py.File() instead."

            )
        else:
            raise ValueError("Invalid mode; must be one of r, r+, w, w-, x, a")

cdef H5PYConfig cfg = H5PYConfig()

cpdef H5PYConfig get_config():
    """() => H5PYConfig

    Get a reference to the global library configuration object.
    """
    return cfg

@with_phil
def get_libversion():
    """ () => TUPLE (major, minor, release)

        Retrieve the HDF5 library version as a 3-tuple.
    """
    cdef unsigned int major
    cdef unsigned int minor
    cdef unsigned int release
    cdef herr_t retval

    H5get_libversion(&major, &minor, &release)

    return (major, minor, release)
