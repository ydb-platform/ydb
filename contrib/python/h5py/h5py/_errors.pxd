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

from .api_types_hdf5 cimport *

# Auto-set exception.  Returns 1 if exception set, 0 if no HDF5 error found.
cdef int set_exception() except -1

cdef extern from "hdf5.h":

    IF HDF5_VERSION < (1, 13, 0):
        ctypedef enum H5E_major_t:
            H5E_NONE_MAJOR       = 0,   # special zero, no error
            H5E_ARGS,                   # invalid arguments to routine
            H5E_RESOURCE,               # resource unavailable
            H5E_INTERNAL,               # Internal error (too specific to document)
            H5E_FILE,                   # file Accessibility
            H5E_IO,                     # Low-level I/O
            H5E_FUNC,                   # function Entry/Exit
            H5E_ATOM,                   # object Atom
            H5E_CACHE,                  # object Cache
            H5E_BTREE,                  # B-Tree Node
            H5E_SYM,                    # symbol Table
            H5E_HEAP,                   # Heap
            H5E_OHDR,                   # object Header
            H5E_DATATYPE,               # Datatype
            H5E_DATASPACE,              # Dataspace
            H5E_DATASET,                # Dataset
            H5E_STORAGE,                # data storage
            H5E_PLIST,                  # Property lists
            H5E_ATTR,                   # Attribute
            H5E_PLINE,                  # Data filters
            H5E_EFL,                    # External file list
            H5E_REFERENCE,              # References
            H5E_VFL,                    # Virtual File Layer
            H5E_TST,                    # Ternary Search Trees
            H5E_RS,                     # Reference Counted Strings
            H5E_ERROR,                  # Error API
            H5E_SLIST                   # Skip Lists

        ctypedef enum H5E_minor_t:
            # Generic low-level file I/O errors
            H5E_SEEKERROR      # Seek failed
            H5E_READERROR      # Read failed
            H5E_WRITEERROR     # Write failed
            H5E_CLOSEERROR     # Close failed
            H5E_OVERFLOW       # Address overflowed
            H5E_FCNTL          # File control (fcntl) failed

            # Resource errors
            H5E_NOSPACE        # No space available for allocation
            H5E_CANTALLOC      # Can't allocate space
            H5E_CANTCOPY       # Unable to copy object
            H5E_CANTFREE       # Unable to free object
            H5E_ALREADYEXISTS  # Object already exists
            H5E_CANTLOCK       # Unable to lock object
            H5E_CANTUNLOCK     # Unable to unlock object
            H5E_CANTGC         # Unable to garbage collect
            H5E_CANTGETSIZE    # Unable to compute size
            H5E_OBJOPEN        # Object is already open

            # Heap errors
            H5E_CANTRESTORE    # Can't restore condition
            H5E_CANTCOMPUTE    # Can't compute value
            H5E_CANTEXTEND     # Can't extend heap's space
            H5E_CANTATTACH     # Can't attach object
            H5E_CANTUPDATE     # Can't update object
            H5E_CANTOPERATE    # Can't operate on object

            # Function entry/exit interface errors
            H5E_CANTINIT       # Unable to initialize object
            H5E_ALREADYINIT    # Object already initialized
            H5E_CANTRELEASE    # Unable to release object

            # Property list errors
            H5E_CANTGET        # Can't get value
            H5E_CANTSET        # Can't set value
            H5E_DUPCLASS       # Duplicate class name in parent class

            # Free space errors
            H5E_CANTMERGE      # Can't merge objects
            H5E_CANTREVIVE     # Can't revive object
            H5E_CANTSHRINK     # Can't shrink container

            # Object header related errors
            H5E_LINKCOUNT      # Bad object header link
            H5E_VERSION        # Wrong version number
            H5E_ALIGNMENT      # Alignment error
            H5E_BADMESG        # Unrecognized message
            H5E_CANTDELETE     # Can't delete message
            H5E_BADITER        # Iteration failed
            H5E_CANTPACK       # Can't pack messages
            H5E_CANTRESET      # Can't reset object count

            # System level errors
            H5E_SYSERRSTR      # System error message

            # I/O pipeline errors
            H5E_NOFILTER       # Requested filter is not available
            H5E_CALLBACK       # Callback failed
            H5E_CANAPPLY       # Error from filter 'can apply' callback
            H5E_SETLOCAL       # Error from filter 'set local' callback
            H5E_NOENCODER      # Filter present but encoding disabled
            H5E_CANTFILTER     # Filter operation failed

            # Group related errors
            H5E_CANTOPENOBJ    # Can't open object
            H5E_CANTCLOSEOBJ   # Can't close object
            H5E_COMPLEN        # Name component is too long
            H5E_PATH           # Problem with path to object

            # No error
            H5E_NONE_MINOR     # No error

            # File accessibility errors
            H5E_FILEEXISTS     # File already exists
            H5E_FILEOPEN       # File already open
            H5E_CANTCREATE     # Unable to create file
            H5E_CANTOPENFILE   # Unable to open file
            H5E_CANTCLOSEFILE  # Unable to close file
            H5E_NOTHDF5        # Not an HDF5 file
            H5E_BADFILE        # Bad file ID accessed
            H5E_TRUNCATED      # File has been truncated
            H5E_MOUNT          # File mount error

            # Object atom related errors
            H5E_BADATOM        # Unable to find atom information (already closed?)
            H5E_BADGROUP       # Unable to find ID group information
            H5E_CANTREGISTER   # Unable to register new atom
            H5E_CANTINC        # Unable to increment reference count
            H5E_CANTDEC        # Unable to decrement reference count
            H5E_NOIDS          # Out of IDs for group

            # Cache related errors
            H5E_CANTFLUSH      # Unable to flush data from cache
            H5E_CANTSERIALIZE  # Unable to serialize data from cache
            H5E_CANTLOAD       # Unable to load metadata into cache
            H5E_PROTECT        # Protected metadata error
            H5E_NOTCACHED      # Metadata not currently cached
            H5E_SYSTEM         # Internal error detected
            H5E_CANTINS        # Unable to insert metadata into cache
            H5E_CANTRENAME     # Unable to rename metadata
            H5E_CANTPROTECT    # Unable to protect metadata
            H5E_CANTUNPROTECT  # Unable to unprotect metadata
            H5E_CANTPIN        # Unable to pin cache entry
            H5E_CANTUNPIN      # Unable to un-pin cache entry
            H5E_CANTMARKDIRTY  # Unable to mark a pinned entry as dirty
            H5E_CANTDIRTY      # Unable to mark metadata as dirty
            H5E_CANTEXPUNGE    # Unable to expunge a metadata cache entry
            H5E_CANTRESIZE     # Unable to resize a metadata cache entry

            # Link related errors
            H5E_TRAVERSE       # Link traversal failure
            H5E_NLINKS         # Too many soft links in path
            H5E_NOTREGISTERED  # Link class not registered
            H5E_CANTMOVE       # Move callback returned error
            H5E_CANTSORT       # Can't sort objects

            # Parallel MPI errors
            H5E_MPI           # Some MPI function failed
            H5E_MPIERRSTR     # MPI Error String
            H5E_CANTRECV      # Can't receive data

            # Dataspace errors
            H5E_CANTCLIP      # Can't clip hyperslab region
            H5E_CANTCOUNT     # Can't count elements
            H5E_CANTSELECT    # Can't select hyperslab
            H5E_CANTNEXT      # Can't move to next iterator location
            H5E_BADSELECT     # Invalid selection
            H5E_CANTCOMPARE   # Can't compare objects

            # Argument errors
            H5E_UNINITIALIZED  # Information is uinitialized
            H5E_UNSUPPORTED    # Feature is unsupported
            H5E_BADTYPE        # Inappropriate type
            H5E_BADRANGE       # Out of range
            H5E_BADVALUE       # Bad value

            # B-tree related errors
            H5E_NOTFOUND            # Object not found
            H5E_EXISTS              # Object already exists
            H5E_CANTENCODE          # Unable to encode value
            H5E_CANTDECODE          # Unable to decode value
            H5E_CANTSPLIT           # Unable to split node
            H5E_CANTREDISTRIBUTE    # Unable to redistribute records
            H5E_CANTSWAP            # Unable to swap records
            H5E_CANTINSERT          # Unable to insert object
            H5E_CANTLIST            # Unable to list node
            H5E_CANTMODIFY          # Unable to modify record
            H5E_CANTREMOVE          # Unable to remove object

            # Datatype conversion errors
            H5E_CANTCONVERT         # Can't convert datatypes
            H5E_BADSIZE             # Bad size for object
    ELSE:
        ctypedef enum H5E_major_t:
            H5E_NONE_MAJOR       = 0,   # special zero, no error
            H5E_ARGS,                   # invalid arguments to routine
            H5E_RESOURCE,               # resource unavailable
            H5E_INTERNAL,               # Internal error (too specific to document)
            H5E_FILE,                   # file Accessibility
            H5E_IO,                     # Low-level I/O
            H5E_FUNC,                   # function Entry/Exit
            H5E_ID,                     # object ID
            H5E_CACHE,                  # object Cache
            H5E_BTREE,                  # B-Tree Node
            H5E_SYM,                    # symbol Table
            H5E_HEAP,                   # Heap
            H5E_OHDR,                   # object Header
            H5E_DATATYPE,               # Datatype
            H5E_DATASPACE,              # Dataspace
            H5E_DATASET,                # Dataset
            H5E_STORAGE,                # data storage
            H5E_PLIST,                  # Property lists
            H5E_ATTR,                   # Attribute
            H5E_PLINE,                  # Data filters
            H5E_EFL,                    # External file list
            H5E_REFERENCE,              # References
            H5E_VFL,                    # Virtual File Layer
            H5E_TST,                    # Ternary Search Trees
            H5E_RS,                     # Reference Counted Strings
            H5E_ERROR,                  # Error API
            H5E_SLIST                   # Skip Lists

        ctypedef enum H5E_minor_t:
            # Generic low-level file I/O errors
            H5E_SEEKERROR      # Seek failed
            H5E_READERROR      # Read failed
            H5E_WRITEERROR     # Write failed
            H5E_CLOSEERROR     # Close failed
            H5E_OVERFLOW       # Address overflowed
            H5E_FCNTL          # File control (fcntl) failed

            # Resource errors
            H5E_NOSPACE        # No space available for allocation
            H5E_CANTALLOC      # Can't allocate space
            H5E_CANTCOPY       # Unable to copy object
            H5E_CANTFREE       # Unable to free object
            H5E_ALREADYEXISTS  # Object already exists
            H5E_CANTLOCK       # Unable to lock object
            H5E_CANTUNLOCK     # Unable to unlock object
            H5E_CANTGC         # Unable to garbage collect
            H5E_CANTGETSIZE    # Unable to compute size
            H5E_OBJOPEN        # Object is already open

            # Heap errors
            H5E_CANTRESTORE    # Can't restore condition
            H5E_CANTCOMPUTE    # Can't compute value
            H5E_CANTEXTEND     # Can't extend heap's space
            H5E_CANTATTACH     # Can't attach object
            H5E_CANTUPDATE     # Can't update object
            H5E_CANTOPERATE    # Can't operate on object

            # Function entry/exit interface errors
            H5E_CANTINIT       # Unable to initialize object
            H5E_ALREADYINIT    # Object already initialized
            H5E_CANTRELEASE    # Unable to release object

            # Property list errors
            H5E_CANTGET        # Can't get value
            H5E_CANTSET        # Can't set value
            H5E_DUPCLASS       # Duplicate class name in parent class

            # Free space errors
            H5E_CANTMERGE      # Can't merge objects
            H5E_CANTREVIVE     # Can't revive object
            H5E_CANTSHRINK     # Can't shrink container

            # Object header related errors
            H5E_LINKCOUNT      # Bad object header link
            H5E_VERSION        # Wrong version number
            H5E_ALIGNMENT      # Alignment error
            H5E_BADMESG        # Unrecognized message
            H5E_CANTDELETE     # Can't delete message
            H5E_BADITER        # Iteration failed
            H5E_CANTPACK       # Can't pack messages
            H5E_CANTRESET      # Can't reset object count

            # System level errors
            H5E_SYSERRSTR      # System error message

            # I/O pipeline errors
            H5E_NOFILTER       # Requested filter is not available
            H5E_CALLBACK       # Callback failed
            H5E_CANAPPLY       # Error from filter 'can apply' callback
            H5E_SETLOCAL       # Error from filter 'set local' callback
            H5E_NOENCODER      # Filter present but encoding disabled
            H5E_CANTFILTER     # Filter operation failed

            # Group related errors
            H5E_CANTOPENOBJ    # Can't open object
            H5E_CANTCLOSEOBJ   # Can't close object
            H5E_COMPLEN        # Name component is too long
            H5E_PATH           # Problem with path to object

            # No error
            H5E_NONE_MINOR     # No error

            # File accessibility errors
            H5E_FILEEXISTS     # File already exists
            H5E_FILEOPEN       # File already open
            H5E_CANTCREATE     # Unable to create file
            H5E_CANTOPENFILE   # Unable to open file
            H5E_CANTCLOSEFILE  # Unable to close file
            H5E_NOTHDF5        # Not an HDF5 file
            H5E_BADFILE        # Bad file ID accessed
            H5E_TRUNCATED      # File has been truncated
            H5E_MOUNT          # File mount error

            # Object ID related errors
            H5E_BADID          # Unable to find ID information (already closed?) */
            H5E_BADGROUP       # Unable to find ID group information
            H5E_CANTREGISTER   # Unable to register new ID
            H5E_CANTINC        # Unable to increment reference count
            H5E_CANTDEC        # Unable to decrement reference count
            H5E_NOIDS          # Out of IDs for group

            # Cache related errors
            H5E_CANTFLUSH      # Unable to flush data from cache
            H5E_CANTSERIALIZE  # Unable to serialize data from cache
            H5E_CANTLOAD       # Unable to load metadata into cache
            H5E_PROTECT        # Protected metadata error
            H5E_NOTCACHED      # Metadata not currently cached
            H5E_SYSTEM         # Internal error detected
            H5E_CANTINS        # Unable to insert metadata into cache
            H5E_CANTRENAME     # Unable to rename metadata
            H5E_CANTPROTECT    # Unable to protect metadata
            H5E_CANTUNPROTECT  # Unable to unprotect metadata
            H5E_CANTPIN        # Unable to pin cache entry
            H5E_CANTUNPIN      # Unable to un-pin cache entry
            H5E_CANTMARKDIRTY  # Unable to mark a pinned entry as dirty
            H5E_CANTDIRTY      # Unable to mark metadata as dirty
            H5E_CANTEXPUNGE    # Unable to expunge a metadata cache entry
            H5E_CANTRESIZE     # Unable to resize a metadata cache entry

            # Link related errors
            H5E_TRAVERSE       # Link traversal failure
            H5E_NLINKS         # Too many soft links in path
            H5E_NOTREGISTERED  # Link class not registered
            H5E_CANTMOVE       # Move callback returned error
            H5E_CANTSORT       # Can't sort objects

            # Parallel MPI errors
            H5E_MPI           # Some MPI function failed
            H5E_MPIERRSTR     # MPI Error String
            H5E_CANTRECV      # Can't receive data

            # Dataspace errors
            H5E_CANTCLIP      # Can't clip hyperslab region
            H5E_CANTCOUNT     # Can't count elements
            H5E_CANTSELECT    # Can't select hyperslab
            H5E_CANTNEXT      # Can't move to next iterator location
            H5E_BADSELECT     # Invalid selection
            H5E_CANTCOMPARE   # Can't compare objects

            # Argument errors
            H5E_UNINITIALIZED  # Information is uinitialized
            H5E_UNSUPPORTED    # Feature is unsupported
            H5E_BADTYPE        # Inappropriate type
            H5E_BADRANGE       # Out of range
            H5E_BADVALUE       # Bad value

            # B-tree related errors
            H5E_NOTFOUND            # Object not found
            H5E_EXISTS              # Object already exists
            H5E_CANTENCODE          # Unable to encode value
            H5E_CANTDECODE          # Unable to decode value
            H5E_CANTSPLIT           # Unable to split node
            H5E_CANTREDISTRIBUTE    # Unable to redistribute records
            H5E_CANTSWAP            # Unable to swap records
            H5E_CANTINSERT          # Unable to insert object
            H5E_CANTLIST            # Unable to list node
            H5E_CANTMODIFY          # Unable to modify record
            H5E_CANTREMOVE          # Unable to remove object

            # Datatype conversion errors
            H5E_CANTCONVERT         # Can't convert datatypes
            H5E_BADSIZE             # Bad size for object

    cdef enum H5E_direction_t:
        H5E_WALK_UPWARD    = 0      # begin deep, end at API function
        H5E_WALK_DOWNWARD = 1       # begin at API function, end deep

    ctypedef struct H5E_error_t:
        H5E_major_t     maj_num     #  major error number
        H5E_minor_t     min_num     #  minor error number
        const char    *func_name          #  function in which error occurred
        const char    *file_name          #  file in which error occurred
        const unsigned    line            #  line in file where error occurs
        const char    *desc               #  optional supplied description

    int H5E_DEFAULT  # ID for default error stack

    char      *H5Eget_major(H5E_major_t n)
    char      *H5Eget_minor(H5E_minor_t n)
    herr_t    H5Eclear(hid_t estack_id) except *

    ctypedef herr_t (*H5E_auto_t)(void *client_data)
    herr_t    H5Eset_auto(hid_t estack_id, H5E_auto_t func, void *client_data) nogil
    herr_t    H5Eget_auto(hid_t estack_id, H5E_auto_t *func, void** client_data)

    herr_t    H5Eprint(hid_t estack_id, void *stream)

    ctypedef herr_t (*H5E_walk_t)(unsigned int n, const H5E_error_t *err_desc, void* client_data)
    herr_t    H5Ewalk(hid_t estack_id, H5E_direction_t direction, H5E_walk_t func, void* client_data)

# --- Functions for managing the HDF5 error callback mechanism ---

ctypedef struct err_cookie:
    # Defines the error handler state (callback and callback data)
    H5E_auto_t func
    void *data

# Set (via H5Eset_auto) the HDF5 error handler for this thread.  Returns
# the old (presently installed) handler.
cdef err_cookie set_error_handler(err_cookie handler)

# Set the default error handler set by silence_errors/unsilence_errors
cdef void set_default_error_handler() noexcept nogil
