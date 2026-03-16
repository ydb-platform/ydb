# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2019 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

# This file contains code or comments from the HDF5 library.  See the file
# licenses/hdf5.txt for the full HDF5 software license.

include "config.pxi"

"""
    File driver constants (H5FD*).
"""

# === Multi-file driver =======================================================

MEM_DEFAULT = H5FD_MEM_DEFAULT
MEM_SUPER = H5FD_MEM_SUPER
MEM_BTREE = H5FD_MEM_BTREE
MEM_DRAW = H5FD_MEM_DRAW
MEM_GHEAP = H5FD_MEM_GHEAP
MEM_LHEAP = H5FD_MEM_LHEAP
MEM_OHDR = H5FD_MEM_OHDR
MEM_NTYPES = H5FD_MEM_NTYPES

# === MPI driver ==============================================================

MPIO_INDEPENDENT = H5FD_MPIO_INDEPENDENT
MPIO_COLLECTIVE = H5FD_MPIO_COLLECTIVE

# === Driver types ============================================================

CORE = H5FD_CORE
FAMILY = H5FD_FAMILY
LOG = H5FD_LOG
MPIO = H5FD_MPIO
MPIPOSIX = -1
MULTI = H5FD_MULTI
SEC2 = H5FD_SEC2
DIRECT = H5FD_DIRECT
STDIO = H5FD_STDIO
ROS3D = H5FD_ROS3
IF UNAME_SYSNAME == "Windows":
    WINDOWS = H5FD_WINDOWS
ELSE:
    WINDOWS = -1

# === Logging driver ==========================================================

LOG_LOC_READ  = H5FD_LOG_LOC_READ   # 0x0001
LOG_LOC_WRITE = H5FD_LOG_LOC_WRITE  # 0x0002
LOG_LOC_SEEK  = H5FD_LOG_LOC_SEEK   # 0x0004
LOG_LOC_IO    = H5FD_LOG_LOC_IO     # (H5FD_LOG_LOC_READ|H5FD_LOG_LOC_WRITE|H5FD_LOG_LOC_SEEK)

# Flags for tracking number of times each byte is read/written
LOG_FILE_READ = H5FD_LOG_FILE_READ  # 0x0008
LOG_FILE_WRITE= H5FD_LOG_FILE_WRITE # 0x0010
LOG_FILE_IO   = H5FD_LOG_FILE_IO    # (H5FD_LOG_FILE_READ|H5FD_LOG_FILE_WRITE)

# Flag for tracking "flavor" (type) of information stored at each byte
LOG_FLAVOR    = H5FD_LOG_FLAVOR     # 0x0020

# Flags for tracking total number of reads/writes/seeks
LOG_NUM_READ  = H5FD_LOG_NUM_READ   # 0x0040
LOG_NUM_WRITE = H5FD_LOG_NUM_WRITE  # 0x0080
LOG_NUM_SEEK  = H5FD_LOG_NUM_SEEK   # 0x0100
LOG_NUM_IO    = H5FD_LOG_NUM_IO     # (H5FD_LOG_NUM_READ|H5FD_LOG_NUM_WRITE|H5FD_LOG_NUM_SEEK)

# Flags for tracking time spent in open/read/write/seek/close
LOG_TIME_OPEN = H5FD_LOG_TIME_OPEN  # 0x0200        # Not implemented yet
LOG_TIME_READ = H5FD_LOG_TIME_READ  # 0x0400        # Not implemented yet
LOG_TIME_WRITE= H5FD_LOG_TIME_WRITE # 0x0800        # Partially implemented (need to track total time)
LOG_TIME_SEEK = H5FD_LOG_TIME_SEEK  # 0x1000        # Partially implemented (need to track total time & track time for seeks during reading)
LOG_TIME_CLOSE= H5FD_LOG_TIME_CLOSE # 0x2000        # Fully implemented
LOG_TIME_IO   = H5FD_LOG_TIME_IO    # (H5FD_LOG_TIME_OPEN|H5FD_LOG_TIME_READ|H5FD_LOG_TIME_WRITE|H5FD_LOG_TIME_SEEK|H5FD_LOG_TIME_CLOSE)

# Flag for tracking allocation of space in file
LOG_ALLOC     = H5FD_LOG_ALLOC      # 0x4000
LOG_ALL       = H5FD_LOG_ALL        # (H5FD_LOG_ALLOC|H5FD_LOG_TIME_IO|H5FD_LOG_NUM_IO|H5FD_LOG_FLAVOR|H5FD_LOG_FILE_IO|H5FD_LOG_LOC_IO)


# Implementation of 'fileobj' Virtual File Driver: HDF5 Virtual File
# Layer wrapper over Python file-like object.
# https://support.hdfgroup.org/HDF5/doc1.8/TechNotes/VFL.html

# HDF5 events (read, write, flush, ...) are dispatched via
# H5FD_class_t (struct of callback pointers, H5FD_fileobj_*). This is
# registered as the handler for 'fileobj' driver via H5FDregister.

# File-like object is passed from Python side via FAPL with
# PropFAID.set_fileobj_driver. Then H5FD_fileobj_open callback acts,
# taking file-like object from FAPL and returning struct
# H5FD_fileobj_t (descendant of base H5FD_t) which will hold file
# state. Other callbacks receive H5FD_fileobj_t and operate on
# f.fileobj. If successful, callbacks must return zero; otherwise
# non-zero value.


# H5FD_t of file-like object
ctypedef struct H5FD_fileobj_t:
    H5FD_t base  # must be first
    PyObject* fileobj
    haddr_t eoa


# A minimal subset of callbacks is implemented. Non-essential
# parameters (dxpl, type) are ignored.

from cpython cimport Py_INCREF, Py_DECREF
from libc.stdlib cimport malloc as stdlib_malloc
from libc.stdlib cimport free as stdlib_free
cimport libc.stdio
cimport libc.stdint


cdef void *H5FD_fileobj_fapl_get(H5FD_fileobj_t *f) with gil:
    Py_INCREF(<object>f.fileobj)
    return f.fileobj

cdef void *H5FD_fileobj_fapl_copy(PyObject *old_fa) with gil:
    cdef PyObject *new_fa = old_fa
    Py_INCREF(<object>new_fa)
    return new_fa

cdef herr_t H5FD_fileobj_fapl_free(PyObject *fa) except -1 with gil:
    Py_DECREF(<object>fa)
    return 0

cdef H5FD_fileobj_t *H5FD_fileobj_open(const char *name, unsigned flags, hid_t fapl, haddr_t maxaddr) except * with gil:
    cdef PyObject *fileobj = <PyObject *>H5Pget_driver_info(fapl)
    f = <H5FD_fileobj_t *>stdlib_malloc(sizeof(H5FD_fileobj_t))
    f.fileobj = fileobj
    Py_INCREF(<object>f.fileobj)
    f.eoa = 0
    return f

cdef herr_t H5FD_fileobj_close(H5FD_fileobj_t *f) except -1 with gil:
    Py_DECREF(<object>f.fileobj)
    stdlib_free(f)
    return 0

cdef haddr_t H5FD_fileobj_get_eoa(const H5FD_fileobj_t *f, H5FD_mem_t type) noexcept nogil:
    return f.eoa

cdef herr_t H5FD_fileobj_set_eoa(H5FD_fileobj_t *f, H5FD_mem_t type, haddr_t addr) noexcept nogil:
    f.eoa = addr
    return 0

cdef haddr_t H5FD_fileobj_get_eof(const H5FD_fileobj_t *f, H5FD_mem_t type) except -1 with gil:  # HADDR_UNDEF
    (<object>f.fileobj).seek(0, libc.stdio.SEEK_END)
    return (<object>f.fileobj).tell()

cdef herr_t H5FD_fileobj_read(H5FD_fileobj_t *f, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, void *buf) except -1 with gil:
    cdef unsigned char[:] mview
    (<object>f.fileobj).seek(addr)
    if hasattr(<object>f.fileobj, 'readinto'):
        mview = <unsigned char[:size]>(buf)
        (<object>f.fileobj).readinto(mview)
    else:
        b = (<object>f.fileobj).read(size)
        if len(b) == size:
            memcpy(buf, <unsigned char *>b, size)
        else:
            return 1
    return 0

cdef herr_t H5FD_fileobj_write(H5FD_fileobj_t *f, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, void *buf) except -1 with gil:
    cdef unsigned char[:] mview
    (<object>f.fileobj).seek(addr)
    mview = <unsigned char[:size]>buf
    (<object>f.fileobj).write(mview)
    return 0

cdef herr_t H5FD_fileobj_truncate(H5FD_fileobj_t *f, hid_t dxpl, hbool_t closing) except -1 with gil:
    (<object>f.fileobj).truncate(f.eoa)
    return 0

cdef herr_t H5FD_fileobj_flush(H5FD_fileobj_t *f, hid_t dxpl, hbool_t closing) except -1 with gil:
    # TODO: avoid unneeded fileobj.flush() when closing for e.g. TemporaryFile
    (<object>f.fileobj).flush()
    return 0


# Construct H5FD_class_t struct and register 'fileobj' driver.

cdef H5FD_class_t info
memset(&info, 0, sizeof(info))

# Cython doesn't support "except X" in casting definition currently
ctypedef herr_t (*file_free_func_ptr)(void *) except -1

ctypedef herr_t (*file_close_func_ptr)(H5FD_t *) except -1
ctypedef haddr_t (*file_get_eoa_func_ptr)(const H5FD_t *, H5FD_mem_t) noexcept
ctypedef herr_t (*file_set_eof_func_ptr)(H5FD_t *, H5FD_mem_t, haddr_t) noexcept
ctypedef haddr_t (*file_get_eof_func_ptr)(const H5FD_t *, H5FD_mem_t) except -1
ctypedef herr_t (*file_read_func_ptr)(H5FD_t *, H5FD_mem_t, hid_t, haddr_t, size_t, void*) except -1
ctypedef herr_t (*file_write_func_ptr)(H5FD_t *, H5FD_mem_t, hid_t, haddr_t, size_t, const void*) except -1
ctypedef herr_t (*file_truncate_func_ptr)(H5FD_t *, hid_t, hbool_t) except -1
ctypedef herr_t (*file_flush_func_ptr)(H5FD_t *, hid_t, hbool_t) except -1


info.name = 'fileobj'
info.maxaddr = libc.stdint.SIZE_MAX - 1
info.fc_degree = H5F_CLOSE_WEAK
info.fapl_size = sizeof(PyObject *)
info.fapl_get = <void *(*)(H5FD_t *)>H5FD_fileobj_fapl_get
info.fapl_copy = <void *(*)(const void *)>H5FD_fileobj_fapl_copy

info.fapl_free = <file_free_func_ptr>H5FD_fileobj_fapl_free

info.open = <H5FD_t *(*)(const char *name, unsigned flags, hid_t fapl, haddr_t maxaddr)>H5FD_fileobj_open

info.close = <file_close_func_ptr>H5FD_fileobj_close
info.get_eoa = <file_get_eoa_func_ptr>H5FD_fileobj_get_eoa
info.set_eoa = <file_set_eof_func_ptr>H5FD_fileobj_set_eoa
info.get_eof = <file_get_eof_func_ptr>H5FD_fileobj_get_eof
info.read = <file_read_func_ptr>H5FD_fileobj_read
info.write = <file_write_func_ptr>H5FD_fileobj_write
info.truncate = <file_truncate_func_ptr>H5FD_fileobj_truncate
info.flush = <file_flush_func_ptr>H5FD_fileobj_flush
# H5FD_FLMAP_DICHOTOMY
info.fl_map = [H5FD_MEM_SUPER,  # default
               H5FD_MEM_SUPER,  # super
               H5FD_MEM_SUPER,  # btree
               H5FD_MEM_DRAW,   # draw
               H5FD_MEM_DRAW,   # gheap
               H5FD_MEM_SUPER,  # lheap
               H5FD_MEM_SUPER   # ohdr
	       ]
IF HDF5_VERSION >= (1, 14, 0):
    info.version = H5FD_CLASS_VERSION

fileobj_driver = H5FDregister(&info)
