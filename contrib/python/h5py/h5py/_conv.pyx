# cython: profile=False
# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2019 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Low-level type-conversion routines.
"""
include "config.pxi"

from logging import getLogger

from .h5 import get_config
from .h5r cimport Reference, RegionReference, hobj_ref_t, hdset_reg_ref_t
from .h5t cimport H5PY_OBJ, typewrap, py_create, TypeID, H5PY_PYTHON_OPAQUE_TAG
from libc.stdlib cimport realloc
from libc.string cimport strcmp
from .utils cimport emalloc, efree
from ._proxy cimport needs_bkg_buffer
cfg = get_config()

# Initialization of numpy
cimport numpy as cnp
from numpy cimport npy_intp, NPY_ARRAY_WRITEABLE, NPY_ARRAY_C_CONTIGUOUS, NPY_ARRAY_OWNDATA, PyArray_DATA
cnp._import_array()
import numpy as np

from cpython.buffer cimport (
    PyObject_GetBuffer, PyBuffer_ToContiguous, PyBuffer_Release, PyBUF_INDIRECT
)
from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF, Py_XDECREF, Py_XINCREF

logger = getLogger(__name__)

cdef PyObject* Py_None = <PyObject*> None

cdef extern from "numpy/arrayobject.h":
    void PyArray_ENABLEFLAGS(cnp.ndarray arr, int flags)


ctypedef int (*conv_operator_t)(void* ipt, void* opt, void* bkg, void* priv) except -1
ctypedef herr_t (*init_operator_t)(hid_t src, hid_t dst, void** priv) except -1

# Generic conversion callback
#
# The actual conversion routines are one-liners which plug the appropriate
# operator callback into this function.  This prevents us from having to
# repeat all the conversion boilerplate for every single callback.
#
# While this is somewhat slower than a custom function, the added overhead is
# likely small compared to the cost of the Python-side API calls required to
# implement the conversions.
cdef herr_t generic_converter(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl, conv_operator_t op,
                    init_operator_t initop, H5T_bkg_t need_bkg)  except -1:
    cdef:
        int command
        conv_size_t *sizes
        int i
        char* buf = <char*>buf_i
        char* bkg = <char*>bkg_i

    command = cdata[0].command
    if command == H5T_CONV_INIT:
        cdata[0].need_bkg = need_bkg
        return initop(src_id, dst_id, &(cdata[0].priv))

    elif command == H5T_CONV_FREE:
        efree(cdata[0].priv)
        cdata[0].priv = NULL

    elif command == H5T_CONV_CONV:
        sizes = <conv_size_t*>cdata[0].priv
        if H5Tis_variable_str(src_id):
            sizes.cset = H5Tget_cset(src_id)
        elif H5Tis_variable_str(dst_id):
            sizes.cset = H5Tget_cset(dst_id)
        if bkg_stride==0:
            bkg_stride = sizes[0].dst_size
        if buf_stride == 0:
            # No explicit stride seems to mean that the elements are packed
            # contiguously in the buffer.  In this case we must be careful
            # not to "stomp on" input elements if the output elements are
            # of a larger size.

            if sizes[0].src_size >= sizes[0].dst_size:
                for i in range(nl):
                    op( buf + (i*sizes[0].src_size),    # input pointer
                        buf + (i*sizes[0].dst_size),    # output pointer
                        bkg + (i*bkg_stride),           # backing buffer
                        cdata[0].priv)                  # conversion context
            else:
                for i in range(nl-1, -1, -1):
                    op( buf + (i*sizes[0].src_size),
                        buf + (i*sizes[0].dst_size),
                        bkg + (i*bkg_stride),
                        cdata[0].priv)
        else:
            # With explicit strides, we assume that the library knows the
            # alignment better than us.  Therefore we use the given stride
            # offsets exclusively.
            for i in range(nl):
                op( buf + (i*buf_stride),
                    buf + (i*buf_stride),   # note this is the same!
                    bkg + (i*bkg_stride),
                    cdata[0].priv)
    else:
        return -2   # Unrecognized command.  Note this is NOT an exception.
    return 0

# =============================================================================
# Helper functions

cdef void log_convert_registered(hid_t src, hid_t dst):
    logger.debug("Creating converter from %s to %s", H5Tget_class(src), H5Tget_class(dst))


# =============================================================================
# Generic conversion

ctypedef struct conv_size_t:
    size_t src_size
    size_t dst_size
    int cset

cdef herr_t init_generic(hid_t src, hid_t dst, void** priv) except -1:

    cdef conv_size_t *sizes
    sizes = <conv_size_t*>emalloc(sizeof(conv_size_t))
    priv[0] = sizes
    sizes[0].src_size = H5Tget_size(src)
    sizes[0].dst_size = H5Tget_size(dst)
    log_convert_registered(src, dst)

    return 0

# =============================================================================
# Vlen string conversion

cdef bint _is_pyobject_opaque(hid_t obj):
    # This complexity is needed to sure:
    #   1) That ctag is freed
    #   2) We don't segfault (for some reason a try-finally statement is needed,
    #   even if we do (what I think are) the right steps in copying and freeing.
    cdef char* ctag = NULL
    try:
        if H5Tget_class(obj) == H5T_OPAQUE:
            ctag = H5Tget_tag(obj)
            if ctag != NULL:
                if strcmp(ctag, H5PY_PYTHON_OPAQUE_TAG) == 0:
                    return True
        return False
    finally:
        H5free_memory(ctag)

cdef herr_t init_vlen2str(hid_t src_vlen, hid_t dst_str, void** priv) except -1:
    # /!\ Untested
    cdef conv_size_t *sizes

    if not H5Tis_variable_str(src_vlen):
        return -2

    if not _is_pyobject_opaque(dst_str):
        return -2

    log_convert_registered(src_vlen, dst_str)

    sizes = <conv_size_t*>emalloc(sizeof(conv_size_t))
    priv[0] = sizes

    sizes[0].src_size = H5Tget_size(src_vlen)
    sizes[0].dst_size = H5Tget_size(dst_str)
    return 0

cdef herr_t init_str2vlen(hid_t src_str, hid_t dst_vlen, void** priv) except -1:
    # /!\ untested !
    cdef conv_size_t *sizes

    if not H5Tis_variable_str(dst_vlen):
        return -2

    if not _is_pyobject_opaque(src_str):
        return -2

    log_convert_registered(src_str, dst_vlen)

    sizes = <conv_size_t*>emalloc(sizeof(conv_size_t))
    priv[0] = sizes
    sizes[0].src_size = H5Tget_size(src_str)
    sizes[0].dst_size = H5Tget_size(dst_vlen)

    return 0

cdef int conv_vlen2str(void* ipt, void* opt, void* bkg, void* priv) except -1:
    cdef:
        PyObject** buf_obj = <PyObject**>opt
        char** buf_cstring = <char**>ipt
        PyObject* tmp_object
        bytes tmp_bytes
        conv_size_t *sizes = <conv_size_t*>priv
        char* buf_cstring0

    buf_cstring0 = buf_cstring[0]

    if buf_cstring0 == NULL:
        tmp_bytes =  b""
    else:
        tmp_bytes = buf_cstring0 # Let cython converts char* -> bytes for us
    tmp_object = <PyObject *>tmp_bytes

    # Since all data conversions are by definition in-place, it
    # is our responsibility to free the memory used by the vlens.
    efree(buf_cstring0)

    # Write the new unicode object to the buffer in-place and ensure it is not destroyed
    buf_obj[0] = tmp_object
    Py_XINCREF(tmp_object)
    return 0

cdef int conv_str2vlen(void* ipt, void* opt, void* bkg, void* priv) except -1:
    cdef:
        PyObject** buf_obj = <PyObject**>ipt
        char** buf_cstring = <char**>opt
        conv_size_t* sizes = <conv_size_t*>priv
        char* temp_string = NULL
        size_t temp_string_len = 0  # Not including null term
        PyObject* buf_obj0
        char* buf_cstring0
        object temp_object

    buf_obj0 = buf_obj[0]
    temp_object = <object> buf_obj0

    if isinstance(temp_object, unicode):
        enc = 'utf-8' if (sizes[0].cset == H5T_CSET_UTF8) else 'ascii'
        temp_object = temp_object.encode(enc)

    elif not isinstance(temp_object, bytes):
        raise TypeError("Can't implicitly convert non-string objects to strings")

    # temp_object is bytes
    temp_string = temp_object  # cython cast it as char *
    temp_string_len = len(temp_object)

    if strlen(temp_string) != temp_string_len:
        raise ValueError("VLEN strings do not support embedded NULLs")
    buf_cstring0 = <char*>emalloc(temp_string_len+1)
    memcpy(buf_cstring0, temp_string, temp_string_len+1)
    buf_cstring[0] = buf_cstring0

    return 0

# =============================================================================
# VLEN to fixed-width strings

cdef herr_t init_vlen2fixed(hid_t src, hid_t dst, void** priv) except -1:
    cdef conv_size_t *sizes

    # /!\ Untested

    if not (H5Tis_variable_str(src) and (not H5Tis_variable_str(dst))):
        return -2
    log_convert_registered(src, dst)

    sizes = <conv_size_t*>emalloc(sizeof(conv_size_t))
    priv[0] = sizes

    sizes[0].src_size = H5Tget_size(src)
    sizes[0].dst_size = H5Tget_size(dst)
    return 0

cdef herr_t init_fixed2vlen(hid_t src, hid_t dst, void** priv) except -1:

    cdef conv_size_t *sizes
    if not (H5Tis_variable_str(dst) and (not H5Tis_variable_str(src))):
        return -2
    log_convert_registered(src, dst)

    # /!\ untested !

    sizes = <conv_size_t*>emalloc(sizeof(conv_size_t))
    priv[0] = sizes
    sizes[0].src_size = H5Tget_size(src)
    sizes[0].dst_size = H5Tget_size(dst)

    return 0

cdef int conv_vlen2fixed(void* ipt, void* opt, void* bkg, void* priv) except -1:
    cdef:
        char** buf_vlen = <char**>ipt
        char* buf_fixed = <char*>opt
        char* temp_string = NULL
        size_t temp_string_len = 0  # Without null term
        conv_size_t *sizes = <conv_size_t*>priv
        char* buf_vlen0

    # /!\ untested !

    buf_vlen0 = buf_vlen[0]

    if buf_vlen0 != NULL:
        temp_string = buf_vlen0
        temp_string_len = strlen(temp_string)

        if temp_string_len <= sizes[0].dst_size:
            # Pad with zeros
            memcpy(buf_fixed, temp_string, temp_string_len)
            memset(buf_fixed + temp_string_len, c'\0', sizes[0].dst_size - temp_string_len)
        else:
            # Simply truncate the string
            memcpy(buf_fixed, temp_string, sizes[0].dst_size)
    else:
        memset(buf_fixed, c'\0', sizes[0].dst_size)

    return 0

cdef int conv_fixed2vlen(void* ipt, void* opt, void* bkg, void* priv) except -1:
    cdef:
        char** buf_vlen = <char**>opt
        char* buf_fixed = <char*>ipt
        char* temp_string = NULL
        conv_size_t *sizes = <conv_size_t*>priv

    # /!\ untested !

    temp_string = <char*>emalloc(sizes[0].src_size+1)
    memcpy(temp_string, buf_fixed, sizes[0].src_size)
    temp_string[sizes[0].src_size] = c'\0'

    memcpy(buf_vlen, &temp_string, sizeof(temp_string))

    return 0

# =============================================================================
# HDF5 references to Python instances of h5r.Reference

cdef inline int conv_objref2pyref(void* ipt, void* opt, void* bkg, void* priv) except -1:
    cdef:
        PyObject** buf_obj = <PyObject**>opt
        hobj_ref_t* buf_ref = <hobj_ref_t*>ipt
        Reference ref
        PyObject* ref_ptr = NULL

    ref = Reference()
    ref.ref.obj_ref = buf_ref[0]
    ref.typecode = H5R_OBJECT

    ref_ptr = <PyObject*>ref
    Py_INCREF(ref)  # prevent ref from garbage collection
    buf_obj[0] = ref_ptr

    return 0

cdef inline int conv_pyref2objref(void* ipt, void* opt, void* bkg, void* priv)  except -1:
    cdef:
        PyObject** buf_obj = <PyObject**>ipt
        hobj_ref_t* buf_ref = <hobj_ref_t*>opt
        object obj
        Reference ref
        PyObject* buf_obj0

    buf_obj0 = buf_obj[0]

    if buf_obj0 != NULL and buf_obj0 != Py_None:
        obj = <object>(buf_obj0)
        if not isinstance(obj, Reference):
            raise TypeError("Can't convert incompatible object to HDF5 object reference")
        ref = <Reference>(buf_obj0)
        buf_ref[0] = ref.ref.obj_ref
    else:
        memset(buf_ref, c'\0', sizeof(hobj_ref_t))

    return 0

cdef inline int conv_regref2pyref(void* ipt, void* opt, void* bkg, void* priv) except -1:
    cdef:
        PyObject** buf_obj = <PyObject**>opt
        PyObject** bkg_obj = <PyObject**>bkg
        hdset_reg_ref_t* buf_ref = <hdset_reg_ref_t*>ipt
        RegionReference ref
        PyObject* ref_ptr = NULL
        PyObject* bkg_obj0

    bkg_obj0 = bkg_obj[0]
    ref = RegionReference()
    ref.ref.reg_ref = buf_ref[0]
    ref.typecode = H5R_DATASET_REGION
    ref_ptr = <PyObject*>ref
    Py_INCREF(ref)  # because Cython discards its reference when the
                        # function exits

    Py_XDECREF(bkg_obj0)
    buf_obj[0] = ref_ptr

    return 0

cdef inline int conv_pyref2regref(void* ipt, void* opt, void* bkg, void* priv) except -1:
    cdef:
        PyObject** buf_obj = <PyObject**>ipt
        hdset_reg_ref_t* buf_ref = <hdset_reg_ref_t*>opt
        object obj
        RegionReference ref
        PyObject* buf_obj0

    buf_obj0 = buf_obj[0]

    if buf_obj0 != NULL and buf_obj0 != Py_None:
        obj = <object>(buf_obj0)
        if not isinstance(obj, RegionReference):
            raise TypeError("Can't convert incompatible object to HDF5 region reference")
        ref = <RegionReference>(buf_obj0)
        IF HDF5_VERSION >= (1, 12, 0):
            memcpy(buf_ref, ref.ref.reg_ref.data, sizeof(hdset_reg_ref_t))
        ELSE:
            memcpy(buf_ref, ref.ref.reg_ref, sizeof(hdset_reg_ref_t))
    else:
        memset(buf_ref, c'\0', sizeof(hdset_reg_ref_t))

    return 0

# =============================================================================
# Conversion functions


cdef inline herr_t vlen2str(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl,  conv_vlen2str, init_vlen2str, H5T_BKG_YES)

cdef inline herr_t str2vlen(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl)except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, conv_str2vlen, init_str2vlen, H5T_BKG_NO)

cdef inline herr_t vlen2fixed(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, conv_vlen2fixed, init_vlen2fixed, H5T_BKG_NO)

cdef inline herr_t fixed2vlen(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, conv_fixed2vlen, init_fixed2vlen, H5T_BKG_NO)

cdef inline herr_t objref2pyref(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, conv_objref2pyref, init_generic, H5T_BKG_NO)

cdef inline herr_t pyref2objref(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, conv_pyref2objref, init_generic, H5T_BKG_NO)

cdef inline herr_t regref2pyref(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, conv_regref2pyref, init_generic, H5T_BKG_YES)

cdef inline herr_t pyref2regref(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return generic_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, conv_pyref2regref, init_generic, H5T_BKG_NO)

# =============================================================================
# Enum to integer converter

cdef struct conv_enum_t:
    size_t src_size
    size_t dst_size

cdef int enum_int_converter_init(hid_t src, hid_t dst,
                                 H5T_cdata_t *cdata, int forward) except -1:
    cdef conv_enum_t *info

    cdata[0].need_bkg = H5T_BKG_NO
    cdata[0].priv = info = <conv_enum_t*>emalloc(sizeof(conv_enum_t))
    info[0].src_size = H5Tget_size(src)
    info[0].dst_size = H5Tget_size(dst)

cdef void enum_int_converter_free(H5T_cdata_t *cdata):
    cdef conv_enum_t *info

    info = <conv_enum_t*>cdata[0].priv
    efree(info)
    cdata[0].priv = NULL


cdef int enum_int_converter_conv(hid_t src, hid_t dst, H5T_cdata_t *cdata,
                                  size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                                 void *bkg_i, hid_t dxpl, int forward) except -1:
    cdef:
        conv_enum_t *info
        size_t nalloc
        int i
        char* cbuf = NULL
        char* buf = <char*>buf_i
        int identical
        hid_t supertype = -1

    info = <conv_enum_t*>cdata[0].priv

    try:
        if forward:
            supertype = H5Tget_super(src)
            identical = H5Tequal(supertype, dst)
        else:
            supertype = H5Tget_super(dst)
            identical = H5Tequal(supertype, src)

        # Short-circuit success
        if identical:
            return 0

        if buf_stride == 0:
            # Contiguous case: call H5Tconvert directly
            if forward:
                H5Tconvert(supertype, dst, nl, buf, NULL, dxpl)
            else:
                H5Tconvert(src, supertype, nl, buf, NULL, dxpl)
        else:
            # Non-contiguous: gather, convert and then scatter
            if info[0].src_size > info[0].dst_size:
                nalloc = info[0].src_size*nl
            else:
                nalloc = info[0].dst_size*nl

            cbuf = <char*>emalloc(nalloc)
            if cbuf == NULL:
                raise MemoryError()

            for i in range(nl):
                memcpy(cbuf + (i*info[0].src_size), buf + (i*buf_stride),
                        info[0].src_size)

            if forward:
                H5Tconvert(supertype, dst, nl, cbuf, NULL, dxpl)
            else:
                H5Tconvert(src, supertype, nl, cbuf, NULL, dxpl)

            for i in range(nl):
                memcpy(buf + (i*buf_stride), cbuf + (i*info[0].dst_size),
                        info[0].dst_size)

    finally:
        efree(cbuf)
        cbuf = NULL
        if supertype > 0:
            H5Tclose(supertype)

    return 0


# Direction ("forward"): 1 = enum to int, 0 = int to enum
cdef herr_t enum_int_converter(hid_t src, hid_t dst, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                               void *bkg_i, hid_t dxpl, int forward) except -1:

    cdef int command = cdata[0].command

    if command == H5T_CONV_INIT:
        enum_int_converter_init(src, dst, cdata, forward)
    elif command == H5T_CONV_FREE:
        enum_int_converter_free(cdata)
    elif command == H5T_CONV_CONV:
        return enum_int_converter_conv(src, dst, cdata, nl, buf_stride,
                                       bkg_stride, buf_i, bkg_i, dxpl, forward)
    else:
        return -2

    return 0


cdef herr_t enum2int(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return enum_int_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, 1)

cdef herr_t int2enum(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                    size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                    void *bkg_i, hid_t dxpl) except -1 with gil:
    return enum_int_converter(src_id, dst_id, cdata, nl, buf_stride, bkg_stride,
             buf_i, bkg_i, dxpl, 0)

# =============================================================================
# ndarray to VLEN routines

cdef herr_t vlen2ndarray(hid_t src_id,
                         hid_t dst_id,
                         H5T_cdata_t *cdata,
                         size_t nl,
                         size_t buf_stride,
                         size_t bkg_stride,
                         void *buf_i,
                         void *bkg_i,
                         hid_t dxpl) except -1 with gil:
    """Convert variable length object to numpy array, typically a list of strings

    :param src_id: Identifier for the source datatype.
    :param dst_id: Identifier for the destination datatype.
    :param nl: number of element
    :param buf_stride: Array containing pre- and post-conversion values.
    :param bkg_stride: Optional background buffer
    :param dxpl: Dataset transfer property list identifier.
    :return: error-code
    """
    cdef:
        int command = cdata[0].command
        size_t src_size, dst_size
        TypeID supertype
        TypeID outtype
        cnp.dtype dt
        int i
        char* buf = <char*>buf_i

    if command == H5T_CONV_INIT:
        cdata[0].need_bkg = H5T_BKG_NO
        if H5Tget_class(src_id) != H5T_VLEN or H5Tget_class(dst_id) != H5T_OPAQUE:
            return -2

    elif command == H5T_CONV_FREE:
        pass

    elif command == H5T_CONV_CONV:
        # need to pass element dtype to converter
        supertype = typewrap(H5Tget_super(src_id))
        dt = supertype.dtype
        outtype = py_create(dt)

        if buf_stride == 0:
            # No explicit stride seems to mean that the elements are packed
            # contiguously in the buffer.  In this case we must be careful
            # not to "stomp on" input elements if the output elements are
            # of a larger size.

            src_size = H5Tget_size(src_id)
            dst_size = H5Tget_size(dst_id)

            if src_size >= dst_size:
                for i in range(nl):
                    conv_vlen2ndarray(buf + (i*src_size), buf + (i*dst_size),
                                      dt, supertype, outtype)
            else:
                for i in range(nl-1, -1, -1):
                    conv_vlen2ndarray(buf + (i*src_size), buf + (i*dst_size),
                                      dt, supertype, outtype)
        else:
            # With explicit strides, we assume that the library knows the
            # alignment better than us.  Therefore we use the given stride
            # offsets exclusively.
            for i in range(nl):
                conv_vlen2ndarray(buf + (i*buf_stride), buf + (i*buf_stride),
                                  dt, supertype, outtype)

    else:
        return -2   # Unrecognized command.  Note this is NOT an exception.

    return 0


cdef struct vlen_t:
    size_t len
    void* ptr

cdef int conv_vlen2ndarray(void* ipt,
                           void* opt,
                           cnp.dtype elem_dtype,
                           TypeID intype,
                           TypeID outtype) except -1:
    """Convert variable length strings to numpy array

    :param ipt: input pointer: Point to the input data
    :param opt: output pointer: will contains the numpy array after exit
    :param elem_dtype: dtype of the element
    :param intype: ?
    :param outtype: ?
    """
    cdef:
        PyObject** buf_obj = <PyObject**>opt
        vlen_t* in_vlen = <vlen_t*>ipt
        int flags = NPY_ARRAY_WRITEABLE | NPY_ARRAY_C_CONTIGUOUS | NPY_ARRAY_OWNDATA
        npy_intp dims[1]
        void* data
        char[:] buf
        void* back_buf = NULL
        cnp.ndarray ndarray
        PyObject* ndarray_obj
        vlen_t in_vlen0
        size_t size, itemsize

    #Replaces the memcpy
    size = in_vlen0.len = in_vlen[0].len
    data = in_vlen0.ptr = in_vlen[0].ptr

    dims[0] = size
    itemsize = H5Tget_size(outtype.id)
    if itemsize > H5Tget_size(intype.id):
        data = realloc(data, itemsize * size)

    if needs_bkg_buffer(intype.id, outtype.id):
        back_buf = emalloc(H5Tget_size(outtype.id)*size)

    try:
        H5Tconvert(intype.id, outtype.id, size, data, back_buf, H5P_DEFAULT)
    finally:
        free(back_buf)

    # We need to use different approaches to creating the ndarray with the converted
    # data depending on the destination dtype.
    # For simple dtypes, we can use SimpleNewFromData, but types like
    # string & void need a size specified, so this function can't be used.
    # Additionally, Cython doesn't expose NumPy C-API functions like NewFromDescr,
    # so we fall back on copying directly to the underlying buffer
    # of a new ndarray for other types.

    if elem_dtype.kind in b"biufcmMO":
        # type_num is enough to create an array for these dtypes
        ndarray = cnp.PyArray_SimpleNewFromData(1, dims, elem_dtype.type_num, data)
    elif not elem_dtype.hasobject:
        # This covers things like string dtypes and simple compound dtypes,
        # which can't be used with SimpleNewFromData.
        # Cython doesn't expose NumPy C-API functions
        # like NewFromDescr, so we'll construct this with a Python function.
        buf = <char[:itemsize * size]> data
        ndarray = np.frombuffer(buf, dtype=elem_dtype)
    else:
        # Compound dtypes containing object fields: frombuffer() refuses these,
        # so we'll fall back to allocating a new array and copying the data in.
        ndarray = np.empty(size, dtype=elem_dtype)
        memcpy(PyArray_DATA(ndarray), data, itemsize * size)

        # In this code path, `data`, allocated by hdf5 to hold the v-len data,
        # will no longer be used since have copied its contents to the ndarray.
        efree(data)

    PyArray_ENABLEFLAGS(ndarray, flags)
    ndarray_obj = <PyObject*>ndarray

    in_vlen0.ptr = NULL

    # Write the new ndarray object to the buffer in-place and ensure it is not destroyed
    buf_obj[0] = ndarray_obj
    Py_INCREF(ndarray)
    Py_INCREF(elem_dtype)
    return 0

cdef herr_t ndarray2vlen(hid_t src_id,
                         hid_t dst_id,
                         H5T_cdata_t *cdata,
                         size_t nl,
                         size_t buf_stride,
                         size_t bkg_stride,
                         void *buf_i,
                         void *bkg_i,
                         hid_t dxpl) except -1 with gil:
    cdef:
        int command = cdata[0].command
        size_t src_size, dst_size
        TypeID supertype
        TypeID outtype
        int i
        PyObject **pdata = <PyObject **> buf_i
        PyObject *pdata_elem
        char* buf = <char*>buf_i

    if command == H5T_CONV_INIT:
        cdata[0].need_bkg = H5T_BKG_NO
        if not H5Tequal(src_id, H5PY_OBJ) or H5Tget_class(dst_id) != H5T_VLEN:
            return -2
        supertype = typewrap(H5Tget_super(dst_id))
        for i in range(nl):
            # smells a lot
            memcpy(&pdata_elem, pdata+i, sizeof(pdata_elem))
            if supertype != py_create((<cnp.ndarray> pdata_elem).dtype, 1):
                return -2
            if (<cnp.ndarray> pdata_elem).ndim != 1:
                return -2
        log_convert_registered(src_id, dst_id)

    elif command == H5T_CONV_FREE:
        pass

    elif command == H5T_CONV_CONV:
        # If there are no elements to convert, pdata will not point to
        # a valid PyObject*, so bail here to prevent accessing the dtype below
        if nl == 0:
            return 0

        # need to pass element dtype to converter
        pdata_elem = pdata[0]
        supertype = py_create((<cnp.ndarray> pdata_elem).dtype)
        outtype = typewrap(H5Tget_super(dst_id))

        if buf_stride == 0:
            # No explicit stride seems to mean that the elements are packed
            # contiguously in the buffer.  In this case we must be careful
            # not to "stomp on" input elements if the output elements are
            # of a larger size.

            src_size = H5Tget_size(src_id)
            dst_size = H5Tget_size(dst_id)

            if src_size >= dst_size:
                for i in range(nl):
                    conv_ndarray2vlen(buf + (i*src_size), buf + (i*dst_size),
                                      supertype, outtype)
            else:
                for i in range(nl-1, -1, -1):
                    conv_ndarray2vlen(buf + (i*src_size), buf + (i*dst_size),
                                      supertype, outtype)
        else:
            # With explicit strides, we assume that the library knows the
            # alignment better than us.  Therefore we use the given stride
            # offsets exclusively.
            for i in range(nl):
                conv_ndarray2vlen(buf + (i*buf_stride), buf + (i*buf_stride),
                                  supertype, outtype)

    else:
        return -2   # Unrecognized command.  Note this is NOT an exception.

    return 0


cdef int conv_ndarray2vlen(void* ipt,
                           void* opt,
                           TypeID intype,
                           TypeID outtype) except -1:
    cdef:
        PyObject** buf_obj = <PyObject**>ipt
        vlen_t* in_vlen = <vlen_t*>opt
        void* data
        cnp.ndarray ndarray
        size_t len, nbytes
        PyObject* buf_obj0
        Py_buffer view
        void* back_buf = NULL
    try:
        buf_obj0 = buf_obj[0]
        ndarray = <cnp.ndarray> buf_obj0
        len = ndarray.shape[0]
        nbytes = len * max(H5Tget_size(outtype.id), H5Tget_size(intype.id))

        data = emalloc(nbytes)

        PyObject_GetBuffer(ndarray, &view, PyBUF_INDIRECT)
        PyBuffer_ToContiguous(data, &view, view.len, b'C')
        PyBuffer_Release(&view)

        if needs_bkg_buffer(intype.id, outtype.id):
            back_buf = emalloc(H5Tget_size(outtype.id)*len)

        H5Tconvert(intype.id, outtype.id, len, data, back_buf, H5P_DEFAULT)

        in_vlen[0].len = len
        in_vlen[0].ptr = data

    finally:
        free(back_buf)

    return 0

# =============================================================================
# B8 to enum bool routines

cdef herr_t b82boolenum(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                        size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                        void *bkg_i, hid_t dxpl) except -1:
    return 0

cdef herr_t boolenum2b8(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                        size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                        void *bkg_i, hid_t dxpl) except -1:
    return 0

# =============================================================================
# BITFIELD to UINT routines

cdef herr_t bitfield2uint(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                     size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                     void *bkg_i, hid_t dxpl) except -1:
    return 0

cdef herr_t uint2bitfield(hid_t src_id, hid_t dst_id, H5T_cdata_t *cdata,
                     size_t nl, size_t buf_stride, size_t bkg_stride, void *buf_i,
                     void *bkg_i, hid_t dxpl) except -1:
    return 0

# =============================================================================

cpdef int register_converters() except -1:
    cdef:
        hid_t vlstring
        hid_t vlentype
        hid_t pyobj
        hid_t enum
        hid_t boolenum = -1
        int8_t f_value = 0
        int8_t t_value = 1

    vlstring = H5Tcopy(H5T_C_S1)
    H5Tset_size(vlstring, H5T_VARIABLE)

    enum = H5Tenum_create(H5T_STD_I32LE)

    vlentype = H5Tvlen_create(H5T_STD_I32LE)

    pyobj = H5PY_OBJ

    boolenum = H5Tenum_create(H5T_NATIVE_INT8)
    H5Tenum_insert(boolenum, cfg._f_name, &f_value)
    H5Tenum_insert(boolenum, cfg._t_name, &t_value)

    H5Tregister(H5T_PERS_SOFT, "vlen2fixed", vlstring, H5T_C_S1, vlen2fixed)
    H5Tregister(H5T_PERS_SOFT, "fixed2vlen", H5T_C_S1, vlstring, fixed2vlen)

    H5Tregister(H5T_PERS_HARD, "objref2pyref", H5T_STD_REF_OBJ, pyobj, objref2pyref)
    H5Tregister(H5T_PERS_HARD, "pyref2objref", pyobj, H5T_STD_REF_OBJ, pyref2objref)

    H5Tregister(H5T_PERS_HARD, "regref2pyref", H5T_STD_REF_DSETREG, pyobj, regref2pyref)
    H5Tregister(H5T_PERS_HARD, "pyref2regref", pyobj, H5T_STD_REF_DSETREG, pyref2regref)

    H5Tregister(H5T_PERS_SOFT, "enum2int", enum, H5T_STD_I32LE, enum2int)
    H5Tregister(H5T_PERS_SOFT, "int2enum", H5T_STD_I32LE, enum, int2enum)

    H5Tregister(H5T_PERS_SOFT, "vlen2ndarray", vlentype, pyobj, vlen2ndarray)
    H5Tregister(H5T_PERS_SOFT, "ndarray2vlen", pyobj, vlentype, ndarray2vlen)

    H5Tregister(H5T_PERS_HARD, "boolenum2b8", boolenum, H5T_NATIVE_B8, boolenum2b8)
    H5Tregister(H5T_PERS_HARD, "b82boolenum", H5T_NATIVE_B8, boolenum, b82boolenum)

    H5Tregister(H5T_PERS_HARD, "uint82b8", H5T_STD_U8BE, H5T_STD_B8BE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b82uint8", H5T_STD_B8BE, H5T_STD_U8BE, bitfield2uint)

    H5Tregister(H5T_PERS_HARD, "uint82b8", H5T_STD_U8LE, H5T_STD_B8LE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b82uint8", H5T_STD_B8LE, H5T_STD_U8LE, bitfield2uint)

    H5Tregister(H5T_PERS_HARD, "uint162b16", H5T_STD_U16BE, H5T_STD_B16BE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b162uint16", H5T_STD_B16BE, H5T_STD_U16BE, bitfield2uint)

    H5Tregister(H5T_PERS_HARD, "uint162b16", H5T_STD_U16LE, H5T_STD_B16LE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b162uint16", H5T_STD_B16LE, H5T_STD_U16LE, bitfield2uint)

    H5Tregister(H5T_PERS_HARD, "uint322b32", H5T_STD_U32BE, H5T_STD_B32BE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b322uint32", H5T_STD_B32BE, H5T_STD_U32BE, bitfield2uint)

    H5Tregister(H5T_PERS_HARD, "uint322b32", H5T_STD_U32LE, H5T_STD_B32LE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b322uint32", H5T_STD_B32LE, H5T_STD_U32LE, bitfield2uint)

    H5Tregister(H5T_PERS_HARD, "uint642b64", H5T_STD_U64BE, H5T_STD_B64BE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b642uint64", H5T_STD_B64BE, H5T_STD_U64BE, bitfield2uint)

    H5Tregister(H5T_PERS_HARD, "uint642b64", H5T_STD_U64LE, H5T_STD_B64LE, uint2bitfield)
    H5Tregister(H5T_PERS_HARD, "b642uint64", H5T_STD_B64LE, H5T_STD_U64LE, bitfield2uint)

    H5Tregister(H5T_PERS_SOFT, "vlen2str", vlstring, pyobj, vlen2str)
    H5Tregister(H5T_PERS_SOFT, "str2vlen", pyobj, vlstring, str2vlen)

    H5Tclose(vlstring)
    H5Tclose(vlentype)
    H5Tclose(enum)
    H5Tclose(boolenum)

    return 0

cpdef int unregister_converters() except -1:

    H5Tunregister(H5T_PERS_SOFT, "vlen2str", -1, -1, vlen2str)
    H5Tunregister(H5T_PERS_SOFT, "str2vlen", -1, -1, str2vlen)

    H5Tunregister(H5T_PERS_SOFT, "vlen2fixed", -1, -1, vlen2fixed)
    H5Tunregister(H5T_PERS_SOFT, "fixed2vlen", -1, -1, fixed2vlen)

    H5Tunregister(H5T_PERS_HARD, "objref2pyref", -1, -1, objref2pyref)
    H5Tunregister(H5T_PERS_HARD, "pyref2objref", -1, -1, pyref2objref)

    H5Tunregister(H5T_PERS_HARD, "regref2pyref", -1, -1, regref2pyref)
    H5Tunregister(H5T_PERS_HARD, "pyref2regref", -1, -1, pyref2regref)

    H5Tunregister(H5T_PERS_SOFT, "enum2int", -1, -1, enum2int)
    H5Tunregister(H5T_PERS_SOFT, "int2enum", -1, -1, int2enum)

    H5Tunregister(H5T_PERS_SOFT, "vlen2ndarray", -1, -1, vlen2ndarray)
    H5Tunregister(H5T_PERS_SOFT, "ndarray2vlen", -1, -1, ndarray2vlen)

    H5Tunregister(H5T_PERS_HARD, "boolenum2b8", -1, -1, boolenum2b8)
    H5Tunregister(H5T_PERS_HARD, "b82boolenum", -1, -1, b82boolenum)

    # Pass an empty string to unregister all methods that use these functions
    H5Tunregister(H5T_PERS_HARD, "", -1, -1, uint2bitfield)
    H5Tunregister(H5T_PERS_HARD, "", -1, -1, bitfield2uint)

    return 0
