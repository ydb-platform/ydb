#------------------------------------------------------------------------------

# From dlpack.h (as of v0.6)

cdef extern from * nogil:
    ctypedef unsigned char      uint8_t
    ctypedef unsigned short     uint16_t
    ctypedef signed   long long int64_t
    ctypedef unsigned long long uint64_t

ctypedef enum DLDeviceType:
    kDLCPU = 1
    kDLCUDA = 2
    kDLCUDAHost = 3
    kDLOpenCL = 4
    kDLVulkan = 7
    kDLMetal = 8
    kDLVPI = 9
    kDLROCM = 10
    kDLROCMHost = 11
    kDLExtDev = 12
    kDLCUDAManaged = 13

ctypedef struct DLDevice:
    DLDeviceType device_type
    int device_id

ctypedef enum DLDataTypeCode:
    kDLInt = 0
    kDLUInt = 1
    kDLFloat = 2
    kDLOpaqueHandle = 3
    kDLBfloat = 4
    kDLComplex = 5

ctypedef struct DLDataType:
    uint8_t code
    uint8_t bits
    uint16_t lanes

ctypedef struct DLTensor:
    void *data
    DLDevice device
    int ndim
    DLDataType dtype
    int64_t *shape
    int64_t *strides
    uint64_t byte_offset

ctypedef struct DLManagedTensor:
    DLTensor dl_tensor
    void *manager_ctx
    void (*deleter)(DLManagedTensor *)

#------------------------------------------------------------------------------

cdef extern from "Python.h":
    void* PyCapsule_GetPointer(object, const char[]) except? NULL
    int PyCapsule_SetName(object, const char[]) except -1
    int PyCapsule_IsValid(object, const char[])

#------------------------------------------------------------------------------

cdef inline int dlpack_is_contig(const DLTensor *dltensor, char order) nogil:
    cdef int i, ndim = dltensor.ndim
    cdef int64_t *shape = dltensor.shape
    cdef int64_t *strides = dltensor.strides
    cdef int64_t start, step, index, dim, size = 1
    if strides == NULL:
        if ndim > 1 and order == c'F':
            return 0
        return 1
    if order == c'F':
        start = 0
        step = 1
    else:
        start = ndim - 1
        step = -1
    for i from 0 <= i < ndim:
        index = start + step * i
        dim = shape[index]
        if dim > 1 and size != strides[index]:
            return 0
        size *= dim
    return 1

cdef inline int dlpack_check_shape(const DLTensor *dltensor) except -1:
    cdef int i, ndim = dltensor.ndim
    if ndim < 0:
        raise BufferError("dlpack: number of dimensions is negative")
    if ndim > 0 and dltensor.shape == NULL:
        raise BufferError("dlpack: shape is NULL")
    for i from 0 <= i < ndim:
        if dltensor.shape[i] < 0:
            raise BufferError("dlpack: shape item is negative")
    if dltensor.strides != NULL:
        for i from 0 <= i < ndim:
            if dltensor.strides[i] < 0:
                raise BufferError("dlpack: strides item is negative")
    return 0

cdef inline int dlpack_check_contig(const DLTensor *dltensor) except -1:
    if dltensor.strides == NULL: return 0
    if dlpack_is_contig(dltensor, c'C'): return 0
    if dlpack_is_contig(dltensor, c'F'): return 0
    raise BufferError("dlpack: buffer is not contiguous")

cdef inline void *dlpack_get_data(const DLTensor *dltensor) nogil:
    return <char*> dltensor.data + dltensor.byte_offset

cdef inline Py_ssize_t dlpack_get_size(const DLTensor *dltensor) nogil:
    cdef int i, ndim = dltensor.ndim
    cdef int64_t *shape = dltensor.shape
    cdef Py_ssize_t bits = dltensor.dtype.bits
    cdef Py_ssize_t lanes = dltensor.dtype.lanes
    cdef Py_ssize_t size = 1
    for i from 0 <= i < ndim:
        size *= <Py_ssize_t> shape[i]
    size *= (bits * lanes + 7) // 8
    return size

cdef inline char *dlpack_get_format(const DLTensor *dltensor) nogil:
    cdef unsigned int code = dltensor.dtype.code
    cdef unsigned int bits = dltensor.dtype.bits
    if dltensor.dtype.lanes != 1:
        if code == kDLFloat and dltensor.dtype.lanes == 2:
            if bits == 8*sizeof(float):       return b"Zf"
            if bits == 8*sizeof(double):      return b"Zd"
            if bits == 8*sizeof(long double): return b"Zg"
        return BYTE_FMT
    if code == kDLInt:
        if bits == 8*sizeof(char):      return b"b"
        if bits == 8*sizeof(short):     return b"h"
        if bits == 8*sizeof(int):       return b"i"
        if bits == 8*sizeof(long):      return b"l"
        if bits == 8*sizeof(long long): return b"q"
    if code == kDLUInt:
        if bits == 8*sizeof(char):      return b"B"
        if bits == 8*sizeof(short):     return b"H"
        if bits == 8*sizeof(int):       return b"I"
        if bits == 8*sizeof(long):      return b"L"
        if bits == 8*sizeof(long long): return b"Q"
    if code == kDLFloat:
        if bits ==  8*sizeof(float)//2:    return b"e"
        if bits ==  8*sizeof(float):       return b"f"
        if bits ==  8*sizeof(double):      return b"d"
        if bits ==  8*sizeof(long double): return b"g"
    if code == kDLComplex:
        if bits ==  8*2*sizeof(float)//2:    return b"Ze"
        if bits ==  8*2*sizeof(float):       return b"Zf"
        if bits ==  8*2*sizeof(double):      return b"Zd"
        if bits ==  8*2*sizeof(long double): return b"Zg"
    return BYTE_FMT

cdef inline Py_ssize_t dlpack_get_itemsize(const DLTensor *dltensor) nogil:
    cdef unsigned int code = dltensor.dtype.code
    cdef unsigned int bits = dltensor.dtype.bits
    if dltensor.dtype.lanes != 1:
        if code == kDLFloat and dltensor.dtype.lanes == 2:
            if (bits == 8*sizeof(float) or
                bits == 8*sizeof(double) or
                bits == 8*sizeof(long double)):
                return <Py_ssize_t> bits // 8 * 2
        return 1
    return <Py_ssize_t> (bits + 7) // 8

#------------------------------------------------------------------------------

cdef int Py_CheckDLPackBuffer(object obj):
    try: return <bint>hasattr(obj, '__dlpack__')
    except: return 0

cdef int Py_GetDLPackBuffer(object obj, Py_buffer *view, int flags) except -1:
    cdef object dlpack
    cdef object dlpack_device
    cdef unsigned device_type
    cdef int device_id
    cdef object capsule
    cdef DLManagedTensor *managed
    cdef const DLTensor *dltensor
    cdef void *buf
    cdef Py_ssize_t size
    cdef bint readonly
    cdef bint fixnull

    try:
        dlpack = obj.__dlpack__
        dlpack_device = obj.__dlpack_device__
    except AttributeError:
        raise NotImplementedError("dlpack: missing support")

    device_type, device_id = dlpack_device()
    if device_type == kDLCPU:
        capsule = dlpack()
    else:
        capsule = dlpack(stream=-1)
    if not PyCapsule_IsValid(capsule, b"dltensor"):
        raise BufferError("dlpack: invalid capsule object")

    managed = <DLManagedTensor*> PyCapsule_GetPointer(capsule, b"dltensor")
    dltensor = &managed.dl_tensor

    try:
        dlpack_check_shape(dltensor)
        dlpack_check_contig(dltensor)

        buf = dlpack_get_data(dltensor)
        size = dlpack_get_size(dltensor)
        readonly = 0

        fixnull = (buf == NULL and size == 0)
        if fixnull: buf = &fixnull
        PyBuffer_FillInfo(view, obj, buf, size, readonly, flags)
        if fixnull: view.buf = NULL

        if (flags & PyBUF_FORMAT) == PyBUF_FORMAT:
            view.format = dlpack_get_format(dltensor)
            if view.format != BYTE_FMT:
                view.itemsize = dlpack_get_itemsize(dltensor)
    finally:
        if managed.deleter != NULL:
            managed.deleter(managed)
        PyCapsule_SetName(capsule, b"used_dltensor")
        del capsule
    return 0

#------------------------------------------------------------------------------
