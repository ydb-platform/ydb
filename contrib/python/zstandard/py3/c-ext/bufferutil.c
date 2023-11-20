/**
 * Copyright (c) 2017-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

static void BufferWithSegments_dealloc(ZstdBufferWithSegments *self) {
    /* Backing memory is either canonically owned by a Py_buffer or by us. */
    if (self->parent.buf) {
        PyBuffer_Release(&self->parent);
    }
    else if (self->useFree) {
        free(self->data);
    }
    else {
        PyMem_Free(self->data);
    }

    self->data = NULL;

    if (self->useFree) {
        free(self->segments);
    }
    else {
        PyMem_Free(self->segments);
    }

    self->segments = NULL;

    PyObject_Del(self);
}

static int BufferWithSegments_init(ZstdBufferWithSegments *self, PyObject *args,
                                   PyObject *kwargs) {
    static char *kwlist[] = {"data", "segments", NULL};

    Py_buffer segments;
    Py_ssize_t segmentCount;
    Py_ssize_t i;

    memset(&self->parent, 0, sizeof(self->parent));

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*y*:BufferWithSegments",
                                     kwlist, &self->parent, &segments)) {
        return -1;
    }

    if (segments.len % sizeof(BufferSegment)) {
        PyErr_Format(PyExc_ValueError,
                     "segments array size is not a multiple of %zu",
                     sizeof(BufferSegment));
        goto except;
    }

    segmentCount = segments.len / sizeof(BufferSegment);

    /* Validate segments data, as blindly trusting it could lead to arbitrary
    memory access. */
    for (i = 0; i < segmentCount; i++) {
        BufferSegment *segment = &((BufferSegment *)(segments.buf))[i];

        if (segment->offset + segment->length >
            (unsigned long long)self->parent.len) {
            PyErr_SetString(PyExc_ValueError,
                            "offset within segments array references memory "
                            "outside buffer");
            goto except;
            return -1;
        }
    }

    /* Make a copy of the segments data. It is cheap to do so and is a guard
       against caller changing offsets, which has security implications. */
    self->segments = PyMem_Malloc(segments.len);
    if (!self->segments) {
        PyErr_NoMemory();
        goto except;
    }

    memcpy(self->segments, segments.buf, segments.len);
    PyBuffer_Release(&segments);

    self->data = self->parent.buf;
    self->dataSize = self->parent.len;
    self->segmentCount = segmentCount;

    return 0;

except:
    PyBuffer_Release(&self->parent);
    PyBuffer_Release(&segments);
    return -1;
}

/**
 * Construct a BufferWithSegments from existing memory and offsets.
 *
 * Ownership of the backing memory and BufferSegments will be transferred to
 * the created object and freed when the BufferWithSegments is destroyed.
 */
ZstdBufferWithSegments *
BufferWithSegments_FromMemory(void *data, unsigned long long dataSize,
                              BufferSegment *segments,
                              Py_ssize_t segmentsSize) {
    ZstdBufferWithSegments *result = NULL;
    Py_ssize_t i;

    if (NULL == data) {
        PyErr_SetString(PyExc_ValueError, "data is NULL");
        return NULL;
    }

    if (NULL == segments) {
        PyErr_SetString(PyExc_ValueError, "segments is NULL");
        return NULL;
    }

    for (i = 0; i < segmentsSize; i++) {
        BufferSegment *segment = &segments[i];

        if (segment->offset + segment->length > dataSize) {
            PyErr_SetString(PyExc_ValueError,
                            "offset in segments overflows buffer size");
            return NULL;
        }
    }

    result = PyObject_New(ZstdBufferWithSegments, ZstdBufferWithSegmentsType);
    if (NULL == result) {
        return NULL;
    }

    result->useFree = 0;

    memset(&result->parent, 0, sizeof(result->parent));
    result->data = data;
    result->dataSize = dataSize;
    result->segments = segments;
    result->segmentCount = segmentsSize;

    return result;
}

static Py_ssize_t BufferWithSegments_length(ZstdBufferWithSegments *self) {
    return self->segmentCount;
}

static ZstdBufferSegment *BufferWithSegments_item(ZstdBufferWithSegments *self,
                                                  Py_ssize_t i) {
    ZstdBufferSegment *result = NULL;

    if (i < 0) {
        PyErr_SetString(PyExc_IndexError, "offset must be non-negative");
        return NULL;
    }

    if (i >= self->segmentCount) {
        PyErr_Format(PyExc_IndexError, "offset must be less than %zd",
                     self->segmentCount);
        return NULL;
    }

    if (self->segments[i].length > PY_SSIZE_T_MAX) {
        PyErr_Format(PyExc_ValueError,
                     "item at offset %zd is too large for this platform", i);
        return NULL;
    }

    result = (ZstdBufferSegment *)PyObject_CallObject(
        (PyObject *)ZstdBufferSegmentType, NULL);
    if (NULL == result) {
        return NULL;
    }

    result->parent = (PyObject *)self;
    Py_INCREF(self);

    result->data = (char *)self->data + self->segments[i].offset;
    result->dataSize = (Py_ssize_t)self->segments[i].length;
    result->offset = self->segments[i].offset;

    return result;
}

static int BufferWithSegments_getbuffer(ZstdBufferWithSegments *self,
                                        Py_buffer *view, int flags) {
    if (self->dataSize > PY_SSIZE_T_MAX) {
        view->obj = NULL;
        PyErr_SetString(PyExc_BufferError,
                        "buffer is too large for this platform");
        return -1;
    }

    return PyBuffer_FillInfo(view, (PyObject *)self, self->data,
                             (Py_ssize_t)self->dataSize, 1, flags);
}

static PyObject *BufferWithSegments_tobytes(ZstdBufferWithSegments *self) {
    if (self->dataSize > PY_SSIZE_T_MAX) {
        PyErr_SetString(PyExc_ValueError,
                        "buffer is too large for this platform");
        return NULL;
    }

    return PyBytes_FromStringAndSize(self->data, (Py_ssize_t)self->dataSize);
}

static ZstdBufferSegments *
BufferWithSegments_segments(ZstdBufferWithSegments *self) {
    ZstdBufferSegments *result = (ZstdBufferSegments *)PyObject_CallObject(
        (PyObject *)ZstdBufferSegmentsType, NULL);
    if (NULL == result) {
        return NULL;
    }

    result->parent = (PyObject *)self;
    Py_INCREF(self);
    result->segments = self->segments;
    result->segmentCount = self->segmentCount;

    return result;
}

#if PY_VERSION_HEX < 0x03090000
static PyBufferProcs BufferWithSegments_as_buffer = {
    (getbufferproc)BufferWithSegments_getbuffer, /* bf_getbuffer */
    0                                            /* bf_releasebuffer */
};
#endif

static PyMethodDef BufferWithSegments_methods[] = {
    {"segments", (PyCFunction)BufferWithSegments_segments, METH_NOARGS, NULL},
    {"tobytes", (PyCFunction)BufferWithSegments_tobytes, METH_NOARGS, NULL},
    {NULL, NULL}};

static PyMemberDef BufferWithSegments_members[] = {
    {"size", T_ULONGLONG, offsetof(ZstdBufferWithSegments, dataSize), READONLY,
     "total size of the buffer in bytes"},
    {NULL}};

PyType_Slot ZstdBufferWithSegmentsSlots[] = {
    {Py_tp_dealloc, BufferWithSegments_dealloc},
    {Py_sq_length, BufferWithSegments_length},
    {Py_sq_item, BufferWithSegments_item},
#if PY_VERSION_HEX >= 0x03090000
    {Py_bf_getbuffer, BufferWithSegments_getbuffer},
#endif
    {Py_tp_methods, BufferWithSegments_methods},
    {Py_tp_members, BufferWithSegments_members},
    {Py_tp_init, BufferWithSegments_init},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdBufferWithSegmentsSpec = {
    "zstd.BufferWithSegments",
    sizeof(ZstdBufferWithSegments),
    0,
    Py_TPFLAGS_DEFAULT,
    ZstdBufferWithSegmentsSlots,
};

PyTypeObject *ZstdBufferWithSegmentsType;

static void BufferSegments_dealloc(ZstdBufferSegments *self) {
    Py_CLEAR(self->parent);
    PyObject_Del(self);
}

static int BufferSegments_getbuffer(ZstdBufferSegments *self, Py_buffer *view,
                                    int flags) {
    return PyBuffer_FillInfo(view, (PyObject *)self, (void *)self->segments,
                             self->segmentCount * sizeof(BufferSegment), 1,
                             flags);
}

PyType_Slot ZstdBufferSegmentsSlots[] = {
    {Py_tp_dealloc, BufferSegments_dealloc},
#if PY_VERSION_HEX >= 0x03090000
    {Py_bf_getbuffer, BufferSegments_getbuffer},
#endif
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdBufferSegmentsSpec = {
    "zstd.BufferSegments",
    sizeof(ZstdBufferSegments),
    0,
    Py_TPFLAGS_DEFAULT,
    ZstdBufferSegmentsSlots,
};

#if PY_VERSION_HEX < 0x03090000
static PyBufferProcs BufferSegments_as_buffer = {
    (getbufferproc)BufferSegments_getbuffer, 0};
#endif

PyTypeObject *ZstdBufferSegmentsType;

static void BufferSegment_dealloc(ZstdBufferSegment *self) {
    Py_CLEAR(self->parent);
    PyObject_Del(self);
}

static Py_ssize_t BufferSegment_length(ZstdBufferSegment *self) {
    return self->dataSize;
}

static int BufferSegment_getbuffer(ZstdBufferSegment *self, Py_buffer *view,
                                   int flags) {
    return PyBuffer_FillInfo(view, (PyObject *)self, self->data, self->dataSize,
                             1, flags);
}

static PyObject *BufferSegment_tobytes(ZstdBufferSegment *self) {
    return PyBytes_FromStringAndSize(self->data, self->dataSize);
}

#if PY_VERSION_HEX < 0x03090000
static PyBufferProcs BufferSegment_as_buffer = {
    (getbufferproc)BufferSegment_getbuffer, 0};
#endif

static PyMethodDef BufferSegment_methods[] = {
    {"tobytes", (PyCFunction)BufferSegment_tobytes, METH_NOARGS, NULL},
    {NULL, NULL}};

static PyMemberDef BufferSegment_members[] = {
    {"offset", T_ULONGLONG, offsetof(ZstdBufferSegment, offset), READONLY,
     "offset of segment within parent buffer"},
    {NULL}};

PyType_Slot ZstdBufferSegmentSlots[] = {
    {Py_tp_dealloc, BufferSegment_dealloc},
    {Py_sq_length, BufferSegment_length},
#if PY_VERSION_HEX >= 0x03090000
    {Py_bf_getbuffer, BufferSegment_getbuffer},
#endif
    {Py_tp_methods, BufferSegment_methods},
    {Py_tp_members, BufferSegment_members},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdBufferSegmentSpec = {
    "zstd.BufferSegment",
    sizeof(ZstdBufferSegment),
    0,
    Py_TPFLAGS_DEFAULT,
    ZstdBufferSegmentSlots,
};

PyTypeObject *ZstdBufferSegmentType;

static void
BufferWithSegmentsCollection_dealloc(ZstdBufferWithSegmentsCollection *self) {
    Py_ssize_t i;

    if (self->firstElements) {
        PyMem_Free(self->firstElements);
        self->firstElements = NULL;
    }

    if (self->buffers) {
        for (i = 0; i < self->bufferCount; i++) {
            Py_CLEAR(self->buffers[i]);
        }

        PyMem_Free(self->buffers);
        self->buffers = NULL;
    }

    PyObject_Del(self);
}

static int
BufferWithSegmentsCollection_init(ZstdBufferWithSegmentsCollection *self,
                                  PyObject *args) {
    Py_ssize_t size;
    Py_ssize_t i;
    Py_ssize_t offset = 0;

    size = PyTuple_Size(args);
    if (-1 == size) {
        return -1;
    }

    if (0 == size) {
        PyErr_SetString(PyExc_ValueError, "must pass at least 1 argument");
        return -1;
    }

    for (i = 0; i < size; i++) {
        PyObject *item = PyTuple_GET_ITEM(args, i);
        if (!PyObject_TypeCheck(item, ZstdBufferWithSegmentsType)) {
            PyErr_SetString(PyExc_TypeError,
                            "arguments must be BufferWithSegments instances");
            return -1;
        }

        if (0 == ((ZstdBufferWithSegments *)item)->segmentCount ||
            0 == ((ZstdBufferWithSegments *)item)->dataSize) {
            PyErr_SetString(PyExc_ValueError,
                            "ZstdBufferWithSegments cannot be empty");
            return -1;
        }
    }

    self->buffers = PyMem_Malloc(size * sizeof(ZstdBufferWithSegments *));
    if (NULL == self->buffers) {
        PyErr_NoMemory();
        return -1;
    }

    self->firstElements = PyMem_Malloc(size * sizeof(Py_ssize_t));
    if (NULL == self->firstElements) {
        PyMem_Free(self->buffers);
        self->buffers = NULL;
        PyErr_NoMemory();
        return -1;
    }

    self->bufferCount = size;

    for (i = 0; i < size; i++) {
        ZstdBufferWithSegments *item =
            (ZstdBufferWithSegments *)PyTuple_GET_ITEM(args, i);

        self->buffers[i] = item;
        Py_INCREF(item);

        if (i > 0) {
            self->firstElements[i - 1] = offset;
        }

        offset += item->segmentCount;
    }

    self->firstElements[size - 1] = offset;

    return 0;
}

static PyObject *
BufferWithSegmentsCollection_size(ZstdBufferWithSegmentsCollection *self) {
    Py_ssize_t i;
    Py_ssize_t j;
    unsigned long long size = 0;

    for (i = 0; i < self->bufferCount; i++) {
        for (j = 0; j < self->buffers[i]->segmentCount; j++) {
            size += self->buffers[i]->segments[j].length;
        }
    }

    return PyLong_FromUnsignedLongLong(size);
}

Py_ssize_t
BufferWithSegmentsCollection_length(ZstdBufferWithSegmentsCollection *self) {
    return self->firstElements[self->bufferCount - 1];
}

static ZstdBufferSegment *
BufferWithSegmentsCollection_item(ZstdBufferWithSegmentsCollection *self,
                                  Py_ssize_t i) {
    Py_ssize_t bufferOffset;

    if (i < 0) {
        PyErr_SetString(PyExc_IndexError, "offset must be non-negative");
        return NULL;
    }

    if (i >= BufferWithSegmentsCollection_length(self)) {
        PyErr_Format(PyExc_IndexError, "offset must be less than %zd",
                     BufferWithSegmentsCollection_length(self));
        return NULL;
    }

    for (bufferOffset = 0; bufferOffset < self->bufferCount; bufferOffset++) {
        Py_ssize_t offset = 0;

        if (i < self->firstElements[bufferOffset]) {
            if (bufferOffset > 0) {
                offset = self->firstElements[bufferOffset - 1];
            }

            return BufferWithSegments_item(self->buffers[bufferOffset],
                                           i - offset);
        }
    }

    PyErr_SetString(ZstdError,
                    "error resolving segment; this should not happen");
    return NULL;
}

static PyMethodDef BufferWithSegmentsCollection_methods[] = {
    {"size", (PyCFunction)BufferWithSegmentsCollection_size, METH_NOARGS,
     PyDoc_STR("total size in bytes of all segments")},
    {NULL, NULL}};

PyType_Slot ZstdBufferWithSegmentsCollectionSlots[] = {
    {Py_tp_dealloc, BufferWithSegmentsCollection_dealloc},
    {Py_sq_length, BufferWithSegmentsCollection_length},
    {Py_sq_item, BufferWithSegmentsCollection_item},
    {Py_tp_methods, BufferWithSegmentsCollection_methods},
    {Py_tp_init, BufferWithSegmentsCollection_init},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdBufferWithSegmentsCollectionSpec = {
    "zstd.BufferWithSegmentsCollection",
    sizeof(ZstdBufferWithSegmentsCollection),
    0,
    Py_TPFLAGS_DEFAULT,
    ZstdBufferWithSegmentsCollectionSlots,
};

PyTypeObject *ZstdBufferWithSegmentsCollectionType;

void bufferutil_module_init(PyObject *mod) {
    ZstdBufferWithSegmentsType =
        (PyTypeObject *)PyType_FromSpec(&ZstdBufferWithSegmentsSpec);
#if PY_VERSION_HEX < 0x03090000
    ZstdBufferWithSegmentsType->tp_as_buffer = &BufferWithSegments_as_buffer;
#endif
    if (PyType_Ready(ZstdBufferWithSegmentsType) < 0) {
        return;
    }

    Py_INCREF(ZstdBufferWithSegmentsType);
    PyModule_AddObject(mod, "BufferWithSegments",
                       (PyObject *)ZstdBufferWithSegmentsType);

    ZstdBufferSegmentsType =
        (PyTypeObject *)PyType_FromSpec(&ZstdBufferSegmentsSpec);
#if PY_VERSION_HEX < 0x03090000
    ZstdBufferSegmentsType->tp_as_buffer = &BufferSegments_as_buffer;
#endif
    if (PyType_Ready(ZstdBufferSegmentsType) < 0) {
        return;
    }

    Py_INCREF(ZstdBufferSegmentsType);
    PyModule_AddObject(mod, "BufferSegments",
                       (PyObject *)ZstdBufferSegmentsType);

    ZstdBufferSegmentType =
        (PyTypeObject *)PyType_FromSpec(&ZstdBufferSegmentSpec);
#if PY_VERSION_HEX < 0x03090000
    ZstdBufferSegmentType->tp_as_buffer = &BufferSegment_as_buffer;
#endif
    if (PyType_Ready(ZstdBufferSegmentType) < 0) {
        return;
    }

    Py_INCREF(ZstdBufferSegmentType);
    PyModule_AddObject(mod, "BufferSegment", (PyObject *)ZstdBufferSegmentType);

    ZstdBufferWithSegmentsCollectionType =
        (PyTypeObject *)PyType_FromSpec(&ZstdBufferWithSegmentsCollectionSpec);
    if (PyType_Ready(ZstdBufferWithSegmentsCollectionType) < 0) {
        return;
    }

    Py_INCREF(ZstdBufferWithSegmentsCollectionType);
    PyModule_AddObject(mod, "BufferWithSegmentsCollection",
                       (PyObject *)ZstdBufferWithSegmentsCollectionType);
}
