/**
 * Copyright (c) 2016-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

#define min(a, b) (((a) < (b)) ? (a) : (b))

extern PyObject *ZstdError;

static void ZstdCompressorIterator_dealloc(ZstdCompressorIterator *self) {
    Py_XDECREF(self->readResult);
    Py_XDECREF(self->compressor);
    Py_XDECREF(self->reader);

    if (self->buffer.buf) {
        PyBuffer_Release(&self->buffer);
        memset(&self->buffer, 0, sizeof(self->buffer));
    }

    if (self->output.dst) {
        PyMem_Free(self->output.dst);
        self->output.dst = NULL;
    }

    PyObject_Del(self);
}

static PyObject *ZstdCompressorIterator_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}

static PyObject *ZstdCompressorIterator_iternext(ZstdCompressorIterator *self) {
    size_t zresult;
    PyObject *readResult = NULL;
    PyObject *chunk;
    char *readBuffer;
    Py_ssize_t readSize = 0;
    Py_ssize_t bufferRemaining;

    if (self->finishedOutput) {
        PyErr_SetString(PyExc_StopIteration, "output flushed");
        return NULL;
    }

feedcompressor:

    /* If we have data left in the input, consume it. */
    if (self->input.pos < self->input.size) {
        Py_BEGIN_ALLOW_THREADS zresult =
            ZSTD_compressStream2(self->compressor->cctx, &self->output,
                                 &self->input, ZSTD_e_continue);
        Py_END_ALLOW_THREADS

            /* Release the Python object holding the input buffer. */
            if (self->input.pos == self->input.size) {
            self->input.src = NULL;
            self->input.pos = 0;
            self->input.size = 0;
            Py_DECREF(self->readResult);
            self->readResult = NULL;
        }

        if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "zstd compress error: %s",
                         ZSTD_getErrorName(zresult));
            return NULL;
        }

        /* If it produced output data, emit it. */
        if (self->output.pos) {
            chunk =
                PyBytes_FromStringAndSize(self->output.dst, self->output.pos);
            self->output.pos = 0;
            return chunk;
        }
    }

    /* We should never have output data sitting around after a previous call. */
    assert(self->output.pos == 0);

    /* The code above should have either emitted a chunk and returned or
    consumed the entire input buffer. So the state of the input buffer is not
    relevant. */
    if (!self->finishedInput) {
        if (self->reader) {
            readResult =
                PyObject_CallMethod(self->reader, "read", "I", self->inSize);
            if (!readResult) {
                return NULL;
            }

            PyBytes_AsStringAndSize(readResult, &readBuffer, &readSize);
        }
        else {
            assert(self->buffer.buf);

            /* Only support contiguous C arrays. */
            assert(self->buffer.strides == NULL &&
                   self->buffer.suboffsets == NULL);
            assert(self->buffer.itemsize == 1);

            readBuffer = (char *)self->buffer.buf + self->bufferOffset;
            bufferRemaining = self->buffer.len - self->bufferOffset;
            readSize = min(bufferRemaining, (Py_ssize_t)self->inSize);
            self->bufferOffset += readSize;
        }

        if (0 == readSize) {
            Py_XDECREF(readResult);
            self->finishedInput = 1;
        }
        else {
            self->readResult = readResult;
        }
    }

    /* EOF */
    if (0 == readSize) {
        self->input.src = NULL;
        self->input.size = 0;
        self->input.pos = 0;

        zresult = ZSTD_compressStream2(self->compressor->cctx, &self->output,
                                       &self->input, ZSTD_e_end);
        if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "error ending compression stream: %s",
                         ZSTD_getErrorName(zresult));
            return NULL;
        }

        assert(self->output.pos);

        if (0 == zresult) {
            self->finishedOutput = 1;
        }

        chunk = PyBytes_FromStringAndSize(self->output.dst, self->output.pos);
        self->output.pos = 0;
        return chunk;
    }

    /* New data from reader. Feed into compressor. */
    self->input.src = readBuffer;
    self->input.size = readSize;
    self->input.pos = 0;

    Py_BEGIN_ALLOW_THREADS zresult = ZSTD_compressStream2(
        self->compressor->cctx, &self->output, &self->input, ZSTD_e_continue);
    Py_END_ALLOW_THREADS

        /* The input buffer currently points to memory managed by Python
        (readBuffer). This object was allocated by this function. If it wasn't
        fully consumed, we need to release it in a subsequent function call.
        If it is fully consumed, do that now.
        */
        if (self->input.pos == self->input.size) {
        self->input.src = NULL;
        self->input.pos = 0;
        self->input.size = 0;
        Py_XDECREF(self->readResult);
        self->readResult = NULL;
    }

    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "zstd compress error: %s",
                     ZSTD_getErrorName(zresult));
        return NULL;
    }

    assert(self->input.pos <= self->input.size);

    /* If we didn't write anything, start the process over. */
    if (0 == self->output.pos) {
        goto feedcompressor;
    }

    chunk = PyBytes_FromStringAndSize(self->output.dst, self->output.pos);
    self->output.pos = 0;
    return chunk;
}

PyType_Slot ZstdCompressorIteratorSlots[] = {
    {Py_tp_dealloc, ZstdCompressorIterator_dealloc},
    {Py_tp_iter, ZstdCompressorIterator_iter},
    {Py_tp_iternext, ZstdCompressorIterator_iternext},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdCompressorIteratorSpec = {
    "zstd.ZstdCompressorIterator",
    sizeof(ZstdCompressorIterator),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    ZstdCompressorIteratorSlots,
};

PyTypeObject *ZstdCompressorIteratorType;

void compressoriterator_module_init(PyObject *mod) {
    ZstdCompressorIteratorType =
        (PyTypeObject *)PyType_FromSpec(&ZstdCompressorIteratorSpec);
    if (PyType_Ready(ZstdCompressorIteratorType) < 0) {
        return;
    }
}
