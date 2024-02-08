/**
 * Copyright (c) 2018-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

static void
ZstdCompressionChunkerIterator_dealloc(ZstdCompressionChunkerIterator *self) {
    Py_XDECREF(self->chunker);

    PyObject_Del(self);
}

static PyObject *ZstdCompressionChunkerIterator_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}

static PyObject *
ZstdCompressionChunkerIterator_iternext(ZstdCompressionChunkerIterator *self) {
    size_t zresult;
    PyObject *chunk;
    ZstdCompressionChunker *chunker = self->chunker;
    ZSTD_EndDirective zFlushMode;

    if (self->mode != compressionchunker_mode_normal &&
        chunker->input.pos != chunker->input.size) {
        PyErr_SetString(ZstdError, "input should have been fully consumed "
                                   "before calling flush() or finish()");
        return NULL;
    }

    if (chunker->finished) {
        return NULL;
    }

    /* If we have data left in the input, consume it. */
    while (chunker->input.pos < chunker->input.size) {
        Py_BEGIN_ALLOW_THREADS zresult =
            ZSTD_compressStream2(chunker->compressor->cctx, &chunker->output,
                                 &chunker->input, ZSTD_e_continue);
        Py_END_ALLOW_THREADS

            /* Input is fully consumed. */
            if (chunker->input.pos == chunker->input.size) {
            chunker->input.src = NULL;
            chunker->input.pos = 0;
            chunker->input.size = 0;
            PyBuffer_Release(&chunker->inBuffer);
        }

        if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "zstd compress error: %s",
                         ZSTD_getErrorName(zresult));
            return NULL;
        }

        /* If it produced a full output chunk, emit it. */
        if (chunker->output.pos == chunker->output.size) {
            chunk = PyBytes_FromStringAndSize(chunker->output.dst,
                                              chunker->output.pos);
            if (!chunk) {
                return NULL;
            }

            chunker->output.pos = 0;

            return chunk;
        }

        /* Else continue to compress available input data. */
    }

    /* We also need this here for the special case of an empty input buffer. */
    if (chunker->input.pos == chunker->input.size) {
        chunker->input.src = NULL;
        chunker->input.pos = 0;
        chunker->input.size = 0;
        PyBuffer_Release(&chunker->inBuffer);
    }

    /* No more input data. A partial chunk may be in chunker->output.
     * If we're in normal compression mode, we're done. Otherwise if we're in
     * flush or finish mode, we need to emit what data remains.
     */
    if (self->mode == compressionchunker_mode_normal) {
        /* We don't need to set StopIteration. */
        return NULL;
    }

    if (self->mode == compressionchunker_mode_flush) {
        zFlushMode = ZSTD_e_flush;
    }
    else if (self->mode == compressionchunker_mode_finish) {
        zFlushMode = ZSTD_e_end;
    }
    else {
        PyErr_SetString(ZstdError,
                        "unhandled compression mode; this should never happen");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS zresult =
        ZSTD_compressStream2(chunker->compressor->cctx, &chunker->output,
                             &chunker->input, zFlushMode);
    Py_END_ALLOW_THREADS

        if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "zstd compress error: %s",
                     ZSTD_getErrorName(zresult));
        return NULL;
    }

    if (!zresult && chunker->output.pos == 0) {
        return NULL;
    }

    chunk = PyBytes_FromStringAndSize(chunker->output.dst, chunker->output.pos);
    if (!chunk) {
        return NULL;
    }

    chunker->output.pos = 0;

    if (!zresult && self->mode == compressionchunker_mode_finish) {
        chunker->finished = 1;
    }

    return chunk;
}

PyType_Slot ZstdCompressionChunkerIteratorSlots[] = {
    {Py_tp_dealloc, ZstdCompressionChunkerIterator_dealloc},
    {Py_tp_iter, ZstdCompressionChunkerIterator_iter},
    {Py_tp_iternext, ZstdCompressionChunkerIterator_iternext},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdCompressionChunkerIteratorSpec = {
    "zstd.ZstdCompressionChunkerIterator",
    sizeof(ZstdCompressionChunkerIterator),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    ZstdCompressionChunkerIteratorSlots,
};

PyTypeObject *ZstdCompressionChunkerIteratorType;

static void ZstdCompressionChunker_dealloc(ZstdCompressionChunker *self) {
    PyBuffer_Release(&self->inBuffer);
    self->input.src = NULL;

    PyMem_Free(self->output.dst);
    self->output.dst = NULL;

    Py_XDECREF(self->compressor);

    PyObject_Del(self);
}

static ZstdCompressionChunkerIterator *
ZstdCompressionChunker_compress(ZstdCompressionChunker *self, PyObject *args,
                                PyObject *kwargs) {
    static char *kwlist[] = {"data", NULL};

    ZstdCompressionChunkerIterator *result;

    if (self->finished) {
        PyErr_SetString(ZstdError,
                        "cannot call compress() after compression finished");
        return NULL;
    }

    if (self->inBuffer.obj) {
        PyErr_SetString(ZstdError, "cannot perform operation before consuming "
                                   "output from previous operation");
        return NULL;
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:compress", kwlist,
                                     &self->inBuffer)) {
        return NULL;
    }

    result = (ZstdCompressionChunkerIterator *)PyObject_CallObject(
        (PyObject *)ZstdCompressionChunkerIteratorType, NULL);
    if (!result) {
        PyBuffer_Release(&self->inBuffer);
        return NULL;
    }

    self->input.src = self->inBuffer.buf;
    self->input.size = self->inBuffer.len;
    self->input.pos = 0;

    result->chunker = self;
    Py_INCREF(result->chunker);

    result->mode = compressionchunker_mode_normal;

    return result;
}

static ZstdCompressionChunkerIterator *
ZstdCompressionChunker_finish(ZstdCompressionChunker *self) {
    ZstdCompressionChunkerIterator *result;

    if (self->finished) {
        PyErr_SetString(ZstdError,
                        "cannot call finish() after compression finished");
        return NULL;
    }

    if (self->inBuffer.obj) {
        PyErr_SetString(ZstdError, "cannot call finish() before consuming "
                                   "output from previous operation");
        return NULL;
    }

    result = (ZstdCompressionChunkerIterator *)PyObject_CallObject(
        (PyObject *)ZstdCompressionChunkerIteratorType, NULL);
    if (!result) {
        return NULL;
    }

    result->chunker = self;
    Py_INCREF(result->chunker);

    result->mode = compressionchunker_mode_finish;

    return result;
}

static ZstdCompressionChunkerIterator *
ZstdCompressionChunker_flush(ZstdCompressionChunker *self, PyObject *args,
                             PyObject *kwargs) {
    ZstdCompressionChunkerIterator *result;

    if (self->finished) {
        PyErr_SetString(ZstdError,
                        "cannot call flush() after compression finished");
        return NULL;
    }

    if (self->inBuffer.obj) {
        PyErr_SetString(ZstdError, "cannot call flush() before consuming "
                                   "output from previous operation");
        return NULL;
    }

    result = (ZstdCompressionChunkerIterator *)PyObject_CallObject(
        (PyObject *)ZstdCompressionChunkerIteratorType, NULL);
    if (!result) {
        return NULL;
    }

    result->chunker = self;
    Py_INCREF(result->chunker);

    result->mode = compressionchunker_mode_flush;

    return result;
}

static PyMethodDef ZstdCompressionChunker_methods[] = {
    {"compress", (PyCFunction)ZstdCompressionChunker_compress,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("compress data")},
    {"finish", (PyCFunction)ZstdCompressionChunker_finish, METH_NOARGS,
     PyDoc_STR("finish compression operation")},
    {"flush", (PyCFunction)ZstdCompressionChunker_flush,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("finish compression operation")},
    {NULL, NULL}};

PyType_Slot ZstdCompressionChunkerSlots[] = {
    {Py_tp_dealloc, ZstdCompressionChunker_dealloc},
    {Py_tp_methods, ZstdCompressionChunker_methods},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdCompressionChunkerSpec = {
    "zstd.ZstdCompressionChunkerType",
    sizeof(ZstdCompressionChunker),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    ZstdCompressionChunkerSlots,
};

PyTypeObject *ZstdCompressionChunkerType;

void compressionchunker_module_init(PyObject *module) {
    ZstdCompressionChunkerIteratorType =
        (PyTypeObject *)PyType_FromSpec(&ZstdCompressionChunkerIteratorSpec);
    if (PyType_Ready(ZstdCompressionChunkerIteratorType) < 0) {
        return;
    }

    ZstdCompressionChunkerType =
        (PyTypeObject *)PyType_FromSpec(&ZstdCompressionChunkerSpec);
    if (PyType_Ready(ZstdCompressionChunkerType) < 0) {
        return;
    }
}
