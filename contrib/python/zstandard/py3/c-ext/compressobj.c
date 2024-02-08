/**
 * Copyright (c) 2016-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

static void ZstdCompressionObj_dealloc(ZstdCompressionObj *self) {
    PyMem_Free(self->output.dst);
    self->output.dst = NULL;

    Py_XDECREF(self->compressor);

    PyObject_Del(self);
}

static PyObject *ZstdCompressionObj_compress(ZstdCompressionObj *self,
                                             PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"data", NULL};

    Py_buffer source;
    ZSTD_inBuffer input;
    size_t zresult;
    PyObject *result = NULL;
    Py_ssize_t resultSize = 0;

    if (self->finished) {
        PyErr_SetString(ZstdError,
                        "cannot call compress() after compressor finished");
        return NULL;
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:compress", kwlist,
                                     &source)) {
        return NULL;
    }

    input.src = source.buf;
    input.size = source.len;
    input.pos = 0;

    while (input.pos < (size_t)source.len) {
        Py_BEGIN_ALLOW_THREADS zresult = ZSTD_compressStream2(
            self->compressor->cctx, &self->output, &input, ZSTD_e_continue);
        Py_END_ALLOW_THREADS

            if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "zstd compress error: %s",
                         ZSTD_getErrorName(zresult));
            Py_CLEAR(result);
            goto finally;
        }

        if (self->output.pos) {
            if (result) {
                resultSize = PyBytes_GET_SIZE(result);

                if (safe_pybytes_resize(&result,
                                        resultSize + self->output.pos)) {
                    Py_CLEAR(result);
                    goto finally;
                }

                memcpy(PyBytes_AS_STRING(result) + resultSize, self->output.dst,
                       self->output.pos);
            }
            else {
                result = PyBytes_FromStringAndSize(self->output.dst,
                                                   self->output.pos);
                if (!result) {
                    goto finally;
                }
            }

            self->output.pos = 0;
        }
    }

    if (NULL == result) {
        result = PyBytes_FromString("");
    }

finally:
    PyBuffer_Release(&source);

    return result;
}

static PyObject *ZstdCompressionObj_flush(ZstdCompressionObj *self,
                                          PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"flush_mode", NULL};

    int flushMode = compressorobj_flush_finish;
    size_t zresult;
    PyObject *result = NULL;
    Py_ssize_t resultSize = 0;
    ZSTD_inBuffer input;
    ZSTD_EndDirective zFlushMode;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:flush", kwlist,
                                     &flushMode)) {
        return NULL;
    }

    if (flushMode != compressorobj_flush_finish &&
        flushMode != compressorobj_flush_block) {
        PyErr_SetString(PyExc_ValueError, "flush mode not recognized");
        return NULL;
    }

    if (self->finished) {
        PyErr_SetString(ZstdError, "compressor object already finished");
        return NULL;
    }

    switch (flushMode) {
    case compressorobj_flush_block:
        zFlushMode = ZSTD_e_flush;
        break;

    case compressorobj_flush_finish:
        zFlushMode = ZSTD_e_end;
        self->finished = 1;
        break;

    default:
        PyErr_SetString(ZstdError, "unhandled flush mode");
        return NULL;
    }

    assert(self->output.pos == 0);

    input.src = NULL;
    input.size = 0;
    input.pos = 0;

    while (1) {
        Py_BEGIN_ALLOW_THREADS zresult = ZSTD_compressStream2(
            self->compressor->cctx, &self->output, &input, zFlushMode);
        Py_END_ALLOW_THREADS

            if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "error ending compression stream: %s",
                         ZSTD_getErrorName(zresult));
            return NULL;
        }

        if (self->output.pos) {
            if (result) {
                resultSize = PyBytes_GET_SIZE(result);

                if (safe_pybytes_resize(&result,
                                        resultSize + self->output.pos)) {
                    Py_XDECREF(result);
                    return NULL;
                }

                memcpy(PyBytes_AS_STRING(result) + resultSize, self->output.dst,
                       self->output.pos);
            }
            else {
                result = PyBytes_FromStringAndSize(self->output.dst,
                                                   self->output.pos);
                if (!result) {
                    return NULL;
                }
            }

            self->output.pos = 0;
        }

        if (!zresult) {
            break;
        }
    }

    if (result) {
        return result;
    }
    else {
        return PyBytes_FromString("");
    }
}

static PyMethodDef ZstdCompressionObj_methods[] = {
    {"compress", (PyCFunction)ZstdCompressionObj_compress,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("compress data")},
    {"flush", (PyCFunction)ZstdCompressionObj_flush,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("finish compression operation")},
    {NULL, NULL}};

PyType_Slot ZstdCompressionObjSlots[] = {
    {Py_tp_dealloc, ZstdCompressionObj_dealloc},
    {Py_tp_methods, ZstdCompressionObj_methods},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdCompressionObjSpec = {
    "zstd.ZstdCompressionObj",
    sizeof(ZstdCompressionObj),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    ZstdCompressionObjSlots,
};

PyTypeObject *ZstdCompressionObjType;

void compressobj_module_init(PyObject *module) {
    ZstdCompressionObjType =
        (PyTypeObject *)PyType_FromSpec(&ZstdCompressionObjSpec);
    if (PyType_Ready(ZstdCompressionObjType) < 0) {
        return;
    }
}
