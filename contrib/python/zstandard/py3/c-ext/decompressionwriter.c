/**
 * Copyright (c) 2016-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

static void ZstdDecompressionWriter_dealloc(ZstdDecompressionWriter *self) {
    Py_XDECREF(self->decompressor);
    Py_XDECREF(self->writer);

    PyObject_Del(self);
}

static PyObject *ZstdDecompressionWriter_enter(ZstdDecompressionWriter *self) {
    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    if (self->entered) {
        PyErr_SetString(ZstdError, "cannot __enter__ multiple times");
        return NULL;
    }

    self->entered = 1;

    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *ZstdDecompressionWriter_exit(ZstdDecompressionWriter *self,
                                              PyObject *args) {
    self->entered = 0;

    if (NULL == PyObject_CallMethod((PyObject *)self, "close", NULL)) {
        return NULL;
    }

    Py_RETURN_FALSE;
}

static PyObject *
ZstdDecompressionWriter_memory_size(ZstdDecompressionWriter *self) {
    return PyLong_FromSize_t(ZSTD_sizeof_DCtx(self->decompressor->dctx));
}

static PyObject *ZstdDecompressionWriter_write(ZstdDecompressionWriter *self,
                                               PyObject *args,
                                               PyObject *kwargs) {
    static char *kwlist[] = {"data", NULL};

    PyObject *result = NULL;
    Py_buffer source;
    size_t zresult = 0;
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;
    PyObject *res;
    Py_ssize_t totalWrite = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:write", kwlist,
                                     &source)) {
        return NULL;
    }

    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    output.dst = PyMem_Malloc(self->outSize);
    if (!output.dst) {
        PyErr_NoMemory();
        goto finally;
    }
    output.size = self->outSize;
    output.pos = 0;

    input.src = source.buf;
    input.size = source.len;
    input.pos = 0;

    while (input.pos < (size_t)source.len) {
        Py_BEGIN_ALLOW_THREADS zresult =
            ZSTD_decompressStream(self->decompressor->dctx, &output, &input);
        Py_END_ALLOW_THREADS

            if (ZSTD_isError(zresult)) {
            PyMem_Free(output.dst);
            PyErr_Format(ZstdError, "zstd decompress error: %s",
                         ZSTD_getErrorName(zresult));
            goto finally;
        }

        if (output.pos) {
            res = PyObject_CallMethod(self->writer, "write", "y#", output.dst,
                                      output.pos);
            if (NULL == res) {
                goto finally;
            }
            Py_XDECREF(res);
            totalWrite += output.pos;
            output.pos = 0;
        }
    }

    PyMem_Free(output.dst);

    if (self->writeReturnRead) {
        result = PyLong_FromSize_t(input.pos);
    }
    else {
        result = PyLong_FromSsize_t(totalWrite);
    }

finally:
    PyBuffer_Release(&source);
    return result;
}

static PyObject *ZstdDecompressionWriter_close(ZstdDecompressionWriter *self) {
    PyObject *result;

    if (self->closed) {
        Py_RETURN_NONE;
    }

    self->closing = 1;
    result = PyObject_CallMethod((PyObject *)self, "flush", NULL);
    self->closing = 0;
    self->closed = 1;

    if (NULL == result) {
        return NULL;
    }

    /* Call close on underlying stream as well. */
    if (self->closefd && PyObject_HasAttrString(self->writer, "close")) {
        return PyObject_CallMethod(self->writer, "close", NULL);
    }

    Py_RETURN_NONE;
}

static PyObject *ZstdDecompressionWriter_fileno(ZstdDecompressionWriter *self) {
    if (PyObject_HasAttrString(self->writer, "fileno")) {
        return PyObject_CallMethod(self->writer, "fileno", NULL);
    }
    else {
        PyErr_SetString(PyExc_OSError,
                        "fileno not available on underlying writer");
        return NULL;
    }
}

static PyObject *ZstdDecompressionWriter_flush(ZstdDecompressionWriter *self) {
    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    if (!self->closing && PyObject_HasAttrString(self->writer, "flush")) {
        return PyObject_CallMethod(self->writer, "flush", NULL);
    }
    else {
        Py_RETURN_NONE;
    }
}

static PyObject *ZstdDecompressionWriter_iter(PyObject *self) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *ZstdDecompressionWriter_iternext(PyObject *self) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *ZstdDecompressionWriter_false(PyObject *self, PyObject *args) {
    Py_RETURN_FALSE;
}

static PyObject *ZstdDecompressionWriter_true(PyObject *self, PyObject *args) {
    Py_RETURN_TRUE;
}

static PyObject *ZstdDecompressionWriter_unsupported(PyObject *self,
                                                     PyObject *args,
                                                     PyObject *kwargs) {
    set_io_unsupported_operation();
    return NULL;
}

static PyMethodDef ZstdDecompressionWriter_methods[] = {
    {"__enter__", (PyCFunction)ZstdDecompressionWriter_enter, METH_NOARGS,
     PyDoc_STR("Enter a decompression context.")},
    {"__exit__", (PyCFunction)ZstdDecompressionWriter_exit, METH_VARARGS,
     PyDoc_STR("Exit a decompression context.")},
    {"memory_size", (PyCFunction)ZstdDecompressionWriter_memory_size,
     METH_NOARGS,
     PyDoc_STR(
         "Obtain the memory size in bytes of the underlying decompressor.")},
    {"close", (PyCFunction)ZstdDecompressionWriter_close, METH_NOARGS, NULL},
    {"fileno", (PyCFunction)ZstdDecompressionWriter_fileno, METH_NOARGS, NULL},
    {"flush", (PyCFunction)ZstdDecompressionWriter_flush, METH_NOARGS, NULL},
    {"isatty", ZstdDecompressionWriter_false, METH_NOARGS, NULL},
    {"readable", ZstdDecompressionWriter_false, METH_NOARGS, NULL},
    {"readline", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"readlines", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"seek", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"seekable", ZstdDecompressionWriter_false, METH_NOARGS, NULL},
    {"tell", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"truncate", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"writable", ZstdDecompressionWriter_true, METH_NOARGS, NULL},
    {"writelines", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"read", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"readall", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"readinto", (PyCFunction)ZstdDecompressionWriter_unsupported,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"write", (PyCFunction)ZstdDecompressionWriter_write,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("Compress data")},
    {NULL, NULL}};

static PyMemberDef ZstdDecompressionWriter_members[] = {
    {"closed", T_BOOL, offsetof(ZstdDecompressionWriter, closed), READONLY,
     NULL},
    {NULL}};

PyType_Slot ZstdDecompressionWriterSlots[] = {
    {Py_tp_dealloc, ZstdDecompressionWriter_dealloc},
    {Py_tp_iter, ZstdDecompressionWriter_iter},
    {Py_tp_iternext, ZstdDecompressionWriter_iternext},
    {Py_tp_methods, ZstdDecompressionWriter_methods},
    {Py_tp_members, ZstdDecompressionWriter_members},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdDecompressionWriterSpec = {
    "zstd.ZstdDecompressionWriter",
    sizeof(ZstdDecompressionWriter),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    ZstdDecompressionWriterSlots,
};

PyTypeObject *ZstdDecompressionWriterType;

void decompressionwriter_module_init(PyObject *mod) {
    ZstdDecompressionWriterType =
        (PyTypeObject *)PyType_FromSpec(&ZstdDecompressionWriterSpec);
    if (PyType_Ready(ZstdDecompressionWriterType) < 0) {
        return;
    }

    Py_INCREF((PyObject *)ZstdDecompressionWriterType);
    PyModule_AddObject(mod, "ZstdDecompressionWriter",
                       (PyObject *)ZstdDecompressionWriterType);
}
