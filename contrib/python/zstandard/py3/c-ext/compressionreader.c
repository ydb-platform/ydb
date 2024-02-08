/**
 * Copyright (c) 2017-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

static void compressionreader_dealloc(ZstdCompressionReader *self) {
    Py_XDECREF(self->compressor);
    Py_XDECREF(self->reader);

    if (self->buffer.buf) {
        PyBuffer_Release(&self->buffer);
        memset(&self->buffer, 0, sizeof(self->buffer));
    }

    PyObject_Del(self);
}

static ZstdCompressionReader *
compressionreader_enter(ZstdCompressionReader *self) {
    if (self->entered) {
        PyErr_SetString(PyExc_ValueError, "cannot __enter__ multiple times");
        return NULL;
    }

    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    self->entered = 1;

    Py_INCREF(self);
    return self;
}

static PyObject *compressionreader_exit(ZstdCompressionReader *self,
                                        PyObject *args) {
    PyObject *exc_type;
    PyObject *exc_value;
    PyObject *exc_tb;
    PyObject *result;

    if (!PyArg_ParseTuple(args, "OOO:__exit__", &exc_type, &exc_value,
                          &exc_tb)) {
        return NULL;
    }

    self->entered = 0;

    result = PyObject_CallMethod((PyObject *)self, "close", NULL);
    if (NULL == result) {
        return NULL;
    }

    /* Release resources associated with source. */
    Py_CLEAR(self->reader);
    if (self->buffer.buf) {
        PyBuffer_Release(&self->buffer);
        memset(&self->buffer, 0, sizeof(self->buffer));
    }

    Py_CLEAR(self->compressor);

    Py_RETURN_FALSE;
}

static PyObject *compressionreader_readable(ZstdCompressionReader *self) {
    Py_RETURN_TRUE;
}

static PyObject *compressionreader_writable(ZstdCompressionReader *self) {
    Py_RETURN_FALSE;
}

static PyObject *compressionreader_seekable(ZstdCompressionReader *self) {
    Py_RETURN_FALSE;
}

static PyObject *compressionreader_readline(PyObject *self, PyObject *args) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *compressionreader_readlines(PyObject *self, PyObject *args) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *compressionreader_write(PyObject *self, PyObject *args) {
    PyErr_SetString(PyExc_OSError, "stream is not writable");
    return NULL;
}

static PyObject *compressionreader_writelines(PyObject *self, PyObject *args) {
    PyErr_SetString(PyExc_OSError, "stream is not writable");
    return NULL;
}

static PyObject *compressionreader_isatty(PyObject *self) {
    Py_RETURN_FALSE;
}

static PyObject *compressionreader_flush(PyObject *self) {
    Py_RETURN_NONE;
}

static PyObject *compressionreader_close(ZstdCompressionReader *self) {
    if (self->closed) {
        Py_RETURN_NONE;
    }

    self->closed = 1;

    if (self->closefd && self->reader != NULL &&
        PyObject_HasAttrString(self->reader, "close")) {
        return PyObject_CallMethod(self->reader, "close", NULL);
    }

    Py_RETURN_NONE;
}

static PyObject *compressionreader_tell(ZstdCompressionReader *self) {
    /* TODO should this raise OSError since stream isn't seekable? */
    return PyLong_FromUnsignedLongLong(self->bytesCompressed);
}

int read_compressor_input(ZstdCompressionReader *self) {
    if (self->finishedInput) {
        return 0;
    }

    if (self->input.pos != self->input.size) {
        return 0;
    }

    if (self->reader) {
        Py_buffer buffer;

        assert(self->readResult == NULL);

        self->readResult =
            PyObject_CallMethod(self->reader, "read", "k", self->readSize);

        if (NULL == self->readResult) {
            return -1;
        }

        memset(&buffer, 0, sizeof(buffer));

        if (0 !=
            PyObject_GetBuffer(self->readResult, &buffer, PyBUF_CONTIG_RO)) {
            return -1;
        }

        /* EOF */
        if (0 == buffer.len) {
            self->finishedInput = 1;
            Py_CLEAR(self->readResult);
        }
        else {
            self->input.src = buffer.buf;
            self->input.size = buffer.len;
            self->input.pos = 0;
        }

        PyBuffer_Release(&buffer);
    }
    else {
        assert(self->buffer.buf);

        self->input.src = self->buffer.buf;
        self->input.size = self->buffer.len;
        self->input.pos = 0;
    }

    return 1;
}

int compress_input(ZstdCompressionReader *self, ZSTD_outBuffer *output) {
    size_t oldPos;
    size_t zresult;

    /* If we have data left over, consume it. */
    if (self->input.pos < self->input.size) {
        oldPos = output->pos;

        Py_BEGIN_ALLOW_THREADS zresult = ZSTD_compressStream2(
            self->compressor->cctx, output, &self->input, ZSTD_e_continue);
        Py_END_ALLOW_THREADS

            self->bytesCompressed += output->pos - oldPos;

        /* Input exhausted. Clear out state tracking. */
        if (self->input.pos == self->input.size) {
            memset(&self->input, 0, sizeof(self->input));
            Py_CLEAR(self->readResult);

            if (self->buffer.buf) {
                self->finishedInput = 1;
            }
        }

        if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "zstd compress error: %s",
                         ZSTD_getErrorName(zresult));
            return -1;
        }
    }

    if (output->pos && output->pos == output->size) {
        return 1;
    }
    else {
        return 0;
    }
}

static PyObject *compressionreader_read(ZstdCompressionReader *self,
                                        PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"size", NULL};

    Py_ssize_t size = -1;
    PyObject *result = NULL;
    char *resultBuffer;
    Py_ssize_t resultSize;
    size_t zresult;
    size_t oldPos;
    int readResult, compressResult;

    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|n", kwlist, &size)) {
        return NULL;
    }

    if (size < -1) {
        PyErr_SetString(PyExc_ValueError,
                        "cannot read negative amounts less than -1");
        return NULL;
    }

    if (size == -1) {
        return PyObject_CallMethod((PyObject *)self, "readall", NULL);
    }

    if (self->finishedOutput || size == 0) {
        return PyBytes_FromStringAndSize("", 0);
    }

    result = PyBytes_FromStringAndSize(NULL, size);
    if (NULL == result) {
        return NULL;
    }

    PyBytes_AsStringAndSize(result, &resultBuffer, &resultSize);

    self->output.dst = resultBuffer;
    self->output.size = resultSize;
    self->output.pos = 0;

readinput:

    compressResult = compress_input(self, &self->output);

    if (-1 == compressResult) {
        Py_XDECREF(result);
        return NULL;
    }
    else if (0 == compressResult) {
        /* There is room in the output. We fall through to below, which will
         * either get more input for us or will attempt to end the stream.
         */
    }
    else if (1 == compressResult) {
        memset(&self->output, 0, sizeof(self->output));
        return result;
    }
    else {
        assert(0);
    }

    readResult = read_compressor_input(self);

    if (-1 == readResult) {
        return NULL;
    }
    else if (0 == readResult) {
    }
    else if (1 == readResult) {
    }
    else {
        assert(0);
    }

    if (self->input.size) {
        goto readinput;
    }

    /* Else EOF */
    oldPos = self->output.pos;

    zresult = ZSTD_compressStream2(self->compressor->cctx, &self->output,
                                   &self->input, ZSTD_e_end);

    self->bytesCompressed += self->output.pos - oldPos;

    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error ending compression stream: %s",
                     ZSTD_getErrorName(zresult));
        Py_XDECREF(result);
        return NULL;
    }

    assert(self->output.pos);

    if (0 == zresult) {
        self->finishedOutput = 1;
    }

    if (safe_pybytes_resize(&result, self->output.pos)) {
        Py_XDECREF(result);
        return NULL;
    }

    memset(&self->output, 0, sizeof(self->output));

    return result;
}

static PyObject *compressionreader_read1(ZstdCompressionReader *self,
                                         PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"size", NULL};

    Py_ssize_t size = -1;
    PyObject *result = NULL;
    char *resultBuffer;
    Py_ssize_t resultSize;
    ZSTD_outBuffer output;
    int compressResult;
    size_t oldPos;
    size_t zresult;

    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|n:read1", kwlist, &size)) {
        return NULL;
    }

    if (size < -1) {
        PyErr_SetString(PyExc_ValueError,
                        "cannot read negative amounts less than -1");
        return NULL;
    }

    if (self->finishedOutput || size == 0) {
        return PyBytes_FromStringAndSize("", 0);
    }

    if (size == -1) {
        size = ZSTD_CStreamOutSize();
    }

    result = PyBytes_FromStringAndSize(NULL, size);
    if (NULL == result) {
        return NULL;
    }

    PyBytes_AsStringAndSize(result, &resultBuffer, &resultSize);

    output.dst = resultBuffer;
    output.size = resultSize;
    output.pos = 0;

    /* read1() is supposed to use at most 1 read() from the underlying stream.
       However, we can't satisfy this requirement with compression because
       not every input will generate output. We /could/ flush the compressor,
       but this may not be desirable. We allow multiple read() from the
       underlying stream. But unlike read(), we return as soon as output data
       is available.
    */

    compressResult = compress_input(self, &output);

    if (-1 == compressResult) {
        Py_XDECREF(result);
        return NULL;
    }
    else if (0 == compressResult || 1 == compressResult) {
    }
    else {
        assert(0);
    }

    if (output.pos) {
        goto finally;
    }

    while (!self->finishedInput) {
        int readResult = read_compressor_input(self);

        if (-1 == readResult) {
            Py_XDECREF(result);
            return NULL;
        }
        else if (0 == readResult || 1 == readResult) {
        }
        else {
            assert(0);
        }

        compressResult = compress_input(self, &output);

        if (-1 == compressResult) {
            Py_XDECREF(result);
            return NULL;
        }
        else if (0 == compressResult || 1 == compressResult) {
        }
        else {
            assert(0);
        }

        if (output.pos) {
            goto finally;
        }
    }

    /* EOF */
    oldPos = output.pos;

    zresult = ZSTD_compressStream2(self->compressor->cctx, &output,
                                   &self->input, ZSTD_e_end);

    self->bytesCompressed += output.pos - oldPos;

    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error ending compression stream: %s",
                     ZSTD_getErrorName(zresult));
        Py_XDECREF(result);
        return NULL;
    }

    if (zresult == 0) {
        self->finishedOutput = 1;
    }

finally:
    if (result) {
        if (safe_pybytes_resize(&result, output.pos)) {
            Py_XDECREF(result);
            return NULL;
        }
    }

    return result;
}

static PyObject *compressionreader_readall(PyObject *self) {
    PyObject *chunks = NULL;
    PyObject *empty = NULL;
    PyObject *result = NULL;

    /* Our strategy is to collect chunks into a list then join all the
     * chunks at the end. We could potentially use e.g. an io.BytesIO. But
     * this feels simple enough to implement and avoids potentially expensive
     * reallocations of large buffers.
     */
    chunks = PyList_New(0);
    if (NULL == chunks) {
        return NULL;
    }

    while (1) {
        PyObject *chunk = PyObject_CallMethod(self, "read", "i", 1048576);
        if (NULL == chunk) {
            Py_DECREF(chunks);
            return NULL;
        }

        if (!PyBytes_Size(chunk)) {
            Py_DECREF(chunk);
            break;
        }

        if (PyList_Append(chunks, chunk)) {
            Py_DECREF(chunk);
            Py_DECREF(chunks);
            return NULL;
        }

        Py_DECREF(chunk);
    }

    empty = PyBytes_FromStringAndSize("", 0);
    if (NULL == empty) {
        Py_DECREF(chunks);
        return NULL;
    }

    result = PyObject_CallMethod(empty, "join", "O", chunks);

    Py_DECREF(empty);
    Py_DECREF(chunks);

    return result;
}

static PyObject *compressionreader_readinto(ZstdCompressionReader *self,
                                            PyObject *args) {
    Py_buffer dest;
    ZSTD_outBuffer output;
    int readResult, compressResult;
    PyObject *result = NULL;
    size_t zresult;
    size_t oldPos;

    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    if (self->finishedOutput) {
        return PyLong_FromLong(0);
    }

    if (!PyArg_ParseTuple(args, "w*:readinto", &dest)) {
        return NULL;
    }

    output.dst = dest.buf;
    output.size = dest.len;
    output.pos = 0;

    compressResult = compress_input(self, &output);

    if (-1 == compressResult) {
        goto finally;
    }
    else if (0 == compressResult) {
    }
    else if (1 == compressResult) {
        result = PyLong_FromSize_t(output.pos);
        goto finally;
    }
    else {
        assert(0);
    }

    while (!self->finishedInput) {
        readResult = read_compressor_input(self);

        if (-1 == readResult) {
            goto finally;
        }
        else if (0 == readResult || 1 == readResult) {
        }
        else {
            assert(0);
        }

        compressResult = compress_input(self, &output);

        if (-1 == compressResult) {
            goto finally;
        }
        else if (0 == compressResult) {
        }
        else if (1 == compressResult) {
            result = PyLong_FromSize_t(output.pos);
            goto finally;
        }
        else {
            assert(0);
        }
    }

    /* EOF */
    oldPos = output.pos;

    zresult = ZSTD_compressStream2(self->compressor->cctx, &output,
                                   &self->input, ZSTD_e_end);

    self->bytesCompressed += self->output.pos - oldPos;

    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error ending compression stream: %s",
                     ZSTD_getErrorName(zresult));
        goto finally;
    }

    assert(output.pos);

    if (0 == zresult) {
        self->finishedOutput = 1;
    }

    result = PyLong_FromSize_t(output.pos);

finally:
    PyBuffer_Release(&dest);

    return result;
}

static PyObject *compressionreader_readinto1(ZstdCompressionReader *self,
                                             PyObject *args) {
    Py_buffer dest;
    PyObject *result = NULL;
    ZSTD_outBuffer output;
    int compressResult;
    size_t oldPos;
    size_t zresult;

    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    if (self->finishedOutput) {
        return PyLong_FromLong(0);
    }

    if (!PyArg_ParseTuple(args, "w*:readinto1", &dest)) {
        return NULL;
    }

    output.dst = dest.buf;
    output.size = dest.len;
    output.pos = 0;

    compressResult = compress_input(self, &output);

    if (-1 == compressResult) {
        goto finally;
    }
    else if (0 == compressResult || 1 == compressResult) {
    }
    else {
        assert(0);
    }

    if (output.pos) {
        result = PyLong_FromSize_t(output.pos);
        goto finally;
    }

    while (!self->finishedInput) {
        int readResult = read_compressor_input(self);

        if (-1 == readResult) {
            goto finally;
        }
        else if (0 == readResult || 1 == readResult) {
        }
        else {
            assert(0);
        }

        compressResult = compress_input(self, &output);

        if (-1 == compressResult) {
            goto finally;
        }
        else if (0 == compressResult) {
        }
        else if (1 == compressResult) {
            result = PyLong_FromSize_t(output.pos);
            goto finally;
        }
        else {
            assert(0);
        }

        /* If we produced output and we're not done with input, emit
         * that output now, as we've hit restrictions of read1().
         */
        if (output.pos && !self->finishedInput) {
            result = PyLong_FromSize_t(output.pos);
            goto finally;
        }

        /* Otherwise we either have no output or we've exhausted the
         * input. Either we try to get more input or we fall through
         * to EOF below */
    }

    /* EOF */
    oldPos = output.pos;

    zresult = ZSTD_compressStream2(self->compressor->cctx, &output,
                                   &self->input, ZSTD_e_end);

    self->bytesCompressed += self->output.pos - oldPos;

    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error ending compression stream: %s",
                     ZSTD_getErrorName(zresult));
        goto finally;
    }

    assert(output.pos);

    if (0 == zresult) {
        self->finishedOutput = 1;
    }

    result = PyLong_FromSize_t(output.pos);

finally:
    PyBuffer_Release(&dest);

    return result;
}

static PyObject *compressionreader_iter(PyObject *self) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *compressionreader_iternext(PyObject *self) {
    set_io_unsupported_operation();
    return NULL;
}

static PyMethodDef compressionreader_methods[] = {
    {"__enter__", (PyCFunction)compressionreader_enter, METH_NOARGS,
     PyDoc_STR("Enter a compression context")},
    {"__exit__", (PyCFunction)compressionreader_exit, METH_VARARGS,
     PyDoc_STR("Exit a compression context")},
    {"close", (PyCFunction)compressionreader_close, METH_NOARGS,
     PyDoc_STR("Close the stream so it cannot perform any more operations")},
    {"flush", (PyCFunction)compressionreader_flush, METH_NOARGS,
     PyDoc_STR("no-ops")},
    {"isatty", (PyCFunction)compressionreader_isatty, METH_NOARGS,
     PyDoc_STR("Returns False")},
    {"readable", (PyCFunction)compressionreader_readable, METH_NOARGS,
     PyDoc_STR("Returns True")},
    {"read", (PyCFunction)compressionreader_read, METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("read compressed data")},
    {"read1", (PyCFunction)compressionreader_read1,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"readall", (PyCFunction)compressionreader_readall, METH_NOARGS,
     PyDoc_STR("Not implemented")},
    {"readinto", (PyCFunction)compressionreader_readinto, METH_VARARGS, NULL},
    {"readinto1", (PyCFunction)compressionreader_readinto1, METH_VARARGS, NULL},
    {"readline", (PyCFunction)compressionreader_readline, METH_VARARGS,
     PyDoc_STR("Not implemented")},
    {"readlines", (PyCFunction)compressionreader_readlines, METH_VARARGS,
     PyDoc_STR("Not implemented")},
    {"seekable", (PyCFunction)compressionreader_seekable, METH_NOARGS,
     PyDoc_STR("Returns False")},
    {"tell", (PyCFunction)compressionreader_tell, METH_NOARGS,
     PyDoc_STR("Returns current number of bytes compressed")},
    {"writable", (PyCFunction)compressionreader_writable, METH_NOARGS,
     PyDoc_STR("Returns False")},
    {"write", compressionreader_write, METH_VARARGS,
     PyDoc_STR("Raises OSError")},
    {"writelines", compressionreader_writelines, METH_VARARGS,
     PyDoc_STR("Not implemented")},
    {NULL, NULL}};

static PyMemberDef compressionreader_members[] = {
    {"closed", T_BOOL, offsetof(ZstdCompressionReader, closed), READONLY,
     "whether stream is closed"},
    {NULL}};

PyType_Slot ZstdCompressionReaderSlots[] = {
    {Py_tp_dealloc, compressionreader_dealloc},
    {Py_tp_iter, compressionreader_iter},
    {Py_tp_iternext, compressionreader_iternext},
    {Py_tp_methods, compressionreader_methods},
    {Py_tp_members, compressionreader_members},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdCompressionReaderSpec = {
    "zstd.ZstdCompressionReader",
    sizeof(ZstdCompressionReader),
    0,
    Py_TPFLAGS_DEFAULT,
    ZstdCompressionReaderSlots,
};

PyTypeObject *ZstdCompressionReaderType;

void compressionreader_module_init(PyObject *mod) {
    /* TODO make reader a sub-class of io.RawIOBase */

    ZstdCompressionReaderType =
        (PyTypeObject *)PyType_FromSpec(&ZstdCompressionReaderSpec);
    if (PyType_Ready(ZstdCompressionReaderType) < 0) {
        return;
    }

    Py_INCREF((PyObject *)ZstdCompressionReaderType);
    PyModule_AddObject(mod, "ZstdCompressionReader",
                       (PyObject *)ZstdCompressionReaderType);
}
