/**
 * Copyright (c) 2017-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

static void decompressionreader_dealloc(ZstdDecompressionReader *self) {
    Py_XDECREF(self->decompressor);
    Py_XDECREF(self->reader);

    if (self->buffer.buf) {
        PyBuffer_Release(&self->buffer);
    }

    Py_CLEAR(self->readResult);

    PyObject_Del(self);
}

static ZstdDecompressionReader *
decompressionreader_enter(ZstdDecompressionReader *self) {
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

static PyObject *decompressionreader_exit(ZstdDecompressionReader *self,
                                          PyObject *args) {
    PyObject *exc_type;
    PyObject *exc_value;
    PyObject *exc_tb;

    if (!PyArg_ParseTuple(args, "OOO:__exit__", &exc_type, &exc_value,
                          &exc_tb)) {
        return NULL;
    }

    self->entered = 0;

    if (NULL == PyObject_CallMethod((PyObject *)self, "close", NULL)) {
        return NULL;
    }

    /* Release resources. */
    Py_CLEAR(self->reader);
    if (self->buffer.buf) {
        PyBuffer_Release(&self->buffer);
        memset(&self->buffer, 0, sizeof(self->buffer));
    }

    Py_CLEAR(self->decompressor);

    Py_RETURN_FALSE;
}

static PyObject *decompressionreader_readable(PyObject *self) {
    Py_RETURN_TRUE;
}

static PyObject *decompressionreader_writable(PyObject *self) {
    Py_RETURN_FALSE;
}

static PyObject *decompressionreader_seekable(PyObject *self) {
    Py_RETURN_FALSE;
}

static PyObject *decompressionreader_close(ZstdDecompressionReader *self) {
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

static PyObject *decompressionreader_flush(PyObject *self) {
    Py_RETURN_NONE;
}

static PyObject *decompressionreader_isatty(PyObject *self) {
    Py_RETURN_FALSE;
}

/**
 * Read available input.
 *
 * Returns 0 if no data was added to input.
 * Returns 1 if new input data is available.
 * Returns -1 on error and sets a Python exception as a side-effect.
 */
int read_decompressor_input(ZstdDecompressionReader *self) {
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
        /*
         * We should only get here once since expectation is we always
         * exhaust input buffer before reading again.
         */
        assert(self->input.src == NULL);

        self->input.src = self->buffer.buf;
        self->input.size = self->buffer.len;
        self->input.pos = 0;
    }

    return 1;
}

/**
 * Decompresses available input into an output buffer.
 *
 * Returns 0 if we need more input.
 * Returns 1 if output buffer should be emitted.
 * Returns -1 on error and sets a Python exception.
 */
int decompress_input(ZstdDecompressionReader *self, ZSTD_outBuffer *output) {
    size_t zresult;

    if (self->input.pos >= self->input.size) {
        return 0;
    }

    Py_BEGIN_ALLOW_THREADS zresult =
        ZSTD_decompressStream(self->decompressor->dctx, output, &self->input);
    Py_END_ALLOW_THREADS

        /* Input exhausted. Clear our state tracking. */
        if (self->input.pos == self->input.size) {
        memset(&self->input, 0, sizeof(self->input));
        Py_CLEAR(self->readResult);

        if (self->buffer.buf) {
            self->finishedInput = 1;
        }
    }

    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "zstd decompress error: %s",
                     ZSTD_getErrorName(zresult));
        return -1;
    }

    /* We fulfilled the full read request. Signal to emit. */
    if (output->pos && output->pos == output->size) {
        return 1;
    }
    /* We're at the end of a frame and we aren't allowed to return data
       spanning frames. */
    else if (output->pos && zresult == 0 && !self->readAcrossFrames) {
        return 1;
    }

    /* There is more room in the output. Signal to collect more data. */
    return 0;
}

static PyObject *decompressionreader_read(ZstdDecompressionReader *self,
                                          PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"size", NULL};

    Py_ssize_t size = -1;
    PyObject *result = NULL;
    char *resultBuffer;
    Py_ssize_t resultSize;
    ZSTD_outBuffer output;
    int decompressResult, readResult;

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

    output.dst = resultBuffer;
    output.size = resultSize;
    output.pos = 0;

readinput:

    decompressResult = decompress_input(self, &output);

    if (-1 == decompressResult) {
        Py_XDECREF(result);
        return NULL;
    }
    else if (0 == decompressResult) {
    }
    else if (1 == decompressResult) {
        self->bytesDecompressed += output.pos;

        if (output.pos != output.size) {
            if (safe_pybytes_resize(&result, output.pos)) {
                Py_XDECREF(result);
                return NULL;
            }
        }
        return result;
    }
    else {
        assert(0);
    }

    readResult = read_decompressor_input(self);

    if (-1 == readResult) {
        Py_XDECREF(result);
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

    /* EOF */
    self->bytesDecompressed += output.pos;

    if (safe_pybytes_resize(&result, output.pos)) {
        Py_XDECREF(result);
        return NULL;
    }

    return result;
}

static PyObject *decompressionreader_read1(ZstdDecompressionReader *self,
                                           PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"size", NULL};

    Py_ssize_t size = -1;
    PyObject *result = NULL;
    char *resultBuffer;
    Py_ssize_t resultSize;
    ZSTD_outBuffer output;

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

    if (self->finishedOutput || size == 0) {
        return PyBytes_FromStringAndSize("", 0);
    }

    if (size == -1) {
        size = ZSTD_DStreamOutSize();
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
     * However, we can't satisfy this requirement with decompression due to the
     * nature of how decompression works. Our strategy is to read + decompress
     * until we get any output, at which point we return. This satisfies the
     * intent of the read1() API to limit read operations.
     */
    while (!self->finishedInput) {
        int readResult, decompressResult;

        readResult = read_decompressor_input(self);
        if (-1 == readResult) {
            Py_XDECREF(result);
            return NULL;
        }
        else if (0 == readResult || 1 == readResult) {
        }
        else {
            assert(0);
        }

        decompressResult = decompress_input(self, &output);

        if (-1 == decompressResult) {
            Py_XDECREF(result);
            return NULL;
        }
        else if (0 == decompressResult || 1 == decompressResult) {
        }
        else {
            assert(0);
        }

        if (output.pos) {
            break;
        }
    }

    self->bytesDecompressed += output.pos;
    if (safe_pybytes_resize(&result, output.pos)) {
        Py_XDECREF(result);
        return NULL;
    }

    return result;
}

static PyObject *decompressionreader_readinto(ZstdDecompressionReader *self,
                                              PyObject *args) {
    Py_buffer dest;
    ZSTD_outBuffer output;
    int decompressResult, readResult;
    PyObject *result = NULL;

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

readinput:

    decompressResult = decompress_input(self, &output);

    if (-1 == decompressResult) {
        goto finally;
    }
    else if (0 == decompressResult) {
    }
    else if (1 == decompressResult) {
        self->bytesDecompressed += output.pos;
        result = PyLong_FromSize_t(output.pos);
        goto finally;
    }
    else {
        assert(0);
    }

    readResult = read_decompressor_input(self);

    if (-1 == readResult) {
        goto finally;
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

    /* EOF */
    self->bytesDecompressed += output.pos;
    result = PyLong_FromSize_t(output.pos);

finally:
    PyBuffer_Release(&dest);

    return result;
}

static PyObject *decompressionreader_readinto1(ZstdDecompressionReader *self,
                                               PyObject *args) {
    Py_buffer dest;
    ZSTD_outBuffer output;
    PyObject *result = NULL;

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

    while (!self->finishedInput && !self->finishedOutput) {
        int decompressResult, readResult;

        readResult = read_decompressor_input(self);

        if (-1 == readResult) {
            goto finally;
        }
        else if (0 == readResult || 1 == readResult) {
        }
        else {
            assert(0);
        }

        decompressResult = decompress_input(self, &output);

        if (-1 == decompressResult) {
            goto finally;
        }
        else if (0 == decompressResult || 1 == decompressResult) {
        }
        else {
            assert(0);
        }

        if (output.pos) {
            break;
        }
    }

    self->bytesDecompressed += output.pos;
    result = PyLong_FromSize_t(output.pos);

finally:
    PyBuffer_Release(&dest);

    return result;
}

static PyObject *decompressionreader_readall(PyObject *self) {
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

static PyObject *decompressionreader_readline(PyObject *self, PyObject *args,
                                              PyObject *kwargs) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *decompressionreader_readlines(PyObject *self, PyObject *args,
                                               PyObject *kwargs) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *decompressionreader_seek(ZstdDecompressionReader *self,
                                          PyObject *args) {
    Py_ssize_t pos;
    int whence = 0;
    unsigned long long readAmount = 0;
    size_t defaultOutSize = ZSTD_DStreamOutSize();

    if (self->closed) {
        PyErr_SetString(PyExc_ValueError, "stream is closed");
        return NULL;
    }

    if (!PyArg_ParseTuple(args, "n|i:seek", &pos, &whence)) {
        return NULL;
    }

    if (whence == SEEK_SET) {
        if (pos < 0) {
            PyErr_SetString(PyExc_OSError,
                            "cannot seek to negative position with SEEK_SET");
            return NULL;
        }

        if ((unsigned long long)pos < self->bytesDecompressed) {
            PyErr_SetString(PyExc_OSError,
                            "cannot seek zstd decompression stream backwards");
            return NULL;
        }

        readAmount = pos - self->bytesDecompressed;
    }
    else if (whence == SEEK_CUR) {
        if (pos < 0) {
            PyErr_SetString(PyExc_OSError,
                            "cannot seek zstd decompression stream backwards");
            return NULL;
        }

        readAmount = pos;
    }
    else if (whence == SEEK_END) {
        /* We /could/ support this with pos==0. But let's not do that until
           someone needs it. */
        PyErr_SetString(
            PyExc_OSError,
            "zstd decompression streams cannot be seeked with SEEK_END");
        return NULL;
    }

    /* It is a bit inefficient to do this via the Python API. But since there
       is a bit of state tracking involved to read from this type, it is the
       easiest to implement. */
    while (readAmount) {
        Py_ssize_t readSize;
        PyObject *readResult = PyObject_CallMethod(
            (PyObject *)self, "read", "K",
            readAmount < defaultOutSize ? readAmount : defaultOutSize);

        if (!readResult) {
            return NULL;
        }

        readSize = PyBytes_GET_SIZE(readResult);

        Py_CLEAR(readResult);

        /* Empty read means EOF. */
        if (!readSize) {
            break;
        }

        readAmount -= readSize;
    }

    return PyLong_FromUnsignedLongLong(self->bytesDecompressed);
}

static PyObject *decompressionreader_tell(ZstdDecompressionReader *self) {
    /* TODO should this raise OSError since stream isn't seekable? */
    return PyLong_FromUnsignedLongLong(self->bytesDecompressed);
}

static PyObject *decompressionreader_write(PyObject *self, PyObject *args) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *decompressionreader_writelines(PyObject *self,
                                                PyObject *args) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *decompressionreader_iter(PyObject *self) {
    set_io_unsupported_operation();
    return NULL;
}

static PyObject *decompressionreader_iternext(PyObject *self) {
    set_io_unsupported_operation();
    return NULL;
}

static PyMethodDef decompressionreader_methods[] = {
    {"__enter__", (PyCFunction)decompressionreader_enter, METH_NOARGS,
     PyDoc_STR("Enter a compression context")},
    {"__exit__", (PyCFunction)decompressionreader_exit, METH_VARARGS,
     PyDoc_STR("Exit a compression context")},
    {"close", (PyCFunction)decompressionreader_close, METH_NOARGS,
     PyDoc_STR("Close the stream so it cannot perform any more operations")},
    {"flush", (PyCFunction)decompressionreader_flush, METH_NOARGS,
     PyDoc_STR("no-ops")},
    {"isatty", (PyCFunction)decompressionreader_isatty, METH_NOARGS,
     PyDoc_STR("Returns False")},
    {"readable", (PyCFunction)decompressionreader_readable, METH_NOARGS,
     PyDoc_STR("Returns True")},
    {"read", (PyCFunction)decompressionreader_read,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("read compressed data")},
    {"read1", (PyCFunction)decompressionreader_read1,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("read compressed data")},
    {"readinto", (PyCFunction)decompressionreader_readinto, METH_VARARGS, NULL},
    {"readinto1", (PyCFunction)decompressionreader_readinto1, METH_VARARGS,
     NULL},
    {"readall", (PyCFunction)decompressionreader_readall, METH_NOARGS,
     PyDoc_STR("Not implemented")},
    {"readline", (PyCFunction)decompressionreader_readline,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("Not implemented")},
    {"readlines", (PyCFunction)decompressionreader_readlines,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("Not implemented")},
    {"seek", (PyCFunction)decompressionreader_seek, METH_VARARGS,
     PyDoc_STR("Seek the stream")},
    {"seekable", (PyCFunction)decompressionreader_seekable, METH_NOARGS,
     PyDoc_STR("Returns False")},
    {"tell", (PyCFunction)decompressionreader_tell, METH_NOARGS,
     PyDoc_STR("Returns current number of bytes compressed")},
    {"writable", (PyCFunction)decompressionreader_writable, METH_NOARGS,
     PyDoc_STR("Returns False")},
    {"write", (PyCFunction)decompressionreader_write, METH_VARARGS,
     PyDoc_STR("unsupported operation")},
    {"writelines", (PyCFunction)decompressionreader_writelines, METH_VARARGS,
     PyDoc_STR("unsupported operation")},
    {NULL, NULL}};

static PyMemberDef decompressionreader_members[] = {
    {"closed", T_BOOL, offsetof(ZstdDecompressionReader, closed), READONLY,
     "whether stream is closed"},
    {NULL}};

PyType_Slot ZstdDecompressionReaderSlots[] = {
    {Py_tp_dealloc, decompressionreader_dealloc},
    {Py_tp_iter, decompressionreader_iter},
    {Py_tp_iternext, decompressionreader_iternext},
    {Py_tp_methods, decompressionreader_methods},
    {Py_tp_members, decompressionreader_members},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdDecompressionReaderSpec = {
    "zstd.ZstdDecompressionReader",
    sizeof(ZstdDecompressionReader),
    0,
    Py_TPFLAGS_DEFAULT,
    ZstdDecompressionReaderSlots,
};

PyTypeObject *ZstdDecompressionReaderType;

void decompressionreader_module_init(PyObject *mod) {
    /* TODO make reader a sub-class of io.RawIOBase */

    ZstdDecompressionReaderType =
        (PyTypeObject *)PyType_FromSpec(&ZstdDecompressionReaderSpec);
    if (PyType_Ready(ZstdDecompressionReaderType) < 0) {
        return;
    }

    Py_INCREF((PyObject *)ZstdDecompressionReaderType);
    PyModule_AddObject(mod, "ZstdDecompressionReader",
                       (PyObject *)ZstdDecompressionReaderType);
}
