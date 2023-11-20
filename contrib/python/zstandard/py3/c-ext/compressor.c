/**
 * Copyright (c) 2016-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

int setup_cctx(ZstdCompressor *compressor) {
    size_t zresult;

    assert(compressor);
    assert(compressor->cctx);
    assert(compressor->params);

    zresult = ZSTD_CCtx_setParametersUsingCCtxParams(compressor->cctx,
                                                     compressor->params);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "could not set compression parameters: %s",
                     ZSTD_getErrorName(zresult));
        return 1;
    }

    if (compressor->dict) {
        if (compressor->dict->cdict) {
            zresult =
                ZSTD_CCtx_refCDict(compressor->cctx, compressor->dict->cdict);
        }
        else {
            zresult = ZSTD_CCtx_loadDictionary_advanced(
                compressor->cctx, compressor->dict->dictData,
                compressor->dict->dictSize, ZSTD_dlm_byRef,
                compressor->dict->dictType);
        }
        if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "could not load compression dictionary: %s",
                         ZSTD_getErrorName(zresult));
            return 1;
        }
    }

    return 0;
}

static PyObject *frame_progression(ZSTD_CCtx *cctx) {
    PyObject *result = NULL;
    PyObject *value;
    ZSTD_frameProgression progression;

    result = PyTuple_New(3);
    if (!result) {
        return NULL;
    }

    progression = ZSTD_getFrameProgression(cctx);

    value = PyLong_FromUnsignedLongLong(progression.ingested);
    if (!value) {
        Py_DECREF(result);
        return NULL;
    }

    PyTuple_SET_ITEM(result, 0, value);

    value = PyLong_FromUnsignedLongLong(progression.consumed);
    if (!value) {
        Py_DECREF(result);
        return NULL;
    }

    PyTuple_SET_ITEM(result, 1, value);

    value = PyLong_FromUnsignedLongLong(progression.produced);
    if (!value) {
        Py_DECREF(result);
        return NULL;
    }

    PyTuple_SET_ITEM(result, 2, value);

    return result;
}

static int ZstdCompressor_init(ZstdCompressor *self, PyObject *args,
                               PyObject *kwargs) {
    static char *kwlist[] = {"level",
                             "dict_data",
                             "compression_params",
                             "write_checksum",
                             "write_content_size",
                             "write_dict_id",
                             "threads",
                             NULL};

    int level = 3;
    PyObject *dict = NULL;
    PyObject *params = NULL;
    PyObject *writeChecksum = NULL;
    PyObject *writeContentSize = NULL;
    PyObject *writeDictID = NULL;
    int threads = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iOOOOOi:ZstdCompressor",
                                     kwlist, &level, &dict, &params,
                                     &writeChecksum, &writeContentSize,
                                     &writeDictID, &threads)) {
        return -1;
    }

    if (level > ZSTD_maxCLevel()) {
        PyErr_Format(PyExc_ValueError, "level must be less than %d",
                     ZSTD_maxCLevel() + 1);
        return -1;
    }

    if (threads < 0) {
        threads = cpu_count();
    }

    if (dict) {
        if (dict == Py_None) {
            dict = NULL;
        }
        else if (!PyObject_IsInstance(dict,
                                      (PyObject *)ZstdCompressionDictType)) {
            PyErr_Format(PyExc_TypeError,
                         "dict_data must be zstd.ZstdCompressionDict");
            return -1;
        }
    }

    if (params) {
        if (params == Py_None) {
            params = NULL;
        }
        else if (!PyObject_IsInstance(
                     params, (PyObject *)ZstdCompressionParametersType)) {
            PyErr_Format(
                PyExc_TypeError,
                "compression_params must be zstd.ZstdCompressionParameters");
            return -1;
        }
    }

    if (writeChecksum == Py_None) {
        writeChecksum = NULL;
    }
    if (writeContentSize == Py_None) {
        writeContentSize = NULL;
    }
    if (writeDictID == Py_None) {
        writeDictID = NULL;
    }

    /* We create a ZSTD_CCtx for reuse among multiple operations to reduce the
       overhead of each compression operation. */
    self->cctx = ZSTD_createCCtx();
    if (!self->cctx) {
        PyErr_NoMemory();
        return -1;
    }

    /* TODO stuff the original parameters away somewhere so we can reset later.
       This will allow us to do things like automatically adjust cparams based
       on input size (assuming zstd isn't doing that internally). */

    self->params = ZSTD_createCCtxParams();
    if (!self->params) {
        PyErr_NoMemory();
        return -1;
    }

    if (params && writeChecksum) {
        PyErr_SetString(PyExc_ValueError,
                        "cannot define compression_params and write_checksum");
        return -1;
    }

    if (params && writeContentSize) {
        PyErr_SetString(
            PyExc_ValueError,
            "cannot define compression_params and write_content_size");
        return -1;
    }

    if (params && writeDictID) {
        PyErr_SetString(PyExc_ValueError,
                        "cannot define compression_params and write_dict_id");
        return -1;
    }

    if (params && threads) {
        PyErr_SetString(PyExc_ValueError,
                        "cannot define compression_params and threads");
        return -1;
    }

    if (params) {
        if (set_parameters(self->params,
                           (ZstdCompressionParametersObject *)params)) {
            return -1;
        }
    }
    else {
        if (set_parameter(self->params, ZSTD_c_compressionLevel, level)) {
            return -1;
        }

        if (set_parameter(self->params, ZSTD_c_contentSizeFlag,
                          writeContentSize ? PyObject_IsTrue(writeContentSize)
                                           : 1)) {
            return -1;
        }

        if (set_parameter(self->params, ZSTD_c_checksumFlag,
                          writeChecksum ? PyObject_IsTrue(writeChecksum) : 0)) {
            return -1;
        }

        if (set_parameter(self->params, ZSTD_c_dictIDFlag,
                          writeDictID ? PyObject_IsTrue(writeDictID) : 1)) {
            return -1;
        }

        if (threads) {
            if (set_parameter(self->params, ZSTD_c_nbWorkers, threads)) {
                return -1;
            }
        }
    }

    if (dict) {
        self->dict = (ZstdCompressionDict *)dict;
        Py_INCREF(dict);
    }

    if (setup_cctx(self)) {
        return -1;
    }

    return 0;
}

static void ZstdCompressor_dealloc(ZstdCompressor *self) {
    if (self->cctx) {
        ZSTD_freeCCtx(self->cctx);
        self->cctx = NULL;
    }

    if (self->params) {
        ZSTD_freeCCtxParams(self->params);
        self->params = NULL;
    }

    Py_XDECREF(self->dict);
    PyObject_Del(self);
}

static PyObject *ZstdCompressor_memory_size(ZstdCompressor *self) {
    if (self->cctx) {
        return PyLong_FromSize_t(ZSTD_sizeof_CCtx(self->cctx));
    }
    else {
        PyErr_SetString(
            ZstdError, "no compressor context found; this should never happen");
        return NULL;
    }
}

static PyObject *ZstdCompressor_frame_progression(ZstdCompressor *self) {
    return frame_progression(self->cctx);
}

static PyObject *ZstdCompressor_copy_stream(ZstdCompressor *self,
                                            PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"ifh",       "ofh",        "size",
                             "read_size", "write_size", NULL};

    PyObject *source;
    PyObject *dest;
    unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
    size_t inSize = ZSTD_CStreamInSize();
    size_t outSize = ZSTD_CStreamOutSize();
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;
    Py_ssize_t totalRead = 0;
    Py_ssize_t totalWrite = 0;
    char *readBuffer;
    Py_ssize_t readSize;
    PyObject *readResult = NULL;
    PyObject *res = NULL;
    size_t zresult;
    PyObject *writeResult;
    PyObject *totalReadPy;
    PyObject *totalWritePy;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Kkk:copy_stream", kwlist,
                                     &source, &dest, &sourceSize, &inSize,
                                     &outSize)) {
        return NULL;
    }

    if (!PyObject_HasAttrString(source, "read")) {
        PyErr_SetString(PyExc_ValueError,
                        "first argument must have a read() method");
        return NULL;
    }

    if (!PyObject_HasAttrString(dest, "write")) {
        PyErr_SetString(PyExc_ValueError,
                        "second argument must have a write() method");
        return NULL;
    }

    ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

    zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, sourceSize);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error setting source size: %s",
                     ZSTD_getErrorName(zresult));
        return NULL;
    }

    /* Prevent free on uninitialized memory in finally. */
    output.dst = PyMem_Malloc(outSize);
    if (!output.dst) {
        PyErr_NoMemory();
        res = NULL;
        goto finally;
    }
    output.size = outSize;
    output.pos = 0;

    input.src = NULL;
    input.size = 0;
    input.pos = 0;

    while (1) {
        /* Try to read from source stream. */
        readResult = PyObject_CallMethod(source, "read", "n", inSize);
        if (!readResult) {
            goto finally;
        }

        PyBytes_AsStringAndSize(readResult, &readBuffer, &readSize);

        /* If no data was read, we're at EOF. */
        if (0 == readSize) {
            break;
        }

        totalRead += readSize;

        /* Send data to compressor */
        input.src = readBuffer;
        input.size = readSize;
        input.pos = 0;

        while (input.pos < input.size) {
            Py_BEGIN_ALLOW_THREADS zresult = ZSTD_compressStream2(
                self->cctx, &output, &input, ZSTD_e_continue);
            Py_END_ALLOW_THREADS

                if (ZSTD_isError(zresult)) {
                res = NULL;
                PyErr_Format(ZstdError, "zstd compress error: %s",
                             ZSTD_getErrorName(zresult));
                goto finally;
            }

            if (output.pos) {
                writeResult = PyObject_CallMethod(dest, "write", "y#",
                                                  output.dst, output.pos);
                if (NULL == writeResult) {
                    res = NULL;
                    goto finally;
                }
                Py_XDECREF(writeResult);
                totalWrite += output.pos;
                output.pos = 0;
            }
        }

        Py_CLEAR(readResult);
    }

    /* We've finished reading. Now flush the compressor stream. */
    assert(input.pos == input.size);

    while (1) {
        Py_BEGIN_ALLOW_THREADS zresult =
            ZSTD_compressStream2(self->cctx, &output, &input, ZSTD_e_end);
        Py_END_ALLOW_THREADS

            if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "error ending compression stream: %s",
                         ZSTD_getErrorName(zresult));
            res = NULL;
            goto finally;
        }

        if (output.pos) {
            writeResult = PyObject_CallMethod(dest, "write", "y#", output.dst,
                                              output.pos);
            if (NULL == writeResult) {
                res = NULL;
                goto finally;
            }
            totalWrite += output.pos;
            Py_XDECREF(writeResult);
            output.pos = 0;
        }

        if (!zresult) {
            break;
        }
    }

    totalReadPy = PyLong_FromSsize_t(totalRead);
    totalWritePy = PyLong_FromSsize_t(totalWrite);
    res = PyTuple_Pack(2, totalReadPy, totalWritePy);
    Py_DECREF(totalReadPy);
    Py_DECREF(totalWritePy);

finally:
    if (output.dst) {
        PyMem_Free(output.dst);
    }

    Py_XDECREF(readResult);

    return res;
}

static ZstdCompressionReader *ZstdCompressor_stream_reader(ZstdCompressor *self,
                                                           PyObject *args,
                                                           PyObject *kwargs) {
    static char *kwlist[] = {"source", "size", "read_size", "closefd", NULL};

    PyObject *source;
    unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
    size_t readSize = ZSTD_CStreamInSize();
    PyObject *closefd = NULL;
    ZstdCompressionReader *result = NULL;
    size_t zresult;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|KkO:stream_reader",
                                     kwlist, &source, &sourceSize, &readSize,
                                     &closefd)) {
        return NULL;
    }

    result = (ZstdCompressionReader *)PyObject_CallObject(
        (PyObject *)ZstdCompressionReaderType, NULL);
    if (!result) {
        return NULL;
    }

    result->entered = 0;
    result->closed = 0;

    if (PyObject_HasAttrString(source, "read")) {
        result->reader = source;
        Py_INCREF(source);
        result->readSize = readSize;
    }
    else if (1 == PyObject_CheckBuffer(source)) {
        if (0 != PyObject_GetBuffer(source, &result->buffer, PyBUF_CONTIG_RO)) {
            goto except;
        }

        assert(result->buffer.len >= 0);

        sourceSize = result->buffer.len;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "must pass an object with a read() method or that "
                        "conforms to the buffer protocol");
        goto except;
    }

    result->closefd = closefd ? PyObject_IsTrue(closefd) : 1;

    ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

    zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, sourceSize);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error setting source source: %s",
                     ZSTD_getErrorName(zresult));
        goto except;
    }

    result->compressor = self;
    Py_INCREF(self);

    return result;

except:
    Py_CLEAR(result);

    return NULL;
}

static PyObject *ZstdCompressor_compress(ZstdCompressor *self, PyObject *args,
                                         PyObject *kwargs) {
    static char *kwlist[] = {"data", NULL};

    Py_buffer source;
    size_t destSize;
    PyObject *output = NULL;
    size_t zresult;
    ZSTD_outBuffer outBuffer;
    ZSTD_inBuffer inBuffer;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*|O:compress", kwlist,
                                     &source)) {
        return NULL;
    }

    ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

    destSize = ZSTD_compressBound(source.len);
    output = PyBytes_FromStringAndSize(NULL, destSize);
    if (!output) {
        goto finally;
    }

    zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, source.len);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error setting source size: %s",
                     ZSTD_getErrorName(zresult));
        Py_CLEAR(output);
        goto finally;
    }

    inBuffer.src = source.buf;
    inBuffer.size = source.len;
    inBuffer.pos = 0;

    outBuffer.dst = PyBytes_AsString(output);
    outBuffer.size = destSize;
    outBuffer.pos = 0;

    Py_BEGIN_ALLOW_THREADS
        /* By avoiding ZSTD_compress(), we don't necessarily write out content
                size. This means the argument to ZstdCompressor to control frame
                parameters is honored. */
        zresult =
            ZSTD_compressStream2(self->cctx, &outBuffer, &inBuffer, ZSTD_e_end);
    Py_END_ALLOW_THREADS

        if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "cannot compress: %s",
                     ZSTD_getErrorName(zresult));
        Py_CLEAR(output);
        goto finally;
    }
    else if (zresult) {
        PyErr_SetString(ZstdError, "unexpected partial frame flush");
        Py_CLEAR(output);
        goto finally;
    }

    Py_SET_SIZE(output, outBuffer.pos);

finally:
    PyBuffer_Release(&source);
    return output;
}

static ZstdCompressionObj *ZstdCompressor_compressobj(ZstdCompressor *self,
                                                      PyObject *args,
                                                      PyObject *kwargs) {
    static char *kwlist[] = {"size", NULL};

    unsigned long long inSize = ZSTD_CONTENTSIZE_UNKNOWN;
    size_t outSize = ZSTD_CStreamOutSize();
    ZstdCompressionObj *result = NULL;
    size_t zresult;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|K:compressobj", kwlist,
                                     &inSize)) {
        return NULL;
    }

    ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

    zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, inSize);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error setting source size: %s",
                     ZSTD_getErrorName(zresult));
        return NULL;
    }

    result = (ZstdCompressionObj *)PyObject_CallObject(
        (PyObject *)ZstdCompressionObjType, NULL);
    if (!result) {
        return NULL;
    }

    result->output.dst = PyMem_Malloc(outSize);
    if (!result->output.dst) {
        PyErr_NoMemory();
        Py_DECREF(result);
        return NULL;
    }
    result->output.size = outSize;
    result->compressor = self;
    Py_INCREF(result->compressor);

    return result;
}

static ZstdCompressorIterator *ZstdCompressor_read_to_iter(ZstdCompressor *self,
                                                           PyObject *args,
                                                           PyObject *kwargs) {
    static char *kwlist[] = {"reader", "size", "read_size", "write_size", NULL};

    PyObject *reader;
    unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
    size_t inSize = ZSTD_CStreamInSize();
    size_t outSize = ZSTD_CStreamOutSize();
    ZstdCompressorIterator *result;
    size_t zresult;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Kkk:read_to_iter", kwlist,
                                     &reader, &sourceSize, &inSize, &outSize)) {
        return NULL;
    }

    result = (ZstdCompressorIterator *)PyObject_CallObject(
        (PyObject *)ZstdCompressorIteratorType, NULL);
    if (!result) {
        return NULL;
    }
    if (PyObject_HasAttrString(reader, "read")) {
        result->reader = reader;
        Py_INCREF(result->reader);
    }
    else if (1 == PyObject_CheckBuffer(reader)) {
        if (0 != PyObject_GetBuffer(reader, &result->buffer, PyBUF_CONTIG_RO)) {
            goto except;
        }

        sourceSize = result->buffer.len;
    }
    else {
        PyErr_SetString(PyExc_ValueError,
                        "must pass an object with a read() method or conforms "
                        "to buffer protocol");
        goto except;
    }

    ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

    zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, sourceSize);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error setting source size: %s",
                     ZSTD_getErrorName(zresult));
        return NULL;
    }

    result->compressor = self;
    Py_INCREF(result->compressor);

    result->inSize = inSize;
    result->outSize = outSize;

    result->output.dst = PyMem_Malloc(outSize);
    if (!result->output.dst) {
        PyErr_NoMemory();
        goto except;
    }
    result->output.size = outSize;

    goto finally;

except:
    Py_CLEAR(result);

finally:
    return result;
}

static ZstdCompressionWriter *ZstdCompressor_stream_writer(ZstdCompressor *self,
                                                           PyObject *args,
                                                           PyObject *kwargs) {
    static char *kwlist[] = {
        "writer", "size", "write_size", "write_return_read", "closefd", NULL};

    PyObject *writer;
    ZstdCompressionWriter *result;
    size_t zresult;
    unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
    size_t outSize = ZSTD_CStreamOutSize();
    PyObject *writeReturnRead = NULL;
    PyObject *closefd = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|KkOO:stream_writer",
                                     kwlist, &writer, &sourceSize, &outSize,
                                     &writeReturnRead, &closefd)) {
        return NULL;
    }

    if (!PyObject_HasAttrString(writer, "write")) {
        PyErr_SetString(PyExc_ValueError,
                        "must pass an object with a write() method");
        return NULL;
    }

    ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

    zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, sourceSize);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error setting source size: %s",
                     ZSTD_getErrorName(zresult));
        return NULL;
    }

    result = (ZstdCompressionWriter *)PyObject_CallObject(
        (PyObject *)ZstdCompressionWriterType, NULL);
    if (!result) {
        return NULL;
    }

    result->entered = 0;
    result->closing = 0;
    result->closed = 0;

    result->output.dst = PyMem_Malloc(outSize);
    if (!result->output.dst) {
        Py_DECREF(result);
        return (ZstdCompressionWriter *)PyErr_NoMemory();
    }

    result->output.pos = 0;
    result->output.size = outSize;

    result->compressor = self;
    Py_INCREF(result->compressor);

    result->writer = writer;
    Py_INCREF(result->writer);

    result->outSize = outSize;
    result->bytesCompressed = 0;
    result->writeReturnRead =
        writeReturnRead ? PyObject_IsTrue(writeReturnRead) : 1;
    result->closefd = closefd ? PyObject_IsTrue(closefd) : 1;

    return result;
}

static ZstdCompressionChunker *
ZstdCompressor_chunker(ZstdCompressor *self, PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"size", "chunk_size", NULL};

    unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
    size_t chunkSize = ZSTD_CStreamOutSize();
    ZstdCompressionChunker *chunker;
    size_t zresult;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|Kk:chunker", kwlist,
                                     &sourceSize, &chunkSize)) {
        return NULL;
    }

    ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

    zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, sourceSize);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "error setting source size: %s",
                     ZSTD_getErrorName(zresult));
        return NULL;
    }

    chunker = (ZstdCompressionChunker *)PyObject_CallObject(
        (PyObject *)ZstdCompressionChunkerType, NULL);
    if (!chunker) {
        return NULL;
    }

    chunker->output.dst = PyMem_Malloc(chunkSize);
    if (!chunker->output.dst) {
        PyErr_NoMemory();
        Py_DECREF(chunker);
        return NULL;
    }
    chunker->output.size = chunkSize;
    chunker->output.pos = 0;

    chunker->compressor = self;
    Py_INCREF(chunker->compressor);

    chunker->chunkSize = chunkSize;

    return chunker;
}

typedef struct {
    void *sourceData;
    size_t sourceSize;
} DataSource;

typedef struct {
    DataSource *sources;
    Py_ssize_t sourcesSize;
    unsigned long long totalSourceSize;
} DataSources;

typedef struct {
    void *dest;
    Py_ssize_t destSize;
    BufferSegment *segments;
    Py_ssize_t segmentsSize;
} CompressorDestBuffer;

typedef enum {
    CompressorWorkerError_none = 0,
    CompressorWorkerError_zstd = 1,
    CompressorWorkerError_no_memory = 2,
    CompressorWorkerError_nospace = 3,
} CompressorWorkerError;

/**
 * Holds state for an individual worker performing multi_compress_to_buffer
 * work.
 */
typedef struct {
    /* Used for compression. */
    ZSTD_CCtx *cctx;

    /* What to compress. */
    DataSource *sources;
    Py_ssize_t sourcesSize;
    Py_ssize_t startOffset;
    Py_ssize_t endOffset;
    unsigned long long totalSourceSize;

    /* Result storage. */
    CompressorDestBuffer *destBuffers;
    Py_ssize_t destCount;

    /* Error tracking. */
    CompressorWorkerError error;
    size_t zresult;
    Py_ssize_t errorOffset;
} CompressorWorkerState;

#ifdef HAVE_ZSTD_POOL_APIS
static void compress_worker(CompressorWorkerState *state) {
    Py_ssize_t inputOffset = state->startOffset;
    Py_ssize_t remainingItems = state->endOffset - state->startOffset + 1;
    Py_ssize_t currentBufferStartOffset = state->startOffset;
    size_t zresult;
    void *newDest;
    size_t allocationSize;
    size_t boundSize;
    Py_ssize_t destOffset = 0;
    DataSource *sources = state->sources;
    CompressorDestBuffer *destBuffer;

    assert(!state->destBuffers);
    assert(0 == state->destCount);

    /*
     * The total size of the compressed data is unknown until we actually
     * compress data. That means we can't pre-allocate the exact size we need.
     *
     * There is a cost to every allocation and reallocation. So, it is in our
     * interest to minimize the number of allocations.
     *
     * There is also a cost to too few allocations. If allocations are too
     * large they may fail. If buffers are shared and all inputs become
     * irrelevant at different lifetimes, then a reference to one segment
     * in the buffer will keep the entire buffer alive. This leads to excessive
     * memory usage.
     *
     * Our current strategy is to assume a compression ratio of 16:1 and
     * allocate buffers of that size, rounded up to the nearest power of 2
     * (because computers like round numbers). That ratio is greater than what
     * most inputs achieve. This is by design: we don't want to over-allocate.
     * But we don't want to under-allocate and lead to too many buffers either.
     */

    state->destCount = 1;

    state->destBuffers = calloc(1, sizeof(CompressorDestBuffer));
    if (NULL == state->destBuffers) {
        state->error = CompressorWorkerError_no_memory;
        return;
    }

    destBuffer = &state->destBuffers[state->destCount - 1];

    /*
     * Rather than track bounds and grow the segments buffer, allocate space
     * to hold remaining items then truncate when we're done with it.
     */
    destBuffer->segments = calloc(remainingItems, sizeof(BufferSegment));
    if (NULL == destBuffer->segments) {
        state->error = CompressorWorkerError_no_memory;
        return;
    }

    destBuffer->segmentsSize = remainingItems;

    assert(state->totalSourceSize <= SIZE_MAX);
    allocationSize = roundpow2((size_t)state->totalSourceSize >> 4);

    /* If the maximum size of the output is larger than that, round up. */
    boundSize = ZSTD_compressBound(sources[inputOffset].sourceSize);

    if (boundSize > allocationSize) {
        allocationSize = roundpow2(boundSize);
    }

    destBuffer->dest = malloc(allocationSize);
    if (NULL == destBuffer->dest) {
        state->error = CompressorWorkerError_no_memory;
        return;
    }

    destBuffer->destSize = allocationSize;

    for (inputOffset = state->startOffset; inputOffset <= state->endOffset;
         inputOffset++) {
        void *source = sources[inputOffset].sourceData;
        size_t sourceSize = sources[inputOffset].sourceSize;
        size_t destAvailable;
        void *dest;
        ZSTD_outBuffer opOutBuffer;
        ZSTD_inBuffer opInBuffer;

        destAvailable = destBuffer->destSize - destOffset;
        boundSize = ZSTD_compressBound(sourceSize);

        /*
         * Not enough space in current buffer to hold largest compressed output.
         * So allocate and switch to a new output buffer.
         */
        if (boundSize > destAvailable) {
            /*
             * The downsizing of the existing buffer is optional. It should be
             * cheap (unlike growing). So we just do it.
             */
            if (destAvailable) {
                newDest = realloc(destBuffer->dest, destOffset);
                if (NULL == newDest) {
                    state->error = CompressorWorkerError_no_memory;
                    return;
                }

                destBuffer->dest = newDest;
                destBuffer->destSize = destOffset;
            }

            /* Truncate segments buffer. */
            newDest = realloc(destBuffer->segments,
                              (inputOffset - currentBufferStartOffset + 1) *
                                  sizeof(BufferSegment));
            if (NULL == newDest) {
                state->error = CompressorWorkerError_no_memory;
                return;
            }

            destBuffer->segments = newDest;
            destBuffer->segmentsSize = inputOffset - currentBufferStartOffset;

            /* Grow space for new struct. */
            /* TODO consider over-allocating so we don't do this every time. */
            newDest =
                realloc(state->destBuffers,
                        (state->destCount + 1) * sizeof(CompressorDestBuffer));
            if (NULL == newDest) {
                state->error = CompressorWorkerError_no_memory;
                return;
            }

            state->destBuffers = newDest;
            state->destCount++;

            destBuffer = &state->destBuffers[state->destCount - 1];

            /* Don't take any chances with non-NULL pointers. */
            memset(destBuffer, 0, sizeof(CompressorDestBuffer));

            /**
             * We could dynamically update allocation size based on work done so
             * far. For now, keep is simple.
             */
            assert(state->totalSourceSize <= SIZE_MAX);
            allocationSize = roundpow2((size_t)state->totalSourceSize >> 4);

            if (boundSize > allocationSize) {
                allocationSize = roundpow2(boundSize);
            }

            destBuffer->dest = malloc(allocationSize);
            if (NULL == destBuffer->dest) {
                state->error = CompressorWorkerError_no_memory;
                return;
            }

            destBuffer->destSize = allocationSize;
            destAvailable = allocationSize;
            destOffset = 0;

            destBuffer->segments =
                calloc(remainingItems, sizeof(BufferSegment));
            if (NULL == destBuffer->segments) {
                state->error = CompressorWorkerError_no_memory;
                return;
            }

            destBuffer->segmentsSize = remainingItems;
            currentBufferStartOffset = inputOffset;
        }

        dest = (char *)destBuffer->dest + destOffset;

        opInBuffer.src = source;
        opInBuffer.size = sourceSize;
        opInBuffer.pos = 0;

        opOutBuffer.dst = dest;
        opOutBuffer.size = destAvailable;
        opOutBuffer.pos = 0;

        zresult = ZSTD_CCtx_setPledgedSrcSize(state->cctx, sourceSize);
        if (ZSTD_isError(zresult)) {
            state->error = CompressorWorkerError_zstd;
            state->zresult = zresult;
            state->errorOffset = inputOffset;
            break;
        }

        zresult = ZSTD_compressStream2(state->cctx, &opOutBuffer, &opInBuffer,
                                       ZSTD_e_end);
        if (ZSTD_isError(zresult)) {
            state->error = CompressorWorkerError_zstd;
            state->zresult = zresult;
            state->errorOffset = inputOffset;
            break;
        }
        else if (zresult) {
            state->error = CompressorWorkerError_nospace;
            state->errorOffset = inputOffset;
            break;
        }

        destBuffer->segments[inputOffset - currentBufferStartOffset].offset =
            destOffset;
        destBuffer->segments[inputOffset - currentBufferStartOffset].length =
            opOutBuffer.pos;

        destOffset += opOutBuffer.pos;
        remainingItems--;
    }

    if (destBuffer->destSize > destOffset) {
        newDest = realloc(destBuffer->dest, destOffset);
        if (NULL == newDest) {
            state->error = CompressorWorkerError_no_memory;
            return;
        }

        destBuffer->dest = newDest;
        destBuffer->destSize = destOffset;
    }
}
#endif

/* We can only use the pool.h APIs if we provide the full library,
   as these are private APIs. */
#ifdef HAVE_ZSTD_POOL_APIS

ZstdBufferWithSegmentsCollection *
compress_from_datasources(ZstdCompressor *compressor, DataSources *sources,
                          Py_ssize_t threadCount) {
    unsigned long long bytesPerWorker;
    POOL_ctx *pool = NULL;
    CompressorWorkerState *workerStates = NULL;
    Py_ssize_t i;
    unsigned long long workerBytes = 0;
    Py_ssize_t workerStartOffset = 0;
    Py_ssize_t currentThread = 0;
    int errored = 0;
    Py_ssize_t segmentsCount = 0;
    Py_ssize_t segmentIndex;
    PyObject *segmentsArg = NULL;
    ZstdBufferWithSegments *buffer;
    ZstdBufferWithSegmentsCollection *result = NULL;

    assert(sources->sourcesSize > 0);
    assert(sources->totalSourceSize > 0);
    assert(threadCount >= 1);

    /* More threads than inputs makes no sense. */
    threadCount =
        sources->sourcesSize < threadCount ? sources->sourcesSize : threadCount;

    /* TODO lower thread count when input size is too small and threads would
    add overhead. */

    workerStates = PyMem_Malloc(threadCount * sizeof(CompressorWorkerState));
    if (NULL == workerStates) {
        PyErr_NoMemory();
        goto finally;
    }

    memset(workerStates, 0, threadCount * sizeof(CompressorWorkerState));

    if (threadCount > 1) {
        pool = POOL_create(threadCount, 1);
        if (NULL == pool) {
            PyErr_SetString(ZstdError, "could not initialize zstd thread pool");
            goto finally;
        }
    }

    bytesPerWorker = sources->totalSourceSize / threadCount;

    for (i = 0; i < threadCount; i++) {
        size_t zresult;

        workerStates[i].cctx = ZSTD_createCCtx();
        if (!workerStates[i].cctx) {
            PyErr_NoMemory();
            goto finally;
        }

        zresult = ZSTD_CCtx_setParametersUsingCCtxParams(workerStates[i].cctx,
                                                         compressor->params);
        if (ZSTD_isError(zresult)) {
            PyErr_Format(ZstdError, "could not set compression parameters: %s",
                         ZSTD_getErrorName(zresult));
            goto finally;
        }

        if (compressor->dict) {
            if (compressor->dict->cdict) {
                zresult = ZSTD_CCtx_refCDict(workerStates[i].cctx,
                                             compressor->dict->cdict);
            }
            else {
                zresult = ZSTD_CCtx_loadDictionary_advanced(
                    workerStates[i].cctx, compressor->dict->dictData,
                    compressor->dict->dictSize, ZSTD_dlm_byRef,
                    compressor->dict->dictType);
            }

            if (ZSTD_isError(zresult)) {
                PyErr_Format(ZstdError,
                             "could not load compression dictionary: %s",
                             ZSTD_getErrorName(zresult));
                goto finally;
            }
        }

        workerStates[i].sources = sources->sources;
        workerStates[i].sourcesSize = sources->sourcesSize;
    }

    Py_BEGIN_ALLOW_THREADS for (i = 0; i < sources->sourcesSize; i++) {
        workerBytes += sources->sources[i].sourceSize;

        /*
         * The last worker/thread needs to handle all remaining work. Don't
         * trigger it prematurely. Defer to the block outside of the loop
         * to run the last worker/thread. But do still process this loop
         * so workerBytes is correct.
         */
        if (currentThread == threadCount - 1) {
            continue;
        }

        if (workerBytes >= bytesPerWorker) {
            assert(currentThread < threadCount);
            workerStates[currentThread].totalSourceSize = workerBytes;
            workerStates[currentThread].startOffset = workerStartOffset;
            workerStates[currentThread].endOffset = i;

            if (threadCount > 1) {
                POOL_add(pool, (POOL_function)compress_worker,
                         &workerStates[currentThread]);
            }
            else {
                compress_worker(&workerStates[currentThread]);
            }

            currentThread++;
            workerStartOffset = i + 1;
            workerBytes = 0;
        }
    }

    if (workerBytes) {
        assert(currentThread < threadCount);
        workerStates[currentThread].totalSourceSize = workerBytes;
        workerStates[currentThread].startOffset = workerStartOffset;
        workerStates[currentThread].endOffset = sources->sourcesSize - 1;

        if (threadCount > 1) {
            POOL_add(pool, (POOL_function)compress_worker,
                     &workerStates[currentThread]);
        }
        else {
            compress_worker(&workerStates[currentThread]);
        }
    }

    if (threadCount > 1) {
        POOL_free(pool);
        pool = NULL;
    }

    Py_END_ALLOW_THREADS

        for (i = 0; i < threadCount; i++) {
        switch (workerStates[i].error) {
        case CompressorWorkerError_no_memory:
            PyErr_NoMemory();
            errored = 1;
            break;

        case CompressorWorkerError_zstd:
            PyErr_Format(ZstdError, "error compressing item %zd: %s",
                         workerStates[i].errorOffset,
                         ZSTD_getErrorName(workerStates[i].zresult));
            errored = 1;
            break;

        case CompressorWorkerError_nospace:
            PyErr_Format(
                ZstdError,
                "error compressing item %zd: not enough space in output",
                workerStates[i].errorOffset);
            errored = 1;
            break;

        default:;
        }

        if (errored) {
            break;
        }
    }

    if (errored) {
        goto finally;
    }

    segmentsCount = 0;
    for (i = 0; i < threadCount; i++) {
        CompressorWorkerState *state = &workerStates[i];
        segmentsCount += state->destCount;
    }

    segmentsArg = PyTuple_New(segmentsCount);
    if (NULL == segmentsArg) {
        goto finally;
    }

    segmentIndex = 0;

    for (i = 0; i < threadCount; i++) {
        Py_ssize_t j;
        CompressorWorkerState *state = &workerStates[i];

        for (j = 0; j < state->destCount; j++) {
            CompressorDestBuffer *destBuffer = &state->destBuffers[j];
            buffer = BufferWithSegments_FromMemory(
                destBuffer->dest, destBuffer->destSize, destBuffer->segments,
                destBuffer->segmentsSize);

            if (NULL == buffer) {
                goto finally;
            }

            /* Tell instance to use free() instsead of PyMem_Free(). */
            buffer->useFree = 1;

            /*
             * BufferWithSegments_FromMemory takes ownership of the backing
             * memory. Unset it here so it doesn't get freed below.
             */
            destBuffer->dest = NULL;
            destBuffer->segments = NULL;

            PyTuple_SET_ITEM(segmentsArg, segmentIndex++, (PyObject *)buffer);
        }
    }

    result = (ZstdBufferWithSegmentsCollection *)PyObject_CallObject(
        (PyObject *)ZstdBufferWithSegmentsCollectionType, segmentsArg);

finally:
    Py_CLEAR(segmentsArg);

    if (pool) {
        POOL_free(pool);
    }

    if (workerStates) {
        Py_ssize_t j;

        for (i = 0; i < threadCount; i++) {
            CompressorWorkerState state = workerStates[i];

            if (state.cctx) {
                ZSTD_freeCCtx(state.cctx);
            }

            /* malloc() is used in worker thread. */

            for (j = 0; j < state.destCount; j++) {
                if (state.destBuffers) {
                    free(state.destBuffers[j].dest);
                    free(state.destBuffers[j].segments);
                }
            }

            free(state.destBuffers);
        }

        PyMem_Free(workerStates);
    }

    return result;
}
#endif

#ifdef HAVE_ZSTD_POOL_APIS
static ZstdBufferWithSegmentsCollection *
ZstdCompressor_multi_compress_to_buffer(ZstdCompressor *self, PyObject *args,
                                        PyObject *kwargs) {
    static char *kwlist[] = {"data", "threads", NULL};

    PyObject *data;
    int threads = 0;
    Py_buffer *dataBuffers = NULL;
    DataSources sources;
    Py_ssize_t i;
    Py_ssize_t sourceCount = 0;
    ZstdBufferWithSegmentsCollection *result = NULL;

    memset(&sources, 0, sizeof(sources));

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                     "O|i:multi_compress_to_buffer", kwlist,
                                     &data, &threads)) {
        return NULL;
    }

    if (threads < 0) {
        threads = cpu_count();
    }

    if (threads < 2) {
        threads = 1;
    }

    if (PyObject_TypeCheck(data, ZstdBufferWithSegmentsType)) {
        ZstdBufferWithSegments *buffer = (ZstdBufferWithSegments *)data;

        sources.sources =
            PyMem_Malloc(buffer->segmentCount * sizeof(DataSource));
        if (NULL == sources.sources) {
            PyErr_NoMemory();
            goto finally;
        }

        for (i = 0; i < buffer->segmentCount; i++) {
            if (buffer->segments[i].length > SIZE_MAX) {
                PyErr_Format(
                    PyExc_ValueError,
                    "buffer segment %zd is too large for this platform", i);
                goto finally;
            }

            sources.sources[i].sourceData =
                (char *)buffer->data + buffer->segments[i].offset;
            sources.sources[i].sourceSize = (size_t)buffer->segments[i].length;
            sources.totalSourceSize += buffer->segments[i].length;
        }

        sources.sourcesSize = buffer->segmentCount;
    }
    else if (PyObject_TypeCheck(data, ZstdBufferWithSegmentsCollectionType)) {
        Py_ssize_t j;
        Py_ssize_t offset = 0;
        ZstdBufferWithSegments *buffer;
        ZstdBufferWithSegmentsCollection *collection =
            (ZstdBufferWithSegmentsCollection *)data;

        sourceCount = BufferWithSegmentsCollection_length(collection);

        sources.sources = PyMem_Malloc(sourceCount * sizeof(DataSource));
        if (NULL == sources.sources) {
            PyErr_NoMemory();
            goto finally;
        }

        for (i = 0; i < collection->bufferCount; i++) {
            buffer = collection->buffers[i];

            for (j = 0; j < buffer->segmentCount; j++) {
                if (buffer->segments[j].length > SIZE_MAX) {
                    PyErr_Format(PyExc_ValueError,
                                 "buffer segment %zd in buffer %zd is too "
                                 "large for this platform",
                                 j, i);
                    goto finally;
                }

                sources.sources[offset].sourceData =
                    (char *)buffer->data + buffer->segments[j].offset;
                sources.sources[offset].sourceSize =
                    (size_t)buffer->segments[j].length;
                sources.totalSourceSize += buffer->segments[j].length;

                offset++;
            }
        }

        sources.sourcesSize = sourceCount;
    }
    else if (PyList_Check(data)) {
        sourceCount = PyList_GET_SIZE(data);

        sources.sources = PyMem_Malloc(sourceCount * sizeof(DataSource));
        if (NULL == sources.sources) {
            PyErr_NoMemory();
            goto finally;
        }

        dataBuffers = PyMem_Malloc(sourceCount * sizeof(Py_buffer));
        if (NULL == dataBuffers) {
            PyErr_NoMemory();
            goto finally;
        }

        memset(dataBuffers, 0, sourceCount * sizeof(Py_buffer));

        for (i = 0; i < sourceCount; i++) {
            if (0 != PyObject_GetBuffer(PyList_GET_ITEM(data, i),
                                        &dataBuffers[i], PyBUF_CONTIG_RO)) {
                PyErr_Clear();
                PyErr_Format(PyExc_TypeError,
                             "item %zd not a bytes like object", i);
                goto finally;
            }

            sources.sources[i].sourceData = dataBuffers[i].buf;
            sources.sources[i].sourceSize = dataBuffers[i].len;
            sources.totalSourceSize += dataBuffers[i].len;
        }

        sources.sourcesSize = sourceCount;
    }
    else {
        PyErr_SetString(PyExc_TypeError,
                        "argument must be list of BufferWithSegments");
        goto finally;
    }

    if (0 == sources.sourcesSize) {
        PyErr_SetString(PyExc_ValueError, "no source elements found");
        goto finally;
    }

    if (0 == sources.totalSourceSize) {
        PyErr_SetString(PyExc_ValueError, "source elements are empty");
        goto finally;
    }

    if (sources.totalSourceSize > SIZE_MAX) {
        PyErr_SetString(PyExc_ValueError,
                        "sources are too large for this platform");
        goto finally;
    }

    result = compress_from_datasources(self, &sources, threads);

finally:
    PyMem_Free(sources.sources);

    if (dataBuffers) {
        for (i = 0; i < sourceCount; i++) {
            PyBuffer_Release(&dataBuffers[i]);
        }

        PyMem_Free(dataBuffers);
    }

    return result;
}
#endif

static PyMethodDef ZstdCompressor_methods[] = {
    {"chunker", (PyCFunction)ZstdCompressor_chunker,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"compress", (PyCFunction)ZstdCompressor_compress,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"compressobj", (PyCFunction)ZstdCompressor_compressobj,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"copy_stream", (PyCFunction)ZstdCompressor_copy_stream,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"stream_reader", (PyCFunction)ZstdCompressor_stream_reader,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"stream_writer", (PyCFunction)ZstdCompressor_stream_writer,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"read_to_iter", (PyCFunction)ZstdCompressor_read_to_iter,
     METH_VARARGS | METH_KEYWORDS, NULL},
#ifdef HAVE_ZSTD_POOL_APIS
    {"multi_compress_to_buffer",
     (PyCFunction)ZstdCompressor_multi_compress_to_buffer,
     METH_VARARGS | METH_KEYWORDS, NULL},
#endif
    {"memory_size", (PyCFunction)ZstdCompressor_memory_size, METH_NOARGS, NULL},
    {"frame_progression", (PyCFunction)ZstdCompressor_frame_progression,
     METH_NOARGS, NULL},
    {NULL, NULL}};

PyType_Slot ZstdCompressorSlots[] = {
    {Py_tp_dealloc, ZstdCompressor_dealloc},
    {Py_tp_methods, ZstdCompressor_methods},
    {Py_tp_init, ZstdCompressor_init},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdCompressorSpec = {
    "zstd.ZstdCompressor",
    sizeof(ZstdCompressor),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    ZstdCompressorSlots,
};

PyTypeObject *ZstdCompressorType;

void compressor_module_init(PyObject *mod) {
    ZstdCompressorType = (PyTypeObject *)PyType_FromSpec(&ZstdCompressorSpec);
    if (PyType_Ready(ZstdCompressorType) < 0) {
        return;
    }

    Py_INCREF((PyObject *)ZstdCompressorType);
    PyModule_AddObject(mod, "ZstdCompressor", (PyObject *)ZstdCompressorType);
}
