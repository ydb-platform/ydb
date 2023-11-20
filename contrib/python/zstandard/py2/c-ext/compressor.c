/**
* Copyright (c) 2016-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"
#include "pool.h"

extern PyObject* ZstdError;

int setup_cctx(ZstdCompressor* compressor) {
	size_t zresult;

	assert(compressor);
	assert(compressor->cctx);
	assert(compressor->params);

	zresult = ZSTD_CCtx_setParametersUsingCCtxParams(compressor->cctx, compressor->params);
	if (ZSTD_isError(zresult)) {
		PyErr_Format(ZstdError, "could not set compression parameters: %s",
			ZSTD_getErrorName(zresult));
		return 1;
	}

	if (compressor->dict) {
		if (compressor->dict->cdict) {
			zresult = ZSTD_CCtx_refCDict(compressor->cctx, compressor->dict->cdict);
		}
		else {
			zresult = ZSTD_CCtx_loadDictionary_advanced(compressor->cctx,
				compressor->dict->dictData, compressor->dict->dictSize,
				ZSTD_dlm_byRef, compressor->dict->dictType);
		}
		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "could not load compression dictionary: %s",
				ZSTD_getErrorName(zresult));
			return 1;
		}
	}

	return 0;
}

static PyObject* frame_progression(ZSTD_CCtx* cctx) {
	PyObject* result = NULL;
	PyObject* value;
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

PyDoc_STRVAR(ZstdCompressor__doc__,
"ZstdCompressor(level=None, dict_data=None, compression_params=None)\n"
"\n"
"Create an object used to perform Zstandard compression.\n"
"\n"
"An instance can compress data various ways. Instances can be used multiple\n"
"times. Each compression operation will use the compression parameters\n"
"defined at construction time.\n"
"\n"
"Compression can be configured via the following names arguments:\n"
"\n"
"level\n"
"   Integer compression level.\n"
"dict_data\n"
"   A ``ZstdCompressionDict`` to be used to compress with dictionary data.\n"
"compression_params\n"
"   A ``CompressionParameters`` instance defining low-level compression"
"   parameters. If defined, this will overwrite the ``level`` argument.\n"
"write_checksum\n"
"   If True, a 4 byte content checksum will be written with the compressed\n"
"   data, allowing the decompressor to perform content verification.\n"
"write_content_size\n"
"   If True (the default), the decompressed content size will be included in\n"
"   the header of the compressed data. This data will only be written if the\n"
"   compressor knows the size of the input data.\n"
"write_dict_id\n"
"   Determines whether the dictionary ID will be written into the compressed\n"
"   data. Defaults to True. Only adds content to the compressed data if\n"
"   a dictionary is being used.\n"
"threads\n"
"   Number of threads to use to compress data concurrently. When set,\n"
"   compression operations are performed on multiple threads. The default\n"
"   value (0) disables multi-threaded compression. A value of ``-1`` means to\n"
"   set the number of threads to the number of detected logical CPUs.\n"
);

static int ZstdCompressor_init(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"level",
		"dict_data",
		"compression_params",
		"write_checksum",
		"write_content_size",
		"write_dict_id",
		"threads",
		NULL
	};

	int level = 3;
	ZstdCompressionDict* dict = NULL;
	ZstdCompressionParametersObject* params = NULL;
	PyObject* writeChecksum = NULL;
	PyObject* writeContentSize = NULL;
	PyObject* writeDictID = NULL;
	int threads = 0;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iO!O!OOOi:ZstdCompressor",
		kwlist,	&level, &ZstdCompressionDictType, &dict,
		&ZstdCompressionParametersType, &params,
		&writeChecksum, &writeContentSize, &writeDictID, &threads)) {
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

	/* We create a ZSTD_CCtx for reuse among multiple operations to reduce the
	   overhead of each compression operation. */
	self->cctx = ZSTD_createCCtx();
	if (!self->cctx) {
		PyErr_NoMemory();
		return -1;
	}

	/* TODO stuff the original parameters away somewhere so we can reset later. This
	   will allow us to do things like automatically adjust cparams based on input
	   size (assuming zstd isn't doing that internally). */

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
		PyErr_SetString(PyExc_ValueError,
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
		if (set_parameters(self->params, params)) {
			return -1;
		}
	}
	else {
		if (set_parameter(self->params, ZSTD_c_compressionLevel, level)) {
			return -1;
		}

		if (set_parameter(self->params, ZSTD_c_contentSizeFlag,
			writeContentSize ? PyObject_IsTrue(writeContentSize) : 1)) {
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
		self->dict = dict;
		Py_INCREF(dict);
	}

    if (setup_cctx(self)) {
        return -1;
    }

	return 0;
}

static void ZstdCompressor_dealloc(ZstdCompressor* self) {
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

PyDoc_STRVAR(ZstdCompressor_memory_size__doc__,
"memory_size()\n"
"\n"
"Obtain the memory usage of this compressor, in bytes.\n"
);

static PyObject* ZstdCompressor_memory_size(ZstdCompressor* self) {
	if (self->cctx) {
		return PyLong_FromSize_t(ZSTD_sizeof_CCtx(self->cctx));
	}
	else {
		PyErr_SetString(ZstdError, "no compressor context found; this should never happen");
		return NULL;
	}
}

PyDoc_STRVAR(ZstdCompressor_frame_progression__doc__,
"frame_progression()\n"
"\n"
"Return information on how much work the compressor has done.\n"
"\n"
"Returns a 3-tuple of (ingested, consumed, produced).\n"
);

static PyObject* ZstdCompressor_frame_progression(ZstdCompressor* self) {
	return frame_progression(self->cctx);
}

PyDoc_STRVAR(ZstdCompressor_copy_stream__doc__,
"copy_stream(ifh, ofh[, size=0, read_size=default, write_size=default])\n"
"compress data between streams\n"
"\n"
"Data will be read from ``ifh``, compressed, and written to ``ofh``.\n"
"``ifh`` must have a ``read(size)`` method. ``ofh`` must have a ``write(data)``\n"
"method.\n"
"\n"
"An optional ``size`` argument specifies the size of the source stream.\n"
"If defined, compression parameters will be tuned based on the size.\n"
"\n"
"Optional arguments ``read_size`` and ``write_size`` define the chunk sizes\n"
"of ``read()`` and ``write()`` operations, respectively. By default, they use\n"
"the default compression stream input and output sizes, respectively.\n"
);

static PyObject* ZstdCompressor_copy_stream(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"ifh",
		"ofh",
		"size",
		"read_size",
		"write_size",
		NULL
	};

	PyObject* source;
	PyObject* dest;
	unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
	size_t inSize = ZSTD_CStreamInSize();
	size_t outSize = ZSTD_CStreamOutSize();
	ZSTD_inBuffer input;
	ZSTD_outBuffer output;
	Py_ssize_t totalRead = 0;
	Py_ssize_t totalWrite = 0;
	char* readBuffer;
	Py_ssize_t readSize;
	PyObject* readResult = NULL;
	PyObject* res = NULL;
	size_t zresult;
	PyObject* writeResult;
	PyObject* totalReadPy;
	PyObject* totalWritePy;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Kkk:copy_stream", kwlist,
		&source, &dest, &sourceSize, &inSize, &outSize)) {
		return NULL;
	}

	if (!PyObject_HasAttrString(source, "read")) {
		PyErr_SetString(PyExc_ValueError, "first argument must have a read() method");
		return NULL;
	}

	if (!PyObject_HasAttrString(dest, "write")) {
		PyErr_SetString(PyExc_ValueError, "second argument must have a write() method");
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
			PyErr_SetString(ZstdError, "could not read() from source");
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
			Py_BEGIN_ALLOW_THREADS
			zresult = ZSTD_compressStream2(self->cctx, &output, &input, ZSTD_e_continue);
			Py_END_ALLOW_THREADS

			if (ZSTD_isError(zresult)) {
				res = NULL;
				PyErr_Format(ZstdError, "zstd compress error: %s", ZSTD_getErrorName(zresult));
				goto finally;
			}

			if (output.pos) {
#if PY_MAJOR_VERSION >= 3
				writeResult = PyObject_CallMethod(dest, "write", "y#",
#else
				writeResult = PyObject_CallMethod(dest, "write", "s#",
#endif
					output.dst, output.pos);
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
		Py_BEGIN_ALLOW_THREADS
		zresult = ZSTD_compressStream2(self->cctx, &output, &input, ZSTD_e_end);
		Py_END_ALLOW_THREADS

		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "error ending compression stream: %s",
				ZSTD_getErrorName(zresult));
			res = NULL;
			goto finally;
		}

		if (output.pos) {
#if PY_MAJOR_VERSION >= 3
			writeResult = PyObject_CallMethod(dest, "write", "y#",
#else
			writeResult = PyObject_CallMethod(dest, "write", "s#",
#endif
				output.dst, output.pos);
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

PyDoc_STRVAR(ZstdCompressor_stream_reader__doc__,
"stream_reader(source, [size=0])\n"
"\n"
"Obtain an object that behaves like an I/O stream.\n"
"\n"
"The source object can be any object with a ``read(size)`` method\n"
"or an object that conforms to the buffer protocol.\n"
);

static ZstdCompressionReader* ZstdCompressor_stream_reader(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"source",
		"size",
		"read_size",
		NULL
	};

	PyObject* source;
	unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
	size_t readSize = ZSTD_CStreamInSize();
	ZstdCompressionReader* result = NULL;
	size_t zresult;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Kk:stream_reader", kwlist,
		&source, &sourceSize, &readSize)) {
		return NULL;
	}

	result = (ZstdCompressionReader*)PyObject_CallObject((PyObject*)&ZstdCompressionReaderType, NULL);
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
			"must pass an object with a read() method or that conforms to the buffer protocol");
		goto except;
	}

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

PyDoc_STRVAR(ZstdCompressor_compress__doc__,
"compress(data)\n"
"\n"
"Compress data in a single operation.\n"
"\n"
"This is the simplest mechanism to perform compression: simply pass in a\n"
"value and get a compressed value back. It is almost the most prone to abuse.\n"
"The input and output values must fit in memory, so passing in very large\n"
"values can result in excessive memory usage. For this reason, one of the\n"
"streaming based APIs is preferred for larger values.\n"
);

static PyObject* ZstdCompressor_compress(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		NULL
	};

	Py_buffer source;
	size_t destSize;
	PyObject* output = NULL;
	size_t zresult;
	ZSTD_outBuffer outBuffer;
	ZSTD_inBuffer inBuffer;

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*|O:compress",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*|O:compress",
#endif
		kwlist, &source)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
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
	zresult = ZSTD_compressStream2(self->cctx, &outBuffer, &inBuffer, ZSTD_e_end);
	Py_END_ALLOW_THREADS

	if (ZSTD_isError(zresult)) {
		PyErr_Format(ZstdError, "cannot compress: %s", ZSTD_getErrorName(zresult));
		Py_CLEAR(output);
		goto finally;
	}
	else if (zresult) {
		PyErr_SetString(ZstdError, "unexpected partial frame flush");
		Py_CLEAR(output);
		goto finally;
	}

	Py_SIZE(output) = outBuffer.pos;

finally:
	PyBuffer_Release(&source);
	return output;
}

PyDoc_STRVAR(ZstdCompressionObj__doc__,
"compressobj()\n"
"\n"
"Return an object exposing ``compress(data)`` and ``flush()`` methods.\n"
"\n"
"The returned object exposes an API similar to ``zlib.compressobj`` and\n"
"``bz2.BZ2Compressor`` so that callers can swap in the zstd compressor\n"
"without changing how compression is performed.\n"
);

static ZstdCompressionObj* ZstdCompressor_compressobj(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"size",
		NULL
	};

	unsigned long long inSize = ZSTD_CONTENTSIZE_UNKNOWN;
	size_t outSize = ZSTD_CStreamOutSize();
	ZstdCompressionObj* result = NULL;
	size_t zresult;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|K:compressobj", kwlist, &inSize)) {
		return NULL;
	}

	ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

	zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, inSize);
	if (ZSTD_isError(zresult)) {
		PyErr_Format(ZstdError, "error setting source size: %s",
			ZSTD_getErrorName(zresult));
		return NULL;
	}

	result = (ZstdCompressionObj*)PyObject_CallObject((PyObject*)&ZstdCompressionObjType, NULL);
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

PyDoc_STRVAR(ZstdCompressor_read_to_iter__doc__,
"read_to_iter(reader, [size=0, read_size=default, write_size=default])\n"
"Read uncompressed data from a reader and return an iterator\n"
"\n"
"Returns an iterator of compressed data produced from reading from ``reader``.\n"
"\n"
"Uncompressed data will be obtained from ``reader`` by calling the\n"
"``read(size)`` method of it. The source data will be streamed into a\n"
"compressor. As compressed data is available, it will be exposed to the\n"
"iterator.\n"
"\n"
"Data is read from the source in chunks of ``read_size``. Compressed chunks\n"
"are at most ``write_size`` bytes. Both values default to the zstd input and\n"
"and output defaults, respectively.\n"
"\n"
"The caller is partially in control of how fast data is fed into the\n"
"compressor by how it consumes the returned iterator. The compressor will\n"
"not consume from the reader unless the caller consumes from the iterator.\n"
);

static ZstdCompressorIterator* ZstdCompressor_read_to_iter(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"reader",
		"size",
		"read_size",
		"write_size",
		NULL
	};

	PyObject* reader;
	unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
	size_t inSize = ZSTD_CStreamInSize();
	size_t outSize = ZSTD_CStreamOutSize();
	ZstdCompressorIterator* result;
	size_t zresult;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Kkk:read_to_iter", kwlist,
		&reader, &sourceSize, &inSize, &outSize)) {
		return NULL;
	}

	result = (ZstdCompressorIterator*)PyObject_CallObject((PyObject*)&ZstdCompressorIteratorType, NULL);
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
			"must pass an object with a read() method or conforms to buffer protocol");
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

PyDoc_STRVAR(ZstdCompressor_stream_writer___doc__,
"Create a context manager to write compressed data to an object.\n"
"\n"
"The passed object must have a ``write()`` method.\n"
"\n"
"The caller feeds input data to the object by calling ``compress(data)``.\n"
"Compressed data is written to the argument given to this function.\n"
"\n"
"The function takes an optional ``size`` argument indicating the total size\n"
"of the eventual input. If specified, the size will influence compression\n"
"parameter tuning and could result in the size being written into the\n"
"header of the compressed data.\n"
"\n"
"An optional ``write_size`` argument is also accepted. It defines the maximum\n"
"byte size of chunks fed to ``write()``. By default, it uses the zstd default\n"
"for a compressor output stream.\n"
);

static ZstdCompressionWriter* ZstdCompressor_stream_writer(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"writer",
		"size",
		"write_size",
		"write_return_read",
		NULL
	};

	PyObject* writer;
	ZstdCompressionWriter* result;
	size_t zresult;
	unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
	size_t outSize = ZSTD_CStreamOutSize();
	PyObject* writeReturnRead = NULL;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|KkO:stream_writer", kwlist,
		&writer, &sourceSize, &outSize, &writeReturnRead)) {
		return NULL;
	}

	if (!PyObject_HasAttrString(writer, "write")) {
		PyErr_SetString(PyExc_ValueError, "must pass an object with a write() method");
		return NULL;
	}

	ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);

	zresult = ZSTD_CCtx_setPledgedSrcSize(self->cctx, sourceSize);
	if (ZSTD_isError(zresult)) {
		PyErr_Format(ZstdError, "error setting source size: %s",
			ZSTD_getErrorName(zresult));
		return NULL;
	}

	result = (ZstdCompressionWriter*)PyObject_CallObject((PyObject*)&ZstdCompressionWriterType, NULL);
	if (!result) {
		return NULL;
	}

	result->entered = 0;
	result->closed = 0;

	result->output.dst = PyMem_Malloc(outSize);
	if (!result->output.dst) {
		Py_DECREF(result);
		return (ZstdCompressionWriter*)PyErr_NoMemory();
	}

	result->output.pos = 0;
	result->output.size = outSize;

	result->compressor = self;
	Py_INCREF(result->compressor);

	result->writer = writer;
	Py_INCREF(result->writer);

	result->outSize = outSize;
	result->bytesCompressed = 0;
	result->writeReturnRead = writeReturnRead ? PyObject_IsTrue(writeReturnRead) : 0;

	return result;
}

PyDoc_STRVAR(ZstdCompressor_chunker__doc__,
"Create an object for iterative compressing to same-sized chunks.\n"
);

static ZstdCompressionChunker* ZstdCompressor_chunker(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"size",
		"chunk_size",
		NULL
	};

	unsigned long long sourceSize = ZSTD_CONTENTSIZE_UNKNOWN;
	size_t chunkSize = ZSTD_CStreamOutSize();
	ZstdCompressionChunker* chunker;
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

	chunker = (ZstdCompressionChunker*)PyObject_CallObject((PyObject*)&ZstdCompressionChunkerType, NULL);
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
	void* sourceData;
	size_t sourceSize;
} DataSource;

typedef struct {
	DataSource* sources;
	Py_ssize_t sourcesSize;
	unsigned long long totalSourceSize;
} DataSources;

typedef struct {
	void* dest;
	Py_ssize_t destSize;
	BufferSegment* segments;
	Py_ssize_t segmentsSize;
} DestBuffer;

typedef enum {
	WorkerError_none = 0,
	WorkerError_zstd = 1,
	WorkerError_no_memory = 2,
	WorkerError_nospace = 3,
} WorkerError;

/**
 * Holds state for an individual worker performing multi_compress_to_buffer work.
 */
typedef struct {
	/* Used for compression. */
	ZSTD_CCtx* cctx;

	/* What to compress. */
	DataSource* sources;
	Py_ssize_t sourcesSize;
	Py_ssize_t startOffset;
	Py_ssize_t endOffset;
	unsigned long long totalSourceSize;

	/* Result storage. */
	DestBuffer* destBuffers;
	Py_ssize_t destCount;

	/* Error tracking. */
	WorkerError error;
	size_t zresult;
	Py_ssize_t errorOffset;
} WorkerState;

static void compress_worker(WorkerState* state) {
	Py_ssize_t inputOffset = state->startOffset;
	Py_ssize_t remainingItems = state->endOffset - state->startOffset + 1;
	Py_ssize_t currentBufferStartOffset = state->startOffset;
	size_t zresult;
	void* newDest;
	size_t allocationSize;
	size_t boundSize;
	Py_ssize_t destOffset = 0;
	DataSource* sources = state->sources;
	DestBuffer* destBuffer;

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

	state->destBuffers = calloc(1, sizeof(DestBuffer));
	if (NULL == state->destBuffers) {
		state->error = WorkerError_no_memory;
		return;
	}

	destBuffer = &state->destBuffers[state->destCount - 1];

	/*
	 * Rather than track bounds and grow the segments buffer, allocate space
	 * to hold remaining items then truncate when we're done with it.
	 */
	destBuffer->segments = calloc(remainingItems, sizeof(BufferSegment));
	if (NULL == destBuffer->segments) {
		state->error = WorkerError_no_memory;
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
		state->error = WorkerError_no_memory;
		return;
	}

	destBuffer->destSize = allocationSize;

	for (inputOffset = state->startOffset; inputOffset <= state->endOffset; inputOffset++) {
		void* source = sources[inputOffset].sourceData;
		size_t sourceSize = sources[inputOffset].sourceSize;
		size_t destAvailable;
		void* dest;
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
			 * The downsizing of the existing buffer is optional. It should be cheap
			 * (unlike growing). So we just do it.
			 */
			if (destAvailable) {
				newDest = realloc(destBuffer->dest, destOffset);
				if (NULL == newDest) {
					state->error = WorkerError_no_memory;
					return;
				}

				destBuffer->dest = newDest;
				destBuffer->destSize = destOffset;
			}

			/* Truncate segments buffer. */
			newDest = realloc(destBuffer->segments,
				(inputOffset - currentBufferStartOffset + 1) * sizeof(BufferSegment));
			if (NULL == newDest) {
				state->error = WorkerError_no_memory;
				return;
			}

			destBuffer->segments = newDest;
			destBuffer->segmentsSize = inputOffset - currentBufferStartOffset;

			/* Grow space for new struct. */
			/* TODO consider over-allocating so we don't do this every time. */
			newDest = realloc(state->destBuffers, (state->destCount + 1) * sizeof(DestBuffer));
			if (NULL == newDest) {
				state->error = WorkerError_no_memory;
				return;
			}

			state->destBuffers = newDest;
			state->destCount++;

			destBuffer = &state->destBuffers[state->destCount - 1];

			/* Don't take any chances with non-NULL pointers. */
			memset(destBuffer, 0, sizeof(DestBuffer));

			/**
			 * We could dynamically update allocation size based on work done so far.
			 * For now, keep is simple.
			 */
			assert(state->totalSourceSize <= SIZE_MAX);
			allocationSize = roundpow2((size_t)state->totalSourceSize >> 4);

			if (boundSize > allocationSize) {
				allocationSize = roundpow2(boundSize);
			}

			destBuffer->dest = malloc(allocationSize);
			if (NULL == destBuffer->dest) {
				state->error = WorkerError_no_memory;
				return;
			}

			destBuffer->destSize = allocationSize;
			destAvailable = allocationSize;
			destOffset = 0;

			destBuffer->segments = calloc(remainingItems, sizeof(BufferSegment));
			if (NULL == destBuffer->segments) {
				state->error = WorkerError_no_memory;
				return;
			}

			destBuffer->segmentsSize = remainingItems;
			currentBufferStartOffset = inputOffset;
		}

		dest = (char*)destBuffer->dest + destOffset;

		opInBuffer.src = source;
		opInBuffer.size = sourceSize;
		opInBuffer.pos = 0;

		opOutBuffer.dst = dest;
		opOutBuffer.size = destAvailable;
		opOutBuffer.pos = 0;

		zresult = ZSTD_CCtx_setPledgedSrcSize(state->cctx, sourceSize);
		if (ZSTD_isError(zresult)) {
			state->error = WorkerError_zstd;
			state->zresult = zresult;
			state->errorOffset = inputOffset;
			break;
		}

		zresult = ZSTD_compressStream2(state->cctx, &opOutBuffer, &opInBuffer, ZSTD_e_end);
		if (ZSTD_isError(zresult)) {
			state->error = WorkerError_zstd;
			state->zresult = zresult;
			state->errorOffset = inputOffset;
			break;
		}
		else if (zresult) {
			state->error = WorkerError_nospace;
			state->errorOffset = inputOffset;
			break;
		}

		destBuffer->segments[inputOffset - currentBufferStartOffset].offset = destOffset;
		destBuffer->segments[inputOffset - currentBufferStartOffset].length = opOutBuffer.pos;

		destOffset += opOutBuffer.pos;
		remainingItems--;
	}

	if (destBuffer->destSize > destOffset) {
		newDest = realloc(destBuffer->dest, destOffset);
		if (NULL == newDest) {
			state->error = WorkerError_no_memory;
			return;
		}

		destBuffer->dest = newDest;
		destBuffer->destSize = destOffset;
	}
}

ZstdBufferWithSegmentsCollection* compress_from_datasources(ZstdCompressor* compressor,
	DataSources* sources, Py_ssize_t threadCount) {
	unsigned long long bytesPerWorker;
	POOL_ctx* pool = NULL;
	WorkerState* workerStates = NULL;
	Py_ssize_t i;
	unsigned long long workerBytes = 0;
	Py_ssize_t workerStartOffset = 0;
	Py_ssize_t currentThread = 0;
	int errored = 0;
	Py_ssize_t segmentsCount = 0;
	Py_ssize_t segmentIndex;
	PyObject* segmentsArg = NULL;
	ZstdBufferWithSegments* buffer;
	ZstdBufferWithSegmentsCollection* result = NULL;

	assert(sources->sourcesSize > 0);
	assert(sources->totalSourceSize > 0);
	assert(threadCount >= 1);

	/* More threads than inputs makes no sense. */
	threadCount = sources->sourcesSize < threadCount ? sources->sourcesSize
													 : threadCount;

	/* TODO lower thread count when input size is too small and threads would add
	overhead. */

	workerStates = PyMem_Malloc(threadCount * sizeof(WorkerState));
	if (NULL == workerStates) {
		PyErr_NoMemory();
		goto finally;
	}

	memset(workerStates, 0, threadCount * sizeof(WorkerState));

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
				zresult = ZSTD_CCtx_refCDict(workerStates[i].cctx, compressor->dict->cdict);
			}
			else {
				zresult = ZSTD_CCtx_loadDictionary_advanced(
					workerStates[i].cctx,
					compressor->dict->dictData,
					compressor->dict->dictSize,
					ZSTD_dlm_byRef,
					compressor->dict->dictType);
			}

			if (ZSTD_isError(zresult)) {
				PyErr_Format(ZstdError, "could not load compression dictionary: %s",
					ZSTD_getErrorName(zresult));
				goto finally;
			}

		}

		workerStates[i].sources = sources->sources;
		workerStates[i].sourcesSize = sources->sourcesSize;
	}

	Py_BEGIN_ALLOW_THREADS
	for (i = 0; i < sources->sourcesSize; i++) {
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
				POOL_add(pool, (POOL_function)compress_worker, &workerStates[currentThread]);
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
			POOL_add(pool, (POOL_function)compress_worker, &workerStates[currentThread]);
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
		case WorkerError_no_memory:
			PyErr_NoMemory();
			errored = 1;
			break;

		case WorkerError_zstd:
			PyErr_Format(ZstdError, "error compressing item %zd: %s",
				workerStates[i].errorOffset, ZSTD_getErrorName(workerStates[i].zresult));
			errored = 1;
			break;

		case WorkerError_nospace:
			PyErr_Format(ZstdError, "error compressing item %zd: not enough space in output",
				workerStates[i].errorOffset);
			errored = 1;
			break;

		default:
			;
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
		WorkerState* state = &workerStates[i];
		segmentsCount += state->destCount;
	}

	segmentsArg = PyTuple_New(segmentsCount);
	if (NULL == segmentsArg) {
		goto finally;
	}

	segmentIndex = 0;

	for (i = 0; i < threadCount; i++) {
		Py_ssize_t j;
		WorkerState* state = &workerStates[i];

		for (j = 0; j < state->destCount; j++) {
			DestBuffer* destBuffer = &state->destBuffers[j];
			buffer = BufferWithSegments_FromMemory(destBuffer->dest, destBuffer->destSize,
				destBuffer->segments, destBuffer->segmentsSize);

			if (NULL == buffer) {
				goto finally;
			}

			/* Tell instance to use free() instsead of PyMem_Free(). */
			buffer->useFree = 1;

			/*
			 * BufferWithSegments_FromMemory takes ownership of the backing memory.
			 * Unset it here so it doesn't get freed below.
			 */
			destBuffer->dest = NULL;
			destBuffer->segments = NULL;

			PyTuple_SET_ITEM(segmentsArg, segmentIndex++, (PyObject*)buffer);
		}
	}

	result = (ZstdBufferWithSegmentsCollection*)PyObject_CallObject(
		(PyObject*)&ZstdBufferWithSegmentsCollectionType, segmentsArg);

finally:
	Py_CLEAR(segmentsArg);

	if (pool) {
		POOL_free(pool);
	}

	if (workerStates) {
		Py_ssize_t j;

		for (i = 0; i < threadCount; i++) {
			WorkerState state = workerStates[i];

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

PyDoc_STRVAR(ZstdCompressor_multi_compress_to_buffer__doc__,
"Compress multiple pieces of data as a single operation\n"
"\n"
"Receives a ``BufferWithSegmentsCollection``, a ``BufferWithSegments``, or\n"
"a list of bytes like objects holding data to compress.\n"
"\n"
"Returns a ``BufferWithSegmentsCollection`` holding compressed data.\n"
"\n"
"This function is optimized to perform multiple compression operations as\n"
"as possible with as little overhead as possbile.\n"
);

static ZstdBufferWithSegmentsCollection* ZstdCompressor_multi_compress_to_buffer(ZstdCompressor* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		"threads",
		NULL
	};

	PyObject* data;
	int threads = 0;
	Py_buffer* dataBuffers = NULL;
	DataSources sources;
	Py_ssize_t i;
	Py_ssize_t sourceCount = 0;
	ZstdBufferWithSegmentsCollection* result = NULL;

	memset(&sources, 0, sizeof(sources));

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|i:multi_compress_to_buffer", kwlist,
		&data, &threads)) {
		return NULL;
	}

	if (threads < 0) {
		threads = cpu_count();
	}

	if (threads < 2) {
		threads = 1;
	}

	if (PyObject_TypeCheck(data, &ZstdBufferWithSegmentsType)) {
		ZstdBufferWithSegments* buffer = (ZstdBufferWithSegments*)data;

		sources.sources = PyMem_Malloc(buffer->segmentCount * sizeof(DataSource));
		if (NULL == sources.sources) {
			PyErr_NoMemory();
			goto finally;
		}

		for (i = 0; i < buffer->segmentCount; i++) {
			if (buffer->segments[i].length > SIZE_MAX) {
				PyErr_Format(PyExc_ValueError,
					"buffer segment %zd is too large for this platform", i);
				goto finally;
			}

			sources.sources[i].sourceData = (char*)buffer->data + buffer->segments[i].offset;
			sources.sources[i].sourceSize = (size_t)buffer->segments[i].length;
			sources.totalSourceSize += buffer->segments[i].length;
		}

		sources.sourcesSize = buffer->segmentCount;
	}
	else if (PyObject_TypeCheck(data, &ZstdBufferWithSegmentsCollectionType)) {
		Py_ssize_t j;
		Py_ssize_t offset = 0;
		ZstdBufferWithSegments* buffer;
		ZstdBufferWithSegmentsCollection* collection = (ZstdBufferWithSegmentsCollection*)data;

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
						"buffer segment %zd in buffer %zd is too large for this platform",
						j, i);
					goto finally;
				}

				sources.sources[offset].sourceData = (char*)buffer->data + buffer->segments[j].offset;
				sources.sources[offset].sourceSize = (size_t)buffer->segments[j].length;
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
				PyErr_Format(PyExc_TypeError, "item %zd not a bytes like object", i);
				goto finally;
			}

			sources.sources[i].sourceData = dataBuffers[i].buf;
			sources.sources[i].sourceSize = dataBuffers[i].len;
			sources.totalSourceSize += dataBuffers[i].len;
		}

		sources.sourcesSize = sourceCount;
	}
	else {
		PyErr_SetString(PyExc_TypeError, "argument must be list of BufferWithSegments");
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
		PyErr_SetString(PyExc_ValueError, "sources are too large for this platform");
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

static PyMethodDef ZstdCompressor_methods[] = {
	{ "chunker", (PyCFunction)ZstdCompressor_chunker,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_chunker__doc__ },
	{ "compress", (PyCFunction)ZstdCompressor_compress,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_compress__doc__ },
	{ "compressobj", (PyCFunction)ZstdCompressor_compressobj,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressionObj__doc__ },
	{ "copy_stream", (PyCFunction)ZstdCompressor_copy_stream,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_copy_stream__doc__ },
	{ "stream_reader", (PyCFunction)ZstdCompressor_stream_reader,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_stream_reader__doc__ },
	{ "stream_writer", (PyCFunction)ZstdCompressor_stream_writer,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_stream_writer___doc__ },
	{ "read_to_iter", (PyCFunction)ZstdCompressor_read_to_iter,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_read_to_iter__doc__ },
	/* TODO Remove deprecated API */
	{ "read_from", (PyCFunction)ZstdCompressor_read_to_iter,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_read_to_iter__doc__ },
	/* TODO remove deprecated API */
	{ "write_to", (PyCFunction)ZstdCompressor_stream_writer,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_stream_writer___doc__ },
	{ "multi_compress_to_buffer", (PyCFunction)ZstdCompressor_multi_compress_to_buffer,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressor_multi_compress_to_buffer__doc__ },
	{ "memory_size", (PyCFunction)ZstdCompressor_memory_size,
	METH_NOARGS, ZstdCompressor_memory_size__doc__ },
	{ "frame_progression", (PyCFunction)ZstdCompressor_frame_progression,
	METH_NOARGS, ZstdCompressor_frame_progression__doc__ },
	{ NULL, NULL }
};

PyTypeObject ZstdCompressorType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"zstd.ZstdCompressor",         /* tp_name */
	sizeof(ZstdCompressor),        /* tp_basicsize */
	0,                              /* tp_itemsize */
	(destructor)ZstdCompressor_dealloc, /* tp_dealloc */
	0,                              /* tp_print */
	0,                              /* tp_getattr */
	0,                              /* tp_setattr */
	0,                              /* tp_compare */
	0,                              /* tp_repr */
	0,                              /* tp_as_number */
	0,                              /* tp_as_sequence */
	0,                              /* tp_as_mapping */
	0,                              /* tp_hash */
	0,                              /* tp_call */
	0,                              /* tp_str */
	0,                              /* tp_getattro */
	0,                              /* tp_setattro */
	0,                              /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
	ZstdCompressor__doc__,          /* tp_doc */
	0,                              /* tp_traverse */
	0,                              /* tp_clear */
	0,                              /* tp_richcompare */
	0,                              /* tp_weaklistoffset */
	0,                              /* tp_iter */
	0,                              /* tp_iternext */
	ZstdCompressor_methods,         /* tp_methods */
	0,                              /* tp_members */
	0,                              /* tp_getset */
	0,                              /* tp_base */
	0,                              /* tp_dict */
	0,                              /* tp_descr_get */
	0,                              /* tp_descr_set */
	0,                              /* tp_dictoffset */
	(initproc)ZstdCompressor_init,  /* tp_init */
	0,                              /* tp_alloc */
	PyType_GenericNew,              /* tp_new */
};

void compressor_module_init(PyObject* mod) {
	Py_TYPE(&ZstdCompressorType) = &PyType_Type;
	if (PyType_Ready(&ZstdCompressorType) < 0) {
		return;
	}

	Py_INCREF((PyObject*)&ZstdCompressorType);
	PyModule_AddObject(mod, "ZstdCompressor",
		(PyObject*)&ZstdCompressorType);
}
