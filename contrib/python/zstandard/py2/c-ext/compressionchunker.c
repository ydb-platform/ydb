/**
* Copyright (c) 2018-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"

extern PyObject* ZstdError;

PyDoc_STRVAR(ZstdCompressionChunkerIterator__doc__,
	"Iterator of output chunks from ZstdCompressionChunker.\n"
);

static void ZstdCompressionChunkerIterator_dealloc(ZstdCompressionChunkerIterator* self) {
	Py_XDECREF(self->chunker);

	PyObject_Del(self);
}

static PyObject* ZstdCompressionChunkerIterator_iter(PyObject* self) {
	Py_INCREF(self);
	return self;
}

static PyObject* ZstdCompressionChunkerIterator_iternext(ZstdCompressionChunkerIterator* self) {
	size_t zresult;
	PyObject* chunk;
	ZstdCompressionChunker* chunker = self->chunker;
	ZSTD_EndDirective zFlushMode;

	if (self->mode != compressionchunker_mode_normal && chunker->input.pos != chunker->input.size) {
		PyErr_SetString(ZstdError, "input should have been fully consumed before calling flush() or finish()");
		return NULL;
	}

	if (chunker->finished) {
		return NULL;
	}

	/* If we have data left in the input, consume it. */
	while (chunker->input.pos < chunker->input.size) {
		Py_BEGIN_ALLOW_THREADS
		zresult = ZSTD_compressStream2(chunker->compressor->cctx, &chunker->output,
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
			PyErr_Format(ZstdError, "zstd compress error: %s", ZSTD_getErrorName(zresult));
			return NULL;
		}

		/* If it produced a full output chunk, emit it. */
		if (chunker->output.pos == chunker->output.size) {
			chunk = PyBytes_FromStringAndSize(chunker->output.dst, chunker->output.pos);
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
		PyErr_SetString(ZstdError, "unhandled compression mode; this should never happen");
		return NULL;
	}

	Py_BEGIN_ALLOW_THREADS
	zresult = ZSTD_compressStream2(chunker->compressor->cctx, &chunker->output,
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

PyTypeObject ZstdCompressionChunkerIteratorType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"zstd.ZstdCompressionChunkerIterator", /* tp_name */
	sizeof(ZstdCompressionChunkerIterator), /* tp_basicsize */
	0,                               /* tp_itemsize */
	(destructor)ZstdCompressionChunkerIterator_dealloc, /* tp_dealloc */
	0,                               /* tp_print */
	0,                               /* tp_getattr */
	0,                               /* tp_setattr */
	0,                               /* tp_compare */
	0,                               /* tp_repr */
	0,                               /* tp_as_number */
	0,                               /* tp_as_sequence */
	0,                               /* tp_as_mapping */
	0,                               /* tp_hash */
	0,                               /* tp_call */
	0,                               /* tp_str */
	0,                               /* tp_getattro */
	0,                               /* tp_setattro */
	0,                               /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
	ZstdCompressionChunkerIterator__doc__, /* tp_doc */
	0,                               /* tp_traverse */
	0,                               /* tp_clear */
	0,                               /* tp_richcompare */
	0,                               /* tp_weaklistoffset */
	ZstdCompressionChunkerIterator_iter, /* tp_iter */
	(iternextfunc)ZstdCompressionChunkerIterator_iternext, /* tp_iternext */
	0,                               /* tp_methods */
	0,                               /* tp_members */
	0,                               /* tp_getset */
	0,                               /* tp_base */
	0,                               /* tp_dict */
	0,                               /* tp_descr_get */
	0,                               /* tp_descr_set */
	0,                               /* tp_dictoffset */
	0,                               /* tp_init */
	0,                               /* tp_alloc */
	PyType_GenericNew,              /* tp_new */
};

PyDoc_STRVAR(ZstdCompressionChunker__doc__,
	"Compress chunks iteratively into exact chunk sizes.\n"
);

static void ZstdCompressionChunker_dealloc(ZstdCompressionChunker* self) {
	PyBuffer_Release(&self->inBuffer);
	self->input.src = NULL;

	PyMem_Free(self->output.dst);
	self->output.dst = NULL;

	Py_XDECREF(self->compressor);

	PyObject_Del(self);
}

static ZstdCompressionChunkerIterator* ZstdCompressionChunker_compress(ZstdCompressionChunker* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		NULL
	};

	ZstdCompressionChunkerIterator* result;

	if (self->finished) {
		PyErr_SetString(ZstdError, "cannot call compress() after compression finished");
		return NULL;
	}

	if (self->inBuffer.obj) {
		PyErr_SetString(ZstdError,
			"cannot perform operation before consuming output from previous operation");
		return NULL;
	}

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:compress",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*:compress",
#endif
		kwlist, &self->inBuffer)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&self->inBuffer, 'C') || self->inBuffer.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		PyBuffer_Release(&self->inBuffer);
		return NULL;
	}

	result = (ZstdCompressionChunkerIterator*)PyObject_CallObject((PyObject*)&ZstdCompressionChunkerIteratorType, NULL);
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

static ZstdCompressionChunkerIterator* ZstdCompressionChunker_finish(ZstdCompressionChunker* self) {
	ZstdCompressionChunkerIterator* result;

	if (self->finished) {
		PyErr_SetString(ZstdError, "cannot call finish() after compression finished");
		return NULL;
	}

	if (self->inBuffer.obj) {
		PyErr_SetString(ZstdError,
			"cannot call finish() before consuming output from previous operation");
		return NULL;
	}

	result = (ZstdCompressionChunkerIterator*)PyObject_CallObject((PyObject*)&ZstdCompressionChunkerIteratorType, NULL);
	if (!result) {
		return NULL;
	}

	result->chunker = self;
	Py_INCREF(result->chunker);

	result->mode = compressionchunker_mode_finish;

	return result;
}

static ZstdCompressionChunkerIterator* ZstdCompressionChunker_flush(ZstdCompressionChunker* self, PyObject* args, PyObject* kwargs) {
	ZstdCompressionChunkerIterator* result;

	if (self->finished) {
		PyErr_SetString(ZstdError, "cannot call flush() after compression finished");
		return NULL;
	}

	if (self->inBuffer.obj) {
		PyErr_SetString(ZstdError,
			"cannot call flush() before consuming output from previous operation");
		return NULL;
	}

	result = (ZstdCompressionChunkerIterator*)PyObject_CallObject((PyObject*)&ZstdCompressionChunkerIteratorType, NULL);
	if (!result) {
		return NULL;
	}

	result->chunker = self;
	Py_INCREF(result->chunker);

	result->mode = compressionchunker_mode_flush;

	return result;
}

static PyMethodDef ZstdCompressionChunker_methods[] = {
	{ "compress", (PyCFunction)ZstdCompressionChunker_compress, METH_VARARGS | METH_KEYWORDS,
	PyDoc_STR("compress data") },
	{ "finish", (PyCFunction)ZstdCompressionChunker_finish, METH_NOARGS,
	PyDoc_STR("finish compression operation") },
	{ "flush", (PyCFunction)ZstdCompressionChunker_flush, METH_VARARGS | METH_KEYWORDS,
	PyDoc_STR("finish compression operation") },
	{ NULL, NULL }
};

PyTypeObject ZstdCompressionChunkerType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"zstd.ZstdCompressionChunkerType",  /* tp_name */
	sizeof(ZstdCompressionChunker),     /* tp_basicsize */
	0,                                  /* tp_itemsize */
	(destructor)ZstdCompressionChunker_dealloc, /* tp_dealloc */
	0,                                  /* tp_print */
	0,                                  /* tp_getattr */
	0,                                  /* tp_setattr */
	0,                                  /* tp_compare */
	0,                                  /* tp_repr */
	0,                                  /* tp_as_number */
	0,                                  /* tp_as_sequence */
	0,                                  /* tp_as_mapping */
	0,                                  /* tp_hash */
	0,                                  /* tp_call */
	0,                                  /* tp_str */
	0,                                  /* tp_getattro */
	0,                                  /* tp_setattro */
	0,                                  /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
	ZstdCompressionChunker__doc__,      /* tp_doc */
	0,                                  /* tp_traverse */
	0,                                  /* tp_clear */
	0,                                  /* tp_richcompare */
	0,                                  /* tp_weaklistoffset */
	0,                                  /* tp_iter */
	0,                                  /* tp_iternext */
	ZstdCompressionChunker_methods,     /* tp_methods */
	0,                                  /* tp_members */
	0,                                  /* tp_getset */
	0,                                  /* tp_base */
	0,                                  /* tp_dict */
	0,                                  /* tp_descr_get */
	0,                                  /* tp_descr_set */
	0,                                  /* tp_dictoffset */
	0,                                  /* tp_init */
	0,                                  /* tp_alloc */
	PyType_GenericNew,                  /* tp_new */
};

void compressionchunker_module_init(PyObject* module) {
	Py_TYPE(&ZstdCompressionChunkerIteratorType) = &PyType_Type;
	if (PyType_Ready(&ZstdCompressionChunkerIteratorType) < 0) {
		return;
	}

	Py_TYPE(&ZstdCompressionChunkerType) = &PyType_Type;
	if (PyType_Ready(&ZstdCompressionChunkerType) < 0) {
		return;
	}
}
