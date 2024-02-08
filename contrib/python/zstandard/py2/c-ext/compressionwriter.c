/**
* Copyright (c) 2016-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"

extern PyObject* ZstdError;

PyDoc_STRVAR(ZstdCompresssionWriter__doc__,
"""A context manager used for writing compressed output to a writer.\n"
);

static void ZstdCompressionWriter_dealloc(ZstdCompressionWriter* self) {
	Py_XDECREF(self->compressor);
	Py_XDECREF(self->writer);

	PyMem_Free(self->output.dst);
	self->output.dst = NULL;

	PyObject_Del(self);
}

static PyObject* ZstdCompressionWriter_enter(ZstdCompressionWriter* self) {
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
	return (PyObject*)self;
}

static PyObject* ZstdCompressionWriter_exit(ZstdCompressionWriter* self, PyObject* args) {
	PyObject* exc_type;
	PyObject* exc_value;
	PyObject* exc_tb;

	if (!PyArg_ParseTuple(args, "OOO:__exit__", &exc_type, &exc_value, &exc_tb)) {
		return NULL;
	}

	self->entered = 0;

	if (exc_type == Py_None && exc_value == Py_None && exc_tb == Py_None) {
		PyObject* result = PyObject_CallMethod((PyObject*)self, "close", NULL);

		if (NULL == result) {
			return NULL;
		}
	}

	Py_RETURN_FALSE;
}

static PyObject* ZstdCompressionWriter_memory_size(ZstdCompressionWriter* self) {
	return PyLong_FromSize_t(ZSTD_sizeof_CCtx(self->compressor->cctx));
}

static PyObject* ZstdCompressionWriter_write(ZstdCompressionWriter* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		NULL
	};

	PyObject* result = NULL;
	Py_buffer source;
	size_t zresult;
	ZSTD_inBuffer input;
	PyObject* res;
	Py_ssize_t totalWrite = 0;

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:write",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*:write",
#endif
		kwlist, &source)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
	}

	if (self->closed) {
		PyErr_SetString(PyExc_ValueError, "stream is closed");
		return NULL;
	}

	self->output.pos = 0;

	input.src = source.buf;
	input.size = source.len;
	input.pos = 0;

	while (input.pos < (size_t)source.len) {
		Py_BEGIN_ALLOW_THREADS
		zresult = ZSTD_compressStream2(self->compressor->cctx, &self->output, &input, ZSTD_e_continue);
		Py_END_ALLOW_THREADS

		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "zstd compress error: %s", ZSTD_getErrorName(zresult));
			goto finally;
		}

		/* Copy data from output buffer to writer. */
		if (self->output.pos) {
#if PY_MAJOR_VERSION >= 3
			res = PyObject_CallMethod(self->writer, "write", "y#",
#else
			res = PyObject_CallMethod(self->writer, "write", "s#",
#endif
				self->output.dst, self->output.pos);
			Py_XDECREF(res);
			totalWrite += self->output.pos;
			self->bytesCompressed += self->output.pos;
		}
		self->output.pos = 0;
	}

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

static PyObject* ZstdCompressionWriter_flush(ZstdCompressionWriter* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"flush_mode",
		NULL
	};

	size_t zresult;
	ZSTD_inBuffer input;
	PyObject* res;
	Py_ssize_t totalWrite = 0;
	unsigned flush_mode = 0;
	ZSTD_EndDirective flush;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|I:flush",
		kwlist, &flush_mode)) {
		return NULL;
	}

	switch (flush_mode) {
		case 0:
			flush = ZSTD_e_flush;
			break;
		case 1:
			flush = ZSTD_e_end;
			break;
		default:
			PyErr_Format(PyExc_ValueError, "unknown flush_mode: %d", flush_mode);
			return NULL;
	}

	if (self->closed) {
		PyErr_SetString(PyExc_ValueError, "stream is closed");
		return NULL;
	}

	self->output.pos = 0;

	input.src = NULL;
	input.size = 0;
	input.pos = 0;

	while (1) {
		Py_BEGIN_ALLOW_THREADS
		zresult = ZSTD_compressStream2(self->compressor->cctx, &self->output, &input, flush);
		Py_END_ALLOW_THREADS

		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "zstd compress error: %s", ZSTD_getErrorName(zresult));
			return NULL;
		}

		/* Copy data from output buffer to writer. */
		if (self->output.pos) {
#if PY_MAJOR_VERSION >= 3
			res = PyObject_CallMethod(self->writer, "write", "y#",
#else
			res = PyObject_CallMethod(self->writer, "write", "s#",
#endif
				self->output.dst, self->output.pos);
			Py_XDECREF(res);
			totalWrite += self->output.pos;
			self->bytesCompressed += self->output.pos;
		}

		self->output.pos = 0;

		if (!zresult) {
			break;
		}
	}

	return PyLong_FromSsize_t(totalWrite);
}

static PyObject* ZstdCompressionWriter_close(ZstdCompressionWriter* self) {
	PyObject* result;

	if (self->closed) {
		Py_RETURN_NONE;
	}

	result = PyObject_CallMethod((PyObject*)self, "flush", "I", 1);
	self->closed = 1;

	if (NULL == result) {
	    return NULL;
	}

    /* Call close on underlying stream as well. */
	if (PyObject_HasAttrString(self->writer, "close")) {
		return PyObject_CallMethod(self->writer, "close", NULL);
	}

	Py_RETURN_NONE;
}

static PyObject* ZstdCompressionWriter_fileno(ZstdCompressionWriter* self) {
	if (PyObject_HasAttrString(self->writer, "fileno")) {
		return PyObject_CallMethod(self->writer, "fileno", NULL);
	}
	else {
		PyErr_SetString(PyExc_OSError, "fileno not available on underlying writer");
		return NULL;
	}
}

static PyObject* ZstdCompressionWriter_tell(ZstdCompressionWriter* self) {
	return PyLong_FromUnsignedLongLong(self->bytesCompressed);
}

static PyObject* ZstdCompressionWriter_writelines(PyObject* self, PyObject* args) {
	PyErr_SetNone(PyExc_NotImplementedError);
	return NULL;
}

static PyObject* ZstdCompressionWriter_false(PyObject* self, PyObject* args) {
	Py_RETURN_FALSE;
}

static PyObject* ZstdCompressionWriter_true(PyObject* self, PyObject* args) {
	Py_RETURN_TRUE;
}

static PyObject* ZstdCompressionWriter_unsupported(PyObject* self, PyObject* args, PyObject* kwargs) {
	PyObject* iomod;
	PyObject* exc;

	iomod = PyImport_ImportModule("io");
	if (NULL == iomod) {
		return NULL;
	}

	exc = PyObject_GetAttrString(iomod, "UnsupportedOperation");
	if (NULL == exc) {
		Py_DECREF(iomod);
		return NULL;
	}

	PyErr_SetNone(exc);
	Py_DECREF(exc);
	Py_DECREF(iomod);

	return NULL;
}

static PyMethodDef ZstdCompressionWriter_methods[] = {
	{ "__enter__", (PyCFunction)ZstdCompressionWriter_enter, METH_NOARGS,
	PyDoc_STR("Enter a compression context.") },
	{ "__exit__", (PyCFunction)ZstdCompressionWriter_exit, METH_VARARGS,
	PyDoc_STR("Exit a compression context.") },
	{ "close", (PyCFunction)ZstdCompressionWriter_close, METH_NOARGS, NULL },
	{ "fileno", (PyCFunction)ZstdCompressionWriter_fileno, METH_NOARGS, NULL },
	{ "isatty", (PyCFunction)ZstdCompressionWriter_false, METH_NOARGS, NULL },
	{ "readable", (PyCFunction)ZstdCompressionWriter_false, METH_NOARGS, NULL },
	{ "readline", (PyCFunction)ZstdCompressionWriter_unsupported, METH_VARARGS | METH_KEYWORDS, NULL },
	{ "readlines", (PyCFunction)ZstdCompressionWriter_unsupported, METH_VARARGS | METH_KEYWORDS, NULL },
	{ "seek", (PyCFunction)ZstdCompressionWriter_unsupported, METH_VARARGS | METH_KEYWORDS, NULL },
	{ "seekable", ZstdCompressionWriter_false, METH_NOARGS, NULL },
	{ "truncate", (PyCFunction)ZstdCompressionWriter_unsupported, METH_VARARGS | METH_KEYWORDS, NULL },
	{ "writable", ZstdCompressionWriter_true, METH_NOARGS, NULL },
	{ "writelines", ZstdCompressionWriter_writelines, METH_VARARGS, NULL },
	{ "read", (PyCFunction)ZstdCompressionWriter_unsupported, METH_VARARGS | METH_KEYWORDS, NULL },
	{ "readall", (PyCFunction)ZstdCompressionWriter_unsupported, METH_VARARGS | METH_KEYWORDS, NULL },
	{ "readinto", (PyCFunction)ZstdCompressionWriter_unsupported, METH_VARARGS | METH_KEYWORDS, NULL },
	{ "memory_size", (PyCFunction)ZstdCompressionWriter_memory_size, METH_NOARGS,
	PyDoc_STR("Obtain the memory size of the underlying compressor") },
	{ "write", (PyCFunction)ZstdCompressionWriter_write, METH_VARARGS | METH_KEYWORDS,
	PyDoc_STR("Compress data") },
	{ "flush", (PyCFunction)ZstdCompressionWriter_flush, METH_VARARGS | METH_KEYWORDS,
	PyDoc_STR("Flush data and finish a zstd frame") },
	{ "tell", (PyCFunction)ZstdCompressionWriter_tell, METH_NOARGS,
	PyDoc_STR("Returns current number of bytes compressed") },
	{ NULL, NULL }
};

static PyMemberDef ZstdCompressionWriter_members[] = {
	 { "closed", T_BOOL, offsetof(ZstdCompressionWriter, closed), READONLY, NULL },
	 { NULL }
};

PyTypeObject ZstdCompressionWriterType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"zstd.ZstdCompressionWriter",  /* tp_name */
	sizeof(ZstdCompressionWriter),  /* tp_basicsize */
	0,                              /* tp_itemsize */
	(destructor)ZstdCompressionWriter_dealloc, /* tp_dealloc */
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
	ZstdCompresssionWriter__doc__,  /* tp_doc */
	0,                              /* tp_traverse */
	0,                              /* tp_clear */
	0,                              /* tp_richcompare */
	0,                              /* tp_weaklistoffset */
	0,                              /* tp_iter */
	0,                              /* tp_iternext */
	ZstdCompressionWriter_methods,  /* tp_methods */
	ZstdCompressionWriter_members,  /* tp_members */
	0,                              /* tp_getset */
	0,                              /* tp_base */
	0,                              /* tp_dict */
	0,                              /* tp_descr_get */
	0,                              /* tp_descr_set */
	0,                              /* tp_dictoffset */
	0,                              /* tp_init */
	0,                              /* tp_alloc */
	PyType_GenericNew,              /* tp_new */
};

void compressionwriter_module_init(PyObject* mod) {
	Py_TYPE(&ZstdCompressionWriterType) = &PyType_Type;
	if (PyType_Ready(&ZstdCompressionWriterType) < 0) {
		return;
	}
}
