/**
* Copyright (c) 2016-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"

extern PyObject* ZstdError;

PyDoc_STRVAR(ZstdCompressionObj__doc__,
"Perform compression using a standard library compatible API.\n"
);

static void ZstdCompressionObj_dealloc(ZstdCompressionObj* self) {
	PyMem_Free(self->output.dst);
	self->output.dst = NULL;

	Py_XDECREF(self->compressor);

	PyObject_Del(self);
}

static PyObject* ZstdCompressionObj_compress(ZstdCompressionObj* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		NULL
	};

	Py_buffer source;
	ZSTD_inBuffer input;
	size_t zresult;
	PyObject* result = NULL;
	Py_ssize_t resultSize = 0;

	if (self->finished) {
		PyErr_SetString(ZstdError, "cannot call compress() after compressor finished");
		return NULL;
	}

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:compress",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*:compress",
#endif
		kwlist, &source)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
	}

	input.src = source.buf;
	input.size = source.len;
	input.pos = 0;

	while (input.pos < (size_t)source.len) {
		Py_BEGIN_ALLOW_THREADS
			zresult = ZSTD_compressStream2(self->compressor->cctx, &self->output,
				&input, ZSTD_e_continue);
		Py_END_ALLOW_THREADS

		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "zstd compress error: %s", ZSTD_getErrorName(zresult));
			Py_CLEAR(result);
			goto finally;
		}

		if (self->output.pos) {
			if (result) {
				resultSize = PyBytes_GET_SIZE(result);

				if (safe_pybytes_resize(&result, resultSize + self->output.pos)) {
					Py_CLEAR(result);
					goto finally;
				}

				memcpy(PyBytes_AS_STRING(result) + resultSize,
					self->output.dst, self->output.pos);
			}
			else {
				result = PyBytes_FromStringAndSize(self->output.dst, self->output.pos);
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

static PyObject* ZstdCompressionObj_flush(ZstdCompressionObj* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"flush_mode",
		NULL
	};

	int flushMode = compressorobj_flush_finish;
	size_t zresult;
	PyObject* result = NULL;
	Py_ssize_t resultSize = 0;
	ZSTD_inBuffer input;
	ZSTD_EndDirective zFlushMode;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:flush", kwlist, &flushMode)) {
		return NULL;
	}

	if (flushMode != compressorobj_flush_finish && flushMode != compressorobj_flush_block) {
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
		Py_BEGIN_ALLOW_THREADS
		zresult = ZSTD_compressStream2(self->compressor->cctx, &self->output,
			&input, zFlushMode);
		Py_END_ALLOW_THREADS

		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "error ending compression stream: %s",
				ZSTD_getErrorName(zresult));
			return NULL;
		}

		if (self->output.pos) {
			if (result) {
				resultSize = PyBytes_GET_SIZE(result);

				if (safe_pybytes_resize(&result, resultSize + self->output.pos)) {
					Py_XDECREF(result);
					return NULL;
				}

				memcpy(PyBytes_AS_STRING(result) + resultSize,
					self->output.dst, self->output.pos);
			}
			else {
				result = PyBytes_FromStringAndSize(self->output.dst, self->output.pos);
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
	{ "compress", (PyCFunction)ZstdCompressionObj_compress, METH_VARARGS | METH_KEYWORDS,
	PyDoc_STR("compress data") },
	{ "flush", (PyCFunction)ZstdCompressionObj_flush, METH_VARARGS | METH_KEYWORDS,
	PyDoc_STR("finish compression operation") },
	{ NULL, NULL }
};

PyTypeObject ZstdCompressionObjType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"zstd.ZstdCompressionObj",      /* tp_name */
	sizeof(ZstdCompressionObj),     /* tp_basicsize */
	0,                              /* tp_itemsize */
	(destructor)ZstdCompressionObj_dealloc, /* tp_dealloc */
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
	ZstdCompressionObj__doc__,      /* tp_doc */
	0,                              /* tp_traverse */
	0,                              /* tp_clear */
	0,                              /* tp_richcompare */
	0,                              /* tp_weaklistoffset */
	0,                              /* tp_iter */
	0,                              /* tp_iternext */
	ZstdCompressionObj_methods,     /* tp_methods */
	0,                              /* tp_members */
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

void compressobj_module_init(PyObject* module) {
	Py_TYPE(&ZstdCompressionObjType) = &PyType_Type;
	if (PyType_Ready(&ZstdCompressionObjType) < 0) {
		return;
	}
}
