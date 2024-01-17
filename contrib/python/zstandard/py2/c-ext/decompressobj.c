/**
* Copyright (c) 2016-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"

extern PyObject* ZstdError;

PyDoc_STRVAR(DecompressionObj__doc__,
"Perform decompression using a standard library compatible API.\n"
);

static void DecompressionObj_dealloc(ZstdDecompressionObj* self) {
	Py_XDECREF(self->decompressor);

	PyObject_Del(self);
}

static PyObject* DecompressionObj_decompress(ZstdDecompressionObj* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		NULL
	};

	Py_buffer source;
	size_t zresult;
	ZSTD_inBuffer input;
	ZSTD_outBuffer output;
	PyObject* result = NULL;
	Py_ssize_t resultSize = 0;

	output.dst = NULL;

	if (self->finished) {
		PyErr_SetString(ZstdError, "cannot use a decompressobj multiple times");
		return NULL;
	}

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:decompress",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*:decompress",
#endif
		kwlist, &source)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
	}

	/* Special case of empty input. Output will always be empty. */
	if (source.len == 0) {
		result = PyBytes_FromString("");
		goto finally;
	}

	input.src = source.buf;
	input.size = source.len;
	input.pos = 0;

	output.dst = PyMem_Malloc(self->outSize);
	if (!output.dst) {
		PyErr_NoMemory();
		goto except;
	}
	output.size = self->outSize;
	output.pos = 0;

	while (1) {
		Py_BEGIN_ALLOW_THREADS
		zresult = ZSTD_decompressStream(self->decompressor->dctx, &output, &input);
		Py_END_ALLOW_THREADS

		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "zstd decompressor error: %s",
				ZSTD_getErrorName(zresult));
			goto except;
		}

		if (0 == zresult) {
			self->finished = 1;
		}

		if (output.pos) {
			if (result) {
				resultSize = PyBytes_GET_SIZE(result);
				if (-1 == safe_pybytes_resize(&result, resultSize + output.pos)) {
					Py_XDECREF(result);
					goto except;
				}

				memcpy(PyBytes_AS_STRING(result) + resultSize,
					output.dst, output.pos);
			}
			else {
				result = PyBytes_FromStringAndSize(output.dst, output.pos);
				if (!result) {
					goto except;
				}
			}
		}

		if (zresult == 0 || (input.pos == input.size && output.pos == 0)) {
			break;
		}

		output.pos = 0;
	}

	if (!result) {
		result = PyBytes_FromString("");
	}

	goto finally;

except:
	Py_CLEAR(result);

finally:
	PyMem_Free(output.dst);
	PyBuffer_Release(&source);

	return result;
}

static PyObject* DecompressionObj_flush(ZstdDecompressionObj* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"length",
		NULL
	};

	PyObject* length = NULL;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:flush", kwlist, &length)) {
	return NULL;
	}

	Py_RETURN_NONE;
}

static PyMethodDef DecompressionObj_methods[] = {
	{ "decompress", (PyCFunction)DecompressionObj_decompress,
	  METH_VARARGS | METH_KEYWORDS, PyDoc_STR("decompress data") },
	{ "flush", (PyCFunction)DecompressionObj_flush,
	  METH_VARARGS | METH_KEYWORDS, PyDoc_STR("no-op") },
	{ NULL, NULL }
};

PyTypeObject ZstdDecompressionObjType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"zstd.ZstdDecompressionObj",    /* tp_name */
	sizeof(ZstdDecompressionObj),   /* tp_basicsize */
	0,                              /* tp_itemsize */
	(destructor)DecompressionObj_dealloc, /* tp_dealloc */
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
	DecompressionObj__doc__,        /* tp_doc */
	0,                              /* tp_traverse */
	0,                              /* tp_clear */
	0,                              /* tp_richcompare */
	0,                              /* tp_weaklistoffset */
	0,                              /* tp_iter */
	0,                              /* tp_iternext */
	DecompressionObj_methods,       /* tp_methods */
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

void decompressobj_module_init(PyObject* module) {
	Py_TYPE(&ZstdDecompressionObjType) = &PyType_Type;
	if (PyType_Ready(&ZstdDecompressionObjType) < 0) {
		return;
	}
}
