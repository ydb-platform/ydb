/**
* Copyright (c) 2017-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"

extern PyObject* ZstdError;

PyDoc_STRVAR(FrameParameters__doc__,
	"FrameParameters: information about a zstd frame");

FrameParametersObject* get_frame_parameters(PyObject* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		NULL
	};

	Py_buffer source;
	ZSTD_frameHeader header;
	FrameParametersObject* result = NULL;
	size_t zresult;

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:get_frame_parameters",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*:get_frame_parameters",
#endif
		kwlist, &source)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
	}

	zresult = ZSTD_getFrameHeader(&header, source.buf, source.len);

	if (ZSTD_isError(zresult)) {
		PyErr_Format(ZstdError, "cannot get frame parameters: %s", ZSTD_getErrorName(zresult));
		goto finally;
	}

	if (zresult) {
		PyErr_Format(ZstdError, "not enough data for frame parameters; need %zu bytes", zresult);
		goto finally;
	}

	result = PyObject_New(FrameParametersObject, &FrameParametersType);
	if (!result) {
		goto finally;
	}

	result->frameContentSize = header.frameContentSize;
	result->windowSize = header.windowSize;
	result->dictID = header.dictID;
	result->checksumFlag = header.checksumFlag ? 1 : 0;

finally:
	PyBuffer_Release(&source);
	return result;
}

static void FrameParameters_dealloc(PyObject* self) {
	PyObject_Del(self);
}

static PyMemberDef FrameParameters_members[] = {
	{ "content_size", T_ULONGLONG,
	  offsetof(FrameParametersObject, frameContentSize), READONLY,
	  "frame content size" },
	{ "window_size", T_ULONGLONG,
	  offsetof(FrameParametersObject, windowSize), READONLY,
	  "window size" },
	{ "dict_id", T_UINT,
	  offsetof(FrameParametersObject, dictID), READONLY,
	  "dictionary ID" },
	{ "has_checksum", T_BOOL,
	  offsetof(FrameParametersObject, checksumFlag), READONLY,
	  "checksum flag" },
	{ NULL }
};

PyTypeObject FrameParametersType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"FrameParameters",          /* tp_name */
	sizeof(FrameParametersObject), /* tp_basicsize */
	0,                         /* tp_itemsize */
	(destructor)FrameParameters_dealloc, /* tp_dealloc */
	0,                         /* tp_print */
	0,                         /* tp_getattr */
	0,                         /* tp_setattr */
	0,                         /* tp_compare */
	0,                         /* tp_repr */
	0,                         /* tp_as_number */
	0,                         /* tp_as_sequence */
	0,                         /* tp_as_mapping */
	0,                         /* tp_hash  */
	0,                         /* tp_call */
	0,                         /* tp_str */
	0,                         /* tp_getattro */
	0,                         /* tp_setattro */
	0,                         /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,        /* tp_flags */
	FrameParameters__doc__,    /* tp_doc */
	0,                         /* tp_traverse */
	0,                         /* tp_clear */
	0,                         /* tp_richcompare */
	0,                         /* tp_weaklistoffset */
	0,                         /* tp_iter */
	0,                         /* tp_iternext */
	0,                         /* tp_methods */
	FrameParameters_members,   /* tp_members */
	0,                         /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
	0,                         /* tp_init */
	0,                         /* tp_alloc */
	0,                         /* tp_new */
};

void frameparams_module_init(PyObject* mod) {
	Py_TYPE(&FrameParametersType) = &PyType_Type;
	if (PyType_Ready(&FrameParametersType) < 0) {
		return;
	}

	Py_INCREF(&FrameParametersType);
	PyModule_AddObject(mod, "FrameParameters", (PyObject*)&FrameParametersType);
}
