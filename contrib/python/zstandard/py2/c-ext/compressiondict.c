/**
* Copyright (c) 2016-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"

extern PyObject* ZstdError;

ZstdCompressionDict* train_dictionary(PyObject* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"dict_size",
		"samples",
		"k",
		"d",
		"notifications",
		"dict_id",
		"level",
		"steps",
		"threads",
		NULL
	};

	size_t capacity;
	PyObject* samples;
	unsigned k = 0;
	unsigned d = 0;
	unsigned notifications = 0;
	unsigned dictID = 0;
	int level = 0;
	unsigned steps = 0;
	int threads = 0;
	ZDICT_cover_params_t params;
	Py_ssize_t samplesLen;
	Py_ssize_t i;
	size_t samplesSize = 0;
	void* sampleBuffer = NULL;
	size_t* sampleSizes = NULL;
	void* sampleOffset;
	Py_ssize_t sampleSize;
	void* dict = NULL;
	size_t zresult;
	ZstdCompressionDict* result = NULL;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "nO!|IIIIiIi:train_dictionary",
		kwlist, &capacity, &PyList_Type, &samples,
		&k, &d, &notifications, &dictID, &level, &steps, &threads)) {
		return NULL;
	}

	if (threads < 0) {
		threads = cpu_count();
	}

	memset(&params, 0, sizeof(params));
	params.k = k;
	params.d = d;
	params.steps = steps;
	params.nbThreads = threads;
	params.zParams.notificationLevel = notifications;
	params.zParams.dictID = dictID;
	params.zParams.compressionLevel = level;

	/* Figure out total size of input samples. */
	samplesLen = PyList_Size(samples);
	for (i = 0; i < samplesLen; i++) {
		PyObject* sampleItem = PyList_GET_ITEM(samples, i);

		if (!PyBytes_Check(sampleItem)) {
			PyErr_SetString(PyExc_ValueError, "samples must be bytes");
			return NULL;
		}
		samplesSize += PyBytes_GET_SIZE(sampleItem);
	}

	sampleBuffer = PyMem_Malloc(samplesSize);
	if (!sampleBuffer) {
		PyErr_NoMemory();
		goto finally;
	}

	sampleSizes = PyMem_Malloc(samplesLen * sizeof(size_t));
	if (!sampleSizes) {
		PyErr_NoMemory();
		goto finally;
	}

	sampleOffset = sampleBuffer;
	for (i = 0; i < samplesLen; i++) {
		PyObject* sampleItem = PyList_GET_ITEM(samples, i);
		sampleSize = PyBytes_GET_SIZE(sampleItem);
		sampleSizes[i] = sampleSize;
		memcpy(sampleOffset, PyBytes_AS_STRING(sampleItem), sampleSize);
		sampleOffset = (char*)sampleOffset + sampleSize;
	}

	dict = PyMem_Malloc(capacity);
	if (!dict) {
		PyErr_NoMemory();
		goto finally;
	}

	Py_BEGIN_ALLOW_THREADS
	/* No parameters uses the default function, which will use default params
	   and call ZDICT_optimizeTrainFromBuffer_cover under the hood. */
	if (!params.k && !params.d && !params.zParams.compressionLevel
		&& !params.zParams.notificationLevel && !params.zParams.dictID) {
		zresult = ZDICT_trainFromBuffer(dict, capacity, sampleBuffer,
			sampleSizes, (unsigned)samplesLen);
	}
	/* Use optimize mode if user controlled steps or threads explicitly. */
	else if (params.steps || params.nbThreads) {
		zresult = ZDICT_optimizeTrainFromBuffer_cover(dict, capacity,
			sampleBuffer, sampleSizes, (unsigned)samplesLen, &params);
	}
	/* Non-optimize mode with explicit control. */
	else {
		zresult = ZDICT_trainFromBuffer_cover(dict, capacity,
			sampleBuffer, sampleSizes, (unsigned)samplesLen, params);
	}
	Py_END_ALLOW_THREADS

	if (ZDICT_isError(zresult)) {
		PyMem_Free(dict);
		PyErr_Format(ZstdError, "cannot train dict: %s", ZDICT_getErrorName(zresult));
		goto finally;
	}

	result = PyObject_New(ZstdCompressionDict, &ZstdCompressionDictType);
	if (!result) {
		PyMem_Free(dict);
		goto finally;
	}

	result->dictData = dict;
	result->dictSize = zresult;
	result->dictType = ZSTD_dct_fullDict;
	result->d = params.d;
	result->k = params.k;
	result->cdict = NULL;
	result->ddict = NULL;

finally:
	PyMem_Free(sampleBuffer);
	PyMem_Free(sampleSizes);

	return result;
}

int ensure_ddict(ZstdCompressionDict* dict) {
	if (dict->ddict) {
		return 0;
	}

	Py_BEGIN_ALLOW_THREADS
	dict->ddict = ZSTD_createDDict_advanced(dict->dictData, dict->dictSize,
		ZSTD_dlm_byRef, dict->dictType, ZSTD_defaultCMem);
	Py_END_ALLOW_THREADS
	if (!dict->ddict) {
		PyErr_SetString(ZstdError, "could not create decompression dict");
		return 1;
	}

	return 0;
}

PyDoc_STRVAR(ZstdCompressionDict__doc__,
"ZstdCompressionDict(data) - Represents a computed compression dictionary\n"
"\n"
"This type holds the results of a computed Zstandard compression dictionary.\n"
"Instances are obtained by calling ``train_dictionary()`` or by passing\n"
"bytes obtained from another source into the constructor.\n"
);

static int ZstdCompressionDict_init(ZstdCompressionDict* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"data",
		"dict_type",
		NULL
	};

	int result = -1;
	Py_buffer source;
	unsigned dictType = ZSTD_dct_auto;

	self->dictData = NULL;
	self->dictSize = 0;
	self->cdict = NULL;
	self->ddict = NULL;

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*|I:ZstdCompressionDict",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*|I:ZstdCompressionDict",
#endif
		kwlist, &source, &dictType)) {
		return -1;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
	}

	if (dictType != ZSTD_dct_auto && dictType != ZSTD_dct_rawContent
		&& dictType != ZSTD_dct_fullDict) {
		PyErr_Format(PyExc_ValueError,
			"invalid dictionary load mode: %d; must use DICT_TYPE_* constants",
			dictType);
		goto finally;
	}

	self->dictType = dictType;

	self->dictData = PyMem_Malloc(source.len);
	if (!self->dictData) {
		PyErr_NoMemory();
		goto finally;
	}

	memcpy(self->dictData, source.buf, source.len);
	self->dictSize = source.len;

	result = 0;

finally:
	PyBuffer_Release(&source);
	return result;
}

static void ZstdCompressionDict_dealloc(ZstdCompressionDict* self) {
	if (self->cdict) {
		ZSTD_freeCDict(self->cdict);
		self->cdict = NULL;
	}

	if (self->ddict) {
		ZSTD_freeDDict(self->ddict);
		self->ddict = NULL;
	}

	if (self->dictData) {
		PyMem_Free(self->dictData);
		self->dictData = NULL;
	}

	PyObject_Del(self);
}

PyDoc_STRVAR(ZstdCompressionDict_precompute_compress__doc__,
"Precompute a dictionary so it can be used by multiple compressors.\n"
);

static PyObject* ZstdCompressionDict_precompute_compress(ZstdCompressionDict* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"level",
		"compression_params",
		NULL
	};

	int level = 0;
	ZstdCompressionParametersObject* compressionParams = NULL;
	ZSTD_compressionParameters cParams;
	size_t zresult;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iO!:precompute_compress", kwlist,
		&level, &ZstdCompressionParametersType, &compressionParams)) {
		return NULL;
	}

	if (level && compressionParams) {
		PyErr_SetString(PyExc_ValueError,
			"must only specify one of level or compression_params");
		return NULL;
	}

	if (!level && !compressionParams) {
		PyErr_SetString(PyExc_ValueError,
			"must specify one of level or compression_params");
		return NULL;
	}

	if (self->cdict) {
		zresult = ZSTD_freeCDict(self->cdict);
		self->cdict = NULL;
		if (ZSTD_isError(zresult)) {
			PyErr_Format(ZstdError, "unable to free CDict: %s",
				ZSTD_getErrorName(zresult));
			return NULL;
		}
	}

	if (level) {
		cParams = ZSTD_getCParams(level, 0, self->dictSize);
	}
	else {
		if (to_cparams(compressionParams, &cParams)) {
			return NULL;
		}
	}

	assert(!self->cdict);
	self->cdict = ZSTD_createCDict_advanced(self->dictData, self->dictSize,
		ZSTD_dlm_byRef, self->dictType, cParams, ZSTD_defaultCMem);

	if (!self->cdict) {
		PyErr_SetString(ZstdError, "unable to precompute dictionary");
		return NULL;
	}

	Py_RETURN_NONE;
}

static PyObject* ZstdCompressionDict_dict_id(ZstdCompressionDict* self) {
	unsigned dictID = ZDICT_getDictID(self->dictData, self->dictSize);

	return PyLong_FromLong(dictID);
}

static PyObject* ZstdCompressionDict_as_bytes(ZstdCompressionDict* self) {
	return PyBytes_FromStringAndSize(self->dictData, self->dictSize);
}

static PyMethodDef ZstdCompressionDict_methods[] = {
	{ "dict_id", (PyCFunction)ZstdCompressionDict_dict_id, METH_NOARGS,
	PyDoc_STR("dict_id() -- obtain the numeric dictionary ID") },
	{ "as_bytes", (PyCFunction)ZstdCompressionDict_as_bytes, METH_NOARGS,
	PyDoc_STR("as_bytes() -- obtain the raw bytes constituting the dictionary data") },
	{ "precompute_compress", (PyCFunction)ZstdCompressionDict_precompute_compress,
	METH_VARARGS | METH_KEYWORDS, ZstdCompressionDict_precompute_compress__doc__ },
	{ NULL, NULL }
};

static PyMemberDef ZstdCompressionDict_members[] = {
	{ "k", T_UINT, offsetof(ZstdCompressionDict, k), READONLY,
	  "segment size" },
	{ "d", T_UINT, offsetof(ZstdCompressionDict, d), READONLY,
	  "dmer size" },
	{ NULL }
};

static Py_ssize_t ZstdCompressionDict_length(ZstdCompressionDict* self) {
	return self->dictSize;
}

static PySequenceMethods ZstdCompressionDict_sq = {
	(lenfunc)ZstdCompressionDict_length, /* sq_length */
	0,                                   /* sq_concat */
	0,                                   /* sq_repeat */
	0,                                   /* sq_item */
	0,                                   /* sq_ass_item */
	0,                                   /* sq_contains */
	0,                                   /* sq_inplace_concat */
	0                                    /* sq_inplace_repeat */
};

PyTypeObject ZstdCompressionDictType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"zstd.ZstdCompressionDict",     /* tp_name */
	sizeof(ZstdCompressionDict),    /* tp_basicsize */
	0,                              /* tp_itemsize */
	(destructor)ZstdCompressionDict_dealloc, /* tp_dealloc */
	0,                              /* tp_print */
	0,                              /* tp_getattr */
	0,                              /* tp_setattr */
	0,                              /* tp_compare */
	0,                              /* tp_repr */
	0,                              /* tp_as_number */
	&ZstdCompressionDict_sq,        /* tp_as_sequence */
	0,                              /* tp_as_mapping */
	0,                              /* tp_hash */
	0,                              /* tp_call */
	0,                              /* tp_str */
	0,                              /* tp_getattro */
	0,                              /* tp_setattro */
	0,                              /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
	ZstdCompressionDict__doc__,     /* tp_doc */
	0,                              /* tp_traverse */
	0,                              /* tp_clear */
	0,                              /* tp_richcompare */
	0,                              /* tp_weaklistoffset */
	0,                              /* tp_iter */
	0,                              /* tp_iternext */
	ZstdCompressionDict_methods,    /* tp_methods */
	ZstdCompressionDict_members,    /* tp_members */
	0,                              /* tp_getset */
	0,                              /* tp_base */
	0,                              /* tp_dict */
	0,                              /* tp_descr_get */
	0,                              /* tp_descr_set */
	0,                              /* tp_dictoffset */
	(initproc)ZstdCompressionDict_init, /* tp_init */
	0,                              /* tp_alloc */
	PyType_GenericNew,              /* tp_new */
};

void compressiondict_module_init(PyObject* mod) {
	Py_TYPE(&ZstdCompressionDictType) = &PyType_Type;
	if (PyType_Ready(&ZstdCompressionDictType) < 0) {
		return;
	}

	Py_INCREF((PyObject*)&ZstdCompressionDictType);
	PyModule_AddObject(mod, "ZstdCompressionDict",
		(PyObject*)&ZstdCompressionDictType);
}
