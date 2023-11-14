/**
* Copyright (c) 2016-present, Gregory Szorc
* All rights reserved.
*
* This software may be modified and distributed under the terms
* of the BSD license. See the LICENSE file for details.
*/

#include "python-zstandard.h"

extern PyObject* ZstdError;

int set_parameter(ZSTD_CCtx_params* params, ZSTD_cParameter param, int value) {
	size_t zresult = ZSTD_CCtxParams_setParameter(params, param, value);
	if (ZSTD_isError(zresult)) {
		PyErr_Format(ZstdError, "unable to set compression context parameter: %s",
			ZSTD_getErrorName(zresult));
		return 1;
	}

	return 0;
}

#define TRY_SET_PARAMETER(params, param, value) if (set_parameter(params, param, value)) return -1;

#define TRY_COPY_PARAMETER(source, dest, param) { \
	int result; \
	size_t zresult = ZSTD_CCtxParams_getParameter(source, param, &result); \
	if (ZSTD_isError(zresult)) { \
		return 1; \
	} \
	zresult = ZSTD_CCtxParams_setParameter(dest, param, result); \
	if (ZSTD_isError(zresult)) { \
		return 1; \
	} \
}

int set_parameters(ZSTD_CCtx_params* params, ZstdCompressionParametersObject* obj) {
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_nbWorkers);

	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_format);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_compressionLevel);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_windowLog);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_hashLog);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_chainLog);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_searchLog);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_minMatch);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_targetLength);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_strategy);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_contentSizeFlag);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_checksumFlag);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_dictIDFlag);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_jobSize);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_overlapLog);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_forceMaxWindow);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_enableLongDistanceMatching);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_ldmHashLog);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_ldmMinMatch);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_ldmBucketSizeLog);
	TRY_COPY_PARAMETER(obj->params, params, ZSTD_c_ldmHashRateLog);

	return 0;
}

int reset_params(ZstdCompressionParametersObject* params) {
	if (params->params) {
		ZSTD_CCtxParams_reset(params->params);
	}
	else {
		params->params = ZSTD_createCCtxParams();
		if (!params->params) {
			PyErr_NoMemory();
			return 1;
		}
	}

	return set_parameters(params->params, params);
}

#define TRY_GET_PARAMETER(params, param, value) { \
    size_t zresult = ZSTD_CCtxParams_getParameter(params, param, value); \
    if (ZSTD_isError(zresult)) { \
        PyErr_Format(ZstdError, "unable to retrieve parameter: %s", ZSTD_getErrorName(zresult)); \
        return 1; \
    } \
}

int to_cparams(ZstdCompressionParametersObject* params, ZSTD_compressionParameters* cparams) {
	int value;

	TRY_GET_PARAMETER(params->params, ZSTD_c_windowLog, &value);
	cparams->windowLog = value;

	TRY_GET_PARAMETER(params->params, ZSTD_c_chainLog, &value);
	cparams->chainLog = value;

	TRY_GET_PARAMETER(params->params, ZSTD_c_hashLog, &value);
	cparams->hashLog = value;

	TRY_GET_PARAMETER(params->params, ZSTD_c_searchLog, &value);
	cparams->searchLog = value;

	TRY_GET_PARAMETER(params->params, ZSTD_c_minMatch, &value);
	cparams->minMatch = value;

	TRY_GET_PARAMETER(params->params, ZSTD_c_targetLength, &value);
	cparams->targetLength = value;

	TRY_GET_PARAMETER(params->params, ZSTD_c_strategy, &value);
	cparams->strategy = value;

	return 0;
}

static int ZstdCompressionParameters_init(ZstdCompressionParametersObject* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"format",
		"compression_level",
		"window_log",
		"hash_log",
		"chain_log",
		"search_log",
		"min_match",
		"target_length",
		"compression_strategy",
		"strategy",
		"write_content_size",
		"write_checksum",
		"write_dict_id",
		"job_size",
		"overlap_log",
		"overlap_size_log",
		"force_max_window",
		"enable_ldm",
		"ldm_hash_log",
		"ldm_min_match",
		"ldm_bucket_size_log",
		"ldm_hash_rate_log",
		"ldm_hash_every_log",
		"threads",
		NULL
	};

	int format = 0;
	int compressionLevel = 0;
	int windowLog = 0;
	int hashLog = 0;
	int chainLog = 0;
	int searchLog = 0;
	int minMatch = 0;
	int targetLength = 0;
	int compressionStrategy = -1;
	int strategy = -1;
	int contentSizeFlag = 1;
	int checksumFlag = 0;
	int dictIDFlag = 0;
	int jobSize = 0;
	int overlapLog = -1;
	int overlapSizeLog = -1;
	int forceMaxWindow = 0;
	int enableLDM = 0;
	int ldmHashLog = 0;
	int ldmMinMatch = 0;
	int ldmBucketSizeLog = 0;
	int ldmHashRateLog = -1;
	int ldmHashEveryLog = -1;
	int threads = 0;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs,
		"|iiiiiiiiiiiiiiiiiiiiiiii:CompressionParameters",
		kwlist, &format, &compressionLevel, &windowLog, &hashLog, &chainLog,
		&searchLog, &minMatch, &targetLength, &compressionStrategy, &strategy,
		&contentSizeFlag, &checksumFlag, &dictIDFlag, &jobSize, &overlapLog,
		&overlapSizeLog, &forceMaxWindow, &enableLDM, &ldmHashLog, &ldmMinMatch,
		&ldmBucketSizeLog, &ldmHashRateLog, &ldmHashEveryLog, &threads)) {
		return -1;
	}

	if (reset_params(self)) {
		return -1;
	}

	if (threads < 0) {
		threads = cpu_count();
	}

	/* We need to set ZSTD_c_nbWorkers before ZSTD_c_jobSize and ZSTD_c_overlapLog
	 * because setting ZSTD_c_nbWorkers resets the other parameters. */
	TRY_SET_PARAMETER(self->params, ZSTD_c_nbWorkers, threads);

	TRY_SET_PARAMETER(self->params, ZSTD_c_format, format);
	TRY_SET_PARAMETER(self->params, ZSTD_c_compressionLevel, compressionLevel);
	TRY_SET_PARAMETER(self->params, ZSTD_c_windowLog, windowLog);
	TRY_SET_PARAMETER(self->params, ZSTD_c_hashLog, hashLog);
	TRY_SET_PARAMETER(self->params, ZSTD_c_chainLog, chainLog);
	TRY_SET_PARAMETER(self->params, ZSTD_c_searchLog, searchLog);
	TRY_SET_PARAMETER(self->params, ZSTD_c_minMatch, minMatch);
	TRY_SET_PARAMETER(self->params, ZSTD_c_targetLength, targetLength);

	if (compressionStrategy != -1 && strategy != -1) {
		PyErr_SetString(PyExc_ValueError, "cannot specify both compression_strategy and strategy");
		return -1;
    }

	if (compressionStrategy != -1) {
		strategy = compressionStrategy;
	}
	else if (strategy == -1) {
		strategy = 0;
	}

	TRY_SET_PARAMETER(self->params, ZSTD_c_strategy, strategy);
	TRY_SET_PARAMETER(self->params, ZSTD_c_contentSizeFlag, contentSizeFlag);
	TRY_SET_PARAMETER(self->params, ZSTD_c_checksumFlag, checksumFlag);
	TRY_SET_PARAMETER(self->params, ZSTD_c_dictIDFlag, dictIDFlag);
	TRY_SET_PARAMETER(self->params, ZSTD_c_jobSize, jobSize);

	if (overlapLog != -1 && overlapSizeLog != -1) {
		PyErr_SetString(PyExc_ValueError, "cannot specify both overlap_log and overlap_size_log");
		return -1;
	}

	if (overlapSizeLog != -1) {
		overlapLog = overlapSizeLog;
	}
	else if (overlapLog == -1) {
		overlapLog = 0;
	}

	TRY_SET_PARAMETER(self->params, ZSTD_c_overlapLog, overlapLog);
	TRY_SET_PARAMETER(self->params, ZSTD_c_forceMaxWindow, forceMaxWindow);
	TRY_SET_PARAMETER(self->params, ZSTD_c_enableLongDistanceMatching, enableLDM);
	TRY_SET_PARAMETER(self->params, ZSTD_c_ldmHashLog, ldmHashLog);
	TRY_SET_PARAMETER(self->params, ZSTD_c_ldmMinMatch, ldmMinMatch);
	TRY_SET_PARAMETER(self->params, ZSTD_c_ldmBucketSizeLog, ldmBucketSizeLog);

	if (ldmHashRateLog != -1 && ldmHashEveryLog != -1) {
		PyErr_SetString(PyExc_ValueError, "cannot specify both ldm_hash_rate_log and ldm_hash_everyLog");
		return -1;
	}

	if (ldmHashEveryLog != -1) {
		ldmHashRateLog = ldmHashEveryLog;
	}
	else if (ldmHashRateLog == -1) {
		ldmHashRateLog = 0;
	}

	TRY_SET_PARAMETER(self->params, ZSTD_c_ldmHashRateLog, ldmHashRateLog);

	return 0;
}

PyDoc_STRVAR(ZstdCompressionParameters_from_level__doc__,
"Create a CompressionParameters from a compression level and target sizes\n"
);

ZstdCompressionParametersObject* CompressionParameters_from_level(PyObject* undef, PyObject* args, PyObject* kwargs) {
	int managedKwargs = 0;
	int level;
	PyObject* sourceSize = NULL;
	PyObject* dictSize = NULL;
	unsigned PY_LONG_LONG iSourceSize = 0;
	Py_ssize_t iDictSize = 0;
	PyObject* val;
	ZSTD_compressionParameters params;
	ZstdCompressionParametersObject* result = NULL;
	int res;

	if (!PyArg_ParseTuple(args, "i:from_level",
		&level)) {
		return NULL;
	}

	if (!kwargs) {
		kwargs = PyDict_New();
		if (!kwargs) {
			return NULL;
		}
		managedKwargs = 1;
	}

	sourceSize = PyDict_GetItemString(kwargs, "source_size");
	if (sourceSize) {
#if PY_MAJOR_VERSION >= 3
		iSourceSize = PyLong_AsUnsignedLongLong(sourceSize);
		if (iSourceSize == (unsigned PY_LONG_LONG)(-1)) {
			goto cleanup;
		}
#else
		iSourceSize = PyInt_AsUnsignedLongLongMask(sourceSize);
#endif

		PyDict_DelItemString(kwargs, "source_size");
	}

	dictSize = PyDict_GetItemString(kwargs, "dict_size");
	if (dictSize) {
#if PY_MAJOR_VERSION >= 3
		iDictSize = PyLong_AsSsize_t(dictSize);
#else
		iDictSize = PyInt_AsSsize_t(dictSize);
#endif
		if (iDictSize == -1) {
			goto cleanup;
		}

		PyDict_DelItemString(kwargs, "dict_size");
	}


	params = ZSTD_getCParams(level, iSourceSize, iDictSize);

	/* Values derived from the input level and sizes are passed along to the
	   constructor. But only if a value doesn't already exist. */
	val = PyDict_GetItemString(kwargs, "window_log");
	if (!val) {
		val = PyLong_FromUnsignedLong(params.windowLog);
		if (!val) {
			goto cleanup;
		}
		PyDict_SetItemString(kwargs, "window_log", val);
		Py_DECREF(val);
	}

	val = PyDict_GetItemString(kwargs, "chain_log");
	if (!val) {
		val = PyLong_FromUnsignedLong(params.chainLog);
		if (!val) {
			goto cleanup;
		}
		PyDict_SetItemString(kwargs, "chain_log", val);
		Py_DECREF(val);
	}

	val = PyDict_GetItemString(kwargs, "hash_log");
	if (!val) {
		val = PyLong_FromUnsignedLong(params.hashLog);
		if (!val) {
			goto cleanup;
		}
		PyDict_SetItemString(kwargs, "hash_log", val);
		Py_DECREF(val);
	}

	val = PyDict_GetItemString(kwargs, "search_log");
	if (!val) {
		val = PyLong_FromUnsignedLong(params.searchLog);
		if (!val) {
			goto cleanup;
		}
		PyDict_SetItemString(kwargs, "search_log", val);
		Py_DECREF(val);
	}

	val = PyDict_GetItemString(kwargs, "min_match");
	if (!val) {
		val = PyLong_FromUnsignedLong(params.minMatch);
		if (!val) {
			goto cleanup;
		}
		PyDict_SetItemString(kwargs, "min_match", val);
		Py_DECREF(val);
	}

	val = PyDict_GetItemString(kwargs, "target_length");
	if (!val) {
		val = PyLong_FromUnsignedLong(params.targetLength);
		if (!val) {
			goto cleanup;
		}
		PyDict_SetItemString(kwargs, "target_length", val);
		Py_DECREF(val);
	}

	val = PyDict_GetItemString(kwargs, "compression_strategy");
	if (!val) {
		val = PyLong_FromUnsignedLong(params.strategy);
		if (!val) {
			goto cleanup;
		}
		PyDict_SetItemString(kwargs, "compression_strategy", val);
		Py_DECREF(val);
	}

	result = PyObject_New(ZstdCompressionParametersObject, &ZstdCompressionParametersType);
	if (!result) {
		goto cleanup;
	}

	result->params = NULL;

	val = PyTuple_New(0);
	if (!val) {
		Py_CLEAR(result);
		goto cleanup;
	}

	res = ZstdCompressionParameters_init(result, val, kwargs);
	Py_DECREF(val);

	if (res) {
		Py_CLEAR(result);
		goto cleanup;
	}

cleanup:
	if (managedKwargs) {
		Py_DECREF(kwargs);
	}

	return result;
}

PyDoc_STRVAR(ZstdCompressionParameters_estimated_compression_context_size__doc__,
"Estimate the size in bytes of a compression context for compression parameters\n"
);

PyObject* ZstdCompressionParameters_estimated_compression_context_size(ZstdCompressionParametersObject* self) {
	return PyLong_FromSize_t(ZSTD_estimateCCtxSize_usingCCtxParams(self->params));
}

PyDoc_STRVAR(ZstdCompressionParameters__doc__,
"ZstdCompressionParameters: low-level control over zstd compression");

static void ZstdCompressionParameters_dealloc(ZstdCompressionParametersObject* self) {
	if (self->params) {
		ZSTD_freeCCtxParams(self->params);
		self->params = NULL;
	}

	PyObject_Del(self);
}

#define PARAM_GETTER(name, param) PyObject* ZstdCompressionParameters_get_##name(PyObject* self, void* unused) { \
    int result; \
    size_t zresult; \
    ZstdCompressionParametersObject* p = (ZstdCompressionParametersObject*)(self); \
    zresult = ZSTD_CCtxParams_getParameter(p->params, param, &result); \
    if (ZSTD_isError(zresult)) { \
        PyErr_Format(ZstdError, "unable to get compression parameter: %s", \
            ZSTD_getErrorName(zresult)); \
        return NULL; \
    } \
    return PyLong_FromLong(result); \
}

PARAM_GETTER(format, ZSTD_c_format)
PARAM_GETTER(compression_level, ZSTD_c_compressionLevel)
PARAM_GETTER(window_log, ZSTD_c_windowLog)
PARAM_GETTER(hash_log, ZSTD_c_hashLog)
PARAM_GETTER(chain_log, ZSTD_c_chainLog)
PARAM_GETTER(search_log, ZSTD_c_searchLog)
PARAM_GETTER(min_match, ZSTD_c_minMatch)
PARAM_GETTER(target_length, ZSTD_c_targetLength)
PARAM_GETTER(compression_strategy, ZSTD_c_strategy)
PARAM_GETTER(write_content_size, ZSTD_c_contentSizeFlag)
PARAM_GETTER(write_checksum, ZSTD_c_checksumFlag)
PARAM_GETTER(write_dict_id, ZSTD_c_dictIDFlag)
PARAM_GETTER(job_size, ZSTD_c_jobSize)
PARAM_GETTER(overlap_log, ZSTD_c_overlapLog)
PARAM_GETTER(force_max_window, ZSTD_c_forceMaxWindow)
PARAM_GETTER(enable_ldm, ZSTD_c_enableLongDistanceMatching)
PARAM_GETTER(ldm_hash_log, ZSTD_c_ldmHashLog)
PARAM_GETTER(ldm_min_match, ZSTD_c_ldmMinMatch)
PARAM_GETTER(ldm_bucket_size_log, ZSTD_c_ldmBucketSizeLog)
PARAM_GETTER(ldm_hash_rate_log, ZSTD_c_ldmHashRateLog)
PARAM_GETTER(threads, ZSTD_c_nbWorkers)

static PyMethodDef ZstdCompressionParameters_methods[] = {
	{
		"from_level",
		(PyCFunction)CompressionParameters_from_level,
		METH_VARARGS | METH_KEYWORDS | METH_STATIC,
		ZstdCompressionParameters_from_level__doc__
	},
	{
		"estimated_compression_context_size",
		(PyCFunction)ZstdCompressionParameters_estimated_compression_context_size,
		METH_NOARGS,
		ZstdCompressionParameters_estimated_compression_context_size__doc__
	},
	{ NULL, NULL }
};

#define GET_SET_ENTRY(name) { #name, ZstdCompressionParameters_get_##name, NULL, NULL, NULL }

static PyGetSetDef ZstdCompressionParameters_getset[] = {
	GET_SET_ENTRY(format),
	GET_SET_ENTRY(compression_level),
	GET_SET_ENTRY(window_log),
	GET_SET_ENTRY(hash_log),
	GET_SET_ENTRY(chain_log),
	GET_SET_ENTRY(search_log),
	GET_SET_ENTRY(min_match),
	GET_SET_ENTRY(target_length),
	GET_SET_ENTRY(compression_strategy),
	GET_SET_ENTRY(write_content_size),
	GET_SET_ENTRY(write_checksum),
	GET_SET_ENTRY(write_dict_id),
	GET_SET_ENTRY(threads),
	GET_SET_ENTRY(job_size),
	GET_SET_ENTRY(overlap_log),
	/* TODO remove this deprecated attribute */
	{ "overlap_size_log", ZstdCompressionParameters_get_overlap_log, NULL, NULL, NULL },
	GET_SET_ENTRY(force_max_window),
	GET_SET_ENTRY(enable_ldm),
	GET_SET_ENTRY(ldm_hash_log),
	GET_SET_ENTRY(ldm_min_match),
	GET_SET_ENTRY(ldm_bucket_size_log),
	GET_SET_ENTRY(ldm_hash_rate_log),
	/* TODO remove this deprecated attribute */
	{ "ldm_hash_every_log", ZstdCompressionParameters_get_ldm_hash_rate_log, NULL, NULL, NULL },
	{ NULL }
};

PyTypeObject ZstdCompressionParametersType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"ZstdCompressionParameters", /* tp_name */
	sizeof(ZstdCompressionParametersObject), /* tp_basicsize */
	0,                         /* tp_itemsize */
	(destructor)ZstdCompressionParameters_dealloc, /* tp_dealloc */
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
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
	ZstdCompressionParameters__doc__, /* tp_doc */
	0,                         /* tp_traverse */
	0,                         /* tp_clear */
	0,                         /* tp_richcompare */
	0,                         /* tp_weaklistoffset */
	0,                         /* tp_iter */
	0,                         /* tp_iternext */
	ZstdCompressionParameters_methods, /* tp_methods */
	0,                          /* tp_members */
	ZstdCompressionParameters_getset,  /* tp_getset */
	0,                         /* tp_base */
	0,                         /* tp_dict */
	0,                         /* tp_descr_get */
	0,                         /* tp_descr_set */
	0,                         /* tp_dictoffset */
	(initproc)ZstdCompressionParameters_init, /* tp_init */
	0,                         /* tp_alloc */
	PyType_GenericNew,         /* tp_new */
};

void compressionparams_module_init(PyObject* mod) {
	Py_TYPE(&ZstdCompressionParametersType) = &PyType_Type;
	if (PyType_Ready(&ZstdCompressionParametersType) < 0) {
		return;
	}

	Py_INCREF(&ZstdCompressionParametersType);
	PyModule_AddObject(mod, "ZstdCompressionParameters",
		(PyObject*)&ZstdCompressionParametersType);

	/* TODO remove deprecated alias. */
	Py_INCREF(&ZstdCompressionParametersType);
	PyModule_AddObject(mod, "CompressionParameters",
		(PyObject*)&ZstdCompressionParametersType);
}
