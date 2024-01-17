/**
 * Copyright (c) 2016-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

ZstdCompressionDict *train_dictionary(PyObject *self, PyObject *args,
                                      PyObject *kwargs) {
    static char *kwlist[] = {
        "dict_size", "samples",     "k",     "d",
        "f",         "split_point", "accel", "notifications",
        "dict_id",   "level",       "steps", "threads",
        NULL};

    size_t capacity;
    PyObject *samples;
    unsigned k = 0;
    unsigned d = 0;
    unsigned f = 0;
    double splitPoint = 0.0;
    unsigned accel = 0;
    unsigned notifications = 0;
    unsigned dictID = 0;
    int level = 0;
    unsigned steps = 0;
    int threads = 0;
    ZDICT_fastCover_params_t params;
    Py_ssize_t samplesLen;
    Py_ssize_t i;
    size_t samplesSize = 0;
    void *sampleBuffer = NULL;
    size_t *sampleSizes = NULL;
    void *sampleOffset;
    Py_ssize_t sampleSize;
    void *dict = NULL;
    size_t zresult;
    ZstdCompressionDict *result = NULL;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "nO!|IIIdIIIiIi:train_dictionary", kwlist, &capacity,
            &PyList_Type, &samples, &k, &d, &f, &splitPoint, &accel,
            &notifications, &dictID, &level, &steps, &threads)) {
        return NULL;
    }

    if (threads < 0) {
        threads = cpu_count();
    }

    if (!steps && !threads) {
        /* Defaults from ZDICT_trainFromBuffer() */
        d = d ? d : 8;
        steps = steps ? steps : 4;
        level = level ? level : 3;
    }

    memset(&params, 0, sizeof(params));
    params.k = k;
    params.d = d;
    params.f = f;
    params.steps = steps;
    params.nbThreads = threads;
    params.splitPoint = splitPoint;
    params.accel = accel;

    params.zParams.compressionLevel = level;
    params.zParams.dictID = dictID;
    params.zParams.notificationLevel = notifications;

    /* Figure out total size of input samples. */
    samplesLen = PyList_Size(samples);
    for (i = 0; i < samplesLen; i++) {
        PyObject *sampleItem = PyList_GET_ITEM(samples, i);

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
        PyObject *sampleItem = PyList_GET_ITEM(samples, i);
        sampleSize = PyBytes_GET_SIZE(sampleItem);
        sampleSizes[i] = sampleSize;
        memcpy(sampleOffset, PyBytes_AS_STRING(sampleItem), sampleSize);
        sampleOffset = (char *)sampleOffset + sampleSize;
    }

    dict = PyMem_Malloc(capacity);
    if (!dict) {
        PyErr_NoMemory();
        goto finally;
    }

    Py_BEGIN_ALLOW_THREADS zresult = ZDICT_optimizeTrainFromBuffer_fastCover(
        dict, capacity, sampleBuffer, sampleSizes, (unsigned)samplesLen,
        &params);
    Py_END_ALLOW_THREADS

        if (ZDICT_isError(zresult)) {
        PyMem_Free(dict);
        PyErr_Format(ZstdError, "cannot train dict: %s",
                     ZDICT_getErrorName(zresult));
        goto finally;
    }

    result = PyObject_New(ZstdCompressionDict, ZstdCompressionDictType);
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

int ensure_ddict(ZstdCompressionDict *dict) {
    if (dict->ddict) {
        return 0;
    }

    Py_BEGIN_ALLOW_THREADS dict->ddict = ZSTD_createDDict_advanced(
        dict->dictData, dict->dictSize, ZSTD_dlm_byRef, dict->dictType,
        ZSTD_defaultCMem);
    Py_END_ALLOW_THREADS if (!dict->ddict) {
        PyErr_SetString(ZstdError, "could not create decompression dict");
        return 1;
    }

    return 0;
}

static int ZstdCompressionDict_init(ZstdCompressionDict *self, PyObject *args,
                                    PyObject *kwargs) {
    static char *kwlist[] = {"data", "dict_type", NULL};

    int result = -1;
    Py_buffer source;
    unsigned dictType = ZSTD_dct_auto;

    self->dictData = NULL;
    self->dictSize = 0;
    self->cdict = NULL;
    self->ddict = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*|I:ZstdCompressionDict",
                                     kwlist, &source, &dictType)) {
        return -1;
    }

    if (dictType != ZSTD_dct_auto && dictType != ZSTD_dct_rawContent &&
        dictType != ZSTD_dct_fullDict) {
        PyErr_Format(
            PyExc_ValueError,
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

static void ZstdCompressionDict_dealloc(ZstdCompressionDict *self) {
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

static PyObject *
ZstdCompressionDict_precompute_compress(ZstdCompressionDict *self,
                                        PyObject *args, PyObject *kwargs) {
    static char *kwlist[] = {"level", "compression_params", NULL};

    int level = 0;
    ZstdCompressionParametersObject *compressionParams = NULL;
    ZSTD_compressionParameters cParams;
    size_t zresult;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "|iO!:precompute_compress", kwlist, &level,
            ZstdCompressionParametersType, &compressionParams)) {
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
                                            ZSTD_dlm_byRef, self->dictType,
                                            cParams, ZSTD_defaultCMem);

    if (!self->cdict) {
        PyErr_SetString(ZstdError, "unable to precompute dictionary");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *ZstdCompressionDict_dict_id(ZstdCompressionDict *self) {
    unsigned dictID = ZDICT_getDictID(self->dictData, self->dictSize);

    return PyLong_FromLong(dictID);
}

static PyObject *ZstdCompressionDict_as_bytes(ZstdCompressionDict *self) {
    return PyBytes_FromStringAndSize(self->dictData, self->dictSize);
}

static PyMethodDef ZstdCompressionDict_methods[] = {
    {"dict_id", (PyCFunction)ZstdCompressionDict_dict_id, METH_NOARGS,
     PyDoc_STR("dict_id() -- obtain the numeric dictionary ID")},
    {"as_bytes", (PyCFunction)ZstdCompressionDict_as_bytes, METH_NOARGS,
     PyDoc_STR("as_bytes() -- obtain the raw bytes constituting the dictionary "
               "data")},
    {"precompute_compress",
     (PyCFunction)ZstdCompressionDict_precompute_compress,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {NULL, NULL}};

static PyMemberDef ZstdCompressionDict_members[] = {
    {"k", T_UINT, offsetof(ZstdCompressionDict, k), READONLY, "segment size"},
    {"d", T_UINT, offsetof(ZstdCompressionDict, d), READONLY, "dmer size"},
    {NULL}};

static Py_ssize_t ZstdCompressionDict_length(ZstdCompressionDict *self) {
    return self->dictSize;
}

PyType_Slot ZstdCompressionDictSlots[] = {
    {Py_tp_dealloc, ZstdCompressionDict_dealloc},
    {Py_sq_length, ZstdCompressionDict_length},
    {Py_tp_methods, ZstdCompressionDict_methods},
    {Py_tp_members, ZstdCompressionDict_members},
    {Py_tp_init, ZstdCompressionDict_init},
    {Py_tp_new, PyType_GenericNew},
    {0, NULL},
};

PyType_Spec ZstdCompressionDictSpec = {
    "zstd.ZstdCompressionDict",
    sizeof(ZstdCompressionDict),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    ZstdCompressionDictSlots,
};

PyTypeObject *ZstdCompressionDictType;

void compressiondict_module_init(PyObject *mod) {
    ZstdCompressionDictType =
        (PyTypeObject *)PyType_FromSpec(&ZstdCompressionDictSpec);
    if (PyType_Ready(ZstdCompressionDictType) < 0) {
        return;
    }

    Py_INCREF((PyObject *)ZstdCompressionDictType);
    PyModule_AddObject(mod, "ZstdCompressionDict",
                       (PyObject *)ZstdCompressionDictType);
}
