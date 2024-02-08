/**
 * Copyright (c) 2017-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include "python-zstandard.h"

extern PyObject *ZstdError;

FrameParametersObject *get_frame_parameters(PyObject *self, PyObject *args,
                                            PyObject *kwargs) {
    static char *kwlist[] = {"data", NULL};

    Py_buffer source;
    ZSTD_frameHeader header;
    FrameParametersObject *result = NULL;
    size_t zresult;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:get_frame_parameters",
                                     kwlist, &source)) {
        return NULL;
    }

    zresult = ZSTD_getFrameHeader(&header, source.buf, source.len);

    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "cannot get frame parameters: %s",
                     ZSTD_getErrorName(zresult));
        goto finally;
    }

    if (zresult) {
        PyErr_Format(ZstdError,
                     "not enough data for frame parameters; need %zu bytes",
                     zresult);
        goto finally;
    }

    result = PyObject_New(FrameParametersObject, FrameParametersType);
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

static void FrameParameters_dealloc(PyObject *self) {
    PyObject_Del(self);
}

static PyMemberDef FrameParameters_members[] = {
    {"content_size", T_ULONGLONG,
     offsetof(FrameParametersObject, frameContentSize), READONLY,
     "frame content size"},
    {"window_size", T_ULONGLONG, offsetof(FrameParametersObject, windowSize),
     READONLY, "window size"},
    {"dict_id", T_UINT, offsetof(FrameParametersObject, dictID), READONLY,
     "dictionary ID"},
    {"has_checksum", T_BOOL, offsetof(FrameParametersObject, checksumFlag),
     READONLY, "checksum flag"},
    {NULL}};

PyType_Slot FrameParametersSlots[] = {
    {Py_tp_dealloc, FrameParameters_dealloc},
    {Py_tp_members, FrameParameters_members},
    {0, NULL},
};

PyType_Spec FrameParametersSpec = {
    "zstd.FrameParameters",
    sizeof(FrameParametersObject),
    0,
    Py_TPFLAGS_DEFAULT,
    FrameParametersSlots,
};

PyTypeObject *FrameParametersType;

void frameparams_module_init(PyObject *mod) {
    FrameParametersType = (PyTypeObject *)PyType_FromSpec(&FrameParametersSpec);
    if (PyType_Ready(FrameParametersType) < 0) {
        return;
    }

    Py_INCREF(FrameParametersType);
    PyModule_AddObject(mod, "FrameParameters", (PyObject *)FrameParametersType);
}
