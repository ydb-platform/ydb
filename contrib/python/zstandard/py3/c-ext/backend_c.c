/**
 * Copyright (c) 2016-present, Gregory Szorc
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

/* A Python C extension for Zstandard. */

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#elif defined(__APPLE__) || defined(__OpenBSD__) || defined(__FreeBSD__) ||    \
    defined(__NetBSD__) || defined(__DragonFly__)
#include <sys/types.h>

#include <sys/sysctl.h>

#endif

#include "python-zstandard.h"

#include "bufferutil.c"
#include "compressionchunker.c"
#include "compressiondict.c"
#include "compressionparams.c"
#include "compressionreader.c"
#include "compressionwriter.c"
#include "compressobj.c"
#include "compressor.c"
#include "compressoriterator.c"
#include "constants.c"
#include "decompressionreader.c"
#include "decompressionwriter.c"
#include "decompressobj.c"
#include "decompressor.c"
#include "decompressoriterator.c"
#include "frameparams.c"

PyObject *ZstdError;

static PyObject *estimate_decompression_context_size(PyObject *self) {
    return PyLong_FromSize_t(ZSTD_estimateDCtxSize());
}

static PyObject *frame_content_size(PyObject *self, PyObject *args,
                                    PyObject *kwargs) {
    static char *kwlist[] = {"source", NULL};

    Py_buffer source;
    PyObject *result = NULL;
    unsigned long long size;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:frame_content_size",
                                     kwlist, &source)) {
        return NULL;
    }

    size = ZSTD_getFrameContentSize(source.buf, source.len);

    if (size == ZSTD_CONTENTSIZE_ERROR) {
        PyErr_SetString(ZstdError, "error when determining content size");
    }
    else if (size == ZSTD_CONTENTSIZE_UNKNOWN) {
        result = PyLong_FromLong(-1);
    }
    else {
        result = PyLong_FromUnsignedLongLong(size);
    }

    PyBuffer_Release(&source);

    return result;
}

static PyObject *frame_header_size(PyObject *self, PyObject *args,
                                   PyObject *kwargs) {
    static char *kwlist[] = {"source", NULL};

    Py_buffer source;
    PyObject *result = NULL;
    size_t zresult;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:frame_header_size",
                                     kwlist, &source)) {
        return NULL;
    }

    zresult = ZSTD_frameHeaderSize(source.buf, source.len);
    if (ZSTD_isError(zresult)) {
        PyErr_Format(ZstdError, "could not determine frame header size: %s",
                     ZSTD_getErrorName(zresult));
    }
    else {
        result = PyLong_FromSize_t(zresult);
    }

    PyBuffer_Release(&source);

    return result;
}

static char zstd_doc[] = "Interface to zstandard";

static PyMethodDef zstd_methods[] = {
    {"estimate_decompression_context_size",
     (PyCFunction)estimate_decompression_context_size, METH_NOARGS, NULL},
    {"frame_content_size", (PyCFunction)frame_content_size,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"frame_header_size", (PyCFunction)frame_header_size,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"get_frame_parameters", (PyCFunction)get_frame_parameters,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {"train_dictionary", (PyCFunction)train_dictionary,
     METH_VARARGS | METH_KEYWORDS, NULL},
    {NULL, NULL}};

void bufferutil_module_init(PyObject *mod);
void compressobj_module_init(PyObject *mod);
void compressor_module_init(PyObject *mod);
void compressionparams_module_init(PyObject *mod);
void constants_module_init(PyObject *mod);
void compressionchunker_module_init(PyObject *mod);
void compressiondict_module_init(PyObject *mod);
void compressionreader_module_init(PyObject *mod);
void compressionwriter_module_init(PyObject *mod);
void compressoriterator_module_init(PyObject *mod);
void decompressor_module_init(PyObject *mod);
void decompressobj_module_init(PyObject *mod);
void decompressionreader_module_init(PyObject *mod);
void decompressionwriter_module_init(PyObject *mod);
void decompressoriterator_module_init(PyObject *mod);
void frameparams_module_init(PyObject *mod);

void zstd_module_init(PyObject *m) {
    /* python-zstandard relies on unstable zstd C API features. This means
       that changes in zstd may break expectations in python-zstandard.

       python-zstandard is distributed with a copy of the zstd sources.
       python-zstandard is only guaranteed to work with the bundled version
       of zstd.

       However, downstream redistributors or packagers may unbundle zstd
       from python-zstandard. This can result in a mismatch between zstd
       versions and API semantics. This essentially "voids the warranty"
       of python-zstandard and may cause undefined behavior.

       We detect this mismatch here and refuse to load the module if this
       scenario is detected.
    */
    PyObject *features = NULL;
    PyObject *feature = NULL;
    unsigned zstd_ver_no = ZSTD_versionNumber();
    unsigned our_hardcoded_version = 10506;
    if (ZSTD_VERSION_NUMBER != our_hardcoded_version ||
        zstd_ver_no != our_hardcoded_version) {
        PyErr_Format(
            PyExc_ImportError,
            "zstd C API versions mismatch; Python bindings were not "
            "compiled/linked against expected zstd version (%u returned by the "
            "lib, %u hardcoded in zstd headers, %u hardcoded in the cext)",
            zstd_ver_no, ZSTD_VERSION_NUMBER, our_hardcoded_version);
        return;
    }

    features = PySet_New(NULL);
    if (NULL == features) {
        PyErr_SetString(PyExc_ImportError, "could not create empty set");
        return;
    }

    feature = PyUnicode_FromString("buffer_types");
    if (NULL == feature) {
        PyErr_SetString(PyExc_ImportError, "could not create feature string");
        return;
    }

    if (PySet_Add(features, feature) == -1) {
        return;
    }

    Py_DECREF(feature);

#ifdef HAVE_ZSTD_POOL_APIS
    feature = PyUnicode_FromString("multi_compress_to_buffer");
    if (NULL == feature) {
        PyErr_SetString(PyExc_ImportError, "could not create feature string");
        return;
    }

    if (PySet_Add(features, feature) == -1) {
        return;
    }

    Py_DECREF(feature);
#endif

#ifdef HAVE_ZSTD_POOL_APIS
    feature = PyUnicode_FromString("multi_decompress_to_buffer");
    if (NULL == feature) {
        PyErr_SetString(PyExc_ImportError, "could not create feature string");
        return;
    }

    if (PySet_Add(features, feature) == -1) {
        return;
    }

    Py_DECREF(feature);
#endif

    if (PyObject_SetAttrString(m, "backend_features", features) == -1) {
        return;
    }

    Py_DECREF(features);

    bufferutil_module_init(m);
    compressionparams_module_init(m);
    compressiondict_module_init(m);
    compressobj_module_init(m);
    compressor_module_init(m);
    compressionchunker_module_init(m);
    compressionreader_module_init(m);
    compressionwriter_module_init(m);
    compressoriterator_module_init(m);
    constants_module_init(m);
    decompressor_module_init(m);
    decompressobj_module_init(m);
    decompressionreader_module_init(m);
    decompressionwriter_module_init(m);
    decompressoriterator_module_init(m);
    frameparams_module_init(m);
}

#if defined(__GNUC__) && (__GNUC__ >= 4)
#define PYTHON_ZSTD_VISIBILITY __attribute__((visibility("default")))
#else
#define PYTHON_ZSTD_VISIBILITY
#endif

static struct PyModuleDef zstd_module = {PyModuleDef_HEAD_INIT, "backend_c",
                                         zstd_doc, -1, zstd_methods};

PYTHON_ZSTD_VISIBILITY PyMODINIT_FUNC PyInit_backend_c(void) {
    PyObject *m = PyModule_Create(&zstd_module);
    if (m) {
        zstd_module_init(m);
        if (PyErr_Occurred()) {
            Py_DECREF(m);
            m = NULL;
        }
    }
    return m;
}

/* Attempt to resolve the number of CPUs in the system. */
int cpu_count() {
    int count = 0;

#if defined(_WIN32)
    SYSTEM_INFO si;
    si.dwNumberOfProcessors = 0;
    GetSystemInfo(&si);
    count = si.dwNumberOfProcessors;
#elif defined(__APPLE__)
    int num;
    size_t size = sizeof(int);

    if (0 == sysctlbyname("hw.logicalcpu", &num, &size, NULL, 0)) {
        count = num;
    }
#elif defined(__linux__)
    count = sysconf(_SC_NPROCESSORS_ONLN);
#elif defined(__OpenBSD__) || defined(__FreeBSD__) || defined(__NetBSD__) ||   \
    defined(__DragonFly__)
    int mib[2];
    size_t len = sizeof(count);
    mib[0] = CTL_HW;
    mib[1] = HW_NCPU;
    if (0 != sysctl(mib, 2, &count, &len, NULL, 0)) {
        count = 0;
    }
#elif defined(__hpux)
    count = mpctl(MPC_GETNUMSPUS, NULL, NULL);
#endif

    return count;
}

size_t roundpow2(size_t i) {
    i--;
    i |= i >> 1;
    i |= i >> 2;
    i |= i >> 4;
    i |= i >> 8;
    i |= i >> 16;
    i++;

    return i;
}

/* Safer version of _PyBytes_Resize().
 *
 * _PyBytes_Resize() only works if the refcount is 1. In some scenarios,
 * we can get an object with a refcount > 1, even if it was just created
 * with PyBytes_FromStringAndSize()! That's because (at least) CPython
 * pre-allocates PyBytes instances of size 1 for every possible byte value.
 *
 * If non-0 is returned, obj may or may not be NULL.
 */
int safe_pybytes_resize(PyObject **obj, Py_ssize_t size) {
    PyObject *tmp;

    if ((*obj)->ob_refcnt == 1) {
        return _PyBytes_Resize(obj, size);
    }

    tmp = PyBytes_FromStringAndSize(NULL, size);
    if (!tmp) {
        return -1;
    }

    memcpy(PyBytes_AS_STRING(tmp), PyBytes_AS_STRING(*obj),
           PyBytes_GET_SIZE(*obj));

    Py_DECREF(*obj);
    *obj = tmp;

    return 0;
}

// Set/raise an `io.UnsupportedOperation` exception.
void set_io_unsupported_operation(void) {
    PyObject *iomod;
    PyObject *exc;

    iomod = PyImport_ImportModule("io");
    if (NULL == iomod) {
        return;
    }

    exc = PyObject_GetAttrString(iomod, "UnsupportedOperation");
    if (NULL == exc) {
        Py_DECREF(iomod);
        return;
    }

    PyErr_SetNone(exc);
    Py_DECREF(exc);
    Py_DECREF(iomod);
}
