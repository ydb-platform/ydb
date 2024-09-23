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
#elif defined(__APPLE__) || defined(__OpenBSD__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__DragonFly__)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

#include "python-zstandard.h"

PyObject *ZstdError;

PyDoc_STRVAR(estimate_decompression_context_size__doc__,
"estimate_decompression_context_size()\n"
"\n"
"Estimate the amount of memory allocated to a decompression context.\n"
);

static PyObject* estimate_decompression_context_size(PyObject* self) {
	return PyLong_FromSize_t(ZSTD_estimateDCtxSize());
}

PyDoc_STRVAR(frame_content_size__doc__,
"frame_content_size(data)\n"
"\n"
"Obtain the decompressed size of a frame."
);

static PyObject* frame_content_size(PyObject* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"source",
		NULL
	};

	Py_buffer source;
	PyObject* result = NULL;
	unsigned long long size;

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:frame_content_size",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*:frame_content_size",
#endif
		kwlist, &source)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
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

finally:
	PyBuffer_Release(&source);

	return result;
}

PyDoc_STRVAR(frame_header_size__doc__,
"frame_header_size(data)\n"
"\n"
"Obtain the size of a frame header.\n"
);

static PyObject* frame_header_size(PyObject* self, PyObject* args, PyObject* kwargs) {
	static char* kwlist[] = {
		"source",
		NULL
	};

	Py_buffer source;
	PyObject* result = NULL;
	size_t zresult;

#if PY_MAJOR_VERSION >= 3
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "y*:frame_header_size",
#else
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s*:frame_header_size",
#endif
		kwlist, &source)) {
		return NULL;
	}

	if (!PyBuffer_IsContiguous(&source, 'C') || source.ndim > 1) {
		PyErr_SetString(PyExc_ValueError,
			"data buffer should be contiguous and have at most one dimension");
		goto finally;
	}

	zresult = ZSTD_frameHeaderSize(source.buf, source.len);
	if (ZSTD_isError(zresult)) {
		PyErr_Format(ZstdError, "could not determine frame header size: %s",
			ZSTD_getErrorName(zresult));
	}
	else {
		result = PyLong_FromSize_t(zresult);
	}

finally:

	PyBuffer_Release(&source);

	return result;
}

PyDoc_STRVAR(get_frame_parameters__doc__,
"get_frame_parameters(data)\n"
"\n"
"Obtains a ``FrameParameters`` instance by parsing data.\n");

PyDoc_STRVAR(train_dictionary__doc__,
"train_dictionary(dict_size, samples, k=None, d=None, steps=None,\n"
"                 threads=None,notifications=0, dict_id=0, level=0)\n"
"\n"
"Train a dictionary from sample data using the COVER algorithm.\n"
"\n"
"A compression dictionary of size ``dict_size`` will be created from the\n"
"iterable of ``samples``. The raw dictionary bytes will be returned.\n"
"\n"
"The COVER algorithm has 2 parameters: ``k`` and ``d``. These control the\n"
"*segment size* and *dmer size*. A reasonable range for ``k`` is\n"
"``[16, 2048+]``. A reasonable range for ``d`` is ``[6, 16]``.\n"
"``d`` must be less than or equal to ``k``.\n"
"\n"
"``steps`` can be specified to control the number of steps through potential\n"
"values of ``k`` and ``d`` to try. ``k`` and ``d`` will only be varied if\n"
"those arguments are not defined. i.e. if ``d`` is ``8``, then only ``k``\n"
"will be varied in this mode.\n"
"\n"
"``threads`` can specify how many threads to use to test various ``k`` and\n"
"``d`` values. ``-1`` will use as many threads as available CPUs. By default,\n"
"a single thread is used.\n"
"\n"
"When ``k`` and ``d`` are not defined, default values are used and the\n"
"algorithm will perform multiple iterations - or steps - to try to find\n"
"ideal parameters. If both ``k`` and ``d`` are specified, then those values\n"
"will be used. ``steps`` or ``threads`` triggers optimization mode to test\n"
"multiple ``k`` and ``d`` variations.\n"
);

static char zstd_doc[] = "Interface to zstandard";

static PyMethodDef zstd_methods[] = {
	{ "estimate_decompression_context_size", (PyCFunction)estimate_decompression_context_size,
	METH_NOARGS, estimate_decompression_context_size__doc__ },
	{ "frame_content_size", (PyCFunction)frame_content_size,
	METH_VARARGS | METH_KEYWORDS, frame_content_size__doc__ },
	{ "frame_header_size", (PyCFunction)frame_header_size,
	METH_VARARGS | METH_KEYWORDS, frame_header_size__doc__ },
	{ "get_frame_parameters", (PyCFunction)get_frame_parameters,
	METH_VARARGS | METH_KEYWORDS, get_frame_parameters__doc__ },
	{ "train_dictionary", (PyCFunction)train_dictionary,
	METH_VARARGS | METH_KEYWORDS, train_dictionary__doc__ },
	{ NULL, NULL }
};

void bufferutil_module_init(PyObject* mod);
void compressobj_module_init(PyObject* mod);
void compressor_module_init(PyObject* mod);
void compressionparams_module_init(PyObject* mod);
void constants_module_init(PyObject* mod);
void compressionchunker_module_init(PyObject* mod);
void compressiondict_module_init(PyObject* mod);
void compressionreader_module_init(PyObject* mod);
void compressionwriter_module_init(PyObject* mod);
void compressoriterator_module_init(PyObject* mod);
void decompressor_module_init(PyObject* mod);
void decompressobj_module_init(PyObject* mod);
void decompressionreader_module_init(PyObject *mod);
void decompressionwriter_module_init(PyObject* mod);
void decompressoriterator_module_init(PyObject* mod);
void frameparams_module_init(PyObject* mod);

void zstd_module_init(PyObject* m) {
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
	if (ZSTD_VERSION_NUMBER != 10506 || ZSTD_versionNumber() != 10506) {
		PyErr_SetString(PyExc_ImportError, "zstd C API mismatch; Python bindings not compiled against expected zstd version");
		return;
	}

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
#  define PYTHON_ZSTD_VISIBILITY __attribute__ ((visibility ("default")))
#else
#  define PYTHON_ZSTD_VISIBILITY
#endif

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef zstd_module = {
	PyModuleDef_HEAD_INIT,
	"zstd",
	zstd_doc,
	-1,
	zstd_methods
};

PYTHON_ZSTD_VISIBILITY PyMODINIT_FUNC PyInit_zstd(void) {
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
#else
PYTHON_ZSTD_VISIBILITY PyMODINIT_FUNC initzstd(void) {
	PyObject *m = Py_InitModule3("zstd", zstd_methods, zstd_doc);
	if (m) {
		zstd_module_init(m);
	}
}
#endif

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
#elif defined(__OpenBSD__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__DragonFly__)
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
int safe_pybytes_resize(PyObject** obj, Py_ssize_t size) {
	PyObject* tmp;

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