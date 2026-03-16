#include <Python.h>
#include <fcntl.h>
#include <errno.h>
#ifdef linux
#include <linux/falloc.h>
#endif


/* Set fallocate error from errno, and return NULL */
static PyObject *
fallocate_error(void)
{
    return PyErr_SetFromErrno(PyExc_OSError);
}

/* A helper used by a number of POSIX-only functions */
#ifndef MS_WINDOWS
static int
_parse_off_t(PyObject* arg, void* addr)
{
#if !defined(HAVE_LARGEFILE_SUPPORT)
    *((off_t*)addr) = PyLong_AsLong(arg);
#else
    *((off_t*)addr) = PyLong_AsLongLong(arg);
#endif
    if (PyErr_Occurred())
        return 0;
    return 1;
}
#endif

PyDoc_STRVAR(fallocate_fallocate__doc__,
"fallocate(fd, mode, offset, len)\n\n\
fallocate() allows the caller to directly manipulate the allocated\n\
disk space for the file referred to by fd for the byte range starting\n\
at offset and continuing for len bytes.\n\n\
mode is only available in Linux, it should always be 0 unless one of the\n\
two following possible flags are specified:\n\n\
  FALLOC_FL_KEEP_SIZE  - do not grow file, default is extend size\n\
  FALLOC_FL_PUNCH_HOLE - punches a hole in file, de-allocates range\n\
  FALLOC_FL_COLLAPSE_SIZE - remove a range of a file without leaving a hole\n");

static PyObject *
fallocate_fallocate(PyObject *self, PyObject *args)
{
    off_t len, offset;
    int res, fd, mode;
    if (!PyArg_ParseTuple(args, "iiO&O&:fallocate",
            &fd, &mode, _parse_off_t, &offset, _parse_off_t, &len))
        return NULL;

#ifdef HAVE_FALLOCATE
    Py_BEGIN_ALLOW_THREADS
    res = fallocate(fd, mode, offset, len);
    Py_END_ALLOW_THREADS
#endif
#ifdef HAVE_APPLE_F_ALLOCATECONTIG
    Py_BEGIN_ALLOW_THREADS
    fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, offset, len, 0};
    res = fcntl(fd, F_PREALLOCATE, &store);
    if (res == -1) {
        // try and allocate space with fragments
        store.fst_flags = F_ALLOCATEALL;
        res = fcntl(fd, F_PREALLOCATE, &store);
    }
    if (res != -1) {
        res = ftruncate(fd, offset+len);
    }
    Py_END_ALLOW_THREADS
#endif
    if (res != 0) {
        // errno = res;
        return fallocate_error();
    }
    Py_RETURN_NONE;
}

#ifdef HAVE_POSIX_FALLOCATE
PyDoc_STRVAR(posix_posix_fallocate__doc__,
"posix_fallocate(fd, offset, len)\n\n\
Ensures that enough disk space is allocated for the file specified by fd\n\
starting from offset and continuing for len bytes.");

static PyObject *
posix_posix_fallocate(PyObject *self, PyObject *args)
{
    off_t len, offset;
    int res, fd;

    if (!PyArg_ParseTuple(args, "iO&O&:posix_fallocate",
            &fd, _parse_off_t, &offset, _parse_off_t, &len))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    res = posix_fallocate(fd, offset, len);
    Py_END_ALLOW_THREADS
    if (res != 0) {
        errno = res;
        return fallocate_error();
    }
    Py_RETURN_NONE;
}
#endif

#ifdef HAVE_POSIX_FADVISE
PyDoc_STRVAR(posix_posix_fadvise__doc__,
"posix_fadvise(fd, offset, len, advice)\n\n\
Announces an intention to access data in a specific pattern thus allowing\n\
the kernel to make optimizations.\n\
The advice applies to the region of the file specified by fd starting at\n\
offset and continuing for len bytes.\n\
advice is one of POSIX_FADV_NORMAL, POSIX_FADV_SEQUENTIAL,\n\
POSIX_FADV_RANDOM, POSIX_FADV_NOREUSE, POSIX_FADV_WILLNEED or\n\
POSIX_FADV_DONTNEED.");

static PyObject *
posix_posix_fadvise(PyObject *self, PyObject *args)
{
    off_t len, offset;
    int res, fd, advice;

    if (!PyArg_ParseTuple(args, "iO&O&i:posix_fadvise",
            &fd, _parse_off_t, &offset, _parse_off_t, &len, &advice))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    res = posix_fadvise(fd, offset, len, advice);
    Py_END_ALLOW_THREADS
    if (res != 0) {
        errno = res;
        return fallocate_error();
    }
    Py_RETURN_NONE;
}
#endif

static PyMethodDef module_methods[] = {
#if defined(HAVE_FALLOCATE) || defined(HAVE_APPLE_F_ALLOCATECONTIG)
    {"fallocate", fallocate_fallocate, METH_VARARGS, fallocate_fallocate__doc__},
#endif
#ifdef HAVE_POSIX_FALLOCATE
    {"posix_fallocate", posix_posix_fallocate, METH_VARARGS, posix_posix_fallocate__doc__},
#endif
#ifdef HAVE_POSIX_FADVISE
    {"posix_fadvise", posix_posix_fadvise, METH_VARARGS, posix_posix_fadvise__doc__},
#endif
    {NULL, NULL, 0, NULL}
};

static char module_docstring[] =
    "Backport of fallocate, posix_fallocate and posix_fadvise to Python 2.x";

#if PY_MAJOR_VERSION >= 3
static PyModuleDef _fallocatemodule = {
    PyModuleDef_HEAD_INIT,
    "_fallocate",
    module_docstring,
    -1,
    module_methods,
    NULL,
    NULL,
    NULL,
    NULL,
};

PyMODINIT_FUNC
PyInit__fallocate(void)
{
    PyObject *m = PyModule_Create(&_fallocatemodule);
    if (m == NULL)
        return NULL;

#else

PyMODINIT_FUNC
init_fallocate(void)
{
    PyObject *m = Py_InitModule3("_fallocate", module_methods, module_docstring);
    if (m == NULL)
        return;
#endif

#ifdef HAVE_FALLOCATE
    if (PyModule_AddIntMacro(m, FALLOC_FL_KEEP_SIZE) == -1 ||
#ifdef FALLOC_FL_COLLAPSE_RANGE
        PyModule_AddIntMacro(m, FALLOC_FL_COLLAPSE_RANGE) == -1 ||
#endif
        PyModule_AddIntMacro(m, FALLOC_FL_PUNCH_HOLE) == -1)
#if PY_MAJOR_VERSION >= 3
        return NULL;
#else
        return;
#endif
#endif
#ifdef HAVE_POSIX_FADVISE
    /* constants for posix_fadvise */
    if (PyModule_AddIntMacro(m, POSIX_FADV_NORMAL) == -1 ||
        PyModule_AddIntMacro(m, POSIX_FADV_SEQUENTIAL) == -1 ||
        PyModule_AddIntMacro(m, POSIX_FADV_RANDOM) == -1 ||
        PyModule_AddIntMacro(m, POSIX_FADV_NOREUSE) == -1 ||
        PyModule_AddIntMacro(m, POSIX_FADV_WILLNEED) == -1 ||
        PyModule_AddIntMacro(m, POSIX_FADV_DONTNEED) == -1)
#if PY_MAJOR_VERSION >= 3
        return NULL;
#else
        return;
#endif
#endif

#if PY_MAJOR_VERSION >= 3
        return m;
#endif
};
