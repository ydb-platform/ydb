/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.
 * Copyright (c) 2009-2010 Heikki Toivonen. All rights reserved.
*/
/* $Id: _rand.i 721 2010-02-13 06:30:33Z heikki $ */

%rename(rand_file_name) RAND_file_name;
extern const char *RAND_file_name(char *, size_t );
%rename(rand_load_file) RAND_load_file;
extern int RAND_load_file(const char *, long);
%rename(rand_save_file) RAND_write_file;
extern int RAND_write_file(const char *);
%rename(rand_poll) RAND_poll;
extern int RAND_poll(void);
%rename(rand_status) RAND_status;
extern int RAND_status(void);
%rename(rand_cleanup) RAND_cleanup;
extern void RAND_cleanup(void);

%warnfilter(454) _rand_err;
%inline %{
static PyObject *_rand_err;

void rand_init(PyObject *rand_err) {
    Py_INCREF(rand_err);
    _rand_err = rand_err;
}

PyObject *rand_seed(PyObject *seed) {
    const void *buf = NULL;
    int len = 0;

    m2_PyObject_AsReadBufferInt(seed, &buf, &len);

    RAND_seed(buf, len);
    Py_RETURN_NONE;
}

PyObject *rand_add(PyObject *blob, double entropy) {
    const void *buf = NULL;
    int len = 0;

    m2_PyObject_AsReadBufferInt(blob, &buf, &len);

    RAND_add(buf, len, entropy);
    Py_RETURN_NONE;
}

PyObject *rand_bytes(int n) {
    void *blob;
    int ret;
    PyObject *obj;

    if (!(blob = PyMem_Malloc(n))) {
        PyErr_SetString(PyExc_MemoryError,
        "Insufficient memory for rand_bytes.");
        return NULL;
    }
    if ((ret = RAND_bytes(blob, n)) == 1) {
        obj = PyBytes_FromStringAndSize(blob, n);
        PyMem_Free(blob);
        return obj;
    } else if (ret == 0) {
        PyErr_SetString(_rand_err, "Not enough randomness.");
        PyMem_Free(blob);
        return NULL;
    } else if (ret == -1) {
        PyErr_SetString(_rand_err,
                        "Not supported by the current RAND method.");
        PyMem_Free(blob);
        return NULL;
    } else {
        PyMem_Free(blob);
        m2_PyErr_Msg(_rand_err);
        return NULL;
    }
}

PyObject *rand_pseudo_bytes(int n) {
    int ret;
    unsigned char *blob;
    PyObject *tuple;

    if (!(blob=(unsigned char *)PyMem_Malloc(n))) {
        PyErr_SetString(PyExc_MemoryError, "Insufficient memory for rand_pseudo_bytes.");
        return NULL;
    }
    if (!(tuple=PyTuple_New(2))) {
        PyErr_SetString(PyExc_RuntimeError, "PyTuple_New() fails");
        PyMem_Free(blob);
        return NULL;
    }
    ret = RAND_pseudo_bytes(blob, n);
    if (ret == -1) {
        PyMem_Free(blob);
        Py_DECREF(tuple);
        PyErr_SetString(_rand_err,
            "Function RAND_pseudo_bytes not supported by the current RAND method.");
        return NULL;
    } else {
        PyTuple_SET_ITEM(tuple, 0, PyBytes_FromStringAndSize((char*)blob, n));

        PyMem_Free(blob);
        PyTuple_SET_ITEM(tuple, 1, PyLong_FromLong((long)ret));
        return tuple;
    }
}

PyObject *rand_file_name(void) {
    PyObject *obj;
    char *str;
    if ((obj = PyBytes_FromStringAndSize(NULL, BUFSIZ))==NULL) {
        PyErr_SetString(PyExc_MemoryError, "rand_file_name");
        return NULL;
    }
    str=PyBytes_AS_STRING(obj);
    if (RAND_file_name(str, BUFSIZ)==NULL) {
        PyErr_SetString(PyExc_RuntimeError, "rand_file_name");
        return NULL;
    }
    if (_PyBytes_Resize(&obj, (Py_ssize_t)strlen(str))!=0)
        return NULL; /* mem exception set by _PyBytes_Resize */
    return obj;
}

void rand_screen(void) {
#ifdef _WIN32
    RAND_screen();
#endif
}

int rand_win32_event(unsigned int imsg, int wparam, long lparam) {
#ifdef _WIN32
    return RAND_event(imsg, wparam, lparam);
#else
    return 0;
#endif
}
%}

/*
2004-04-05, ngps: Still missing:
  RAND_egd
  RAND_egd_bytes
  RAND_query_egd_bytes
*/


