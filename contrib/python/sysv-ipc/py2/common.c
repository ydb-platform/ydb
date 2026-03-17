#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "structmember.h"

#include "common.h"

#include <stdlib.h>

key_t
get_random_key(void) {
    int key;

    /* ******************************************************************
    The inability to know the range of a key_t requires careful code here.
    Remember that KEY_MIN and KEY_MAX refer only to the limits inherent in the
    variable type I use internally when turning a key into a Python object and
    vice versa. Those limits may exceed the operating system's limits of key_t.

    For instance, if key_t is typedef-ed as uint, I should generate a key
    where 0 <= key <= UINT_MAX.

    Since I can't know what key_t is typedef-ed as, I take a conservative
    approach and generate only keys where
    1 <= key <= SHRT_MAX.

    Such values will work if key_t is typedef-ed as a short, int, uint,
    long or ulong.
    ****************************************************************** */
    do {
        // ref: http://www.c-faq.com/lib/randrange.html
        key = ((int)((double)rand() / ((double)RAND_MAX + 1) * (SHRT_MAX - 1))) + 1;
    } while (key == IPC_PRIVATE);

    return (key_t)key;
}

#if PY_MAJOR_VERSION < 3
PyObject *
py_int_or_long_from_ulong(unsigned long value) {
    // Python ints are guaranteed to accept up to LONG_MAX. Anything
    // larger needs to be a Python long.
    if (value > LONG_MAX)
        return PyLong_FromUnsignedLong(value);
    else
        return PyInt_FromLong(value);
}
#endif


int
convert_key_param(PyObject *py_key, void *converted_key) {
    // Converts a PyObject into a key if possible. Returns 0 on failure.
    // The converted_key param should point to a NoneableKey type.
    // None is an acceptable key, in which case converted_key->is_none
    // is non-zero and converted_key->value is undefined.
    int rc = 0;
    long key = 0;

    ((NoneableKey *)converted_key)->is_none = 0;

    if (py_key == Py_None) {
        rc = 1;
        ((NoneableKey *)converted_key)->is_none = 1;
    }
#if PY_MAJOR_VERSION < 3
    else if (PyInt_Check(py_key)) {
        rc = 1;
        key = PyInt_AsLong(py_key);
    }
#endif
    else if (PyLong_Check(py_key)) {
        rc = 1;
        key = PyLong_AsLong(py_key);
        if (PyErr_Occurred()) {
            // This happens when the Python long is too big for a C long.
            rc = 0;
            PyErr_Format(PyExc_ValueError,
                         "Key must be between %ld (KEY_MIN) and %ld (KEY_MAX)",
                         KEY_MIN, KEY_MAX);
        }
    }

    if (rc) {
        // Param is OK
        if (! ((NoneableKey *)converted_key)->is_none) {
            // It's not None; ensure it is in range
            if ((key >= KEY_MIN) && (key <= KEY_MAX))
                ((NoneableKey *)converted_key)->value = (key_t)key;
            else {
                rc = 0;
                PyErr_Format(PyExc_ValueError,
                             "Key must be between %ld (KEY_MIN) and %ld (KEY_MAX)",
                             KEY_MIN, KEY_MAX);
            }
        }
    }
    else
        PyErr_SetString(PyExc_TypeError, "Key must be an integer or None");

    return rc;
}
