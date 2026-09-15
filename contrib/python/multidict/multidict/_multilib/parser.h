#ifndef _MULTIDICT_PARSER_H
#define _MULTIDICT_PARSER_H

#ifdef __cplusplus
extern "C" {
#endif

static inline int
raise_unexpected_kwarg(const char *fname, PyObject *argname)
{
    PyErr_Format(PyExc_TypeError,
                 "%.150s() got an unexpected keyword argument '%.150U'",
                 fname,
                 argname);
    return -1;
}

static inline int
raise_missing_posarg(const char *fname, const char *argname)
{
    PyErr_Format(PyExc_TypeError,
                 "%.150s() missing 1 required positional argument: '%.150s'",
                 fname,
                 argname);
    return -1;
}

/* Parse FASTCALL|METH_KEYWORDS arguments as two args,
the first arg is mandatory and the second one is optional.
If the second arg is not passed it remains NULL pointer.

The parser accepts three forms:
1. all positional args,
2. fist positional, second keyword-arg
3. all named keyword args.
*/

static inline int
parse2(const char *fname, PyObject *const *args, Py_ssize_t nargs,
       PyObject *kwnames, Py_ssize_t minargs, const char *arg1name,
       PyObject **arg1, const char *arg2name, PyObject **arg2)
{
    assert(minargs >= 1);
    assert(minargs <= 2);

    if (kwnames != NULL) {
        Py_ssize_t kwsize = PyTuple_Size(kwnames);
        if (kwsize < 0) {
            return -1;
        }
        PyObject *argname;  // borrowed ref
        if (kwsize == 2) {
            /* All args are passed by keyword, possible combinations:
               arg1, arg2 and arg2, arg1 */
            argname = PyTuple_GetItem(kwnames, 0);
            if (argname == NULL) {
                return -1;
            }
            if (PyUnicode_CompareWithASCIIString(argname, arg1name) == 0) {
                argname = PyTuple_GetItem(kwnames, 1);
                if (argname == NULL) {
                    return -1;
                }
                if (PyUnicode_CompareWithASCIIString(argname, arg2name) == 0) {
                    *arg1 = args[0];
                    *arg2 = args[1];
                    return 0;
                } else {
                    return raise_unexpected_kwarg(fname, argname);
                }
            } else if (PyUnicode_CompareWithASCIIString(argname, arg2name) ==
                       0) {
                argname = PyTuple_GetItem(kwnames, 1);
                if (argname == NULL) {
                    return -1;
                }
                if (PyUnicode_CompareWithASCIIString(argname, arg1name) == 0) {
                    *arg1 = args[1];
                    *arg2 = args[0];
                    return 0;
                } else {
                    return raise_unexpected_kwarg(fname, argname);
                }
            } else {
                return raise_unexpected_kwarg(fname, argname);
            }
        } else {
            // kwsize == 1
            argname = PyTuple_GetItem(kwnames, 0);
            if (argname == NULL) {
                return -1;
            }
            if (nargs == 1) {
                if (PyUnicode_CompareWithASCIIString(argname, arg2name) == 0) {
                    *arg1 = args[0];
                    *arg2 = args[1];
                    return 0;
                } else {
                    return raise_unexpected_kwarg(fname, argname);
                }
            } else {
                // nargs == 0
                if (PyUnicode_CompareWithASCIIString(argname, arg1name) == 0) {
                    *arg1 = args[0];
                    *arg2 = NULL;
                    return 0;
                } else {
                    return raise_missing_posarg(fname, arg1name);
                }
            }
        }
    } else {
        if (nargs < 1) {
            PyErr_Format(
                PyExc_TypeError,
                "%.150s() missing 1 required positional argument: '%s'",
                fname,
                arg1name);
            return -1;
        }
        if (nargs < minargs || nargs > 2) {
            const char *txt;
            if (minargs == 2) {
                txt = "from 1 to 2 positional arguments";
            } else {
                txt = "exactly 1 positional argument";
            }
            PyErr_Format(PyExc_TypeError,
                         "%.150s() takes %s but %zd were given",
                         fname,
                         txt,
                         nargs);
            return -1;
        }
        *arg1 = args[0];
        if (nargs == 2) {
            *arg2 = args[1];
        } else {
            *arg2 = NULL;
        }
        return 0;
    }
}

#ifdef __cplusplus
}
#endif
#endif
