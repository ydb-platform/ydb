/*
Developed by ESN, an Electronic Arts Inc. studio.
Copyright (c) 2014, Electronic Arts Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
* Neither the name of ESN, Electronic Arts Inc. nor the
names of its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS INC. BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


Portions of code from MODP_ASCII - Ascii transformations (upper/lower, etc)
http://code.google.com/p/stringencoders/
Copyright (c) 2007  Nick Galbreath -- nickg [at] modp [dot] com. All rights reserved.

Numeric decoder derived from from TCL library
http://www.opensource.apple.com/source/tcl/tcl-14/tcl/license.terms
* Copyright (c) 1988-1993 The Regents of the University of California.
* Copyright (c) 1994 Sun Microsystems, Inc.
*/

#include <Python.h>
#include "version.h"
#include "ujson.h"

/* objToJSON */
PyObject* objToJSON(PyObject* self, PyObject *args, PyObject *kwargs);

/* JSONToObj */
PyObject* JSONToObj(PyObject* self, PyObject *args, PyObject *kwargs);

/* objToJSONFile */
PyObject* objToJSONFile(PyObject* self, PyObject *args, PyObject *kwargs);

/* JSONFileToObj */
PyObject* JSONFileToObj(PyObject* self, PyObject *args, PyObject *kwargs);

PyObject* JSONDecodeError;


#define ENCODER_HELP_TEXT "Use ensure_ascii=false to output UTF-8. " \
    "Set encode_html_chars=True to encode < > & as unicode escape sequences. "\
    "Set escape_forward_slashes=False to prevent escaping / characters." \
    "Set allow_nan=False to raise an exception when NaN or Infinity would be serialized." \
    "Set reject_bytes=True to raise TypeError on bytes."

static PyMethodDef ujsonMethods[] = {
  {"encode", (PyCFunction) objToJSON, METH_VARARGS | METH_KEYWORDS, "Converts arbitrary object recursively into JSON. " ENCODER_HELP_TEXT},
  {"decode", (PyCFunction) JSONToObj, METH_VARARGS | METH_KEYWORDS, "Converts JSON as string to dict object structure."},
  {"dumps", (PyCFunction) objToJSON, METH_VARARGS | METH_KEYWORDS,  "Converts arbitrary object recursively into JSON. " ENCODER_HELP_TEXT},
  {"loads", (PyCFunction) JSONToObj, METH_VARARGS | METH_KEYWORDS,  "Converts JSON as string to dict object structure."},
  {"dump", (PyCFunction) objToJSONFile, METH_VARARGS | METH_KEYWORDS, "Converts arbitrary object recursively into JSON file. " ENCODER_HELP_TEXT},
  {"load", (PyCFunction) JSONFileToObj, METH_VARARGS | METH_KEYWORDS, "Converts JSON as file to dict object structure."},
  {NULL, NULL, 0, NULL}       /* Sentinel */
};

typedef struct {
  PyObject *type_decimal;
} modulestate;

static int module_traverse(PyObject *m, visitproc visit, void *arg);
static int module_clear(PyObject *m);
static void module_free(void *m);

static struct PyModuleDef moduledef = {
  PyModuleDef_HEAD_INIT,
  "ujson",
  0,                    /* m_doc */
  sizeof(modulestate),  /* m_size */
  ujsonMethods,         /* m_methods */
  NULL,                 /* m_slots */
  module_traverse,      /* m_traverse */
  module_clear,         /* m_clear */
  module_free           /* m_free */
};

#define modulestate(o) ((modulestate *)PyModule_GetState(o))
#define modulestate_global modulestate(PyState_FindModule(&moduledef))

#ifndef PYPY_VERSION
/* Used in objToJSON.c */
int object_is_decimal_type(PyObject *obj)
{
  PyObject *module = PyState_FindModule(&moduledef);
  if (module == NULL) return 0;
  modulestate *state = modulestate(module);
  if (state == NULL) return 0;
  PyObject *type_decimal = state->type_decimal;
  if (type_decimal == NULL) {
    PyErr_Clear();
    return 0;
  }
  int result = PyObject_IsInstance(obj, type_decimal);
  if (result == -1) {
    PyErr_Clear();
    return 0;
  }
  return result;
}
#else
/* Used in objToJSON.c */
int object_is_decimal_type(PyObject *obj)
{
  PyObject *module = PyImport_ImportModule("decimal");
  if (module == NULL) {
    PyErr_Clear();
    return 0;
  }
  PyObject *type_decimal = PyObject_GetAttrString(module, "Decimal");
  if (type_decimal == NULL) {
    Py_DECREF(module);
    PyErr_Clear();
    return 0;
  }
  int result = PyObject_IsInstance(obj, type_decimal);
  if (result == -1) {
    Py_DECREF(module);
    Py_DECREF(type_decimal);
    PyErr_Clear();
    return 0;
  }
  return result;
}
#endif

static int module_traverse(PyObject *m, visitproc visit, void *arg)
{
  Py_VISIT(modulestate(m)->type_decimal);
  return 0;
}

static int module_clear(PyObject *m)
{
  Py_CLEAR(modulestate(m)->type_decimal);
  return 0;
}

static void module_free(void *m)
{
  module_clear((PyObject *)m);
}

PyMODINIT_FUNC PyInit_ujson(void)
{
  PyObject* module;

#ifndef PYPY_VERSION
  // This function is not supported in PyPy.
  if ((module = PyState_FindModule(&moduledef)) != NULL)
  {
    Py_INCREF(module);
    return module;
  }
#endif

  module = PyModule_Create(&moduledef);
  if (module == NULL)
  {
    return NULL;
  }

  PyModule_AddStringConstant(module, "__version__", UJSON_VERSION);

#ifndef PYPY_VERSION
  PyObject *mod_decimal = PyImport_ImportModule("decimal");
  if (mod_decimal)
  {
    PyObject* type_decimal = PyObject_GetAttrString(mod_decimal, "Decimal");
    assert(type_decimal != NULL);
    modulestate(module)->type_decimal = type_decimal;
    Py_DECREF(mod_decimal);
  }
  else
    PyErr_Clear();
#endif

  JSONDecodeError = PyErr_NewException("ujson.JSONDecodeError", PyExc_ValueError, NULL);
  Py_XINCREF(JSONDecodeError);
  if (PyModule_AddObject(module, "JSONDecodeError", JSONDecodeError) < 0)
  {
    Py_XDECREF(JSONDecodeError);
    Py_CLEAR(JSONDecodeError);
    Py_DECREF(module);
    return NULL;
  }

  return module;
}
