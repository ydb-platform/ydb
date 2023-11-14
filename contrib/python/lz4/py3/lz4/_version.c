/*
 * Copyright (c) 2016 Jonathan Underwood
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of Steeve Morin nor the names of its contributors may be
 *    used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <Python.h>

#include <stdlib.h>
#include <lz4.h>
#include <lz4hc.h>

static PyObject *
library_version_number (PyObject * Py_UNUSED (self), PyObject * Py_UNUSED (args))
{
  return Py_BuildValue ("i", LZ4_versionNumber ());
}

static PyObject *
library_version_string (PyObject * Py_UNUSED (self), PyObject * Py_UNUSED (args))
{
  return Py_BuildValue ("s", LZ4_versionString ());
}

PyDoc_STRVAR
(
 library_version_number__doc,
 "library_version_number()\n\n"                       \
 "Returns the version number of the LZ4 library.\n"   \
 "\n"                                                 \
 "Args:\n"                                            \
 "    None\n"                                         \
 "\n"                                                 \
 "Returns:\n"                                         \
 "    int: version number eg. 10705"
 );

PyDoc_STRVAR
(
 library_version_string__doc,
 "library_version_string()\n\n"                                \
 "Returns the version number of the LZ4 library as a string\n" \
 "containing the semantic version.\n"                          \
 "\n"                                                          \
 "Args:\n"                                                     \
 "    None\n"                                                  \
 "\n"                                                          \
 "Returns:\n"                                                  \
 "    str: version number eg. \"1.7.5\""
 );

static PyMethodDef module_methods[] = {
  {
    "library_version_number",
    (PyCFunction) library_version_number,
    METH_VARARGS,
    library_version_number__doc
  },
  {
    "library_version_string",
    (PyCFunction) library_version_string,
    METH_VARARGS,
    library_version_string__doc
  },
  {
    /* Sentinel */
    NULL,
    NULL,
    0,
    NULL
  }
};

static struct PyModuleDef moduledef =
  {
    PyModuleDef_HEAD_INIT,
    "_version",
    NULL,
    -1,
    module_methods
  };

PyMODINIT_FUNC
PyInit__version(void)
{
  PyObject *module = PyModule_Create (&moduledef);

  if (module == NULL)
    return NULL;

  return module;
}
