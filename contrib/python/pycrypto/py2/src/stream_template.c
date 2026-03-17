/* -*- C -*- */

/*
 *  stream_template.c : Generic framework for stream ciphers
 *
 * Written by Andrew Kuchling and others
 *
 * ===================================================================
 * The contents of this file are dedicated to the public domain.  To
 * the extent that dedication to the public domain is not available,
 * everyone is granted a worldwide, perpetual, royalty-free,
 * non-exclusive license to exercise all rights associated with the
 * contents of this file for any purpose whatsoever.
 * No rights are reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ===================================================================
 */


#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef _HAVE_STDC_HEADERS
#include <string.h>
#endif

#include "Python.h"
#include "pycrypto_compat.h"
#include "modsupport.h"

#define _STR(x) #x
#define _XSTR(x) _STR(x)
#define _PASTE(x,y) x##y
#define _PASTE2(x,y) _PASTE(x,y)
#ifdef IS_PY3K
#define _MODULE_NAME _PASTE2(PyInit_,MODULE_NAME)
#else
#define _MODULE_NAME _PASTE2(init,MODULE_NAME)
#endif
#define _MODULE_STRING _XSTR(MODULE_NAME)

        /*
	 *
	 * Python interface
	 *
	 */

typedef struct 
{
	PyObject_HEAD 
	stream_state st;
} ALGobject;

/* Please see PEP3123 for a discussion of PyObject_HEAD and changes made in 3.x to make it conform to Standard C.
 * These changes also dictate using Py_TYPE to check type, and PyVarObject_HEAD_INIT(NULL, 0) to initialize
 */
#ifdef IS_PY3K
static PyTypeObject ALGtype;
#define is_ALGobject(v)		(Py_TYPE(v) == &ALGtype)
#else
staticforward PyTypeObject ALGtype;
#define is_ALGobject(v)		((v)->ob_type == &ALGtype)
#define PyLong_FromLong PyInt_FromLong /* For Python 2.x */
#endif

static ALGobject *
newALGobject(void)
{
	ALGobject * new;
	new = PyObject_New(ALGobject, &ALGtype);
	return new;
}

static void
ALGdealloc(PyObject *ptr)
{
	ALGobject *self = (ALGobject *)ptr;

	/* Overwrite the contents of the object */
	memset((char*)&(self->st), 0, sizeof(stream_state));
	PyObject_Del(ptr);
}

static char ALGnew__doc__[] = 
"Return a new " _MODULE_STRING " encryption object.";

static char *kwlist[] = {"key", NULL};

static ALGobject *
ALGnew(PyObject *self, PyObject *args, PyObject *kwdict)
{
	unsigned char *key;
	ALGobject * new;
	int keylen;

	new = newALGobject();
	if (!PyArg_ParseTupleAndKeywords(args, kwdict, "s#", kwlist, 
					 &key, &keylen))
	{
		Py_DECREF(new);
		return NULL;
	}

	if (KEY_SIZE!=0 && keylen != KEY_SIZE)
	{
		PyErr_SetString(PyExc_ValueError, 
				_MODULE_STRING " key must be "
				"KEY_SIZE bytes long");
		return NULL;
	}
	if (KEY_SIZE== 0 && keylen == 0)
	{
		PyErr_SetString(PyExc_ValueError, 
				_MODULE_STRING " key cannot be "
				"the null string (0 bytes long)");
		return NULL;
	}
	stream_init(&(new->st), key, keylen);
	if (PyErr_Occurred())
	{
		Py_DECREF(new);
		return NULL;
	}
	return new;
}

static char ALG_Encrypt__doc__[] =
"Decrypt the provided string of binary data.";

static PyObject *
ALG_Encrypt(ALGobject *self, PyObject *args)
{
	unsigned char *buffer, *str;
	int len;
	PyObject *result;

	if (!PyArg_Parse(args, "s#", &str, &len))
		return NULL;
	if (len == 0)			/* Handle empty string */
	{
		return PyBytes_FromStringAndSize(NULL, 0);
	}
	buffer = malloc(len);
	if (buffer == NULL)
	{
		PyErr_SetString(PyExc_MemoryError, "No memory available in "
				_MODULE_STRING " encrypt");
		return NULL;
	}
	Py_BEGIN_ALLOW_THREADS;
	memcpy(buffer, str, len);
	stream_encrypt(&(self->st), buffer, len);
	Py_END_ALLOW_THREADS;
	result = PyBytes_FromStringAndSize((char *)buffer, len);
	free(buffer);
	return (result);
}

static char ALG_Decrypt__doc__[] =
"decrypt(string): Decrypt the provided string of binary data.";

static PyObject *
ALG_Decrypt(ALGobject *self, PyObject *args)
{
	unsigned char *buffer, *str;
	int len;
	PyObject *result;

	if (!PyArg_Parse(args, "s#", &str, &len))
		return NULL;
	if (len == 0)			/* Handle empty string */
	{
		return PyBytes_FromStringAndSize(NULL, 0);
	}
	buffer = malloc(len);
	if (buffer == NULL)
	{
		PyErr_SetString(PyExc_MemoryError, "No memory available in "
				_MODULE_STRING " decrypt");
		return NULL;
	}
	Py_BEGIN_ALLOW_THREADS;
	memcpy(buffer, str, len);
	stream_decrypt(&(self->st), buffer, len);
	Py_END_ALLOW_THREADS;
	result = PyBytes_FromStringAndSize((char *)buffer, len);
	free(buffer);
	return (result);
}

/* ALGobject methods */
static PyMethodDef ALGmethods[] =
 {
#ifdef IS_PY3K
	{"encrypt", (PyCFunction) ALG_Encrypt, METH_O, ALG_Encrypt__doc__},
	{"decrypt", (PyCFunction) ALG_Decrypt, METH_O, ALG_Decrypt__doc__},
#else
	{"encrypt", (PyCFunction) ALG_Encrypt, 0, ALG_Encrypt__doc__},
	{"decrypt", (PyCFunction) ALG_Decrypt, 0, ALG_Decrypt__doc__},
#endif
 	{NULL, NULL}			/* sentinel */
 };

static PyObject *
#ifdef IS_PY3K
ALGgetattro(PyObject *self, PyObject *attr)
#else
ALGgetattr(PyObject *self, char *name)
#endif
{
#ifdef IS_PY3K
	if (!PyUnicode_Check(attr))
		goto generic;

	if (PyUnicode_CompareWithASCIIString(attr, "block_size") == 0)
#else
	if (strcmp(name, "block_size") == 0)
#endif
	{
		return PyLong_FromLong(BLOCK_SIZE);
	}
#ifdef IS_PY3K
	if (PyUnicode_CompareWithASCIIString(attr, "key_size") == 0)
#else
	if (strcmp(name, "key_size") == 0)
#endif
	{
		return PyLong_FromLong(KEY_SIZE);
	}
#ifdef IS_PY3K
  generic:
	return PyObject_GenericGetAttr(self, attr);
#else
	return Py_FindMethod(ALGmethods, self, name);
#endif
}

/* List of functions defined in the module */

static struct PyMethodDef modulemethods[] =
{
	{"new", (PyCFunction) ALGnew, 
	 METH_VARARGS|METH_KEYWORDS, ALGnew__doc__},
	{NULL, NULL}			/* sentinel */
};

static PyTypeObject ALGtype =
 {
#ifdef IS_PY3K
	PyVarObject_HEAD_INIT(NULL, 0)  /* deferred type init for compilation on Windows, type will be filled in at runtime */
#else
	PyObject_HEAD_INIT(NULL)
	0,				/*ob_size*/
#endif
 	_MODULE_STRING,		/*tp_name*/
 	sizeof(ALGobject),	/*tp_size*/
 	0,				/*tp_itemsize*/
 	/* methods */
	(destructor) ALGdealloc,	/*tp_dealloc*/
 	0,				/*tp_print*/
#ifdef IS_PY3K
	0,				/*tp_getattr*/
#else
	ALGgetattr,		/*tp_getattr*/
#endif
	0,				/*tp_setattr*/
	0,				/*tp_compare*/
	0,				/*tp_repr*/
 	0,				/*tp_as_number*/
#ifdef IS_PY3K
	0,				/*tp_as_sequence*/
	0,				/*tp_as_mapping*/
	0,				/*tp_hash*/
	0,				/*tp_call*/
	0,				/*tp_str*/
	ALGgetattro,	/*tp_getattro*/
	0,				/*tp_setattro*/
	0,				/*tp_as_buffer*/
	Py_TPFLAGS_DEFAULT,		/*tp_flags*/
	0,				/*tp_doc*/
	0,				/*tp_traverse*/
	0,				/*tp_clear*/
	0,				/*tp_richcompare*/
	0,				/*tp_weaklistoffset*/
	0,				/*tp_iter*/
	0,				/*tp_iternext*/
	ALGmethods,		/*tp_methods*/
#endif
 };

#ifdef IS_PY3K
 static struct PyModuleDef moduledef = {
	PyModuleDef_HEAD_INIT,
	"Crypto.Cipher." _MODULE_STRING,
	NULL,
	-1,
	modulemethods,
	NULL,
	NULL,
	NULL,
	NULL
};
#endif

/* Initialization function for the module */

/* Deal with old API in Python 2.1 */
#if PYTHON_API_VERSION < 1011
#define PyModule_AddIntConstant(m,n,v) {PyObject *o=PyInt_FromLong(v); \
           if (o!=NULL) \
             {PyDict_SetItemString(PyModule_GetDict(m),n,o); Py_DECREF(o);}}
#endif

#ifdef IS_PY3K
PyMODINIT_FUNC
#else
void
#endif
 _MODULE_NAME (void)
 {
 	PyObject *m, *d, *x;
 
#ifdef IS_PY3K
	/* PyType_Ready automatically fills in ob_type with &PyType_Type if it's not already set */
	if (PyType_Ready(&ALGtype) < 0)
		return NULL;

	/* Create the module and add the functions */
	m = PyModule_Create(&moduledef);
	if (m == NULL)
        	return NULL;
#else
	ALGtype.ob_type = &PyType_Type;
	/* Create the module and add the functions */
	m = Py_InitModule("Crypto.Cipher." _MODULE_STRING, modulemethods);
#endif
 
 	/* Add some symbolic constants to the module */
 	d = PyModule_GetDict(m);
	x = PyUnicode_FromString(_MODULE_STRING ".error");
 	PyDict_SetItemString(d, "error", x);
 
 	PyModule_AddIntConstant(m, "block_size", BLOCK_SIZE);
	PyModule_AddIntConstant(m, "key_size", KEY_SIZE);

 	/* Check for errors */
 	if (PyErr_Occurred())
 		Py_FatalError("can't initialize module " _MODULE_STRING);

#ifdef IS_PY3K
	return m;
#endif
 }
 
/* vim:set ts=4 sw=4 sts=0 noexpandtab: */
