/*
 *  hash_template.c : Generic framework for hash function extension modules
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
  
/* Basic object type */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#ifdef _HAVE_STDC_HEADERS
#include <string.h>
#endif
#include "Python.h"
#include "pycrypto_compat.h"

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

typedef struct {
	PyObject_HEAD
	hash_state st;
} ALGobject;

/* Please see PEP3123 for a discussion of PyObject_HEAD and changes made in 3.x to make it conform to Standard C.
 * These changes also dictate using Py_TYPE to check type, and PyVarObject_HEAD_INIT(NULL, 0) to initialize
 */
#ifdef IS_PY3K
static PyTypeObject ALGtype;
#define is_ALGobject(v) (Py_TYPE(v) == &ALGtype)
#else
staticforward PyTypeObject ALGtype;
#define is_ALGobject(v) ((v)->ob_type == &ALGtype)
#define PyLong_FromLong PyInt_FromLong /* For Python 2.x */
#endif

static ALGobject *
newALGobject(void)
{
	ALGobject *new;

	new = PyObject_New(ALGobject, &ALGtype);
	return new;
}

/* Internal methods for a hashing object */

static void
ALG_dealloc(PyObject *ptr)
{
	ALGobject *self = (ALGobject *)ptr;

	/* Overwrite the contents of the object */
	memset((char*)&(self->st), 0, sizeof(hash_state));
	PyObject_Del(ptr);
}


/* External methods for a hashing object */

static char ALG_copy__doc__[] = 
"copy(): Return a copy of the hashing object.";

static PyObject *
ALG_copy(ALGobject *self, PyObject *args)
{
	ALGobject *newobj;

	if (!PyArg_ParseTuple(args, "")) {
		return NULL;
	}
	
	if ( (newobj = newALGobject())==NULL)
		return NULL;

	hash_copy(&(self->st), &(newobj->st));
	return((PyObject *)newobj); 
}

static char ALG_digest__doc__[] = 
"digest(): Return the digest value as a string of binary data.";

static PyObject *
ALG_digest(ALGobject *self, PyObject *args)
{
	if (!PyArg_ParseTuple(args, ""))
		return NULL;

	return (PyObject *)hash_digest(&(self->st));
}

static char ALG_hexdigest__doc__[] = 
"hexdigest(): Return the digest value as a string of hexadecimal digits.";

static PyObject *
ALG_hexdigest(ALGobject *self, PyObject *args)
{
	PyObject *value, *retval;
	unsigned char *raw_digest, *hex_digest;
	int i, j, size;

	if (!PyArg_ParseTuple(args, ""))
		return NULL;

	/* Get the raw (binary) digest value */
	value = (PyObject *)hash_digest(&(self->st));
	size = PyBytes_Size(value);
	raw_digest = (unsigned char *) PyBytes_AsString(value);

	/* Create a new string */
	retval = PyBytes_FromStringAndSize(NULL, size * 2 );
	hex_digest = (unsigned char *) PyBytes_AsString(retval);

	/* Make hex version of the digest */
	for(i=j=0; i<size; i++)
	{
		char c;
		c = raw_digest[i] / 16; c = (c>9) ? c+'a'-10 : c + '0';
		hex_digest[j++] = c;
		c = raw_digest[i] % 16; c = (c>9) ? c+'a'-10 : c + '0';
		hex_digest[j++] = c;
	}
#ifdef IS_PY3K
	/* Create a text string return value */
	retval = PyUnicode_FromEncodedObject(retval,"latin-1","strict");
#endif

	Py_DECREF(value);
	return retval;
}

static char ALG_update__doc__[] = 
"update(string): Update this hashing object's state with the provided string.";

static PyObject *
ALG_update(ALGobject *self, PyObject *args)
{
	unsigned char *cp;
	int len;

	if (!PyArg_ParseTuple(args, "s#", &cp, &len))
		return NULL;

	Py_BEGIN_ALLOW_THREADS;

	hash_update(&(self->st), cp, len);
	Py_END_ALLOW_THREADS;

	Py_INCREF(Py_None);

	return Py_None;
}

/** Forward declaration for this module's new() method **/
static char ALG_new__doc__[] =
"new([string]): Return a new " _MODULE_STRING 
" hashing object.  An optional string "
"argument may be provided; if present, this string will be "
"automatically hashed into the initial state of the object."; 

static PyObject *ALG_new(PyObject*, PyObject*);

static PyMethodDef ALG_methods[] = {
	{"copy", (PyCFunction)ALG_copy, METH_VARARGS, ALG_copy__doc__},
	{"digest", (PyCFunction)ALG_digest, METH_VARARGS, ALG_digest__doc__},
	{"hexdigest", (PyCFunction)ALG_hexdigest, METH_VARARGS, ALG_hexdigest__doc__},
	{"update", (PyCFunction)ALG_update, METH_VARARGS, ALG_update__doc__},
	{"new", (PyCFunction)ALG_new, METH_VARARGS, ALG_new__doc__},
	{NULL,			NULL}		/* sentinel */
};

static PyObject *
#ifdef IS_PY3K
ALG_getattro(PyObject *self, PyObject *attr)
#else
ALG_getattr(PyObject *self, char *name)
#endif
{
#ifdef IS_PY3K
	if (!PyUnicode_Check(attr))
		goto generic;
 
	if (PyUnicode_CompareWithASCIIString(attr, "digest_size")==0)
		return PyLong_FromLong(DIGEST_SIZE);
#else
	if (strcmp(name, "digest_size")==0)
		return PyInt_FromLong(DIGEST_SIZE);
#endif

#ifdef IS_PY3K
  generic:
	return PyObject_GenericGetAttr(self, attr);
#else
	return Py_FindMethod(ALG_methods, self, name);
#endif
}

static PyTypeObject ALGtype = {
#ifdef IS_PY3K
	PyVarObject_HEAD_INIT(NULL, 0)  /* deferred type init for compilation on Windows, type will be filled in at runtime */
#else
	PyObject_HEAD_INIT(NULL)
	0,			/*ob_size*/
#endif
 	_MODULE_STRING,			/*tp_name*/
 	sizeof(ALGobject),	/*tp_size*/
 	0,			/*tp_itemsize*/
 	/* methods */
	(destructor) ALG_dealloc, /*tp_dealloc*/
 	0,			/*tp_print*/
#ifdef IS_PY3K
	0, 			/*tp_getattr*/
#else
	ALG_getattr, /*tp_getattr*/
#endif
 	0,			/*tp_setattr*/
 	0,			/*tp_compare*/
 	0,			/*tp_repr*/
    0,			/*tp_as_number*/
#ifdef IS_PY3K
	0,				/*tp_as_sequence */
	0,				/*tp_as_mapping */
	0,				/*tp_hash*/
	0,				/*tp_call*/
	0,				/*tp_str*/
	ALG_getattro,	/*tp_getattro*/
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
	ALG_methods,		/*tp_methods*/
#endif
 };

/* The single module-level function: new() */

/** This method belong to both the module and the hash object **/
static PyObject *
ALG_new(PyObject *self, PyObject *args)
{
        ALGobject *new;
	unsigned char *cp = NULL;
	int len;
	
	if ((new = newALGobject()) == NULL)
		return NULL;

	if (!PyArg_ParseTuple(args, "|s#",
			      &cp, &len)) {
	        Py_DECREF(new);
		return NULL;
	}

        hash_init(&(new->st));

	if (PyErr_Occurred()) {
		Py_DECREF(new); 
		return NULL;
	}
	if (cp) {
		Py_BEGIN_ALLOW_THREADS;
		hash_update(&(new->st), cp, len);
		Py_END_ALLOW_THREADS;
	}

	return (PyObject *)new;
}

/* List of functions exported by this module */

static struct PyMethodDef ALG_functions[] = {
	{"new", (PyCFunction)ALG_new, METH_VARARGS, ALG_new__doc__},
	{NULL,			NULL}		 /* Sentinel */
};

#ifdef IS_PY3K
static struct PyModuleDef moduledef = {
	PyModuleDef_HEAD_INIT,
	"Crypto.Hash." _MODULE_STRING,
	NULL,
	-1,
	ALG_functions,
	NULL,
	NULL,
	NULL,
	NULL
};
#endif

/* Initialize this module. */

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
	PyObject *m;

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
	m = Py_InitModule("Crypto.Hash." _MODULE_STRING, ALG_functions);
#endif

	/* Add some symbolic constants to the module */
	PyModule_AddIntConstant(m, "digest_size", DIGEST_SIZE);
	PyModule_AddIntConstant(m, "block_size", BLOCK_SIZE);

	/* Check for errors */
	if (PyErr_Occurred())
		Py_FatalError("can't initialize module " 
                              _MODULE_STRING);
#ifdef IS_PY3K
	return m;
#endif
}
