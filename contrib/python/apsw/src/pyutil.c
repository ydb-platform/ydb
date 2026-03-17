/*
  Cross Python version compatibility and utility code

  See the accompanying LICENSE file.
*/

/* used in calls to AddTraceBackHere where O format takes non-null but
   we often have null so convert to None */
#define OBJ(o) ((o) ? (o) : (Py_None))

/* we clear weakref lists when close is called on a blob/cursor as
   well as when it is deallocated */
#define APSW_CLEAR_WEAKREFS                     \
  do                                            \
  {                                             \
    if (self->weakreflist)                      \
    {                                           \
      PyObject_ClearWeakRefs((PyObject *)self); \
      self->weakreflist = 0;                    \
    }                                           \
  } while (0)

/* Calls the named method of object with the provided args */
static PyObject *
Call_PythonMethod(PyObject *obj, const char *methodname, int mandatory, PyObject *args)
{
  PyObject *method = NULL;
  PyObject *res = NULL;

  /* we may be called when there is already an error.  eg if you return an error in
     a cursor method, then SQLite calls vtabClose which calls us.  We don't want to
     clear pre-existing errors, but we do want to clear ones when the function doesn't
     exist but is optional */
  PyObject *etype = NULL, *evalue = NULL, *etraceback = NULL;
  void *pyerralreadyoccurred = PyErr_Occurred();
  if (pyerralreadyoccurred)
    PyErr_Fetch(&etype, &evalue, &etraceback);

  /* we should only be called with ascii methodnames so no need to do
   character set conversions etc */
  method = PyObject_GetAttrString(obj, methodname);

  assert(method != obj);
  if (!method)
  {
    if (!mandatory)
    {
      /* pretend method existed and returned None */
      PyErr_Clear();
      res = Py_None;
      Py_INCREF(res);
    }
    goto finally;
  }

  res = PyObject_CallObject(method, args);
  if (!pyerralreadyoccurred && PyErr_Occurred())
    AddTraceBackHere(__FILE__, __LINE__, "Call_PythonMethod", "{s: s, s: i, s: O, s: O}",
                     "methodname", methodname,
                     "mandatory", mandatory,
                     "args", OBJ(args),
                     "method", OBJ(method));

finally:
  if (pyerralreadyoccurred)
    PyErr_Restore(etype, evalue, etraceback);
  Py_XDECREF(method);
  return res;
}

static PyObject *
Call_PythonMethodV(PyObject *obj, const char *methodname, int mandatory, const char *format, ...)
{
  PyObject *args = NULL, *result = NULL;
  va_list list;
  va_start(list, format);
  args = Py_VaBuildValue(format, list);
  va_end(list);

  if (args)
    result = Call_PythonMethod(obj, methodname, mandatory, args);

  Py_XDECREF(args);
  return result;
}

/* CONVENIENCE FUNCTIONS */

/* Convert a NULL terminated UTF-8 string into a Python object.  None
   is returned if NULL is passed in. */
static PyObject *
convertutf8string(const char *str)
{
  if (!str)
    Py_RETURN_NONE;

  return PyUnicode_FromStringAndSize(str, strlen(str));
}

#define GET_BUFFER(faultName, var, src, dest) APSW_FAULT_INJECT(faultName, var = PyObject_GetBuffer(src, dest, PyBUF_SIMPLE), (PyErr_NoMemory(), var = -1))

#define STRING_NEW(faultName, var, size, maxchar) APSW_FAULT_INJECT(faultName, var = PyUnicode_New(size, maxchar), var = PyErr_NoMemory())

/* These correspond to the slots tp_version_tag, tp_finalize, tp_vectorcall */
#if PY_VERSION_HEX < 0x03080000
#define PyType_TRAILER 0
#else
#define PyType_TRAILER 0, 0, 0
#endif

#if PY_VERSION_HEX < 0x030a0000
static PyObject *
Py_NewRef(PyObject *o)
{
  Py_INCREF(o);
  return o;
}
#endif

#define Py_TypeName(o) ((o) ? (Py_TYPE(o)->tp_name) : "NULL")

/*

This is necessary for calling into CPython for methods that cannot
handle an existing exception, such as PyObject_Call*.  If those
methods are called with an existing exception then when they return
CPython will raise:

SystemError: _PyEval_EvalFrameDefault returned a result with an
exception set

If there was an exception coming in, then this macro will turn any
exception in `x` into an unraisable exception and requires the
parameters for AddTraceBackHere to be provided.

*/

#define PY_EXC_HANDLE(x, func_name, locals_dict_spec, ...)                              \
  do                                                                                    \
  {                                                                                     \
    PyObject *e_type = NULL, *e_value = NULL, *e_traceback = NULL;                      \
    PyErr_Fetch(&e_type, &e_value, &e_traceback);                                       \
                                                                                        \
    x;                                                                                  \
                                                                                        \
    if ((e_type || e_value || e_traceback))                                             \
    {                                                                                   \
      if (PyErr_Occurred())                                                             \
      {                                                                                 \
        /* report the new error as unraisable because of the existing error */          \
        AddTraceBackHere(__FILE__, __LINE__, func_name, locals_dict_spec, __VA_ARGS__); \
        apsw_write_unraisable(NULL);                                                    \
      } /* put the old error back */                                                    \
      PyErr_Restore(e_type, e_value, e_traceback);                                      \
    }                                                                                   \
                                                                                        \
  } while (0)
;