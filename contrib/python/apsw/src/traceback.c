
/* These are Python header files */

#include "frameobject.h"

/* Add a dummy frame to the traceback so the developer has a better idea of what C code was doing

   @param filename: Use __FILE__ for this - it will be the filename reported in the frame
   @param lineno: Use __LINE__ for this - it will be the line number reported in the frame
   @param functionname: Name of the function reported
   @param localsformat: Format string for Py_BuildValue() that must specify a dictionary or NULL to make
                        an empty dictionary.  An example is "{s:i, s: s}" with the varargs then conforming
      to this format (the corresponding params could be "seven", 7, "foo", "bar"

*/
static void AddTraceBackHere(const char *filename, int lineno, const char *functionname, const char *localsformat, ...)
{
  /* See the implementation of _PyTraceback_Add for a template of what
     this code should do. That method does everything we need, except
     attaching variables */

  PyObject *localargs = 0, *one = 0, *two = 0, *three = 0, *empty_dict;
  PyCodeObject *code = 0;
  PyFrameObject *frame = 0;
  va_list localargsva;

  va_start(localargsva, localsformat);

  empty_dict = PyDict_New();
  /* we have to save and restore the error indicators otherwise intermediate code has no effect! */
  assert(PyErr_Occurred());
  PyErr_Fetch(&one, &two, &three);

  localargs = localsformat ? (Py_VaBuildValue((char *)localsformat, localargsva)) : NULL;
  /* this will only happen due to error in Py_BuildValue, usually
     because NULL was passed to O (PyObject*) format */
  assert(!PyErr_Occurred());
  assert(!localsformat || localsformat[0] == '{');
  assert(!localargs || PyDict_Check(localargs));

  /* make the dummy code object */
  code = PyCode_NewEmpty(filename, functionname, lineno);
  if (!code)
    goto end;

  /* make the dummy frame */
  frame = PyFrame_New(
      PyThreadState_Get(), /* PyThreadState *tstate */
      code,                /* PyCodeObject *code */
      empty_dict,          /* PyObject *globals */
      localargs            /* PyObject *locals */
  );
  if (!frame)
    goto end;

#if PY_VERSION_HEX < 0x030b0000
  frame->f_lineno = lineno;
#endif

  /* add dummy frame to traceback after restoring exception info */
  PyErr_Restore(one, two, three);
  PyTraceBack_Here(frame);

  /* this epilogue deals with success or failure cases */
end:
  va_end(localargsva);
  Py_XDECREF(localargs);
  Py_XDECREF(empty_dict);
  Py_XDECREF(code);
  Py_XDECREF(frame);
}
