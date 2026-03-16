/*
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef _PYPGMODULE_H
#define _PYPGMODULE_H

#define SQL_WMETADATA -888
// This is a custom constant that can be passed to Connection.setencoding.  Pick a value that
// is very different from SQL_CHAR and SQL_WCHAR and similar items.

inline namespace pyodbc {

extern PyObject* Error;
extern PyObject* Warning;
extern PyObject* InterfaceError;
extern PyObject* DatabaseError;
extern PyObject* InternalError;
extern PyObject* OperationalError;
extern PyObject* ProgrammingError;
extern PyObject* IntegrityError;
extern PyObject* DataError;
extern PyObject* NotSupportedError;

}

/*
  Returns the given class, specific to the current thread's interpreter.  For performance these
  are cached for each thread.

  This is for internal use only, so we'll cache using only the class name.  Make sure they are
  unique.  (That is, don't try to import classes with the same name from two different
  modules.)
*/
PyObject* GetClassForThread(const char* szModule, const char* szClass);

bool IsInstanceForThread(PyObject* param, const char* szModule, const char* szClass, PyObject** pcls);

extern PyObject* null_binary;

extern HENV henv;

extern PyTypeObject RowType;
extern PyTypeObject CursorType;
extern PyTypeObject ConnectionType;

// Thd pyodbc module.
extern PyObject* pModule;

inline bool lowercase()
{
    return PyObject_GetAttrString(pModule, "lowercase") == Py_True;
}

bool UseNativeUUID();
// Returns True if pyodbc.native_uuid is true, meaning uuid.UUID objects should be returned.

#endif // _PYPGMODULE_H
