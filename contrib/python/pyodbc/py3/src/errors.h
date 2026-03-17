
#ifndef _ERRORS_H_
#define _ERRORS_H_

// Sets an exception based on the ODBC SQLSTATE and error message and returns zero.  If either handle is not available,
// pass SQL_NULL_HANDLE.
//
// conn
//   The connection object, from which it will use the Unicode encoding. May be null if not available.
//
// szFunction
//   The name of the function that failed.  Python generates a useful stack trace, but we often don't know where in the
//   C++ code we failed.
//
PyObject* RaiseErrorFromHandle(Connection *conn, const char* szFunction, HDBC hdbc, HSTMT hstmt);

// Sets an exception using a printf-like error message.
//
// szSqlState
//   The optional SQLSTATE reported by ODBC.  If not provided (sqlstate is NULL or sqlstate[0] is NULL), "HY000"
//   (General Error) is used.  Note that HY000 causes Error to be used if exc_class is not provided.
//
// exc_class
//   The optional exception class (DatabaseError, etc.) to construct.  If NULL, the appropriate class will be
//   determined from the SQLSTATE.
//
PyObject* RaiseErrorV(const char* sqlstate, PyObject* exc_class, const char* format, ...);


// Constructs an exception and returns it.
//
// This function is like RaiseErrorFromHandle, but gives you the ability to examine the error first (in particular,
// used to examine the SQLSTATE using HasSqlState).  If you want to use the error, call PyErr_SetObject(ex->ob_type,
// ex).  Otherwise, dispose of the error using Py_DECREF(ex).
//
// conn
//   The connection object, from which it will use the Unicode encoding. May be null if not available.
//
// szFunction
//   The name of the function that failed.  Python generates a useful stack trace, but we often don't know where in the
//   C++ code we failed.
//
PyObject* GetErrorFromHandle(Connection *conn, const char* szFunction, HDBC hdbc, HSTMT hstmt);


// Returns true if `ex` is a database exception with SQLSTATE `szSqlState`.  Returns false otherwise.
// 
// It is safe to call with ex set to zero.  The SQLSTATE comparison is case-insensitive.
//
bool HasSqlState(PyObject* ex, const char* szSqlState);


// Returns true if the HSTMT has a diagnostic record with the given SQLSTATE.  This is used after SQLGetData call that
// returned SQL_SUCCESS_WITH_INFO to see if it also has SQLSTATE 01004, indicating there is more data.
//
bool HasSqlState(HSTMT hstmt, const char* szSqlState);

inline PyObject* RaiseErrorFromException(PyObject* pError)
{
    // PyExceptionInstance_Class doesn't exist in 2.4
    PyErr_SetObject((PyObject*)Py_TYPE(pError), pError);
    return 0;
}

inline void CopySqlState(const uint16_t* src, char* dest)
{
    // Copies a SQLSTATE read as SQLWCHAR into a character buffer.  We know that SQLSTATEs are
    // composed of ASCII characters and we need one standard to compare when choosing
    // exceptions.
    //
    // Strangely, even when the error messages are UTF-8, PostgreSQL and MySQL encode the
    // sqlstate as UTF-16LE.  We'll simply copy all non-zero bytes, with some checks for
    // running off the end of the buffers which will work for ASCII, UTF8, and UTF16 LE & BE.
    // It would work for UTF32 if I increase the size of the uint16_t buffer to handle it.
    //
    // (In the worst case, if a driver does something totally weird, we'll have an incomplete
    // SQLSTATE.)
    //

    const char* pchSrc = (const char*)src;
    const char* pchSrcMax = pchSrc + sizeof(uint16_t) * 5;
    char* pchDest = dest;         // Where we are copying into dest
    char* pchDestMax = dest + 5;  // We know a SQLSTATE is 5 characters long

    while (pchDest < pchDestMax && pchSrc < pchSrcMax)
    {
        if (*pchSrc)
            *pchDest++ = *pchSrc;
        pchSrc++;
    }
    *pchDest = 0;
}

#endif // _ERRORS_H_
