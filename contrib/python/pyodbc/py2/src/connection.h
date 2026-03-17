
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#ifndef CONNECTION_H
#define CONNECTION_H

struct Cursor;

extern PyTypeObject ConnectionType;

struct TextEnc;

struct Connection
{
    PyObject_HEAD

    // Set to SQL_NULL_HANDLE when the connection is closed.
    HDBC hdbc;

    // Will be SQL_AUTOCOMMIT_ON or SQL_AUTOCOMMIT_OFF.
    uintptr_t nAutoCommit;

    // The ODBC version the driver supports, from SQLGetInfo(DRIVER_ODBC_VER).  This is set after connecting.
    char odbc_major;
    char odbc_minor;

    // The escape character from SQLGetInfo.  This is not initialized until requested, so this may be zero!
    PyObject* searchescape;

    // Will be true if SQLDescribeParam is supported.  If false, we'll have to guess but the user will not be able
    // to insert NULLs into binary columns.
    bool supports_describeparam;

    // The column size of datetime columns, obtained from SQLGetInfo(), used to determine the datetime precision.
    int datetime_precision;

    // The connection timeout in seconds.
    long timeout;

    // Pointer connection attributes may require that the pointed-to object be kept
    // valid until some unspecified time in the future, so keep them here for now.
    PyObject* attrs_before;

    TextEnc sqlchar_enc;        // encoding used when reading SQL_CHAR data
    TextEnc sqlwchar_enc;       // encoding used when reading SQL_WCHAR data
    TextEnc unicode_enc;        // encoding used when writing unicode strings
#if PY_MAJOR_VERSION < 3
    TextEnc str_enc;            // encoding used when writing non-unicode strings
#endif

    TextEnc metadata_enc;
    // Used when reading column names for Cursor.description.  I originally thought I could use
    // the TextEncs above based on whether I called SQLDescribeCol vs SQLDescribeColW.
    // Unfortunately it looks like PostgreSQL and MySQL (and probably others) ignore the ODBC
    // specification regarding encoding everywhere *except* in these functions - SQLDescribeCol
    // seems to always return UTF-16LE by them regardless of the connection settings.

    long maxwrite;
    // Used to override varchar_maxlength, etc.  Those are initialized from
    // SQLGetTypeInfo but some drivers (e.g. psqlodbc) return almost arbitrary
    // values (like 255 chars) leading to very slow insert performance (lots of
    // small calls to SQLPutData).  If this is zero the values from
    // SQLGetTypeInfo are used.  Otherwise this value is used.

    // These are copied from cnxn info for performance and convenience.

    int varchar_maxlength;
    int wvarchar_maxlength;
    int binary_maxlength;

    SQLLEN GetMaxLength(SQLSMALLINT ctype) const
    {
        I(ctype == SQL_C_BINARY || ctype == SQL_C_WCHAR || ctype == SQL_C_CHAR);
        if (maxwrite != 0)
            return maxwrite;
        if (ctype == SQL_C_BINARY)
            return binary_maxlength;
        if (ctype == SQL_C_WCHAR)
            return wvarchar_maxlength;
        return varchar_maxlength;
    }

    bool need_long_data_len;

    // Output conversions.  Maps from SQL type in conv_types to the converter function in conv_funcs.
    //
    // If conv_count is zero, conv_types and conv_funcs will also be zero.
    //
    // pyodbc uses this manual mapping for speed and portability.  The STL collection classes use the new operator and
    // throw exceptions when out of memory.  pyodbc does not use any exceptions.

    int conv_count;             // how many items are in conv_types and conv_funcs.
    SQLSMALLINT* conv_types;            // array of SQL_TYPEs to convert
    PyObject** conv_funcs;      // array of Python functions
};

#define Connection_Check(op) PyObject_TypeCheck(op, &ConnectionType)
#define Connection_CheckExact(op) (Py_TYPE(op) == &ConnectionType)

/*
 * Used by the module's connect function to create new connection objects.  If unable to connect to the database, an
 * exception is set and zero is returned.
 */
PyObject* Connection_New(PyObject* pConnectString, bool fAutoCommit, bool fAnsi, long timeout, bool fReadOnly,
                         PyObject* attrs_before, Object& encoding);

/*
 * Used by the Cursor to implement commit and rollback.
 */
PyObject* Connection_endtrans(Connection* cnxn, SQLSMALLINT type);

#endif
