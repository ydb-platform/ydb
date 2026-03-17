// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "pyodbc.h"
#include "buffer.h"
#include "wrapper.h"
#include "textenc.h"
#include "connection.h"
#include "cursor.h"
#include "pyodbcmodule.h"
#include "errors.h"
#include "cnxninfo.h"

#if PY_MAJOR_VERSION < 3
static bool IsStringType(PyObject* t) { return (void*)t == (void*)&PyString_Type; }
static bool IsUnicodeType(PyObject* t) { return (void*)t == (void*)&PyUnicode_Type; }
#endif

static char connection_doc[] =
    "Connection objects manage connections to the database.\n"
    "\n"
    "Each manages a single ODBC HDBC.";

static Connection* Connection_Validate(PyObject* self)
{
    Connection* cnxn;

    if (self == 0 || !Connection_Check(self))
    {
        PyErr_SetString(PyExc_TypeError, "Connection object required");
        return 0;
    }

    cnxn = (Connection*)self;

    if (cnxn->hdbc == SQL_NULL_HANDLE)
    {
        PyErr_SetString(ProgrammingError, "Attempt to use a closed connection.");
        return 0;
    }

    return cnxn;
}

static bool Connect(PyObject* pConnectString, HDBC hdbc, bool fAnsi, long timeout,
                    Object& encoding)
{
    // This should have been checked by the global connect function.
    I(PyString_Check(pConnectString) || PyUnicode_Check(pConnectString));

    // The driver manager determines if the app is a Unicode app based on whether we call SQLDriverConnectA or
    // SQLDriverConnectW.  Some drivers, notably Microsoft Access/Jet, change their behavior based on this, so we try
    // the Unicode version first.  (The Access driver only supports Unicode text, but SQLDescribeCol returns SQL_CHAR
    // instead of SQL_WCHAR if we connect with the ANSI version.  Obviously this causes lots of errors since we believe
    // what it tells us (SQL_CHAR).)

    // Python supports only UCS-2 and UCS-4, so we shouldn't need to worry about receiving surrogate pairs.  However,
    // Windows does use UCS-16, so it is possible something would be misinterpreted as one.  We may need to examine
    // this more.

    SQLRETURN ret;

    if (timeout > 0)
    {
        Py_BEGIN_ALLOW_THREADS
        ret = SQLSetConnectAttr(hdbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)(uintptr_t)timeout, SQL_IS_UINTEGER);
        Py_END_ALLOW_THREADS
        if (!SQL_SUCCEEDED(ret))
            RaiseErrorFromHandle(0, "SQLSetConnectAttr(SQL_ATTR_LOGIN_TIMEOUT)", hdbc, SQL_NULL_HANDLE);
    }

    const char* szEncoding = 0;
    Object encBytes;
    if (encoding)
    {
        #if PY_MAJOR_VERSION < 3
        if (PyString_Check(encoding))
        {
            szEncoding = PyString_AsString(encoding);
            if (!szEncoding)
                return false;
        }
        #endif
        if (PyUnicode_Check(encoding))
        {
            #if PY_MAJOR_VERSION < 3
                encBytes = PyUnicode_AsUTF8String(encoding);
                if (!encBytes)
                    return false;
                szEncoding = PyString_AS_STRING(encBytes.Get());
            #else
                szEncoding = PyUnicode_AsUTF8(encoding);
            #endif
        }
    }

    if (!fAnsi)
    {
        // I want to call the W version when possible since the driver can use it as an
        // indication that we can handle Unicode.

        SQLWChar wchar(pConnectString, szEncoding ? szEncoding : ENCSTR_UTF16NE);
        if (!wchar.isValid())
            return false;

        Py_BEGIN_ALLOW_THREADS
        ret = SQLDriverConnectW(hdbc, 0, wchar.psz, SQL_NTS, 0, 0, 0, SQL_DRIVER_NOPROMPT);
        Py_END_ALLOW_THREADS
        if (SQL_SUCCEEDED(ret))
            return true;
    }

    SQLWChar wchar(pConnectString, szEncoding ? szEncoding : "utf-8");
    if (!wchar.isValid())
        return false;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLDriverConnect(hdbc, 0, (SQLCHAR*)wchar.psz, SQL_NTS, 0, 0, 0, SQL_DRIVER_NOPROMPT);
    Py_END_ALLOW_THREADS
    if (SQL_SUCCEEDED(ret))
        return true;

    RaiseErrorFromHandle(0, "SQLDriverConnect", hdbc, SQL_NULL_HANDLE);

    return false;
}

static bool ApplyPreconnAttrs(HDBC hdbc, SQLINTEGER ikey, PyObject *value, char *strencoding)
{
    SQLRETURN ret;
    SQLPOINTER ivalue = 0;
    SQLINTEGER vallen = 0;

    if (PyLong_Check(value))
    {
        if (_PyLong_Sign(value) >= 0)
        {
            ivalue = (SQLPOINTER)PyLong_AsUnsignedLong(value);
            vallen = SQL_IS_UINTEGER;
        } else
        {
            ivalue = (SQLPOINTER)PyLong_AsLong(value);
            vallen = SQL_IS_INTEGER;
        }
    }
#if PY_MAJOR_VERSION < 3
    else if (PyInt_Check(value))
    {
        ivalue = (SQLPOINTER)PyInt_AsLong(value);
        vallen = SQL_IS_INTEGER;
    }
    else if (PyBuffer_Check(value))
    {
        // We can only assume and take the first segment.
        PyBuffer_GetMemory(value, (const char**)&ivalue);
        vallen = SQL_IS_POINTER;
    }
#endif
#if PY_VERSION_HEX >= 0x02060000
    else if (PyByteArray_Check(value))
    {
        ivalue = (SQLPOINTER)PyByteArray_AsString(value);
        vallen = SQL_IS_POINTER;
    }
#endif
    else if (PyBytes_Check(value))
    {
        ivalue = PyBytes_AS_STRING(value);
#if PY_MAJOR_VERSION < 3
        vallen = SQL_NTS;
#else
        vallen = SQL_IS_POINTER;
#endif
    }
    else if (PyUnicode_Check(value))
    {
        Object stringholder;
if (sizeof(Py_UNICODE) == 2 // This part should be compile-time.
    && (!strencoding || !strcmp(strencoding, "utf-16le")))
{
        // default or utf-16le is set, pass through directly
        ivalue = PyUnicode_AS_UNICODE(value);
}
else
{
        // use strencoding to convert, default to utf-16le if not set.
        stringholder = PyCodec_Encode(value, strencoding ? strencoding : "utf-16le", "strict");
        ivalue = PyBytes_AS_STRING(stringholder.Get());
}
        vallen = SQL_NTS;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLSetConnectAttrW(hdbc, ikey, ivalue, vallen);
        Py_END_ALLOW_THREADS
        goto checkSuccess;
    }
    else if (PySequence_Check(value))
    {
        // To allow for possibility of setting multiple attributes more than once.
        Py_ssize_t len = PySequence_Size(value);
        for (Py_ssize_t i = 0; i < len; i++)
        {
            Object v(PySequence_GetItem(value, i));
            if (!ApplyPreconnAttrs(hdbc, ikey, v.Get(), strencoding))
                return false;
        }
        return true;
    }

    Py_BEGIN_ALLOW_THREADS
    ret = SQLSetConnectAttr(hdbc, ikey, ivalue, vallen);
    Py_END_ALLOW_THREADS

checkSuccess:
    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(0, "SQLSetConnectAttr", hdbc, SQL_NULL_HANDLE);
        Py_BEGIN_ALLOW_THREADS
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        Py_END_ALLOW_THREADS
        return false;
    }
    return true;
}

PyObject* Connection_New(PyObject* pConnectString, bool fAutoCommit, bool fAnsi, long timeout, bool fReadOnly,
                         PyObject* attrs_before, Object& encoding)
{
    // pConnectString
    //   A string or unicode object.  (This must be checked by the caller.)
    //
    // fAnsi
    //   If true, do not attempt a Unicode connection.

    //
    // Allocate HDBC and connect
    //

    Object attrs_before_o(attrs_before);
    HDBC hdbc = SQL_NULL_HANDLE;
    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(0, "SQLAllocHandle", SQL_NULL_HANDLE, SQL_NULL_HANDLE);

    //
    // Attributes that must be set before connecting.
    //

    if (attrs_before)
    {
        Py_ssize_t pos = 0;
        PyObject* key = 0;
        PyObject* value = 0;

        Object encodingholder;
        char *strencoding = encoding.Get() ?
            (PyUnicode_Check(encoding) ? PyBytes_AsString(encodingholder = PyCodec_Encode(encoding, "utf-8", "strict")) :
             PyBytes_Check(encoding) ? PyBytes_AsString(encoding) : 0) : 0;

        while (PyDict_Next(attrs_before, &pos, &key, &value))
        {
            SQLINTEGER ikey = 0;

            if (PyLong_Check(key))
                ikey = (int)PyLong_AsLong(key);
#if PY_MAJOR_VERSION < 3
            else if (PyInt_Check(key))
                ikey = (int)PyInt_AsLong(key);
#endif
            if (!ApplyPreconnAttrs(hdbc, ikey, value, strencoding))
            {
                return 0;
            }
        }
    }

    if (!Connect(pConnectString, hdbc, fAnsi, timeout, encoding))
    {
        // Connect has already set an exception.
        Py_BEGIN_ALLOW_THREADS
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        Py_END_ALLOW_THREADS
        return 0;
    }

    //
    // Connected, so allocate the Connection object.
    //

    // Set all variables to something valid, so we don't crash in dealloc if this function fails.

#ifdef _MSC_VER
#pragma warning(disable : 4365)
#endif
    Connection* cnxn = PyObject_NEW(Connection, &ConnectionType);
#ifdef _MSC_VER
#pragma warning(default : 4365)
#endif

    if (cnxn == 0)
    {
        Py_BEGIN_ALLOW_THREADS
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        Py_END_ALLOW_THREADS
        return 0;
    }

    cnxn->hdbc         = hdbc;
    cnxn->nAutoCommit  = fAutoCommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    cnxn->searchescape = 0;
    cnxn->maxwrite     = 0;
    cnxn->timeout      = 0;
    cnxn->conv_count   = 0;
    cnxn->conv_types   = 0;
    cnxn->conv_funcs   = 0;

    cnxn->attrs_before = attrs_before_o.Detach();

    // This is an inefficient default, but should work all the time.  When we are offered
    // single-byte text we don't actually know what the encoding is.  For example, with SQL
    // Server the encoding is based on the database's collation.  We ask the driver / DB to
    // convert to SQL_C_WCHAR and use the ODBC default of UTF-16LE.
    cnxn->sqlchar_enc.optenc = OPTENC_UTF16NE;
    cnxn->sqlchar_enc.name   = _strdup(ENCSTR_UTF16NE);
    cnxn->sqlchar_enc.ctype  = SQL_C_WCHAR;

    cnxn->sqlwchar_enc.optenc = OPTENC_UTF16NE;
    cnxn->sqlwchar_enc.name   = _strdup(ENCSTR_UTF16NE);
    cnxn->sqlwchar_enc.ctype  = SQL_C_WCHAR;

    cnxn->metadata_enc.optenc = OPTENC_UTF16NE;
    cnxn->metadata_enc.name   = _strdup(ENCSTR_UTF16NE);
    cnxn->metadata_enc.ctype  = SQL_C_WCHAR;

    // Note: I attempted to use UTF-8 here too since it can hold any type, but SQL Server fails
    // with a data truncation error if we send something encoded in 2 bytes to a column with 1
    // character.  I don't know if this is a bug in SQL Server's driver or if I'm missing
    // something, so we'll stay with the default ODBC conversions.
    cnxn->unicode_enc.optenc = OPTENC_UTF16NE;
    cnxn->unicode_enc.name   = _strdup(ENCSTR_UTF16NE);
    cnxn->unicode_enc.ctype  = SQL_C_WCHAR;

#if PY_MAJOR_VERSION < 3
    cnxn->str_enc.optenc = OPTENC_UTF8;
    cnxn->str_enc.name   = _strdup("utf-8");
    cnxn->str_enc.ctype  = SQL_C_CHAR;

    cnxn->sqlchar_enc.to  = TO_UNICODE;
    cnxn->sqlwchar_enc.to = TO_UNICODE;
    cnxn->metadata_enc.to = TO_UNICODE;
#endif

    if (!cnxn->sqlchar_enc.name || !cnxn->sqlwchar_enc.name || !cnxn->metadata_enc.name || !cnxn->unicode_enc.name
#if PY_MAJOR_VERSION < 3
        || !cnxn->str_enc.name
#endif
        )
    {
        PyErr_NoMemory();
        Py_DECREF(cnxn);
        return 0;
    }

    //
    // Initialize autocommit mode.
    //

    // The DB API says we have to default to manual-commit, but ODBC defaults to auto-commit.  We also provide a
    // keyword parameter that allows the user to override the DB API and force us to start in auto-commit (in which
    // case we don't have to do anything).

    if (fAutoCommit == false)
    {
        Py_BEGIN_ALLOW_THREADS
        ret = SQLSetConnectAttr(cnxn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)cnxn->nAutoCommit, SQL_IS_UINTEGER);
        Py_END_ALLOW_THREADS

        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cnxn, "SQLSetConnectAttr(SQL_ATTR_AUTOCOMMIT)", cnxn->hdbc, SQL_NULL_HANDLE);
            Py_DECREF(cnxn);
            return 0;
        }
    }

    if (fReadOnly)
    {
        Py_BEGIN_ALLOW_THREADS
        ret = SQLSetConnectAttr(cnxn->hdbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
        Py_END_ALLOW_THREADS

        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cnxn, "SQLSetConnectAttr(SQL_ATTR_ACCESS_MODE)", cnxn->hdbc, SQL_NULL_HANDLE);
            Py_DECREF(cnxn);
            return 0;
        }
    }

    TRACE("cnxn.new cnxn=%p hdbc=%d\n", cnxn, cnxn->hdbc);

    //
    // Gather connection-level information we'll need later.
    //

    Object info(GetConnectionInfo(pConnectString, cnxn));

    if (!info.IsValid())
    {
        Py_DECREF(cnxn);
        return 0;
    }

    CnxnInfo* p = (CnxnInfo*)info.Get();
    cnxn->odbc_major             = p->odbc_major;
    cnxn->odbc_minor             = p->odbc_minor;
    cnxn->supports_describeparam = p->supports_describeparam;
    cnxn->datetime_precision     = p->datetime_precision;
    cnxn->need_long_data_len     = p->need_long_data_len;
    cnxn->varchar_maxlength      = p->varchar_maxlength;
    cnxn->wvarchar_maxlength     = p->wvarchar_maxlength;
    cnxn->binary_maxlength       = p->binary_maxlength;

    return reinterpret_cast<PyObject*>(cnxn);
}

static void _clear_conv(Connection* cnxn)
{
    if (cnxn->conv_count != 0)
    {
        pyodbc_free(cnxn->conv_types);
        cnxn->conv_types = 0;

        for (int i = 0; i < cnxn->conv_count; i++)
            Py_XDECREF(cnxn->conv_funcs[i]);
        pyodbc_free(cnxn->conv_funcs);
        cnxn->conv_funcs = 0;

        cnxn->conv_count = 0;
    }
}

static char set_attr_doc[] =
    "set_attr(attr_id, value) -> None\n\n"
    "Calls SQLSetConnectAttr with the given values.\n\n"
    "attr_id\n"
    "  The attribute id (integer) to set.  These are ODBC or driver constants.\n\n"
    "value\n"
    "  An integer value.\n\n"
    "At this time, only integer values are supported and are always passed as SQLUINTEGER.";

static PyObject* Connection_set_attr(PyObject* self, PyObject* args)
{
    int id;
    int value;
    if (!PyArg_ParseTuple(args, "ii", &id, &value))
        return 0;

    Connection* cnxn = (Connection*)self;

    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLSetConnectAttr(cnxn->hdbc, id, (SQLPOINTER)(intptr_t)value, SQL_IS_INTEGER);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cnxn, "SQLSetConnectAttr", cnxn->hdbc, SQL_NULL_HANDLE);
    Py_RETURN_NONE;
}

static char conv_clear_doc[] =
    "clear_output_converters() --> None\n\n"
    "Remove all output converter functions.";

static PyObject* Connection_conv_clear(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Connection* cnxn = (Connection*)self;
    _clear_conv(cnxn);
    Py_RETURN_NONE;
}

static int Connection_clear(PyObject* self)
{
    // Internal method for closing the connection.  (Not called close so it isn't confused with the external close
    // method.)

    Connection* cnxn = (Connection*)self;

    if (cnxn->hdbc != SQL_NULL_HANDLE)
    {
        TRACE("cnxn.clear cnxn=%p hdbc=%d\n", cnxn, cnxn->hdbc);

        HDBC hdbc = cnxn->hdbc;
        cnxn->hdbc = SQL_NULL_HANDLE;
        Py_BEGIN_ALLOW_THREADS
        if (cnxn->nAutoCommit == SQL_AUTOCOMMIT_OFF)
            SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);

        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        Py_END_ALLOW_THREADS
    }

    Py_XDECREF(cnxn->searchescape);
    cnxn->searchescape = 0;

    free((void*)cnxn->sqlchar_enc.name);
    cnxn->sqlchar_enc.name = 0;
    free((void*)cnxn->sqlwchar_enc.name);
    cnxn->sqlwchar_enc.name = 0;
    free((void*)cnxn->metadata_enc.name);
    cnxn->metadata_enc.name = 0;
    free((void*)cnxn->unicode_enc.name);
    cnxn->unicode_enc.name = 0;
#if PY_MAJOR_VERSION < 3
    free((void*)cnxn->str_enc.name);
    cnxn->str_enc.name = 0;
#endif

    Py_XDECREF(cnxn->attrs_before);
    cnxn->attrs_before = 0;

    _clear_conv(cnxn);

    return 0;
}

static void Connection_dealloc(PyObject* self)
{
    Connection_clear(self);
    PyObject_Del(self);
}

static char close_doc[] =
    "Close the connection now (rather than whenever __del__ is called).\n"
    "\n"
    "The connection will be unusable from this point forward and a ProgrammingError\n"
    "will be raised if any operation is attempted with the connection.  The same\n"
    "applies to all cursor objects trying to use the connection.\n"
    "\n"
    "Note that closing a connection without committing the changes first will cause\n"
    "an implicit rollback to be performed.";

static PyObject* Connection_close(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    Connection_clear(self);

    Py_RETURN_NONE;
}

static PyObject* Connection_cursor(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    return (PyObject*)Cursor_New(cnxn);
}

static PyObject* Connection_execute(PyObject* self, PyObject* args)
{
    PyObject* result = 0;

    Cursor* cursor;
    Connection* cnxn = Connection_Validate(self);

    if (!cnxn)
        return 0;

    cursor = Cursor_New(cnxn);
    if (!cursor)
        return 0;

    result = Cursor_execute((PyObject*)cursor, args);

    Py_DECREF((PyObject*)cursor);

    return result;
}

enum
{
    GI_YESNO,
    GI_STRING,
    GI_UINTEGER,
    GI_USMALLINT,
};

struct GetInfoType
{
    SQLUSMALLINT infotype;
    int datatype; // GI_XXX
};

static const GetInfoType aInfoTypes[] = {

    // SQL_CONVERT_X
    { SQL_CONVERT_FUNCTIONS, GI_UINTEGER },
    { SQL_CONVERT_BIGINT, GI_UINTEGER },
    { SQL_CONVERT_BINARY, GI_UINTEGER },
    { SQL_CONVERT_BIT, GI_UINTEGER },
    { SQL_CONVERT_CHAR, GI_UINTEGER },
    { SQL_CONVERT_DATE, GI_UINTEGER },
    { SQL_CONVERT_DECIMAL, GI_UINTEGER },
    { SQL_CONVERT_DOUBLE, GI_UINTEGER },
    { SQL_CONVERT_FLOAT, GI_UINTEGER },
    { SQL_CONVERT_INTEGER, GI_UINTEGER },
    { SQL_CONVERT_LONGVARCHAR, GI_UINTEGER },
    { SQL_CONVERT_NUMERIC, GI_UINTEGER },
    { SQL_CONVERT_REAL, GI_UINTEGER },
    { SQL_CONVERT_SMALLINT, GI_UINTEGER },
    { SQL_CONVERT_TIME, GI_UINTEGER },
    { SQL_CONVERT_TIMESTAMP, GI_UINTEGER },
    { SQL_CONVERT_TINYINT, GI_UINTEGER },
    { SQL_CONVERT_VARBINARY, GI_UINTEGER },
    { SQL_CONVERT_VARCHAR, GI_UINTEGER },
    { SQL_CONVERT_LONGVARBINARY, GI_UINTEGER },
    { SQL_CONVERT_WCHAR, GI_UINTEGER },
    { SQL_CONVERT_INTERVAL_DAY_TIME, GI_UINTEGER },
    { SQL_CONVERT_INTERVAL_YEAR_MONTH, GI_UINTEGER },
    { SQL_CONVERT_WLONGVARCHAR, GI_UINTEGER },
    { SQL_CONVERT_WVARCHAR, GI_UINTEGER },
    { SQL_CONVERT_GUID, GI_UINTEGER },

    { SQL_ACCESSIBLE_PROCEDURES, GI_YESNO },
    { SQL_ACCESSIBLE_TABLES, GI_YESNO },
    { SQL_ACTIVE_ENVIRONMENTS, GI_USMALLINT },
    { SQL_AGGREGATE_FUNCTIONS, GI_UINTEGER },
    { SQL_ALTER_DOMAIN, GI_UINTEGER },
    { SQL_ALTER_TABLE, GI_UINTEGER },
    { SQL_ASYNC_MODE, GI_UINTEGER },
    { SQL_BATCH_ROW_COUNT, GI_UINTEGER },
    { SQL_BATCH_SUPPORT, GI_UINTEGER },
    { SQL_BOOKMARK_PERSISTENCE, GI_UINTEGER },
    { SQL_CATALOG_LOCATION, GI_USMALLINT },
    { SQL_CATALOG_NAME, GI_YESNO },
    { SQL_CATALOG_NAME_SEPARATOR, GI_STRING },
    { SQL_CATALOG_TERM, GI_STRING },
    { SQL_CATALOG_USAGE, GI_UINTEGER },
    { SQL_COLLATION_SEQ, GI_STRING },
    { SQL_COLUMN_ALIAS, GI_YESNO },
    { SQL_CONCAT_NULL_BEHAVIOR, GI_USMALLINT },
    { SQL_CORRELATION_NAME, GI_USMALLINT },
    { SQL_CREATE_ASSERTION, GI_UINTEGER },
    { SQL_CREATE_CHARACTER_SET, GI_UINTEGER },
    { SQL_CREATE_COLLATION, GI_UINTEGER },
    { SQL_CREATE_DOMAIN, GI_UINTEGER },
    { SQL_CREATE_SCHEMA, GI_UINTEGER },
    { SQL_CREATE_TABLE, GI_UINTEGER },
    { SQL_CREATE_TRANSLATION, GI_UINTEGER },
    { SQL_CREATE_VIEW, GI_UINTEGER },
    { SQL_CURSOR_COMMIT_BEHAVIOR, GI_USMALLINT },
    { SQL_CURSOR_ROLLBACK_BEHAVIOR, GI_USMALLINT },
    { SQL_DATABASE_NAME, GI_STRING },
    { SQL_DATA_SOURCE_NAME, GI_STRING },
    { SQL_DATA_SOURCE_READ_ONLY, GI_YESNO },
    { SQL_DATETIME_LITERALS, GI_UINTEGER },
    { SQL_DBMS_NAME, GI_STRING },
    { SQL_DBMS_VER, GI_STRING },
    { SQL_DDL_INDEX, GI_UINTEGER },
    { SQL_DEFAULT_TXN_ISOLATION, GI_UINTEGER },
    { SQL_DESCRIBE_PARAMETER, GI_YESNO },
    { SQL_DM_VER, GI_STRING },
    { SQL_DRIVER_NAME, GI_STRING },
    { SQL_DRIVER_ODBC_VER, GI_STRING },
    { SQL_DRIVER_VER, GI_STRING },
    { SQL_DROP_ASSERTION, GI_UINTEGER },
    { SQL_DROP_CHARACTER_SET, GI_UINTEGER },
    { SQL_DROP_COLLATION, GI_UINTEGER },
    { SQL_DROP_DOMAIN, GI_UINTEGER },
    { SQL_DROP_SCHEMA, GI_UINTEGER },
    { SQL_DROP_TABLE, GI_UINTEGER },
    { SQL_DROP_TRANSLATION, GI_UINTEGER },
    { SQL_DROP_VIEW, GI_UINTEGER },
    { SQL_DYNAMIC_CURSOR_ATTRIBUTES1, GI_UINTEGER },
    { SQL_DYNAMIC_CURSOR_ATTRIBUTES2, GI_UINTEGER },
    { SQL_EXPRESSIONS_IN_ORDERBY, GI_YESNO },
    { SQL_FILE_USAGE, GI_USMALLINT },
    { SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1, GI_UINTEGER },
    { SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2, GI_UINTEGER },
    { SQL_GETDATA_EXTENSIONS, GI_UINTEGER },
    { SQL_GROUP_BY, GI_USMALLINT },
    { SQL_IDENTIFIER_CASE, GI_USMALLINT },
    { SQL_IDENTIFIER_QUOTE_CHAR, GI_STRING },
    { SQL_INDEX_KEYWORDS, GI_UINTEGER },
    { SQL_INFO_SCHEMA_VIEWS, GI_UINTEGER },
    { SQL_INSERT_STATEMENT, GI_UINTEGER },
    { SQL_INTEGRITY, GI_YESNO },
    { SQL_KEYSET_CURSOR_ATTRIBUTES1, GI_UINTEGER },
    { SQL_KEYSET_CURSOR_ATTRIBUTES2, GI_UINTEGER },
    { SQL_KEYWORDS, GI_STRING },
    { SQL_LIKE_ESCAPE_CLAUSE, GI_YESNO },
    { SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, GI_UINTEGER },
    { SQL_MAX_BINARY_LITERAL_LEN, GI_UINTEGER },
    { SQL_MAX_CATALOG_NAME_LEN, GI_USMALLINT },
    { SQL_MAX_CHAR_LITERAL_LEN, GI_UINTEGER },
    { SQL_MAX_COLUMNS_IN_GROUP_BY, GI_USMALLINT },
    { SQL_MAX_COLUMNS_IN_INDEX, GI_USMALLINT },
    { SQL_MAX_COLUMNS_IN_ORDER_BY, GI_USMALLINT },
    { SQL_MAX_COLUMNS_IN_SELECT, GI_USMALLINT },
    { SQL_MAX_COLUMNS_IN_TABLE, GI_USMALLINT },
    { SQL_MAX_COLUMN_NAME_LEN, GI_USMALLINT },
    { SQL_MAX_CONCURRENT_ACTIVITIES, GI_USMALLINT },
    { SQL_MAX_CURSOR_NAME_LEN, GI_USMALLINT },
    { SQL_MAX_DRIVER_CONNECTIONS, GI_USMALLINT },
    { SQL_MAX_IDENTIFIER_LEN, GI_USMALLINT },
    { SQL_MAX_INDEX_SIZE, GI_UINTEGER },
    { SQL_MAX_PROCEDURE_NAME_LEN, GI_USMALLINT },
    { SQL_MAX_ROW_SIZE, GI_UINTEGER },
    { SQL_MAX_ROW_SIZE_INCLUDES_LONG, GI_YESNO },
    { SQL_MAX_SCHEMA_NAME_LEN, GI_USMALLINT },
    { SQL_MAX_STATEMENT_LEN, GI_UINTEGER },
    { SQL_MAX_TABLES_IN_SELECT, GI_USMALLINT },
    { SQL_MAX_TABLE_NAME_LEN, GI_USMALLINT },
    { SQL_MAX_USER_NAME_LEN, GI_USMALLINT },
    { SQL_MULTIPLE_ACTIVE_TXN, GI_YESNO },
    { SQL_MULT_RESULT_SETS, GI_YESNO },
    { SQL_NEED_LONG_DATA_LEN, GI_YESNO },
    { SQL_NON_NULLABLE_COLUMNS, GI_USMALLINT },
    { SQL_NULL_COLLATION, GI_USMALLINT },
    { SQL_NUMERIC_FUNCTIONS, GI_UINTEGER },
    { SQL_ODBC_INTERFACE_CONFORMANCE, GI_UINTEGER },
    { SQL_ODBC_VER, GI_STRING },
    { SQL_OJ_CAPABILITIES, GI_UINTEGER },
    { SQL_ORDER_BY_COLUMNS_IN_SELECT, GI_YESNO },
    { SQL_PARAM_ARRAY_ROW_COUNTS, GI_UINTEGER },
    { SQL_PARAM_ARRAY_SELECTS, GI_UINTEGER },
    { SQL_PROCEDURES, GI_YESNO },
    { SQL_PROCEDURE_TERM, GI_STRING },
    { SQL_QUOTED_IDENTIFIER_CASE, GI_USMALLINT },
    { SQL_ROW_UPDATES, GI_YESNO },
    { SQL_SCHEMA_TERM, GI_STRING },
    { SQL_SCHEMA_USAGE, GI_UINTEGER },
    { SQL_SCROLL_OPTIONS, GI_UINTEGER },
    { SQL_SEARCH_PATTERN_ESCAPE, GI_STRING },
    { SQL_SERVER_NAME, GI_STRING },
    { SQL_SPECIAL_CHARACTERS, GI_STRING },
    { SQL_SQL92_DATETIME_FUNCTIONS, GI_UINTEGER },
    { SQL_SQL92_FOREIGN_KEY_DELETE_RULE, GI_UINTEGER },
    { SQL_SQL92_FOREIGN_KEY_UPDATE_RULE, GI_UINTEGER },
    { SQL_SQL92_GRANT, GI_UINTEGER },
    { SQL_SQL92_NUMERIC_VALUE_FUNCTIONS, GI_UINTEGER },
    { SQL_SQL92_PREDICATES, GI_UINTEGER },
    { SQL_SQL92_RELATIONAL_JOIN_OPERATORS, GI_UINTEGER },
    { SQL_SQL92_REVOKE, GI_UINTEGER },
    { SQL_SQL92_ROW_VALUE_CONSTRUCTOR, GI_UINTEGER },
    { SQL_SQL92_STRING_FUNCTIONS, GI_UINTEGER },
    { SQL_SQL92_VALUE_EXPRESSIONS, GI_UINTEGER },
    { SQL_SQL_CONFORMANCE, GI_UINTEGER },
    { SQL_STANDARD_CLI_CONFORMANCE, GI_UINTEGER },
    { SQL_STATIC_CURSOR_ATTRIBUTES1, GI_UINTEGER },
    { SQL_STATIC_CURSOR_ATTRIBUTES2, GI_UINTEGER },
    { SQL_STRING_FUNCTIONS, GI_UINTEGER },
    { SQL_SUBQUERIES, GI_UINTEGER },
    { SQL_SYSTEM_FUNCTIONS, GI_UINTEGER },
    { SQL_TABLE_TERM, GI_STRING },
    { SQL_TIMEDATE_ADD_INTERVALS, GI_UINTEGER },
    { SQL_TIMEDATE_DIFF_INTERVALS, GI_UINTEGER },
    { SQL_TIMEDATE_FUNCTIONS, GI_UINTEGER },
    { SQL_TXN_CAPABLE, GI_USMALLINT },
    { SQL_TXN_ISOLATION_OPTION, GI_UINTEGER },
    { SQL_UNION, GI_UINTEGER },
    { SQL_USER_NAME, GI_STRING },
    { SQL_XOPEN_CLI_YEAR, GI_STRING },
};


static PyObject* Connection_getinfo(PyObject* self, PyObject* args)
{
    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    unsigned long infotype;
    if (!PyArg_ParseTuple(args, "k", &infotype))
        return 0;

    unsigned int i = 0;
    for (; i < _countof(aInfoTypes); i++)
    {
        if (aInfoTypes[i].infotype == infotype)
            break;
    }

    if (i == _countof(aInfoTypes))
        return RaiseErrorV(0, ProgrammingError, "Unsupported getinfo value: %d", infotype);

    char szBuffer[0x1000];
    SQLSMALLINT cch = 0;

    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLGetInfo(cnxn->hdbc, (SQLUSMALLINT)infotype, szBuffer, sizeof(szBuffer), &cch);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(cnxn, "SQLGetInfo", cnxn->hdbc, SQL_NULL_HANDLE);
        return 0;
    }

    PyObject* result = 0;

    switch (aInfoTypes[i].datatype)
    {
    case GI_YESNO:
        result = (szBuffer[0] == 'Y') ? Py_True : Py_False;
        Py_INCREF(result);
        break;

    case GI_STRING:
        result = PyString_FromStringAndSize(szBuffer, (Py_ssize_t)cch);
        break;

    case GI_UINTEGER:
    {
        SQLUINTEGER n = *(SQLUINTEGER*)szBuffer; // Does this work on PPC or do we need a union?
#if PY_MAJOR_VERSION >= 3
        result = PyLong_FromLong((long)n);
#else
        if (n <= (SQLUINTEGER)PyInt_GetMax())
            result = PyInt_FromLong((long)n);
        else
            result = PyLong_FromUnsignedLong(n);
#endif
        break;
    }

    case GI_USMALLINT:
        result = PyInt_FromLong(*(SQLUSMALLINT*)szBuffer);
        break;
    }

    return result;
}

PyObject* Connection_endtrans(Connection* cnxn, SQLSMALLINT type)
{
    // If called from Cursor.commit, it is possible that `cnxn` is deleted by another thread when we release them
    // below.  (The cursor has had its reference incremented by the method it is calling, but nothing has incremented
    // the connections count.  We could, but we really only need the HDBC.)
    HDBC hdbc = cnxn->hdbc;

    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, type);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(cnxn, "SQLEndTran", hdbc, SQL_NULL_HANDLE);
        return 0;
    }

    Py_RETURN_NONE;
}

static PyObject* Connection_commit(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    TRACE("commit: cnxn=%p hdbc=%d\n", cnxn, cnxn->hdbc);

    return Connection_endtrans(cnxn, SQL_COMMIT);
}

static PyObject* Connection_rollback(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    TRACE("rollback: cnxn=%p hdbc=%d\n", cnxn, cnxn->hdbc);

    return Connection_endtrans(cnxn, SQL_ROLLBACK);
}

static char cursor_doc[] =
    "Return a new Cursor object using the connection.";

static char execute_doc[] =
    "execute(sql, [params]) --> Cursor\n"
    "\n"
    "Create a new Cursor object, call its execute method, and return it.  See\n"
    "Cursor.execute for more details.\n"
    "\n"
    "This is a convenience method that is not part of the DB API.  Since a new\n"
    "Cursor is allocated by each call, this should not be used if more than one SQL\n"
    "statement needs to be executed.";

static char commit_doc[] =
    "Commit any pending transaction to the database.";

static char rollback_doc[] =
    "Causes the the database to roll back to the start of any pending transaction.";

static char getinfo_doc[] =
    "getinfo(type) --> str | int | bool\n"
    "\n"
    "Calls SQLGetInfo, passing `type`, and returns the result formatted as a Python object.";


PyObject* Connection_getautocommit(PyObject* self, void* closure)
{
    UNUSED(closure);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    PyObject* result = (cnxn->nAutoCommit == SQL_AUTOCOMMIT_ON) ? Py_True : Py_False;
    Py_INCREF(result);
    return result;
}

static int Connection_setautocommit(PyObject* self, PyObject* value, void* closure)
{
    UNUSED(closure);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return -1;

    if (value == 0)
    {
        PyErr_SetString(PyExc_TypeError, "Cannot delete the autocommit attribute.");
        return -1;
    }

    uintptr_t nAutoCommit = PyObject_IsTrue(value) ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLSetConnectAttr(cnxn->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)nAutoCommit, SQL_IS_UINTEGER);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(cnxn, "SQLSetConnectAttr", cnxn->hdbc, SQL_NULL_HANDLE);
        return -1;
    }

    cnxn->nAutoCommit = nAutoCommit;

    return 0;
}


static PyObject* Connection_getclosed(PyObject* self, void* closure)
{
    UNUSED(closure);
    Connection* cnxn;

    if (self == 0 || !Connection_Check(self))
    {
        PyErr_SetString(PyExc_TypeError, "Connection object required");
        return 0;
    }

    cnxn = (Connection*)self;

    if (cnxn->hdbc == SQL_NULL_HANDLE)
    {
        Py_RETURN_TRUE;
    }

    Py_RETURN_FALSE;
}


static PyObject* Connection_getsearchescape(PyObject* self, void* closure)
{
    UNUSED(closure);

    Connection* cnxn = (Connection*)self;

    if (!cnxn->searchescape)
    {
        char sz[8] = { 0 };
        SQLSMALLINT cch = 0;

        SQLRETURN ret;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLGetInfo(cnxn->hdbc, SQL_SEARCH_PATTERN_ESCAPE, &sz, _countof(sz), &cch);
        Py_END_ALLOW_THREADS
        if (!SQL_SUCCEEDED(ret))
            return RaiseErrorFromHandle(cnxn, "SQLGetInfo", cnxn->hdbc, SQL_NULL_HANDLE);

        cnxn->searchescape = PyString_FromStringAndSize(sz, (Py_ssize_t)cch);
    }

    Py_INCREF(cnxn->searchescape);
    return cnxn->searchescape;
}

static PyObject* Connection_getmaxwrite(PyObject* self, void* closure)
{
    UNUSED(closure);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    return PyLong_FromSsize_t(cnxn->maxwrite);
}

static int Connection_setmaxwrite(PyObject* self, PyObject* value, void* closure)
{
    UNUSED(closure);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return -1;

    if (value == 0)
    {
        PyErr_SetString(PyExc_TypeError, "Cannot delete the maxwrite attribute.");
        return -1;
    }
    long maxwrite = PyLong_AsLong(value);
    if (PyErr_Occurred())
        return -1;

    Py_ssize_t minval = 255;

    if (maxwrite != 0 && maxwrite < minval)
    {
        PyErr_Format(PyExc_ValueError, "Cannot set maxwrite less than %d unless setting to 0.", (int)minval);
        return -1;
    }

    cnxn->maxwrite = maxwrite;

    return 0;
}


static PyObject* Connection_gettimeout(PyObject* self, void* closure)
{
    UNUSED(closure);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return 0;

    return PyInt_FromLong(cnxn->timeout);
}

static int Connection_settimeout(PyObject* self, PyObject* value, void* closure)
{
    UNUSED(closure);

    Connection* cnxn = Connection_Validate(self);
    if (!cnxn)
        return -1;

    if (value == 0)
    {
        PyErr_SetString(PyExc_TypeError, "Cannot delete the timeout attribute.");
        return -1;
    }
    long timeout = PyInt_AsLong(value);
    if (timeout == -1 && PyErr_Occurred())
        return -1;
    if (timeout < 0)
    {
        PyErr_SetString(PyExc_ValueError, "Cannot set a negative timeout.");
        return -1;
    }

    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLSetConnectAttr(cnxn->hdbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)(uintptr_t)timeout, SQL_IS_UINTEGER);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(cnxn, "SQLSetConnectAttr", cnxn->hdbc, SQL_NULL_HANDLE);
        return -1;
    }

    cnxn->timeout = timeout;

    return 0;
}

static bool _remove_converter(PyObject* self, SQLSMALLINT sqltype)
{
    Connection* cnxn = (Connection*)self;

    if (!cnxn->conv_count)
    {
        // There are no converters, so nothing to remove.
        return true;
    }

    int          count = cnxn->conv_count;
    SQLSMALLINT* types = cnxn->conv_types;
    PyObject**   funcs = cnxn->conv_funcs;

    int i = 0;
    for (; i < count; i++)
        if (types[i] == sqltype)
            break;

    if (i == count)
    {
        // There is no converter for this type, so nothing to remove.
        return true;
    }

    Py_DECREF(funcs[i]);

    int move = count - i - 1;  // How many are we moving?
    if (move > 0)
    {
        memcpy(&types[i], &types[i+1], move * sizeof(SQLSMALLINT));
        memcpy(&funcs[i], &funcs[i+1], move * sizeof(PyObject*));
    }
    count--;

    // Note: If the realloc fails, the old array is still around and is 1 element too long but
    // everything will still work, so we ignore.
    pyodbc_realloc((BYTE**)&types, count * sizeof(SQLSMALLINT));
    pyodbc_realloc((BYTE**)&funcs, count * sizeof(PyObject*));

    cnxn->conv_count = count;
    cnxn->conv_types = types;
    cnxn->conv_funcs = funcs;

    return true;
}


static bool _add_converter(PyObject* self, SQLSMALLINT sqltype, PyObject* func)
{
    Connection* cnxn = (Connection*)self;

    if (cnxn->conv_count)
    {
        // If the sqltype is already registered, replace the old conversion function with the new.
        for (int i = 0; i < cnxn->conv_count; i++)
        {
            if (cnxn->conv_types[i] == sqltype)
            {
                Py_XDECREF(cnxn->conv_funcs[i]);
                cnxn->conv_funcs[i] = func;
                Py_INCREF(func);
                return true;
            }
        }
    }

    int          oldcount = cnxn->conv_count;
    SQLSMALLINT* oldtypes = cnxn->conv_types;
    PyObject**   oldfuncs = cnxn->conv_funcs;

    int          newcount = oldcount + 1;
    SQLSMALLINT* newtypes = (SQLSMALLINT*)pyodbc_malloc(sizeof(SQLSMALLINT) * newcount);
    PyObject**   newfuncs = (PyObject**)pyodbc_malloc(sizeof(PyObject*) * newcount);

    if (newtypes == 0 || newfuncs == 0)
    {
        if (newtypes)
            pyodbc_free(newtypes);
        if (newfuncs)
            pyodbc_free(newfuncs);
        PyErr_NoMemory();
        return false;
    }

    newtypes[0] = sqltype;
    newfuncs[0] = func;
    Py_INCREF(func);

    cnxn->conv_count = newcount;
    cnxn->conv_types = newtypes;
    cnxn->conv_funcs = newfuncs;

    if (oldcount != 0)
    {
        // copy old items
        memcpy(&newtypes[1], oldtypes, sizeof(SQLSMALLINT) * oldcount);
        memcpy(&newfuncs[1], oldfuncs, sizeof(PyObject*) * oldcount);

        pyodbc_free(oldtypes);
        pyodbc_free(oldfuncs);
    }

    return true;
}

static char conv_add_doc[] =
    "add_output_converter(sqltype, func) --> None\n"
    "\n"
    "Register an output converter function that will be called whenever a value with\n"
    "the given SQL type is read from the database.\n"
    "\n"
    "sqltype\n"
    "  The integer SQL type value to convert, which can be one of the defined\n"
    "  standard constants (e.g. pyodbc.SQL_VARCHAR) or a database-specific value\n"
    "  (e.g. -151 for the SQL Server 2008 geometry data type).\n"
    "\n"
    "func\n"
    "  The converter function which will be called with a single parameter, the\n"
    "  value, and should return the converted value.  If the value is NULL, the\n"
    "  parameter will be None.  Otherwise it will be a "
#if PY_MAJOR_VERSION >= 3
    "bytes object.\n"
#else
    "str object with the raw bytes.\n"
#endif
    "\n"
    "If func is None, any existing converter is removed."
    ;

static PyObject* Connection_conv_add(PyObject* self, PyObject* args)
{
    int sqltype;
    PyObject* func;
    if (!PyArg_ParseTuple(args, "iO", &sqltype, &func))
        return 0;

    if (func != Py_None)
    {
        if (!_add_converter(self, (SQLSMALLINT)sqltype, func))
            return 0;
    }
    else
    {
        if (!_remove_converter(self, (SQLSMALLINT)sqltype))
            return 0;
    }

    Py_RETURN_NONE;
}

static char conv_remove_doc[] =
    "remove_output_converter(sqltype) --> None\n"
    "\n"
    "Remove an output converter function that was registered with\n"
    "add_output_converter.  It is safe to call if no converter is\n"
    "registered for the type.\n"
    "\n"
    "sqltype\n"
    "  The integer SQL type value being converted, which can be one of the defined\n"
    "  standard constants (e.g. pyodbc.SQL_VARCHAR) or a database-specific value\n"
    "  (e.g. -151 for the SQL Server 2008 geometry data type).\n"
    ;

static PyObject* Connection_conv_remove(PyObject* self, PyObject* args)
{
    int sqltype;
    if (!PyArg_ParseTuple(args, "i", &sqltype))
        return 0;

    if (!_remove_converter(self, (SQLSMALLINT)sqltype))
        return 0;

    Py_RETURN_NONE;
}

static char conv_get_doc[] =
    "get_output_converter(sqltype) --> <class 'function'>\n"
    "\n"
    "Get the output converter function that was registered with\n"
    "add_output_converter.  It is safe to call if no converter is\n"
    "registered for the type (returns None).\n"
    "\n"
    "sqltype\n"
    "  The integer SQL type value being converted, which can be one of the defined\n"
    "  standard constants (e.g. pyodbc.SQL_VARCHAR) or a database-specific value\n"
    "  (e.g. -151 for the SQL Server 2008 geometry data type).\n"
    ;

static PyObject* _get_converter(PyObject* self, SQLSMALLINT sqltype)
{
    Connection* cnxn = (Connection*)self;

    if (cnxn->conv_count)
    {
        for (int i = 0; i < cnxn->conv_count; i++)
        {
            if (cnxn->conv_types[i] == sqltype)
            {
                return cnxn->conv_funcs[i];
            }
        }
    }
    Py_RETURN_NONE;
}

static PyObject* Connection_conv_get(PyObject* self, PyObject* args)
{
    int sqltype;
    if (!PyArg_ParseTuple(args, "i", &sqltype))
        return 0;

    return _get_converter(self, (SQLSMALLINT)sqltype);
}

static void NormalizeCodecName(const char* src, char* dest, size_t cbDest)
{
    // Copies the codec name to dest, lowercasing it and replacing underscores with dashes.
    // (Same as _Py_normalize_encoding which is not public.)  It also wraps the value with
    // pipes so we can search with it.
    //
    // UTF_8 --> |utf-8|
    //
    // This is an internal function - it will truncate so you should use a buffer bigger than
    // anything you expect to search for.

    char* pch = &dest[0];
    char* pchLast = pch + cbDest - 2; // -2 -> leave room for pipe and null

    *pch++ = '|';

    while (*src && pch < pchLast)
    {
        if (isupper(*src))
        {
            *pch++ = (char)tolower(*src++);
        }
        else if (*src == '_')
        {
            *pch++ = '-';
            src++;
        }
        else
        {
            *pch++ = *src++;
        }
    }

    *pch++ = '|';
    *pch = '\0';
}

static bool SetTextEncCommon(TextEnc& enc, const char* encoding, int ctype, bool allow_raw)
{
    // Code common to setencoding and setdecoding.

    if (!encoding)
    {
        PyErr_Format(PyExc_ValueError, "encoding is required");
        return false;
    }

    // Normalize the names so we don't have to worry about case or dashes vs underscores.
    // We'll lowercase everything and convert underscores to dashes.  The results are then
    // surrounded with pipes so we can search strings.  (See the `strstr` calls below.)
    char lower[30];
    NormalizeCodecName(encoding, lower, sizeof(lower));

#if PY_MAJOR_VERSION < 3
    if (strcmp(lower, "|raw|") == 0)
    {
        if (!allow_raw)
        {
            // Give a better error message for 'raw' than "not a registered codec".  It is never
            // registered.
            PyErr_Format(PyExc_ValueError, "Raw codec is only allowed for str / SQL_CHAR");
            return false;
        }
    }
    else if (!PyCodec_KnownEncoding(encoding))
    {
        PyErr_Format(PyExc_ValueError, "not a registered codec: '%s'", encoding);
        return false;
    }
#else
    if (!PyCodec_KnownEncoding(encoding))
    {
        PyErr_Format(PyExc_ValueError, "not a registered codec: '%s'", encoding);
        return false;
    }
#endif

    if (ctype != 0 && ctype != SQL_WCHAR && ctype != SQL_CHAR)
    {
        PyErr_Format(PyExc_ValueError, "Invalid ctype %d.  Must be SQL_CHAR or SQL_WCHAR", ctype);
        return false;
    }

    char* cpy = _strdup(encoding);
    if (!cpy)
    {
        PyErr_NoMemory();
        return false;
    }

    free((void*)enc.name);
    enc.name = cpy;

    if (strstr("|utf-8|utf8|", lower))
    {
        enc.optenc = OPTENC_UTF8;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_CHAR);
    }
    else if (strstr("|utf-16|utf16|", lower))
    {
        enc.optenc = OPTENC_UTF16;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_WCHAR);
    }
    else if (strstr("|utf-16-be|utf-16be|utf16be|", lower))
    {
        enc.optenc = OPTENC_UTF16BE;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_WCHAR);
    }
    else if (strstr("|utf-16-le|utf-16le|utf16le|", lower))
    {
        enc.optenc = OPTENC_UTF16LE;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_WCHAR);
    }
    else if (strstr("|utf-32|utf32|", lower))
    {
        enc.optenc = OPTENC_UTF32;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_WCHAR);
    }
    else if (strstr("|utf-32-be|utf-32be|utf32be|", lower))
    {
        enc.optenc = OPTENC_UTF32BE;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_WCHAR);
    }
    else if (strstr("|utf-32-le|utf-32le|utf32le|", lower))
    {
        enc.optenc = OPTENC_UTF32LE;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_WCHAR);
    }
    else if (strstr("|latin-1|latin1|iso-8859-1|iso8859-1|", lower))
    {
        enc.optenc = OPTENC_LATIN1;
        enc.ctype  = (SQLSMALLINT)(ctype ? ctype : SQL_C_CHAR);
    }
#if PY_MAJOR_VERSION < 3
    else if (strstr("|raw|", lower))
    {
        enc.optenc = OPTENC_RAW;
        enc.ctype  = SQL_C_CHAR;
    }
#endif
    else
    {
        enc.optenc = OPTENC_NONE;
        enc.ctype  = SQL_C_CHAR;
    }

    return true;
}

static PyObject* Connection_setencoding(PyObject* self, PyObject* args, PyObject* kwargs)
{
    Connection* cnxn = (Connection*)self;

#if PY_MAJOR_VERSION >= 3
    // In Python 3 we only support encodings for Unicode text.
    char* encoding = 0;
    int ctype = 0;
    static char *kwlist[] = { "encoding", "ctype", 0 };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|si", kwlist, &encoding, &ctype))
        return 0;
    TextEnc& enc = cnxn->unicode_enc;
    bool allow_raw = false;
#else
    // In Python 2, we support encodings for Unicode and strings.
    PyObject* from_type;
    char* encoding = 0;
    int ctype = 0;
    static char *kwlist[] = { "fromtype", "encoding", "ctype", 0 };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|si", kwlist, &from_type, &encoding, &ctype))
        return 0;

    if (!IsUnicodeType(from_type) && ! IsStringType(from_type))
        return PyErr_Format(PyExc_TypeError, "fromtype must be str or unicode");

    TextEnc& enc = IsStringType(from_type) ? cnxn->str_enc : cnxn->unicode_enc;
    bool allow_raw = IsStringType(from_type);
#endif

    if (!SetTextEncCommon(enc, encoding, ctype, allow_raw))
        return 0;

    Py_RETURN_NONE;
}

static char setdecoding_doc[] =
#if PY_MAJOR_VERSION >= 3
    "setdecoding(sqltype, encoding=None, ctype=None) --> None\n"
#else
    "setdecoding(sqltype, encoding=None, ctype=None, to=None) --> None\n"
#endif
    "\n"
    "Configures how text of type `ctype` (SQL_CHAR or SQL_WCHAR) is decoded\n"
    "when read from the database.\n"
    "\n"
    "When reading, the database will assign one of the sqltypes to text columns.\n"
    "pyodbc uses this lookup the decoding information set by this function.\n"
    "sqltype: pyodbc.SQL_CHAR or pyodbc.SQL_WCHAR\n\n"
    "encoding: A registered Python encoding such as \"utf-8\".\n\n"
#if PY_MAJOR_VERSION < 3
    "to: the desired Python object type - str or unicode"
#endif
    "ctype: The C data type should be requested.  Set this to SQL_CHAR for\n"
    "  single-byte encodings like UTF-8 and to SQL_WCHAR for two-byte encodings\n"
    "  like UTF-16.";


static PyObject* Connection_setdecoding(PyObject* self, PyObject* args, PyObject* kwargs)
{
    Connection* cnxn = (Connection*)self;

    int sqltype;
    char* encoding = 0;
    int ctype = 0;
    bool allow_raw = false;

#if PY_MAJOR_VERSION >= 3
    static char *kwlist[] = {"sqltype", "encoding", "ctype", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|si", kwlist, &sqltype, &encoding, &ctype))
        return 0;
#else
    int to = 0;
    PyObject* toObj = 0;
    static char *kwlist[] = {"sqltype", "encoding", "ctype", "to", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|siO", kwlist, &sqltype, &encoding, &ctype, &toObj))
        return 0;

    if (toObj)
    {
        if (IsUnicodeType(toObj))
            to = TO_UNICODE;
        else if (IsStringType(toObj))
            to = TO_STR;
        else
            return PyErr_Format(PyExc_ValueError, "`to` can only be unicode or str");
    }

    allow_raw = (sqltype == SQL_CHAR && to != TO_UNICODE);
#endif

    if (sqltype != SQL_WCHAR && sqltype != SQL_CHAR && sqltype != SQL_WMETADATA)
        return PyErr_Format(PyExc_ValueError, "Invalid sqltype %d.  Must be SQL_CHAR or SQL_WCHAR or SQL_WMETADATA", sqltype);

    TextEnc& enc = (sqltype == SQL_CHAR) ? cnxn->sqlchar_enc :
        ((sqltype == SQL_WMETADATA) ? cnxn->metadata_enc : cnxn->sqlwchar_enc);

    if (!SetTextEncCommon(enc, encoding, ctype, allow_raw))
        return 0;

#if PY_MAJOR_VERSION < 3
    if (!to && enc.optenc == OPTENC_RAW)
        enc.to = TO_STR;
    else
        enc.to = to ? to : TO_UNICODE;
#endif

    Py_RETURN_NONE;
}



static char enter_doc[] = "__enter__() -> self.";
static PyObject* Connection_enter(PyObject* self, PyObject* args)
{
    UNUSED(args);
    Py_INCREF(self);
    return self;
}

static char exit_doc[] = "__exit__(*excinfo) -> None.  Commits the connection if necessary.";
static PyObject* Connection_exit(PyObject* self, PyObject* args)
{
    Connection* cnxn = (Connection*)self;

    // If an error has occurred, `args` will be a tuple of 3 values.  Otherwise it will be a tuple of 3 `None`s.
    I(PyTuple_Check(args));

    if (cnxn->nAutoCommit == SQL_AUTOCOMMIT_OFF)
    {
        SQLSMALLINT CompletionType = (PyTuple_GetItem(args, 0) == Py_None) ? SQL_COMMIT : SQL_ROLLBACK;
        SQLRETURN ret;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLEndTran(SQL_HANDLE_DBC, cnxn->hdbc, CompletionType);
        Py_END_ALLOW_THREADS

        if (!SQL_SUCCEEDED(ret))
        {
            const char* szFunc = (CompletionType == SQL_COMMIT) ? "SQLEndTran(SQL_COMMIT)" : "SQLEndTran(SQL_ROLLBACK)";
            return RaiseErrorFromHandle(cnxn, szFunc, cnxn->hdbc, SQL_NULL_HANDLE);
        }
    }

    Py_RETURN_NONE;
}


static struct PyMethodDef Connection_methods[] =
{
    { "cursor",                  Connection_cursor,          METH_NOARGS,  cursor_doc     },
    { "close",                   Connection_close,           METH_NOARGS,  close_doc      },
    { "execute",                 Connection_execute,         METH_VARARGS, execute_doc    },
    { "commit",                  Connection_commit,          METH_NOARGS,  commit_doc     },
    { "rollback",                Connection_rollback,        METH_NOARGS,  rollback_doc   },
    { "getinfo",                 Connection_getinfo,         METH_VARARGS, getinfo_doc    },
    { "add_output_converter",    Connection_conv_add,        METH_VARARGS, conv_add_doc   },
    { "remove_output_converter", Connection_conv_remove,     METH_VARARGS, conv_remove_doc },
    { "get_output_converter",    Connection_conv_get,        METH_VARARGS, conv_get_doc },
    { "clear_output_converters", Connection_conv_clear,      METH_NOARGS,  conv_clear_doc },
    { "setdecoding",             (PyCFunction)Connection_setdecoding,     METH_VARARGS|METH_KEYWORDS, setdecoding_doc },
    { "setencoding",             (PyCFunction)Connection_setencoding,     METH_VARARGS|METH_KEYWORDS, 0 },
    { "set_attr",                Connection_set_attr,        METH_VARARGS, set_attr_doc   },
    { "__enter__",               Connection_enter,           METH_NOARGS,  enter_doc      },
    { "__exit__",                Connection_exit,            METH_VARARGS, exit_doc       },

    { 0, 0, 0, 0 }
};

static PyGetSetDef Connection_getseters[] = {
    { "closed", (getter)Connection_getclosed, 0,
      "Returns True if the connection is closed; False otherwise.", 0},
    { "searchescape", (getter)Connection_getsearchescape, 0,
        "The ODBC search pattern escape character, as returned by\n"
        "SQLGetInfo(SQL_SEARCH_PATTERN_ESCAPE).  These are driver specific.", 0 },
    { "autocommit", Connection_getautocommit, Connection_setautocommit,
      "Returns True if the connection is in autocommit mode; False otherwise.", 0 },
    { "timeout", Connection_gettimeout, Connection_settimeout,
      "The timeout in seconds, zero means no timeout.", 0 },
    { "maxwrite", Connection_getmaxwrite, Connection_setmaxwrite, "The maximum bytes to write before using SQLPutData.", 0 },
    { 0 }
};

PyTypeObject ConnectionType =
{
    PyVarObject_HEAD_INIT(0, 0)
    "pyodbc.Connection",        // tp_name
    sizeof(Connection),         // tp_basicsize
    0,                          // tp_itemsize
    Connection_dealloc,         // destructor tp_dealloc
    0,                          // tp_print
    0,                          // tp_getattr
    0,                          // tp_setattr
    0,                          // tp_compare
    0,                          // tp_repr
    0,                          // tp_as_number
    0,                          // tp_as_sequence
    0,                          // tp_as_mapping
    0,                          // tp_hash
    0,                          // tp_call
    0,                          // tp_str
    0,                          // tp_getattro
    0,                          // tp_setattro
    0,                          // tp_as_buffer
    Py_TPFLAGS_DEFAULT,         // tp_flags
    connection_doc,             // tp_doc
    0,                          // tp_traverse
    0,                          // tp_clear
    0,                          // tp_richcompare
    0,                          // tp_weaklistoffset
    0,                          // tp_iter
    0,                          // tp_iternext
    Connection_methods,         // tp_methods
    0,                          // tp_members
    Connection_getseters,       // tp_getset
    0,                          // tp_base
    0,                          // tp_dict
    0,                          // tp_descr_get
    0,                          // tp_descr_set
    0,                          // tp_dictoffset
    0,                          // tp_init
    0,                          // tp_alloc
    0,                          // tp_new
    0,                          // tp_free
    0,                          // tp_is_gc
    0,                          // tp_bases
    0,                          // tp_mro
    0,                          // tp_cache
    0,                          // tp_subclasses
    0,                          // tp_weaklist
};
