// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// Note: This project has gone from C++ (when it was ported from pypgdb) to C, back to C++ (where it will stay).  If
// you are making modifications, feel free to move variable declarations from the top of functions to where they are
// actually used.

#include "pyodbc.h"
#include "wrapper.h"
#include "textenc.h"
#include "cursor.h"
#include "pyodbcmodule.h"
#include "connection.h"
#include "row.h"
#include "params.h"
#include "errors.h"
#include "getdata.h"
#include "dbspecific.h"
#include <datetime.h>

enum
{
    CURSOR_REQUIRE_CNXN    = 0x00000001,
    CURSOR_REQUIRE_OPEN    = 0x00000003, // includes _CNXN
    CURSOR_REQUIRE_RESULTS = 0x00000007, // includes _OPEN
    CURSOR_RAISE_ERROR     = 0x00000010,
};

inline bool StatementIsValid(Cursor* cursor)
{
    return cursor->cnxn != 0 && ((Connection*)cursor->cnxn)->hdbc != SQL_NULL_HANDLE && cursor->hstmt != SQL_NULL_HANDLE;
}

extern PyTypeObject CursorType;

inline bool Cursor_Check(PyObject* o)
{
    return o != 0 && Py_TYPE(o) == &CursorType;
}

static Cursor* Cursor_Validate(PyObject* obj, DWORD flags)
{
    //  Validates that a PyObject is a Cursor (like Cursor_Check) and optionally some other requirements controlled by
    //  `flags`.  If valid and all requirements (from the flags) are met, the cursor is returned, cast to Cursor*.
    //  Otherwise zero is returned.
    //
    //  Designed to be used at the top of methods to convert the PyObject pointer and perform necessary checks.
    //
    //  Valid flags are from the CURSOR_ enum above.  Note that unless CURSOR_RAISE_ERROR is supplied, an exception
    //  will not be set.  (When deallocating, we really don't want an exception.)

    Connection* cnxn   = 0;
    Cursor*     cursor = 0;

    if (!Cursor_Check(obj))
    {
        if (flags & CURSOR_RAISE_ERROR)
            PyErr_SetString(ProgrammingError, "Invalid cursor object.");
        return 0;
    }

    cursor = (Cursor*)obj;
    cnxn   = (Connection*)cursor->cnxn;

    if (cnxn == 0)
    {
        if (flags & CURSOR_RAISE_ERROR)
            PyErr_SetString(ProgrammingError, "Attempt to use a closed cursor.");
        return 0;
    }

    if (IsSet(flags, CURSOR_REQUIRE_OPEN))
    {
        if (cursor->hstmt == SQL_NULL_HANDLE)
        {
            if (flags & CURSOR_RAISE_ERROR)
                PyErr_SetString(ProgrammingError, "Attempt to use a closed cursor.");
            return 0;
        }

        if (cnxn->hdbc == SQL_NULL_HANDLE)
        {
            if (flags & CURSOR_RAISE_ERROR)
                PyErr_SetString(ProgrammingError, "The cursor's connection has been closed.");
            return 0;
        }
    }

    if (IsSet(flags, CURSOR_REQUIRE_RESULTS) && cursor->colinfos == 0)
    {
        if (flags & CURSOR_RAISE_ERROR)
            PyErr_SetString(ProgrammingError, "No results.  Previous SQL was not a query.");
        return 0;
    }

    return cursor;
}


inline bool IsNumericType(SQLSMALLINT sqltype)
{
    switch (sqltype)
    {
    case SQL_DECIMAL:
    case SQL_NUMERIC:
    case SQL_REAL:
    case SQL_FLOAT:
    case SQL_DOUBLE:
    case SQL_SMALLINT:
    case SQL_INTEGER:
    case SQL_TINYINT:
    case SQL_BIGINT:
        return true;
    }

    return false;
}


static bool create_name_map(Cursor* cur, SQLSMALLINT field_count, bool lower)
{
    // Called after an execute to construct the map shared by rows.

    bool success = false;
    PyObject *desc = 0, *colmap = 0, *colinfo = 0, *type = 0, *index = 0, *nullable_obj=0;
    SQLSMALLINT nameLen = 300;
    uint16_t *szName = NULL;
    SQLRETURN ret;

    assert(cur->hstmt != SQL_NULL_HANDLE && cur->colinfos != 0);

    // These are the values we expect after free_results.  If this function fails, we do not modify any members, so
    // they should be set to something Cursor_close can deal with.
    assert(cur->description == Py_None);
    assert(cur->map_name_to_index == 0);

    if (cur->cnxn->hdbc == SQL_NULL_HANDLE)
    {
        RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
        return false;
    }

    desc   = PyTuple_New((Py_ssize_t)field_count);
    colmap = PyDict_New();
    szName = (uint16_t*) PyMem_Malloc((nameLen + 1) * sizeof(uint16_t));
    if (!desc || !colmap || !szName)
        goto done;

    for (int i = 0; i < field_count; i++)
    {
        SQLSMALLINT cchName;
        SQLSMALLINT nDataType;
        SQLULEN nColSize;           // precision
        SQLSMALLINT cDecimalDigits; // scale
        SQLSMALLINT nullable;

        retry:
        Py_BEGIN_ALLOW_THREADS
        ret = SQLDescribeColW(cur->hstmt, (SQLUSMALLINT)(i + 1), (SQLWCHAR*)szName, nameLen, &cchName, &nDataType, &nColSize, &cDecimalDigits, &nullable);
        Py_END_ALLOW_THREADS

        if (cur->cnxn->hdbc == SQL_NULL_HANDLE)
        {
            // The connection was closed by another thread in the ALLOW_THREADS block above.
            RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
            goto done;
        }

        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cur->cnxn, "SQLDescribeCol", cur->cnxn->hdbc, cur->hstmt);
            goto done;
        }

        // If needed, allocate a bigger column name message buffer and retry.
        if (cchName > nameLen - 1) {
            nameLen = cchName + 1;
            if (!PyMem_Realloc((BYTE**) &szName, (nameLen + 1) * sizeof(uint16_t))) {
                PyErr_NoMemory();
                goto done;
            }
            goto retry;
        }

        const TextEnc& enc = cur->cnxn->metadata_enc;

        // HACK: I don't know the exact issue, but iODBC + Teradata results in either UCS4 data
        // or 4-byte SQLWCHAR.  I'm going to use UTF-32 as an indication that's what we have.

        Py_ssize_t cbName = cchName;
        switch (enc.optenc)
        {
        case OPTENC_UTF32:
        case OPTENC_UTF32LE:
        case OPTENC_UTF32BE:
            cbName *= 4;
            break;
        default:
            if (enc.ctype == SQL_C_WCHAR)
                cbName *= 2;
            break;
        }

        TRACE("Col %d: type=%s (%d) colsize=%d\n", (i+1), SqlTypeName(nDataType), (int)nDataType, (int)nColSize);

        Object name(TextBufferToObject(enc, (byte*)szName, cbName));

        if (!name)
            goto done;

        if (lower)
        {
            PyObject* l = PyObject_CallMethod(name, "lower", 0);
            if (!l)
                goto done;
            name.Attach(l);
        }

        type = PythonTypeFromSqlType(cur, nDataType);
        if (!type)
            goto done;

        switch (nullable)
        {
        case SQL_NO_NULLS:
            nullable_obj = Py_False;
            break;
        case SQL_NULLABLE:
            nullable_obj = Py_True;
            break;
        case SQL_NULLABLE_UNKNOWN:
        default:
            nullable_obj = Py_None;
            break;
        }

        // The Oracle ODBC driver has a bug (I call it) that it returns a data size of 0 when a numeric value is
        // retrieved from a UNION: http://support.microsoft.com/?scid=kb%3Ben-us%3B236786&x=13&y=6
        //
        // Unfortunately, I don't have a test system for this yet, so I'm *trying* something.  (Not a good sign.)  If
        // the size is zero and it appears to be a numeric type, we'll try to come up with our own length using any
        // other data we can get.

        if (nColSize == 0 && IsNumericType(nDataType))
        {
            // I'm not sure how
            if (cDecimalDigits != 0)
            {
                nColSize = (SQLUINTEGER)(cDecimalDigits + 3);
            }
            else
            {
                // I'm not sure if this is a good idea, but ...
                nColSize = 42;
            }
        }

        colinfo = Py_BuildValue("(OOOiiiO)",
                                name.Get(),
                                type,                // type_code
                                Py_None,             // display size
                                (int)nColSize,       // internal_size
                                (int)nColSize,       // precision
                                (int)cDecimalDigits, // scale
                                nullable_obj);       // null_ok
        if (!colinfo)
            goto done;

        nullable_obj = 0;

        index = PyLong_FromLong(i);
        if (!index)
            goto done;

        PyDict_SetItem(colmap, name.Get(), index);
        Py_DECREF(index);       // SetItemString increments
        index = 0;

        PyTuple_SET_ITEM(desc, i, colinfo);
        colinfo = 0;            // reference stolen by SET_ITEM
    }

    Py_XDECREF(cur->description);
    cur->description = desc;
    desc = 0;
    cur->map_name_to_index = colmap;
    colmap = 0;

    success = true;

  done:
    Py_XDECREF(nullable_obj);
    Py_XDECREF(desc);
    Py_XDECREF(colmap);
    Py_XDECREF(index);
    Py_XDECREF(colinfo);
    PyMem_Free(szName);

    return success;
}


enum free_results_flags
{
    FREE_STATEMENT = 0x01,
    KEEP_STATEMENT = 0x02,
    FREE_PREPARED  = 0x04,
    KEEP_PREPARED  = 0x08,
    KEEP_MESSAGES  = 0x10,

    STATEMENT_MASK = 0x03,
    PREPARED_MASK  = 0x0C
};

static bool free_results(Cursor* self, int flags)
{
    // Internal function called any time we need to free the memory associated with query results.  It is safe to call
    // this even when a query has not been executed.

    // If we ran out of memory, it is possible that we have a cursor but colinfos is zero.  However, we should be
    // deleting this object, so the cursor will be freed when the HSTMT is destroyed. */

    assert((flags & STATEMENT_MASK) != 0);
    assert((flags & PREPARED_MASK) != 0);

    if ((flags & PREPARED_MASK) == FREE_PREPARED)
    {
        Py_XDECREF(self->pPreparedSQL);
        self->pPreparedSQL = 0;
    }

    if (self->colinfos)
    {
        PyMem_Free(self->colinfos);
        self->colinfos = 0;
    }

    if (StatementIsValid(self))
    {
        if ((flags & STATEMENT_MASK) == FREE_STATEMENT)
        {
            Py_BEGIN_ALLOW_THREADS
            SQLFreeStmt(self->hstmt, SQL_CLOSE);
            Py_END_ALLOW_THREADS;
        }
        else
        {
            Py_BEGIN_ALLOW_THREADS
            SQLFreeStmt(self->hstmt, SQL_UNBIND);
            SQLFreeStmt(self->hstmt, SQL_RESET_PARAMS);
            Py_END_ALLOW_THREADS;
        }

        if (self->cnxn->hdbc == SQL_NULL_HANDLE)
        {
            // The connection was closed by another thread in the ALLOW_THREADS block above.
            RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
            return false;
        }
    }

    if (self->description != Py_None)
    {
        Py_DECREF(self->description);
        self->description = Py_None;
        Py_INCREF(Py_None);
    }

    if (self->map_name_to_index)
    {
        Py_DECREF(self->map_name_to_index);
        self->map_name_to_index = 0;
    }

    if ((flags & KEEP_MESSAGES) == 0)
    {
        Py_XDECREF(self->messages);
        self->messages = PyList_New(0);
    }

    self->rowcount = -1;

    return true;
}


static void closeimpl(Cursor* cur)
{
    // An internal function for the shared 'closing' code used by Cursor_close and Cursor_dealloc.
    //
    // This method releases the GIL lock while closing, so verify the HDBC still exists if you use it.

    free_results(cur, FREE_STATEMENT | FREE_PREPARED);

    FreeParameterData(cur);
    FreeParameterInfo(cur);

    if (StatementIsValid(cur))
    {
        HSTMT hstmt = cur->hstmt;
        cur->hstmt = SQL_NULL_HANDLE;

        SQLRETURN ret;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        Py_END_ALLOW_THREADS

        // If there is already an exception, don't overwrite it.
        if (!SQL_SUCCEEDED(ret) && !PyErr_Occurred())
            RaiseErrorFromHandle(cur->cnxn, "SQLFreeHandle", cur->cnxn->hdbc, SQL_NULL_HANDLE);
    }

    Py_XDECREF(cur->pPreparedSQL);
    Py_XDECREF(cur->description);
    Py_XDECREF(cur->map_name_to_index);
    Py_XDECREF(cur->cnxn);
    Py_XDECREF(cur->messages);

    cur->pPreparedSQL = 0;
    cur->description = 0;
    cur->map_name_to_index = 0;
    cur->cnxn = 0;
    cur->messages = 0;
}

static char close_doc[] =
    "Close the cursor now (rather than whenever __del__ is called).  The cursor will\n"
    "be unusable from this point forward; a ProgrammingError exception will be\n"
    "raised if any operation is attempted with the cursor.";

static PyObject* Cursor_close(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    closeimpl(cursor);

    if (PyErr_Occurred())
        return 0;

    Py_INCREF(Py_None);
    return Py_None;
}

static void Cursor_dealloc(Cursor* cursor)
{
    if (Cursor_Validate((PyObject*)cursor, CURSOR_REQUIRE_CNXN))
    {
        closeimpl(cursor);
    }
    Py_XDECREF(cursor->inputsizes);
    PyObject_Del(cursor);
}


bool InitColumnInfo(Cursor* cursor, SQLUSMALLINT iCol, ColumnInfo* pinfo)
{
    // Initializes ColumnInfo from result set metadata.

    SQLRETURN ret;

    // REVIEW: This line fails on OS/X with the FileMaker driver : http://www.filemaker.com/support/updaters/xdbc_odbc_mac.html
    //
    // I suspect the problem is that it doesn't allow NULLs in some of the parameters, so I'm going to supply them all
    // to see what happens.

    SQLCHAR     ColumnName[200];
    SQLSMALLINT BufferLength  = _countof(ColumnName);
    SQLSMALLINT NameLength    = 0;
    SQLSMALLINT DataType      = 0;
    SQLULEN     ColumnSize    = 0;
    SQLSMALLINT DecimalDigits = 0;
    SQLSMALLINT Nullable      = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLDescribeCol(cursor->hstmt, iCol,
                         ColumnName,
                         BufferLength,
                         &NameLength,
                         &DataType,
                         &ColumnSize,
                         &DecimalDigits,
                         &Nullable);
    Py_END_ALLOW_THREADS

    pinfo->sql_type    = DataType;
    pinfo->column_size = ColumnSize;

    if (cursor->cnxn->hdbc == SQL_NULL_HANDLE)
    {
        // The connection was closed by another thread in the ALLOW_THREADS block above.
        RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
        return false;
    }
    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(cursor->cnxn, "SQLDescribeCol", cursor->cnxn->hdbc, cursor->hstmt);
        return false;
    }

    // If it is an integer type, determine if it is signed or unsigned.  The buffer size is the same but we'll need to
    // know when we convert to a Python integer.

    switch (pinfo->sql_type)
    {
    case SQL_TINYINT:
    case SQL_SMALLINT:
    case SQL_INTEGER:
    case SQL_BIGINT:
    {
        SQLLEN f;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLColAttribute(cursor->hstmt, iCol, SQL_DESC_UNSIGNED, 0, 0, 0, &f);
        Py_END_ALLOW_THREADS

        if (cursor->cnxn->hdbc == SQL_NULL_HANDLE)
        {
            // The connection was closed by another thread in the ALLOW_THREADS block above.
            RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
            return false;
        }

        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cursor->cnxn, "SQLColAttribute", cursor->cnxn->hdbc, cursor->hstmt);
            return false;
        }
        pinfo->is_unsigned = (f == SQL_TRUE);
        break;
    }

    default:
        pinfo->is_unsigned = false;
    }

    return true;
}


static bool PrepareResults(Cursor* cur, int cCols)
{
    // Called after a SELECT has been executed to perform pre-fetch work.
    //
    // Allocates the ColumnInfo structures describing the returned data.

    int i;
    assert(cur->colinfos == 0);

    cur->colinfos = (ColumnInfo*)PyMem_Malloc(sizeof(ColumnInfo) * cCols);
    if (cur->colinfos == 0)
    {
        PyErr_NoMemory();
        return false;
    }

    for (i = 0; i < cCols; i++)
    {
        if (!InitColumnInfo(cur, (SQLUSMALLINT)(i + 1), &cur->colinfos[i]))
        {
            PyMem_Free(cur->colinfos);
            cur->colinfos = 0;
            return false;
        }
    }

    return true;
}


int GetDiagRecs(Cursor* cur)
{
    // Retrieves all diagnostic records from the cursor and assigns them to the "messages" attribute.

    PyObject* msg_list;  // the "messages" as a Python list of diagnostic records

    SQLSMALLINT iRecNumber = 1;  // the index of the diagnostic records (1-based)
    uint16_t    cSQLState[6];  // five-character SQLSTATE code (plus terminating NULL)
    SQLINTEGER  iNativeError;
    SQLSMALLINT iMessageLen = 1023;
    uint16_t    *cMessageText = (uint16_t*) PyMem_Malloc((iMessageLen + 1) * sizeof(uint16_t));
    SQLSMALLINT iTextLength;

    SQLRETURN ret;
    char sqlstate_ascii[6] = "";  // ASCII version of the SQLState

    if (!cMessageText) {
      PyErr_NoMemory();
      return 0;
    }

    msg_list = PyList_New(0);
    if (!msg_list)
        return 0;

    for (;;)
    {
        cSQLState[0]    = 0;
        iNativeError    = 0;
        cMessageText[0] = 0;
        iTextLength     = 0;

        Py_BEGIN_ALLOW_THREADS
        ret = SQLGetDiagRecW(
            SQL_HANDLE_STMT, cur->hstmt, iRecNumber, (SQLWCHAR*)cSQLState, &iNativeError,
            (SQLWCHAR*)cMessageText, iMessageLen, &iTextLength
        );
        Py_END_ALLOW_THREADS
        if (!SQL_SUCCEEDED(ret))
            break;

        // If needed, allocate a bigger error message buffer and retry.
        if (iTextLength > iMessageLen - 1) {
            iMessageLen = iTextLength + 1;
            if (!PyMem_Realloc((BYTE**) &cMessageText, (iMessageLen + 1) * sizeof(uint16_t))) {
                PyMem_Free(cMessageText);
                PyErr_NoMemory();
                return 0;
            }
            Py_BEGIN_ALLOW_THREADS
            ret = SQLGetDiagRecW(
                SQL_HANDLE_STMT, cur->hstmt, iRecNumber, (SQLWCHAR*)cSQLState, &iNativeError,
                (SQLWCHAR*)cMessageText, iMessageLen, &iTextLength
            );
            Py_END_ALLOW_THREADS
            if (!SQL_SUCCEEDED(ret))
                break;
        }

        cSQLState[5] = 0;  // Not always NULL terminated (MS Access)
        CopySqlState(cSQLState, sqlstate_ascii);
        PyObject* msg_class = PyUnicode_FromFormat("[%s] (%ld)", sqlstate_ascii, (long)iNativeError);

        // Default to UTF-16, which may not work if the driver/manager is using some other encoding
        const char *unicode_enc = cur->cnxn ? cur->cnxn->metadata_enc.name : ENCSTR_UTF16NE;
        PyObject* msg_value = PyUnicode_Decode(
            (char*)cMessageText, iTextLength * sizeof(uint16_t), unicode_enc, "strict"
        );
        if (!msg_value)
        {
            // If the char cannot be decoded, return something rather than nothing.
            Py_XDECREF(msg_value);
            msg_value = PyBytes_FromStringAndSize((char*)cMessageText, iTextLength * sizeof(uint16_t));
        }

        PyObject* msg_tuple = PyTuple_New(2);  // the message as a Python tuple of class and value

        if (msg_class && msg_value && msg_tuple)
        {
            PyTuple_SetItem(msg_tuple, 0, msg_class);  // msg_tuple now owns the msg_class reference
            PyTuple_SetItem(msg_tuple, 1, msg_value);  // msg_tuple now owns the msg_value reference

            PyList_Append(msg_list, msg_tuple);
            Py_XDECREF(msg_tuple);  // whether PyList_Append succeeds or not
        }
        else
        {
            Py_XDECREF(msg_class);
            Py_XDECREF(msg_value);
            Py_XDECREF(msg_tuple);
        }

        iRecNumber++;
    }
    PyMem_Free(cMessageText);

    Py_XDECREF(cur->messages);
    cur->messages = msg_list;  // cur->messages now owns the msg_list reference

    return 0;
}


static PyObject* execute(Cursor* cur, PyObject* pSql, PyObject* params, bool skip_first)
{
    // Internal function to execute SQL, called by .execute and .executemany.
    //
    // pSql
    //   A PyString, PyUnicode, or derived object containing the SQL.
    //
    // params
    //   Pointer to an optional sequence of parameters, and possibly the SQL statement (see skip_first):
    //   (SQL, param1, param2) or (param1, param2).
    //
    // skip_first
    //   If true, the first element in `params` is ignored.  (It will be the SQL statement and `params` will be the
    //   entire tuple passed to Cursor.execute.)  Otherwise all of the params are used.  (This case occurs when called
    //   from Cursor.executemany, in which case the sequences do not contain the SQL statement.)  Ignored if params is
    //   zero.

    if (params)
    {
        if (!PyTuple_Check(params) && !PyList_Check(params) && !Row_Check(params))
            return RaiseErrorV(0, PyExc_TypeError, "Params must be in a list, tuple, or Row");
    }

    // Normalize the parameter variables.

    int        params_offset = skip_first ? 1 : 0;
    Py_ssize_t cParams       = params == 0 ? 0 : PySequence_Length(params) - params_offset;

    SQLRETURN ret = 0;

    free_results(cur, FREE_STATEMENT | KEEP_PREPARED);

    const char* szLastFunction = "";

    if (cParams > 0)
    {
        // There are parameters, so we'll need to prepare the SQL statement and bind the parameters.  (We need to
        // prepare the statement because we can't bind a NULL (None) object without knowing the target datatype.  There
        // is no one data type that always maps to the others (no, not even varchar)).

        if (!PrepareAndBind(cur, pSql, params, skip_first))
            return 0;

        szLastFunction = "SQLExecute";
        Py_BEGIN_ALLOW_THREADS
        ret = SQLExecute(cur->hstmt);
        Py_END_ALLOW_THREADS
    }
    else
    {
        // REVIEW: Why don't we always prepare?  It is highly unlikely that a user would need to execute the same SQL
        // repeatedly if it did not have parameters, so we are not losing performance, but it would simplify the code.

        Py_XDECREF(cur->pPreparedSQL);
        cur->pPreparedSQL = 0;

        szLastFunction = "SQLExecDirect";

        const TextEnc* penc = 0;
        penc = &cur->cnxn->unicode_enc;

        Object query(penc->Encode(pSql));
        if (!query)
            return 0;

        bool isWide = (penc->ctype == SQL_C_WCHAR);

        const char* pch = PyBytes_AS_STRING(query.Get());
        SQLINTEGER  cch = (SQLINTEGER)(PyBytes_GET_SIZE(query.Get()) / (isWide ? sizeof(uint16_t) : 1));

        Py_BEGIN_ALLOW_THREADS
        if (isWide)
            ret = SQLExecDirectW(cur->hstmt, (SQLWCHAR*)pch, cch);
        else
            ret = SQLExecDirect(cur->hstmt, (SQLCHAR*)pch, cch);
        Py_END_ALLOW_THREADS
    }

    if (cur->cnxn->hdbc == SQL_NULL_HANDLE)
    {
        // The connection was closed by another thread in the ALLOW_THREADS block above.

        FreeParameterData(cur);
        return RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
    }

    if (!SQL_SUCCEEDED(ret) && ret != SQL_NEED_DATA && ret != SQL_NO_DATA)
    {
        // We could try dropping through the while and if below, but if there is an error, we need to raise it before
        // FreeParameterData calls more ODBC functions.
        RaiseErrorFromHandle(cur->cnxn, "SQLExecDirectW", cur->cnxn->hdbc, cur->hstmt);
        FreeParameterData(cur);
        return 0;
    }

    if (ret == SQL_SUCCESS_WITH_INFO)
    {
        GetDiagRecs(cur);
    }

    while (ret == SQL_NEED_DATA)
    {
        // One or more parameters were too long to bind normally so we set the
        // length to SQL_LEN_DATA_AT_EXEC.  ODBC will return SQL_NEED_DATA for
        // each of the parameters we did this for.
        //
        // For each one we set a pointer to the ParamInfo as the "parameter
        // data" we can access with SQLParamData.  We've stashed everything we
        // need in there.

        szLastFunction = "SQLParamData";
        ParamInfo* pInfo;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLParamData(cur->hstmt, (SQLPOINTER*)&pInfo);
        Py_END_ALLOW_THREADS

        if (ret != SQL_NEED_DATA && ret != SQL_NO_DATA && !SQL_SUCCEEDED(ret))
            return RaiseErrorFromHandle(cur->cnxn, "SQLParamData", cur->cnxn->hdbc, cur->hstmt);

        TRACE("SQLParamData() --> %d\n", ret);

        if (ret == SQL_NEED_DATA)
        {
            szLastFunction = "SQLPutData";
            if (pInfo->pObject && (PyBytes_Check(pInfo->pObject) || PyByteArray_Check(pInfo->pObject)
            ))
            {
                char *(*pGetPtr)(PyObject*);
                Py_ssize_t (*pGetLen)(PyObject*);
                if (PyByteArray_Check(pInfo->pObject))
                {
                    pGetPtr = PyByteArray_AsString;
                    pGetLen = PyByteArray_Size;
                }
                else
                {
                    pGetPtr = PyBytes_AsString;
                    pGetLen = PyBytes_Size;
                }

                const char* p = pGetPtr(pInfo->pObject);
                SQLLEN cb = (SQLLEN)pGetLen(pInfo->pObject);
                SQLLEN offset = 0;

                do
                {
                    SQLLEN remaining = pInfo->maxlength ? min(pInfo->maxlength, cb - offset) : cb;
                    TRACE("SQLPutData [%d] (%d) %.10s\n", offset, remaining, &p[offset]);
                    Py_BEGIN_ALLOW_THREADS
                    ret = SQLPutData(cur->hstmt, (SQLPOINTER)&p[offset], remaining);
                    Py_END_ALLOW_THREADS
                    if (!SQL_SUCCEEDED(ret))
                        return RaiseErrorFromHandle(cur->cnxn, "SQLPutData", cur->cnxn->hdbc, cur->hstmt);
                    offset += remaining;
                }
                while (offset < cb);
            }
            else if (pInfo->ParameterType == SQL_SS_TABLE)
            {
                // TVP
                // Need to convert its columns into the bound row buffers
                int hasTvpRows = 0;
                if (pInfo->curTvpRow < PySequence_Length(pInfo->pObject))
                {
                    PyObject *tvpRow = PySequence_GetItem(pInfo->pObject, pInfo->curTvpRow);
                    Py_XDECREF(tvpRow);
                    for (Py_ssize_t i = 0; i < PySequence_Size(tvpRow); i++)
                    {
                        struct ParamInfo newParam;
                        struct ParamInfo *prevParam = pInfo->nested + i;
                        PyObject *cell = PySequence_GetItem(tvpRow, i);
                        Py_XDECREF(cell);
                        memset(&newParam, 0, sizeof(newParam));
                        if (!GetParameterInfo(cur, i, cell, newParam, true))
                        {
                            // Error converting object
                            FreeParameterData(cur);
                            return NULL;
                        }

                        if((newParam.ValueType != SQL_C_DEFAULT && prevParam->ValueType != SQL_C_DEFAULT) &&
                           (newParam.ValueType != prevParam->ValueType ||
                            newParam.ParameterType != prevParam->ParameterType))
                        {
                            FreeParameterData(cur);
                            return RaiseErrorV(0, ProgrammingError, "Type mismatch between TVP row values");
                        }

                        if (prevParam->allocated)
                            PyMem_Free(prevParam->ParameterValuePtr);
                        Py_XDECREF(prevParam->pObject);
                        newParam.BufferLength = newParam.StrLen_or_Ind;
                        newParam.StrLen_or_Ind = SQL_DATA_AT_EXEC;
                        *prevParam = newParam;
                        if(prevParam->ParameterValuePtr == &newParam.Data)
                        {
                            prevParam->ParameterValuePtr = &prevParam->Data;
                        }
                    }
                    pInfo->curTvpRow++;
                    hasTvpRows = 1;
                }
                Py_BEGIN_ALLOW_THREADS
                ret = SQLPutData(cur->hstmt, hasTvpRows ? (SQLPOINTER)1 : 0, hasTvpRows);
                Py_END_ALLOW_THREADS
                if (!SQL_SUCCEEDED(ret))
                    return RaiseErrorFromHandle(cur->cnxn, "SQLPutData", cur->cnxn->hdbc, cur->hstmt);
            }
            else
            {
                // TVP column sent as DAE
                Py_BEGIN_ALLOW_THREADS
                ret = SQLPutData(cur->hstmt, pInfo->ParameterValuePtr, pInfo->BufferLength);
                Py_END_ALLOW_THREADS
                if (!SQL_SUCCEEDED(ret))
                    return RaiseErrorFromHandle(cur->cnxn, "SQLPutData", cur->cnxn->hdbc, cur->hstmt);
            }
            ret = SQL_NEED_DATA;
        }
    }

    FreeParameterData(cur);

    if (ret == SQL_NO_DATA)
    {
        // Example: A delete statement that did not delete anything.
        cur->rowcount = 0;
        Py_INCREF(cur);
        return (PyObject*)cur;
    }

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, szLastFunction, cur->cnxn->hdbc, cur->hstmt);

    SQLLEN cRows = -1;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLRowCount(cur->hstmt, &cRows);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLRowCount", cur->cnxn->hdbc, cur->hstmt);

    cur->rowcount = (int)cRows;

    TRACE("SQLRowCount: %d\n", cRows);

    SQLSMALLINT cCols = 0;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
    {
        // Note: The SQL Server driver sometimes returns HY007 here if multiple statements (separated by ;) were
        // submitted.  This is not documented, but I've seen it with multiple successful inserts.

        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);
    }

    TRACE("SQLNumResultCols: %d\n", cCols);

    if (cur->cnxn->hdbc == SQL_NULL_HANDLE)
    {
        // The connection was closed by another thread in the ALLOW_THREADS block above.
        return RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
    }

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLRowCount", cur->cnxn->hdbc, cur->hstmt);

    if (cCols != 0)
    {
        // A result set was created.

        if (!PrepareResults(cur, cCols))
            return 0;

        if (!create_name_map(cur, cCols, lowercase()))
            return 0;
    }

    Py_INCREF(cur);
    return (PyObject*)cur;
}


inline bool IsSequence(PyObject* p)
{
    // Used to determine if the first parameter of execute is a collection of SQL parameters or is a SQL parameter
    // itself.  If the first parameter is a list, tuple, or Row object, then we consider it a collection.  Anything
    // else, including other sequences (e.g. bytearray), are considered SQL parameters.

    return PyList_Check(p) || PyTuple_Check(p) || Row_Check(p);
}


static char execute_doc[] =
    "C.execute(sql, [params]) --> Cursor\n"
    "\n"
    "Prepare and execute a database query or command.\n"
    "\n"
    "Parameters may be provided as a sequence (as specified by the DB API) or\n"
    "simply passed in one after another (non-standard):\n"
    "\n"
    "  cursor.execute(sql, (param1, param2))\n"
    "\n"
    "    or\n"
    "\n"
    "  cursor.execute(sql, param1, param2)\n";

PyObject* Cursor_execute(PyObject* self, PyObject* args)
{
    Py_ssize_t cParams = PyTuple_Size(args) - 1;

    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    if (cParams < 0)
    {
        PyErr_SetString(PyExc_TypeError, "execute() takes at least 1 argument (0 given)");
        return 0;
    }

    PyObject* pSql = PyTuple_GET_ITEM(args, 0);

    if (!PyUnicode_Check(pSql) && !PyUnicode_Check(pSql))
    {
        PyErr_SetString(PyExc_TypeError, "The first argument to execute must be a string or unicode query.");
        return 0;
    }

    // Figure out if there were parameters and how they were passed.  Our optional parameter passing complicates this slightly.

    bool skip_first = false;
    PyObject *params = 0;
    if (cParams == 1 && IsSequence(PyTuple_GET_ITEM(args, 1)))
    {
        // There is a single argument and it is a sequence, so we must treat it as a sequence of parameters.  (This is
        // the normal Cursor.execute behavior.)

        params     = PyTuple_GET_ITEM(args, 1);
        skip_first = false;
    }
    else if (cParams > 0)
    {
        params     = args;
        skip_first = true;
    }

    // Execute.

    return execute(cursor, pSql, params, skip_first);
}


static PyObject* Cursor_executemany(PyObject* self, PyObject* args)
{
    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    cursor->rowcount = -1;

    PyObject *pSql, *param_seq;
    if (!PyArg_ParseTuple(args, "OO", &pSql, &param_seq))
        return 0;

    if (!PyUnicode_Check(pSql) && !PyUnicode_Check(pSql))
    {
        PyErr_SetString(PyExc_TypeError, "The first argument to execute must be a string or unicode query.");
        return 0;
    }

    if (IsSequence(param_seq))
    {
        Py_ssize_t c = PySequence_Size(param_seq);

        if (c == 0)
        {
            PyErr_SetString(ProgrammingError, "The second parameter to executemany must not be empty.");
            return 0;
        }
        if (cursor->fastexecmany)
        {
            free_results(cursor, FREE_STATEMENT | KEEP_PREPARED);
            if (!ExecuteMulti(cursor, pSql, param_seq))
                return 0;
        }
        else
        {
            for (Py_ssize_t i = 0; i < c; i++)
            {
                PyObject* params = PySequence_GetItem(param_seq, i);
                PyObject* result = execute(cursor, pSql, params, false);
                bool success = result != 0;
                Py_XDECREF(result);
                Py_DECREF(params);
                if (!success)
                {
                    cursor->rowcount = -1;
                    return 0;
                }
            }
        }
    }
    else if (PyGen_Check(param_seq) || PyIter_Check(param_seq))
    {
        Object iter;

        if (PyGen_Check(param_seq))
        {
            iter = PyObject_GetIter(param_seq);
        }
        else
        {
            iter = param_seq;
            Py_INCREF(param_seq);
        }

        Object params;

        while (params.Attach(PyIter_Next(iter)))
        {
            PyObject* result = execute(cursor, pSql, params, false);
            bool success = result != 0;
            Py_XDECREF(result);

            if (!success)
            {
                cursor->rowcount = -1;
                return 0;
            }
        }

        if (PyErr_Occurred())
            return 0;
    }
    else
    {
        PyErr_SetString(ProgrammingError, "The second parameter to executemany must be a sequence, iterator, or generator.");
        return 0;
    }

    cursor->rowcount = -1;

    Py_RETURN_NONE;
}

static PyObject* Cursor_setinputsizes(PyObject* self, PyObject* sizes)
{
    if (!Cursor_Check(self))
    {
        PyErr_SetString(ProgrammingError, "Invalid cursor object.");
        return 0;
    }
    
    Cursor *cur = (Cursor*)self;
    if (Py_None == sizes)
    {
        Py_XDECREF(cur->inputsizes);
        cur->inputsizes = 0;
    }
    else
    {
        if (!IsSequence(sizes))
        {
            PyErr_SetString(ProgrammingError, "A non-None parameter to setinputsizes must be a sequence, iterator, or generator.");
            return 0;
        }

        Py_XDECREF(cur->inputsizes);
        Py_INCREF(sizes);
        cur->inputsizes = sizes;
    }

    Py_RETURN_NONE;
}

static PyObject* Cursor_fetch(Cursor* cur)
{
    // Internal function to fetch a single row and construct a Row object from it.  Used by all of the fetching
    // functions.
    //
    // Returns a Row object if successful.  If there are no more rows, zero is returned.  If an error occurs, an
    // exception is set and zero is returned.  (To differentiate between the last two, use PyErr_Occurred.)

    SQLRETURN ret = 0;
    Py_ssize_t field_count, i;
    PyObject** apValues;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLFetch(cur->hstmt);
    Py_END_ALLOW_THREADS

    if (cur->cnxn->hdbc == SQL_NULL_HANDLE)
    {
        // The connection was closed by another thread in the ALLOW_THREADS block above.
        return RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
    }

    if (ret == SQL_NO_DATA)
        return 0;

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLFetch", cur->cnxn->hdbc, cur->hstmt);

    field_count = PyTuple_GET_SIZE(cur->description);

    apValues = (PyObject**)PyMem_Malloc(sizeof(PyObject*) * field_count);

    if (apValues == 0)
        return PyErr_NoMemory();

    for (i = 0; i < field_count; i++)
    {
        PyObject* value = GetData(cur, i);

        if (!value)
        {
            FreeRowValues(i, apValues);
            return 0;
        }

        apValues[i] = value;
    }

    return (PyObject*)Row_InternalNew(cur->description, cur->map_name_to_index, field_count, apValues);
}


static PyObject* Cursor_fetchlist(Cursor* cur, Py_ssize_t max)
{
    // max
    //   The maximum number of rows to fetch.  If -1, fetch all rows.
    //
    // Returns a list of Rows.  If there are no rows, an empty list is returned.

    PyObject* results;
    PyObject* row;

    results = PyList_New(0);
    if (!results)
        return 0;

    while (max == -1 || max > 0)
    {
        row = Cursor_fetch(cur);

        if (!row)
        {
            if (PyErr_Occurred())
            {
                Py_DECREF(results);
                return 0;
            }
            break;
        }

        PyList_Append(results, row);
        Py_DECREF(row);

        if (max != -1)
            max--;
    }

    return results;
}


static PyObject* Cursor_iter(PyObject* self)
{
    Py_INCREF(self);
    return self;
}


static PyObject* Cursor_iternext(PyObject* self)
{
    // Implements the iterator protocol for cursors.  Fetches the next row.  Returns zero without setting an exception
    // when there are no rows.

    PyObject* result;

    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_RESULTS | CURSOR_RAISE_ERROR);

    if (!cursor)
        return 0;

    result = Cursor_fetch(cursor);

    return result;
}

static PyObject* Cursor_fetchval(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_RESULTS | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    Object row(Cursor_fetch(cursor));

    if (!row)
    {
        if (PyErr_Occurred())
            return 0;
        Py_RETURN_NONE;
    }

    return Row_item(row, 0);
}

static PyObject* Cursor_fetchone(PyObject* self, PyObject* args)
{
    UNUSED(args);

    PyObject* row;
    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_RESULTS | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    row = Cursor_fetch(cursor);

    if (!row)
    {
        if (PyErr_Occurred())
            return 0;
        Py_RETURN_NONE;
    }

    return row;
}


static PyObject* Cursor_fetchall(PyObject* self, PyObject* args)
{
    UNUSED(args);

    PyObject* result;
    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_RESULTS | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    result = Cursor_fetchlist(cursor, -1);

    return result;
}


static PyObject* Cursor_fetchmany(PyObject* self, PyObject* args)
{
    long rows;
    PyObject* result;

    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_RESULTS | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    rows = cursor->arraysize;
    if (!PyArg_ParseTuple(args, "|l", &rows))
        return 0;

    result = Cursor_fetchlist(cursor, rows);

    return result;
}


static char tables_doc[] =
    "C.tables(table=None, catalog=None, schema=None, tableType=None) --> self\n"
    "\n"
    "Executes SQLTables and creates a results set of tables defined in the data\n"
    "source.\n"
    "\n"
    "The table, catalog, and schema interpret the '_' and '%' characters as\n"
    "wildcards.  The escape character is driver specific, so use\n"
    "`Connection.searchescape`.\n"
    "\n"
    "Each row fetched has the following columns:\n"
    " 0) table_cat: The catalog name.\n"
    " 1) table_schem: The schema name.\n"
    " 2) table_name: The table name.\n"
    " 3) table_type: One of 'TABLE', 'VIEW', SYSTEM TABLE', 'GLOBAL TEMPORARY'\n"
    "    'LOCAL TEMPORARY', 'ALIAS', 'SYNONYM', or a data source-specific type name.";

char* Cursor_tables_kwnames[] = { "table", "catalog", "schema", "tableType", 0 };

static PyObject* Cursor_tables(PyObject* self, PyObject* args, PyObject* kwargs)
{
    const char* szCatalog = 0;
    const char* szSchema = 0;
    const char* szTableName = 0;
    const char* szTableType = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zzzz", Cursor_tables_kwnames, &szTableName, &szCatalog, &szSchema, &szTableType))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLTables(cur->hstmt, (SQLCHAR*)szCatalog, SQL_NTS, (SQLCHAR*)szSchema, SQL_NTS,
                    (SQLCHAR*)szTableName, SQL_NTS, (SQLCHAR*)szTableType, SQL_NTS);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLTables", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}


static char columns_doc[] =
    "C.columns(table=None, catalog=None, schema=None, column=None)\n\n"
    "Creates a results set of column names in specified tables by executing the ODBC SQLColumns function.\n"
    "Each row fetched has the following columns:\n"
    "  0) table_cat\n"
    "  1) table_schem\n"
    "  2) table_name\n"
    "  3) column_name\n"
    "  4) data_type\n"
    "  5) type_name\n"
    "  6) column_size\n"
    "  7) buffer_length\n"
    "  8) decimal_digits\n"
    "  9) num_prec_radix\n"
    " 10) nullable\n"
    " 11) remarks\n"
    " 12) column_def\n"
    " 13) sql_data_type\n"
    " 14) sql_datetime_sub\n"
    " 15) char_octet_length\n"
    " 16) ordinal_position\n"
    " 17) is_nullable";

char* Cursor_column_kwnames[] = { "table", "catalog", "schema", "column", 0 };

static PyObject* Cursor_columns(PyObject* self, PyObject* args, PyObject* kwargs)
{
    PyObject* pCatalog = 0;
    PyObject* pSchema  = 0;
    PyObject* pTable   = 0;
    PyObject* pColumn  = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOOO", Cursor_column_kwnames, &pTable, &pCatalog, &pSchema, &pColumn))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    const TextEnc& enc = cur->cnxn->metadata_enc;
    SQLWChar catalog(pCatalog, enc);
    SQLWChar schema(pSchema, enc);
    SQLWChar table(pTable, enc);
    SQLWChar column(pColumn, enc);

    if (!catalog.isValidOrNone() || !schema.isValidOrNone() || !table.isValidOrNone() || !column.isValidOrNone())
        return 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLColumnsW(cur->hstmt,
                      catalog, SQL_NTS,
                      schema, SQL_NTS,
                      table, SQL_NTS,
                      column, SQL_NTS);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLColumns", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}


static char statistics_doc[] =
    "C.statistics(catalog=None, schema=None, unique=False, quick=True) --> self\n\n"
    "Creates a results set of statistics about a single table and the indexes associated with \n"
    "the table by executing SQLStatistics.\n"
    "unique\n"
    "  If True, only unique indexes are returned.  Otherwise all indexes are returned.\n"
    "quick\n"
    "  If True, CARDINALITY and PAGES are returned  only if they are readily available\n"
    "  from the server\n"
    "\n"
    "Each row fetched has the following columns:\n\n"
    "  0) table_cat\n"
    "  1) table_schem\n"
    "  2) table_name\n"
    "  3) non_unique\n"
    "  4) index_qualifier\n"
    "  5) index_name\n"
    "  6) type\n"
    "  7) ordinal_position\n"
    "  8) column_name\n"
    "  9) asc_or_desc\n"
    " 10) cardinality\n"
    " 11) pages\n"
    " 12) filter_condition";

char* Cursor_statistics_kwnames[] = { "table", "catalog", "schema", "unique", "quick", 0 };

static PyObject* Cursor_statistics(PyObject* self, PyObject* args, PyObject* kwargs)
{
    const char* szCatalog = 0;
    const char* szSchema  = 0;
    const char* szTable   = 0;
    PyObject* pUnique = Py_False;
    PyObject* pQuick  = Py_True;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|zzOO", Cursor_statistics_kwnames, &szTable, &szCatalog, &szSchema,
                                     &pUnique, &pQuick))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLUSMALLINT nUnique   = (SQLUSMALLINT)(PyObject_IsTrue(pUnique) ? SQL_INDEX_UNIQUE : SQL_INDEX_ALL);
    SQLUSMALLINT nReserved = (SQLUSMALLINT)(PyObject_IsTrue(pQuick)  ? SQL_QUICK : SQL_ENSURE);

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLStatistics(cur->hstmt, (SQLCHAR*)szCatalog, SQL_NTS, (SQLCHAR*)szSchema, SQL_NTS, (SQLCHAR*)szTable, SQL_NTS,
                        nUnique, nReserved);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLStatistics", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}


static char rowIdColumns_doc[] =
    "C.rowIdColumns(table, catalog=None, schema=None, nullable=True) -->\n\n"
    "Executes SQLSpecialColumns with SQL_BEST_ROWID which creates a result set of columns that\n"
    "uniquely identify a row\n\n"
    "Each row fetched has the following columns:\n"
    " 0) scope\n"
    " 1) column_name\n"
    " 2) data_type\n"
    " 3) type_name\n"
    " 4) column_size\n"
    " 5) buffer_length\n"
    " 6) decimal_digits\n"
    " 7) pseudo_column";

static char rowVerColumns_doc[] =
    "C.rowIdColumns(table, catalog=None, schema=None, nullable=True) --> self\n\n"
    "Executes SQLSpecialColumns with SQL_ROWVER which creates a result set of columns that\n"
    "are automatically updated when any value in the row is updated.\n\n"
    "Each row fetched has the following columns:\n"
    " 0) scope\n"
    " 1) column_name\n"
    " 2) data_type\n"
    " 3) type_name\n"
    " 4) column_size\n"
    " 5) buffer_length\n"
    " 6) decimal_digits\n"
    " 7) pseudo_column";

char* Cursor_specialColumn_kwnames[] = { "table", "catalog", "schema", "nullable", 0 };

static PyObject* _specialColumns(PyObject* self, PyObject* args, PyObject* kwargs, SQLUSMALLINT nIdType)
{
    const char* szTable;
    const char* szCatalog = 0;
    const char* szSchema  = 0;
    PyObject* pNullable = Py_True;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|zzO", Cursor_specialColumn_kwnames, &szTable, &szCatalog, &szSchema, &pNullable))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    SQLUSMALLINT nNullable = (SQLUSMALLINT)(PyObject_IsTrue(pNullable) ? SQL_NULLABLE : SQL_NO_NULLS);

    Py_BEGIN_ALLOW_THREADS
    ret = SQLSpecialColumns(cur->hstmt, nIdType, (SQLCHAR*)szCatalog, SQL_NTS, (SQLCHAR*)szSchema, SQL_NTS, (SQLCHAR*)szTable, SQL_NTS,
                            SQL_SCOPE_TRANSACTION, nNullable);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLSpecialColumns", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}


static PyObject* Cursor_rowIdColumns(PyObject* self, PyObject* args, PyObject* kwargs)
{
    return _specialColumns(self, args, kwargs, SQL_BEST_ROWID);
}


static PyObject* Cursor_rowVerColumns(PyObject* self, PyObject* args, PyObject* kwargs)
{
    return _specialColumns(self, args, kwargs, SQL_ROWVER);
}


static char primaryKeys_doc[] =
    "C.primaryKeys(table, catalog=None, schema=None) --> self\n\n"
    "Creates a results set of column names that make up the primary key for a table\n"
    "by executing the SQLPrimaryKeys function.\n"
    "Each row fetched has the following columns:\n"
    " 0) table_cat\n"
    " 1) table_schem\n"
    " 2) table_name\n"
    " 3) column_name\n"
    " 4) key_seq\n"
    " 5) pk_name";

char* Cursor_primaryKeys_kwnames[] = { "table", "catalog", "schema", 0 };

static PyObject* Cursor_primaryKeys(PyObject* self, PyObject* args, PyObject* kwargs)
{
    const char* szTable;
    const char* szCatalog = 0;
    const char* szSchema  = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|zz", Cursor_primaryKeys_kwnames, &szTable, &szCatalog, &szSchema))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLPrimaryKeys(cur->hstmt, (SQLCHAR*)szCatalog, SQL_NTS, (SQLCHAR*)szSchema, SQL_NTS, (SQLCHAR*)szTable, SQL_NTS);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLPrimaryKeys", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}


static char foreignKeys_doc[] =
    "C.foreignKeys(table=None, catalog=None, schema=None,\n"
    "            foreignTable=None, foreignCatalog=None, foreignSchema=None) --> self\n\n"
    "Executes the SQLForeignKeys function and creates a results set of column names\n"
    "that are foreign keys in the specified table (columns in the specified table\n"
    "that refer to primary keys in other tables) or foreign keys in other tables\n"
    "that refer to the primary key in the specified table.\n\n"
    "Each row fetched has the following columns:\n"
    "  0) pktable_cat\n"
    "  1) pktable_schem\n"
    "  2) pktable_name\n"
    "  3) pkcolumn_name\n"
    "  4) fktable_cat\n"
    "  5) fktable_schem\n"
    "  6) fktable_name\n"
    "  7) fkcolumn_name\n"
    "  8) key_seq\n"
    "  9) update_rule\n"
    " 10) delete_rule\n"
    " 11) fk_name\n"
    " 12) pk_name\n"
    " 13) deferrability";

char* Cursor_foreignKeys_kwnames[] = { "table", "catalog", "schema", "foreignTable", "foreignCatalog", "foreignSchema", 0 };

static PyObject* Cursor_foreignKeys(PyObject* self, PyObject* args, PyObject* kwargs)
{
    const char* szTable          = 0;
    const char* szCatalog        = 0;
    const char* szSchema         = 0;
    const char* szForeignTable   = 0;
    const char* szForeignCatalog = 0;
    const char* szForeignSchema  = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zzzzzz", Cursor_foreignKeys_kwnames, &szTable, &szCatalog, &szSchema,
        &szForeignTable, &szForeignCatalog, &szForeignSchema))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLForeignKeys(cur->hstmt, (SQLCHAR*)szCatalog, SQL_NTS, (SQLCHAR*)szSchema, SQL_NTS, (SQLCHAR*)szTable, SQL_NTS,
                         (SQLCHAR*)szForeignCatalog, SQL_NTS, (SQLCHAR*)szForeignSchema, SQL_NTS, (SQLCHAR*)szForeignTable, SQL_NTS);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLForeignKeys", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}

static char getTypeInfo_doc[] =
    "C.getTypeInfo(sqlType=None) --> self\n\n"
    "Executes SQLGetTypeInfo a creates a result set with information about the\n"
    "specified data type or all data types supported by the ODBC driver if not\n"
    "specified.\n\n"
    "Each row fetched has the following columns:\n"
    " 0) type_name\n"
    " 1) data_type\n"
    " 2) column_size\n"
    " 3) literal_prefix\n"
    " 4) literal_suffix\n"
    " 5) create_params\n"
    " 6) nullable\n"
    " 7) case_sensitive\n"
    " 8) searchable\n"
    " 9) unsigned_attribute\n"
    "10) fixed_prec_scale\n"
    "11) auto_unique_value\n"
    "12) local_type_name\n"
    "13) minimum_scale\n"
    "14) maximum_scale\n"
    "15) sql_data_type\n"
    "16) sql_datetime_sub\n"
    "17) num_prec_radix\n"
    "18) interval_precision";

static PyObject* Cursor_getTypeInfo(PyObject* self, PyObject* args, PyObject* kwargs)
{
    UNUSED(kwargs);

    int nDataType = SQL_ALL_TYPES;

    if (!PyArg_ParseTuple(args, "|i", &nDataType))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLGetTypeInfo(cur->hstmt, (SQLSMALLINT)nDataType);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLGetTypeInfo", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}


static PyObject* Cursor_nextset(PyObject* self, PyObject* args)
{
    UNUSED(args);

    Cursor* cur = Cursor_Validate(self, 0);

    if (!cur)
        return 0;

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLMoreResults(cur->hstmt);
    Py_END_ALLOW_THREADS

    if (ret == SQL_NO_DATA)
    {
        free_results(cur, FREE_STATEMENT | KEEP_PREPARED);
        Py_RETURN_FALSE;
    }

    if (!SQL_SUCCEEDED(ret))
    {
        TRACE("nextset: %d not SQL_SUCCEEDED\n", ret);
        // Note: The SQL Server driver sometimes returns HY007 here if multiple statements (separated by ;) were
        // submitted.  This is not documented, but I've seen it with multiple successful inserts.

        PyObject* pError = GetErrorFromHandle(cur->cnxn, "SQLMoreResults", cur->cnxn->hdbc, cur->hstmt);
        //
        // free_results must be run after the error has been collected
        // from the cursor as it's lost otherwise.
        // If free_results raises an error (eg a lost connection) report that instead.
        //
        if (!free_results(cur, FREE_STATEMENT | KEEP_PREPARED)) {
            return 0;
        }
        //
        // Return any error from the GetErrorFromHandle call above.
        //
        if (pError)
        {
            RaiseErrorFromException(pError);
            Py_DECREF(pError);
            return 0;
        }

        //
        // Not clear how we'd get here, but if we're in an error state
        // without an error, behave as if we had no nextset
        //
        Py_RETURN_FALSE;
    }

    // Must retrieve DiagRecs immediately after SQLMoreResults
    if (ret == SQL_SUCCESS_WITH_INFO)
    {
        GetDiagRecs(cur);
    }
    else
    {
        Py_XDECREF(cur->messages);
        cur->messages = PyList_New(0);
    }

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
    {
        // Note: The SQL Server driver sometimes returns HY007 here if multiple statements (separated by ;) were
        // submitted.  This is not documented, but I've seen it with multiple successful inserts.

        PyObject* pError = GetErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);
        free_results(cur, FREE_STATEMENT | KEEP_PREPARED | KEEP_MESSAGES);
        return pError;
    }
    free_results(cur, KEEP_STATEMENT | KEEP_PREPARED | KEEP_MESSAGES);

    if (cCols != 0)
    {
        // A result set was created.

        if (!PrepareResults(cur, cCols))
            return 0;

        if (!create_name_map(cur, cCols, lowercase()))
            return 0;
    }

    SQLLEN cRows;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLRowCount(cur->hstmt, &cRows);
    Py_END_ALLOW_THREADS
    cur->rowcount = (int)cRows;

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLRowCount", cur->cnxn->hdbc, cur->hstmt);

    Py_RETURN_TRUE;
}


static char procedureColumns_doc[] =
    "C.procedureColumns(procedure=None, catalog=None, schema=None) --> self\n\n"
    "Executes SQLProcedureColumns and creates a result set of information\n"
    "about stored procedure columns and results.\n"
    "  0) procedure_cat\n"
    "  1) procedure_schem\n"
    "  2) procedure_name\n"
    "  3) column_name\n"
    "  4) column_type\n"
    "  5) data_type\n"
    "  6) type_name\n"
    "  7) column_size\n"
    "  8) buffer_length\n"
    "  9) decimal_digits\n"
    " 10) num_prec_radix\n"
    " 11) nullable\n"
    " 12) remarks\n"
    " 13) column_def\n"
    " 14) sql_data_type\n"
    " 15) sql_datetime_sub\n"
    " 16) char_octet_length\n"
    " 17) ordinal_position\n"
    " 18) is_nullable";

char* Cursor_procedureColumns_kwnames[] = { "procedure", "catalog", "schema", 0 };

static PyObject* Cursor_procedureColumns(PyObject* self, PyObject* args, PyObject* kwargs)
{
    const char* szProcedure = 0;
    const char* szCatalog   = 0;
    const char* szSchema    = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zzz", Cursor_procedureColumns_kwnames, &szProcedure, &szCatalog, &szSchema))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLProcedureColumns(cur->hstmt, (SQLCHAR*)szCatalog, SQL_NTS, (SQLCHAR*)szSchema, SQL_NTS,
                              (SQLCHAR*)szProcedure, SQL_NTS, 0, 0);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLProcedureColumns", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}


static char procedures_doc[] =
    "C.procedures(procedure=None, catalog=None, schema=None) --> self\n\n"
    "Executes SQLProcedures and creates a result set of information about the\n"
    "procedures in the data source.\n"
    "Each row fetched has the following columns:\n"
    " 0) procedure_cat\n"
    " 1) procedure_schem\n"
    " 2) procedure_name\n"
    " 3) num_input_params\n"
    " 4) num_output_params\n"
    " 5) num_result_sets\n"
    " 6) remarks\n"
    " 7) procedure_type";

char* Cursor_procedures_kwnames[] = { "procedure", "catalog", "schema", 0 };

static PyObject* Cursor_procedures(PyObject* self, PyObject* args, PyObject* kwargs)
{
    const char* szProcedure = 0;
    const char* szCatalog   = 0;
    const char* szSchema    = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|zzz", Cursor_procedures_kwnames, &szProcedure, &szCatalog, &szSchema))
        return 0;

    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN);

    if (!free_results(cur, FREE_STATEMENT | FREE_PREPARED))
        return 0;

    SQLRETURN ret = 0;

    Py_BEGIN_ALLOW_THREADS
    ret = SQLProcedures(cur->hstmt, (SQLCHAR*)szCatalog, SQL_NTS, (SQLCHAR*)szSchema, SQL_NTS, (SQLCHAR*)szProcedure, SQL_NTS);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLProcedures", cur->cnxn->hdbc, cur->hstmt);

    SQLSMALLINT cCols;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLNumResultCols(cur->hstmt, &cCols);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLNumResultCols", cur->cnxn->hdbc, cur->hstmt);

    if (!PrepareResults(cur, cCols))
        return 0;

    if (!create_name_map(cur, cCols, true))
        return 0;

    // Return the cursor so the results can be iterated over directly.
    Py_INCREF(cur);
    return (PyObject*)cur;
}

static char skip_doc[] =
    "skip(count) --> None\n" \
    "\n" \
    "Skips the next `count` records by calling SQLFetchScroll with SQL_FETCH_NEXT.\n"
    "For convenience, skip(0) is accepted and will do nothing.";

static PyObject* Cursor_skip(PyObject* self, PyObject* args)
{
    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_RESULTS | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    int count;
    if (!PyArg_ParseTuple(args, "i", &count))
        return 0;
    if (count == 0)
        Py_RETURN_NONE;

    // Note: I'm not sure about the performance implications of looping here -- I certainly would rather use
    // SQLFetchScroll(SQL_FETCH_RELATIVE, count), but it requires scrollable cursors which are often slower.  I would
    // not expect skip to be used in performance intensive code since different SQL would probably be the "right"
    // answer instead of skip anyway.

    SQLRETURN ret = SQL_SUCCESS;
    Py_BEGIN_ALLOW_THREADS
    for (int i = 0; i < count && SQL_SUCCEEDED(ret); i++)
        ret = SQLFetchScroll(cursor->hstmt, SQL_FETCH_NEXT, 0);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA)
        return RaiseErrorFromHandle(cursor->cnxn, "SQLFetchScroll", cursor->cnxn->hdbc, cursor->hstmt);

    Py_RETURN_NONE;
}

static const char* commit_doc =
    "Commits any pending transaction to the database on the current connection,\n"
    "including those from other cursors.\n";

static PyObject* Cursor_commit(PyObject* self, PyObject* args)
{
    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cur)
        return 0;
    return Connection_endtrans(cur->cnxn, SQL_COMMIT);
}

static char rollback_doc[] =
    "Rolls back any pending transaction to the database on the current connection,\n"
    "including those from other cursors.\n";

static PyObject* Cursor_rollback(PyObject* self, PyObject* args)
{
    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cur)
        return 0;
    return Connection_endtrans(cur->cnxn, SQL_ROLLBACK);
}


static char cancel_doc[] =
    "Cursor.cancel() -> None\n"
    "Cancels the processing of the current statement.\n"
    "\n"
    "Cancels the processing of the current statement.\n"
    "\n"
    "This calls SQLCancel and is designed to be called from another thread to"
    "stop processing of an ongoing query.";

static PyObject* Cursor_cancel(PyObject* self, PyObject* args)
{
    UNUSED(args);
    Cursor* cur = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cur)
        return 0;
    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLCancel(cur->hstmt);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
        return RaiseErrorFromHandle(cur->cnxn, "SQLCancel", cur->cnxn->hdbc, cur->hstmt);

    Py_RETURN_NONE;
}


static PyObject* Cursor_ignored(PyObject* self, PyObject* args)
{
    UNUSED(self, args);
    Py_RETURN_NONE;
}


static char rowcount_doc[] =
    "This read-only attribute specifies the number of rows the last DML statement\n"
    " (INSERT, UPDATE, DELETE) affected.  This is set to -1 for SELECT statements.";

static char description_doc[] =
    "This read-only attribute is a sequence of 7-item sequences.  Each of these\n" \
    "sequences contains information describing one result column: (name, type_code,\n" \
    "display_size, internal_size, precision, scale, null_ok).  All values except\n" \
    "name, type_code, and internal_size are None.  The type_code entry will be the\n" \
    "type object used to create values for that column (e.g. `str` or\n" \
    "`datetime.datetime`).\n" \
    "\n" \
    "This attribute will be None for operations that do not return rows or if the\n" \
    "cursor has not had an operation invoked via the execute() method yet.\n" \
    "\n" \
    "The type_code can be interpreted by comparing it to the Type Objects defined in\n" \
    "the DB API and defined the pyodbc module: Date, Time, Timestamp, Binary,\n" \
    "STRING, BINARY, NUMBER, and DATETIME.";

static char arraysize_doc[] =
    "This read/write attribute specifies the number of rows to fetch at a time with\n" \
    "fetchmany(). It defaults to 1 meaning to fetch a single row at a time.";

static char connection_doc[] =
    "This read-only attribute return a reference to the Connection object on which\n" \
    "the cursor was created.\n" \
    "\n" \
    "The attribute simplifies writing polymorph code in multi-connection\n" \
    "environments.";

static char fastexecmany_doc[] =
    "This read/write attribute specifies whether to use a faster executemany() which\n" \
    "uses parameter arrays. Not all drivers may work with this implementation.";

static char messages_doc[] =
    "This read-only attribute is a list of all the diagnostic messages in the\n" \
    "current result set.";

static PyMemberDef Cursor_members[] =
{
    {"rowcount",    T_INT,       offsetof(Cursor, rowcount),        READONLY, rowcount_doc },
    {"description", T_OBJECT_EX, offsetof(Cursor, description),     READONLY, description_doc },
    {"arraysize",   T_INT,       offsetof(Cursor, arraysize),       0,        arraysize_doc },
    {"connection",  T_OBJECT_EX, offsetof(Cursor, cnxn),            READONLY, connection_doc },
    {"fast_executemany",T_BOOL,  offsetof(Cursor, fastexecmany),    0,        fastexecmany_doc },
    {"messages",    T_OBJECT_EX, offsetof(Cursor, messages),        READONLY, messages_doc },
    { 0 }
};

static PyObject* Cursor_getnoscan(PyObject* self, void *closure)
{
    UNUSED(closure);

    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    SQLULEN noscan = SQL_NOSCAN_OFF;
    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLGetStmtAttr(cursor->hstmt, SQL_ATTR_NOSCAN, (SQLPOINTER)&noscan, sizeof(SQLULEN), 0);
    Py_END_ALLOW_THREADS

    if (!SQL_SUCCEEDED(ret))
    {
        // Not supported?  We're going to assume 'no'.
        Py_RETURN_FALSE;
    }

    if (noscan == SQL_NOSCAN_OFF)
        Py_RETURN_FALSE;

    Py_RETURN_TRUE;
}

static int Cursor_setnoscan(PyObject* self, PyObject* value, void *closure)
{
    UNUSED(closure);

    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cursor)
        return -1;

    if (value == 0)
    {
        PyErr_SetString(PyExc_TypeError, "Cannot delete the noscan attribute");
        return -1;
    }

    uintptr_t noscan = PyObject_IsTrue(value) ? SQL_NOSCAN_ON : SQL_NOSCAN_OFF;
    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLSetStmtAttr(cursor->hstmt, SQL_ATTR_NOSCAN, (SQLPOINTER)noscan, 0);
    Py_END_ALLOW_THREADS
    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(cursor->cnxn, "SQLSetStmtAttr(SQL_ATTR_NOSCAN)", cursor->cnxn->hdbc, cursor->hstmt);
        return -1;
    }

    return 0;
}

static PyGetSetDef Cursor_getsetters[] =
{
    {"noscan", Cursor_getnoscan, Cursor_setnoscan, "NOSCAN statement attr", 0},
    { 0 }
};

static char executemany_doc[] =
    "executemany(sql, seq_of_params) --> Cursor | count | None\n" \
    "\n" \
    "Prepare a database query or command and then execute it against all parameter\n" \
    "sequences  found in the sequence seq_of_params.\n" \
    "\n" \
    "Only the result of the final execution is returned.  See `execute` for a\n" \
    "description of parameter passing the return value.";

static char nextset_doc[] = "nextset() --> True | None\n" \
    "\n" \
    "Jumps to the next resultset if the last sql has multiple resultset." \
    "Returns True if there is a next resultset otherwise None.";

static char ignored_doc[] = "Ignored.";

static char fetchval_doc[] =
    "fetchval() --> value | None\n" \
    "\n"
    "Returns the first column of the next row in the result set or None\n" \
    "if there are no more rows.";

static char fetchone_doc[] =
    "fetchone() --> Row | None\n" \
    "\n" \
    "Fetch the next row of a query result set, returning a single Row instance, or\n" \
    "None when no more data is available.\n" \
    "\n" \
    "A ProgrammingError exception is raised if the previous call to execute() did\n" \
    "not produce any result set or no call was issued yet.";

static char fetchmany_doc[] =
    "fetchmany(size=cursor.arraysize) --> list of Rows\n" \
    "\n" \
    "Fetch the next set of rows of a query result, returning a list of Row\n" \
    "instances. An empty list is returned when no more rows are available.\n" \
    "\n" \
    "The number of rows to fetch per call is specified by the parameter.  If it is\n" \
    "not given, the cursor's arraysize determines the number of rows to be\n" \
    "fetched. The method should try to fetch as many rows as indicated by the size\n" \
    "parameter. If this is not possible due to the specified number of rows not\n" \
    "being available, fewer rows may be returned.\n" \
    "\n" \
    "A ProgrammingError exception is raised if the previous call to execute() did\n" \
    "not produce any result set or no call was issued yet.";

static char fetchall_doc[] =
    "fetchall() --> list of Rows\n" \
    "\n" \
    "Fetch all remaining rows of a query result, returning them as a list of Rows.\n" \
    "An empty list is returned if there are no more rows.\n" \
    "\n" \
    "A ProgrammingError exception is raised if the previous call to execute() did\n" \
    "not produce any result set or no call was issued yet.";

static char setinputsizes_doc[] =
    "setinputsizes(sizes) -> None\n" \
    "\n" \
    "Sets the type information to be used when binding parameters.\n" \
    "sizes must be a sequence of values, one for each input parameter.\n" \
    "Each value may be an integer to override the column size when binding character\n" \
    "data, a Type Object to override the SQL type, or a sequence of integers to specify\n" \
    "(SQL type, column size, decimal digits) where any may be none to use the default.\n" \
    "\n" \
    "Parameters beyond the length of the sequence will be bound with the defaults.\n" \
    "Setting sizes to None reverts all parameters to the defaults.";

static char enter_doc[] = "__enter__() -> self.";
static PyObject* Cursor_enter(PyObject* self, PyObject* args)
{
    UNUSED(args);
    Py_INCREF(self);
    return self;
}

static char exit_doc[] = "__exit__(*excinfo) -> None.  Commits the connection if necessary..";
static PyObject* Cursor_exit(PyObject* self, PyObject* args)
{
    Cursor* cursor = Cursor_Validate(self, CURSOR_REQUIRE_OPEN | CURSOR_RAISE_ERROR);
    if (!cursor)
        return 0;

    // If an error has occurred, `args` will be a tuple of 3 values.  Otherwise it will be a tuple of 3 `None`s.
    assert(PyTuple_Check(args));

    if (cursor->cnxn->nAutoCommit == SQL_AUTOCOMMIT_OFF && PyTuple_GetItem(args, 0) == Py_None)
    {
        SQLRETURN ret;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLEndTran(SQL_HANDLE_DBC, cursor->cnxn->hdbc, SQL_COMMIT);
        Py_END_ALLOW_THREADS

        if (!SQL_SUCCEEDED(ret))
            return RaiseErrorFromHandle(cursor->cnxn, "SQLEndTran(SQL_COMMIT)", cursor->cnxn->hdbc, cursor->hstmt);
    }

    Py_RETURN_NONE;
}


static PyMethodDef Cursor_methods[] =
{
    { "close",            (PyCFunction)Cursor_close,            METH_NOARGS,                close_doc            },
    { "execute",          (PyCFunction)Cursor_execute,          METH_VARARGS,               execute_doc          },
    { "executemany",      (PyCFunction)Cursor_executemany,      METH_VARARGS,               executemany_doc      },
    { "setinputsizes",    (PyCFunction)Cursor_setinputsizes,    METH_O,                     setinputsizes_doc    },
    { "setoutputsize",    (PyCFunction)Cursor_ignored,          METH_VARARGS,               ignored_doc          },
    { "fetchval",         (PyCFunction)Cursor_fetchval,         METH_NOARGS,                fetchval_doc         },
    { "fetchone",         (PyCFunction)Cursor_fetchone,         METH_NOARGS,                fetchone_doc         },
    { "fetchall",         (PyCFunction)Cursor_fetchall,         METH_NOARGS,                fetchall_doc         },
    { "fetchmany",        (PyCFunction)Cursor_fetchmany,        METH_VARARGS,               fetchmany_doc        },
    { "nextset",          (PyCFunction)Cursor_nextset,          METH_NOARGS,                nextset_doc          },
    { "tables",           (PyCFunction)Cursor_tables,           METH_VARARGS|METH_KEYWORDS, tables_doc           },
    { "columns",          (PyCFunction)Cursor_columns,          METH_VARARGS|METH_KEYWORDS, columns_doc          },
    { "statistics",       (PyCFunction)Cursor_statistics,       METH_VARARGS|METH_KEYWORDS, statistics_doc       },
    { "rowIdColumns",     (PyCFunction)Cursor_rowIdColumns,     METH_VARARGS|METH_KEYWORDS, rowIdColumns_doc     },
    { "rowVerColumns",    (PyCFunction)Cursor_rowVerColumns,    METH_VARARGS|METH_KEYWORDS, rowVerColumns_doc    },
    { "primaryKeys",      (PyCFunction)Cursor_primaryKeys,      METH_VARARGS|METH_KEYWORDS, primaryKeys_doc      },
    { "foreignKeys",      (PyCFunction)Cursor_foreignKeys,      METH_VARARGS|METH_KEYWORDS, foreignKeys_doc      },
    { "getTypeInfo",      (PyCFunction)Cursor_getTypeInfo,      METH_VARARGS|METH_KEYWORDS, getTypeInfo_doc      },
    { "procedures",       (PyCFunction)Cursor_procedures,       METH_VARARGS|METH_KEYWORDS, procedures_doc       },
    { "procedureColumns", (PyCFunction)Cursor_procedureColumns, METH_VARARGS|METH_KEYWORDS, procedureColumns_doc },
    { "skip",             (PyCFunction)Cursor_skip,             METH_VARARGS,               skip_doc             },
    { "commit",           (PyCFunction)Cursor_commit,           METH_NOARGS,                commit_doc           },
    { "rollback",         (PyCFunction)Cursor_rollback,         METH_NOARGS,                rollback_doc         },
    {"cancel",           (PyCFunction)Cursor_cancel,           METH_NOARGS,                cancel_doc},
    {"__enter__",        Cursor_enter,                         METH_NOARGS,                enter_doc            },
    {"__exit__",         Cursor_exit,                          METH_VARARGS,               exit_doc             },
    {0, 0, 0, 0}
};

static char cursor_doc[] =
    "Cursor objects represent a database cursor, which is used to manage the context\n" \
    "of a fetch operation.  Cursors created from the same connection are not\n" \
    "isolated, i.e., any changes done to the database by a cursor are immediately\n" \
    "visible by the other cursors.  Cursors created from different connections are\n" \
    "isolated.\n" \
    "\n" \
    "Cursors implement the iterator protocol, so results can be iterated:\n" \
    "\n" \
    "  cursor.execute(sql)\n" \
    "  for row in cursor:\n" \
    "     print row[0]";

PyTypeObject CursorType =
{
    PyVarObject_HEAD_INIT(0, 0)
    "pyodbc.Cursor",                                        // tp_name
    sizeof(Cursor),                                         // tp_basicsize
    0,                                                      // tp_itemsize
    (destructor)Cursor_dealloc,                             // destructor tp_dealloc
    0,                                                      // tp_print
    0,                                                      // tp_getattr
    0,                                                      // tp_setattr
    0,                                                      // tp_compare
    0,                                                      // tp_repr
    0,                                                      // tp_as_number
    0,                                                      // tp_as_sequence
    0,                                                      // tp_as_mapping
    0,                                                      // tp_hash
    0,                                                      // tp_call
    0,                                                      // tp_str
    0,                                                      // tp_getattro
    0,                                                      // tp_setattro
    0,                                                      // tp_as_buffer
    Py_TPFLAGS_DEFAULT,
    cursor_doc,                                             // tp_doc
    0,                                                      // tp_traverse
    0,                                                      // tp_clear
    0,                                                      // tp_richcompare
    0,                                                      // tp_weaklistoffset
    Cursor_iter,                               // tp_iter
    Cursor_iternext,                          // tp_iternext
    Cursor_methods,                                         // tp_methods
    Cursor_members,                                         // tp_members
    Cursor_getsetters,                                      // tp_getset
    0,                                                      // tp_base
    0,                                                      // tp_dict
    0,                                                      // tp_descr_get
    0,                                                      // tp_descr_set
    0,                                                      // tp_dictoffset
    0,                                                      // tp_init
    0,                                                      // tp_alloc
    0,                                                      // tp_new
    0,                                                      // tp_free
    0,                                                      // tp_is_gc
    0,                                                      // tp_bases
    0,                                                      // tp_mro
    0,                                                      // tp_cache
    0,                                                      // tp_subclasses
    0,                                                      // tp_weaklist
};

Cursor*
Cursor_New(Connection* cnxn)
{
    // Exported to allow the connection class to create cursors.

#ifdef _MSC_VER
#pragma warning(disable : 4365)
#endif
    Cursor* cur = PyObject_NEW(Cursor, &CursorType);
#ifdef _MSC_VER
#pragma warning(default : 4365)
#endif

    if (cur)
    {
        cur->cnxn              = cnxn;
        cur->hstmt             = SQL_NULL_HANDLE;
        cur->description       = Py_None;
        cur->pPreparedSQL      = 0;
        cur->paramcount        = 0;
        cur->paramtypes        = 0;
        cur->paramInfos        = 0;
        cur->inputsizes        = 0;
        cur->colinfos          = 0;
        cur->arraysize         = 1;
        cur->rowcount          = -1;
        cur->map_name_to_index = 0;
        cur->fastexecmany      = 0;
        cur->messages          = Py_None;

        Py_INCREF(cnxn);
        Py_INCREF(cur->description);
        Py_INCREF(cur->messages);

        SQLRETURN ret;
        Py_BEGIN_ALLOW_THREADS
        ret = SQLAllocHandle(SQL_HANDLE_STMT, cnxn->hdbc, &cur->hstmt);
        Py_END_ALLOW_THREADS

        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cnxn, "SQLAllocHandle", cnxn->hdbc, SQL_NULL_HANDLE);
            Py_DECREF(cur);
            return 0;
        }

        if (cnxn->timeout)
        {
            Py_BEGIN_ALLOW_THREADS
            ret = SQLSetStmtAttr(cur->hstmt, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER)(uintptr_t)cnxn->timeout, 0);
            Py_END_ALLOW_THREADS

            if (!SQL_SUCCEEDED(ret))
            {
                RaiseErrorFromHandle(cnxn, "SQLSetStmtAttr(SQL_ATTR_QUERY_TIMEOUT)", cnxn->hdbc, cur->hstmt);
                Py_DECREF(cur);
                return 0;
            }
        }

        TRACE("cursor.new cnxn=%p hdbc=%d cursor=%p hstmt=%d\n", (Connection*)cur->cnxn, ((Connection*)cur->cnxn)->hdbc, cur, cur->hstmt);
    }

    return cur;
}

void Cursor_init()
{
    PyDateTime_IMPORT;
}
