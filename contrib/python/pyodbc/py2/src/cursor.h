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

#ifndef CURSOR_H
#define CURSOR_H

struct Connection;

struct ColumnInfo
{
    SQLSMALLINT sql_type;

    // The column size from SQLDescribeCol.  For character types, this is the maximum length, not including the NULL
    // terminator.  For binary values, this is the maximum length.  For numeric and decimal values, it is the defined
    // number of digits. For example, the precision of a column defined as NUMERIC(10,3) is 10.
    //
    // This value can be SQL_NO_TOTAL in which case the driver doesn't know the maximum length, such as for LONGVARCHAR
    // fields.
    SQLULEN column_size;

    // Tells us if an integer type is signed or unsigned.  This is determined after a query using SQLColAttribute.  All
    // of the integer types are the same size whether signed and unsigned, so we can allocate memory ahead of time
    // without knowing this.  We use this during the fetch when converting to a Python integer or long.
    bool is_unsigned;
};

struct ParamInfo
{
    // The following correspond to the SQLBindParameter parameters.
    SQLSMALLINT ValueType;
    SQLSMALLINT ParameterType;
    SQLULEN     ColumnSize;
    SQLSMALLINT DecimalDigits;

    // The value pointer that will be bound.  If `alloc` is true, this was allocated with malloc and must be freed.
    // Otherwise it is zero or points into memory owned by the original Python parameter.
    SQLPOINTER ParameterValuePtr;

    SQLLEN BufferLength;
    SQLLEN StrLen_or_Ind;

    // If true, the memory in ParameterValuePtr was allocated via malloc and must be freed.
    bool allocated;

    PyObject* pObject;
    // An optional object that will be decremented at the end of the execute.
    // This is useful when the ParameterValuePtr data is in a Python object -
    // the object can be put here (and INCREFed if necessary!) instead of
    // copying the data out.
    //
    // If SQLPutData is used, this must be set to a bytes or bytearray object!

    SQLLEN maxlength;
    // If SQLPutData is being used, this must be set to the amount that can be
    // written to each SQLPutData call.  (It is not clear if they are limited
    // like SQLBindParameter or not.)

    // For TVPs, the nested descriptors and current row.
    struct ParamInfo *nested;
    SQLLEN curTvpRow;

    // Optional data.  If used, ParameterValuePtr will point into this.
    union
    {
        unsigned char ch;
        int i32;
        INT64 i64;
        double dbl;
        TIMESTAMP_STRUCT timestamp;
        DATE_STRUCT date;
        TIME_STRUCT time;
    } Data;
};

struct Cursor
{
    PyObject_HEAD

    // The Connection object (which is a PyObject) that created this cursor.
    Connection* cnxn;

    // Set to SQL_NULL_HANDLE when the cursor is closed.
    HSTMT hstmt;

    //
    // SQL Parameters
    //

    // If non-zero, a pointer to the previously prepared SQL string, allowing us to skip the prepare and gathering of
    // parameter data.
    PyObject* pPreparedSQL;

    // The number of parameter markers in pPreparedSQL.  This will be zero when pPreparedSQL is zero but is set
    // immediately after preparing the SQL.
    int paramcount;

    // If non-zero, a pointer to an array of SQL type values allocated via malloc.  This is zero until we actually ask
    // for the type of parameter, which is only when a parameter is None (NULL).  At that point, the entire array is
    // allocated (length == paramcount) but all entries are set to SQL_UNKNOWN_TYPE.
    SQLSMALLINT* paramtypes;

    // If non-zero, a pointer to a buffer containing the actual parameters bound.  If pPreparedSQL is zero, this should
    // be freed using free and set to zero.
    //
    // Even if the same SQL statement is executed twice, the parameter bindings are redone from scratch since we try to
    // bind into the Python objects directly.
    ParamInfo* paramInfos;

    // Parameter set array (used with executemany)
    unsigned char *paramArray;
    
    // Whether to use fast executemany with parameter arrays and other optimisations
    char fastexecmany;
    
    // The list of information for setinputsizes().
    PyObject *inputsizes;

    //
    // Result Information
    //

    // An array of ColumnInfos, allocated via malloc.  This will be zero when closed or when there are no query
    // results.
    ColumnInfo* colinfos;

    // The description tuple described in the DB API 2.0 specification.  Set to None when there are no results.
    PyObject* description;

    int arraysize;

    // The Cursor.rowcount attribute from the DB API specification.
    int rowcount;

    // A dictionary that maps from column name (PyString) to index into the result columns (PyInteger).  This is
    // constructed during an execute and shared with each row (reference counted) to implement accessing results by
    // column name.
    //
    // This duplicates some ODBC functionality, but allows us to use Row objects after the statement is closed and
    // should use less memory than putting each column into the Row's __dict__.
    //
    // Since this is shared by Row objects, it cannot be reused.  New dictionaries are created for every execute.  This
    // will be zero whenever there are no results.
    PyObject* map_name_to_index;

    // The messages attribute described in the DB API 2.0 specification.
    // Contains a list of all non-data messages provided by the driver, retrieved using SQLGetDiagRec.
    PyObject* messages;
};

void Cursor_init();

Cursor* Cursor_New(Connection* cnxn);
PyObject* Cursor_execute(PyObject* self, PyObject* args);

#endif
