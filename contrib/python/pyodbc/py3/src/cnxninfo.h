
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#ifndef CNXNINFO_H
#define CNXNINFO_H

struct Connection;
extern PyTypeObject CnxnInfoType;

struct CnxnInfo
{
    PyObject_HEAD

    // The description of these fields is in the connection structure.

    char odbc_major;
    char odbc_minor;

    bool supports_describeparam;
    int datetime_precision;

    // Do we need to use SQL_LEN_DATA_AT_EXEC?  Some drivers (e.g. FreeTDS 0.91) have problems with long values, so
    // we'll use SQL_DATA_AT_EXEC when possible.  If this is true, however, we'll need to pass the length.
    bool need_long_data_len;

    // These are from SQLGetTypeInfo.column_size, so the char ones are in characters, not bytes.
    int varchar_maxlength;
    int wvarchar_maxlength;
    int binary_maxlength;
};

bool CnxnInfo_init();

// Looks-up or creates a CnxnInfo object for the given connection string.  The connection string can be a Unicode or
// String object.

PyObject* GetConnectionInfo(PyObject* pConnectionString, Connection* cnxn);

#endif // CNXNINFO_H
