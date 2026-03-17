// https://msdn.microsoft.com/en-us/library/ms711014(v=vs.85).aspx
//
// "The length of both the data buffer and the data it contains is measured in bytes, as
// opposed to characters."
//
// https://msdn.microsoft.com/en-us/library/ms711786(v=vs.85).aspx
//
// Column Size: "For character types, this is the length in characters of the data"

// NOTE: I have not ported the "fast executemany" code from 4.x to 5.x yet.  Once 5.0 is
// complete, I'll port it in 5.1.  My goal is to ensure it uses the exact same binding code
// between both code paths.  I'll probably also rename the feature to something that describes
// it more precisely like "array binding".

#include "pyodbc.h"
#include "wrapper.h"
#include "textenc.h"
#include "pyodbcmodule.h"
#include "cursor.h"
#include "params.h"
#include "connection.h"
#include "errors.h"
#include "dbspecific.h"
#include "row.h"
#include <datetime.h>


inline Connection* GetConnection(Cursor* cursor)
{
    return (Connection*)cursor->cnxn;
}

struct DAEParam
{
    PyObject *cell;
    SQLLEN maxlen;
};


static int DetectCType(PyObject *cell, ParamInfo *pi)
{
    // Detects and sets the appropriate C type to use for binding the specified Python object.
    // Also sets the buffer length to use.  Returns false if unsuccessful.
    //
    // We're setting the pi ParameterType and BufferLength.  These are based on the Python
    // value if not None or binary.  For those, the *existing* ParameterType is used.  This
    // could be from a previous row or could have been initialized from SQLDescribeParam.

    PyObject* cls = 0;
    if (PyBool_Check(cell))
    {
    Type_Bool:
        pi->ValueType = SQL_C_BIT;
        pi->BufferLength = 1;
    }
    else if (PyLong_Check(cell))
    {
    Type_Long:
        if (pi->ParameterType == SQL_NUMERIC ||
            pi->ParameterType == SQL_DECIMAL)
        {
            pi->ValueType = SQL_C_NUMERIC;
            pi->BufferLength = sizeof(SQL_NUMERIC_STRUCT);
        }
        else
        {
            pi->ValueType = SQL_C_SBIGINT;
            pi->BufferLength = sizeof(long long);
        }
    }
    else if (PyFloat_Check(cell))
    {
    Type_Float:
        pi->ValueType = SQL_C_DOUBLE;
        pi->BufferLength = sizeof(double);
    }
    else if (PyBytes_Check(cell))
    {
    Type_Bytes:
        // Assume the SQL type is also character (2.x) or binary (3.x).
        // If it is a max-type (ColumnSize == 0), use DAE.
        pi->ValueType = SQL_C_BINARY;
        pi->BufferLength = pi->ColumnSize ? pi->ColumnSize : sizeof(DAEParam);
    }
    else if (PyUnicode_Check(cell))
    {
    Type_Unicode:
        // Assume the SQL type is also wide character.
        // If it is a max-type (ColumnSize == 0), use DAE.
        pi->ValueType = SQL_C_WCHAR;
        pi->BufferLength = pi->ColumnSize ? pi->ColumnSize * sizeof(SQLWCHAR) : sizeof(DAEParam);
    }
    else if (PyDateTime_Check(cell))
    {
    Type_DateTime:
        pi->ValueType = SQL_C_TYPE_TIMESTAMP;
        pi->BufferLength = sizeof(SQL_TIMESTAMP_STRUCT);
    }
    else if (PyDate_Check(cell))
    {
    Type_Date:
        pi->ValueType = SQL_C_TYPE_DATE;
        pi->BufferLength = sizeof(SQL_DATE_STRUCT);
    }
    else if (PyTime_Check(cell))
    {
    Type_Time:
        if (pi->ParameterType == SQL_SS_TIME2)
        {
            pi->ValueType = SQL_C_BINARY;
            pi->BufferLength = sizeof(SQL_SS_TIME2_STRUCT);
        }
        else
        {
            pi->ValueType = SQL_C_TYPE_TIME;
            pi->BufferLength = sizeof(SQL_TIME_STRUCT);
        }
    }
    else if (PyByteArray_Check(cell))
    {
        // Type_ByteArray:
        pi->ValueType = SQL_C_BINARY;
        pi->BufferLength = pi->ColumnSize ? pi->ColumnSize : sizeof(DAEParam);
    }
    else if (cell == Py_None || cell == null_binary)
    {
        // Use the SQL type to guess what Nones should be inserted as here.
        switch (pi->ParameterType)
        {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
            goto Type_Bytes;
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
            goto Type_Unicode;
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            goto Type_Decimal;
        case SQL_BIGINT:
            goto Type_Long;
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_TINYINT:
            goto Type_Long;
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            goto Type_Float;
        case SQL_BIT:
            goto Type_Bool;
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
          // TODO: Shouldn't this be bytes?
            //  goto Type_ByteArray;
            goto Type_Bytes;
        case SQL_TYPE_DATE:
            goto Type_Date;
        case SQL_SS_TIME2:
        case SQL_TYPE_TIME:
            goto Type_Time;
        case SQL_TYPE_TIMESTAMP:
            goto Type_DateTime;
        case SQL_GUID:
            goto Type_UUID;
        default:
            goto Type_Bytes;
        }
    }
    else if (IsInstanceForThread(cell, "uuid", "UUID", &cls) && cls)
    {
    Type_UUID:
        // UUID
        pi->ValueType = SQL_C_GUID;
        pi->BufferLength = 16;
    }
    else if (IsInstanceForThread(cell, "decimal", "Decimal", &cls) && cls)
    {
    Type_Decimal:
        pi->ValueType = SQL_C_NUMERIC;
        pi->BufferLength = sizeof(SQL_NUMERIC_STRUCT);
    }
    else
    {
        RaiseErrorV(0, ProgrammingError, "Unknown object type %s during describe", cell->ob_type->tp_name);
        return false;
    }
    return true;
}

#define WRITEOUT(type, ptr, val, indv) { *(type*)(*ptr) = (val); *ptr += sizeof(type); indv = sizeof(type); }
// Convert Python object into C data for binding.
// Output pointer is written to with data, indicator, and updated.
// Returns false if object could not be converted.
static int PyToCType(Cursor *cur, unsigned char **outbuf, PyObject *cell, ParamInfo *pi)
{
    PyObject *cls = 0;
    // TODO: Any way to make this a switch (O(1)) or similar instead of if-else chain?
    // TODO: Otherwise, rearrange these cases in order of frequency...
    SQLLEN ind;
    if (PyBool_Check(cell))
    {
        if (pi->ValueType != SQL_C_BIT)
            return false;
        WRITEOUT(char, outbuf, cell == Py_True, ind);
    }
    else if (PyLong_Check(cell))
    {
        if (pi->ValueType == SQL_C_SBIGINT)
        {
            WRITEOUT(long long, outbuf, PyLong_AsLongLong(cell), ind);
        }
        else if (pi->ValueType == SQL_C_NUMERIC)
        {
            // Convert a PyLong into a SQL_NUMERIC_STRUCT, without losing precision
            // or taking an unnecessary trip through character strings.
            SQL_NUMERIC_STRUCT *pNum = (SQL_NUMERIC_STRUCT*)*outbuf;
            PyObject *absVal = PyNumber_Absolute(cell);
            if (pi->DecimalDigits)
            {
                static PyObject *scaler_table[38];
                static PyObject *tenObject;

                // Need to scale by 10**pi->DecimalDigits
                if (pi->DecimalDigits > 38)
                {
                NumericOverflow:
                    RaiseErrorV(0, ProgrammingError, "Numeric overflow");
                    Py_XDECREF(absVal);
                    return false;
                }

                if (!scaler_table[pi->DecimalDigits - 1])
                {
                    if (!tenObject)
                        tenObject = PyLong_FromLong(10);
                    PyObject *scaleObj = PyLong_FromLong(pi->DecimalDigits);
                    scaler_table[pi->DecimalDigits - 1] = PyNumber_Power(tenObject, scaleObj, Py_None);
                    Py_XDECREF(scaleObj);
                }
                PyObject *scaledVal = PyNumber_Multiply(absVal, scaler_table[pi->DecimalDigits - 1]);
                Py_XDECREF(absVal);
                absVal = scaledVal;
            }
            pNum->precision = (SQLCHAR)pi->ColumnSize;
            pNum->scale = (SQLCHAR)pi->DecimalDigits;
            pNum->sign = _PyLong_Sign(cell) >= 0;
#if PY_VERSION_HEX < 0x030D0000
            if (_PyLong_AsByteArray((PyLongObject*)absVal, pNum->val, sizeof(pNum->val), 1, 0)) {
#else
            if (_PyLong_AsByteArray((PyLongObject*)absVal, pNum->val, sizeof(pNum->val), 1, 0, 1)) {
#endif
                goto NumericOverflow;
            }
            Py_XDECREF(absVal);
            *outbuf += pi->BufferLength;
            ind = sizeof(SQL_NUMERIC_STRUCT);
        }
        else
            return false;
    }
    else if (PyFloat_Check(cell))
    {
        if (pi->ValueType != SQL_C_DOUBLE)
            return false;
        WRITEOUT(double, outbuf, PyFloat_AS_DOUBLE(cell), ind);
    }
    else if (PyBytes_Check(cell))
    {
        if (pi->ValueType != SQL_C_BINARY)
            return false;
        Py_ssize_t len = PyBytes_GET_SIZE(cell);

        if (!pi->ColumnSize) // DAE
        {
            DAEParam *pParam = (DAEParam*)*outbuf;
            Py_INCREF(cell);
            pParam->cell = cell;
            pParam->maxlen = cur->cnxn->GetMaxLength(pi->ValueType);
            *outbuf += sizeof(DAEParam);
            ind = cur->cnxn->need_long_data_len ? SQL_LEN_DATA_AT_EXEC((SQLLEN)len) : SQL_DATA_AT_EXEC;
        }
        else
        {
            if (len > pi->BufferLength)
            {
                RaiseErrorV(0, ProgrammingError, "String data, right truncation: length %u buffer %u", len, pi->BufferLength);
                return false;
            }
            memcpy(*outbuf, PyBytes_AS_STRING(cell), len);
            *outbuf += pi->BufferLength;
            ind = len;
        }
    }
    else if (PyUnicode_Check(cell))
    {
        if (pi->ValueType != SQL_C_WCHAR)
            return false;

        const TextEnc& enc = cur->cnxn->unicode_enc;
        Object encoded(PyCodec_Encode(cell, enc.name, "strict"));
        if (!encoded)
            return false;

        if (enc.optenc == OPTENC_NONE && !PyBytes_CheckExact(encoded))
        {
            PyErr_Format(PyExc_TypeError, "Unicode write encoding '%s' returned unexpected data type: %s",
                         enc.name, encoded.Get()->ob_type->tp_name);
            return false;
        }

        Py_ssize_t len = PyBytes_GET_SIZE(encoded);
        if (!pi->ColumnSize)
        {
            // DAE
            DAEParam *pParam = (DAEParam*)*outbuf;
            pParam->cell = encoded.Detach();
            pParam->maxlen = cur->cnxn->GetMaxLength(pi->ValueType);
            *outbuf += sizeof(DAEParam);
            ind = cur->cnxn->need_long_data_len ? SQL_LEN_DATA_AT_EXEC((SQLLEN)len) : SQL_DATA_AT_EXEC;
        }
        else
        {
            if (len > pi->BufferLength)
            {
                RaiseErrorV(0, ProgrammingError, "String data, right truncation: length %u buffer %u", len, pi->BufferLength);
                return false;
            }
            memcpy(*outbuf, PyBytes_AS_STRING((PyObject*)encoded), len);
            *outbuf += pi->BufferLength;
            ind = len;
        }
    }
    else if (PyDateTime_Check(cell))
    {
        if (pi->ValueType != SQL_C_TYPE_TIMESTAMP)
            return false;
        SQL_TIMESTAMP_STRUCT *pts = (SQL_TIMESTAMP_STRUCT*)*outbuf;
        pts->year = PyDateTime_GET_YEAR(cell);
        pts->month = PyDateTime_GET_MONTH(cell);
        pts->day = PyDateTime_GET_DAY(cell);
        pts->hour = PyDateTime_DATE_GET_HOUR(cell);
        pts->minute = PyDateTime_DATE_GET_MINUTE(cell);
        pts->second = PyDateTime_DATE_GET_SECOND(cell);

        // Truncate the fraction according to precision
        size_t digits = min(9, pi->DecimalDigits);
        long fast_pow10[] = {1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000};
        SQLUINTEGER milliseconds = PyDateTime_DATE_GET_MICROSECOND(cell) * 1000;
        pts->fraction = milliseconds - (milliseconds % fast_pow10[9 - digits]);

        *outbuf += sizeof(SQL_TIMESTAMP_STRUCT);
        ind = sizeof(SQL_TIMESTAMP_STRUCT);
    }
    else if (PyDate_Check(cell))
    {
        if (pi->ValueType != SQL_C_TYPE_DATE)
            return false;
        SQL_DATE_STRUCT *pds = (SQL_DATE_STRUCT*)*outbuf;
        pds->year = PyDateTime_GET_YEAR(cell);
        pds->month = PyDateTime_GET_MONTH(cell);
        pds->day = PyDateTime_GET_DAY(cell);
        *outbuf += sizeof(SQL_DATE_STRUCT);
        ind = sizeof(SQL_DATE_STRUCT);
    }
    else if (PyTime_Check(cell))
    {
        if (pi->ParameterType == SQL_SS_TIME2)
        {
            if (pi->ValueType != SQL_C_BINARY)
                return false;
            SQL_SS_TIME2_STRUCT *pt2s = (SQL_SS_TIME2_STRUCT*)*outbuf;
            pt2s->hour = PyDateTime_TIME_GET_HOUR(cell);
            pt2s->minute = PyDateTime_TIME_GET_MINUTE(cell);
            pt2s->second = PyDateTime_TIME_GET_SECOND(cell);
            // This is in units of nanoseconds.
            pt2s->fraction = PyDateTime_TIME_GET_MICROSECOND(cell)*1000;
            *outbuf += sizeof(SQL_SS_TIME2_STRUCT);
            ind = sizeof(SQL_SS_TIME2_STRUCT);
        }
        else
        {
            if (pi->ValueType != SQL_C_TYPE_TIME)
                return false;
            SQL_TIME_STRUCT *pts = (SQL_TIME_STRUCT*)*outbuf;
            pts->hour = PyDateTime_TIME_GET_HOUR(cell);
            pts->minute = PyDateTime_TIME_GET_MINUTE(cell);
            pts->second = PyDateTime_TIME_GET_SECOND(cell);
            *outbuf += sizeof(SQL_TIME_STRUCT);
            ind = sizeof(SQL_TIME_STRUCT);
        }
    }
    else if (PyByteArray_Check(cell))
    {
        if (pi->ValueType != SQL_C_BINARY)
            return false;
        Py_ssize_t len = PyByteArray_GET_SIZE(cell);
        if (!pi->ColumnSize) // DAE
        {
            DAEParam *pParam = (DAEParam*)*outbuf;
            Py_INCREF(cell);
            pParam->cell = cell;
            pParam->maxlen = cur->cnxn->GetMaxLength(pi->ValueType);
            *outbuf += sizeof(DAEParam);
            ind = cur->cnxn->need_long_data_len ? SQL_LEN_DATA_AT_EXEC((SQLLEN)len) : SQL_DATA_AT_EXEC;
        }
        else
        {
            if (len > pi->BufferLength)
            {
                RaiseErrorV(0, ProgrammingError, "String data, right truncation: length %u buffer %u", len, pi->BufferLength);
                return false;
            }
            memcpy(*outbuf, PyByteArray_AS_STRING(cell), len);
            *outbuf += pi->BufferLength;
            ind = len;
        }
    }
    else if (IsInstanceForThread(cell, "uuid", "UUID", &cls) && cls)
    {
        if (pi->ValueType != SQL_C_GUID)
            return false;
        pi->BufferLength = 16;
        // Do we need to use "bytes" on a big endian machine?
        Object b(PyObject_GetAttrString(cell, "bytes_le"));
        if (!b)
            return false;
        memcpy(*outbuf, PyBytes_AS_STRING(b.Get()), sizeof(SQLGUID));
        *outbuf += pi->BufferLength;
        ind = 16;
    }
    else if (IsInstanceForThread(cell, "decimal", "Decimal", &cls) && cls)
    {
        if (pi->ValueType != SQL_C_NUMERIC)
            return false;
        // Normalise, then get sign, exponent, and digits.
        PyObject *normCell = PyObject_CallMethod(cell, "normalize", 0);
        if (!normCell)
            return false;
        PyObject *cellParts = PyObject_CallMethod(normCell, "as_tuple", 0);
        if (!cellParts)
            return false;

        Py_XDECREF(normCell);

        SQL_NUMERIC_STRUCT *pNum = (SQL_NUMERIC_STRUCT*)*outbuf;
        pNum->sign = !PyLong_AsLong(PyTuple_GET_ITEM(cellParts, 0));
        PyObject*  digits = PyTuple_GET_ITEM(cellParts, 1);
        long       exp    = PyLong_AsLong(PyTuple_GET_ITEM(cellParts, 2));
        Py_ssize_t numDigits = PyTuple_GET_SIZE(digits);

        // PyDecimal is digits * 10**exp = digits / 10**-exp
        // SQL_NUMERIC_STRUCT is val / 10**scale
        Py_ssize_t scaleDiff = pi->DecimalDigits + exp;
        if (scaleDiff < 0)
        {
            RaiseErrorV(0, ProgrammingError, "Converting decimal loses precision");
            return false;
        }

        // Append '0's to the end of the digits to effect the scaling.
        PyObject *newDigits = PyTuple_New(numDigits + scaleDiff);
        for (Py_ssize_t i = 0; i < numDigits; i++)
        {
            PyTuple_SET_ITEM(newDigits, i, PyLong_FromLong(PyNumber_AsSsize_t(PyTuple_GET_ITEM(digits, i), 0)));
        }
        for (Py_ssize_t i = numDigits; i < scaleDiff + numDigits; i++)
        {
            PyTuple_SET_ITEM(newDigits, i, PyLong_FromLong(0));
        }
        PyObject *args = Py_BuildValue("((iOi))", 0, newDigits, 0);
        PyObject *scaledDecimal = PyObject_CallObject((PyObject*)cell->ob_type, args);
        PyObject *digitLong = PyNumber_Long(scaledDecimal);

        Py_XDECREF(args);
        Py_XDECREF(scaledDecimal);
        Py_XDECREF(newDigits);
        Py_XDECREF(cellParts);

        pNum->precision = (SQLCHAR)pi->ColumnSize;
        pNum->scale = (SQLCHAR)pi->DecimalDigits;


#if PY_VERSION_HEX < 0x030D0000
        int ret = _PyLong_AsByteArray((PyLongObject*)digitLong, pNum->val, sizeof(pNum->val), 1, 0);
#else
        int ret = _PyLong_AsByteArray((PyLongObject*)digitLong, pNum->val, sizeof(pNum->val), 1, 0, 1);
#endif
        Py_XDECREF(digitLong);
        if (ret)
        {
            PyErr_Clear();
            RaiseErrorV(0, ProgrammingError, "Numeric overflow");
            return false;
        }
        *outbuf += pi->BufferLength;
        ind = sizeof(SQL_NUMERIC_STRUCT);
    }
    else if (cell == Py_None || cell == null_binary)
    {
        // REVIEW: Theoretically we could eliminate the initial call to SQLDescribeParam for
        // all columns if we had a special value for "unknown" and called SQLDescribeParam only
        // here when we hit it.  Even then, only if we don't already have previous Python
        // objects!

        *outbuf += pi->BufferLength;
        ind = SQL_NULL_DATA;
    }
    else
    {
        RaiseErrorV(0, ProgrammingError, "Unknown object type: %s",cell->ob_type->tp_name);
        return false;
    }
    *(SQLLEN*)(*outbuf) = ind;
    *outbuf += sizeof(SQLLEN);
    return true;
}


static bool GetParamType(Cursor* cur, Py_ssize_t iParam, SQLSMALLINT& type);

static void FreeInfos(ParamInfo* a, Py_ssize_t count)
{
    for (Py_ssize_t i = 0; i < count; i++)
    {
        if (a[i].allocated)
            PyMem_Free(a[i].ParameterValuePtr);
        if (a[i].ParameterType == SQL_SS_TABLE && a[i].nested)
            FreeInfos(a[i].nested, a[i].maxlength);
        Py_XDECREF(a[i].pObject);
    }
    PyMem_Free(a);
}

static bool GetNullInfo(Cursor* cur, Py_ssize_t index, ParamInfo& info)
{
    if (!GetParamType(cur, index, info.ParameterType))
        return false;

    info.ValueType     = SQL_C_DEFAULT;
    info.ColumnSize    = 1;
    info.StrLen_or_Ind = SQL_NULL_DATA;
    return true;
}

static bool GetNullBinaryInfo(Cursor* cur, Py_ssize_t index, ParamInfo& info)
{
    info.ValueType         = SQL_C_BINARY;
    info.ParameterType     = SQL_BINARY;
    info.ColumnSize        = 1;
    info.ParameterValuePtr = 0;
    info.StrLen_or_Ind     = SQL_NULL_DATA;
    return true;
}


static bool GetBytesInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, bool isTVP)
{
    // The Python 3 version that writes bytes as binary data.
    Py_ssize_t cb = PyBytes_GET_SIZE(param);

    info.ValueType  = SQL_C_BINARY;
    info.ColumnSize = isTVP ? 0 : (SQLUINTEGER)max(cb, 1);

    SQLLEN maxlength = cur->cnxn->GetMaxLength(info.ValueType);

    if (maxlength == 0 || cb <= maxlength || isTVP)
    {
        info.ParameterType     = SQL_VARBINARY;
        info.StrLen_or_Ind     = cb;
        info.BufferLength      = (SQLLEN)info.ColumnSize;
        info.ParameterValuePtr = PyBytes_AS_STRING(param);
    }
    else
    {
        // Too long to pass all at once, so we'll provide the data at execute.
        info.ParameterType     = SQL_LONGVARBINARY;
        info.StrLen_or_Ind     = cur->cnxn->need_long_data_len ? SQL_LEN_DATA_AT_EXEC((SQLLEN)cb) : SQL_DATA_AT_EXEC;
        info.ParameterValuePtr = &info;
        info.BufferLength      = sizeof(ParamInfo*);
        info.pObject           = param;
        Py_INCREF(info.pObject);
        info.maxlength = maxlength;
    }

    return true;
}


static bool GetUnicodeInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, bool isTVP)
{
    const TextEnc& enc = cur->cnxn->unicode_enc;

    info.ValueType = enc.ctype;

    Object encoded(PyCodec_Encode(param, enc.name, "strict"));
    if (!encoded)
        return false;

    if (enc.optenc == OPTENC_NONE && !PyBytes_CheckExact(encoded))
    {
        PyErr_Format(PyExc_TypeError, "Unicode write encoding '%s' returned unexpected data type: %s",
                     enc.name, encoded.Get()->ob_type->tp_name);
        return false;
    }

    Py_ssize_t cb = PyBytes_GET_SIZE(encoded);

    int denom = 1;

    if (enc.optenc == OPTENC_UTF16)
    {
        denom = 2;
    }
    else if (enc.optenc == OPTENC_UTF32)
    {
        denom = 4;
    }

    info.ColumnSize = isTVP ? 0 : (SQLUINTEGER)max(cb / denom, 1);

    info.pObject = encoded.Detach();

    SQLLEN maxlength = cur->cnxn->GetMaxLength(enc.ctype);

    if (maxlength == 0 || cb <= maxlength || isTVP)
    {
        info.ParameterType     = (enc.ctype == SQL_C_CHAR) ? SQL_VARCHAR : SQL_WVARCHAR;
        info.ParameterValuePtr = PyBytes_AS_STRING(info.pObject);
        info.BufferLength      = (SQLINTEGER)cb;
        info.StrLen_or_Ind     = (SQLINTEGER)cb;
    }
    else
    {
        // Too long to pass all at once, so we'll provide the data at execute.
        info.ParameterType     = (enc.ctype == SQL_C_CHAR) ? SQL_LONGVARCHAR : SQL_WLONGVARCHAR;
        info.ParameterValuePtr = &info;
        info.BufferLength      = sizeof(ParamInfo*);
        info.StrLen_or_Ind     = cur->cnxn->need_long_data_len ? SQL_LEN_DATA_AT_EXEC((SQLINTEGER)cb) : SQL_DATA_AT_EXEC;
        info.maxlength = maxlength;
    }

    return true;
}

static bool GetBooleanInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info)
{
    info.ValueType         = SQL_C_BIT;
    info.ParameterType     = SQL_BIT;
    info.StrLen_or_Ind     = 1;
    info.Data.ch           = (unsigned char)(param == Py_True ? 1 : 0);
    info.ParameterValuePtr = &info.Data.ch;
    return true;
}

static bool GetDateTimeInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info)
{
    info.Data.timestamp.year   = (SQLSMALLINT) PyDateTime_GET_YEAR(param);
    info.Data.timestamp.month  = (SQLUSMALLINT)PyDateTime_GET_MONTH(param);
    info.Data.timestamp.day    = (SQLUSMALLINT)PyDateTime_GET_DAY(param);
    info.Data.timestamp.hour   = (SQLUSMALLINT)PyDateTime_DATE_GET_HOUR(param);
    info.Data.timestamp.minute = (SQLUSMALLINT)PyDateTime_DATE_GET_MINUTE(param);
    info.Data.timestamp.second = (SQLUSMALLINT)PyDateTime_DATE_GET_SECOND(param);

    // SQL Server chokes if the fraction has more data than the database supports.  We expect other databases to be the
    // same, so we reduce the value to what the database supports.  http://support.microsoft.com/kb/263872

    int precision = ((Connection*)cur->cnxn)->datetime_precision - 20; // (20 includes a separating period)
    if (precision <= 0)
    {
        info.Data.timestamp.fraction = 0;
    }
    else
    {
        info.Data.timestamp.fraction = (SQLUINTEGER)(PyDateTime_DATE_GET_MICROSECOND(param) * 1000); // 1000 == micro -> nano

        // (How many leading digits do we want to keep?  With SQL Server 2005, this should be 3: 123000000)
        int keep = (int)pow(10.0, 9-min(9, precision));
        info.Data.timestamp.fraction = info.Data.timestamp.fraction / keep * keep;
        info.DecimalDigits = (SQLSMALLINT)precision;
    }

    info.ValueType         = SQL_C_TIMESTAMP;
    info.ParameterType     = SQL_TIMESTAMP;
    info.ColumnSize        = (SQLUINTEGER)((Connection*)cur->cnxn)->datetime_precision;
    info.StrLen_or_Ind     = sizeof(TIMESTAMP_STRUCT);
    info.ParameterValuePtr = &info.Data.timestamp;
    return true;
}

static bool GetDateInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info)
{
    info.Data.date.year  = (SQLSMALLINT) PyDateTime_GET_YEAR(param);
    info.Data.date.month = (SQLUSMALLINT)PyDateTime_GET_MONTH(param);
    info.Data.date.day   = (SQLUSMALLINT)PyDateTime_GET_DAY(param);

    info.ValueType         = SQL_C_TYPE_DATE;
    info.ParameterType     = SQL_TYPE_DATE;
    info.ColumnSize        = 10;
    info.ParameterValuePtr = &info.Data.date;
    info.StrLen_or_Ind     = sizeof(DATE_STRUCT);
    return true;
}

static bool GetTimeInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info)
{
    info.Data.time.hour   = (SQLUSMALLINT)PyDateTime_TIME_GET_HOUR(param);
    info.Data.time.minute = (SQLUSMALLINT)PyDateTime_TIME_GET_MINUTE(param);
    info.Data.time.second = (SQLUSMALLINT)PyDateTime_TIME_GET_SECOND(param);

    info.ValueType         = SQL_C_TYPE_TIME;
    info.ParameterType     = SQL_TYPE_TIME;
    info.ColumnSize        = 8;
    info.ParameterValuePtr = &info.Data.time;
    info.StrLen_or_Ind     = sizeof(TIME_STRUCT);
    return true;
}


inline bool NeedsBigInt(long long ll)
{
    // NOTE: Smallest 32-bit int should be -214748368 but the MS compiler v.1900 AMD64
    // says that (10 < -2147483648).  Perhaps I miscalculated the minimum?
    return ll < -2147483647 || ll > 2147483647;
}

static bool GetLongInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, bool isTVP)
{
    // Since some drivers like Access don't support BIGINT, we use INTEGER when possible.
    // Unfortunately this may mean that we end up with two execution plans for the same SQL.
    // We could use SQLDescribeParam but that's kind of expensive.

    long long value = PyLong_AsLongLong(param);
    if (PyErr_Occurred())
        return false;

    if (isTVP || NeedsBigInt(value))
    {
        info.Data.i64          = (INT64)value;
        info.ValueType         = SQL_C_SBIGINT;
        info.ParameterType     = SQL_BIGINT;
        info.ParameterValuePtr = &info.Data.i64;
        info.StrLen_or_Ind     = 8;
    }
    else
    {
        info.Data.i32          = (int)value;
        info.ValueType         = SQL_C_LONG;
        info.ParameterType     = SQL_INTEGER;
        info.ParameterValuePtr = &info.Data.i32;
        info.StrLen_or_Ind     = 4;
    }
    return true;
}

static bool GetFloatInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info)
{
    // Python floats are usually numeric values, but can also be "Infinity" or "NaN".
    // https://docs.python.org/3/library/functions.html#float
    // PyFloat_AsDouble() does not generate an error for Infinity/NaN, and it is not
    // easy to check for those values.  Typically, the database will reject them.
    double value = PyFloat_AsDouble(param);
    if (PyErr_Occurred())
        return false;

    info.Data.dbl          = value;
    info.ValueType         = SQL_C_DOUBLE;
    info.ParameterType     = SQL_DOUBLE;
    info.ParameterValuePtr = &info.Data.dbl;
    info.ColumnSize = 15;
    info.StrLen_or_Ind = sizeof(double);
    return true;
}

static char* CreateDecimalString(long sign, PyObject* digits, long exp)
{
    // Allocate an ASCII string containing the given decimal.

    long count = (long)PyTuple_GET_SIZE(digits);

    char* pch;
    long len;

    if (exp >= 0)
    {
        // (1 2 3) exp = 2 --> '12300'

        len = sign + count + exp + 1; // 1: NULL
        pch = (char*)PyMem_Malloc((size_t)len);
        if (pch)
        {
            char* p = pch;
            if (sign)
                *p++ = '-';
            for (long i = 0; i < count; i++)
                *p++ = (char)('0' + PyLong_AS_LONG(PyTuple_GET_ITEM(digits, i)));
            for (long i = 0; i < exp; i++)
                *p++ = '0';
            *p = 0;
        }
    }
    else if (-exp < count)
    {
        // (1 2 3) exp = -2 --> 1.23 : prec = 3, scale = 2

        len = sign + count + 2; // 2: decimal + NULL
        pch = (char*)PyMem_Malloc((size_t)len);
        if (pch)
        {
            char* p = pch;
            if (sign)
                *p++ = '-';
            int i = 0;
            for (; i < (count + exp); i++)
                *p++ = (char)('0' + PyLong_AS_LONG(PyTuple_GET_ITEM(digits, i)));
            *p++ = '.';
            for (; i < count; i++)
                *p++ = (char)('0' + PyLong_AS_LONG(PyTuple_GET_ITEM(digits, i)));
            *p++ = 0;
        }
    }
    else
    {
        // (1 2 3) exp = -5 --> 0.00123 : prec = 5, scale = 5

        len = sign + -exp + 3; // 3: leading zero + decimal + NULL

        pch = (char*)PyMem_Malloc((size_t)len);
        if (pch)
        {
            char* p = pch;
            if (sign)
                *p++ = '-';
            *p++ = '0';
            *p++ = '.';

            for (int i = 0; i < -(exp + count); i++)
                *p++ = '0';

            for (int i = 0; i < count; i++)
                *p++ = (char)('0' + PyLong_AS_LONG(PyTuple_GET_ITEM(digits, i)));
            *p++ = 0;
        }
    }

    assert(pch == 0 || (int)(strlen(pch) + 1) == len);

    return pch;
}

static bool GetUUIDInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, PyObject* uuid_type)
{
    // uuid_type: This is a new reference that we are responsible for freeing.
    Object tmp(uuid_type);

    info.ValueType = SQL_C_GUID;
    info.ParameterType = SQL_GUID;
    info.ColumnSize = 16;

    info.allocated = true;
    info.ParameterValuePtr = PyMem_Malloc(sizeof(SQLGUID));
    if (!info.ParameterValuePtr)
    {
        PyErr_NoMemory();
        return false;
    }

    // Do we need to use "bytes" on a big endian machine?
    Object b(PyObject_GetAttrString(param, "bytes_le"));
    if (!b)
        return false;
    memcpy(info.ParameterValuePtr, PyBytes_AS_STRING(b.Get()), sizeof(SQLGUID));
    info.StrLen_or_Ind = sizeof(SQLGUID);
    return true;
}


static bool GetDecimalInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, PyObject* decimal_type)
{
    // decimal_type: This is a new reference that we are responsible for freeing.
    Object tmp(decimal_type);

    // The NUMERIC structure never works right with SQL Server and probably a lot of other drivers.  We'll bind as a
    // string.  Unfortunately, the Decimal class doesn't seem to have a way to force it to return a string without
    // exponents, so we'll have to build it ourselves.

    Object t(PyObject_CallMethod(param, "as_tuple", 0));
    if (!t)
        return false;

    long       sign   = PyLong_AsLong(PyTuple_GET_ITEM(t.Get(), 0));
    PyObject*  digits = PyTuple_GET_ITEM(t.Get(), 1);
    long       exp    = PyLong_AsLong(PyTuple_GET_ITEM(t.Get(), 2));

    Py_ssize_t count = PyTuple_GET_SIZE(digits);

    info.ValueType     = SQL_C_CHAR;
    info.ParameterType = SQL_NUMERIC;

    if (exp >= 0)
    {
        // (1 2 3) exp = 2 --> '12300'

        info.ColumnSize    = (SQLUINTEGER)count + exp;
        info.DecimalDigits = 0;

    }
    else if (-exp <= count)
    {
        // (1 2 3) exp = -2 --> 1.23 : prec = 3, scale = 2
        info.ColumnSize    = (SQLUINTEGER)count;
        info.DecimalDigits = (SQLSMALLINT)-exp;
    }
    else
    {
        // (1 2 3) exp = -5 --> 0.00123 : prec = 5, scale = 5
        info.ColumnSize    = (SQLUINTEGER)(-exp);
        info.DecimalDigits = (SQLSMALLINT)info.ColumnSize;
    }

    assert(info.ColumnSize >= (SQLULEN)info.DecimalDigits);

    info.ParameterValuePtr = CreateDecimalString(sign, digits, exp);
    if (!info.ParameterValuePtr)
    {
        PyErr_NoMemory();
        return false;
    }
    info.allocated = true;

    info.StrLen_or_Ind = (SQLINTEGER)strlen((char*)info.ParameterValuePtr);

    return true;
}


static bool GetByteArrayInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, bool isTVP)
{
    info.ValueType = SQL_C_BINARY;

    Py_ssize_t cb = PyByteArray_Size(param);

    SQLLEN maxlength = cur->cnxn->GetMaxLength(info.ValueType);

    if (maxlength == 0 || cb <= maxlength || isTVP)
    {
        info.ParameterType     = SQL_VARBINARY;
        info.ParameterValuePtr = (SQLPOINTER)PyByteArray_AsString(param);
        info.BufferLength      = (SQLINTEGER)cb;
        info.ColumnSize        = isTVP?0:(SQLUINTEGER)max(cb, 1);
        info.StrLen_or_Ind     = (SQLINTEGER)cb;
    }
    else
    {
        info.ParameterType     = SQL_LONGVARBINARY;
        info.ParameterValuePtr = &info;
        info.BufferLength      = sizeof(ParamInfo*);
        info.ColumnSize        = (SQLUINTEGER)cb;
        info.StrLen_or_Ind     = cur->cnxn->need_long_data_len ? SQL_LEN_DATA_AT_EXEC((SQLLEN)cb) : SQL_DATA_AT_EXEC;
        info.pObject = param;
        Py_INCREF(info.pObject);
        info.maxlength = maxlength;
    }
    return true;
}


// TVP
static bool GetTableInfo(Cursor *cur, Py_ssize_t index, PyObject* param, ParamInfo& info)
{
    // This is used for SQL Server's "table valued parameters" or TVPs.
    int nskip = 0;
    Py_ssize_t nrows = PySequence_Size(param);
    if (nrows > 0)
    {
        PyObject *cell0 = PySequence_GetItem(param, 0);
        if (cell0 == NULL)
        {
            return false;
        }
        if (PyBytes_Check(cell0) || PyUnicode_Check(cell0))
        {
            nskip++;
            if (nrows > 1)
            {
                PyObject *cell1 = PySequence_GetItem(param, 1);
                nskip += (PyBytes_Check(cell1) || PyUnicode_Check(cell1));
                Py_XDECREF(cell1);
            }
        }
        Py_XDECREF(cell0);
    }
    nrows -= nskip;

    if (!nskip)
    {
        // Need to describe in order to fill in IPD with the TVP's type name, because user has
        // not provided it
        SQLSMALLINT tvptype;
        SQLDescribeParam(cur->hstmt, index + 1, &tvptype, 0, 0, 0);
    }

    info.pObject = param;
    Py_INCREF(param);
    info.ValueType = SQL_C_BINARY;
    info.ParameterType = SQL_SS_TABLE;
    info.ColumnSize = nrows;
    info.DecimalDigits = 0;
    info.ParameterValuePtr = &info;
    info.BufferLength = 0;
    info.curTvpRow = nskip;
    info.StrLen_or_Ind = SQL_DATA_AT_EXEC;
    info.allocated = false;
    return true;
}

bool GetParameterInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, bool isTVP)
{
    // Determines the type of SQL parameter that will be used for this parameter based on the Python data type.
    //
    // Populates `info`.

    if (param == Py_None)
        return GetNullInfo(cur, index, info);

    if (param == null_binary)
        return GetNullBinaryInfo(cur, index, info);

    if (PyBytes_Check(param))
        return GetBytesInfo(cur, index, param, info, isTVP);

    if (PyUnicode_Check(param))
        return GetUnicodeInfo(cur, index, param, info, isTVP);

    if (PyBool_Check(param))
        return GetBooleanInfo(cur, index, param, info);

    if (PyDateTime_Check(param))
        return GetDateTimeInfo(cur, index, param, info);

    if (PyDate_Check(param))
        return GetDateInfo(cur, index, param, info);

    if (PyTime_Check(param))
        return GetTimeInfo(cur, index, param, info);

    if (PyLong_Check(param))
        return GetLongInfo(cur, index, param, info, isTVP);

    if (PyFloat_Check(param))
        return GetFloatInfo(cur, index, param, info);

    if (PyByteArray_Check(param))
        return GetByteArrayInfo(cur, index, param, info, isTVP);

    PyObject* cls = 0;
    if (!IsInstanceForThread(param, "decimal", "Decimal", &cls))
        return false;

    if (cls != 0)
        return GetDecimalInfo(cur, index, param, info, cls);

    if (!IsInstanceForThread(param, "uuid", "UUID", &cls))
        return false;

    if (cls != 0)
        return GetUUIDInfo(cur, index, param, info, cls);

    if (PySequence_Check(param))
        return GetTableInfo(cur, index, param, info);

    RaiseErrorV("HY105", ProgrammingError, "Invalid parameter type.  param-index=%zd param-type=%s", index, Py_TYPE(param)->tp_name);
    return false;
}

static bool getObjectValue(PyObject *pObject, long& nValue)
{
  if (pObject == NULL)
    return false;

  if (PyLong_Check(pObject))
  {
    nValue = PyLong_AsLong(pObject);
    return true;
  }

  return false;
}

static long getSequenceValue(PyObject *pSequence, Py_ssize_t nIndex, long nDefault, bool &bChanged)
{
  PyObject *obj;
  long v = nDefault;

  obj = PySequence_GetItem(pSequence, nIndex);
  if (obj != NULL)
  {
    if (getObjectValue(obj, v))
      bChanged = true;
  }
  Py_CLEAR(obj);

  return v;
}

/**
 * UpdateParamInfo updates the current columnsizes with the information provided
 * by a set from the client code, to manually override values returned by SQLDescribeParam()
 * which can be wrong in case of SQL Server statements.
 *
 * sparhawk@gmx.at (Gerhard Gruber)
 */
static bool UpdateParamInfo(Cursor* pCursor, Py_ssize_t nIndex, ParamInfo *pInfo)
{
  if (pCursor->inputsizes == NULL || nIndex >= PySequence_Length(pCursor->inputsizes))
    return false;

  PyObject *desc = PySequence_GetItem(pCursor->inputsizes, nIndex);
  if (desc == NULL)
    return false;

  bool rc = false;
  long v;
  bool clearError = true;

  // If the error was already set before we entered here, it is not from us, so we leave it alone.
  if (PyErr_Occurred())
    clearError = false;

  // integer - sets colsize
  // type object - sets sqltype (mapping between Python and SQL types is not 1:1 so it may not always work)
  // Consider: sequence of (colsize, sqltype, scale)
  if (getObjectValue(desc, v))
  {
    pInfo->ColumnSize = (SQLULEN)v;
    rc = true;
  }
  else if (PySequence_Check(desc))
  {
    pInfo->ParameterType = (SQLSMALLINT)getSequenceValue(desc, 0, (long)pInfo->ParameterType, rc);
    pInfo->ColumnSize = (SQLUINTEGER)getSequenceValue(desc, 1, (long)pInfo->ColumnSize, rc);
    pInfo->DecimalDigits = (SQLSMALLINT)getSequenceValue(desc, 2, (long)pInfo->ColumnSize, rc);
  }

  Py_CLEAR(desc);

  // If the user didn't provide the full array (in case he gave us an array), the above code would
  // set an internal error on the cursor object, as we try to read three values from an array
  // which may not have as many. This is ok, because we don't really care if the array is not completely
  // specified, so we clear the error in case it comes from this. If the error was already present before that
  // we keep it, so the user can handle it.
  if (clearError)
    PyErr_Clear();

  return rc;
}

bool BindParameter(Cursor* cur, Py_ssize_t index, ParamInfo& info)
{
    SQLSMALLINT sqltype = info.ParameterType;
    SQLULEN colsize = info.ColumnSize;
    SQLSMALLINT scale = info.DecimalDigits;

    if (UpdateParamInfo(cur, index, &info))
    {
    // Reload in case it has changed.
    colsize = info.ColumnSize;
    sqltype = info.ParameterType;
    scale = info.DecimalDigits;
    }

  TRACE("BIND: param=%ld ValueType=%d (%s) ParameterType=%d (%s) ColumnSize=%ld DecimalDigits=%d BufferLength=%ld *pcb=%ld\n",
          (index+1), info.ValueType, CTypeName(info.ValueType), sqltype, SqlTypeName(sqltype), colsize,
          scale, info.BufferLength, info.StrLen_or_Ind);

    SQLRETURN ret = -1;
    Py_BEGIN_ALLOW_THREADS
    ret = SQLBindParameter(cur->hstmt, (SQLUSMALLINT)(index + 1), SQL_PARAM_INPUT,
        info.ValueType, sqltype, colsize, scale, sqltype == SQL_SS_TABLE ? 0 : info.ParameterValuePtr, info.BufferLength, &info.StrLen_or_Ind);
    Py_END_ALLOW_THREADS;

    if (GetConnection(cur)->hdbc == SQL_NULL_HANDLE)
    {
        // The connection was closed by another thread in the ALLOW_THREADS block above.
        RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
        return false;
    }

    if (!SQL_SUCCEEDED(ret))
    {
        RaiseErrorFromHandle(cur->cnxn, "SQLBindParameter", GetConnection(cur)->hdbc, cur->hstmt);
        return false;
    }

    // This is a TVP. Enter and bind its parameters, allocate descriptors for its columns (all as DAE)
    if (sqltype == SQL_SS_TABLE)
    {
        Py_ssize_t nrows = PySequence_Size(info.pObject);
        if (nrows > 0)
        {
            PyObject *cell0 = PySequence_GetItem(info.pObject, 0);
            Py_XDECREF(cell0);
            if (PyBytes_Check(cell0) || PyUnicode_Check(cell0))
            {
                SQLHDESC desc;
                PyObject *tvpname = PyCodec_Encode(cell0, "UTF-16LE", 0);
                SQLGetStmtAttr(cur->hstmt, SQL_ATTR_IMP_PARAM_DESC, &desc, 0, 0);
                SQLSetDescFieldW(desc, index + 1, SQL_CA_SS_TYPE_NAME, (SQLPOINTER)PyBytes_AsString(tvpname), PyBytes_Size(tvpname));
                Py_XDECREF(tvpname);

                if (nrows > 1)
                {
                    PyObject *cell1 = PySequence_GetItem(info.pObject, 1);
                    Py_XDECREF(cell1);
                    if (PyBytes_Check(cell1) || PyUnicode_Check(cell1))
                    {
                        PyObject *tvpschema = PyCodec_Encode(cell1, "UTF-16LE", 0);
                        SQLSetDescFieldW(desc, index + 1, SQL_CA_SS_SCHEMA_NAME, (SQLPOINTER)PyBytes_AsString(tvpschema), PyBytes_Size(tvpschema));
                        Py_XDECREF(tvpschema);
                    }
                }
            }
        }

        SQLHDESC desc;
        SQLGetStmtAttr(cur->hstmt, SQL_ATTR_APP_PARAM_DESC, &desc, 0, 0);
        SQLSetDescField(desc, index + 1, SQL_DESC_DATA_PTR, (SQLPOINTER)info.ParameterValuePtr, 0);

        int err = 0;
        ret = SQLSetStmtAttr(cur->hstmt, SQL_SOPT_SS_PARAM_FOCUS, (SQLPOINTER)(index + 1), SQL_IS_INTEGER);
        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cur->cnxn, "SQLSetStmtAttr", GetConnection(cur)->hdbc, cur->hstmt);
            return false;
        }

        Py_ssize_t i = PySequence_Size(info.pObject) - info.ColumnSize;
        Py_ssize_t ncols = 0;
        while (i >= 0 && i < PySequence_Size(info.pObject))
        {
            PyObject *row = PySequence_GetItem(info.pObject, i);
            Py_XDECREF(row);
            if (!PySequence_Check(row))
            {
                RaiseErrorV(0, ProgrammingError, "A TVP's rows must be Sequence objects.");
                err = 1;
                break;
            }
            if (ncols && ncols != PySequence_Size(row))
            {
                RaiseErrorV(0, ProgrammingError, "A TVP's rows must all be the same size.");
                err = 1;
                break;
            }
            ncols = PySequence_Size(row);
            i++;
        }
        if (!ncols)
        {
            // TVP has no columns --- is null
            info.nested = 0;
            info.StrLen_or_Ind = SQL_DEFAULT_PARAM;
        }
        else
        {
            PyObject *row = PySequence_GetItem(info.pObject, PySequence_Size(info.pObject) - info.ColumnSize);
            Py_XDECREF(row);

            info.nested = (ParamInfo*)PyMem_Malloc(ncols * sizeof(ParamInfo));
            info.maxlength = ncols;
            memset(info.nested, 0, ncols * sizeof(ParamInfo));

            for(i=0;i<ncols;i++)
            {
                // Bind the TVP's columns --- all need to use DAE
                PyObject *param = PySequence_GetItem(row, i);
                Py_XDECREF(param);
                GetParameterInfo(cur, i, param, info.nested[i], true);
                info.nested[i].BufferLength = info.nested[i].StrLen_or_Ind;
                info.nested[i].StrLen_or_Ind = SQL_DATA_AT_EXEC;

                Py_BEGIN_ALLOW_THREADS
                ret = SQLBindParameter(cur->hstmt, (SQLUSMALLINT)(i + 1), SQL_PARAM_INPUT,
                    info.nested[i].ValueType, info.nested[i].ParameterType,
                    info.nested[i].ColumnSize, info.nested[i].DecimalDigits,
                    info.nested + i, info.nested[i].BufferLength, &info.nested[i].StrLen_or_Ind);
                Py_END_ALLOW_THREADS;
                if (GetConnection(cur)->hdbc == SQL_NULL_HANDLE)
                {
                    // The connection was closed by another thread in the ALLOW_THREADS block above.
                    RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
                    return false;
                }

                if (!SQL_SUCCEEDED(ret))
                {
                    RaiseErrorFromHandle(cur->cnxn, "SQLBindParameter", GetConnection(cur)->hdbc, cur->hstmt);
                    return false;
                }
            }
        }

        ret = SQLSetStmtAttr(cur->hstmt, SQL_SOPT_SS_PARAM_FOCUS, 0, SQL_IS_INTEGER);
        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cur->cnxn, "SQLSetStmtAttr", GetConnection(cur)->hdbc, cur->hstmt);
            return false;
        }

        if (err)
            return false;
    }

    return true;
}


void FreeParameterData(Cursor* cur)
{
    // Unbinds the parameters and frees the parameter buffer.

    if (cur->paramInfos)
    {
        // MS ODBC will crash if we use an HSTMT after the HDBC has been freed.
        if (cur->cnxn->hdbc != SQL_NULL_HANDLE)
        {
            Py_BEGIN_ALLOW_THREADS
            SQLFreeStmt(cur->hstmt, SQL_RESET_PARAMS);
            Py_END_ALLOW_THREADS
        }

        FreeInfos(cur->paramInfos, cur->paramcount);
        cur->paramInfos = 0;
    }
}

void FreeParameterInfo(Cursor* cur)
{
    // Internal function to free just the cached parameter information.  This is not used by the general cursor code
    // since this information is also freed in the less granular free_results function that clears everything.

    Py_XDECREF(cur->pPreparedSQL);
    PyMem_Free(cur->paramtypes);
    cur->pPreparedSQL = 0;
    cur->paramtypes   = 0;
    cur->paramcount   = 0;
}

bool Prepare(Cursor* cur, PyObject* pSql)
{
    //
    // Prepare the SQL if necessary.
    //
    if (pSql != cur->pPreparedSQL)
    {
        FreeParameterInfo(cur);

        SQLRETURN ret = 0;
        SQLSMALLINT cParamsT = 0;
        const char* szErrorFunc = "SQLPrepare";

        const TextEnc* penc;
        penc = &cur->cnxn->unicode_enc;

        Object query(penc->Encode(pSql));
        if (!query)
            return 0;

        bool isWide = (penc->ctype == SQL_C_WCHAR);

        const char* pch = PyBytes_AS_STRING(query.Get());
        SQLINTEGER  cch = (SQLINTEGER)(PyBytes_GET_SIZE(query.Get()) / (isWide ? sizeof(uint16_t) : 1));

        TRACE("SQLPrepare(%s)\n", pch);

        Py_BEGIN_ALLOW_THREADS
        if (isWide)
            ret = SQLPrepareW(cur->hstmt, (SQLWCHAR*)pch, cch);
        else
            ret = SQLPrepare(cur->hstmt, (SQLCHAR*)pch, cch);
        if (SQL_SUCCEEDED(ret))
        {
            szErrorFunc = "SQLNumParams";
            ret = SQLNumParams(cur->hstmt, &cParamsT);
        }
        Py_END_ALLOW_THREADS

        if (cur->cnxn->hdbc == SQL_NULL_HANDLE)
        {
            // The connection was closed by another thread in the ALLOW_THREADS block above.
            RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
            return false;
        }

        if (!SQL_SUCCEEDED(ret))
        {
            RaiseErrorFromHandle(cur->cnxn, szErrorFunc, GetConnection(cur)->hdbc, cur->hstmt);
            return false;
        }

        cur->paramcount = (int)cParamsT;

        cur->pPreparedSQL = pSql;
        Py_INCREF(cur->pPreparedSQL);
    }
    return true;
}

bool PrepareAndBind(Cursor* cur, PyObject* pSql, PyObject* original_params, bool skip_first)
{
    //
    // Normalize the parameter variables.
    //

    // Since we may replace parameters (we replace objects with Py_True/Py_False when writing to a bit/bool column),
    // allocate an array and use it instead of the original sequence

    int        params_offset = skip_first ? 1 : 0;
    Py_ssize_t cParams       = original_params == 0 ? 0 : PySequence_Length(original_params) - params_offset;

    if (!Prepare(cur, pSql))
        return false;

    if (cParams != cur->paramcount)
    {
        RaiseErrorV(0, ProgrammingError, "The SQL contains %d parameter markers, but %d parameters were supplied",
                    cur->paramcount, cParams);
        return false;
    }

    cur->paramInfos = (ParamInfo*)PyMem_Malloc(sizeof(ParamInfo) * cParams);
    if (cur->paramInfos == 0)
    {
        PyErr_NoMemory();
        return 0;
    }
    memset(cur->paramInfos, 0, sizeof(ParamInfo) * cParams);

    // Since you can't call SQLDesribeParam *after* calling SQLBindParameter, we'll loop through all of the
    // GetParameterInfos first, then bind.

    for (Py_ssize_t i = 0; i < cParams; i++)
    {
        Object param(PySequence_GetItem(original_params, i + params_offset));
        if (!GetParameterInfo(cur, i, param, cur->paramInfos[i], false))
        {
            FreeInfos(cur->paramInfos, cParams);
            cur->paramInfos = 0;
            return false;
        }
    }

    for (Py_ssize_t i = 0; i < cParams; i++)
    {
        if (!BindParameter(cur, i, cur->paramInfos[i]))
        {
            FreeInfos(cur->paramInfos, cParams);
            cur->paramInfos = 0;
            return false;
        }
    }

    return true;
}


bool ExecuteMulti(Cursor* cur, PyObject* pSql, PyObject* paramArrayObj)
{
    bool ret = true;
    char *szLastFunction = 0;
    SQLRETURN rc = SQL_SUCCESS;
    if (!Prepare(cur, pSql))
        return false;

    if (!(cur->paramInfos = (ParamInfo*)PyMem_Malloc(sizeof(ParamInfo) * cur->paramcount)))
    {
        PyErr_NoMemory();
        return false;
    }
    memset(cur->paramInfos, 0, sizeof(ParamInfo) * cur->paramcount);

    // Wouldn't hurt to free threads here?  Or is this fast enough because it is local?

  // Describe each parameter (SQL type) in preparation for allocation of paramset array
    for (Py_ssize_t i = 0; i < cur->paramcount; i++)
    {
        SQLSMALLINT nullable;
        if (!SQL_SUCCEEDED(SQLDescribeParam(cur->hstmt, i + 1, &(cur->paramInfos[i].ParameterType),
            &cur->paramInfos[i].ColumnSize, &cur->paramInfos[i].DecimalDigits,
            &nullable)))
        {
            // Default to a medium-length varchar if describing the parameter didn't work
            cur->paramInfos[i].ParameterType = SQL_VARCHAR;
            cur->paramInfos[i].ColumnSize = 255;
            cur->paramInfos[i].DecimalDigits = 0;
        }

    // This supports overriding of input sizes via setinputsizes
    // See issue 380
    // The logic is duplicated from BindParameter
    UpdateParamInfo(cur, i, &cur->paramInfos[i]);
  }

    PyObject *rowseq = PySequence_Fast(paramArrayObj, "Parameter array must be a sequence.");
    if (!rowseq)
    {
    ErrorRet1:
        if (cur->paramInfos)
            FreeInfos(cur->paramInfos, cur->paramcount);
        cur->paramInfos = 0;
        return false;
    }
    Py_ssize_t rowcount = PySequence_Fast_GET_SIZE(rowseq);
    PyObject **rowptr = PySequence_Fast_ITEMS(rowseq);

    Py_ssize_t r = 0;
    while ( r < rowcount )
    {
        // Scan current row to determine C types
        PyObject *currow = *rowptr++;
        // REVIEW: This check is not needed - PySequence_Fast below is sufficient.
        if (!PyTuple_Check(currow) && !PyList_Check(currow) && !Row_Check(currow))
        {
            RaiseErrorV(0, PyExc_TypeError, "Params must be in a list, tuple, or Row");
        ErrorRet2:
            Py_XDECREF(rowseq);
            goto ErrorRet1;
        }
        PyObject *colseq = PySequence_Fast(currow, "Row must be a sequence.");
        if (!colseq)
        {
            goto ErrorRet2;
        }
        if (PySequence_Fast_GET_SIZE(colseq) != cur->paramcount)
        {
            RaiseErrorV(0, ProgrammingError, "Expected %u parameters, supplied %u", cur->paramcount, PySequence_Fast_GET_SIZE(colseq));
        ErrorRet3:
            Py_XDECREF(colseq);
            goto ErrorRet2;
        }
        PyObject **cells = PySequence_Fast_ITEMS(colseq);

        // REVIEW: We need a better description of what is going on here.  Why is it OK to pass
        // a fake bindptr to SQLBindParameter.

        // Start at a non-zero offset to prevent null pointer detection.
        char *bindptr = (char*)16;

        Py_ssize_t i = 0;
        for (; i < cur->paramcount; i++)
        {
            if (!DetectCType(cells[i], &cur->paramInfos[i]))
            {
                goto ErrorRet3;
            }

            if (!SQL_SUCCEEDED(SQLBindParameter(cur->hstmt, i + 1, SQL_PARAM_INPUT, cur->paramInfos[i].ValueType,
                cur->paramInfos[i].ParameterType, cur->paramInfos[i].ColumnSize, cur->paramInfos[i].DecimalDigits,
                bindptr, cur->paramInfos[i].BufferLength, (SQLLEN*)(bindptr + cur->paramInfos[i].BufferLength))))
            {
                RaiseErrorFromHandle(cur->cnxn, "SQLBindParameter", GetConnection(cur)->hdbc, cur->hstmt);
            ErrorRet4:
                SQLFreeStmt(cur->hstmt, SQL_RESET_PARAMS);
                goto ErrorRet3;
            }
            if (cur->paramInfos[i].ValueType == SQL_C_NUMERIC)
            {
                SQLHDESC desc;
                SQLGetStmtAttr(cur->hstmt, SQL_ATTR_APP_PARAM_DESC, &desc, 0, 0);
                SQLSetDescField(desc, i + 1, SQL_DESC_TYPE, (SQLPOINTER)SQL_C_NUMERIC, 0);
                SQLSetDescField(desc, i + 1, SQL_DESC_PRECISION, (SQLPOINTER)cur->paramInfos[i].ColumnSize, 0);
                SQLSetDescField(desc, i + 1, SQL_DESC_SCALE, (SQLPOINTER)(uintptr_t)cur->paramInfos[i].DecimalDigits, 0);
                SQLSetDescField(desc, i + 1, SQL_DESC_DATA_PTR, bindptr, 0);
            }
            bindptr += cur->paramInfos[i].BufferLength + sizeof(SQLLEN);
        }

        Py_ssize_t rowlen = bindptr - (char*)16;
        // Assume parameters are homogeneous between rows in the common case, to avoid
        // another rescan for determining the array height.
        // Subtract number of rows processed as an upper bound.
        if (!(cur->paramArray = (unsigned char*)PyMem_Malloc(rowlen * (rowcount - r))))
        {
            PyErr_NoMemory();
            goto ErrorRet4;
        }

        unsigned char *pParamDat = cur->paramArray;
        Py_ssize_t rows_converted = 0;

        ParamInfo *pi;
        for (;;)
        {
            // Column loop.
            pi = &cur->paramInfos[0];
            for (int c = 0; c < cur->paramcount; c++, pi++)
            {
                if (!PyToCType(cur, &pParamDat, *cells++, pi))
                {
                    // "schema change" or conversion error. Try again on next batch.
                    rowptr--;
                    Py_XDECREF(colseq);
                    colseq = 0;
                    // Finish this batch of rows and attempt to execute before starting another.
                    goto DoExecute;
                }
            }
            rows_converted++;
            Py_XDECREF(colseq);
            colseq = 0;
            r++;
            if ( r >= rowcount )
            {
                break;
            }
            currow = *rowptr++;
            colseq = PySequence_Fast(currow, "Row must be a sequence.");
            if (!colseq)
            {
            ErrorRet5:
                PyMem_Free(cur->paramArray);
                cur->paramArray = 0;
                goto ErrorRet4;
            }
            if (PySequence_Fast_GET_SIZE(colseq) != cur->paramcount)
            {
                RaiseErrorV(0, ProgrammingError, "Expected %u parameters, supplied %u", cur->paramcount, PySequence_Fast_GET_SIZE(colseq));
                Py_XDECREF(colseq);
                goto ErrorRet5;
            }
            cells = PySequence_Fast_ITEMS(colseq);
        }
    DoExecute:
        if (!rows_converted || PyErr_Occurred())
        {
            if (!PyErr_Occurred())
                RaiseErrorV(0, ProgrammingError, "No suitable conversion for one or more parameters.");
            goto ErrorRet5;
        }

        SQLULEN bop = (SQLULEN)(cur->paramArray) - 16;
        if (!SQL_SUCCEEDED(SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)rowlen, SQL_IS_UINTEGER)))
        {
            RaiseErrorFromHandle(cur->cnxn, "SQLSetStmtAttr", GetConnection(cur)->hdbc, cur->hstmt);
        ErrorRet6:
            SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_BIND_BY_COLUMN, SQL_IS_UINTEGER);
            goto ErrorRet5;
        }
        if (!SQL_SUCCEEDED(SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)rows_converted, SQL_IS_UINTEGER)))
        {
            RaiseErrorFromHandle(cur->cnxn, "SQLSetStmtAttr", GetConnection(cur)->hdbc, cur->hstmt);
            goto ErrorRet6;
        }
        if (!SQL_SUCCEEDED(SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, (SQLPOINTER)&bop, SQL_IS_POINTER)))
        {
            RaiseErrorFromHandle(cur->cnxn, "SQLSetStmtAttr", GetConnection(cur)->hdbc, cur->hstmt);
        ErrorRet7:
            SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, SQL_IS_UINTEGER);
            goto ErrorRet6;
        }

        // The code below was copy-pasted from cursor.cpp's execute() for convenience.
        // TODO: REFACTOR if there is possibility to reuse (maybe not, because DAE structure is different)
        Py_BEGIN_ALLOW_THREADS
        rc = SQLExecute(cur->hstmt);
        Py_END_ALLOW_THREADS

        if (cur->cnxn->hdbc == SQL_NULL_HANDLE)
        {
            // The connection was closed by another thread in the ALLOW_THREADS block above.
            RaiseErrorV(0, ProgrammingError, "The cursor's connection was closed.");
        ErrorRet8:
            FreeParameterData(cur);
            SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, 0, SQL_IS_POINTER);
            goto ErrorRet7;
        }

        if (!SQL_SUCCEEDED(rc) && rc != SQL_NEED_DATA && rc != SQL_NO_DATA)
        {
            // We could try dropping through the while and if below, but if there is an error, we need to raise it before
            // FreeParameterData calls more ODBC functions.
            RaiseErrorFromHandle(cur->cnxn, "SQLExecute", cur->cnxn->hdbc, cur->hstmt);
            goto ErrorRet8;
        }

        if (rc == SQL_SUCCESS_WITH_INFO)
        {
            GetDiagRecs(cur);
        }

        // TODO: Refactor into ProcessDAEParams() ?
        while (rc == SQL_NEED_DATA)
        {
            // One or more parameters were too long to bind normally so we set the
            // length to SQL_LEN_DATA_AT_EXEC.  ODBC will return SQL_NEED_DATA for
            // each of the parameters we did this for.
            //
            // For each one we set a pointer to the ParamInfo as the "parameter
            // data" we can access with SQLParamData.  We've stashed everything we
            // need in there.

            szLastFunction = "SQLParamData";
            DAEParam *pInfo;
            Py_BEGIN_ALLOW_THREADS
            rc = SQLParamData(cur->hstmt, (SQLPOINTER*)&pInfo);
            Py_END_ALLOW_THREADS

            if (rc != SQL_NEED_DATA && rc != SQL_NO_DATA && !SQL_SUCCEEDED(rc))
                return RaiseErrorFromHandle(cur->cnxn, "SQLParamData", cur->cnxn->hdbc, cur->hstmt) != NULL;

            TRACE("SQLParamData() --> %d\n", rc);

            if (rc == SQL_NEED_DATA)
            {
                PyObject* objCell = pInfo->cell;

                // If the object is Unicode it needs to be converted into bytes before it can be used by SQLPutData
                if (PyUnicode_Check(objCell))
                {
                    const TextEnc& enc = cur->cnxn->sqlwchar_enc;
                    PyObject* bytes = NULL;

                    switch (enc.optenc)
                    {
                    case OPTENC_UTF8:
                        bytes = PyUnicode_AsUTF8String(objCell);
                        break;
                    case OPTENC_UTF16:
                        bytes = PyUnicode_AsUTF16String(objCell);
                        break;
                    case OPTENC_UTF16LE:
                        bytes = PyUnicode_AsEncodedString(objCell, "utf_16_le", NULL);
                        break;
                    case OPTENC_UTF16BE:
                        bytes = PyUnicode_AsEncodedString(objCell, "utf_16_be", NULL);
                        break;
                    }
                    if (bytes && PyBytes_Check(bytes))
                    {
                        objCell = bytes;
                    }
                    //TODO: Raise or clear error when bytes == NULL.
                }

                szLastFunction = "SQLPutData";
                if (PyBytes_Check(objCell) || PyByteArray_Check(objCell))
                {
                    char *(*pGetPtr)(PyObject*);
                    Py_ssize_t (*pGetLen)(PyObject*);
                    if (PyByteArray_Check(objCell))
                    {
                        pGetPtr = PyByteArray_AsString;
                        pGetLen = PyByteArray_Size;
                    }
                    else
                    {
                        pGetPtr = PyBytes_AsString;
                        pGetLen = PyBytes_Size;
                    }

                    const char* p = pGetPtr(objCell);
                    SQLLEN cb = (SQLLEN)pGetLen(objCell);
                    SQLLEN offset = 0;

                    do
                    {
                        SQLLEN remaining = min(pInfo->maxlen, cb - offset);
                        TRACE("SQLPutData [%d] (%d) %.10s\n", offset, remaining, &p[offset]);

                        Py_BEGIN_ALLOW_THREADS
                        rc = SQLPutData(cur->hstmt, (SQLPOINTER)&p[offset], remaining);
                        Py_END_ALLOW_THREADS
                        if (!SQL_SUCCEEDED(rc))
                            return RaiseErrorFromHandle(cur->cnxn, "SQLPutData", cur->cnxn->hdbc, cur->hstmt) != NULL;
                        offset += remaining;
                    }
                    while (offset < cb);

                    if (PyUnicode_Check(pInfo->cell) && PyBytes_Check(objCell))
                    {
                        Py_XDECREF(objCell);
                    }
                }
                Py_XDECREF(pInfo->cell);
                rc = SQL_NEED_DATA;
            }
        }

        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA)
            return RaiseErrorFromHandle(cur->cnxn, szLastFunction, cur->cnxn->hdbc, cur->hstmt) != NULL;

        SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, SQL_IS_UINTEGER);
        SQLSetStmtAttr(cur->hstmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, 0, SQL_IS_POINTER);
        PyMem_Free(cur->paramArray);
        cur->paramArray = 0;
    }

    Py_XDECREF(rowseq);
    FreeParameterData(cur);
  return ret;
}


static bool GetParamType(Cursor* cur, Py_ssize_t index, SQLSMALLINT& type)
{
    // Returns the ODBC type of the of given parameter.
    //
    // Normally we set the parameter type based on the parameter's Python object type (e.g. str --> SQL_CHAR), so this
    // is only called when the parameter is None.  In that case, we can't guess the type and have to use
    // SQLDescribeParam.
    //
    // If the database doesn't support SQLDescribeParam, we return SQL_VARCHAR since it converts to most other types.
    // However, it will not usually work if the target column is a binary column.

    if (!GetConnection(cur)->supports_describeparam || cur->paramcount == 0)
    {
        type = SQL_VARCHAR;
        return true;
    }

    if (cur->paramtypes == 0)
    {
        cur->paramtypes = reinterpret_cast<SQLSMALLINT*>(PyMem_Malloc(sizeof(SQLSMALLINT) * cur->paramcount));
        if (cur->paramtypes == 0)
        {
            PyErr_NoMemory();
            return false;
        }

        // SQL_UNKNOWN_TYPE is zero, so zero out all columns since we haven't looked any up yet.
        memset(cur->paramtypes, 0, sizeof(SQLSMALLINT) * cur->paramcount);
    }

    if (cur->paramtypes[index] == SQL_UNKNOWN_TYPE)
    {
        SQLULEN ParameterSizePtr;
        SQLSMALLINT DecimalDigitsPtr;
        SQLSMALLINT NullablePtr;
        SQLRETURN ret;

        Py_BEGIN_ALLOW_THREADS
        ret = SQLDescribeParam(cur->hstmt, (SQLUSMALLINT)(index + 1), &cur->paramtypes[index], &ParameterSizePtr, &DecimalDigitsPtr, &NullablePtr);
        Py_END_ALLOW_THREADS

        if (!SQL_SUCCEEDED(ret))
        {
            // This can happen with ("select ?", None).  We'll default to VARCHAR which works with most types.
            cur->paramtypes[index] = SQL_VARCHAR;
        }
    }

    type = cur->paramtypes[index];
    return true;
}

struct NullParam
{
    PyObject_HEAD
};


PyTypeObject NullParamType =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "pyodbc.NullParam",         // tp_name
    sizeof(NullParam),          // tp_basicsize
    0,                          // tp_itemsize
    0,                          // destructor tp_dealloc
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
};

PyObject* null_binary;

bool Params_init()
{
    if (PyType_Ready(&NullParamType) < 0)
        return false;

    null_binary = (PyObject*)PyObject_New(NullParam, &NullParamType);
    if (null_binary == 0)
        return false;

    PyDateTime_IMPORT;

    return true;
}
