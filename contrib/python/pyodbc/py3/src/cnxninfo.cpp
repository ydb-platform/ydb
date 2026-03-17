// There is a bunch of information we want from connections which requires calls to SQLGetInfo when we first connect.
// However, this isn't something we really want to do for every connection, so we cache it by the hash of the
// connection string.  When we create a new connection, we copy the values into the connection structure.
//
// We hash the connection string since it may contain sensitive information we wouldn't want exposed in a core dump.

#include "pyodbc.h"
#include "wrapper.h"
#include "textenc.h"
#include "cnxninfo.h"
#include "connection.h"

// Maps from a Python string of the SHA1 hash to a CnxnInfo object.
//
static PyObject* map_hash_to_info;

static PyObject* hashlib;       // The hashlib module
static PyObject* update;        // The string 'update', used in GetHash.

bool CnxnInfo_init()
{
    // Called during startup to give us a chance to import the hash code.  If we can't find it, we'll print a warning
    // to the console and not cache anything.

    map_hash_to_info = PyDict_New();

    update = PyUnicode_FromString("update");
    if (!map_hash_to_info || !update)
        return false;

    hashlib = PyImport_ImportModule("hashlib");
    if (!hashlib)
        return false;

    return true;
}

static PyObject* GetHash(PyObject* p)
{
    Object bytes(PyUnicode_AsUTF8String(p));
    if (!bytes)
        return 0;
    p = bytes.Get();

    Object hash(PyObject_CallMethod(hashlib, "new", "s", "sha1"));
    if (!hash.IsValid())
        return 0;

    Object result(PyObject_CallMethodObjArgs(hash, update, p, 0));
    if (!result.IsValid())
        return 0;

    return PyObject_CallMethod(hash, "hexdigest", 0);
}

inline void GetColumnSize(Connection* cnxn, SQLSMALLINT sqltype, int* psize)
{
    // For some reason I can't seem to reuse the HSTMT multiple times in a row here.  Until I
    // figure it out I'll simply allocate a new one each time.
    HSTMT hstmt;
    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, cnxn->hdbc, &hstmt)))
        return;

    SQLINTEGER columnsize;

    if (SQL_SUCCEEDED(SQLGetTypeInfo(hstmt, sqltype)) &&
        SQL_SUCCEEDED(SQLFetch(hstmt)) &&
        SQL_SUCCEEDED(SQLGetData(hstmt, 3, SQL_INTEGER, &columnsize, sizeof(columnsize), 0)))
    {
        // I believe some drivers are returning negative numbers for "unlimited" text fields,
        // such as FileMaker.  Ignore anything that seems too small.
        if (columnsize >= 1)
            *psize = (int)columnsize;
    }

    SQLFreeStmt(hstmt, SQL_CLOSE);
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
}

static PyObject* CnxnInfo_New(Connection* cnxn)
{
#ifdef _MSC_VER
#pragma warning(disable : 4365)
#endif
    CnxnInfo* p = PyObject_NEW(CnxnInfo, &CnxnInfoType);
    if (!p)
        return 0;
    Object info((PyObject*)p);

    // set defaults
    p->odbc_major             = 0;
    p->odbc_minor             = 0;
    p->supports_describeparam = false;
    p->datetime_precision     = 19; // default: "yyyy-mm-dd hh:mm:ss"
    p->need_long_data_len     = false;

    p->varchar_maxlength  = 1 * 1024 * 1024 * 1024;
    p->wvarchar_maxlength = 1 * 1024 * 1024 * 1024;
    p->binary_maxlength   = 1 * 1024 * 1024 * 1024;

    // WARNING: The GIL lock is released for the *entire* function here.  Do not
    // touch any objects, call Python APIs, etc.  We are simply making ODBC
    // calls and setting atomic values (ints & chars).  Also, make sure the lock
    // gets reaquired -- do not add an early exit.

    SQLRETURN ret;
    Py_BEGIN_ALLOW_THREADS

    char szVer[20];
    SQLSMALLINT cch = 0;
    ret = SQLGetInfo(cnxn->hdbc, SQL_DRIVER_ODBC_VER, szVer, _countof(szVer), &cch);
    if (SQL_SUCCEEDED(ret))
    {
        char* dot = strchr(szVer, '.');
        if (dot)
        {
            *dot = '\0';
            p->odbc_major=(char)atoi(szVer);
            p->odbc_minor=(char)atoi(dot + 1);
        }
    }

    char szYN[2];
    if (SQL_SUCCEEDED(SQLGetInfo(cnxn->hdbc, SQL_DESCRIBE_PARAMETER, szYN, _countof(szYN), &cch)))
        p->supports_describeparam = szYN[0] == 'Y';

    if (SQL_SUCCEEDED(SQLGetInfo(cnxn->hdbc, SQL_NEED_LONG_DATA_LEN, szYN, _countof(szYN), &cch)))
        p->need_long_data_len = (szYN[0] == 'Y');

    GetColumnSize(cnxn, SQL_VARCHAR, &p->varchar_maxlength);
    GetColumnSize(cnxn, SQL_WVARCHAR, &p->wvarchar_maxlength);
    GetColumnSize(cnxn, SQL_VARBINARY, &p->binary_maxlength);
    GetColumnSize(cnxn, SQL_TYPE_TIMESTAMP, &p->datetime_precision);

    Py_END_ALLOW_THREADS

    return info.Detach();
}


PyObject* GetConnectionInfo(PyObject* pConnectionString, Connection* cnxn)
{
    // Looks-up or creates a CnxnInfo object for the given connection string.  The connection string can be a Unicode
    // or String object.

    Object hash(GetHash(pConnectionString));

    if (hash.IsValid())
    {
        PyObject* info = PyDict_GetItem(map_hash_to_info, hash);

        if (info)
        {
            Py_INCREF(info);
            return info;
        }
    }

    PyObject* info = CnxnInfo_New(cnxn);
    if (info != 0 && hash.IsValid())
        PyDict_SetItem(map_hash_to_info, hash, info);

    return info;
}


PyTypeObject CnxnInfoType =
{
    PyVarObject_HEAD_INIT(0, 0)
    "pyodbc.CnxnInfo",                                      // tp_name
    sizeof(CnxnInfo),                                       // tp_basicsize
    0,                                                      // tp_itemsize
    0,                                                      // destructor tp_dealloc
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
    Py_TPFLAGS_DEFAULT,                                     // tp_flags
};
