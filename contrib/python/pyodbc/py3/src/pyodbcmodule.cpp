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
#include "wrapper.h"
#include "textenc.h"
#include "pyodbcmodule.h"
#include "connection.h"
#include "cursor.h"
#include "row.h"
#include "errors.h"
#include "getdata.h"
#include "cnxninfo.h"
#include "params.h"
#include "dbspecific.h"
#include "decimal.h"
#include <datetime.h>

#include <time.h>
#include <stdarg.h>

static PyObject* MakeConnectionString(PyObject* existing, PyObject* parts);

PyObject* pModule = 0;

static char module_doc[] =
    "A database module for accessing databases via ODBC.\n"
    "\n"
    "This module conforms to the DB API 2.0 specification while providing\n"
    "non-standard convenience features.  Only standard Python data types are used\n"
    "so additional DLLs are not required.\n"
    "\n"
    "Static Variables:\n\n"
    "version\n"
    "  The module version string.  Official builds will have a version in the format\n"
    "  `major.minor.revision`, such as 2.1.7.  Beta versions will have -beta appended,\n"
    "  such as 2.1.8-beta03.  (This would be a build before the official 2.1.8 release.)\n"
    "  Some special test builds will have a test name (the git branch name) prepended,\n"
    "  such as fixissue90-2.1.8-beta03.\n"
    "\n"
    "apilevel\n"
    "  The string constant '2.0' indicating this module supports DB API level 2.0.\n"
    "\n"
    "lowercase\n"
    "  A Boolean that controls whether column names in result rows are lowercased.\n"
    "  This can be changed any time and affects queries executed after the change.\n"
    "  The default is False.  This can be useful when database columns have\n"
    "  inconsistent capitalization.\n"
    "\n"
    "pooling\n"
    "  A Boolean indicating whether connection pooling is enabled.  This is a\n"
    "  global (HENV) setting, so it can only be modified before the first\n"
    "  connection is made.  The default is True, which enables ODBC connection\n"
    "  pooling.\n"
    "\n"
    "threadsafety\n"
    "  The integer 1, indicating that threads may share the module but not\n"
    "  connections.  Note that connections and cursors may be used by different\n"
    "  threads, just not at the same time.\n"
    "\n"
    "paramstyle\n"
    "  The string constant 'qmark' to indicate parameters are identified using\n"
    "  question marks.\n"
    "\n"
    "odbcversion\n"
    "  The ODBC version number as a string, such as '3.X' for ODBC 3.X compatibility.\n"
    "  This is a global (HENV) setting, so it can only be modified before the first\n"
    "  connection is made. Use 3.8 if you are using unixodbc connection pooling and your\n"
    "  drivers are all 3.8 compatible. The default is '3.X'.";

inline namespace pyodbc {

PyObject* Error;
PyObject* Warning;
PyObject* InterfaceError;
PyObject* DatabaseError;
PyObject* InternalError;
PyObject* OperationalError;
PyObject* ProgrammingError;
PyObject* IntegrityError;
PyObject* DataError;
PyObject* NotSupportedError;

}

struct ExcInfo
{
    const char* szName;
    const char* szFullName;
    PyObject** ppexc;
    PyObject** ppexcParent;
    const char* szDoc;
};

#define MAKEEXCINFO(name, parent, doc) { #name, "pyodbc." #name, &name, &parent, doc }

static ExcInfo aExcInfos[] = {
    MAKEEXCINFO(Error, PyExc_Exception,
                "Exception that is the base class of all other error exceptions. You can use\n"
                "this to catch all errors with one single 'except' statement."),
    MAKEEXCINFO(Warning, PyExc_Exception,
                "Exception raised for important warnings like data truncations while inserting,\n"
                " etc."),
    MAKEEXCINFO(InterfaceError, Error,
                "Exception raised for errors that are related to the database interface rather\n"
                "than the database itself."),
    MAKEEXCINFO(DatabaseError, Error, "Exception raised for errors that are related to the database."),
    MAKEEXCINFO(DataError, DatabaseError,
                "Exception raised for errors that are due to problems with the processed data\n"
                "like division by zero, numeric value out of range, etc."),
    MAKEEXCINFO(OperationalError, DatabaseError,
                "Exception raised for errors that are related to the database's operation and\n"
                "not necessarily under the control of the programmer, e.g. an unexpected\n"
                "disconnect occurs, the data source name is not found, a transaction could not\n"
                "be processed, a memory allocation error occurred during processing, etc."),
    MAKEEXCINFO(IntegrityError, DatabaseError,
                "Exception raised when the relational integrity of the database is affected,\n"
                "e.g. a foreign key check fails."),
    MAKEEXCINFO(InternalError, DatabaseError,
                "Exception raised when the database encounters an internal error, e.g. the\n"
                "cursor is not valid anymore, the transaction is out of sync, etc."),
    MAKEEXCINFO(ProgrammingError, DatabaseError,
                "Exception raised for programming errors, e.g. table not found or already\n"
                "exists, syntax error in the SQL statement, wrong number of parameters\n"
                "specified, etc."),
    MAKEEXCINFO(NotSupportedError, DatabaseError,
                "Exception raised in case a method or database API was used which is not\n"
                "supported by the database, e.g. requesting a .rollback() on a connection that\n"
                "does not support transaction or has transactions turned off.")
};


bool PyMem_Realloc(BYTE** pp, size_t newlen)
{
    // A wrapper around realloc with a safer interface.  If it is successful, *pp is updated to the
    // new pointer value.  If not successful, it is not modified.  (It is easy to forget and lose
    // the old pointer value with realloc.)

    BYTE* pT = (BYTE*)PyMem_Realloc(*pp, newlen);
    if (pT == 0)
        return false;
    *pp = pT;
    return true;
}



bool UseNativeUUID()
{
    PyObject* o = PyObject_GetAttrString(pModule, "native_uuid");
    // If this fails for some reason, we'll assume false and allow the exception to pop up later.
    bool b = o && PyObject_IsTrue(o);
    Py_XDECREF(o);
    return b;
}

HENV henv = SQL_NULL_HANDLE;

PyObject* GetClassForThread(const char* szModule, const char* szClass)
{
    // Returns the given class, specific to the current thread's interpreter.  For performance
    // these are cached for each thread.
    //
    // This is for internal use only, so we'll cache using only the class name.  Make sure they
    // are unique.  (That is, don't try to import classes with the same name from two different
    // modules.)

    PyObject* dict = PyThreadState_GetDict();
    assert(dict);
    if (dict == 0)
    {
        // I don't know why there wouldn't be thread state so I'm going to raise an exception
        // unless I find more info.
        return PyErr_Format(PyExc_Exception, "pyodbc: PyThreadState_GetDict returned NULL");
    }

    // Check the cache.  GetItemString returns a borrowed reference.
    PyObject* cls = PyDict_GetItemString(dict, szClass);
    if (cls)
    {
        Py_INCREF(cls);
        return cls;
    }

    // Import the class and cache it.  GetAttrString returns a new reference.
    PyObject* mod = PyImport_ImportModule(szModule);
    if (!mod)
        return 0;

    cls = PyObject_GetAttrString(mod, szClass);
    Py_DECREF(mod);
    if (!cls)
        return 0;

    // SetItemString increments the refcount (not documented)
    PyDict_SetItemString(dict, szClass, cls);

    return cls;
}

bool IsInstanceForThread(PyObject* param, const char* szModule, const char* szClass, PyObject** pcls)
{
    // Like PyObject_IsInstance but compares against a class specific to the current thread's
    // interpreter, for proper subinterpreter support.  Uses GetClassForThread.
    //
    // If `param` is an instance of the given class, true is returned and a new reference to
    // the class, specific to the current thread, is returned via pcls.  The caller is
    // responsible for decrementing the class.
    //
    // If `param` is not an instance, true is still returned (!) but *pcls will be zero.
    //
    // False is only returned when an exception has been raised.  (That is, the return value is
    // not used to indicate whether the instance check matched or not.)

    if (param == 0)
    {
        *pcls = 0;
        return true;
    }

    PyObject* cls = GetClassForThread(szModule, szClass);
    if (!cls)
    {
        *pcls = 0;
        return false;
    }

    int n = PyObject_IsInstance(param, cls);
    // (The checks below can be compressed into just a few lines, but I was concerned it
    //  wouldn't be clear.)

    if (n == 1)
    {
        // We have a match.
        *pcls = cls;
        return true;
    }

    Py_DECREF(cls);
    *pcls = 0;

    if (n == 0)
    {
        // No exception, but not a match.
        return true;
    }

    // n == -1; an exception occurred
    return false;
}


static bool import_types()
{
    // Note: We can only import types from C extensions since they are shared among all
    // interpreters.  Other classes are imported per-thread via GetClassForThread.

    // In Python 2.5 final, PyDateTime_IMPORT no longer works unless the datetime module was previously
    // imported (among other problems).

    PyObject* pdt = PyImport_ImportModule("datetime");

    if (!pdt)
        return false;

    PyDateTime_IMPORT;

    Cursor_init();
    if (!CnxnInfo_init())
        return false;
    GetData_init();
    if (!Params_init())
        return false;
    if (!InitializeDecimal())
        return false;

    return true;
}


static bool AllocateEnv()
{
    PyObject* pooling = PyObject_GetAttrString(pModule, "pooling");
    bool bPooling = pooling == Py_True;
    Py_DECREF(pooling);

    if (bPooling)
    {
        if (!SQL_SUCCEEDED(SQLSetEnvAttr(SQL_NULL_HANDLE, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_HENV, sizeof(int))))
        {
            PyErr_SetString(PyExc_RuntimeError, "Unable to set SQL_ATTR_CONNECTION_POOLING attribute.");
            return false;
        }
    }

    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv)))
    {
        PyErr_SetString(PyExc_RuntimeError, "Can't initialize module pyodbc.  SQLAllocEnv failed.");
        return false;
    }

    SQLPOINTER defaultVersion = (SQLPOINTER)SQL_OV_ODBC3;
    PyObject* odbcversion = PyObject_GetAttrString(pModule, "odbcversion");
    if (PyObject_TypeCheck(odbcversion, &PyUnicode_Type)) {
        if (PyUnicode_CompareWithASCIIString(odbcversion, "3.8") == 0)
        {
            defaultVersion = (SQLPOINTER)SQL_OV_ODBC3_80;
        }
    }
    Py_DECREF(odbcversion);
    
    if (!SQL_SUCCEEDED(SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, defaultVersion, sizeof(int))))
    {
        PyErr_SetString(PyExc_RuntimeError, "Unable to set SQL_ATTR_ODBC_VERSION attribute.");
        return false;
    }

    return true;
}

static bool CheckAttrsVal(PyObject *val, bool allowSeq)
{
    if (PyLong_Check(val)
     || PyByteArray_Check(val)
     || PyBytes_Check(val)
     || PyUnicode_Check(val))
        return true;

    if (allowSeq && PySequence_Check(val))
    {
        Py_ssize_t len = PySequence_Size(val);
        for (Py_ssize_t i = 0; i < len; i++)
        {
            Object v(PySequence_GetItem(val, i));
            if (!CheckAttrsVal(v, false))
                return false;
        }
        return true;
    }

    return PyErr_Format(PyExc_TypeError, "Attribute dictionary attrs must be"
        " integers, buffers, bytes, %s", allowSeq ? "strings, or sequences" : "or strings") != 0;
}

static PyObject* _CheckAttrsDict(PyObject* attrs)
{
    // The attrs_before dictionary must be keys to integer values.  If valid and non-empty,
    // increment the reference count and return the pointer to indicate the calling code should
    // keep it.  If empty, just return zero which indicates to the calling code it should not
    // keep the value.  If an error occurs, set an error.  The calling code must look for this
    // in the zero case.

    // We already know this is a dictionary.

    if (PyDict_Size(attrs) == 0)
        return 0;

    Py_ssize_t pos = 0;
    PyObject* key = 0;
    PyObject* value = 0;
    while (PyDict_Next(attrs, &pos, &key, &value))
    {
        if (!PyLong_Check(key))
            return PyErr_Format(PyExc_TypeError, "Attribute dictionary keys must be integers");

        if (!CheckAttrsVal(value, true))
            return 0;
    }
    Py_INCREF(attrs);
    return attrs;
}


// Map DB API recommended keywords to ODBC keywords.

struct keywordmap
{
    const char* oldname;
    const char* newname;
    PyObject* newnameObject;    // PyString object version of newname, created as needed.
};

static keywordmap keywordmaps[] =
{
    { "user",     "uid",    0 },
    { "password", "pwd",    0 },
    { "host",     "server", 0 },
};


static PyObject* mod_connect(PyObject* self, PyObject* args, PyObject* kwargs)
{
    UNUSED(self);

    Object pConnectString;
    int fAutoCommit = 0;
    int fReadOnly = 0;
    long timeout = 0;
    PyObject* encoding = 0;

    Object attrs_before; // Optional connect attrs set before connecting

    Py_ssize_t size = args ? PyTuple_Size(args) : 0;

    if (size > 1)
    {
        PyErr_SetString(PyExc_TypeError, "function takes at most 1 non-keyword argument");
        return 0;
    }

    if (size == 1)
    {
        if (!PyUnicode_Check(PyTuple_GET_ITEM(args, 0)) && !PyUnicode_Check(PyTuple_GET_ITEM(args, 0)))
            return PyErr_Format(PyExc_TypeError, "argument 1 must be a string or unicode object");

        pConnectString.Attach(PyUnicode_FromObject(PyTuple_GetItem(args, 0)));
        if (!pConnectString.IsValid())
            return 0;
    }

    if (kwargs && PyDict_Size(kwargs) > 0)
    {
        Object partsdict(PyDict_New());
        if (!partsdict.IsValid())
            return 0;

        Py_ssize_t pos = 0;
        PyObject* key = 0;
        PyObject* value = 0;

        Object okey; // in case we need to allocate a new key

        while (PyDict_Next(kwargs, &pos, &key, &value))
        {
            if (!PyUnicode_Check(key))
                return PyErr_Format(PyExc_TypeError, "Dictionary keys passed to connect must be strings");

            // // Note: key and value are *borrowed*.
            //
            // // Check for the two non-connection string keywords we accept.  (If we get many more of these, create something
            // // table driven.  Are we sure there isn't a Python function to parse keywords but leave those it doesn't know?)
            // const char* szKey = PyUnicode_AsString(key);

            if (PyUnicode_CompareWithASCIIString(key, "autocommit") == 0)
            {
                fAutoCommit = PyObject_IsTrue(value);
                continue;
            }
            if (PyUnicode_CompareWithASCIIString(key, "timeout") == 0)
            {
                timeout = PyLong_AsLong(value);
                if (PyErr_Occurred())
                    return 0;
                continue;
            }
            if (PyUnicode_CompareWithASCIIString(key, "readonly") == 0)
            {
                fReadOnly = PyObject_IsTrue(value);
                continue;
            }
            if (PyUnicode_CompareWithASCIIString(key, "attrs_before") == 0 && PyDict_Check(value))
            {
                attrs_before = _CheckAttrsDict(value);
                if (PyErr_Occurred())
                    return 0;
                continue;
            }
            if (PyUnicode_CompareWithASCIIString(key, "encoding") == 0)
            {
                if (!PyUnicode_Check(value))
                    return PyErr_Format(PyExc_TypeError, "encoding must be a string");
                encoding = value;
                continue;
            }

            // Map DB API recommended names to ODBC names (e.g. user --> uid).

            for (size_t i = 0; i < _countof(keywordmaps); i++)
            {
                if (PyUnicode_CompareWithASCIIString(key, keywordmaps[i].oldname) == 0)
                {
                    if (keywordmaps[i].newnameObject == 0)
                    {
                        keywordmaps[i].newnameObject = PyUnicode_FromString(keywordmaps[i].newname);
                        if (keywordmaps[i].newnameObject == 0)
                            return 0;
                    }

                    key = keywordmaps[i].newnameObject;
                    break;
                }
            }

            PyObject* str = PyObject_Str(value); // convert if necessary
            if (!str)
                return 0;

            if (PyDict_SetItem(partsdict.Get(), key, str) == -1)
            {
                Py_XDECREF(str);
                return 0;
            }

            Py_XDECREF(str);
        }

        if (PyDict_Size(partsdict.Get()))
            pConnectString.Attach(MakeConnectionString(pConnectString.Get(), partsdict));
    }

    if (!pConnectString.IsValid())
        return PyErr_Format(PyExc_TypeError, "no connection information was passed");

    if (henv == SQL_NULL_HANDLE)
    {
        if (!AllocateEnv())
            return 0;
    }

    return (PyObject*)Connection_New(pConnectString.Get(), fAutoCommit != 0, timeout,
                                     fReadOnly != 0, attrs_before.Detach(), encoding);
}


static PyObject* mod_drivers(PyObject* self)
{
    UNUSED(self);

    if (henv == SQL_NULL_HANDLE && !AllocateEnv())
        return 0;

    Object result(PyList_New(0));
    if (!result)
        return 0;

    SQLCHAR szDriverDesc[500];
    SWORD cbDriverDesc;
    SWORD cbAttrs;

    SQLRETURN ret;
    SQLUSMALLINT nDirection = SQL_FETCH_FIRST;

    for (;;)
    {
        ret = SQLDrivers(henv, nDirection, szDriverDesc, _countof(szDriverDesc), &cbDriverDesc, 0, 0, &cbAttrs);

        if (!SQL_SUCCEEDED(ret))
            break;

        // REVIEW: This is another reason why we really need a factory that we can use.  At this
        // point we don't have a global text encoding that we can assume for this.  Somehow it
        // seems to be working to use UTF-8, even on Windows.
        Object name(PyUnicode_FromString((const char*)szDriverDesc));
        if (!name)
            return 0;

        if (PyList_Append(result, name.Get()) != 0)
            return 0;
        name.Detach();

        nDirection = SQL_FETCH_NEXT;
    }

    if (ret != SQL_NO_DATA)
    {
        Py_DECREF(result);
        return RaiseErrorFromHandle(0, "SQLDrivers", SQL_NULL_HANDLE, SQL_NULL_HANDLE);
    }

    return result.Detach();
}


static PyObject* mod_datasources(PyObject* self)
{
    UNUSED(self);

    if (henv == SQL_NULL_HANDLE && !AllocateEnv())
        return 0;

    PyObject* result = PyDict_New();
    if (!result)
        return 0;

// Using a buffer larger than SQL_MAX_DSN_LENGTH + 1 for systems that ignore it.
#if defined(WIN32) || defined(_WIN32) || defined(__WIN32__) || defined(__NT__)
    SQLWCHAR szDSN[500];
    SQLWCHAR szDesc[500];
#else
    SQLCHAR szDSN[500];
    SQLCHAR szDesc[500];
#endif

    SWORD cbDSN;
    SWORD cbDesc;

    SQLUSMALLINT nDirection = SQL_FETCH_FIRST;

    SQLRETURN ret;

    for (;;)
    {
#if defined(WIN32) || defined(_WIN32) || defined(__WIN32__) || defined(__NT__)
        // wchar_t and UTF-16 on Windows
        ret = SQLDataSourcesW(henv, nDirection, szDSN,  _countof(szDSN),  &cbDSN, szDesc, _countof(szDesc), &cbDesc);

        if (!SQL_SUCCEEDED(ret))
            break;

        int byteorder = BYTEORDER_NATIVE;
        PyObject* key = PyUnicode_DecodeUTF16((char*)szDSN, cbDSN * sizeof(wchar_t), "strict", &byteorder);
        PyObject* val = PyUnicode_DecodeUTF16((char*)szDesc, cbDesc * sizeof(wchar_t), "strict", &byteorder);
#else
        // UTF-8
        ret = SQLDataSources(henv, nDirection, szDSN,  _countof(szDSN),  &cbDSN, szDesc, _countof(szDesc), &cbDesc);

        if (!SQL_SUCCEEDED(ret))
            break;

        PyObject* key = PyUnicode_FromString((const char*)szDSN);
        PyObject* val = PyUnicode_FromString((const char*)szDesc);
#endif

        if(key && val)
            PyDict_SetItem(result, key, val);

        nDirection = SQL_FETCH_NEXT;
    }

    if (ret != SQL_NO_DATA)
    {
        Py_DECREF(result);
        return RaiseErrorFromHandle(0, "SQLDataSources", SQL_NULL_HANDLE, SQL_NULL_HANDLE);
    }

    return result;
}


static PyObject* mod_timefromticks(PyObject* self, PyObject* args)
{
    UNUSED(self);

    PyObject* num;
    if (!PyArg_ParseTuple(args, "O", &num))
        return 0;

    if (!PyNumber_Check(num))
        return PyErr_Format(PyExc_TypeError, "TimeFromTicks requires a number.");

    Object l(PyNumber_Long(num));
    if (!l)
        return 0;

    time_t t = PyLong_AsLong(num);
    struct tm* fields = localtime(&t);

    return PyTime_FromTime(fields->tm_hour, fields->tm_min, fields->tm_sec, 0);
}


static PyObject* mod_datefromticks(PyObject* self, PyObject* args)
{
    UNUSED(self);
    return PyDate_FromTimestamp(args);
}


static PyObject* mod_timestampfromticks(PyObject* self, PyObject* args)
{
    UNUSED(self);
    return PyDateTime_FromTimestamp(args);
}

static PyObject* mod_setdecimalsep(PyObject* self, PyObject* args)
{
    UNUSED(self);

    const char* type = "U";

    PyObject* p;
    if (!PyArg_ParseTuple(args, type, &p))
        return 0;
    if (!SetDecimalPoint(p))
        return 0;
    Py_RETURN_NONE;
}

static PyObject* mod_getdecimalsep(PyObject* self)
{
    UNUSED(self);
    return GetDecimalPoint();
}

static char connect_doc[] =
    "connect(str, autocommit=False, timeout=0, **kwargs) --> Connection\n"
    "\n"
    "Accepts an ODBC connection string and returns a new Connection object.\n"
    "\n"
    "The connection string will be passed to SQLDriverConnect, so a DSN connection\n"
    "can be created using:\n"
    "\n"
    "  cnxn = pyodbc.connect('DSN=DataSourceName;UID=user;PWD=password')\n"
    "\n"
    "To connect without requiring a DSN, specify the driver and connection\n"
    "information:\n"
    "\n"
    "  DRIVER={SQL Server};SERVER=localhost;DATABASE=testdb;UID=user;PWD=password\n"
    "\n"
    "Note the use of braces when a value contains spaces.  Refer to SQLDriverConnect\n"
    "documentation or the documentation of your ODBC driver for details.\n"
    "\n"
    "The connection string can be passed as the string `str`, as a list of keywords,\n"
    "or a combination of the two.  Any keywords except autocommit and timeout\n"
    "(see below) are simply added to the connection string.\n"
    "\n"
    "  connect('server=localhost;user=me')\n"
    "  connect(server='localhost', user='me')\n"
    "  connect('server=localhost', user='me')\n"
    "\n"
    "The DB API recommends the keywords 'user', 'password', and 'host', but these\n"
    "are not valid ODBC keywords, so these will be converted to 'uid', 'pwd', and\n"
    "'server'.\n"
    "\n"
    "Special Keywords\n"
    "\n"
    "The following specal keywords are processed by pyodbc and are not added to the\n"
    "connection string.  (If you must use these in your connection string, pass them\n"
    "as a string, not as keywords.)\n"
    "\n"
    "  autocommit\n"
    "    If False or zero, the default, transactions are created automatically as\n"
    "    defined in the DB API 2.  If True or non-zero, the connection is put into\n"
    "    ODBC autocommit mode and statements are committed automatically.\n"
    "   \n"
    "  timeout\n"
    "    An integer login timeout in seconds, used to set the SQL_ATTR_LOGIN_TIMEOUT\n"
    "    attribute of the connection.  The default is 0 which means the database's\n"
    "    default timeout, if any, is used.\n";

static char timefromticks_doc[] =
    "TimeFromTicks(ticks) --> datetime.time\n"
    "\n"
    "Returns a time object initialized from the given ticks value (number of seconds\n"
    "since the epoch; see the documentation of the standard Python time module for\n"
    "details).";

static char datefromticks_doc[] =
    "DateFromTicks(ticks) --> datetime.date\n"  \
    "\n"                                                                \
    "Returns a date object initialized from the given ticks value (number of seconds\n" \
    "since the epoch; see the documentation of the standard Python time module for\n" \
    "details).";

static char timestampfromticks_doc[] =
    "TimestampFromTicks(ticks) --> datetime.datetime\n"  \
    "\n"                                                                \
    "Returns a datetime object initialized from the given ticks value (number of\n" \
    "seconds since the epoch; see the documentation of the standard Python time\n" \
    "module for details";

static char drivers_doc[] =
    "drivers() --> [ DriverName1, DriverName2 ... DriverNameN ]\n" \
    "\n" \
    "Returns a list of installed drivers.";

static char datasources_doc[] =
    "dataSources() --> { DSN : Description }\n" \
    "\n" \
    "Returns a dictionary mapping available DSNs to their descriptions.";

static char setdecimalsep_doc[] =
    "setDecimalSeparator(string) -> None\n" \
    "\n" \
    "Sets the decimal separator character used when parsing NUMERIC from the database.";

static char getdecimalsep_doc[] =
    "getDecimalSeparator() -> string\n" \
    "\n" \
    "Gets the decimal separator character used when parsing NUMERIC from the database.";


static PyMethodDef pyodbc_methods[] =
{
    { "connect",            (PyCFunction)mod_connect,            METH_VARARGS|METH_KEYWORDS, connect_doc },
    { "TimeFromTicks",      (PyCFunction)mod_timefromticks,      METH_VARARGS,               timefromticks_doc },
    { "DateFromTicks",      (PyCFunction)mod_datefromticks,      METH_VARARGS,               datefromticks_doc },
    { "setDecimalSeparator", (PyCFunction)mod_setdecimalsep,      METH_VARARGS,               setdecimalsep_doc },
    { "getDecimalSeparator", (PyCFunction)mod_getdecimalsep,      METH_NOARGS,               getdecimalsep_doc },
    { "TimestampFromTicks", (PyCFunction)mod_timestampfromticks, METH_VARARGS,               timestampfromticks_doc },
    { "drivers",            (PyCFunction)mod_drivers,            METH_NOARGS,                drivers_doc },
    { "dataSources",        (PyCFunction)mod_datasources,        METH_NOARGS,                datasources_doc },
    { 0, 0, 0, 0 }
};


static void ErrorInit()
{
    // Called during startup to initialize any variables that will be freed by ErrorCleanup.

    Error = 0;
    Warning = 0;
    InterfaceError = 0;
    DatabaseError = 0;
    InternalError = 0;
    OperationalError = 0;
    ProgrammingError = 0;
    IntegrityError = 0;
    DataError = 0;
    NotSupportedError = 0;
}


static void ErrorCleanup()
{
    // Called when an error occurs during initialization to release any objects we may have accessed.  Make sure each
    // item released was initialized to zero.  (Static objects are -- non-statics should be initialized in ErrorInit.)

    Py_XDECREF(Error);
    Py_XDECREF(Warning);
    Py_XDECREF(InterfaceError);
    Py_XDECREF(DatabaseError);
    Py_XDECREF(InternalError);
    Py_XDECREF(OperationalError);
    Py_XDECREF(ProgrammingError);
    Py_XDECREF(IntegrityError);
    Py_XDECREF(DataError);
    Py_XDECREF(NotSupportedError);
}

struct ConstantDef
{
    const char* szName;
    int value;
};

#define MAKECONST(v) { #v, v }

static const ConstantDef aConstants[] = {
    MAKECONST(SQL_WMETADATA),
    MAKECONST(SQL_UNKNOWN_TYPE),
    MAKECONST(SQL_CHAR),
    MAKECONST(SQL_VARCHAR),
    MAKECONST(SQL_LONGVARCHAR),
    MAKECONST(SQL_WCHAR),
    MAKECONST(SQL_WVARCHAR),
    MAKECONST(SQL_WLONGVARCHAR),
    MAKECONST(SQL_DECIMAL),
    MAKECONST(SQL_NUMERIC),
    MAKECONST(SQL_SMALLINT),
    MAKECONST(SQL_INTEGER),
    MAKECONST(SQL_REAL),
    MAKECONST(SQL_FLOAT),
    MAKECONST(SQL_DOUBLE),
    MAKECONST(SQL_BIT),
    MAKECONST(SQL_TINYINT),
    MAKECONST(SQL_BIGINT),
    MAKECONST(SQL_BINARY),
    MAKECONST(SQL_VARBINARY),
    MAKECONST(SQL_LONGVARBINARY),
    MAKECONST(SQL_TYPE_DATE),
    MAKECONST(SQL_TYPE_TIME),
    MAKECONST(SQL_TYPE_TIMESTAMP),
    MAKECONST(SQL_SS_TIME2),
    MAKECONST(SQL_SS_VARIANT),
    MAKECONST(SQL_SS_XML),
    MAKECONST(SQL_INTERVAL_MONTH),
    MAKECONST(SQL_INTERVAL_YEAR),
    MAKECONST(SQL_INTERVAL_YEAR_TO_MONTH),
    MAKECONST(SQL_INTERVAL_DAY),
    MAKECONST(SQL_INTERVAL_HOUR),
    MAKECONST(SQL_INTERVAL_MINUTE),
    MAKECONST(SQL_INTERVAL_SECOND),
    MAKECONST(SQL_INTERVAL_DAY_TO_HOUR),
    MAKECONST(SQL_INTERVAL_DAY_TO_MINUTE),
    MAKECONST(SQL_INTERVAL_DAY_TO_SECOND),
    MAKECONST(SQL_INTERVAL_HOUR_TO_MINUTE),
    MAKECONST(SQL_INTERVAL_HOUR_TO_SECOND),
    MAKECONST(SQL_INTERVAL_MINUTE_TO_SECOND),
    MAKECONST(SQL_GUID),
    MAKECONST(SQL_NULLABLE),
    MAKECONST(SQL_NO_NULLS),
    MAKECONST(SQL_NULLABLE_UNKNOWN),
    // MAKECONST(SQL_INDEX_BTREE),
    // MAKECONST(SQL_INDEX_CLUSTERED),
    // MAKECONST(SQL_INDEX_CONTENT),
    // MAKECONST(SQL_INDEX_HASHED),
    // MAKECONST(SQL_INDEX_OTHER),
    MAKECONST(SQL_SCOPE_CURROW),
    MAKECONST(SQL_SCOPE_TRANSACTION),
    MAKECONST(SQL_SCOPE_SESSION),
    MAKECONST(SQL_PC_UNKNOWN),
    MAKECONST(SQL_PC_NOT_PSEUDO),
    MAKECONST(SQL_PC_PSEUDO),

    // SQLGetInfo
    MAKECONST(SQL_ACCESSIBLE_PROCEDURES),
    MAKECONST(SQL_ACCESSIBLE_TABLES),
    MAKECONST(SQL_ACTIVE_ENVIRONMENTS),
    MAKECONST(SQL_AGGREGATE_FUNCTIONS),
    MAKECONST(SQL_ALTER_DOMAIN),
    MAKECONST(SQL_ALTER_TABLE),
    MAKECONST(SQL_ASYNC_MODE),
    MAKECONST(SQL_BATCH_ROW_COUNT),
    MAKECONST(SQL_BATCH_SUPPORT),
    MAKECONST(SQL_BOOKMARK_PERSISTENCE),
    MAKECONST(SQL_CATALOG_LOCATION),
    MAKECONST(SQL_CATALOG_NAME),
    MAKECONST(SQL_CATALOG_NAME_SEPARATOR),
    MAKECONST(SQL_CATALOG_TERM),
    MAKECONST(SQL_CATALOG_USAGE),
    MAKECONST(SQL_COLLATION_SEQ),
    MAKECONST(SQL_COLUMN_ALIAS),
    MAKECONST(SQL_CONCAT_NULL_BEHAVIOR),
    MAKECONST(SQL_CONVERT_VARCHAR),
    MAKECONST(SQL_CORRELATION_NAME),
    MAKECONST(SQL_CREATE_ASSERTION),
    MAKECONST(SQL_CREATE_CHARACTER_SET),
    MAKECONST(SQL_CREATE_COLLATION),
    MAKECONST(SQL_CREATE_DOMAIN),
    MAKECONST(SQL_CREATE_SCHEMA),
    MAKECONST(SQL_CREATE_TABLE),
    MAKECONST(SQL_CREATE_TRANSLATION),
    MAKECONST(SQL_CREATE_VIEW),
    MAKECONST(SQL_CURSOR_COMMIT_BEHAVIOR),
    MAKECONST(SQL_CURSOR_ROLLBACK_BEHAVIOR),
    // MAKECONST(SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY),
    MAKECONST(SQL_DATABASE_NAME),
    MAKECONST(SQL_DATA_SOURCE_NAME),
    MAKECONST(SQL_DATA_SOURCE_READ_ONLY),
    MAKECONST(SQL_DATETIME_LITERALS),
    MAKECONST(SQL_DBMS_NAME),
    MAKECONST(SQL_DBMS_VER),
    MAKECONST(SQL_DDL_INDEX),
    MAKECONST(SQL_DEFAULT_TXN_ISOLATION),
    MAKECONST(SQL_DESCRIBE_PARAMETER),
    MAKECONST(SQL_DM_VER),
    MAKECONST(SQL_DRIVER_HDESC),
    MAKECONST(SQL_DRIVER_HENV),
    MAKECONST(SQL_DRIVER_HLIB),
    MAKECONST(SQL_DRIVER_HSTMT),
    MAKECONST(SQL_DRIVER_NAME),
    MAKECONST(SQL_DRIVER_ODBC_VER),
    MAKECONST(SQL_DRIVER_VER),
    MAKECONST(SQL_DROP_ASSERTION),
    MAKECONST(SQL_DROP_CHARACTER_SET),
    MAKECONST(SQL_DROP_COLLATION),
    MAKECONST(SQL_DROP_DOMAIN),
    MAKECONST(SQL_DROP_SCHEMA),
    MAKECONST(SQL_DROP_TABLE),
    MAKECONST(SQL_DROP_TRANSLATION),
    MAKECONST(SQL_DROP_VIEW),
    MAKECONST(SQL_DYNAMIC_CURSOR_ATTRIBUTES1),
    MAKECONST(SQL_DYNAMIC_CURSOR_ATTRIBUTES2),
    MAKECONST(SQL_EXPRESSIONS_IN_ORDERBY),
    MAKECONST(SQL_FILE_USAGE),
    MAKECONST(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1),
    MAKECONST(SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2),
    MAKECONST(SQL_GETDATA_EXTENSIONS),
    MAKECONST(SQL_GROUP_BY),
    MAKECONST(SQL_IDENTIFIER_CASE),
    MAKECONST(SQL_IDENTIFIER_QUOTE_CHAR),
    MAKECONST(SQL_INDEX_KEYWORDS),
    MAKECONST(SQL_INFO_SCHEMA_VIEWS),
    MAKECONST(SQL_INSERT_STATEMENT),
    MAKECONST(SQL_INTEGRITY),
    MAKECONST(SQL_KEYSET_CURSOR_ATTRIBUTES1),
    MAKECONST(SQL_KEYSET_CURSOR_ATTRIBUTES2),
    MAKECONST(SQL_KEYWORDS),
    MAKECONST(SQL_LIKE_ESCAPE_CLAUSE),
    MAKECONST(SQL_MAX_ASYNC_CONCURRENT_STATEMENTS),
    MAKECONST(SQL_MAX_BINARY_LITERAL_LEN),
    MAKECONST(SQL_MAX_CATALOG_NAME_LEN),
    MAKECONST(SQL_MAX_CHAR_LITERAL_LEN),
    MAKECONST(SQL_MAX_COLUMNS_IN_GROUP_BY),
    MAKECONST(SQL_MAX_COLUMNS_IN_INDEX),
    MAKECONST(SQL_MAX_COLUMNS_IN_ORDER_BY),
    MAKECONST(SQL_MAX_COLUMNS_IN_SELECT),
    MAKECONST(SQL_MAX_COLUMNS_IN_TABLE),
    MAKECONST(SQL_MAX_COLUMN_NAME_LEN),
    MAKECONST(SQL_MAX_CONCURRENT_ACTIVITIES),
    MAKECONST(SQL_MAX_CURSOR_NAME_LEN),
    MAKECONST(SQL_MAX_DRIVER_CONNECTIONS),
    MAKECONST(SQL_MAX_IDENTIFIER_LEN),
    MAKECONST(SQL_MAX_INDEX_SIZE),
    MAKECONST(SQL_MAX_PROCEDURE_NAME_LEN),
    MAKECONST(SQL_MAX_ROW_SIZE),
    MAKECONST(SQL_MAX_ROW_SIZE_INCLUDES_LONG),
    MAKECONST(SQL_MAX_SCHEMA_NAME_LEN),
    MAKECONST(SQL_MAX_STATEMENT_LEN),
    MAKECONST(SQL_MAX_TABLES_IN_SELECT),
    MAKECONST(SQL_MAX_TABLE_NAME_LEN),
    MAKECONST(SQL_MAX_USER_NAME_LEN),
    MAKECONST(SQL_MULTIPLE_ACTIVE_TXN),
    MAKECONST(SQL_MULT_RESULT_SETS),
    MAKECONST(SQL_NEED_LONG_DATA_LEN),
    MAKECONST(SQL_NON_NULLABLE_COLUMNS),
    MAKECONST(SQL_NULL_COLLATION),
    MAKECONST(SQL_NUMERIC_FUNCTIONS),
    MAKECONST(SQL_ODBC_INTERFACE_CONFORMANCE),
    MAKECONST(SQL_ODBC_VER),
    MAKECONST(SQL_OJ_CAPABILITIES),
    MAKECONST(SQL_ORDER_BY_COLUMNS_IN_SELECT),
    MAKECONST(SQL_PARAM_ARRAY_ROW_COUNTS),
    MAKECONST(SQL_PARAM_ARRAY_SELECTS),
    MAKECONST(SQL_PARAM_TYPE_UNKNOWN),
    MAKECONST(SQL_PARAM_INPUT),
    MAKECONST(SQL_PARAM_INPUT_OUTPUT),
    MAKECONST(SQL_PARAM_OUTPUT),
    MAKECONST(SQL_RETURN_VALUE),
    MAKECONST(SQL_RESULT_COL),
    MAKECONST(SQL_PROCEDURES),
    MAKECONST(SQL_PROCEDURE_TERM),
    MAKECONST(SQL_QUOTED_IDENTIFIER_CASE),
    MAKECONST(SQL_ROW_UPDATES),
    MAKECONST(SQL_SCHEMA_TERM),
    MAKECONST(SQL_SCHEMA_USAGE),
    MAKECONST(SQL_SCROLL_OPTIONS),
    MAKECONST(SQL_SEARCH_PATTERN_ESCAPE),
    MAKECONST(SQL_SERVER_NAME),
    MAKECONST(SQL_SPECIAL_CHARACTERS),
    MAKECONST(SQL_SQL92_DATETIME_FUNCTIONS),
    MAKECONST(SQL_SQL92_FOREIGN_KEY_DELETE_RULE),
    MAKECONST(SQL_SQL92_FOREIGN_KEY_UPDATE_RULE),
    MAKECONST(SQL_SQL92_GRANT),
    MAKECONST(SQL_SQL92_NUMERIC_VALUE_FUNCTIONS),
    MAKECONST(SQL_SQL92_PREDICATES),
    MAKECONST(SQL_SQL92_RELATIONAL_JOIN_OPERATORS),
    MAKECONST(SQL_SQL92_REVOKE),
    MAKECONST(SQL_SQL92_ROW_VALUE_CONSTRUCTOR),
    MAKECONST(SQL_SQL92_STRING_FUNCTIONS),
    MAKECONST(SQL_SQL92_VALUE_EXPRESSIONS),
    MAKECONST(SQL_SQL_CONFORMANCE),
    MAKECONST(SQL_STANDARD_CLI_CONFORMANCE),
    MAKECONST(SQL_STATIC_CURSOR_ATTRIBUTES1),
    MAKECONST(SQL_STATIC_CURSOR_ATTRIBUTES2),
    MAKECONST(SQL_STRING_FUNCTIONS),
    MAKECONST(SQL_SUBQUERIES),
    MAKECONST(SQL_SYSTEM_FUNCTIONS),
    MAKECONST(SQL_TABLE_TERM),
    MAKECONST(SQL_TIMEDATE_ADD_INTERVALS),
    MAKECONST(SQL_TIMEDATE_DIFF_INTERVALS),
    MAKECONST(SQL_TIMEDATE_FUNCTIONS),
    MAKECONST(SQL_TXN_CAPABLE),
    MAKECONST(SQL_TXN_ISOLATION_OPTION),
    MAKECONST(SQL_UNION),
    MAKECONST(SQL_USER_NAME),
    MAKECONST(SQL_XOPEN_CLI_YEAR),

    // Connection Attributes
    MAKECONST(SQL_ACCESS_MODE), MAKECONST(SQL_ATTR_ACCESS_MODE),
    MAKECONST(SQL_AUTOCOMMIT), MAKECONST(SQL_ATTR_AUTOCOMMIT),
    MAKECONST(SQL_LOGIN_TIMEOUT), MAKECONST(SQL_ATTR_LOGIN_TIMEOUT),
    MAKECONST(SQL_OPT_TRACE), MAKECONST(SQL_ATTR_TRACE),
    MAKECONST(SQL_OPT_TRACEFILE), MAKECONST(SQL_ATTR_TRACEFILE),
    MAKECONST(SQL_TRANSLATE_DLL), MAKECONST(SQL_ATTR_TRANSLATE_LIB),
    MAKECONST(SQL_TRANSLATE_OPTION), MAKECONST(SQL_ATTR_TRANSLATE_OPTION),
    MAKECONST(SQL_TXN_ISOLATION), MAKECONST(SQL_ATTR_TXN_ISOLATION),
    MAKECONST(SQL_CURRENT_QUALIFIER), MAKECONST(SQL_ATTR_CURRENT_CATALOG),
    MAKECONST(SQL_ODBC_CURSORS), MAKECONST(SQL_ATTR_ODBC_CURSORS),
    MAKECONST(SQL_QUIET_MODE), MAKECONST(SQL_ATTR_QUIET_MODE),
    MAKECONST(SQL_PACKET_SIZE),
    MAKECONST(SQL_ATTR_ANSI_APP),

    // SQL_CONVERT_X
    MAKECONST(SQL_CONVERT_FUNCTIONS),
    MAKECONST(SQL_CONVERT_BIGINT),
    MAKECONST(SQL_CONVERT_BINARY),
    MAKECONST(SQL_CONVERT_BIT),
    MAKECONST(SQL_CONVERT_CHAR),
    MAKECONST(SQL_CONVERT_DATE),
    MAKECONST(SQL_CONVERT_DECIMAL),
    MAKECONST(SQL_CONVERT_DOUBLE),
    MAKECONST(SQL_CONVERT_FLOAT),
    MAKECONST(SQL_CONVERT_GUID),
    MAKECONST(SQL_CONVERT_INTEGER),
    MAKECONST(SQL_CONVERT_INTERVAL_DAY_TIME),
    MAKECONST(SQL_CONVERT_INTERVAL_YEAR_MONTH),
    MAKECONST(SQL_CONVERT_LONGVARBINARY),
    MAKECONST(SQL_CONVERT_LONGVARCHAR),
    MAKECONST(SQL_CONVERT_NUMERIC),
    MAKECONST(SQL_CONVERT_REAL),
    MAKECONST(SQL_CONVERT_SMALLINT),
    MAKECONST(SQL_CONVERT_TIME),
    MAKECONST(SQL_CONVERT_TIMESTAMP),
    MAKECONST(SQL_CONVERT_TINYINT),
    MAKECONST(SQL_CONVERT_VARBINARY),
    MAKECONST(SQL_CONVERT_VARCHAR),
    MAKECONST(SQL_CONVERT_WCHAR),
    MAKECONST(SQL_CONVERT_WLONGVARCHAR),
    MAKECONST(SQL_CONVERT_WVARCHAR),

    // SQLSetConnectAttr transaction isolation
    MAKECONST(SQL_ATTR_TXN_ISOLATION),
    MAKECONST(SQL_TXN_READ_UNCOMMITTED),
    MAKECONST(SQL_TXN_READ_COMMITTED),
    MAKECONST(SQL_TXN_REPEATABLE_READ),
    MAKECONST(SQL_TXN_SERIALIZABLE),

    // Outer Join Capabilities
    MAKECONST(SQL_OJ_LEFT),
    MAKECONST(SQL_OJ_RIGHT),
    MAKECONST(SQL_OJ_FULL),
    MAKECONST(SQL_OJ_NESTED),
    MAKECONST(SQL_OJ_NOT_ORDERED),
    MAKECONST(SQL_OJ_INNER),
    MAKECONST(SQL_OJ_ALL_COMPARISON_OPS),
};


static bool CreateExceptions()
{
    for (unsigned int i = 0; i < _countof(aExcInfos); i++)
    {
        ExcInfo& info = aExcInfos[i];

        PyObject* classdict = PyDict_New();
        if (!classdict)
            return false;

        PyObject* doc = PyUnicode_FromString(info.szDoc);
        if (!doc)
        {
            Py_DECREF(classdict);
            return false;
        }

        PyDict_SetItemString(classdict, "__doc__", doc);
        Py_DECREF(doc);

        *info.ppexc = PyErr_NewException((char*)info.szFullName, *info.ppexcParent, classdict);
        if (*info.ppexc == 0)
        {
            Py_DECREF(classdict);
            return false;
        }

        // Keep a reference for our internal (C++) use.
        Py_INCREF(*info.ppexc);

        PyModule_AddObject(pModule, (char*)info.szName, *info.ppexc);
    }

    return true;
}

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "pyodbc",                   // m_name
    module_doc,
    -1,                         // m_size
    pyodbc_methods,             // m_methods
    0,                          // m_reload
    0,                          // m_traverse
    0,                          // m_clear
    0,                          // m_free
};


PyMODINIT_FUNC PyInit_pyodbc()
{
    ErrorInit();

    if (PyType_Ready(&ConnectionType) < 0 || PyType_Ready(&CursorType) < 0 || PyType_Ready(&RowType) < 0 || PyType_Ready(&CnxnInfoType) < 0)
        return 0;

    Object module;

    module.Attach(PyModule_Create(&moduledef));

    pModule = module.Get();

    if (!module || !import_types() || !CreateExceptions())
        return 0;

    const char* szVersion = TOSTRING(PYODBC_VERSION);
    PyModule_AddStringConstant(module, "version", (char*)szVersion);

    PyModule_AddIntConstant(module, "threadsafety", 1);
    PyModule_AddStringConstant(module, "apilevel", "2.0");
    PyModule_AddStringConstant(module, "paramstyle", "qmark");
    PyModule_AddStringConstant(module, "odbcversion", "3.X");
    PyModule_AddObject(module, "pooling", Py_True);
    Py_INCREF(Py_True);
    PyModule_AddObject(module, "lowercase", Py_False);
    Py_INCREF(Py_False);
    PyModule_AddObject(module, "native_uuid", Py_False);
    Py_INCREF(Py_False);

    PyModule_AddObject(module, "Connection", (PyObject*)&ConnectionType);
    Py_INCREF((PyObject*)&ConnectionType);
    PyModule_AddObject(module, "Cursor", (PyObject*)&CursorType);
    Py_INCREF((PyObject*)&CursorType);
    PyModule_AddObject(module, "Row", (PyObject*)&RowType);
    Py_INCREF((PyObject*)&RowType);

    // Add the SQL_XXX defines from ODBC.
    for (unsigned int i = 0; i < _countof(aConstants); i++)
        PyModule_AddIntConstant(module, (char*)aConstants[i].szName, aConstants[i].value);

    PyModule_AddObject(module, "Date", (PyObject*)PyDateTimeAPI->DateType);
    Py_INCREF((PyObject*)PyDateTimeAPI->DateType);
    PyModule_AddObject(module, "Time", (PyObject*)PyDateTimeAPI->TimeType);
    Py_INCREF((PyObject*)PyDateTimeAPI->TimeType);
    PyModule_AddObject(module, "Timestamp", (PyObject*)PyDateTimeAPI->DateTimeType);
    Py_INCREF((PyObject*)PyDateTimeAPI->DateTimeType);
    PyModule_AddObject(module, "DATETIME", (PyObject*)PyDateTimeAPI->DateTimeType);
    Py_INCREF((PyObject*)PyDateTimeAPI->DateTimeType);
    PyModule_AddObject(module, "STRING", (PyObject*)&PyUnicode_Type);
    Py_INCREF((PyObject*)&PyUnicode_Type);
    PyModule_AddObject(module, "NUMBER", (PyObject*)&PyFloat_Type);
    Py_INCREF((PyObject*)&PyFloat_Type);
    PyModule_AddObject(module, "ROWID", (PyObject*)&PyLong_Type);
    Py_INCREF((PyObject*)&PyLong_Type);

    PyObject* binary_type;
    binary_type = (PyObject*)&PyByteArray_Type;
    PyModule_AddObject(module, "BINARY", binary_type);
    Py_INCREF(binary_type);
    PyModule_AddObject(module, "Binary", binary_type);
    Py_INCREF(binary_type);

    assert(null_binary != 0);        // must be initialized first
    PyModule_AddObject(module, "BinaryNull", null_binary);

    PyModule_AddIntConstant(module, "SQLWCHAR_SIZE", sizeof(SQLWCHAR));

    if (!PyErr_Occurred())
    {
        module.Detach();
    }
    else
    {
        ErrorCleanup();
    }

    return pModule;
}


#ifdef WINVER
BOOL WINAPI DllMain(HINSTANCE hMod, DWORD fdwReason, LPVOID lpvReserved)
{
    UNUSED(hMod, fdwReason, lpvReserved);
    return TRUE;
}
#endif


static PyObject* MakeConnectionString(PyObject* existing, PyObject* parts)
{
    // Creates a connection string from an optional existing connection string plus a dictionary of keyword value
    // pairs.
    //
    // existing
    //   Optional Unicode connection string we will be appending to.  Used when a partial connection string is passed
    //   in, followed by keyword parameters:
    //
    //   connect("driver={x};database={y}", user='z')
    //
    // parts
    //   A dictionary of text keywords and text values that will be appended.

    Py_ssize_t pos = 0;
    PyObject* key = 0;
    PyObject* value = 0;
    Py_ssize_t length = 0;      // length in *characters*
    int result_kind = PyUnicode_1BYTE_KIND;
    if (existing) {
        assert(PyUnicode_Check(existing));
        length = PyUnicode_GET_LENGTH(existing) + 1; // + 1 to add a trailing semicolon
        int kind = PyUnicode_KIND(existing);
        if (result_kind < kind)
            result_kind = kind;
    }

    while (PyDict_Next(parts, &pos, &key, &value))
    {
        // key=value;
        length += PyUnicode_GET_LENGTH(key) + 1;
        length += PyUnicode_GET_LENGTH(value) + 1;
        int kind = PyUnicode_KIND(key);
        if (result_kind < kind)
            result_kind = kind;
        kind = PyUnicode_KIND(value);
        if (result_kind < kind)
            result_kind = kind;
    }

    Py_UCS4 maxchar = 0x10ffff;
    if (result_kind == PyUnicode_2BYTE_KIND)
        maxchar = 0xffff;
    else if (result_kind == PyUnicode_1BYTE_KIND)
        maxchar = 0xff;
    PyObject* result = PyUnicode_New(length, maxchar);
    if (!result)
        return 0;

    Py_ssize_t offset = 0;
    if (existing)
    {
        Py_ssize_t count = PyUnicode_GET_LENGTH(existing);
        Py_ssize_t n = PyUnicode_CopyCharacters(result, offset, existing, 0,
                                                count);
        if (n < 0)
            return 0;
        offset += count;
        PyUnicode_WriteChar(result, offset++, (Py_UCS4)';');
    }

    pos = 0;
    while (PyDict_Next(parts, &pos, &key, &value))
    {
        Py_ssize_t count = PyUnicode_GET_LENGTH(key);
        Py_ssize_t n = PyUnicode_CopyCharacters(result, offset, key, 0, count);
        if (n < 0)
            return 0;
        offset += count;
        PyUnicode_WriteChar(result, offset++, (Py_UCS4)'=');
        count = PyUnicode_GET_LENGTH(value);
        n = PyUnicode_CopyCharacters(result, offset, value, 0, count);
        if (n < 0)
            return 0;
        offset += count;
        PyUnicode_WriteChar(result, offset++, (Py_UCS4)';');
    }

    assert(offset == length);

    return result;
}
