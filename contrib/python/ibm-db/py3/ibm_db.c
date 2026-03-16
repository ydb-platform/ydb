/*
+--------------------------------------------------------------------------+
| Licensed Materials - Property of IBM                                     |
|                                                                          |
| (C) Copyright IBM Corporation 2006-2020                                 |
+--------------------------------------------------------------------------+
| This module complies with SQLAlchemy 0.4 and is                          |
| Licensed under the Apache License, Version 2.0 (the "License");          |
| you may not use this file except in compliance with the License.         |
| You may obtain a copy of the License at                                  |
| http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
| law or agreed to in writing, software distributed under the License is   |
| distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
| KIND, either express or implied. See the License for the specific        |
| language governing permissions and limitations under the License.        |
+--------------------------------------------------------------------------+
| Authors: Manas Dadarkar, Salvador Ledezma, Sushant Koduru,               |
|   Lynh Nguyen, Kanchana Padmanabhan, Dan Scott, Helmut Tessarek,         |
|   Sam Ruby, Kellen Bombardier, Tony Cairns, Abhigyan Agrawal,            |
|   Tarun Pasrija, Rahul Priyadarshi, Akshay Anand, Saba Kauser ,          |
|   Hemlata Bhatt                                                          |
+--------------------------------------------------------------------------+
*/

#define MODULE_RELEASE "3.2.8"

#include <Python.h>
#include <datetime.h>
#include "ibm_db.h"
#include <ctype.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif
#ifdef _LP64
#ifdef __MVS__
#define BIGINT_IS_SHORTER_THAN_LONG 0
#else
#define BIGINT_IS_SHORTER_THAN_LONG 1
#endif
#else
#define BIGINT_IS_SHORTER_THAN_LONG 1
#endif
/* MAX length for DECFLOAT */
#define MAX_DECFLOAT_LENGTH 44

/* True global resources - no need for thread safety here */
static struct _ibm_db_globals *ibm_db_globals;

static void _python_ibm_db_check_sql_errors(SQLHANDLE handle, SQLSMALLINT hType, int rc, int cpy_to_global, char *ret_str, int API, SQLSMALLINT recno);
static int _python_ibm_db_assign_options(void *handle, int type, long opt_key, PyObject *data);
static SQLWCHAR *getUnicodeDataAsSQLWCHAR(PyObject *pyobj, int *isNewBuffer);
static SQLCHAR *getUnicodeDataAsSQLCHAR(PyObject *pyobj, int *isNewBuffer);
static PyObject *getSQLWCharAsPyUnicodeObject(SQLWCHAR *sqlwcharData, int sqlwcharBytesLen);

const int _check_i = 1;
#define is_bigendian() ((*(char *)&_check_i) == 0)
static int is_systemi, is_informix; /* 1 == TRUE; 0 == FALSE; */

#ifdef _WIN32
#define DLOPEN LoadLibrary
#define DLSYM GetProcAddress
#define DLCLOSE FreeLibrary
#ifdef _WIN64
#define LIBDB2 "db2cli64.dll"
#else
#define LIBDB2 "db2cli.dll"
#endif
#elif _AIX
#define DLOPEN dlopen
#define DLSYM dlsym
#define DLCLOSE dlclose
#ifdef __64BIT__
/*64-bit library in the archive libdb2.a*/
#define LIBDB2 "libdb2.a(shr_64.o)"
#else
/*32-bit library in the archive libdb2.a*/
#define LIBDB2 "libdb2.a(shr.o)"
#endif
#elif __APPLE__
#define DLOPEN dlopen
#define DLSYM dlsym
#define DLCLOSE dlclose
#define LIBDB2 "libdb2.dylib"
#else
#define DLOPEN dlopen
#define DLSYM dlsym
#define DLCLOSE dlclose
#define LIBDB2 "libdb2.so.1"
#endif

// Define macros for log levels
#define DEBUG "DEBUG"
#define INFO "INFO"
#define WARNING "WARNING"
#define ERROR "ERROR"
#define EXCEPTION "EXCEPTION"

static int debug_mode = 0;
static char *fileName = NULL;
static char messageStr[2024];

// LogMsg function
static void LogMsg(const char *log_level, const char *message)
{
    // logging if in debug mode
    if (debug_mode)
    {
        // Print formatted log message
        if (fileName == NULL)
        {
            printf("[%s] - %s\n", log_level, message);
        }
        else
        {
            FILE *file = fopen(fileName, "a");
            if (file != NULL)
            {
                fprintf(file, "[%s] - %s\n", log_level, message);
                fclose(file);
            }
            else
            {
                printf("Failed to open log file: %s\n", fileName);
            }
        }
    }
}

static void LogUTF8Msg(PyObject *args)
{
    if(debug_mode)
    {
        PyObject *argsStr = PyObject_Repr(args);
        snprintf(messageStr, sizeof(messageStr), "Received arguments: %s", PyUnicode_AsUTF8(argsStr));
        LogMsg(INFO, messageStr);
        Py_XDECREF(argsStr);
    }
}

/* Defines a linked list structure for error messages */
typedef struct _error_msg_node
{
    char err_msg[DB2_MAX_ERR_MSG_LEN];
    struct _error_msg_node *next;
} error_msg_node;

/* Defines a linked list structure for caching param data */
typedef struct _param_cache_node
{
    SQLSMALLINT data_type;            /* Datatype */
    SQLUINTEGER param_size;           /* param size */
    SQLSMALLINT nullable;             /* is Nullable */
    SQLSMALLINT scale;                /* Decimal scale */
    SQLUINTEGER file_options;         /* File options if PARAM_FILE */
    SQLINTEGER bind_indicator;        /* indicator variable for SQLBindParameter */
    int param_num;                    /* param number in stmt */
    int param_type;                   /* Type of param - INP/OUT/INP-OUT/FILE */
    int size;                         /* Size of param */
    char *varname;                    /* bound variable name */
    PyObject *var_pyvalue;            /* bound variable value */
    SQLUINTEGER ivalue;                /* Temp storage value */
    double fvalue;                    /* Temp storage value */
    char *svalue;                     /* Temp storage value */
    SQLWCHAR *uvalue;                 /* Temp storage value */
    DATE_STRUCT *date_value;          /* Temp storage value */
    TIME_STRUCT *time_value;          /* Temp storage value */
    TIMESTAMP_STRUCT *ts_value;       /* Temp storage value */
    TIMESTAMP_STRUCT_EXT_TZ *tstz_value;    /* Temp storage value */
    SQLINTEGER *ivalueArray;          /* Temp storage array of values */
    double *fvalueArray;              /* Temp storage array of values */
    SQLINTEGER *bind_indicator_array; /* Temp storage array of values */
    struct _param_cache_node *next;   /* Pointer to next node */
} param_node;

typedef struct _conn_handle_struct
{
    PyObject_HEAD
        SQLHANDLE henv;
    SQLHANDLE hdbc;
    long auto_commit;
    long c_bin_mode;
    long c_case_mode;
    long c_cursor_type;
    long c_use_wchar;
    int handle_active;
    SQLSMALLINT error_recno_tracker;
    SQLSMALLINT errormsg_recno_tracker;
    int flag_pconnect; /* Indicates that this connection is persistent */
} conn_handle;

static void _python_ibm_db_free_conn_struct(conn_handle *handle);

static PyTypeObject conn_handleType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name           */ "ibm_db.IBM_DBConnection",
    /* tp_basicsize      */ sizeof(conn_handle),
    /* tp_itemsize       */ 0,
    /* tp_dealloc        */ (destructor)_python_ibm_db_free_conn_struct,
    /* tp_print          */ 0,
    /* tp_getattr        */ 0,
    /* tp_setattr        */ 0,
    /* tp_compare        */ 0,
    /* tp_repr           */ 0,
    /* tp_as_number      */ 0,
    /* tp_as_sequence    */ 0,
    /* tp_as_mapping     */ 0,
    /* tp_hash           */ 0,
    /* tp_call           */ 0,
    /* tp_str            */ 0,
    /* tp_getattro       */ 0,
    /* tp_setattro       */ 0,
    /* tp_as_buffer      */ 0,
    /* tp_flags          */ Py_TPFLAGS_DEFAULT,
    /* tp_doc            */ "IBM DataServer connection object",
    /* tp_traverse       */ 0,
    /* tp_clear          */ 0,
    /* tp_richcompare    */ 0,
    /* tp_weaklistoffset */ 0,
    /* tp_iter           */ 0,
    /* tp_iternext       */ 0,
    /* tp_methods        */ 0,
    /* tp_members        */ 0,
    /* tp_getset         */ 0,
    /* tp_base           */ 0,
    /* tp_dict           */ 0,
    /* tp_descr_get      */ 0,
    /* tp_descr_set      */ 0,
    /* tp_dictoffset     */ 0,
    /* tp_init           */ 0,
};

typedef union
{
    SQLINTEGER i_val;
    SQLDOUBLE d_val;
    SQLFLOAT f_val;
    SQLSMALLINT s_val;
    SQLCHAR *str_val;
    SQLREAL r_val;
    SQLWCHAR *w_val;
    TIMESTAMP_STRUCT *ts_val;
    TIMESTAMP_STRUCT_EXT_TZ *tstz_val;
    DATE_STRUCT *date_val;
    TIME_STRUCT *time_val;
} ibm_db_row_data_type;

typedef struct
{
    SQLINTEGER out_length;
    ibm_db_row_data_type data;
} ibm_db_row_type;

typedef struct _ibm_db_result_set_info_struct
{
    SQLCHAR *name;
    SQLSMALLINT type;
    SQLUINTEGER size;
    SQLSMALLINT scale;
    SQLSMALLINT nullable;
    unsigned char *mem_alloc; /* Mem free */
} ibm_db_result_set_info;

typedef struct _row_hash_struct
{
    PyObject *hash;
} row_hash_struct;

typedef struct _stmt_handle_struct
{
    PyObject_HEAD
        SQLHANDLE hdbc;
    SQLHANDLE hstmt;
    long s_bin_mode;
    long cursor_type;
    long s_case_mode;
    long s_use_wchar;
    SQLSMALLINT error_recno_tracker;
    SQLSMALLINT errormsg_recno_tracker;

    /* Parameter Caching variables */
    param_node *head_cache_list;
    param_node *current_node;

    int num_params; /* Number of Params */
    int file_param; /* if option passed in is FILE_PARAM */
    int num_columns;
    ibm_db_result_set_info *column_info;
    ibm_db_row_type *row_data;
} stmt_handle;

static void _python_ibm_db_free_stmt_struct(stmt_handle *handle);

static PyTypeObject stmt_handleType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /* tp_name           */ "ibm_db.IBM_DBStatement",
    /* tp_basicsize      */ sizeof(stmt_handle),
    /* tp_itemsize       */ 0,
    /* tp_dealloc        */ (destructor)_python_ibm_db_free_stmt_struct,
    /* tp_print          */ 0,
    /* tp_getattr        */ 0,
    /* tp_setattr        */ 0,
    /* tp_compare        */ 0,
    /* tp_repr           */ 0,
    /* tp_as_number      */ 0,
    /* tp_as_sequence    */ 0,
    /* tp_as_mapping     */ 0,
    /* tp_hash           */ 0,
    /* tp_call           */ 0,
    /* tp_str            */ 0,
    /* tp_getattro       */ 0,
    /* tp_setattro       */ 0,
    /* tp_as_buffer      */ 0,
    /* tp_flags          */ Py_TPFLAGS_DEFAULT,
    /* tp_doc            */ "IBM DataServer cursor object",
    /* tp_traverse       */ 0,
    /* tp_clear          */ 0,
    /* tp_richcompare    */ 0,
    /* tp_weaklistoffset */ 0,
    /* tp_iter           */ 0,
    /* tp_iternext       */ 0,
    /* tp_methods        */ 0,
    /* tp_members        */ 0,
    /* tp_getset         */ 0,
    /* tp_base           */ 0,
    /* tp_dict           */ 0,
    /* tp_descr_get      */ 0,
    /* tp_descr_set      */ 0,
    /* tp_dictoffset     */ 0,
    /* tp_init           */ 0,
};

/* equivalent functions on different platforms */
#ifdef _WIN32
#define STRCASECMP stricmp
#else
#define STRCASECMP strcasecmp
#endif

static void python_ibm_db_init_globals(struct _ibm_db_globals *ibm_db_globals)
{
    /* env handle */
    ibm_db_globals->bin_mode = 1;

    memset(ibm_db_globals->__python_conn_err_msg, 0, DB2_MAX_ERR_MSG_LEN);
    memset(ibm_db_globals->__python_stmt_err_msg, 0, DB2_MAX_ERR_MSG_LEN);
    memset(ibm_db_globals->__python_conn_err_state, 0, SQL_SQLSTATE_SIZE + 1);
    memset(ibm_db_globals->__python_stmt_err_state, 0, SQL_SQLSTATE_SIZE + 1);
    memset(ibm_db_globals->__python_err_code, 0, SQL_SQLCODE_SIZE + 1);
}

static PyObject *persistent_list;
static PyObject *os_getpid;

char *estrdup(char *data)
{
    int len = strlen(data);
    char *dup = ALLOC_N(char, len + 1);
    if (dup == NULL)
    {
        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
        return NULL;
    }
    strcpy(dup, data);
    return dup;
}

char *estrndup(char *data, int max)
{
    int len = strlen(data);
    char *dup;
    if (len > max)
    {
        len = max;
    }
    dup = ALLOC_N(char, len + 1);
    if (dup == NULL)
    {
        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
        return NULL;
    }
    strcpy(dup, data);
    return dup;
}

char *strtolower(char *data, int max)
{
    while (max--)
    {
        data[max] = tolower(data[max]);
    }
    return data;
}

char *strtoupper(char *data, int max)
{
    while (max--)
    {
        data[max] = toupper(data[max]);
    }
    return data;
}

PyObject* format_timestamp_pystr(const TIMESTAMP_STRUCT_EXT_TZ* ts) {
    char formatted[160];
    char sign;
    int tz_hour_abs;
    int tz_minute_abs;
    LogMsg(INFO, "Entry format_timestamp_pystr()");
    if (ts->timezone_hour < -14 || ts->timezone_hour > 14 ||
        ts->timezone_minute < -59 || ts->timezone_minute > 59) {
        char error_msg[64];
        snprintf(error_msg, sizeof(error_msg),
             "Invalid timezone offset detected: %d:%02d (allowed: -14:00 to +14:59)",
             ts->timezone_hour, ts->timezone_minute);
        LogMsg(EXCEPTION, error_msg);
        PyErr_SetString(PyExc_ValueError, error_msg);
        return NULL;
    }

    sign = (ts->timezone_hour < 0) ? '-' : '+';
    tz_hour_abs = abs(ts->timezone_hour);
    tz_minute_abs = abs(ts->timezone_minute);

    unsigned long long full_fraction =
        ((unsigned long long)ts->fraction * 1000ULL) + ts->fraction2;  // 12 digits total

     snprintf(formatted, sizeof(formatted),
             "%04d-%02d-%02d-%02d.%02d.%02d.%012llu %c%02d:%02d",
             ts->year, ts->month, ts->day,
             ts->hour, ts->minute, ts->second,
             full_fraction,  // print 12-digit precision
             sign, tz_hour_abs, tz_minute_abs);
    snprintf(messageStr, sizeof(messageStr),"Final formatted string:%s",formatted);
    LogMsg(INFO, messageStr);
    LogMsg(INFO, "exit format_timestamp_pystr()");
    return PyUnicode_FromString(formatted);
}


/*    static void _python_ibm_db_free_conn_struct */
static void _python_ibm_db_free_conn_struct(conn_handle *handle)
{
    LogMsg(INFO, "entry _python_ibm_db_free_conn_struct");
    /* Disconnect from DB. If stmt is allocated, it is freed automatically */
    snprintf(messageStr, sizeof(messageStr), "Handle details: handle_active=%d, flag_pconnect=%d, auto_commit=%d",
             handle->handle_active, handle->flag_pconnect, handle->auto_commit);
    LogMsg(DEBUG, messageStr);
    if (handle->handle_active && !handle->flag_pconnect)
    {
        if (handle->auto_commit == 0)
        {
            Py_BEGIN_ALLOW_THREADS;
            SQLEndTran(SQL_HANDLE_DBC, (SQLHDBC)handle->hdbc, SQL_ROLLBACK);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLEndTran called with SQL_HANDLE_DBC=%d, hdbc=%p, SQL_ROLLBACK=%d",
                     SQL_HANDLE_DBC, (void *)handle->hdbc, SQL_ROLLBACK);
            LogMsg(DEBUG, messageStr);
        }
        Py_BEGIN_ALLOW_THREADS;
        SQLDisconnect((SQLHDBC)handle->hdbc);
        snprintf(messageStr, sizeof(messageStr), "SQLDisconnect called with hdbc=%p", (void *)handle->hdbc);
        LogMsg(DEBUG, messageStr);
        SQLFreeHandle(SQL_HANDLE_DBC, handle->hdbc);
        snprintf(messageStr, sizeof(messageStr), "SQLFreeHandle called with SQL_HANDLE_DBC=%d, handle_hdbc=%p", SQL_HANDLE_DBC, (void *)handle->hdbc);
        LogMsg(DEBUG, messageStr);
        SQLFreeHandle(SQL_HANDLE_ENV, handle->henv);
        snprintf(messageStr, sizeof(messageStr), "SQLFreeHandle called with SQL_HANDLE_ENV=%d, handle->henv=%p", SQL_HANDLE_ENV, (void *)handle->henv);
        LogMsg(DEBUG, messageStr);
        Py_END_ALLOW_THREADS;
    }
    else
    {
        snprintf(messageStr, sizeof(messageStr), "Connection not active or is a persistent connection. No disconnect needed.");
        LogMsg(INFO, messageStr);
    }
    LogMsg(INFO, "exit _python_ibm_db_free_conn_struct");
    Py_TYPE(handle)->tp_free((PyObject *)handle);
}

/*    static void _python_ibm_db_free_row_struct */
/*
 * static void _python_ibm_db_free_row_struct(row_hash_struct *handle) {
 *  free(handle);
 * }
 */

static void _python_ibm_db_clear_param_cache(stmt_handle *stmt_res)
{
    LogMsg(INFO, "entry _python_ibm_db_clear_param_cache()");
    snprintf(messageStr, sizeof(messageStr), "Initial state: head_cache_list=%p, num_params=%d",
             stmt_res->head_cache_list, stmt_res->num_params);
    LogMsg(DEBUG, messageStr);
    param_node *temp_ptr, *curr_ptr;

    /* Free param cache list */
    curr_ptr = stmt_res->head_cache_list;

    while (curr_ptr != NULL)
    {
        snprintf(messageStr, sizeof(messageStr), "Freeing node: var_pyvalue=%p, varname=%p, svalue=%p, uvalue=%p, date_value=%p, time_value=%p, ts_value=%p,tstz_value=%p,ivalueArray=%p, fvalueArray=%p, bind_indicator_array=%p",
                 curr_ptr->var_pyvalue, curr_ptr->varname, curr_ptr->svalue, curr_ptr->uvalue,
                 curr_ptr->date_value, curr_ptr->time_value, curr_ptr->ts_value,curr_ptr->tstz_value,
                 curr_ptr->ivalueArray, curr_ptr->fvalueArray, curr_ptr->bind_indicator_array);
        LogMsg(DEBUG, messageStr);
        /* Decrement refcount on Python handle */
        /* NOTE: Py_XDECREF checks NULL value */
        Py_XDECREF(curr_ptr->var_pyvalue);

        /* Free Values */
        /* NOTE: PyMem_Free checks NULL value */
        PyMem_Free(curr_ptr->varname);
        PyMem_Free(curr_ptr->svalue);
        PyMem_Free(curr_ptr->uvalue);
        PyMem_Free(curr_ptr->date_value);
        PyMem_Free(curr_ptr->time_value);
        PyMem_Free(curr_ptr->ts_value);
        PyMem_Free(curr_ptr->tstz_value);
        PyMem_Free(curr_ptr->ivalueArray);
        PyMem_Free(curr_ptr->fvalueArray);
        PyMem_Free(curr_ptr->bind_indicator_array);

        temp_ptr = curr_ptr;
        curr_ptr = curr_ptr->next;

        PyMem_Free(temp_ptr);
    }

    stmt_res->head_cache_list = NULL;
    stmt_res->num_params = 0;
    snprintf(messageStr, sizeof(messageStr), "Final state: head_cache_list=%p, num_params=%d",
             stmt_res->head_cache_list, stmt_res->num_params);
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit _python_ibm_db_clear_param_cache()");
}

/*    static void _python_ibm_db_free_result_struct(stmt_handle* handle) */
static void _python_ibm_db_free_result_struct(stmt_handle *handle)
{
    LogMsg(INFO, "entry _python_ibm_db_free_result_struct()");
    int i;

    if (handle != NULL)
    {
        snprintf(messageStr, sizeof(messageStr), "handle=%p, num_columns=%d", (void *)handle, handle->num_columns);
        LogMsg(DEBUG, messageStr);
        _python_ibm_db_clear_param_cache(handle);

        /* free row data cache */
        if (handle->row_data)
        {
            for (i = 0; i < handle->num_columns; i++)
            {
                switch (handle->column_info[i].type)
                {
                case SQL_CHAR:
                case SQL_VARCHAR:
                case SQL_LONGVARCHAR:
                case SQL_WCHAR:
                case SQL_WVARCHAR:
                case SQL_GRAPHIC:
                case SQL_VARGRAPHIC:
                case SQL_LONGVARGRAPHIC:
                case SQL_BIGINT:
                case SQL_DECIMAL:
                case SQL_NUMERIC:
                case SQL_XML:
                case SQL_DECFLOAT:
                    if (handle->row_data[i].data.str_val != NULL)
                    {
                        snprintf(messageStr, sizeof(messageStr), "Freeing row_data[%d].data.str_val=%p", i, handle->row_data[i].data.str_val);
                        LogMsg(DEBUG, messageStr);
                        PyMem_Del(handle->row_data[i].data.str_val);
                        handle->row_data[i].data.str_val = NULL;
                    }
                    if (handle->row_data[i].data.w_val != NULL)
                    {
                        snprintf(messageStr, sizeof(messageStr), "Freeing row_data[%d].data.w_val=%p", i, handle->row_data[i].data.w_val);
                        LogMsg(DEBUG, messageStr);
                        PyMem_Del(handle->row_data[i].data.w_val);
                        handle->row_data[i].data.w_val = NULL;
                    }
                    break;
                case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
                    if ( handle->row_data[i].data.tstz_val != NULL ) {
                        snprintf(messageStr, sizeof(messageStr), "Freeing row_data[%d].data.tsrz_val=%p", i, handle->row_data[i].data.tstz_val);
                        LogMsg(DEBUG, messageStr);
                        PyMem_Del(handle->row_data[i].data.tstz_val);
                        handle->row_data[i].data.tstz_val = NULL;
                    }
                    break;
                case SQL_TYPE_TIMESTAMP:
                    if (handle->row_data[i].data.ts_val != NULL)
                    {
                        snprintf(messageStr, sizeof(messageStr), "Freeing row_data[%d].data.ts_val=%p", i, handle->row_data[i].data.ts_val);
                        LogMsg(DEBUG, messageStr);
                        PyMem_Del(handle->row_data[i].data.ts_val);
                        handle->row_data[i].data.ts_val = NULL;
                    }
                    break;
                case SQL_TYPE_DATE:
                    if (handle->row_data[i].data.date_val != NULL)
                    {
                        snprintf(messageStr, sizeof(messageStr), "Freeing row_data[%d].data.date_val=%p", i, handle->row_data[i].data.date_val);
                        LogMsg(DEBUG, messageStr);
                        PyMem_Del(handle->row_data[i].data.date_val);
                        handle->row_data[i].data.date_val = NULL;
                    }
                    break;
                case SQL_TYPE_TIME:
                    if (handle->row_data[i].data.time_val != NULL)
                    {
                        snprintf(messageStr, sizeof(messageStr), "Freeing row_data[%d].data.time_val=%p", i, handle->row_data[i].data.time_val);
                        LogMsg(DEBUG, messageStr);
                        PyMem_Del(handle->row_data[i].data.time_val);
                        handle->row_data[i].data.time_val = NULL;
                    }
                    break;
                }
            }
            PyMem_Del(handle->row_data);
            handle->row_data = NULL;
        }

        /* free column info cache */
        if (handle->column_info)
        {
            for (i = 0; i < handle->num_columns; i++)
            {
                snprintf(messageStr, sizeof(messageStr), "Freeing column_info[%d].name=%p", i, handle->column_info[i].name);
                LogMsg(DEBUG, messageStr);
                PyMem_Del(handle->column_info[i].name);
                /* Mem free */
                if (handle->column_info[i].mem_alloc)
                {
                    snprintf(messageStr, sizeof(messageStr), "Freeing column_info[%d].mem_alloc=%p", i, handle->column_info[i].mem_alloc);
                    LogMsg(DEBUG, messageStr);
                    PyMem_Del(handle->column_info[i].mem_alloc);
                }
            }
            PyMem_Del(handle->column_info);
            handle->column_info = NULL;
            handle->num_columns = 0;
        }
    }
    LogMsg(INFO, "exit _python_ibm_db_free_result_struct()");
}

/* static stmt_handle *_ibm_db_new_stmt_struct(conn_handle* conn_res) */
static stmt_handle *_ibm_db_new_stmt_struct(conn_handle *conn_res)
{
    LogMsg(INFO, "entry _ibm_db_new_stmt_struct()");
    snprintf(messageStr, sizeof(messageStr), "Initializing stmt_handle: hdbc=%p, c_bin_mode=%d, c_cursor_type=%d, c_case_mode=%d, c_use_wchar=%d",
             conn_res->hdbc, conn_res->c_bin_mode, conn_res->c_cursor_type, conn_res->c_case_mode, conn_res->c_use_wchar);
    LogMsg(DEBUG, messageStr);
    stmt_handle *stmt_res;

    stmt_res = PyObject_NEW(stmt_handle, &stmt_handleType);
    /* memset(stmt_res, 0, sizeof(stmt_handle)); */

    /* Initialize stmt resource so parsing assigns updated options if needed */
    stmt_res->hdbc = conn_res->hdbc;
    stmt_res->s_bin_mode = conn_res->c_bin_mode;
    stmt_res->cursor_type = conn_res->c_cursor_type;
    stmt_res->s_case_mode = conn_res->c_case_mode;
    stmt_res->s_use_wchar = conn_res->c_use_wchar;
    snprintf(messageStr, sizeof(messageStr), "New stmt_handle initialized: hdbc=%p, s_bin_mode=%d, cursor_type=%d, s_case_mode=%d, s_use_wchar=%d",
             stmt_res->hdbc, stmt_res->s_bin_mode, stmt_res->cursor_type, stmt_res->s_case_mode, stmt_res->s_use_wchar);
    LogMsg(DEBUG, messageStr);
    stmt_res->head_cache_list = NULL;
    stmt_res->current_node = NULL;

    stmt_res->num_params = 0;
    stmt_res->file_param = 0;

    stmt_res->column_info = NULL;
    stmt_res->num_columns = 0;

    stmt_res->error_recno_tracker = 1;
    stmt_res->errormsg_recno_tracker = 1;

    stmt_res->row_data = NULL;
    snprintf(messageStr, sizeof(messageStr), "Final stmt_handle state: head_cache_list=%p, current_node=%p, num_params=%d, file_param=%d, column_info=%p, num_columns=%d, error_recno_tracker=%d, errormsg_recno_tracker=%d, row_data=%p",
             stmt_res->head_cache_list, stmt_res->current_node, stmt_res->num_params, stmt_res->file_param, stmt_res->column_info, stmt_res->num_columns, stmt_res->error_recno_tracker, stmt_res->errormsg_recno_tracker, stmt_res->row_data);
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit _ibm_db_new_stmt_struct()");
    return stmt_res;
}

/*    static _python_ibm_db_free_stmt_struct */
static void _python_ibm_db_free_stmt_struct(stmt_handle *handle)
{
    LogMsg(INFO, "entry _python_ibm_db_free_stmt_struct()");
    if (handle != NULL && handle->hstmt != -1)
    {
        snprintf(messageStr, sizeof(messageStr), "handle->hstmt=%p, preparing to call SQLFreeHandle", (void *)handle->hstmt);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        SQLFreeHandle(SQL_HANDLE_STMT, handle->hstmt);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLFreeHandle called with handle=%p", (void *)handle->hstmt);
        LogMsg(DEBUG, messageStr);
        if (handle != NULL)
        {
            _python_ibm_db_free_result_struct(handle);
        }
    }
    snprintf(messageStr, sizeof(messageStr), "Py_TYPE(handle)->tp_free called for handle=%p", (void *)handle);
    LogMsg(DEBUG, messageStr);
    if (handle != NULL)
    {
        Py_TYPE(handle)->tp_free((PyObject *)handle);
    }
    LogMsg(INFO, "exit _python_ibm_db_free_stmt_struct()");
}

/*    static void _python_ibm_db_init_error_info(stmt_handle *stmt_res) */
static void _python_ibm_db_init_error_info(stmt_handle *stmt_res)
{
    LogMsg(INFO, "entry _python_ibm_db_init_error_info()");
    snprintf(messageStr, sizeof(messageStr), "Initial state: error_recno_tracker = %d, errormsg_recno_tracker = %d",
             stmt_res->error_recno_tracker, stmt_res->errormsg_recno_tracker);
    LogMsg(DEBUG, messageStr);
    stmt_res->error_recno_tracker = 1;
    stmt_res->errormsg_recno_tracker = 1;
    snprintf(messageStr, sizeof(messageStr), "Modified state: error_recno_tracker = %d, errormsg_recno_tracker = %d",
             stmt_res->error_recno_tracker, stmt_res->errormsg_recno_tracker);
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit _python_ibm_db_init_error_info()");
}

/*    static void _python_ibm_db_check_sql_errors( SQLHANDLE handle, SQLSMALLINT hType, int rc, int cpy_to_global, char* ret_str, int API SQLSMALLINT recno)
 */
static void _python_ibm_db_check_sql_errors(SQLHANDLE handle, SQLSMALLINT hType, int rc, int cpy_to_global, char *ret_str, int API, SQLSMALLINT recno)
{
    LogMsg(INFO, "entry _python_ibm_db_check_sql_errors");
    SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH + 1] = {0};
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1] = {0};
    SQLCHAR errMsg[DB2_MAX_ERR_MSG_LEN] = {0};
    SQLCHAR errcode[SQL_SQLCODE_SIZE + 1] = {0};
    SQLINTEGER sqlcode = 0;
    SQLSMALLINT length = 0;
    char *p = NULL;
    SQLINTEGER rc1 = SQL_SUCCESS;
    int i = 0;

    snprintf(messageStr, sizeof(messageStr), "handle=%p, hType=%d, rc=%d, cpy_to_global=%d, API=%d, recno=%d",
             handle, hType, rc, cpy_to_global, API, recno);
    LogMsg(DEBUG, messageStr);
    rc1 = SQLGetDiagRec(hType, handle, recno, sqlstate, &sqlcode, msg,
                        SQL_MAX_MESSAGE_LENGTH + 1, &length);
    snprintf(messageStr, sizeof(messageStr), "SQLGetDiagRec returned rc1=%d, sqlstate=%s, sqlcode=%d, length=%d",
             rc1, sqlstate, sqlcode, length);
    sprintf((char *)errcode, "SQLCODE=%d", (int)sqlcode);
    LogMsg(DEBUG, messageStr);
    if (rc1 == SQL_SUCCESS)
    {
        while ((p = strchr((char *)msg, '\n')))
        {
            *p = '\0';
        }
        snprintf((char *)errMsg, DB2_MAX_ERR_MSG_LEN, "%s SQLCODE=%d", (char *)msg, (int)sqlcode);
#ifdef _WIN32
        for (i = 0; i < strlen(errMsg); i++)
        {
            if (errMsg[i] == '\r')
            {
                errMsg[i] = ' ';
            }
        }
#endif

        snprintf(messageStr, sizeof(messageStr), "Final error message (UTF-8): %s", errMsg);
        LogMsg(ERROR, messageStr);
        if (cpy_to_global != 0 && rc != 1)
        {
            PyErr_SetString(PyExc_Exception, (char *)errMsg);
        }

        switch (rc)
        {
        case SQL_ERROR:
            /* Need to copy the error msg and sqlstate into the symbol Table
             * to cache these results */
            if (cpy_to_global)
            {
                switch (hType)
                {
                case SQL_HANDLE_DBC:
                    snprintf(messageStr, sizeof(messageStr),
                             "Copying to global: SQL_HANDLE_DBC, sqlstate=%s, errMsg=%s",
                             sqlstate, errMsg);
                    LogMsg(DEBUG, messageStr);
                    strncpy(IBM_DB_G(__python_conn_err_state), (char *)sqlstate, SQL_SQLSTATE_SIZE);
                    IBM_DB_G(__python_conn_err_state)[SQL_SQLSTATE_SIZE] = '\0';

                    strncpy(IBM_DB_G(__python_conn_err_msg), (char *)errMsg, DB2_MAX_ERR_MSG_LEN - 1);
                    IBM_DB_G(__python_conn_err_msg)[DB2_MAX_ERR_MSG_LEN - 1] = '\0';

                    strncpy(IBM_DB_G(__python_err_code), (char *)errcode, SQL_SQLCODE_SIZE - 1);
                    IBM_DB_G(__python_err_code)[SQL_SQLCODE_SIZE - 1] = '\0';

                    break;

                case SQL_HANDLE_STMT:
                    snprintf(messageStr, sizeof(messageStr),
                             "Copying to global: SQL_HANDLE_STMT, sqlstate=%s, errMsg=%s",
                             sqlstate, errMsg);
                    LogMsg(DEBUG, messageStr);
                    strncpy(IBM_DB_G(__python_stmt_err_state), (char *)sqlstate, SQL_SQLSTATE_SIZE);
                    IBM_DB_G(__python_stmt_err_state)[SQL_SQLSTATE_SIZE] = '\0';

                    strncpy(IBM_DB_G(__python_stmt_err_msg), (char *)errMsg, DB2_MAX_ERR_MSG_LEN - 1);
                    IBM_DB_G(__python_stmt_err_msg)[DB2_MAX_ERR_MSG_LEN - 1] = '\0';

                    strncpy(IBM_DB_G(__python_err_code), (char *)errcode, SQL_SQLCODE_SIZE - 1);
                    IBM_DB_G(__python_err_code)[SQL_SQLCODE_SIZE - 1] = '\0';

                    break;
                }
            }
            /* This call was made from ibm_db_errmsg or ibm_db_error or ibm_db_warn */
            /* Check for error and return */
            switch (API)
            {
            case DB2_ERR:
                if (ret_str != NULL)
                {
                    snprintf(messageStr, sizeof(messageStr), "Returning SQLSTATE for DB2_ERR: %s", sqlstate);
                    LogMsg(DEBUG, messageStr);
                    strncpy(ret_str, (char *)sqlstate, SQL_SQLSTATE_SIZE);
                    ret_str[SQL_SQLSTATE_SIZE] = '\0';
                }
                return;
            case DB2_ERRMSG:
                if (ret_str != NULL)
                {
                    snprintf(messageStr, sizeof(messageStr), "Returning error message for DB2_ERRMSG: %s", errMsg);
                    LogMsg(DEBUG, messageStr);
                    strncpy(ret_str, (char *)errMsg, DB2_MAX_ERR_MSG_LEN - 1);
                    ret_str[DB2_MAX_ERR_MSG_LEN - 1] = '\0';
                }
                return;
            default:
                break;
            }
            break;
        case SQL_SUCCESS_WITH_INFO:
            /* Need to copy the warning msg and sqlstate into the symbol Table
             * to cache these results */
            if (cpy_to_global)
            {
                switch (hType)
                {
                case SQL_HANDLE_DBC:
                    snprintf(messageStr, sizeof(messageStr),
                             "Copying warning to global: SQL_HANDLE_DBC, sqlstate=%s, errMsg=%s",
                             sqlstate, errMsg);
                    LogMsg(DEBUG, messageStr);
                    strncpy(IBM_DB_G(__python_conn_warn_state), (char *)sqlstate, SQL_SQLSTATE_SIZE);
                    IBM_DB_G(__python_conn_warn_state)[SQL_SQLSTATE_SIZE] = '\0';

                    strncpy(IBM_DB_G(__python_conn_warn_msg), (char *)errMsg, DB2_MAX_ERR_MSG_LEN - 1);
                    IBM_DB_G(__python_conn_warn_msg)[DB2_MAX_ERR_MSG_LEN - 1] = '\0';

                    strncpy(IBM_DB_G(__python_err_code), (char *)errcode, SQL_SQLCODE_SIZE);
                    IBM_DB_G(__python_err_code)[SQL_SQLCODE_SIZE] = '\0';

                 break;

                case SQL_HANDLE_STMT:
                    snprintf(messageStr, sizeof(messageStr),
                             "Copying warning to global: SQL_HANDLE_STMT, sqlstate=%s, errMsg=%s",
                             sqlstate, errMsg);
                    LogMsg(DEBUG, messageStr);
                    strncpy(IBM_DB_G(__python_stmt_warn_state), (char *)sqlstate, SQL_SQLSTATE_SIZE);
                    IBM_DB_G(__python_stmt_warn_state)[SQL_SQLSTATE_SIZE] = '\0';

                    strncpy(IBM_DB_G(__python_stmt_warn_msg), (char *)errMsg, DB2_MAX_ERR_MSG_LEN - 1);
                    IBM_DB_G(__python_stmt_warn_msg)[DB2_MAX_ERR_MSG_LEN - 1] = '\0';

                    strncpy(IBM_DB_G(__python_err_code), (char *)errcode, SQL_SQLCODE_SIZE);
                    IBM_DB_G(__python_err_code)[SQL_SQLCODE_SIZE] = '\0';

                    break;
                }
            }
            /* This call was made from ibm_db_errmsg or ibm_db_error or ibm_db_warn */
            /* Check for error and return */
            if ((API == DB2_WARNMSG) && (ret_str != NULL))
            {
                snprintf(messageStr, sizeof(messageStr), "Returning warning message for DB2_WARNMSG: %s", errMsg);
                LogMsg(DEBUG, messageStr);
                strncpy(ret_str, (char *)errMsg, DB2_MAX_ERR_MSG_LEN - 1);
                ret_str[DB2_MAX_ERR_MSG_LEN - 1] = '\0';
            }
            return;
        default:
            break;
        }
    }
}

/*    static int _python_ibm_db_assign_options( void *handle, int type, long opt_key, PyObject *data ) */
static int _python_ibm_db_assign_options(void *handle, int type, long opt_key, PyObject *data)
{
    LogMsg(INFO, "entry _python_ibm_db_assign_options()");
    int rc = SQL_SUCCESS;
    long option_num = 0;
    SQLINTEGER value_int = 0;
#ifdef __MVS__
    SQLCHAR *option_str = NULL;
#else
    SQLWCHAR *option_str = NULL;
#endif
    int isNewBuffer = 0;
    snprintf(messageStr, sizeof(messageStr), "Parameters - Handle: %p, type: %d, opt_key: %ld, data: %p", handle, type, opt_key, data);
    LogMsg(DEBUG, messageStr);
    /* First check to see if it is a non-cli attribut */
    if (opt_key == ATTR_CASE)
    {
        option_num = NUM2LONG(data);
        snprintf(messageStr, sizeof(messageStr), "ATTR_CASE option_num: %ld", option_num);
        LogMsg(DEBUG, messageStr);
        if (type == SQL_HANDLE_STMT)
        {
            switch (option_num)
            {
            case CASE_LOWER:
                ((stmt_handle *)handle)->s_case_mode = CASE_LOWER;
                snprintf(messageStr, sizeof(messageStr), "Setting s_case_mode to CASE_LOWER");
                LogMsg(DEBUG, messageStr);
                break;
            case CASE_UPPER:
                ((stmt_handle *)handle)->s_case_mode = CASE_UPPER;
                snprintf(messageStr, sizeof(messageStr), "Setting s_case_mode to CASE_UPPER");
                LogMsg(DEBUG, messageStr);
                break;
            case CASE_NATURAL:
                ((stmt_handle *)handle)->s_case_mode = CASE_NATURAL;
                snprintf(messageStr, sizeof(messageStr), "Setting s_case_mode to CASE_UPPER");
                LogMsg(DEBUG, messageStr);
                break;
            default:
                LogMsg(EXCEPTION, "ATTR_CASE attribute must be one of CASE_LOWER, CASE_UPPER, or CASE_NATURAL");
                PyErr_SetString(PyExc_Exception, "ATTR_CASE attribute must be one of CASE_LOWER, CASE_UPPER, or CASE_NATURAL");
                return -1;
            }
        }
        else if (type == SQL_HANDLE_DBC)
        {
            switch (option_num)
            {
            case CASE_LOWER:
                ((conn_handle *)handle)->c_case_mode = CASE_LOWER;
                snprintf(messageStr, sizeof(messageStr), "Setting c_case_mode to CASE_LOWER");
                LogMsg(DEBUG, messageStr);
                break;
            case CASE_UPPER:
                ((conn_handle *)handle)->c_case_mode = CASE_UPPER;
                snprintf(messageStr, sizeof(messageStr), "Setting c_case_mode to CASE_UPPER");
                LogMsg(DEBUG, messageStr);
                break;
            case CASE_NATURAL:
                ((conn_handle *)handle)->c_case_mode = CASE_NATURAL;
                snprintf(messageStr, sizeof(messageStr), "Setting c_case_mode to CASE_NATURAL");
                LogMsg(DEBUG, messageStr);
                break;
            default:
                LogMsg(EXCEPTION, "ATTR_CASE attribute must be one of CASE_LOWER, CASE_UPPER, or CASE_NATURAL");
                PyErr_SetString(PyExc_Exception, "ATTR_CASE attribute must be one of CASE_LOWER, CASE_UPPER, or CASE_NATURAL");
                return -1;
            }
        }
        else
        {
            LogMsg(ERROR, "Connection or statement handle must be passed in.");
            PyErr_SetString(PyExc_Exception, "Connection or statement handle must be passed in.");
            return -1;
        }
    }
    else if (opt_key == USE_WCHAR)
    {
        option_num = NUM2LONG(data);
        snprintf(messageStr, sizeof(messageStr), "USE_WCHAR option_num: %ld", option_num);
        LogMsg(DEBUG, messageStr);
        if (type == SQL_HANDLE_STMT)
        {
            switch (option_num)
            {
            case WCHAR_YES:
                ((stmt_handle *)handle)->s_use_wchar = WCHAR_YES;
                snprintf(messageStr, sizeof(messageStr), "Setting s_use_wchar to WCHAR_YES");
                LogMsg(DEBUG, messageStr);
                break;
            case WCHAR_NO:
                ((stmt_handle *)handle)->s_use_wchar = WCHAR_NO;
                snprintf(messageStr, sizeof(messageStr), "Setting s_use_wchar to WCHAR_NO");
                LogMsg(DEBUG, messageStr);
                break;
            default:
                LogMsg(EXCEPTION, "USE_WCHAR attribute must be one of WCHAR_YES or WCHAR_NO");
                PyErr_SetString(PyExc_Exception, "USE_WCHAR attribute must be one of WCHAR_YES or WCHAR_NO");
                return -1;
            }
        }
        else if (type == SQL_HANDLE_DBC)
        {
            switch (option_num)
            {
            case WCHAR_YES:
                ((conn_handle *)handle)->c_use_wchar = WCHAR_YES;
                snprintf(messageStr, sizeof(messageStr), "Setting c_use_wchar to WCHAR_YES");
                LogMsg(DEBUG, messageStr);
                break;
            case WCHAR_NO:
                ((conn_handle *)handle)->c_use_wchar = WCHAR_NO;
                snprintf(messageStr, sizeof(messageStr), "Setting c_use_wchar to WCHAR_NO");
                LogMsg(DEBUG, messageStr);
                break;
            default:
                LogMsg(EXCEPTION, "USE_WCHAR attribute must be one of WCHAR_YES or WCHAR_NO");
                PyErr_SetString(PyExc_Exception, "USE_WCHAR attribute must be one of WCHAR_YES or WCHAR_NO");
                return -1;
            }
        }
    }
    else if (type == SQL_HANDLE_STMT)
    {
        if (PyString_Check(data) || PyUnicode_Check(data))
        {
            data = PyUnicode_FromObject(data);
            snprintf(messageStr, sizeof(messageStr), "Converted data to Unicode: %p", data);
            LogMsg(DEBUG, messageStr);
#ifdef __MVS__
            option_str = getUnicodeDataAsSQLCHAR(data, &isNewBuffer);
            snprintf(messageStr, sizeof(messageStr), "Option string (SQLCHAR) pointer: %p, isNewBuffer: %d", option_str, isNewBuffer);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetStmtAttr((SQLHSTMT)((stmt_handle *)handle)->hstmt, opt_key, (SQLPOINTER)option_str, SQL_IS_INTEGER);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLSetStmtAttr called with hstmt: %p, opt_key: %ld, option_str: %p, SQL_IS_INTEGER, and returned rc: %d",
                     ((stmt_handle *)handle)->hstmt, opt_key, option_str, rc);
            LogMsg(DEBUG, messageStr);
#else
            option_str = getUnicodeDataAsSQLWCHAR(data, &isNewBuffer);
            snprintf(messageStr, sizeof(messageStr), "Option string (SQLWCHAR) pointer: %p, isNewBuffer: %d", option_str, isNewBuffer);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetStmtAttrW((SQLHSTMT)((stmt_handle *)handle)->hstmt, opt_key, (SQLPOINTER)option_str, SQL_IS_INTEGER);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLSetStmtAttrW called with hstmt: %p, opt_key: %ld, option_str: %p, SQL_IS_INTEGER, and returned rc: %d",
                     ((stmt_handle *)handle)->hstmt, opt_key, option_str, rc);
            LogMsg(DEBUG, messageStr);
#endif
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if (isNewBuffer)
                PyMem_Del(option_str);
        }
        else
        {
            option_num = NUM2LONG(data);
            snprintf(messageStr, sizeof(messageStr), "Option number: %ld", option_num);
            LogMsg(DEBUG, messageStr);
            if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_OFF)
            {
                ((conn_handle *)handle)->auto_commit = 0;
                snprintf(messageStr, sizeof(messageStr), "Setting auto_commit to OFF");
                LogMsg(DEBUG, messageStr);
            }
            else if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_ON)
            {
                ((conn_handle *)handle)->auto_commit = 1;
                snprintf(messageStr, sizeof(messageStr), "Setting auto_commit to ON");
                LogMsg(DEBUG, messageStr);
            }
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetStmtAttr((SQLHSTMT)((stmt_handle *)handle)->hstmt, opt_key, (SQLPOINTER)option_num, SQL_IS_INTEGER);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLSetStmtAttr called with hstmt: %p, opt_key: %ld, option_num: %ld, SQL_IS_INTEGER, and returned rc: %d",
                     ((stmt_handle *)handle)->hstmt, opt_key, option_num, rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if (opt_key == SQL_ATTR_CURSOR_TYPE)
            {
                ((stmt_handle *)handle)->cursor_type = option_num;
                snprintf(messageStr, sizeof(messageStr), "Cursor type set to %ld", option_num);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_SUCCESS_WITH_INFO)
                {
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetStmtAttr(((stmt_handle *)handle)->hstmt, opt_key, &value_int, SQL_IS_INTEGER, NULL);
                    Py_END_ALLOW_THREADS;
                    snprintf(messageStr, sizeof(messageStr), "SQLGetStmtAttr called with hstmt: %p, opt_key: %ld, value_int: %d, SQL_IS_INTEGER, and retured rc: %d",
                             ((stmt_handle *)handle)->hstmt, opt_key, value_int, rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_ERROR)
                    {
                        _python_ibm_db_check_sql_errors(((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        PyErr_Clear();
                        return -1;
                    }
                    ((stmt_handle *)handle)->cursor_type = value_int;
                }
            }
        }
    }
    else if (type == SQL_HANDLE_DBC)
    {
        if (PyString_Check(data) || PyUnicode_Check(data))
        {
            data = PyUnicode_FromObject(data);
            snprintf(messageStr, sizeof(messageStr), "Converted data to Unicode: %p", data);
            LogMsg(DEBUG, messageStr);
#ifdef __MVS__
            option_str = getUnicodeDataAsSQLCHAR(data, &isNewBuffer);
            snprintf(messageStr, sizeof(messageStr), "Option string (SQLCHAR) pointer: %p, isNewBuffer: %d", option_str, isNewBuffer);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttr((SQLHSTMT)((conn_handle *)handle)->hdbc, opt_key, (SQLPOINTER)option_str, SQL_NTS);
            snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttr called with hdbc: %p, opt_key: %ld, option_str: %p, SQL_NTS, and retured rc: %d",
                     ((conn_handle *)handle)->hdbc, opt_key, (void *)option_str, rc);
            LogMsg(DEBUG, messageStr);
            Py_END_ALLOW_THREADS;
#else
            option_str = getUnicodeDataAsSQLWCHAR(data, &isNewBuffer);
            snprintf(messageStr, sizeof(messageStr), "Option string (SQLWCHAR) pointer: %p, isNewBuffer: %d", option_str, isNewBuffer);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttrW((SQLHSTMT)((conn_handle *)handle)->hdbc, opt_key, (SQLPOINTER)option_str, SQL_NTS);
            snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttrW called with hdbc: %p, opt_key: %ld, option_str: %p, SQL_NTS, and retured rc: %d",
                     ((conn_handle *)handle)->hdbc, opt_key, (void *)option_str, rc);
            LogMsg(INFO, messageStr);
            Py_END_ALLOW_THREADS;
#endif
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if (isNewBuffer)
                PyMem_Del(option_str);
        }
        else
        {
            option_num = NUM2LONG(data);
            snprintf(messageStr, sizeof(messageStr), "Option number: %ld", option_num);
            LogMsg(DEBUG, messageStr);
            if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_OFF)
            {
                ((conn_handle *)handle)->auto_commit = 0;
                snprintf(messageStr, sizeof(messageStr), "Setting auto_commit to OFF");
                LogMsg(DEBUG, messageStr);
            }
            else if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_ON)
            {
                ((conn_handle *)handle)->auto_commit = 1;
                snprintf(messageStr, sizeof(messageStr), "Setting auto_commit to ON");
                LogMsg(DEBUG, messageStr);
            }
#ifdef __MVS__
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttr((SQLHSTMT)((conn_handle *)handle)->hdbc, opt_key, (SQLPOINTER)option_num, SQL_IS_INTEGER);
            snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttr called with hdbc: %p, opt_key: %ld, option_num: %ld, SQL_IS_INTEGER, and retured rc: %d",
                     ((conn_handle *)handle)->hdbc, opt_key, (long)option_num, rc);
            LogMsg(DEBUG, messageStr);
            Py_END_ALLOW_THREADS;
#else
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttrW((SQLHSTMT)((conn_handle *)handle)->hdbc, opt_key, (SQLPOINTER)option_num, SQL_IS_INTEGER);
            snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttrW called with hdbc: %p, opt_key: %ld, option_num: %ld, SQL_IS_INTEGER, and retured rc: %d",
                     ((conn_handle *)handle)->hdbc, opt_key, (long)option_num, rc);
            LogMsg(DEBUG, messageStr);
            Py_END_ALLOW_THREADS;
#endif
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
        }
    }
    else
    {
        LogMsg(ERROR, "Connection or statement handle must be passed in.");
        PyErr_SetString(PyExc_Exception, "Connection or statement handle must be passed in.");
        return -1;
    }
    LogMsg(INFO, "exit _python_ibm_db_assign_options()");
    return 0;
}

/*    static int _python_ibm_db_parse_options( PyObject *options, int type, void *handle)
 */
static int _python_ibm_db_parse_options(PyObject *options, int type, void *handle)
{
    LogMsg(INFO, "entry _python_ibm_db_parse_options()");
    int numOpts = 0, i = 0;
    PyObject *keys = NULL;
    PyObject *key = NULL; /* Holds the Option Index Key */
    PyObject *data = NULL;
    PyObject *tc_pass = NULL;
    int rc = 0;
    snprintf(messageStr, sizeof(messageStr), "Options parameter: %p, Type: %d, Handle: %p", options, type, handle);
    LogMsg(INFO, messageStr);
    if (!NIL_P(options))
    {
        keys = PyDict_Keys(options);
        numOpts = PyList_Size(keys);
        snprintf(messageStr, sizeof(messageStr), "Number of options: %d", numOpts);
        LogMsg(INFO, messageStr);
        for (i = 0; i < numOpts; i++)
        {
            key = PyList_GetItem(keys, i);
            data = PyDict_GetItem(options, key);
            snprintf(messageStr, sizeof(messageStr), "Option %d: Key: %ld, Data: %p", i, NUM2LONG(key), data);
            LogMsg(INFO, messageStr);
            if (NUM2LONG(key) == SQL_ATTR_TRUSTED_CONTEXT_PASSWORD)
            {
                tc_pass = data;
            }
            else
            {
                snprintf(messageStr, sizeof(messageStr), "Assigning option: Key: %ld, Data: %p", NUM2LONG(key), data);
                LogMsg(INFO, messageStr);
                /* Assign options to handle. */
                /* Sets the options in the handle with CLI/ODBC calls */
                rc = _python_ibm_db_assign_options(handle, type, NUM2LONG(key), data);
                snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_assign_options returned: %d", rc);
                LogMsg(INFO, messageStr);
            }
            if (rc)
            {
                LogMsg(INFO, "exit _python_ibm_db_parse_options()");
                return SQL_ERROR;
            }
        }
        if (!NIL_P(tc_pass))
        {
            snprintf(messageStr, sizeof(messageStr), "Assigning trusted context password: %p", tc_pass);
            LogMsg(INFO, messageStr);
            rc = _python_ibm_db_assign_options(handle, type, SQL_ATTR_TRUSTED_CONTEXT_PASSWORD, tc_pass);
            snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_assign_options for tc_pass returned: %d", rc);
            LogMsg(INFO, messageStr);
        }
        if (rc)
        {
            LogMsg(INFO, "exit _python_ibm_db_parse_options()");
            return SQL_ERROR;
        }
    }
    LogMsg(INFO, "exit _python_ibm_db_parse_options()");
    return SQL_SUCCESS;
}

/*    static int _python_ibm_db_get_result_set_info(stmt_handle *stmt_res)
initialize the result set information of each column. This must be done once
*/
static int _python_ibm_db_get_result_set_info(stmt_handle *stmt_res)
{
    LogMsg(INFO, "entry _python_ibm_db_get_result_set_info()");
    snprintf(messageStr, sizeof(messageStr), "stmt_res pointer: %p", (void *)stmt_res);
    LogMsg(DEBUG, messageStr);
    int rc = -1, i;
    SQLSMALLINT nResultCols = 0, name_length;
    SQLCHAR tmp_name[BUFSIZ];
    snprintf(messageStr, sizeof(messageStr), "Calling SQLNumResultCols() with hstmt = %p", (void *)stmt_res->hstmt);
    LogMsg(DEBUG, messageStr);
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLNumResultCols((SQLHSTMT)stmt_res->hstmt, &nResultCols);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLNumResultCols() returned rc = %d, nResultCols = %d", rc, nResultCols);
    LogMsg(DEBUG, messageStr);
    if (rc == SQL_ERROR || nResultCols == 0)
    {
        _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                        SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
        return -1;
    }
    /*  if( rc == SQL_SUCCESS_WITH_INFO )
      {
          _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                         SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
      } */
    snprintf(messageStr, sizeof(messageStr), "Allocating memory for column_info array of size %d", nResultCols);
    LogMsg(DEBUG, messageStr);
    stmt_res->num_columns = nResultCols;
    stmt_res->column_info = ALLOC_N(ibm_db_result_set_info, nResultCols);
    if (stmt_res->column_info == NULL)
    {
        LogMsg(EXCEPTION, "Failed to Allocate Memory");
        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
        return -1;
    }
    memset(stmt_res->column_info, 0, sizeof(ibm_db_result_set_info) * nResultCols);
    /* return a set of attributes for a column */
    for (i = 0; i < nResultCols; i++)
    {
        snprintf(messageStr, sizeof(messageStr), "Calling SQLDescribeCol() for column %d with hstmt = %p, col_num = %d, tmp_name buffer size = %d",
                 i, (void *)stmt_res->hstmt, i + 1, BUFSIZ);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLDescribeCol((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)(i + 1),
                            (SQLCHAR *)&tmp_name, BUFSIZ, &name_length,
                            &stmt_res->column_info[i].type,
                            &stmt_res->column_info[i].size,
                            &stmt_res->column_info[i].scale,
                            &stmt_res->column_info[i].nullable);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLDescribeCol() for column %d returned rc = %d, tmp_name = %s, name_length = %d, type = %d, size = %d, scale = %d, nullable = %d",
                 i, rc, tmp_name, name_length,
                 stmt_res->column_info[i].type,
                 stmt_res->column_info[i].size,
                 stmt_res->column_info[i].scale,
                 stmt_res->column_info[i].nullable);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            return -1;
        }
        if (name_length <= 0)
        {
            stmt_res->column_info[i].name = (SQLCHAR *)estrdup("");
            if (stmt_res->column_info[i].name == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
        }
        else if (name_length >= BUFSIZ)
        {
            /* column name is longer than BUFSIZ */
            LogMsg(DEBUG, "column name is longer than BUFSIZ");
            snprintf(messageStr, sizeof(messageStr), "Re-querying SQLDescribeCol() for long column name %d with hstmt = %p, name_length = %d",
                     i, (void *)stmt_res->hstmt, name_length);
            LogMsg(DEBUG, messageStr);
            stmt_res->column_info[i].name = (SQLCHAR *)ALLOC_N(char, name_length + 1);
            if (stmt_res->column_info[i].name == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLDescribeCol((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)(i + 1),
                                stmt_res->column_info[i].name, name_length,
                                &name_length, &stmt_res->column_info[i].type,
                                &stmt_res->column_info[i].size,
                                &stmt_res->column_info[i].scale,
                                &stmt_res->column_info[i].nullable);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLDescribeCol() re-query for column %d returned rc = %d",
                     i, rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if (rc == SQL_ERROR)
            {
                return -1;
            }
        }
        else
        {
            stmt_res->column_info[i].name = (SQLCHAR *)estrdup((char *)tmp_name);
            if (stmt_res->column_info[i].name == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
        }
    }
    LogMsg(DEBUG, "Successfully completed _python_ibm_db_get_result_set_info()");
    LogMsg(INFO, "exit _python_ibm_db_get_result_set_info()");
    return 0;
}

/*    static int _python_ibn_bind_column_helper(stmt_handle *stmt_res)
    bind columns to data, this must be done once
*/
static int _python_ibm_db_bind_column_helper(stmt_handle *stmt_res)
{
    LogMsg(INFO, "entry _python_ibm_db_bind_column_helper()");
    SQLINTEGER in_length = 0;
    SQLSMALLINT column_type;
    ibm_db_row_data_type *row_data;
    int i, rc = SQL_SUCCESS;

    stmt_res->row_data = ALLOC_N(ibm_db_row_type, stmt_res->num_columns);
    if (stmt_res->row_data == NULL)
    {
        LogMsg(EXCEPTION, "Failed to Allocate Memory");
        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
        return -1;
    }
    memset(stmt_res->row_data, 0, sizeof(ibm_db_row_type) * stmt_res->num_columns);
    LogMsg(DEBUG, "Allocated memory for row_data");

    for (i = 0; i < stmt_res->num_columns; i++)
    {
        column_type = stmt_res->column_info[i].type;
        row_data = &stmt_res->row_data[i].data;
        snprintf(messageStr, sizeof(messageStr), "Processing column %d with type %d", i, column_type);
        LogMsg(DEBUG, messageStr);
        switch (column_type)
        {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
            snprintf(messageStr, sizeof(messageStr),
                     "Case SQL_CHAR/SQL_VARCHAR/SQL_LONGVARCHAR, i=%d, s_use_wchar=%d", i, stmt_res->s_use_wchar);
            LogMsg(DEBUG, messageStr);
            if (stmt_res->s_use_wchar == WCHAR_NO)
            {
                in_length = stmt_res->column_info[i].size + 1;
                row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                if (row_data->str_val == NULL)
                {
                    LogMsg(EXCEPTION, "Failed to Allocate Memory for str_val");
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }
                snprintf(messageStr, sizeof(messageStr),
                         "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_CHAR, buffer_size=%d, out_length=%p",
                         stmt_res->hstmt, i + 1, in_length, &stmt_res->row_data[i].out_length);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                                SQL_C_CHAR, row_data->str_val, in_length,
                                (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                    SQL_HANDLE_STMT, rc, 1, NULL,
                                                    -1, 1);
                }
                break;
            }
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_GRAPHIC:
        case SQL_VARGRAPHIC:
        case SQL_LONGVARGRAPHIC:
            snprintf(messageStr, sizeof(messageStr),
                     "Case SQL_WCHAR/SQL_WVARCHAR/SQL_GRAPHIC/SQL_VARGRAPHIC/SQL_LONGVARGRAPHIC, i=%d", i);
            LogMsg("DEBUG", messageStr);
            in_length = stmt_res->column_info[i].size + 1;
            row_data->w_val = (SQLWCHAR *)ALLOC_N(SQLWCHAR, in_length);
            snprintf(messageStr, sizeof(messageStr),
                     "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_WCHAR, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, in_length * sizeof(SQLWCHAR), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_WCHAR, row_data->w_val, in_length * sizeof(SQLWCHAR),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL,
                                                -1, 1);
            }
            break;

        case SQL_BINARY:
        case SQL_LONGVARBINARY:
        case SQL_VARBINARY:
            snprintf(messageStr, sizeof(messageStr),
                     "Case SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY, i=%d, s_bin_mode=%d", i, stmt_res->s_bin_mode);
            LogMsg(DEBUG, messageStr);
            if (stmt_res->s_bin_mode == CONVERT)
            {
                in_length = 2 * (stmt_res->column_info[i].size) + 1;
                row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                if (row_data->str_val == NULL)
                {
                    LogMsg(EXCEPTION, "Failed to Allocate Memory for str_val");
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }
                snprintf(messageStr, sizeof(messageStr),
                         "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_CHAR, buffer_size=%d, out_length=%p",
                         stmt_res->hstmt, i + 1, in_length, &stmt_res->row_data[i].out_length);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                                SQL_C_CHAR, row_data->str_val, in_length,
                                (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                    SQL_HANDLE_STMT, rc, 1, NULL,
                                                    -1, 1);
                }
            }
            else
            {
                in_length = stmt_res->column_info[i].size + 1;
                row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                if (row_data->str_val == NULL)
                {
                    LogMsg(EXCEPTION, "Failed to Allocate Memory for str_val");
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }
                snprintf(messageStr, sizeof(messageStr),
                         "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_DEFAULT, buffer_size=%d, out_length=%p",
                         stmt_res->hstmt, i + 1, in_length, &stmt_res->row_data[i].out_length);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                                SQL_C_DEFAULT, row_data->str_val, in_length,
                                (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                    SQL_HANDLE_STMT, rc, 1, NULL,
                                                    -1, 1);
                }
            }
            break;

        case SQL_BIGINT:
        case SQL_DECFLOAT:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_BIGINT/SQL_DECFLOAT, i=%d", i);
            LogMsg(DEBUG, messageStr);
            in_length = stmt_res->column_info[i].size + 3;
            row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
            if (row_data->str_val == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory for str_val");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
            snprintf(messageStr, sizeof(messageStr), "Allocated memory for str_val with length %d", in_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr),
                     "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_CHAR, buffer_size=%d, out_length=%p",
                     stmt_res->hstmt, i + 1, in_length, &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_CHAR, row_data->str_val, in_length,
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            break;

        case SQL_TYPE_DATE:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_TYPE_DATE for column index %d", i);
            LogMsg(DEBUG, messageStr);
            row_data->date_val = ALLOC(DATE_STRUCT);
            if (row_data->date_val == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory for date_val");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
            snprintf(messageStr, sizeof(messageStr), "Allocated memory for date_val with size %zu", sizeof(DATE_STRUCT));
            LogMsg(DEBUG, messageStr);
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_TYPE_DATE, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, sizeof(DATE_STRUCT), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_TYPE_DATE, row_data->date_val, sizeof(DATE_STRUCT),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            break;

        case SQL_TYPE_TIME:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_TYPE_TIME for column index %d", i);
            LogMsg(DEBUG, messageStr);
            row_data->time_val = ALLOC(TIME_STRUCT);
            if (row_data->time_val == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory for time_val");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
            snprintf(messageStr, sizeof(messageStr), "Allocated memory for time_val with size %zu", sizeof(TIME_STRUCT));
            LogMsg(DEBUG, messageStr);
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_TYPE_TIME, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, sizeof(TIME_STRUCT), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_TYPE_TIME, row_data->time_val, sizeof(TIME_STRUCT),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            break;

        case SQL_TYPE_TIMESTAMP:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_TYPE_TIMESTAMP for column index %d", i);
            LogMsg(DEBUG, messageStr);
            row_data->ts_val = ALLOC(TIMESTAMP_STRUCT);
            if (row_data->ts_val == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory for ts_val");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
            snprintf(messageStr, sizeof(messageStr), "Allocated memory for ts_val with size %zu", sizeof(TIMESTAMP_STRUCT));
            LogMsg(DEBUG, messageStr);
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_TYPE_TIMESTAMP, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, sizeof(TIMESTAMP_STRUCT), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_TYPE_TIMESTAMP, row_data->time_val, sizeof(TIMESTAMP_STRUCT),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                return -1;
            }
            break;

        case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE for column index %d", i);
            LogMsg(DEBUG, messageStr);
            row_data->tstz_val = ALLOC(TIMESTAMP_STRUCT_EXT_TZ);
            if ( row_data->tstz_val == NULL ) {
                LogMsg(EXCEPTION, "Failed to Allocate Memory for tstz_val");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
            snprintf(messageStr, sizeof(messageStr), "Allocated memory for tstz_val with size %zu", sizeof(TIMESTAMP_STRUCT_EXT_TZ));
            LogMsg(DEBUG, messageStr);
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d,sql_type=SQL_C_TYPE_TIMESTAMP_EXT_TZ, buffer_size=%zu, out_length=%p",stmt_res->hstmt, i + 1, sizeof(TIMESTAMP_STRUCT_EXT_TZ), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_TYPE_TIMESTAMP_EXT_TZ, row_data->tstz_val, sizeof(TIMESTAMP_STRUCT_EXT_TZ),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
            LogMsg(DEBUG, messageStr);

            if ( rc == SQL_ERROR ) {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                return -1;
            }
            break;

#ifdef __MVS__
        case SQL_SMALLINT:
#else
        case SQL_SMALLINT:
        case SQL_BOOLEAN:
#endif
            snprintf(messageStr, sizeof(messageStr), "Case SQL_SMALLINT or SQL_BOOLEAN for column index %d", i);
            LogMsg(DEBUG, messageStr);
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_DEFAULT, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, sizeof(row_data->s_val), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_DEFAULT, &row_data->s_val,
                            sizeof(row_data->s_val),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1,
                                                1);
            }
            break;

        case SQL_INTEGER:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_INTEGER for column index %d", i);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_DEFAULT, buffer=%p, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, &row_data->i_val, sizeof(row_data->i_val), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_DEFAULT, &row_data->i_val,
                            sizeof(row_data->i_val),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for SQL_INTEGER column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1,
                                                1);
            }
            break;

        case SQL_BIT:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_BIT for column index %d", i);
            LogMsg(DEBUG, messageStr);
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_LONG, buffer=%p, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, &row_data->i_val, sizeof(row_data->i_val), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_LONG, &row_data->i_val,
                            sizeof(row_data->i_val),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for SQL_BIT column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1,
                                                1);
            }
            break;

        case SQL_REAL:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_REAL for column index %d", i);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_FLOAT, buffer=%p, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, &row_data->r_val, sizeof(row_data->r_val), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_FLOAT, &row_data->r_val,
                            sizeof(row_data->r_val),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for SQL_REAL column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1,
                                                1);
            }
            break;

        case SQL_FLOAT:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_FLOAT for column index %d", i);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_DEFAULT, buffer=%p, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, &row_data->f_val, sizeof(row_data->f_val), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_DEFAULT, &row_data->f_val,
                            sizeof(row_data->f_val),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for SQL_FLOAT column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1,
                                                1);
            }
            break;

        case SQL_DOUBLE:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_DOUBLE for column index %d", i);
            LogMsg(DEBUG, messageStr);
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_DEFAULT, buffer=%p, buffer_size=%zu, out_length=%p",
                     stmt_res->hstmt, i + 1, &row_data->d_val, sizeof(row_data->d_val), &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_DEFAULT, &row_data->d_val,
                            sizeof(row_data->d_val),
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for SQL_DOUBLE column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1,
                                                1);
            }
            break;

        case SQL_DECIMAL:
        case SQL_NUMERIC:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_DECIMAL/SQL_NUMERIC for column index %d", i);
            LogMsg(DEBUG, messageStr);
            in_length = stmt_res->column_info[i].size +
                        stmt_res->column_info[i].scale + 2 + 1;
            snprintf(messageStr, sizeof(messageStr), "Allocating memory with size %lu for SQL_DECIMAL/SQL_NUMERIC column %d", in_length, i);
            LogMsg(DEBUG, messageStr);
            row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
            if (row_data->str_val == NULL)
            {
                LogMsg(EXCEPTION, "Failed to Allocate Memory for SQL_DECIMAL/SQL_NUMERIC");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }
            snprintf(messageStr, sizeof(messageStr), "Calling SQLBindCol with parameters: hstmt=%p, col=%d, sql_type=SQL_C_CHAR, buffer=%p, buffer_size=%lu, out_length=%p",
                     stmt_res->hstmt, i + 1, row_data->str_val, in_length, &stmt_res->row_data[i].out_length);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i + 1),
                            SQL_C_CHAR, row_data->str_val, in_length,
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindCol returned %d for SQL_DECIMAL/SQL_NUMERIC column %d", rc, i);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1,
                                                1);
            }
            break;

        case SQL_BLOB:
        case SQL_CLOB:
        case SQL_DBCLOB:
        case SQL_XML:
            snprintf(messageStr, sizeof(messageStr), "Case SQL_BLOB/SQL_CLOB/SQL_DBCLOB/SQL_XML for column index %d", i);
            LogMsg(DEBUG, messageStr);
            stmt_res->row_data[i].out_length = 0;
            snprintf(messageStr, sizeof(messageStr), "Set out_length to 0 for SQL_BLOB/SQL_CLOB/SQL_DBCLOB/SQL_XML column %d", i);
            LogMsg(DEBUG, messageStr);
            break;

        default:
            snprintf(messageStr, sizeof(messageStr), "Case default case for column index %d", i);
            LogMsg(DEBUG, messageStr);
            break;
        }
    }
    LogMsg(INFO, "exit _python_ibm_db_bind_column_helper()");
    return rc;
}

/*    static void _python_ibm_db_clear_stmt_err_cache () */
static void _python_ibm_db_clear_stmt_err_cache(void)
{
    LogMsg(INFO, "entry _python_ibm_db_clear_stmt_err_cache()");
    memset(IBM_DB_G(__python_stmt_err_msg), 0, DB2_MAX_ERR_MSG_LEN);
    memset(IBM_DB_G(__python_stmt_err_state), 0, SQL_SQLSTATE_SIZE + 1);
    LogMsg(INFO, "exit _python_ibm_db_clear_stmt_err_cache()");
}

/*    static int _python_ibm_db_connect_helper( argc, argv, isPersistent ) */
static PyObject *_python_ibm_db_connect_helper(PyObject *self, PyObject *args, int isPersistent)
{
    LogMsg(INFO, "entry _python_ibm_db_connect_helper()");
    LogMsg(DEBUG, "In process of connection");
    LogUTF8Msg(args);
    PyObject *databaseObj = NULL;
    PyObject *uidObj = NULL;
    PyObject *passwordObj = NULL;
    SQLWCHAR *database = NULL;
    SQLWCHAR *uid = NULL;
    SQLWCHAR *password = NULL;
    PyObject *options = NULL;
    PyObject *literal_replacementObj = NULL;
    SQLINTEGER literal_replacement;
    PyObject *equal = StringOBJ_FromASCII("=");
    int rc = SQL_SUCCESS;
    SQLINTEGER conn_alive;
    conn_handle *conn_res = NULL;
    int reused = 0;
    PyObject *hKey = NULL;
    PyObject *entry = NULL;
    char server[2048];
    int isNewBuffer = 0;
    PyObject *pid = NULL;
    conn_alive = 1;
    if (!PyArg_ParseTuple(args, "OOO|OO", &databaseObj, &uidObj, &passwordObj, &options, &literal_replacementObj))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: databaseObj=%p, uidObj=%p, passwordObj=%p, options=%p, literal_replacementObj=%p",
             databaseObj, uidObj, passwordObj, options, literal_replacementObj);
    LogMsg(DEBUG, messageStr);
    do
    {
        databaseObj = PyUnicode_FromObject(databaseObj);
        uidObj = PyUnicode_FromObject(uidObj);
        passwordObj = PyUnicode_FromObject(passwordObj);

        /* Check if we already have a connection for this userID & database
         * combination in this process.
         */
        if (isPersistent)
        {
            // we do not want to process a None type and segfault. Better safe than sorry!
            if (NIL_P(databaseObj))
            {
                LogMsg(ERROR, "Supplied Parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
                return NULL;
            }
            else if (!(PyUnicode_Contains(databaseObj, equal) > 0) && (NIL_P(uidObj) || NIL_P(passwordObj)))
            {
                LogMsg(ERROR, "Supplied Parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
                return NULL;
            }
            else
            {
                if (NIL_P(uidObj) || NIL_P(passwordObj))
                {
                    LogMsg(ERROR, "Supplied Parameter is invalid");
                    PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
                    return NULL;
                }
            }

            hKey = PyUnicode_Concat(StringOBJ_FromASCII("__ibm_db_"), uidObj);
            hKey = PyUnicode_Concat(hKey, databaseObj);
            hKey = PyUnicode_Concat(hKey, passwordObj);

            pid = PyObject_CallObject(os_getpid, NULL);
            if (pid == NULL)
            {
                LogMsg(EXCEPTION, "Failed to obtain current process id");
                PyErr_SetString(PyExc_Exception, "Failed to obtain current process id");
                return NULL;
            }
            snprintf(messageStr, sizeof(messageStr), "Obtain process id: %p", pid);
            LogMsg(INFO, messageStr);
            hKey = PyUnicode_Concat(hKey, PyUnicode_FromFormat("%ld", PyLong_AsLong(pid)));
            Py_DECREF(pid);

            entry = PyDict_GetItem(persistent_list, hKey);

            if (entry != NULL)
            {
                Py_INCREF(entry);
                conn_res = (conn_handle *)entry;
#if !defined(PASE) && !defined(__MVS__) /* i5/OS server mode is persistant */
                /* Need to reinitialize connection? */
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLGetConnectAttr(conn_res->hdbc, SQL_ATTR_PING_DB,
                                       (SQLPOINTER)&conn_alive, 0, NULL);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLGetConnectAttr with parameters: conn_res->hdbc=%p, SQL_ATTR_PING_DB=%d, conn_alive=%d and returned rc=%d",
                         (void *)conn_res->hdbc, SQL_ATTR_PING_DB, conn_alive, rc);
                LogMsg(DEBUG, messageStr);
                if ((rc == SQL_SUCCESS) && conn_alive)
                {
                    _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                    rc, 1, NULL, -1, 1);
                    reused = 1;
                } /* else will re-connect since connection is dead */
                else
                {
                    LogMsg(INFO, "Connection is dead. Reconnecting...");
                }
#endif /* PASE */
#if defined(__MVS__)
                /* Since SQL_ATTR_PING_DB is not supported by z ODBC driver,
                 * we will not check for db connection status */
                reused = 1;
#endif
            }
        }
        else
        {
            /* Need to check for max pconnections? */
        }

        if (!NIL_P(literal_replacementObj))
        {
            literal_replacement = (SQLINTEGER)PyLong_AsLong(literal_replacementObj);
            snprintf(messageStr, sizeof(messageStr), "Setting literal_replacement to: %d", literal_replacement);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            literal_replacement = SET_QUOTED_LITERAL_REPLACEMENT_OFF; /*QUOTED LITERAL replacemnt is OFF by default*/
            LogMsg(DEBUG, "Setting literal_replacement to SET_QUOTED_LITERAL_REPLACEMENT_OFF");
        }

        if (conn_res == NULL)
        {
            conn_res = PyObject_NEW(conn_handle, &conn_handleType);
            if (conn_res != NULL)
            {
                conn_res->henv = 0;
                conn_res->hdbc = 0;
                LogMsg(DEBUG, "Created a new connection handle");
            }
            else
            {
                LogMsg(ERROR, "Failed to allocate memory for connection handle");
                return NULL;
            }
        }

        /* We need to set this early, in case we get an error below,
        so we know how to free the connection */
        conn_res->flag_pconnect = isPersistent;
        snprintf(messageStr, sizeof(messageStr), "Set flag_pconnect to: %d", conn_res->flag_pconnect);
        LogMsg(DEBUG, messageStr);
        /* Allocate ENV handles if not present */
        if (!conn_res->henv)
        {
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &(conn_res->henv));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle called with parameters: SQL_HANDLE_ENV=%d, SQL_NULL_HANDLE=%p, &conn_res->henv=%p and returned rc=%d",
                     SQL_HANDLE_ENV, (void *)SQL_NULL_HANDLE, (void *)&(conn_res->henv), rc);
            LogMsg(DEBUG, messageStr);
            if (rc != SQL_SUCCESS)
            {
                LogMsg(ERROR, "Failed to allocate ENV handles");
                _python_ibm_db_check_sql_errors(conn_res->henv, SQL_HANDLE_ENV, rc,
                                                1, NULL, -1, 1);
                break;
            }
            else
            {
                LogMsg(DEBUG, "Successfully allocated ENV handles");
            }
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetEnvAttr((SQLHENV)conn_res->henv, SQL_ATTR_ODBC_VERSION,
                               (void *)SQL_OV_ODBC3, 0);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLSetEnvAttr called with parameters: conn_res->henv=%p, SQL_ATTR_ODBC_VERSION=%d, SQL_OV_ODBC3=%d and returned rc=%d",
                     (void *)conn_res->henv, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3, rc);
            LogMsg(DEBUG, messageStr);
        }

        if (!reused)
        {
            /* Alloc CONNECT Handle */
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLAllocHandle(SQL_HANDLE_DBC, conn_res->henv, &(conn_res->hdbc));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle called with parameters: SQL_HANDLE_DBC=%d, conn_res->henv=%p, &(conn_res->hdbc)=%p and returned rc=%d",
                     SQL_HANDLE_DBC, (void *)conn_res->henv, (void *)&(conn_res->hdbc), rc);
            LogMsg(DEBUG, messageStr);
            if (rc != SQL_SUCCESS)
            {
                LogMsg(ERROR, "Failed to allocate CONNECT Handle");
                _python_ibm_db_check_sql_errors(conn_res->henv, SQL_HANDLE_ENV, rc,
                                                1, NULL, -1, 1);
                break;
            }
            else
            {
                LogMsg(DEBUG, "Successfully allocated CONNECT Handle");
            }
        }

        /* Set this after the connection handle has been allocated to avoid
        unnecessary network flows. Initialize the structure to default values */
        conn_res->auto_commit = SQL_AUTOCOMMIT_ON;
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc, SQL_ATTR_AUTOCOMMIT,
                               (SQLPOINTER)(conn_res->auto_commit), SQL_NTS);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Set auto_commit to: %d", conn_res->auto_commit);
        LogMsg(DEBUG, messageStr);
        snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttr called with parameters: conn_res->hdbc=%p, SQL_ATTR_AUTOCOMMIT=%d, conn_res->auto_commit=%d, SQL_NTS=%d and returned rc=%d",
                 (void *)conn_res->hdbc, SQL_ATTR_AUTOCOMMIT, conn_res->auto_commit, SQL_NTS, rc);
        LogMsg(DEBUG, messageStr);
        conn_res->c_bin_mode = IBM_DB_G(bin_mode);
        conn_res->c_case_mode = CASE_NATURAL;
        conn_res->c_use_wchar = WCHAR_YES;
        conn_res->c_cursor_type = SQL_SCROLL_FORWARD_ONLY;

        conn_res->error_recno_tracker = 1;
        conn_res->errormsg_recno_tracker = 1;

        /* handle not active as of yet */
        conn_res->handle_active = 0;

        /* Set Options */
        if (!NIL_P(options))
        {
            if (!PyDict_Check(options))
            {
                LogMsg(EXCEPTION, "options Parameter must be of type dictionary");
                PyErr_SetString(PyExc_Exception, "options Parameter must be of type dictionary");
                return NULL;
            }
            rc = _python_ibm_db_parse_options(options, SQL_HANDLE_DBC, conn_res);
            if (rc != SQL_SUCCESS)
            {
                LogMsg(ERROR, "Failed to parse options");
                Py_BEGIN_ALLOW_THREADS;
                SQLFreeHandle(SQL_HANDLE_DBC, conn_res->hdbc);
                SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
                Py_END_ALLOW_THREADS;
                break;
            }
            else
            {
                LogMsg(DEBUG, "Successfully parsed options");
            }
        }

        if (!reused)
        {
            /* Connect */
            /* If the string contains a =, use SQLDriverConnect */
            if (NIL_P(databaseObj))
            {
                LogMsg(ERROR, "Invalid database parameter");
                PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
                return NULL;
            }
            database = getUnicodeDataAsSQLWCHAR(databaseObj, &isNewBuffer);
            if (PyUnicode_Contains(databaseObj, equal) > 0)
            {
                LogMsg(DEBUG, "Using SQLDriverConnectW for connection");
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLDriverConnectW((SQLHDBC)conn_res->hdbc, (SQLHWND)NULL,
                                       database, SQL_NTS, NULL, 0, NULL,
                                       SQL_DRIVER_NOPROMPT);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLDriverConnectW called with parameters: conn_res->hdbc=%p, SQLHWND=NULL, database=%ls, SQL_NTS=%d, NULL, 0, NULL, SQL_DRIVER_NOPROMPT=%d and returned rc=%d",
                         (void *)conn_res->hdbc, database, SQL_NTS, SQL_DRIVER_NOPROMPT, rc);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(DEBUG, "Using SQLConnectW for connection");
                if (NIL_P(uidObj) || NIL_P(passwordObj))
                {
                    LogMsg(ERROR, "Invalid uid or password parameter");
                    PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
                    return NULL;
                }
                uid = getUnicodeDataAsSQLWCHAR(uidObj, &isNewBuffer);
                password = getUnicodeDataAsSQLWCHAR(passwordObj, &isNewBuffer);
                Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
                rc = SQLConnectW((SQLHDBC)conn_res->hdbc,
                                 database,
                                 PyUnicode_GetLength(databaseObj) * 2,
                                 uid,
                                 PyUnicode_GetLength(uidObj) * 2,
                                 password,
                                 PyUnicode_GetLength(passwordObj) * 2);
                snprintf(messageStr, sizeof(messageStr), "SQLConnectW called with parameters: conn_res->hdbc=%p, database=%ls, databaseLen=%zd, uid=%ls, uidLen=%zd, password=%ls, passwordLen=%zd and returned rc=%d",
                         (void *)conn_res->hdbc, database,
                         PyUnicode_GetLength(databaseObj) * 2, uid,
                         PyUnicode_GetLength(uidObj) * 2, password,
                         PyUnicode_GetLength(passwordObj) * 2, rc);
                LogMsg(DEBUG, messageStr);
#else
                rc = SQLConnectW((SQLHDBC)conn_res->hdbc,
                                 database,
                                 PyUnicode_GetLength(databaseObj),
                                 uid,
                                 PyUnicode_GetLength(uidObj),
                                 password,
                                 PyUnicode_GetLength(passwordObj));
                snprintf(messageStr, sizeof(messageStr), "SQLConnectW called with parameters: conn_res->hdbc=%p, database=%ls, databaseLen=%zd, uid=%ls, uidLen=%zd, password=%ls, passwordLen=%zd and returned rc=%d",
                         (void *)conn_res->hdbc, database,
                         PyUnicode_GetLength(databaseObj), uid,
                         PyUnicode_GetLength(uidObj), password,
                         PyUnicode_GetLength(passwordObj), rc);
                LogMsg(DEBUG, messageStr);
#endif
                Py_END_ALLOW_THREADS;
            }
            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                LogMsg(DEBUG, "Checking SQL connection errors");
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                rc, 1, NULL, -1, 1);
            }
            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "Failed to connect to database");
                Py_BEGIN_ALLOW_THREADS;
                SQLFreeHandle(SQL_HANDLE_DBC, conn_res->hdbc);
                SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
                Py_END_ALLOW_THREADS;
                break;
            }

#ifdef CLI_DBC_SERVER_TYPE_DB2LUW
#ifdef SQL_ATTR_DECFLOAT_ROUNDING_MODE
            /**
             * Code for setting SQL_ATTR_DECFLOAT_ROUNDING_MODE
             * for implementation of Decfloat Datatype
             */
            LogMsg(DEBUG, "Setting SQL_ATTR_DECFLOAT_ROUNDING_MODE");

            rc = _python_ibm_db_set_decfloat_rounding_mode_client(conn_res->hdbc);
            if (rc != SQL_SUCCESS)
            {
                LogMsg(ERROR, "Failed to set SQL_ATTR_DECFLOAT_ROUNDING_MODE");
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc,
                                                1, NULL, -1, 1);
            }
#endif
#endif

            /* Get the server name */
            LogMsg(INFO, "Getting server name");
            memset(server, 0, sizeof(server));

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLGetInfo(conn_res->hdbc, SQL_DBMS_NAME, (SQLPOINTER)server,
                            2048, NULL);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters: conn_res->hdbc=%p, SQL_DBMS_NAME=%d, server=%s, 2048 and returned rc=%d",
                     (void *)conn_res->hdbc, SQL_DBMS_NAME, server, rc);
            LogMsg(DEBUG, messageStr);
            if (!strcmp(server, "AS"))
            {
                LogMsg(INFO, "Server identified as AS");
                is_systemi = 1;
            }
            if (!strncmp(server, "IDS", 3))
            {
                LogMsg(INFO, "Server identified as Informix");
                is_informix = 1;
            }

            /* Set SQL_ATTR_REPLACE_QUOTED_LITERALS connection attribute to
             * enable CLI numeric literal feature. This is equivalent to
             * PATCH2=71 in the db2cli.ini file
             * Note, for backward compatibility with older CLI drivers having a
             * different value for SQL_ATTR_REPLACE_QUOTED_LITERALS, we call
             * SQLSetConnectAttr() with both the old and new value
             */
            /* Only enable this feature if we are not connected to an Informix data
             * server
             */
            if (!is_informix && (literal_replacement == SET_QUOTED_LITERAL_REPLACEMENT_ON))
            {
                LogMsg(DEBUG, "Enabling SQL_ATTR_REPLACE_QUOTED_LITERALS");
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc,
                                       SQL_ATTR_REPLACE_QUOTED_LITERALS,
                                       (SQLPOINTER)(ENABLE_NUMERIC_LITERALS),
                                       SQL_IS_INTEGER);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttr called with parameters: conn_res->hdbc=%p, SQL_ATTR_REPLACE_QUOTED_LITERALS=%d, ENABLE_NUMERIC_LITERALS=%d, SQL_IS_INTEGER=%d and returned rc=%d",
                         (void *)conn_res->hdbc, SQL_ATTR_REPLACE_QUOTED_LITERALS, ENABLE_NUMERIC_LITERALS, SQL_IS_INTEGER, rc);
                LogMsg(DEBUG, messageStr);
                if (rc != SQL_SUCCESS)
                {
                    LogMsg(ERROR, "Failed to set SQL_ATTR_REPLACE_QUOTED_LITERALS");
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc,
                                           SQL_ATTR_REPLACE_QUOTED_LITERALS_OLDVALUE,
                                           (SQLPOINTER)(ENABLE_NUMERIC_LITERALS),
                                           SQL_IS_INTEGER);
                    Py_END_ALLOW_THREADS;
                    snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttr called with parameters: conn_res->hdbc=%p, SQL_ATTR_REPLACE_QUOTED_LITERALS_OLDVALUE=%d, ENABLE_NUMERIC_LITERALS=%d, SQL_IS_INTEGER=%d and returned rc=%d",
                             (void *)conn_res->hdbc, SQL_ATTR_REPLACE_QUOTED_LITERALS_OLDVALUE, ENABLE_NUMERIC_LITERALS, SQL_IS_INTEGER, rc);
                    LogMsg(DEBUG, messageStr);
                }
            }
            if (rc != SQL_SUCCESS)
            {
                LogMsg(ERROR, "Failed to set connection attributes");
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc,
                                                1, NULL, -1, 1);
            }
        }
        Py_XDECREF(databaseObj);
        Py_XDECREF(uidObj);
        Py_XDECREF(passwordObj);
        conn_res->handle_active = 1;
    } while (0);

    if (hKey != NULL)
    {
        if (!reused && rc == SQL_SUCCESS)
        {
            /* If we created a new persistent connection, add it to the
             *  persistent_list
             */
            LogMsg(DEBUG, "Adding connection to persistent_list");
            PyDict_SetItem(persistent_list, hKey, (PyObject *)conn_res);
        }
        Py_DECREF(hKey);
    }

    if (isNewBuffer)
    {
        PyMem_Del(database);
        PyMem_Del(uid);
        PyMem_Del(password);
    }

    if (rc != SQL_SUCCESS)
    {
        if (conn_res != NULL && conn_res->handle_active)
        {
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeHandle(SQL_HANDLE_DBC, conn_res->hdbc);
            rc = SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
            Py_END_ALLOW_THREADS;
        }
        if (conn_res != NULL)
        {
            PyObject_Del(conn_res);
        }
        LogMsg(ERROR, "Failed to establish connection!");
        return NULL;
    }
    LogMsg(INFO, "Connection successfully established!");
    LogMsg(INFO, "exit _python_ibm_db_connect_helper()");
    if (isPersistent)
    {
        LogMsg(INFO, "exit pconnect()");
    }
    else
    {
        LogMsg(INFO, "exit connect()");
    }
    return (PyObject *)conn_res;
}

/**
 *This function takes a SQLWCHAR buffer (UCS-2) and returns back a PyUnicode object
 * of it that is in the correct current UCS encoding (either UCS2 or UCS4)of
 * the current executing python VM
 *
 * @sqlwcharBytesLen - the length of sqlwcharData in bytes (not characters)
 **/
static PyObject *getSQLWCharAsPyUnicodeObject(SQLWCHAR *sqlwcharData, int sqlwcharBytesLen)
{
    LogMsg(INFO, "entry getSQLWCharAsPyUnicodeObject()");
    snprintf(messageStr, sizeof(messageStr), "sqlwcharData=%p, sqlwcharBytesLen=%d", (void *)sqlwcharData, sqlwcharBytesLen);
    LogMsg(DEBUG, messageStr);
    PyObject *sysmodule = NULL, *maxuni = NULL;
    long maxuniValue;
    PyObject *u;
    sysmodule = PyImport_ImportModule("sys");
    maxuni = PyObject_GetAttrString(sysmodule, "maxunicode");
    maxuniValue = PyLong_AsLong(maxuni);
    snprintf(messageStr, sizeof(messageStr), "sysmodule obtained: %p, maxuni obtained: %p, maxuniValue: %ld",
             (void *)sysmodule, (void *)maxuni, maxuniValue);
    LogMsg(DEBUG, messageStr);

    if (maxuniValue <= 65536)
    {
        /* this is UCS2 python.. nothing to do really */
        LogMsg(DEBUG, "Python is UCS2, using PyUnicode_FromWideChar");
        PyObject *result = PyUnicode_FromWideChar((wchar_t *)sqlwcharData, sqlwcharBytesLen / sizeof(SQLWCHAR));
        snprintf(messageStr, sizeof(messageStr), "UCS2 conversion result: %p", (void *)result);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit getSQLWCharAsPyUnicodeObject()");
        return PyUnicode_FromWideChar((wchar_t *)sqlwcharData, sqlwcharBytesLen / sizeof(SQLWCHAR));
    }

    if (is_bigendian())
    {
        int bo = 1;
        LogMsg(INFO, "Big endian detected, decoding UTF16");
        u = PyUnicode_DecodeUTF16((char *)sqlwcharData, sqlwcharBytesLen, "strict", &bo);
        snprintf(messageStr, sizeof(messageStr), "UTF16 decoding result: %p, byteorder: %d", (void *)u, bo);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        int bo = -1;
        LogMsg(INFO, "Little endian detected, decoding UTF16");
        u = PyUnicode_DecodeUTF16((char *)sqlwcharData, sqlwcharBytesLen, "strict", &bo);
        snprintf(messageStr, sizeof(messageStr), "UTF16 decoding result: %p, byteorder: %d", (void *)u, bo);
        LogMsg(DEBUG, messageStr);
    }
    LogMsg(INFO, "exit getSQLWCharAsPyUnicodeObject()");
    return u;
}

static SQLCHAR *getUnicodeDataAsSQLCHAR(PyObject *pyobj, int *isNewBuffer)
{
    LogMsg(INFO, "entry getUnicodeDataAsSQLCHAR()");
    snprintf(messageStr, sizeof(messageStr), "pyobj=%p, isNewBuffer=%p", (void *)pyobj, (void *)isNewBuffer);
    LogMsg(DEBUG, messageStr);
    SQLCHAR *pNewBuffer = NULL;
    PyObject *pyBytesobj = PyUnicode_AsUTF8String(pyobj);
    int nCharLen = PyBytes_GET_SIZE(pyBytesobj);
    snprintf(messageStr, sizeof(messageStr), "pyBytesobj obtained: %p, Number of bytes (nCharLen): %d", (void *)pyBytesobj, nCharLen);
    LogMsg(DEBUG, messageStr);

    *isNewBuffer = 1;
    pNewBuffer = (SQLCHAR *)ALLOC_N(SQLCHAR, nCharLen + 1);
    snprintf(messageStr, sizeof(messageStr), "Allocated new buffer: pNewBuffer=%p, size=%d", (void *)pNewBuffer, nCharLen + 1);
    LogMsg(DEBUG, messageStr);
    memset(pNewBuffer, 0, sizeof(SQLCHAR) * (nCharLen + 1));
    memcpy(pNewBuffer, PyBytes_AsString(pyBytesobj), sizeof(SQLCHAR) * (nCharLen));
    LogMsg(DEBUG, "Buffer filled with data from pyBytesobj");
    Py_DECREF(pyBytesobj);
    LogMsg(DEBUG, "Decremented reference count for pyBytesobj, now returning pNewBuffer");
    LogMsg(INFO, "exit getUnicodeDataAsSQLCHAR()");
    return pNewBuffer;
}

/**
 *This function takes value as pyObject and convert it to SQLWCHAR and return it
 *
 **/
static SQLWCHAR *getUnicodeDataAsSQLWCHAR(PyObject *pyobj, int *isNewBuffer)
{
    LogMsg(INFO, "entry getUnicodeDataAsSQLWCHAR()");
    snprintf(messageStr, sizeof(messageStr), "pyobj=%p, isNewBuffer=%p", (void *)pyobj, (void *)isNewBuffer);
    LogMsg(DEBUG, messageStr);
    PyObject *sysmodule = NULL, *maxuni = NULL;
    long maxuniValue;
    PyObject *pyUTFobj;
    SQLWCHAR *pNewBuffer = NULL;
    int nCharLen = PyUnicode_GET_LENGTH(pyobj);
    snprintf(messageStr, sizeof(messageStr), "Unicode length (nCharLen): %d", nCharLen);
    LogMsg(DEBUG, messageStr);
    sysmodule = PyImport_ImportModule("sys");
    maxuni = PyObject_GetAttrString(sysmodule, "maxunicode");
    maxuniValue = PyLong_AsLong(maxuni);
    snprintf(messageStr, sizeof(messageStr), "sysmodule obtained: %p, maxuni obtained: %p, maxuniValue: %ld", (void *)sysmodule, (void *)maxuni, maxuniValue);
    LogMsg(DEBUG, messageStr);
    if (maxuniValue <= 65536)
    {
        *isNewBuffer = 0;
        SQLWCHAR *result = (SQLWCHAR *)PyUnicode_AsWideCharString(pyobj, &maxuniValue);
        if (result == NULL) {
            LogMsg(ERROR, "PyUnicode_AsWideCharString() failed");
            return NULL;
        }
        snprintf(messageStr, sizeof(messageStr), " result obtained: %p", (void *)result);
        LogMsg(DEBUG, "UCS2 case:");
        LogMsg(INFO, "exit getUnicodeDataAsSQLWCHAR()");
        return result;
    }
    *isNewBuffer = 1;
    pNewBuffer = (SQLWCHAR *)ALLOC_N(SQLWCHAR, nCharLen + 1);
    snprintf(messageStr, sizeof(messageStr), "Allocated new buffer: pNewBuffer=%p, size=%d", (void *)pNewBuffer, nCharLen + 1);
    LogMsg(DEBUG, messageStr);
    memset(pNewBuffer, 0, sizeof(SQLWCHAR) * (nCharLen + 1));
    LogMsg(DEBUG, "Buffer initialized to zero");
    if (is_bigendian())
    {
        pyUTFobj = PyCodec_Encode(pyobj, "utf-16-be", "strict");
        snprintf(messageStr, sizeof(messageStr), "Encoded to UTF-16 Big Endian: pyUTFobj=%p", (void *)pyUTFobj);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        pyUTFobj = PyCodec_Encode(pyobj, "utf-16-le", "strict");
        snprintf(messageStr, sizeof(messageStr), "Encoded to UTF-16 Little Endian: pyUTFobj=%p", (void *)pyUTFobj);
        LogMsg(DEBUG, messageStr);
    }
    memcpy(pNewBuffer, PyBytes_AsString(pyUTFobj), sizeof(SQLWCHAR) * (nCharLen));
    snprintf(messageStr, sizeof(messageStr), "Copied data to pNewBuffer: pNewBuffer=%p", (void *)pNewBuffer);
    LogMsg(DEBUG, messageStr);
    Py_DECREF(pyUTFobj);
    Py_DECREF(sysmodule);
    LogMsg(DEBUG, "Decremented reference count for pyUTFobj");
    LogMsg(INFO, "exit getUnicodeDataAsSQLWCHAR()");
    return pNewBuffer;
}

#ifdef CLI_DBC_SERVER_TYPE_DB2LUW
#ifdef SQL_ATTR_DECFLOAT_ROUNDING_MODE

/**
 * Function for implementation of DECFLOAT Datatype
 *
 * Description :
 * This function retrieves the value of special register decflt_rounding
 * from the database server which signifies the current rounding mode set
 * on the server. For using decfloat, the rounding mode has to be in sync
 * on the client as well as server. Thus we set here on the client, the
 * same rounding mode as the server.
 *
 * @return: success or failure
 * */
static int _python_ibm_db_set_decfloat_rounding_mode_client(SQLHANDLE hdbc)
{
    LogMsg(INFO, "entry _python_ibm_db_set_decfloat_rounding_mode_client()");
    SQLCHAR decflt_rounding[20];
    SQLHANDLE hstmt;
    int rc = 0;
    int rounding_mode;
    SQLINTEGER decfloat;

    SQLCHAR *stmt = (SQLCHAR *)"values current decfloat rounding mode";

    /* Allocate a Statement Handle */
    snprintf(messageStr, sizeof(messageStr), "Calling SQLAllocHandle with SQL_HANDLE_STMT, hdbc=%p, hstmt=%p",
             (void *)hdbc, (void *)hstmt);
    LogMsg(DEBUG, messageStr);
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle result: rc=%d, hstmt=%p", rc, (void *)hstmt);
    LogMsg(DEBUG, messageStr);
    if (rc == SQL_ERROR)
    {
        _python_ibm_db_check_sql_errors(hdbc, SQL_HANDLE_DBC, rc, 1,
                                        NULL, -1, 1);
        return rc;
    }
    snprintf(messageStr, sizeof(messageStr), "Executing SQL statement: %s", stmt);
    LogMsg(DEBUG, messageStr);
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLExecDirect((SQLHSTMT)hstmt, stmt, SQL_NTS);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLExecDirect result: rc=%d", rc);
    LogMsg(DEBUG, messageStr);
    if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
    {
        _python_ibm_db_check_sql_errors((SQLHSTMT)hstmt,
                                        SQL_HANDLE_STMT, rc, 1, NULL,
                                        -1, 1);
    }
    if (rc == SQL_ERROR)
    {
        return rc;
    }
    snprintf(messageStr, sizeof(messageStr),
             "Calling SQLBindCol with hstmt=%p, column_number=%d, C_type=%d, buffer=%p, buffer_length=%zu, indicator=NULL",
             (void *)hstmt, 1, SQL_C_DEFAULT, (void *)decflt_rounding, sizeof(decflt_rounding));
    LogMsg(DEBUG, messageStr);
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLBindCol((SQLHSTMT)hstmt, 1, SQL_C_DEFAULT, decflt_rounding, 20, NULL);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLBindCol result: rc=%d, bound_column_value='%s'", rc, decflt_rounding);
    LogMsg(DEBUG, messageStr);

    if (rc == SQL_ERROR)
    {
        _python_ibm_db_check_sql_errors((SQLHSTMT)hstmt,
                                        SQL_HANDLE_STMT, rc, 1, NULL,
                                        -1, 1);
        return rc;
    }
    snprintf(messageStr, sizeof(messageStr), "Calling SQLFetch on hstmt=%p", (void *)hstmt);
    LogMsg(DEBUG, messageStr);
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLFetch(hstmt);
    if (rc == SQL_SUCCESS_WITH_INFO)
    {
        _python_ibm_db_check_sql_errors((SQLHSTMT)hstmt,
                                        SQL_HANDLE_STMT, rc, 1, NULL,
                                        -1, 1);
    }
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLFetch result: rc=%d", rc);
    LogMsg(DEBUG, messageStr);

    snprintf(messageStr, sizeof(messageStr), "Calling SQLFreeHandle with SQL_HANDLE_STMT, hstmt=%p", (void *)hstmt);
    LogMsg(DEBUG, messageStr);
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLFreeHandle result: rc=%d", rc);
    LogMsg(DEBUG, messageStr);
    /* Now setting up the same rounding mode on the client*/
    if (strcmp(decflt_rounding, "ROUND_HALF_EVEN") == 0)
        rounding_mode = ROUND_HALF_EVEN;
    if (strcmp(decflt_rounding, "ROUND_HALF_UP") == 0)
        rounding_mode = ROUND_HALF_UP;
    if (strcmp(decflt_rounding, "ROUND_DOWN") == 0)
        rounding_mode = ROUND_DOWN;
    if (strcmp(decflt_rounding, "ROUND_CEILING") == 0)
        rounding_mode = ROUND_CEILING;
    if (strcmp(decflt_rounding, "ROUND_FLOOR") == 0)
        rounding_mode = ROUND_FLOOR;

    snprintf(messageStr, sizeof(messageStr), "Setting client rounding mode to: %d", rounding_mode);
    LogMsg(DEBUG, messageStr);

    snprintf(messageStr, sizeof(messageStr),
             "Calling SQLSetConnectAttr with SQL_ATTR_DECFLOAT_ROUNDING_MODE, rounding_mode=%d", rounding_mode);
    LogMsg(DEBUG, messageStr);
    Py_BEGIN_ALLOW_THREADS;
#ifndef PASE
    rc = SQLSetConnectAttr(hdbc, SQL_ATTR_DECFLOAT_ROUNDING_MODE, (SQLPOINTER)rounding_mode, SQL_NTS);
#else
    rc = SQLSetConnectAttr(hdbc, SQL_ATTR_DECFLOAT_ROUNDING_MODE, (SQLPOINTER)&rounding_mode, SQL_NTS);
#endif
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttr result: rc=%d", rc);
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit _python_ibm_db_set_decfloat_rounding_mode_client()");
    return rc;
}
#endif
#endif

/* static void _python_ibm_db_clear_conn_err_cache () */
static void _python_ibm_db_clear_conn_err_cache(void)
{
    /* Clear out the cached conn messages */
    LogMsg(INFO, "entry _python_ibm_db_clear_conn_err_cache()");
    memset(IBM_DB_G(__python_conn_err_msg), 0, DB2_MAX_ERR_MSG_LEN);
    memset(IBM_DB_G(__python_conn_err_state), 0, SQL_SQLSTATE_SIZE + 1);
    memset(IBM_DB_G(__python_err_code), 0, SQL_SQLCODE_SIZE + 1);
    LogMsg(INFO, "exit _python_ibm_db_clear_conn_err_cache()");
}

/*!#
 * ibm_db.connect
 * ibm_db.pconnect
 * ibm_db.autocommit
 * ibm_db.bind_param
 * ibm_db.close
 * ibm_db.column_privileges
 * ibm_db.columns
 * ibm_db.foreign_keys
 * ibm_db.primary_keys
 * ibm_db.procedure_columns
 * ibm_db.procedures
 * ibm_db.special_columns
 * ibm_db.statistics
 * ibm_db.table_privileges
 * ibm_db.tables
 * ibm_db.commit
 * ibm_db.exec
 * ibm_db.free_result
 * ibm_db.prepare
 * ibm_db.execute
 * ibm_db.conn_errormsg
 * ibm_db.stmt_errormsg
 * ibm_db.conn_error
 * ibm_db.stmt_error
 * ibm_db.next_result
 * ibm_db.num_fields
 * ibm_db.num_rows
 * ibm_db.get_num_result
 * ibm_db.field_name
 * ibm_db.field_display_size
 * ibm_db.field_num
 * ibm_db.field_precision
 * ibm_db.field_scale
 * ibm_db.field_type
 * ibm_db.field_width
 * ibm_db.cursor_type
 * ibm_db.rollback
 * ibm_db.free_stmt
 * ibm_db.result
 * ibm_db.fetch_row
 * ibm_db.fetch_assoc
 * ibm_db.fetch_array
 * ibm_db.fetch_both
 * ibm_db.set_option
 * ibm_db.server_info
 * ibm_db.client_info
 * ibm_db.active
 * ibm_db.get_option
 */

static PyObject *ibm_db_get_sqlcode(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry get_sqlcode()");
    LogUTF8Msg(args);
    conn_handle *conn_res = NULL;
    PyObject *py_conn_res = NULL;
    stmt_handle *stmt_res = NULL;
    PyObject *py_stmt_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return err
                              * strings */
    if (!PyArg_ParseTuple(args, "|O", &py_conn_res) | (!PyArg_ParseTuple(args, "|O", &py_stmt_res)))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_stmt_res=%p", py_conn_res, py_stmt_res);
    LogMsg(DEBUG, messageStr);

    if ((!NIL_P(py_conn_res)) || (!NIL_P(py_stmt_res)))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied Connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }

        return_str = PyMem_New(char, DB2_MAX_ERR_MSG_LEN);
        if (return_str == NULL)
        {
            PyErr_NoMemory();
            return NULL;
        }
        memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);
        snprintf(messageStr, sizeof(messageStr), "Allocated return_str: %p, size: %d", (void *)return_str, DB2_MAX_ERR_MSG_LEN);
        LogMsg(DEBUG, messageStr);
        if (SQL_HANDLE_DBC)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, -1, 0,
                                            return_str, DB2_ERR,
                                            conn_res->error_recno_tracker);
            snprintf(messageStr, sizeof(messageStr), "SQL errors checked for DBC. return_str: %s", return_str);
            LogMsg(DEBUG, messageStr);
        }
        if (SQL_HANDLE_STMT)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, -1, 0,
                                            return_str, DB2_ERR,
                                            stmt_res->error_recno_tracker);
            snprintf(messageStr, sizeof(messageStr), "SQL errors checked for STMT. return_str: %s", return_str);
            LogMsg(DEBUG, messageStr);
        }
        if (conn_res->error_recno_tracker - conn_res->errormsg_recno_tracker >= 1)
        {
            LogMsg(DEBUG, "Updating conn_res->errormsg_recno_tracker");
            conn_res->errormsg_recno_tracker = conn_res->error_recno_tracker;
        }
        conn_res->error_recno_tracker++;
        snprintf(messageStr, sizeof(messageStr), "Updated conn error_recno_tracker: %d, errormsg_recno_tracker: %d", conn_res->error_recno_tracker, stmt_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);
        if (stmt_res->error_recno_tracker - stmt_res->errormsg_recno_tracker >= 1)
        {
            LogMsg(DEBUG, "Updating stmt_res->errormsg_recno_tracker");
            stmt_res->errormsg_recno_tracker = stmt_res->error_recno_tracker;
        }
        stmt_res->error_recno_tracker++;
        snprintf(messageStr, sizeof(messageStr), "Updated stmt error_recno_tracker: %d, errormsg_recno_tracker: %d", stmt_res->error_recno_tracker, stmt_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);
        if (return_str != NULL)
        {
            retVal = StringOBJ_FromASCII(return_str);
            PyMem_Del(return_str);
            return_str = NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Created return value: %p", (void *)retVal);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit get_sqlcode()");
        return retVal;
    }
    else
    {
        PyObject *defaultErrorCode = StringOBJ_FromASCII(IBM_DB_G(__python_err_code));
        snprintf(messageStr, sizeof(messageStr), "Connection or Statement object is not provided. Returning default error sqlcode: %s", PyUnicode_AsUTF8(defaultErrorCode));
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit get_sqlcode()");
        return defaultErrorCode;
    }
}

/*!# ibm_db.connect
 *
 * ===Description
 *
 *  --    Returns a connection to a database
 * IBM_DBConnection ibm_db.connect (dsn=<..>, user=<..>, password=<..>,
 *                                  host=<..>, database=<..>, options=<..>)
 *
 * Creates a new connection to an IBM DB2 Universal Database, IBM Cloudscape,
 * or Apache Derby database.
 *
 * ===Parameters
 *
 * ====dsn
 *
 * For an uncataloged connection to a database, database represents a complete
 * connection string in the following format:
 * DRIVER={IBM DB2 ODBC DRIVER};DATABASE=database;HOSTNAME=hostname;PORT=port;
 * PROTOCOL=TCPIP;UID=username;PWD=password;
 *      where the parameters represent the following values:
 *        hostname
 *            The hostname or IP address of the database server.
 *        port
 *            The TCP/IP port on which the database is listening for requests.
 *        username
 *            The username with which you are connecting to the database.
 *        password
 *            The password with which you are connecting to the database.
 *
 * ====user
 *
 * The username with which you are connecting to the database.
 * This is optional if username is specified in the "dsn" string
 *
 * ====password
 *
 * The password with which you are connecting to the database.
 * This is optional if password is specified in the "dsn" string
 *
 * ====host
 *
 * The hostname or IP address of the database server.
 * This is optional if hostname is specified in the "dsn" string
 *
 * ====database
 *
 * For a cataloged connection to a database, database represents the database
 * alias in the DB2 client catalog.
 * This is optional if database is specified in the "dsn" string
 *
 * ====options
 *
 *      An dictionary of connection options that affect the behavior of the
 *      connection,
 *      where valid array keys include:
 *        SQL_ATTR_AUTOCOMMIT
 *            Passing the SQL_AUTOCOMMIT_ON value turns autocommit on for this
 *            connection handle.
 *            Passing the SQL_AUTOCOMMIT_OFF value turns autocommit off for this
 *            connection handle.
 *        ATTR_CASE
 *            Passing the CASE_NATURAL value specifies that column names are
 *            returned in natural case.
 *            Passing the CASE_LOWER value specifies that column names are
 *            returned in lower case.
 *            Passing the CASE_UPPER value specifies that column names are
 *            returned in upper case.
 *        SQL_ATTR_CURSOR_TYPE
 *            Passing the SQL_SCROLL_FORWARD_ONLY value specifies a forward-only
 *            cursor for a statement resource.
 *            This is the default cursor type and is supported on all database
 *            servers.
 *            Passing the SQL_CURSOR_KEYSET_DRIVEN value specifies a scrollable
 *            cursor for a statement resource.
 *            This mode enables random access to rows in a result set, but
 *            currently is supported only by IBM DB2 Universal Database.
 * ====set_replace_quoted_literal
 *      This variable indicates if the CLI Connection attribute SQL_ATTR_REPLACE_QUOTED_LITERAL is to be set or not
 *      To turn it ON pass  IBM_DB::SET_QUOTED_LITERAL_REPLACEMENT_ON
 *      To turn it OFF pass IBM_DB::SET_QUOTED_LITERAL_REPLACEMENT_OFF
 *
 *      Default Setting: - IBM_DB::SET_QUOTED_LITERAL_REPLACEMENT_ON
 *
 * ===Return Values
 *
 *
 * Returns a IBM_DBConnection connection object if the connection attempt is
 * successful.
 * If the connection attempt fails, ibm_db.connect() returns None.
 *
 */
static PyObject *ibm_db_connect(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry connect()");
    _python_ibm_db_clear_conn_err_cache();
    return _python_ibm_db_connect_helper(self, args, 0);
}

/*!# ibm_db.pconnect
 *
 * ===Description
 *  --    Returns a persistent connection to a database
 * resource ibm_db.pconnect ( string database, string username, string password
 * [, array options] )
 *
 * Returns a persistent connection to an IBM DB2 Universal Database,
 * IBM Cloudscape, Apache Derby or Informix Dynamic Server database.
 *
 * Calling ibm_db.close() on a persistent connection always returns TRUE, but
 * the underlying DB2 client connection remains open and waiting to serve the
 * next matching ibm_db.pconnect() request.
 *
 * ===Parameters
 *
 * ====database
 *        The database alias in the DB2 client catalog.
 *
 * ====username
 *        The username with which you are connecting to the database.
 *
 * ====password
 *        The password with which you are connecting to the database.
 *
 * ====options
 *        An associative array of connection options that affect the behavior of
 * the connection,
 *        where valid array keys include:
 *
 *        autocommit
 *             Passing the DB2_AUTOCOMMIT_ON value turns autocommit on for this
 * connection handle.
 *             Passing the DB2_AUTOCOMMIT_OFF value turns autocommit off for
 * this connection handle.
 *
 *        DB2_ATTR_CASE
 *             Passing the DB2_CASE_NATURAL value specifies that column names
 * are returned in natural case.
 *             Passing the DB2_CASE_LOWER value specifies that column names are
 * returned in lower case.
 *             Passing the DB2_CASE_UPPER value specifies that column names are
 * returned in upper case.
 *
 *        CURSOR
 *             Passing the SQL_SCROLL_FORWARD_ONLY value specifies a
 * forward-only cursor for a statement resource.  This is the default cursor
 * type and is supported on all database servers.
 *             Passing the SQL_CURSOR_KEYSET_DRIVEN value specifies a scrollable
 * cursor for a statement resource. This mode enables random access to rows in a
 * result set, but currently is supported only by IBM DB2 Universal Database.
 *
 * ====set_replace_quoted_literal
 *      This variable indicates if the CLI Connection attribute SQL_ATTR_REPLACE_QUOTED_LITERAL is to be set or not
 *      To turn it ON pass  IBM_DB::SET_QUOTED_LITERAL_REPLACEMENT_ON
 *      To turn it OFF pass IBM_DB::SET_QUOTED_LITERAL_REPLACEMENT_OFF
 *
 *      Default Setting: - IBM_DB::SET_QUOTED_LITERAL_REPLACEMENT_ON
 *
 * ===Return Values
 *
 * Returns a connection handle resource if the connection attempt is successful.
 * ibm_db.pconnect() tries to reuse an existing connection resource that exactly
 * matches the database, username, and password parameters. If the connection
 * attempt fails, ibm_db.pconnect() returns FALSE.
 */
static PyObject *ibm_db_pconnect(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry pconnect()");
    _python_ibm_db_clear_conn_err_cache();
    return _python_ibm_db_connect_helper(self, args, 1);
}

/*
 * static void _python_clear_local_var(PyObject *dbNameObj, SQLWCHAR *dbName, PyObject *codesetObj, SQLWCHAR *codesetObj, PyObject *modeObj, SQLWCHAR *mode, int isNewBuffer)
 */
static void _python_clear_local_var(PyObject *dbNameObj, SQLWCHAR *dbName, PyObject *codesetObj, SQLWCHAR *codeset, PyObject *modeObj, SQLWCHAR *mode, int isNewBuffer)
{
    LogMsg(INFO, "entry _python_clear_local_var()");
    snprintf(messageStr, sizeof(messageStr),
             "Before clearing: dbNameObj=%p, dbName=%p, codesetObj=%p, codeset=%p, modeObj=%p, mode=%p",
             (void *)dbNameObj, (void *)dbName, (void *)codesetObj, (void *)codeset, (void *)modeObj, (void *)mode);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(dbNameObj))
    {
        Py_XDECREF(dbNameObj);
        if (isNewBuffer)
        {
            PyMem_Del(dbName);
        }
    }

    if (!NIL_P(codesetObj))
    {
        Py_XDECREF(codesetObj);
        if (isNewBuffer)
        {
            PyMem_Del(codeset);
        }
    }

    if (!NIL_P(modeObj))
    {
        Py_XDECREF(modeObj);
        if (isNewBuffer)
        {
            PyMem_Del(mode);
        }
    }
    snprintf(messageStr, sizeof(messageStr),
             "After clearing: dbNameObj=%p, dbName=%p, codesetObj=%p, codeset=%p, modeObj=%p, mode=%p",
             (void *)dbNameObj, (void *)dbName, (void *)codesetObj, (void *)codeset, (void *)modeObj, (void *)mode);
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit _python_clear_local_var()");
}

/*
 * static int _python_ibm_db_createdb(conn_handle *conn_res, PyObject *dbNameObj, PyObject *codesetObj, PyObject *modeObj, int createNX)
 */
static int _python_ibm_db_createdb(conn_handle *conn_res, PyObject *dbNameObj, PyObject *codesetObj, PyObject *modeObj, int createNX)
{
    LogMsg(INFO, "entry _python_ibm_db_createdb()");
    SQLWCHAR *dbName = NULL;
    SQLWCHAR *codeset = NULL;
    SQLWCHAR *mode = NULL;
    SQLINTEGER sqlcode;
    SQLSMALLINT length;
    SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
    int isNewBuffer = 0;
    int rc = SQL_SUCCESS;
#ifdef _WIN32
    HINSTANCE cliLib = NULL;
    FARPROC sqlcreatedb;
#else
    void *cliLib = NULL;
    typedef int (*sqlcreatedbType)(SQLHDBC, SQLWCHAR *, SQLINTEGER, SQLWCHAR *, SQLINTEGER, SQLWCHAR *, SQLINTEGER);
    sqlcreatedbType sqlcreatedb;
#endif

#if defined(__MVS__)
    LogMsg(ERROR, "Not supported: This function is not supported on this platform");
    PyErr_SetString(PyExc_Exception, "Not supported: This function not supported on this platform");
    return -1;
#endif

    if (!NIL_P(conn_res))
    {
        snprintf(messageStr, sizeof(messageStr), "Connection resource is valid, handle_active=%d", conn_res->handle_active);
        LogMsg(INFO, messageStr);
        if (NIL_P(dbNameObj))
        {
            LogMsg(ERROR, "Supplied database name parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied database name Parameter is invalid");
            return -1;
        }
        /* Check to ensure the connection resource given is active */
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return -1;
        }

        dbNameObj = PyUnicode_FromObject(dbNameObj);
        if (dbNameObj != NULL && dbNameObj != Py_None)
        {
            dbName = getUnicodeDataAsSQLWCHAR(dbNameObj, &isNewBuffer);
            snprintf(messageStr, sizeof(messageStr), "dbName obtained, dbName=%ls, isNewBuffer=%d", dbName, isNewBuffer);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "Failed to convert dbNameObj to SQLWCHAR");
            return -1;
        }

        if (!NIL_P(codesetObj))
        {
            codesetObj = PyUnicode_FromObject(codesetObj);
            if (codesetObj != NULL && codesetObj != Py_None)
            {
                codeset = getUnicodeDataAsSQLWCHAR(codesetObj, &isNewBuffer);
                snprintf(messageStr, sizeof(messageStr), "codeset obtained, codeset=%ls, isNewBuffer=%d", codeset, isNewBuffer);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Failed to convert codesetObj to SQLWCHAR");
                _python_clear_local_var(dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer);
                return -1;
            }
        }
        if (!NIL_P(modeObj))
        {
            modeObj = PyUnicode_FromObject(modeObj);
            if (codesetObj != NULL && codesetObj != Py_None)
            {
                mode = getUnicodeDataAsSQLWCHAR(modeObj, &isNewBuffer);
                snprintf(messageStr, sizeof(messageStr), "mode obtained, mode=%ls, isNewBuffer=%d", mode, isNewBuffer);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Failed to convert modeObj to SQLWCHAR");
                _python_clear_local_var(dbNameObj, dbName, codesetObj, codeset, NULL, NULL, isNewBuffer);
                return -1;
            }
        }

#ifndef __MVS__
#ifdef _WIN32
        cliLib = DLOPEN(LIBDB2);
#elif _AIX
        cliLib = DLOPEN(LIBDB2, RTLD_MEMBER | RTLD_LAZY);
#else
        cliLib = DLOPEN(LIBDB2, RTLD_LAZY);
#endif
        if (!cliLib)
        {
            sprintf((char *)msg, "Error in loading %s library file", LIBDB2);
            LogMsg(ERROR, (char *)msg);
            PyErr_SetString(PyExc_Exception, (char *)msg);
            _python_clear_local_var(dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer);
            return -1;
        }
#endif
        Py_BEGIN_ALLOW_THREADS;
#ifdef _WIN32
        sqlcreatedb = DLSYM(cliLib, "SQLCreateDbW");
#else
        sqlcreatedb = (sqlcreatedbType)DLSYM(cliLib, "SQLCreateDbW");
#endif
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Function pointer sqlcreatedb resolved to address=%p", (void *)sqlcreatedb);
        LogMsg(DEBUG, messageStr);
        if (sqlcreatedb == NULL)
        {
#ifdef _WIN32
            sprintf((char *)msg, "Not supported: This function is only supported from v97fp4 version of cli on window");
#else
            sprintf((char *)msg, "Not supported: This function is only supported from v97fp3 version of cli");
#endif
            LogMsg(ERROR, (char *)msg);
            PyErr_SetString(PyExc_Exception, (char *)msg);
            DLCLOSE(cliLib);
            _python_clear_local_var(dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer);
            return -1;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = (*sqlcreatedb)((SQLHDBC)conn_res->hdbc, dbName, SQL_NTS, codeset, SQL_NTS, mode, SQL_NTS);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLCreateDbW call returned rc=%d", rc);
        LogMsg(DEBUG, messageStr);

        DLCLOSE(cliLib);
        if (rc != SQL_SUCCESS)
        {
            if (createNX == 1)
            {
                if (SQLGetDiagRec(SQL_HANDLE_DBC, (SQLHDBC)conn_res->hdbc, 1, sqlstate, &sqlcode, msg, SQL_MAX_MESSAGE_LENGTH + 1, &length) == SQL_SUCCESS)
                {
                    snprintf(messageStr, sizeof(messageStr), "SQLGetDiagRec returned sqlcode=%d, sqlstate=%s, msg=%s", sqlcode, sqlstate, msg);
                    LogMsg(DEBUG, messageStr);
                    if (sqlcode == -1005)
                    {
                        _python_clear_local_var(dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer);
                        return 0;
                    }
                }
            }
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1, NULL, -1, 1);
            _python_clear_local_var(dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer);
            return -1;
        }
        _python_clear_local_var(dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer);
        LogMsg(INFO, "Database creation process completed successfully");
        LogMsg(INFO, "exit _python_ibm_db_createdb()");
        return 0;
    }
    else
    {
        LogMsg(ERROR, "Supplied connection object parameter is invalid");
        LogMsg(INFO, "exit _python_ibm_db_createdb()");
        PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
        return -1;
    }
}

/*
 * static int _python_ibm_db_dropdb(conn_handle *conn_res, PyObject *dbNameObj, int recreate)
 */
static int _python_ibm_db_dropdb(conn_handle *conn_res, PyObject *dbNameObj, int recreate)
{
    LogMsg(INFO, "entry _python_ibm_db_dropdb()");
    SQLWCHAR *dbName = NULL;
    SQLINTEGER sqlcode;
    SQLSMALLINT length;
    SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
    int isNewBuffer = 0;
    int rc = SQL_SUCCESS;
#ifdef _WIN32
    FARPROC sqldropdb;
    HINSTANCE cliLib = NULL;
#else
    typedef int (*sqldropdbType)(SQLHDBC, SQLWCHAR *, SQLINTEGER);
    sqldropdbType sqldropdb;
    void *cliLib;
#endif

#if defined(__MVS__)
    LogMsg(ERROR, "Not supported: This function is not supported on this platform");
    PyErr_SetString(PyExc_Exception, "Not supported: This function not supported on this platform");
    return -1;
#endif

    if (!NIL_P(conn_res))
    {
        snprintf(messageStr, sizeof(messageStr), "Connection resource is valid, handle_active=%d", conn_res->handle_active);
        LogMsg(INFO, messageStr);
        if (NIL_P(dbNameObj))
        {
            LogMsg(ERROR, "Supplied database name parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied database name Parameter is invalid");
            return -1;
        }
        /* Check to ensure the connection resource given is active */
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return -1;
        }

        dbNameObj = PyUnicode_FromObject(dbNameObj);
        if (dbNameObj != NULL && dbNameObj != Py_None)
        {
            dbName = getUnicodeDataAsSQLWCHAR(dbNameObj, &isNewBuffer);
            snprintf(messageStr, sizeof(messageStr), "dbName obtained, dbName=%ls, isNewBuffer=%d", dbName, isNewBuffer);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "Failed to convert dbNameObj to SQLWCHAR");
            return -1;
        }
#ifndef __MVS__
#ifdef _WIN32
        cliLib = DLOPEN(LIBDB2);
#elif _AIX
        cliLib = DLOPEN(LIBDB2, RTLD_MEMBER | RTLD_LAZY);
#else
        cliLib = DLOPEN(LIBDB2, RTLD_LAZY);
#endif
        if (!cliLib)
        {
            sprintf((char *)msg, "Error in loading %s library file", LIBDB2);
            LogMsg(ERROR, (char *)msg);
            PyErr_SetString(PyExc_Exception, (char *)msg);
            _python_clear_local_var(dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer);
            return -1;
        }
#endif
        Py_BEGIN_ALLOW_THREADS;
#ifdef _WIN32
        sqldropdb = DLSYM(cliLib, "SQLDropDbW");
#else
        sqldropdb = (sqldropdbType)DLSYM(cliLib, "SQLDropDbW");
#endif
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Function pointer sqldropdb resolved to address=%p", (void *)sqldropdb);
        LogMsg(DEBUG, messageStr);
        if (sqldropdb == NULL)
        {
#ifdef _WIN32
            sprintf((char *)msg, "Not supported: This function is only supported from v97fp4 version of cli on window");
#else
            sprintf((char *)msg, "Not supported: This function is only supported from v97fp3 version of cli");
#endif
            LogMsg(ERROR, (char *)msg);
            PyErr_SetString(PyExc_Exception, (char *)msg);
            DLCLOSE(cliLib);
            _python_clear_local_var(dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer);
            return -1;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = sqldropdb(conn_res->hdbc, dbName, SQL_NTS);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "sqldropdb call returned rc=%d", rc);
        LogMsg(DEBUG, messageStr);
        DLCLOSE(cliLib);
        if (rc != SQL_SUCCESS)
        {
            if (recreate)
            {
                if (SQLGetDiagRec(SQL_HANDLE_DBC, (SQLHDBC)conn_res->hdbc, 1, sqlstate, &sqlcode, msg, SQL_MAX_MESSAGE_LENGTH + 1, &length) == SQL_SUCCESS)
                {
                    snprintf(messageStr, sizeof(messageStr), "SQLGetDiagRec returned sqlcode=%d, sqlstate=%s, msg=%s", sqlcode, sqlstate, msg);
                    LogMsg(DEBUG, messageStr);
                    if (sqlcode == -1013)
                    {
                        _python_clear_local_var(dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer);
                        return 0;
                    }
                }
            }
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1, NULL, -1, 1);
            LogMsg(INFO, "exit _python_ibm_db_dropdb()");
            return -1;
        }
        _python_clear_local_var(dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer);
        LogMsg(INFO, "Database drop process completed successfully");
        LogMsg(INFO, "exit _python_ibm_db_dropdb()");
        return 0;
    }
    else
    {
        LogMsg(ERROR, "Supplied connection object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
        return -1;
    }
}

static void _python_ibm_db_debug(PyObject *self, PyObject *args)
{
    PyObject *arg = NULL;
    debug_mode = 0; // 0 means false by default
    fileName = NULL;

    if (!PyArg_ParseTuple(args, "O", &arg))
    {
    }

    // Check if the argument is a boolean
    if (PyBool_Check(arg))
    {
        debug_mode = PyObject_IsTrue(arg);
    }
    // Check if the argument is a string
    else if (PyUnicode_Check(arg))
    {
        debug_mode = 1; // Enable debug mode
        fileName = PyUnicode_AsUTF8(arg);
        if (fileName == NULL)
        {
            PyErr_SetString(PyExc_TypeError, "file name must be a valid string");
        }
        // Open the file and do debugging
        FILE *log_file = fopen(fileName, "w");
        if (log_file == NULL)
        {
            PyErr_SetString(PyExc_IOError, "Failed to open the log file");
        }
        else
        {
            fclose(log_file);
        }
    }
    else
    {
        PyErr_SetString(PyExc_TypeError, "argument must be a boolean or a string");
    }
}

/*!# ibm_db.createdb
 *
 * ===Description
 *  True/None ibm_db.createdb ( IBM_DBConnection connection, string dbName [, codeSet, mode] )
 *
 * Creates a database by using the specified database name, code set, and mode
 *
 * ===Parameters
 *
 * ====connection
 *      A valid database server instance connection resource variable as returned from ibm_db.connect() by specifying the ATTACH keyword.
 *
 * ====dbName
 *      Name of the database that is to be created.
 *
 * ====codeSet
 *      Database code set information.
 *      Note: If the value of the codeSet argument not specified, the database is created in the Unicode code page for DB2 data servers and in the UTF-8 code page for IDS data servers.
 *
 * ====mode
 *      Database logging mode.
 *      Note: This value is applicable only to IDS data servers.
 *
 * ===Return Value
 *  Returns True on successful creation of database else return None
 */
PyObject *ibm_db_createdb(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry createdb()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    PyObject *codesetObj = NULL;
    PyObject *modeObj = NULL;
    int rc = -1;

    if (!PyArg_ParseTuple(args, "OO|OO", &py_conn_res, &dbNameObj, &codesetObj, &modeObj))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, dbNameObj=%p, codesetObj=%p, modeObj=%p",
             py_conn_res, dbNameObj, codesetObj, modeObj);
    LogMsg(DEBUG, messageStr);

    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
    {
        LogMsg(ERROR, "Supplied connection object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
        return NULL;
    }
    rc = _python_ibm_db_createdb((conn_handle *)py_conn_res, dbNameObj, codesetObj, modeObj, 0);
    if (rc == 0)
    {
        LogMsg(INFO, "Database created successfully");
        LogMsg(INFO, "exit createdb()");
        Py_RETURN_TRUE;
    }
    else
    {
        LogMsg(ERROR, "Failed to create database");
        LogMsg(INFO, "exit createdb()");
        return NULL;
    }
}

/*!# ibm_db.dropdb
 *
 * ===Description
 *  True/None ibm_db.dropdb ( IBM_DBConnection connection, string dbName )
 *
 * Drops the specified database
 *
 * ===Parameters
 *
 * ====connection
 *      A valid database server instance connection resource variable as returned from ibm_db.connect() by specifying the ATTACH keyword.
 *
 * ====dbName
 *      Name of the database that is to be dropped.
 *
 * ===Return Value
 *  Returns True if specified database dropped sucessfully else None
 */
PyObject *ibm_db_dropdb(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry dropdb()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    int rc = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_conn_res, &dbNameObj))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, dbNameObj=%p", py_conn_res, dbNameObj);
    LogMsg(DEBUG, messageStr);
    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
    {
        LogMsg(ERROR, "Supplied connection object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
        return NULL;
    }
    rc = _python_ibm_db_dropdb((conn_handle *)py_conn_res, dbNameObj, 0);
    if (rc == 0)
    {
        LogMsg(INFO, "Database droped successfully");
        LogMsg(INFO, "exit dropdb()");
        Py_RETURN_TRUE;
    }
    else
    {
        LogMsg(ERROR, "Failed to drop database");
        LogMsg(INFO, "exit dropdb()");
        return NULL;
    }
}

/*ibm_db.recreatedb
 *
 * ===Description
 *  True/None ibm_db.recreatedb ( IBM_DBConnection connection, string dbName [, codeSet, mode] )
 *
 * Drop and then recreates a database by using the specified database name, code set, and mode
 *
 * ===Parameters
 *
 * ====connection
 *      A valid database server instance connection resource variable as returned from ibm_db.connect() by specifying the ATTACH keyword.
 *
 * ====dbName
 *      Name of the database that is to be created.
 *
 * ====codeSet
 *      Database code set information.
 *      Note: If the value of the codeSet argument not specified, the database is created in the Unicode code page for DB2 data servers and in the UTF-8 code page for IDS data servers.
 *
 * ====mode
 *      Database logging mode.
 *      Note: This value is applicable only to IDS data servers.
 *
 * ===Return Value
 *  Returns True if specified database created successfully else return None
 */
PyObject *ibm_db_recreatedb(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry recreatedb()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    PyObject *codesetObj = NULL;
    PyObject *modeObj = NULL;
    int rc = -1;

    if (!PyArg_ParseTuple(args, "OO|OO", &py_conn_res, &dbNameObj, &codesetObj, &modeObj))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, dbNameObj=%p, codesetObj=%p, modeObj=%p", (void *)py_conn_res, (void *)dbNameObj, (void *)codesetObj, (void *)modeObj);
    LogMsg(DEBUG, messageStr);
    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
    {
        LogMsg(ERROR, "Supplied connection object Parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Calling _python_ibm_db_dropdb with parameters: conn_handle=%p, dbNameObj=%p, recreate=1",
             (void *)py_conn_res, (void *)dbNameObj);
    LogMsg(DEBUG, messageStr);
    rc = _python_ibm_db_dropdb((conn_handle *)py_conn_res, dbNameObj, 1);
    snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_dropdb returned: rc=%d", rc);
    LogMsg(DEBUG, messageStr);
    if (rc != 0)
    {
        LogMsg(ERROR, "Failed to drop the database");
        LogMsg(INFO, "exit recreatedb()");
        return NULL;
    }
    else
    {
        LogMsg(DEBUG, "Database dropped successfully");
    }
    snprintf(messageStr, sizeof(messageStr), "Calling _python_ibm_db_createdb with parameters: conn_handle=%p, dbNameObj=%p, codesetObj=%p, modeObj=%p, createNX=0",
             (void *)py_conn_res, (void *)dbNameObj, (void *)codesetObj, (void *)modeObj);
    LogMsg(DEBUG, messageStr);
    rc = _python_ibm_db_createdb((conn_handle *)py_conn_res, dbNameObj, codesetObj, modeObj, 0);
    snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_createdb returned: rc=%d", rc);
    LogMsg(DEBUG, messageStr);
    if (rc == 0)
    {
        LogMsg(DEBUG, "Database created successfully");
        LogMsg(INFO, "exit recreatedb()");
        Py_RETURN_TRUE;
    }
    else
    {
        LogMsg(ERROR, "Failed to create the database");
        LogMsg(INFO, "exit recreatedb()");
        return NULL;
    }
}

/*!# ibm_db.createdbNX
 *
 * ===Description
 *  True/None ibm_db.createdbNX ( IBM_DBConnection connection, string dbName [, codeSet, mode] )
 *
 * Creates the database if not exist by using the specified database name, code set, and mode
 *
 * ===Parameters
 *
 * ====connection
 *      A valid database server instance connection resource variable as returned from ibm_db.connect() by specifying the ATTACH keyword.
 *
 * ====dbName
 *      Name of the database that is to be created.
 *
 * ====codeSet
 *      Database code set information.
 *      Note: If the value of the codeSet argument not specified, the database is created in the Unicode code page for DB2 data servers and in the UTF-8 code page for IDS data servers.
 *
 * ====mode
 *      Database logging mode.
 *      Note: This value is applicable only to IDS data servers.
 *
 * ===Return Value
 *  Returns True if database already exists or created sucessfully else return None
 */
PyObject *ibm_db_createdbNX(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry createdbNX()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    PyObject *codesetObj = NULL;
    PyObject *modeObj = NULL;
    int rc = -1;

    if (!PyArg_ParseTuple(args, "OO|OO", &py_conn_res, &dbNameObj, &codesetObj, &modeObj))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, dbNameObj=%p, codesetObj=%p, modeObj=%p",
             py_conn_res, dbNameObj, codesetObj, modeObj);
    LogMsg(DEBUG, messageStr);

    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
    {
        LogMsg(ERROR, "Supplied connection object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
        return NULL;
    }
    rc = _python_ibm_db_createdb((conn_handle *)py_conn_res, dbNameObj, codesetObj, modeObj, 1);
    if (rc == 0)
    {
        LogMsg(INFO, "Database created successfully");
        LogMsg(INFO, "exit createdbNX()");
        Py_RETURN_TRUE;
    }
    else
    {
        LogMsg(ERROR, "Failed to create database");
        LogMsg(INFO, "exit createdbNX()");
        return NULL;
    }
}

/*!# ibm_db.autocommit
 *
 * ===Description
 *
 * mixed ibm_db.autocommit ( resource connection [, bool value] )
 *
 * Returns or sets the AUTOCOMMIT behavior of the specified connection resource.
 *
 * ===Parameters
 *
 * ====connection
 *    A valid database connection resource variable as returned from connect()
 * or pconnect().
 *
 * ====value
 *    One of the following constants:
 *    SQL_AUTOCOMMIT_OFF
 *          Turns AUTOCOMMIT off.
 *    SQL_AUTOCOMMIT_ON
 *          Turns AUTOCOMMIT on.
 *
 * ===Return Values
 *
 * When ibm_db.autocommit() receives only the connection parameter, it returns
 * the current state of AUTOCOMMIT for the requested connection as an integer
 * value. A value of 0 indicates that AUTOCOMMIT is off, while a value of 1
 * indicates that AUTOCOMMIT is on.
 *
 * When ibm_db.autocommit() receives both the connection parameter and
 * autocommit parameter, it attempts to set the AUTOCOMMIT state of the
 * requested connection to the corresponding state.
 *
 * Returns TRUE on success or FALSE on failure.
 */
static PyObject *ibm_db_autocommit(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry autocommt()");
    LogUTF8Msg(args);
    PyObject *py_autocommit = NULL;
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res = NULL;
    int rc;
    SQLINTEGER autocommit = -1;

    if (!PyArg_ParseTuple(args, "O|O", &py_conn_res, &py_autocommit))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_autocommit=%p", (void *)py_conn_res, (void *)py_autocommit);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (!NIL_P(py_autocommit))
        {
            if (PyInt_Check(py_autocommit))
            {
                autocommit = (SQLINTEGER)PyLong_AsLong(py_autocommit);
                snprintf(messageStr, sizeof(messageStr), "Autocommit value parsed: %d", autocommit);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(EXCEPTION, "Supplied parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        /* If value in handle is different from value passed in */
        if (PyTuple_Size(args) == 2)
        {
            if (autocommit != (conn_res->auto_commit))
            {
                snprintf(messageStr, sizeof(messageStr), "Updating autocommit setting. Current: %d, New: %d", conn_res->auto_commit, autocommit);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
#ifndef PASE
                rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)(autocommit == 0 ? SQL_AUTOCOMMIT_OFF : SQL_AUTOCOMMIT_ON), SQL_IS_INTEGER);
#else
                rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&autocommit, SQL_IS_INTEGER);
#endif
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLSetConnectAttr return code: rc=%d", rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    snprintf(messageStr, sizeof(messageStr), "An error occurred while setting autocommit. rc=%d", rc);
                    LogMsg(ERROR, messageStr);
                    _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                    rc, 1, NULL, -1, 1);
                }
                conn_res->auto_commit = autocommit;
                snprintf(messageStr, sizeof(messageStr), "Autocommit setting updated successfully. New value: %d", conn_res->auto_commit);
                LogMsg(DEBUG, messageStr);
            }
            Py_INCREF(Py_True);
            LogMsg(INFO, "exit autocommit()");
            return Py_True;
        }
        else
        {
            snprintf(messageStr, sizeof(messageStr), "Returning current autocommit value: %d", conn_res->auto_commit);
            LogMsg(INFO, messageStr);
            LogMsg(INFO, "exit autocommit()");
            return PyInt_FromLong(conn_res->auto_commit);
        }
    }
    LogMsg(INFO, "exit autocommit()");
    return NULL;
}

/*    static void _python_ibm_db_add_param_cache( stmt_handle *stmt_res, int param_no, PyObject *var_pyvalue, char *varname, int varname_len, int param_type, int size, SQLSMALLINT data_type, SQLSMALLINT precision, SQLSMALLINT scale, SQLSMALLINT nullable )
 */
static void _python_ibm_db_add_param_cache(stmt_handle *stmt_res, int param_no, PyObject *var_pyvalue, int param_type, int size, SQLSMALLINT data_type, SQLUINTEGER precision, SQLSMALLINT scale, SQLSMALLINT nullable)
{
    LogMsg(INFO, "entry _python_ibm_db_add_param_cache()");
    snprintf(messageStr, sizeof(messageStr),
             "stmt_res=%p, param_no=%d, var_pyvalue=%p, param_type=%d, size=%d, data_type=%d, precision=%u, scale=%d, nullable=%d",
             (void *)stmt_res, param_no, (void *)var_pyvalue, param_type, size, data_type, precision, scale, nullable);
    LogMsg(DEBUG, messageStr);
    snprintf(messageStr, sizeof(messageStr),
             "Initial state: head_cache_list=%p, num_params=%d",
             (void *)stmt_res->head_cache_list, stmt_res->num_params);
    LogMsg(DEBUG, messageStr);
    param_node *tmp_curr = NULL, *prev = stmt_res->head_cache_list, *curr = stmt_res->head_cache_list;

    while ((curr != NULL) && (curr->param_num < param_no))
    {
        prev = curr;
        curr = curr->next;
    }

    if (curr == NULL || curr->param_num != param_no)
    {
        /* Allocate memory and make new node to be added */
        tmp_curr = ALLOC(param_node);
        memset(tmp_curr, 0, sizeof(param_node));

        /* assign values */
        tmp_curr->data_type = data_type;
        tmp_curr->param_size = precision;
        tmp_curr->nullable = nullable;
        tmp_curr->scale = scale;
        tmp_curr->param_num = param_no;
        tmp_curr->file_options = SQL_FILE_READ;
        tmp_curr->param_type = param_type;
        tmp_curr->size = size;

        /* Set this flag in stmt_res if a FILE INPUT is present */
        if (param_type == PARAM_FILE)
        {
            stmt_res->file_param = 1;
        }

        if (var_pyvalue != NULL)
        {
            Py_INCREF(var_pyvalue);
            tmp_curr->var_pyvalue = var_pyvalue;
        }

        /* link pointers for the list */
        if (prev == NULL)
        {
            stmt_res->head_cache_list = tmp_curr;
        }
        else
        {
            prev->next = tmp_curr;
        }
        tmp_curr->next = curr;

        /* Increment num params added */
        stmt_res->num_params++;
        snprintf(messageStr, sizeof(messageStr),
                 "Added new node: param_no=%d, tmp_curr=%p, head_cache_list=%p, num_params=%d",
                 param_no, (void *)tmp_curr, (void *)stmt_res->head_cache_list, stmt_res->num_params);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Both the nodes are for the same param no */
        /* Replace Information */
        snprintf(messageStr, sizeof(messageStr),
                 "Replacing existing node: param_no=%d, curr=%p",
                 param_no, (void *)curr);
        LogMsg(DEBUG, messageStr);
        curr->data_type = data_type;
        curr->param_size = precision;
        curr->nullable = nullable;
        curr->scale = scale;
        curr->param_num = param_no;
        curr->file_options = SQL_FILE_READ;
        curr->param_type = param_type;
        curr->size = size;

        /* Set this flag in stmt_res if a FILE INPUT is present */
        if (param_type == PARAM_FILE)
        {
            stmt_res->file_param = 1;
        }

        if (var_pyvalue != NULL)
        {
            Py_DECREF(curr->var_pyvalue);
            Py_INCREF(var_pyvalue);
            curr->var_pyvalue = var_pyvalue;
        }
    }
    snprintf(messageStr, sizeof(messageStr), "Final state: head_cache_list=%p, num_params=%d",
             (void *)stmt_res->head_cache_list, stmt_res->num_params);
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit _python_ibm_db_add_param_cache()");
}

/*
 * static PyObject *_python_ibm_db_bind_param_helper(int argc, stmt_handle *stmt_res, SQLUSMALLINT param_no, PyObject *var_pyvalue, long param_type,
 *                      long data_type, long precision, long scale, long size)
 */
static PyObject *_python_ibm_db_bind_param_helper(int argc, stmt_handle *stmt_res, SQLUSMALLINT param_no, PyObject *var_pyvalue, long param_type, long data_type, long precision, long scale, long size)
{
    LogMsg(INFO, "entry _python_ibm_db_bind_param_helper()");
    SQLSMALLINT sql_data_type = 0;
    SQLUINTEGER sql_precision = 0;
    SQLSMALLINT sql_scale = 0;
    SQLSMALLINT sql_nullable = SQL_NO_NULLS;
    char error[DB2_MAX_ERR_MSG_LEN + 50];
    int rc = 0;
    snprintf(messageStr, sizeof(messageStr),
             "argc=%d, stmt_res=%p, param_no=%d, var_pyvalue=%p, param_type=%ld, data_type=%ld, precision=%ld, scale=%ld, size=%ld",
             argc, (void *)stmt_res, param_no, (void *)var_pyvalue, param_type, data_type, precision, scale, size);
    LogMsg(DEBUG, messageStr);

    snprintf(messageStr, sizeof(messageStr),
             "Before SQLDescribeParam: sql_data_type=%d, sql_precision=%u, sql_scale=%d, sql_nullable=%d",
             sql_data_type, sql_precision, sql_scale, sql_nullable);
    LogMsg(DEBUG, messageStr);
    /* Check for Param options */
    switch (argc)
    {
    /* if argc == 3, then the default value for param_type will be used */
    case 3:
        param_type = SQL_PARAM_INPUT;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)param_no, &sql_data_type, &sql_precision, &sql_scale, &sql_nullable);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLDescribeParam called with parameters hstmt=%p, param_no=%d, and returned: rc=%d, sql_data_type=%d, sql_precision=%u, sql_scale=%d, sql_nullable=%d",
                 (void *)stmt_res->hstmt, param_no, rc, sql_data_type, sql_precision, sql_scale, sql_nullable);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1,
                                            NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "Describe Param Failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            LogMsg(ERROR, error);
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        /* Add to cache */
        _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                       param_type, size,
                                       sql_data_type, sql_precision,
                                       sql_scale, sql_nullable);
        break;

    case 4:
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt,
                              (SQLUSMALLINT)param_no, &sql_data_type,
                              &sql_precision, &sql_scale, &sql_nullable);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLDescribeParam called with parameters hstmt=%p, param_no=%d and returned: rc=%d, sql_data_type=%d, sql_precision=%u, sql_scale=%d, sql_nullable=%d",
                 (void *)stmt_res->hstmt, param_no, rc, sql_data_type, sql_precision, sql_scale, sql_nullable);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1,
                                            NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "Describe Param Failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            LogMsg(ERROR, error);
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        /* Add to cache */
        _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                       param_type, size,
                                       sql_data_type, sql_precision,
                                       sql_scale, sql_nullable);
        break;

    case 5:
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt,
                              (SQLUSMALLINT)param_no, &sql_data_type,
                              &sql_precision, &sql_scale, &sql_nullable);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLDescribeParam called with parameters hstmt=%p, param_no=%d and returned: rc=%d, sql_data_type=%d, sql_precision=%u, sql_scale=%d, sql_nullable=%d",
                 (void *)stmt_res->hstmt, param_no, rc, sql_data_type, sql_precision, sql_scale, sql_nullable);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1,
                                            NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "Describe Param Failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            LogMsg(ERROR, error);
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        sql_data_type = (SQLSMALLINT)data_type;
        /* Add to cache */
        _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                       param_type, size,
                                       sql_data_type, sql_precision,
                                       sql_scale, sql_nullable);
        break;

    case 6:
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt,
                              (SQLUSMALLINT)param_no, &sql_data_type,
                              &sql_precision, &sql_scale, &sql_nullable);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLDescribeParam called with: hstmt=%p, param_no=%d; returned: rc=%d, sql_data_type=%d, sql_precision=%u, sql_scale=%d, sql_nullable=%d",
                 (void *)stmt_res->hstmt, param_no, rc, sql_data_type, sql_precision, sql_scale, sql_nullable);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1,
                                            NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "Describe Param Failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            LogMsg(ERROR, error);
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        sql_data_type = (SQLSMALLINT)data_type;
        sql_precision = (SQLUINTEGER)precision;
        /* Add to cache */
        _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                       param_type, size,
                                       sql_data_type, sql_precision,
                                       sql_scale, sql_nullable);
        break;

    case 7:
    case 8:
        /* Cache param data passed
         * I am using a linked list of nodes here because I don't know
         * before hand how many params are being passed in/bound.
         * To determine this, a call to SQLNumParams is necessary.
         * This is take away any advantages an array would have over
         * linked list access
         * Data is being copied over to the correct types for subsequent
         * CLI call because this might cause problems on other platforms
         * such as AIX
         */
        snprintf(messageStr, sizeof(messageStr),
                 "Before adding to cache: param_no=%d, var_pyvalue=%p, param_type=%ld, size=%ld, sql_data_type=%d, sql_precision=%u, sql_scale=%d, sql_nullable=%d",
                 param_no, (void *)var_pyvalue, param_type, size, sql_data_type, sql_precision, sql_scale, sql_nullable);
        LogMsg(DEBUG, messageStr);
        sql_data_type = (SQLSMALLINT)data_type;
        sql_precision = (SQLUINTEGER)precision;
        sql_scale = (SQLSMALLINT)scale;
        _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                       param_type, size,
                                       sql_data_type, sql_precision,
                                       sql_scale, sql_nullable);
        snprintf(messageStr, sizeof(messageStr),
                 "Added to cache: param_no=%d, var_pyvalue=%p, param_type=%ld, size=%ld, sql_data_type=%d, sql_precision=%u, sql_scale=%d, sql_nullable=%d",
                 param_no, (void *)var_pyvalue, param_type, size, sql_data_type, sql_precision, sql_scale, sql_nullable);
        LogMsg(DEBUG, messageStr);
        break;

    default:
        /* WRONG_PARAM_COUNT; */
        LogMsg(ERROR, "Invalid argc value");
        LogMsg(INFO, "exit _python_ibm_db_bind_param_helper()");
        return NULL;
    }
    /* end Switch */

    /* We bind data with DB2 CLI in ibm_db.execute() */
    /* This will save network flow if we need to override params in it */
    LogMsg(DEBUG, "return value=Py_True");
    LogMsg(INFO, "exit _python_ibm_db_bind_param_helper()");
    Py_INCREF(Py_True);
    return Py_True;
}

/*!# ibm_db.bind_param
 *
 * ===Description
 * Py_True/Py_None ibm_db.bind_param (resource stmt, int parameter-number,
 *                                    string variable [, int parameter-type
 *                                    [, int data-type [, int precision
 *                                    [, int scale [, int size[]]]]]] )
 *
 * Binds a Python variable to an SQL statement parameter in a IBM_DBStatement
 * resource returned by ibm_db.prepare().
 * This function gives you more control over the parameter type, data type,
 * precision, and scale for the parameter than simply passing the variable as
 * part of the optional input array to ibm_db.execute().
 *
 * ===Parameters
 *
 * ====stmt
 *
 *    A prepared statement returned from ibm_db.prepare().
 *
 * ====parameter-number
 *
 *    Specifies the 1-indexed position of the parameter in the prepared
 * statement.
 *
 * ====variable
 *
 *    A Python variable to bind to the parameter specified by parameter-number.
 *
 * ====parameter-type
 *
 *    A constant specifying whether the Python variable should be bound to the
 * SQL parameter as an input parameter (SQL_PARAM_INPUT), an output parameter
 * (SQL_PARAM_OUTPUT), or as a parameter that accepts input and returns output
 * (SQL_PARAM_INPUT_OUTPUT). To avoid memory overhead, you can also specify
 * PARAM_FILE to bind the Python variable to the name of a file that contains
 * large object (BLOB, CLOB, or DBCLOB) data.
 *
 * ====data-type
 *
 *    A constant specifying the SQL data type that the Python variable should be
 * bound as: one of SQL_BINARY, DB2_CHAR, DB2_DOUBLE, or DB2_LONG .
 *
 * ====precision
 *
 *    Specifies the precision that the variable should be bound to the database. *
 * ====scale
 *
 *      Specifies the scale that the variable should be bound to the database.
 *
 * ====size
 *
 *      Specifies the size that should be retreived from an INOUT/OUT parameter.
 *
 * ===Return Values
 *
 *    Returns Py_True on success or NULL on failure.
 */
static PyObject *ibm_db_bind_param(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry bind_param()");
    LogUTF8Msg(args);
    PyObject *var_pyvalue = NULL;
    PyObject *py_param_type = NULL;
    PyObject *py_data_type = NULL;
    PyObject *py_precision = NULL;
    PyObject *py_scale = NULL;
    PyObject *py_size = NULL;
    PyObject *py_param_no = NULL;
    PyObject *py_stmt_res = NULL;

    long param_type = SQL_PARAM_INPUT;
    /* LONG types used for data being passed in */
    SQLUSMALLINT param_no = 0;
    long data_type = 0;
    long precision = 0;
    long scale = 0;
    long size = 0;
    stmt_handle *stmt_res;

    if (!PyArg_ParseTuple(args, "OOO|OOOOO", &py_stmt_res, &py_param_no,
                          &var_pyvalue, &py_param_type,
                          &py_data_type, &py_precision,
                          &py_scale, &py_size))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, py_param_no=%p, var_pyvalue=%p, py_param_type=%p, py_data_type=%p, py_precision=%p, py_scale=%p, py_size=%p",
             py_stmt_res, py_param_no, var_pyvalue, py_param_type, py_data_type, py_precision, py_scale, py_size);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_param_no))
    {
        if (PyInt_Check(py_param_no))
        {
            param_no = (SQLUSMALLINT)PyLong_AsLong(py_param_no);
            LogMsg(DEBUG, "Parameter number set");
        }
        else
        {
            LogMsg(ERROR, "Supplied parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            return NULL;
        }
    }
    if (py_param_type != NULL && py_param_type != Py_None &&
        TYPE(py_param_type) == PYTHON_FIXNUM)
    {
        param_type = PyInt_AS_LONG(py_param_type);
        LogMsg(DEBUG, "Parameter type set");
    }

    if (py_data_type != NULL && py_data_type != Py_None &&
        TYPE(py_data_type) == PYTHON_FIXNUM)
    {
        data_type = PyInt_AS_LONG(py_data_type);
        LogMsg(DEBUG, "Data type set");
    }

    if (py_precision != NULL && py_precision != Py_None &&
        TYPE(py_precision) == PYTHON_FIXNUM)
    {
        precision = PyInt_AS_LONG(py_precision);
        LogMsg(DEBUG, "Precision set");
    }

    if (py_scale != NULL && py_scale != Py_None &&
        TYPE(py_scale) == PYTHON_FIXNUM)
    {
        scale = PyInt_AS_LONG(py_scale);
        LogMsg(DEBUG, "Scale set");
    }

    if (py_size != NULL && py_size != Py_None &&
        TYPE(py_size) == PYTHON_FIXNUM)
    {
        size = PyInt_AS_LONG(py_size);
        LogMsg(DEBUG, "Size set");
    }

    snprintf(messageStr, sizeof(messageStr), "Final values: param_no=%d, param_type=%ld, data_type=%ld, precision=%ld, scale=%ld, size=%ld",
             param_no, param_type, data_type, precision, scale, size);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        LogMsg(INFO, "Calling and returning _python_ibm_db_bind_param_helper");
        LogMsg(INFO, "exit bind_param()");
        return _python_ibm_db_bind_param_helper(PyTuple_Size(args), stmt_res, param_no, var_pyvalue, param_type, data_type, precision, scale, size);
    }
    else
    {
        LogMsg(ERROR, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
}

/*!# ibm_db.close
 *
 * ===Description
 *
 * bool ibm_db.close ( resource connection )
 *
 * This function closes a DB2 client connection created with ibm_db.connect()
 * and returns the corresponding resources to the database server.
 *
 * If you attempt to close a persistent DB2 client connection created with
 * ibm_db.pconnect(), the close request returns TRUE and the persistent IBM Data
 * Server client connection remains available for the next caller.
 *
 * ===Parameters
 *
 * ====connection
 *    Specifies an active DB2 client connection.
 *
 * ===Return Values
 * Returns TRUE on success or FALSE on failure.
 */
static PyObject *ibm_db_close(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry close()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res = NULL;
    int rc;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p", py_conn_res);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        /* Check to see if it's a persistent connection;
         * if so, just return true
         */

        if (!conn_res->handle_active)
        {
            LogMsg(EXCEPTION, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        if (conn_res->handle_active && !conn_res->flag_pconnect)
        {
            /* Disconnect from DB. If stmt is allocated,
             * it is freed automatically
             */
            if (conn_res->auto_commit == 0)
            {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLEndTran(SQL_HANDLE_DBC, (SQLHDBC)conn_res->hdbc,
                                SQL_ROLLBACK);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQL rollback returned: rc=%d", rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                    rc, 1, NULL, -1, 1);
                    return NULL;
                }
            }
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLDisconnect((SQLHDBC)conn_res->hdbc);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQL disconnect returned: rc=%d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            if (rc == SQL_ERROR)
            {
                return NULL;
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeHandle(SQL_HANDLE_DBC, conn_res->hdbc);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQL free handle (DBC) returned: rc=%d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }

            if (rc == SQL_ERROR)
            {

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQL free handle (ENV) returned: rc=%d", rc);
                LogMsg(DEBUG, messageStr);
                return NULL;
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQL free handle (ENV) returned: rc=%d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR)
            {
                _python_ibm_db_check_sql_errors(conn_res->henv,
                                                SQL_HANDLE_ENV, rc, 1,
                                                NULL, -1, 1);
            }

            if (rc == SQL_ERROR)
            {
                return NULL;
            }

            conn_res->handle_active = 0;
            Py_INCREF(Py_True);
            LogMsg(INFO, "exit close()");
            return Py_True;
        }
        else if (conn_res->flag_pconnect)
        {
            /* Do we need to call FreeStmt or something to close cursors? */
            LogMsg(INFO, "Persistent connection detected; no action required");
            Py_INCREF(Py_True);
            LogMsg(INFO, "exit close()");
            return Py_True;
        }
        else
        {
            LogMsg(INFO, "exit close()");
            return NULL;
        }
    }
    else
    {
        LogMsg(INFO, "No connection object provided");
        LogMsg(INFO, "exit close()");
        return NULL;
    }
}

/*!# ibm_db.column_privileges
 *
 * ===Description
 * resource ibm_db.column_privileges ( resource connection [, string qualifier
 * [, string schema [, string table-name [, string column-name]]]] )
 *
 * Returns a result set listing the columns and associated privileges for a
 * table.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the tables. To match all schemas, pass NULL
 * or an empty string.
 *
 * ====table-name
 *        The name of the table or view. To match all tables in the database,
 * pass NULL or an empty string.
 *
 * ====column-name
 *        The name of the column. To match all columns in the table, pass NULL
 * or an empty string.
 *
 * ===Return Values
 * Returns a statement resource with a result set containing rows describing
 * the column privileges for columns matching the specified parameters. The rows
 * are composed of the following columns:
 *
 * TABLE_CAT:: Name of the catalog. The value is NULL if this table does not
 * have catalogs.
 * TABLE_SCHEM:: Name of the schema.
 * TABLE_NAME:: Name of the table or view.
 * COLUMN_NAME:: Name of the column.
 * GRANTOR:: Authorization ID of the user who granted the privilege.
 * GRANTEE:: Authorization ID of the user to whom the privilege was granted.
 * PRIVILEGE:: The privilege for the column.
 * IS_GRANTABLE:: Whether the GRANTEE is permitted to grant this privilege to
 * other users.
 */
static PyObject *ibm_db_column_privileges(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry column_privileges()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    SQLWCHAR *column_name = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    PyObject *py_column_name = NULL;
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int rc = SQL_SUCCESS;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner,
                          &py_table_name, &py_column_name))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_table_name=%p, py_column_name=%p",
             py_conn_res, py_qualifier, py_owner, py_table_name, py_column_name);
    LogMsg(DEBUG, messageStr);

    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted py_qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "Qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted py_owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "Owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None)
    {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name))
        {
            py_table_name = PyUnicode_FromObject(py_table_name);
            snprintf(messageStr, sizeof(messageStr), "Converted py_table_name to Unicode: %s", PyUnicode_AsUTF8(py_table_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "Table name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_column_name != NULL && py_column_name != Py_None)
    {
        if (PyString_Check(py_column_name) || PyUnicode_Check(py_column_name))
        {
            py_column_name = PyUnicode_FromObject(py_column_name);
            snprintf(messageStr, sizeof(messageStr), "Converted py_column_name to Unicode: %s", PyUnicode_AsUTF8(py_column_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "Column name must be a string");
            PyErr_SetString(PyExc_Exception, "column_name must be a string");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valie. conn_res=%p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle(SQL_HANDLE_STMT) rc=%d, stmt_res->hstmt=%p", rc, (void *)stmt_res->hstmt);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            LogMsg(ERROR, "SQLAllocHandle failed");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None)
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);
        if (py_column_name && py_column_name != Py_None)
            column_name = getUnicodeDataAsSQLWCHAR(py_column_name, &isNewBuffer);

        snprintf(messageStr, sizeof(messageStr), "Calling SQLColumnPrivilegesW: qualifier=%s, owner=%s, table_name=%s, column_name=%s",
                 qualifier ? (char *)qualifier : "NULL",
                 owner ? (char *)owner : "NULL",
                 table_name ? (char *)table_name : "NULL",
                 column_name ? (char *)column_name : "NULL");
        LogMsg(DEBUG, messageStr);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLColumnPrivilegesW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
                                  owner, SQL_NTS, table_name, SQL_NTS, column_name,
                                  SQL_NTS);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLColumnPrivilegesW rc=%d", rc);
        LogMsg(DEBUG, messageStr);

        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (table_name)
                PyMem_Del(table_name);
            if (column_name)
                PyMem_Del(column_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            LogMsg(ERROR, "SQLColumnPrivilegesW failed");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_column_name);
        LogMsg(INFO, "exit column_privileges()");
        return (PyObject *)stmt_res;
    }
    else
    {
        LogMsg(ERROR, "Connection resource is NULL or invalid");
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_column_name);
        LogMsg(INFO, "exit column_privileges()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.columns
 * ===Description
 * resource ibm_db.columns ( resource connection [, string qualifier
 * [, string schema [, string table-name [, string column-name]]]] )
 *
 * Returns a result set listing the columns and associated metadata for a table.
 *
 * ===Parameters
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the tables. To match all schemas, pass '%'.
 *
 * ====table-name
 *        The name of the table or view. To match all tables in the database,
 * pass NULL or an empty string.
 *
 * ====column-name
 *        The name of the column. To match all columns in the table, pass NULL or
 * an empty string.
 *
 * ===Return Values
 * Returns a statement resource with a result set containing rows describing the
 * columns matching the specified parameters.
 * The rows are composed of the following columns:
 *
 * TABLE_CAT:: Name of the catalog. The value is NULL if this table does not
 * have catalogs.
 * TABLE_SCHEM:: Name of the schema.
 * TABLE_NAME:: Name of the table or view.
 * COLUMN_NAME:: Name of the column.
 * DATA_TYPE:: The SQL data type for the column represented as an integer value.
 * TYPE_NAME:: A string representing the data type for the column.
 * COLUMN_SIZE:: An integer value representing the size of the column.
 * BUFFER_LENGTH:: Maximum number of bytes necessary to store data from this
 * column.
 * DECIMAL_DIGITS:: The scale of the column, or NULL where scale is not
 * applicable.
 * NUM_PREC_RADIX:: An integer value of either 10 (representing an exact numeric
 * data type), 2 (representing an approximate numeric data type), or NULL
 * (representing a data type for which radix is not applicable).
 * NULLABLE:: An integer value representing whether the column is nullable or
 * not.
 * REMARKS:: Description of the column.
 * COLUMN_DEF:: Default value for the column.
 * SQL_DATA_TYPE:: An integer value representing the size of the column.
 * SQL_DATETIME_SUB:: Returns an integer value representing a datetime subtype
 * code, or NULL for SQL data types to which this does not apply.
 * CHAR_OCTET_LENGTH::    Maximum length in octets for a character data type
 * column, which matches COLUMN_SIZE for single-byte character set data, or
 * NULL for non-character data types.
 * ORDINAL_POSITION:: The 1-indexed position of the column in the table.
 * IS_NULLABLE:: A string value where 'YES' means that the column is nullable
 * and 'NO' means that the column is not nullable.
 */
static PyObject *ibm_db_columns(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry columns()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    SQLWCHAR *column_name = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    PyObject *py_column_name = NULL;
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int rc = SQL_SUCCESS;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner,
                          &py_table_name, &py_column_name))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr),
             "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_table_name=%p, py_column_name=%p",
             py_conn_res, py_qualifier, py_owner, py_table_name, py_column_name);
    LogMsg(DEBUG, messageStr);

    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            LogMsg(DEBUG, "py_qualifier converted to Unicode");
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            LogMsg(DEBUG, "py_owner converted to Unicode");
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None)
    {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name))
        {
            py_table_name = PyUnicode_FromObject(py_table_name);
            LogMsg(DEBUG, "py_table_name converted to Unicode");
        }
        else
        {
            LogMsg(EXCEPTION, "table_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_column_name != NULL && py_column_name != Py_None)
    {
        if (PyString_Check(py_column_name) || PyUnicode_Check(py_column_name))
        {
            py_column_name = PyUnicode_FromObject(py_column_name);
            LogMsg(DEBUG, "py_column_name converted to Unicode");
        }
        else
        {
            LogMsg(EXCEPTION, "column_name must be a string");
            PyErr_SetString(PyExc_Exception, "column_name must be a string");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle: %p, handle_active=%d", conn_res, conn_res->handle_active);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "Statement handle created: %p", stmt_res);
        LogMsg(DEBUG, messageStr);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle returned rc=%d, hstmt=%p", rc, stmt_res->hstmt);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            LogMsg(ERROR, "SQLAllocHandle failed");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }

        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None)
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);
        if (py_column_name && py_column_name != Py_None)
            column_name = getUnicodeDataAsSQLWCHAR(py_column_name, &isNewBuffer);

        snprintf(messageStr, sizeof(messageStr),
                 "SQL buffers: qualifier=%p, owner=%p, table_name=%p, column_name=%p, isNewBuffer=%d",
                 qualifier, owner, table_name, column_name, isNewBuffer);
        LogMsg(DEBUG, messageStr);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLColumnsW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
                         owner, SQL_NTS, table_name, SQL_NTS, column_name, SQL_NTS);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLColumnsW returned rc=%d", rc);
        LogMsg(DEBUG, messageStr);

        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (table_name)
                PyMem_Del(table_name);
            if (column_name)
                PyMem_Del(column_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            LogMsg(ERROR, "SQLColumnsW failed");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_column_name);

        Py_INCREF(Py_None);
        LogMsg(INFO, "exit columns()");
        return (PyObject *)stmt_res;
    }
    else
    {
        LogMsg(INFO, "No connection provided or connection is NIL");
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_column_name);
        LogMsg(INFO, "exit columns()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.foreign_keys
 *
 * ===Description
 * resource ibm_db.foreign_keys ( resource connection, string pk_qualifier,
 * string pk_schema, string pk_table-name, string fk_qualifier
 * string fk_schema, string fk_table-name )
 *
 * Returns a result set listing the foreign keys for a table.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====pk_qualifier
 *        A qualifier for the pk_table-name argument for the DB2 databases
 * running on OS/390 or z/OS servers. For other databases, pass NULL or an empty
 * string.
 *
 * ====pk_schema
 *        The schema for the pk_table-name argument which contains the tables. If
 * schema is NULL, ibm_db.foreign_keys() matches the schema for the current
 * connection.
 *
 * ====pk_table-name
 *        The name of the table which contains the primary key.
 *
 * ====fk_qualifier
 *        A qualifier for the fk_table-name argument for the DB2 databases
 * running on OS/390 or z/OS servers. For other databases, pass NULL or an empty
 * string.
 *
 * ====fk_schema
 *        The schema for the fk_table-name argument which contains the tables. If
 * schema is NULL, ibm_db.foreign_keys() matches the schema for the current
 * connection.
 *
 * ====fk_table-name
 *        The name of the table which contains the foreign key.
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows describing the
 * foreign keys for the specified table. The result set is composed of the
 * following columns:
 *
 * Column name::    Description
 * PKTABLE_CAT:: Name of the catalog for the table containing the primary key.
 * The value is NULL if this table does not have catalogs.
 * PKTABLE_SCHEM:: Name of the schema for the table containing the primary key.
 * PKTABLE_NAME:: Name of the table containing the primary key.
 * PKCOLUMN_NAME:: Name of the column containing the primary key.
 * FKTABLE_CAT:: Name of the catalog for the table containing the foreign key.
 * The value is NULL if this table does not have catalogs.
 * FKTABLE_SCHEM:: Name of the schema for the table containing the foreign key.
 * FKTABLE_NAME:: Name of the table containing the foreign key.
 * FKCOLUMN_NAME:: Name of the column containing the foreign key.
 * KEY_SEQ:: 1-indexed position of the column in the key.
 * UPDATE_RULE:: Integer value representing the action applied to the foreign
 * key when the SQL operation is UPDATE.
 * DELETE_RULE:: Integer value representing the action applied to the foreign
 * key when the SQL operation is DELETE.
 * FK_NAME:: The name of the foreign key.
 * PK_NAME:: The name of the primary key.
 * DEFERRABILITY:: An integer value representing whether the foreign key
 * deferrability is SQL_INITIALLY_DEFERRED, SQL_INITIALLY_IMMEDIATE, or
 * SQL_NOT_DEFERRABLE.
 */
static PyObject *ibm_db_foreign_keys(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry foreign_keys()");
    LogUTF8Msg(args);
    SQLWCHAR *pk_qualifier = NULL;
    SQLWCHAR *pk_owner = NULL;
    SQLWCHAR *pk_table_name = NULL;
    SQLWCHAR *fk_qualifier = NULL;
    SQLWCHAR *fk_owner = NULL;
    SQLWCHAR *fk_table_name = NULL;
    int rc = SQL_SUCCESS;
    conn_handle *conn_res = NULL;
    stmt_handle *stmt_res;
    PyObject *py_conn_res = NULL;
    PyObject *py_pk_qualifier = NULL;
    PyObject *py_pk_owner = NULL;
    PyObject *py_pk_table_name = NULL;
    PyObject *py_fk_qualifier = NULL;
    PyObject *py_fk_owner = NULL;
    PyObject *py_fk_table_name = NULL;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "OOOO|OOO", &py_conn_res, &py_pk_qualifier,
                          &py_pk_owner, &py_pk_table_name, &py_fk_qualifier,
                          &py_fk_owner, &py_fk_table_name))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, pk_qualifier=%p, pk_owner=%p, pk_table_name=%p, fk_qualifier=%p, fk_owner=%p, fk_table_name=%p",
             (void *)py_conn_res, (void *)py_pk_qualifier, (void *)py_pk_owner, (void *)py_pk_table_name, (void *)py_fk_qualifier, (void *)py_fk_owner, (void *)py_fk_table_name);
    LogMsg(DEBUG, messageStr);
    if (py_pk_qualifier != NULL && py_pk_qualifier != Py_None)
    {
        if (PyString_Check(py_pk_qualifier) || PyUnicode_Check(py_pk_qualifier))
        {
            py_pk_qualifier = PyUnicode_FromObject(py_pk_qualifier);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier for table containing primary key must be a string or unicode");
            PyErr_SetString(PyExc_Exception,
                            "qualifier for table containing primary key must be a string or unicode");
            return NULL;
        }
    }

    if (py_pk_owner != NULL && py_pk_owner != Py_None)
    {
        if (PyString_Check(py_pk_owner) || PyUnicode_Check(py_pk_owner))
        {
            py_pk_owner = PyUnicode_FromObject(py_pk_owner);
        }
        else
        {
            LogMsg(EXCEPTION, "owner of table containing primary key must be a string or unicode");
            PyErr_SetString(PyExc_Exception,
                            "owner of table containing primary key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            return NULL;
        }
    }

    if (py_pk_table_name != NULL && py_pk_table_name != Py_None)
    {
        if (PyString_Check(py_pk_table_name) || PyUnicode_Check(py_pk_table_name))
        {
            py_pk_table_name = PyUnicode_FromObject(py_pk_table_name);
        }
        else
        {
            LogMsg(EXCEPTION, "name of the table that contains primary key must be a string or unicode");
            PyErr_SetString(PyExc_Exception,
                            "name of the table that contains primary key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            return NULL;
        }
    }

    if (py_fk_qualifier != NULL && py_fk_qualifier != Py_None)
    {
        if (PyString_Check(py_fk_qualifier) || PyUnicode_Check(py_fk_qualifier))
        {
            py_fk_qualifier = PyUnicode_FromObject(py_fk_qualifier);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier for table containing the foreign key must be a string or unicode");
            PyErr_SetString(PyExc_Exception,
                            "qualifier for table containing the foreign key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            return NULL;
        }
    }

    if (py_fk_owner != NULL && py_fk_owner != Py_None)
    {
        if (PyString_Check(py_fk_owner) || PyUnicode_Check(py_fk_owner))
        {
            py_fk_owner = PyUnicode_FromObject(py_fk_owner);
        }
        else
        {
            LogMsg(EXCEPTION, "owner of table containing the foreign key must be a string or unicode");
            PyErr_SetString(PyExc_Exception,
                            "owner of table containing the foreign key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            Py_XDECREF(py_fk_qualifier);
            return NULL;
        }
    }

    if (py_fk_table_name != NULL && py_fk_table_name != Py_None)
    {
        if (PyString_Check(py_fk_table_name) || PyUnicode_Check(py_fk_table_name))
        {
            py_fk_table_name = PyUnicode_FromObject(py_fk_table_name);
        }
        else
        {
            LogMsg(EXCEPTION, "name of the table that contains foreign key must be a string or unicode");
            PyErr_SetString(PyExc_Exception,
                            "name of the table that contains foreign key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            Py_XDECREF(py_fk_qualifier);
            Py_XDECREF(py_fk_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. stmt_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            Py_XDECREF(py_fk_qualifier);
            Py_XDECREF(py_fk_owner);
            Py_XDECREF(py_fk_table_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle called with SQL_HANDLE_STMT, conn_res->hdbc=%p, &(stmt_res->hstmt)=%p and returned rc=%d",
                 (void *)conn_res->hdbc, (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            Py_XDECREF(py_fk_qualifier);
            Py_XDECREF(py_fk_owner);
            Py_XDECREF(py_fk_table_name);

            Py_RETURN_FALSE;
        }

        if (py_pk_qualifier && py_pk_qualifier != Py_None)
            pk_qualifier = getUnicodeDataAsSQLWCHAR(py_pk_qualifier, &isNewBuffer);
        if (py_pk_owner && py_pk_owner != Py_None)
            pk_owner = getUnicodeDataAsSQLWCHAR(py_pk_owner, &isNewBuffer);
        if (py_pk_table_name && py_pk_table_name != Py_None)
            pk_table_name = getUnicodeDataAsSQLWCHAR(py_pk_table_name, &isNewBuffer);
        if (py_fk_qualifier && py_fk_qualifier != Py_None)
            fk_qualifier = getUnicodeDataAsSQLWCHAR(py_fk_qualifier, &isNewBuffer);
        if (py_fk_owner && py_fk_owner != Py_None)
            fk_owner = getUnicodeDataAsSQLWCHAR(py_fk_owner, &isNewBuffer);
        if (py_fk_table_name && py_fk_table_name != Py_None)
            fk_table_name = getUnicodeDataAsSQLWCHAR(py_fk_table_name, &isNewBuffer);
        snprintf(messageStr, sizeof(messageStr), "Calling SQLForeignKeysW with parameters: pk_qualifier=%p, pk_owner=%p, pk_table_name=%p, fk_qualifier=%p, fk_owner=%p, fk_table_name=%p",
                 (void *)pk_qualifier, (void *)pk_owner, (void *)pk_table_name,
                 (void *)fk_qualifier, (void *)fk_owner, (void *)fk_table_name);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLForeignKeysW((SQLHSTMT)stmt_res->hstmt, pk_qualifier, SQL_NTS,
                             pk_owner, SQL_NTS, pk_table_name, SQL_NTS, fk_qualifier, SQL_NTS,
                             fk_owner, SQL_NTS, fk_table_name, SQL_NTS);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLForeignKeysW returned rc=%d", rc);
        LogMsg(DEBUG, messageStr);
        if (isNewBuffer)
        {
            if (pk_qualifier)
                PyMem_Del(pk_qualifier);
            if (pk_owner)
                PyMem_Del(pk_owner);
            if (pk_table_name)
                PyMem_Del(pk_table_name);
            if (fk_qualifier)
                PyMem_Del(fk_qualifier);
            if (fk_owner)
                PyMem_Del(fk_owner);
            if (fk_table_name)
                PyMem_Del(fk_table_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            Py_XDECREF(py_fk_qualifier);
            Py_XDECREF(py_fk_owner);
            Py_XDECREF(py_fk_table_name);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_pk_qualifier);
        Py_XDECREF(py_pk_owner);
        Py_XDECREF(py_pk_table_name);
        Py_XDECREF(py_fk_qualifier);
        Py_XDECREF(py_fk_owner);
        Py_XDECREF(py_fk_table_name);
        LogMsg(INFO, "exit foreign_keys()");
        return (PyObject *)stmt_res;
    }
    else
    {
        Py_XDECREF(py_pk_qualifier);
        Py_XDECREF(py_pk_owner);
        Py_XDECREF(py_pk_table_name);
        Py_XDECREF(py_fk_qualifier);
        Py_XDECREF(py_fk_owner);
        Py_XDECREF(py_fk_table_name);
        LogMsg(INFO, "exit foreign_keys()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.primary_keys
 *
 * ===Description
 * resource ibm_db.primary_keys ( resource connection, string qualifier,
 * string schema, string table-name )
 *
 * Returns a result set listing the primary keys for a table.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the tables. If schema is NULL,
 * ibm_db.primary_keys() matches the schema for the current connection.
 *
 * ====table-name
 *        The name of the table.
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows describing the
 * primary keys for the specified table.
 * The result set is composed of the following columns:
 *
 * Column name:: Description
 * TABLE_CAT:: Name of the catalog for the table containing the primary key.
 * The value is NULL if this table does not have catalogs.
 * TABLE_SCHEM:: Name of the schema for the table containing the primary key.
 * TABLE_NAME:: Name of the table containing the primary key.
 * COLUMN_NAME:: Name of the column containing the primary key.
 * KEY_SEQ:: 1-indexed position of the column in the key.
 * PK_NAME:: The name of the primary key.
 */
static PyObject *ibm_db_primary_keys(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry primary_keys()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    int rc = SQL_SUCCESS;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "OOOO", &py_conn_res, &py_qualifier, &py_owner,
                          &py_table_name))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_table_name=%p", py_conn_res, py_qualifier, py_owner, py_table_name);
    LogMsg(INFO, messageStr);
    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be as string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None)
    {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name))
        {
            py_table_name = PyUnicode_FromObject(py_table_name);
            snprintf(messageStr, sizeof(messageStr), "Converted table_name to Unicode: %s", PyUnicode_AsUTF8(py_table_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "table_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure created. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLAllocHandle called with parameters SQL_HANDLE_STMT=%d, conn_res->hdbc=%p, stmt_res->hstmt=%p and returned rc=%d",
                 SQL_HANDLE_STMT, (void *)conn_res->hdbc, (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None)
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLPrimaryKeysW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
                             owner, SQL_NTS, table_name, SQL_NTS);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLPrimaryKeysW called with parameters SQLHSTMT=%p, qualifier=%p, SQL_NTS, owner=%p, SQL_NTS, table_name=%p, SQL_NTS and returned rc=%d",
                 (SQLHSTMT)stmt_res->hstmt, (void *)qualifier, (void *)owner, (void *)table_name, rc);
        LogMsg(DEBUG, messageStr);

        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (table_name)
                PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit primary_keys()");
        return (PyObject *)stmt_res;
    }
    else
    {
        LogMsg(ERROR, "Supplied connection object parameter is NULL");
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit primary_keys()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.procedure_columns
 *
 * ===Description
 * resource ibm_db.procedure_columns ( resource connection, string qualifier,
 * string schema, string procedure, string parameter )
 *
 * Returns a result set listing the parameters for one or more stored procedures
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the procedures. This parameter accepts a
 * search pattern containing _ and % as wildcards.
 *
 * ====procedure
 *        The name of the procedure. This parameter accepts a search pattern
 * containing _ and % as wildcards.
 *
 * ====parameter
 *        The name of the parameter. This parameter accepts a search pattern
 * containing _ and % as wildcards.
 *        If this parameter is NULL, all parameters for the specified stored
 * procedures are returned.
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows describing the
 * parameters for the stored procedures matching the specified parameters. The
 * rows are composed of the following columns:
 *
 * Column name::    Description
 * PROCEDURE_CAT:: The catalog that contains the procedure. The value is NULL
 * if this table does not have catalogs.
 * PROCEDURE_SCHEM:: Name of the schema that contains the stored procedure.
 * PROCEDURE_NAME:: Name of the procedure.
 * COLUMN_NAME:: Name of the parameter.
 * COLUMN_TYPE:: An integer value representing the type of the parameter:
 *                      Return value:: Parameter type
 *                      1:: (SQL_PARAM_INPUT)    Input (IN) parameter.
 *                      2:: (SQL_PARAM_INPUT_OUTPUT) Input/output (INOUT)
 *                          parameter.
 *                      3:: (SQL_PARAM_OUTPUT) Output (OUT) parameter.
 * DATA_TYPE:: The SQL data type for the parameter represented as an integer
 * value.
 * TYPE_NAME:: A string representing the data type for the parameter.
 * COLUMN_SIZE:: An integer value representing the size of the parameter.
 * BUFFER_LENGTH:: Maximum number of bytes necessary to store data for this
 * parameter.
 * DECIMAL_DIGITS:: The scale of the parameter, or NULL where scale is not
 * applicable.
 * NUM_PREC_RADIX:: An integer value of either 10 (representing an exact numeric
 * data type), 2 (representing anapproximate numeric data type), or NULL
 * (representing a data type for which radix is not applicable).
 * NULLABLE:: An integer value representing whether the parameter is nullable or
 * not.
 * REMARKS:: Description of the parameter.
 * COLUMN_DEF:: Default value for the parameter.
 * SQL_DATA_TYPE:: An integer value representing the size of the parameter.
 * SQL_DATETIME_SUB:: Returns an integer value representing a datetime subtype
 * code, or NULL for SQL data types to which this does not apply.
 * CHAR_OCTET_LENGTH:: Maximum length in octets for a character data type
 * parameter, which matches COLUMN_SIZE for single-byte character set data, or
 * NULL for non-character data types.
 * ORDINAL_POSITION:: The 1-indexed position of the parameter in the CALL
 * statement.
 * IS_NULLABLE:: A string value where 'YES' means that the parameter accepts or
 * returns NULL values and 'NO' means that the parameter does not accept or
 * return NULL values.
 */
static PyObject *ibm_db_procedure_columns(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry procedure_columns()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *proc_name = NULL;
    SQLWCHAR *column_name = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_proc_name = NULL;
    PyObject *py_column_name = NULL;
    PyObject *py_conn_res = NULL;
    int rc = SQL_SUCCESS;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner,
                          &py_proc_name, &py_column_name))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_proc_name=%p, py_column_name=%p", py_conn_res, py_qualifier, py_owner, py_proc_name, py_column_name);
    LogMsg(DEBUG, messageStr);
    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_proc_name != NULL && py_proc_name != Py_None)
    {
        if (PyString_Check(py_proc_name) || PyUnicode_Check(py_proc_name))
        {
            py_proc_name = PyUnicode_FromObject(py_proc_name);
            snprintf(messageStr, sizeof(messageStr), "Converted proc_name to Unicode: %s", PyUnicode_AsUTF8(py_proc_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "proc_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "proc_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_column_name != NULL && py_column_name != Py_None)
    {
        if (PyString_Check(py_column_name) || PyUnicode_Check(py_column_name))
        {
            py_column_name = PyUnicode_FromObject(py_column_name);
            snprintf(messageStr, sizeof(messageStr), "Converted column_name to Unicode: %s", PyUnicode_AsUTF8(py_column_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "column_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "column_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            Py_XDECREF(py_column_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure created. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLAllocHandle called with parameters SQL_HANDLE_STMT=%d, conn_res->hdbc=%p, stmt_res->hstmt=%p and returned rc=%d",
                 SQL_HANDLE_STMT, (void *)conn_res->hdbc, (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_proc_name && py_proc_name != Py_None)
            proc_name = getUnicodeDataAsSQLWCHAR(py_proc_name, &isNewBuffer);
        if (py_column_name && py_column_name != Py_None)
            column_name = getUnicodeDataAsSQLWCHAR(py_column_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLProcedureColumnsW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
                                  owner, SQL_NTS, proc_name, SQL_NTS, column_name,
                                  SQL_NTS);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLProcedureColumnsW called with parameters SQLHSTMT=%p, qualifier=%p, owner=%p, proc_name=%p, column_name=%p and returned rc=%d",
                 (SQLHSTMT)stmt_res->hstmt, (void *)qualifier, (void *)owner, (void *)proc_name, (void *)column_name, rc);
        LogMsg(DEBUG, messageStr);
        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (proc_name)
                PyMem_Del(proc_name);
            if (column_name)
                PyMem_Del(column_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_proc_name);
        Py_XDECREF(py_column_name);
        LogMsg(INFO, "exit procedure_columns()");
        return (PyObject *)stmt_res;
    }
    else
    {
        LogMsg(ERROR, "Supplied connection object parameter is NULL");
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_proc_name);
        Py_XDECREF(py_column_name);
        LogMsg(INFO, "exit procedure_columns()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.procedures
 *
 * ===Description
 * resource ibm_db.procedures ( resource connection, string qualifier,
 * string schema, string procedure )
 *
 * Returns a result set listing the stored procedures registered in a database.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the procedures. This parameter accepts a
 * search pattern containing _ and % as wildcards.
 *
 * ====procedure
 *        The name of the procedure. This parameter accepts a search pattern
 * containing _ and % as wildcards.
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows describing the
 * stored procedures matching the specified parameters. The rows are composed of
 * the following columns:
 *
 * Column name:: Description
 * PROCEDURE_CAT:: The catalog that contains the procedure. The value is NULL if
 * this table does not have catalogs.
 * PROCEDURE_SCHEM:: Name of the schema that contains the stored procedure.
 * PROCEDURE_NAME:: Name of the procedure.
 * NUM_INPUT_PARAMS:: Number of input (IN) parameters for the stored procedure.
 * NUM_OUTPUT_PARAMS:: Number of output (OUT) parameters for the stored
 * procedure.
 * NUM_RESULT_SETS:: Number of result sets returned by the stored procedure.
 * REMARKS:: Any comments about the stored procedure.
 * PROCEDURE_TYPE:: Always returns 1, indicating that the stored procedure does
 * not return a return value.
 */
static PyObject *ibm_db_procedures(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry procedures()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *proc_name = NULL;
    int rc = SQL_SUCCESS;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_proc_name = NULL;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "OOOO", &py_conn_res, &py_qualifier, &py_owner, &py_proc_name))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_proc_name=%p", py_conn_res, py_qualifier, py_owner, py_proc_name);
    LogMsg(DEBUG, messageStr);
    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_proc_name != NULL && py_proc_name != Py_None)
    {
        if (PyString_Check(py_proc_name) || PyUnicode_Check(py_proc_name))
        {
            py_proc_name = PyUnicode_FromObject(py_proc_name);
            snprintf(messageStr, sizeof(messageStr), "Converted proc_name to Unicode: %s", PyUnicode_AsUTF8(py_proc_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "proc_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "proc_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure created. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLAllocHandle called with parameters SQL_HANDLE_STMT=%d, conn_res->hdbc=%p, stmt_res->hstmt=%p and returned rc=%d",
                 SQL_HANDLE_STMT, (void *)conn_res->hdbc, (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_proc_name && py_proc_name != Py_None)
            proc_name = getUnicodeDataAsSQLWCHAR(py_proc_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLProceduresW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS, owner,
                            SQL_NTS, proc_name, SQL_NTS);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLProceduresW called with parameters SQLHSTMT=%p, qualifier=%p, owner=%p, proc_name=%p and returned rc=%d",
                 (SQLHSTMT)stmt_res->hstmt, (void *)qualifier, (void *)owner, (void *)proc_name, rc);
        LogMsg(DEBUG, messageStr);
        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (proc_name)
                PyMem_Del(proc_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_proc_name);
        LogMsg(INFO, "exit procedures()");
        return (PyObject *)stmt_res;
    }
    else
    {
        LogMsg(ERROR, "Supplied connection object parameter is NULL");
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_proc_name);
        LogMsg(INFO, "exit procedures()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.special_columns
 *
 * ===Description
 * resource ibm_db.special_columns ( resource connection, string qualifier,
 * string schema, string table_name, int scope )
 *
 * Returns a result set listing the unique row identifier columns for a table.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the tables.
 *
 * ====table_name
 *        The name of the table.
 *
 * ====scope
 *        Integer value representing the minimum duration for which the unique
 * row identifier is valid. This can be one of the following values:
 *
 *        0: Row identifier is valid only while the cursor is positioned on the
 * row. (SQL_SCOPE_CURROW)
 *        1: Row identifier is valid for the duration of the transaction.
 * (SQL_SCOPE_TRANSACTION)
 *        2: Row identifier is valid for the duration of the connection.
 * (SQL_SCOPE_SESSION)
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows with unique
 * row identifier information for a table.
 * The rows are composed of the following columns:
 *
 * Column name:: Description
 *
 * SCOPE:: Integer value representing the minimum duration for which the unique
 * row identifier is valid.
 *
 *             0: Row identifier is valid only while the cursor is positioned on
 * the row. (SQL_SCOPE_CURROW)
 *
 *             1: Row identifier is valid for the duration of the transaction.
 * (SQL_SCOPE_TRANSACTION)
 *
 *             2: Row identifier is valid for the duration of the connection.
 * (SQL_SCOPE_SESSION)
 *
 * COLUMN_NAME:: Name of the unique column.
 *
 * DATA_TYPE:: SQL data type for the column.
 *
 * TYPE_NAME:: Character string representation of the SQL data type for the
 * column.
 *
 * COLUMN_SIZE:: An integer value representing the size of the column.
 *
 * BUFFER_LENGTH:: Maximum number of bytes necessary to store data from this
 * column.
 *
 * DECIMAL_DIGITS:: The scale of the column, or NULL where scale is not
 * applicable.
 *
 * NUM_PREC_RADIX:: An integer value of either 10 (representing an exact numeric
 * data type), 2 (representing an approximate numeric data type), or NULL
 * (representing a data type for which radix is not applicable).
 *
 * PSEUDO_COLUMN:: Always returns 1.
 */
static PyObject *ibm_db_special_columns(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry special_columns()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    int scope = 0;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int rc = SQL_SUCCESS;
    PyObject *py_conn_res = NULL;
    PyObject *py_scope = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "OOOOO", &py_conn_res, &py_qualifier, &py_owner, &py_table_name, &py_scope))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_table_name=%p, py_scope=%p", py_conn_res, py_qualifier, py_owner, py_table_name, py_scope);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_scope))
    {
        if (PyInt_Check(py_scope))
        {
            scope = (int)PyLong_AsLong(py_scope);
            snprintf(messageStr, sizeof(messageStr), "Scope value: %d", scope);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "Supplied scope parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            return NULL;
        }
    }
    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None)
    {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name))
        {
            py_table_name = PyUnicode_FromObject(py_table_name);
            snprintf(messageStr, sizeof(messageStr), "Converted table_name to Unicode: %s", PyUnicode_AsUTF8(py_table_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "table_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure created. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLAllocHandle with parameters SQL_HANDLE_STMT=%d, hdbc=%p, hstmt=%p and returned rc=%d",
                 SQL_HANDLE_STMT, (void *)conn_res->hdbc, (void *)&(stmt_res->hstmt), rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None)
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLSpecialColumnsW((SQLHSTMT)stmt_res->hstmt, SQL_BEST_ROWID,
                                qualifier, SQL_NTS, owner, SQL_NTS, table_name,
                                SQL_NTS, scope, SQL_NULLABLE);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLSpecialColumnsW with parameters: "
                                                 "hstmt=%p, scope=%d, qualifier=%p, owner=%p, table_name=%p, nullable=%d and returned rc=%d",
                 (void *)stmt_res->hstmt, scope, (void *)qualifier, (void *)owner, (void *)table_name, SQL_NULLABLE, rc);
        LogMsg(DEBUG, messageStr);
        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (table_name)
                PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit special_columns()");
        return (PyObject *)stmt_res;
    }
    else
    {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit special_columns()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.statistics
 *
 * ===Description
 * resource ibm_db.statistics ( resource connection, string qualifier,
 * string schema, string table-name, bool unique )
 *
 * Returns a result set listing the index and statistics for a table.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema that contains the targeted table. If this parameter is NULL,
 * the statistics and indexes are returned for the schema of the current user.
 *
 * ====table_name
 *        The name of the table.
 *
 * ====unique
 *        A boolean value representing the type of index information to return.
 *
 *        False     Return only the information for unique indexes on the table.
 *
 *        True      Return the information for all indexes on the table.
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows describing the
 * statistics and indexes for the base tables matching the specified parameters.
 * The rows are composed of the following columns:
 *
 * Column name:: Description
 * TABLE_CAT:: The catalog that contains the table. The value is NULL if this
 * table does not have catalogs.
 * TABLE_SCHEM:: Name of the schema that contains the table.
 * TABLE_NAME:: Name of the table.
 * NON_UNIQUE:: An integer value representing whether the index prohibits unique
 * values, or whether the row represents statistics on the table itself:
 *
 *                     Return value:: Parameter type
 *                     0 (SQL_FALSE):: The index allows duplicate values.
 *                     1 (SQL_TRUE):: The index values must be unique.
 *                     NULL:: This row is statistics information for the table
 *                     itself.
 *
 * INDEX_QUALIFIER:: A string value representing the qualifier that would have
 * to be prepended to INDEX_NAME to fully qualify the index.
 * INDEX_NAME:: A string representing the name of the index.
 * TYPE:: An integer value representing the type of information contained in
 * this row of the result set:
 *
 *            Return value:: Parameter type
 *            0 (SQL_TABLE_STAT):: The row contains statistics about the table
 *                                 itself.
 *            1 (SQL_INDEX_CLUSTERED):: The row contains information about a
 *                                      clustered index.
 *            2 (SQL_INDEX_HASH):: The row contains information about a hashed
 *                                 index.
 *            3 (SQL_INDEX_OTHER):: The row contains information about a type of
 * index that is neither clustered nor hashed.
 *
 * ORDINAL_POSITION:: The 1-indexed position of the column in the index. NULL if
 * the row contains statistics information about the table itself.
 * COLUMN_NAME:: The name of the column in the index. NULL if the row contains
 * statistics information about the table itself.
 * ASC_OR_DESC:: A if the column is sorted in ascending order, D if the column
 * is sorted in descending order, NULL if the row contains statistics
 * information about the table itself.
 * CARDINALITY:: If the row contains information about an index, this column
 * contains an integer value representing the number of unique values in the
 * index. If the row contains information about the table itself, this column
 * contains an integer value representing the number of rows in the table.
 * PAGES:: If the row contains information about an index, this column contains
 * an integer value representing the number of pages used to store the index. If
 * the row contains information about the table itself, this column contains an
 * integer value representing the number of pages used to store the table.
 * FILTER_CONDITION:: Always returns NULL.
 */
static PyObject *ibm_db_statistics(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry statistics()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    int unique = 0;
    int rc = SQL_SUCCESS;
    SQLUSMALLINT sql_unique;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    PyObject *py_unique = NULL;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "OOOOO", &py_conn_res, &py_qualifier, &py_owner, &py_table_name, &py_unique))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_table_name=%p, py_unique=%p", py_conn_res, py_qualifier, py_owner, py_table_name, py_unique);
    LogMsg(DEBUG, messageStr);
    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None)
    {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name))
        {
            py_table_name = PyUnicode_FromObject(py_table_name);
            snprintf(messageStr, sizeof(messageStr), "Converted table_name to Unicode: %s", PyUnicode_AsUTF8(py_table_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "table_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_unique != NULL && py_unique != Py_None)
    {
        if (PyBool_Check(py_unique))
        {
            if (py_unique == Py_True)
                unique = 1;
            else
                unique = 0;
            snprintf(messageStr, sizeof(messageStr), "Converted unique to integer: %d", unique);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "unique must be a boolean");
            PyErr_SetString(PyExc_Exception, "unique must be a boolean");
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure created. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
        sql_unique = unique;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLAllocHandle called with parameters SQL_HANDLE_STMT=%d, conn_res->hdbc=%p, stmt_res->hstmt=%p and returned rc=%d",
                 SQL_HANDLE_STMT, (void *)conn_res->hdbc, (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None)
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLStatisticsW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS, owner,
                            SQL_NTS, table_name, SQL_NTS, sql_unique, SQL_QUICK);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLStatisticsW called with parameters: stmt_res->hstmt=%p, qualifier=%p, owner=%p, table_name=%p, sql_unique=%d, SQL_QUICK=%d and returned rc=%d",
                 (void *)stmt_res->hstmt, (void *)qualifier, (void *)owner, (void *)table_name, sql_unique, SQL_QUICK, rc);
        LogMsg(DEBUG, messageStr);
        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (table_name)
                PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit statistics()");
        return (PyObject *)stmt_res;
    }
    else
    {
        LogMsg(ERROR, "Supplied connection object parameter is NULL");
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit statistics()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.table_privileges
 *
 * ===Description
 * resource ibm_db.table_privileges ( resource connection [, string qualifier
 * [, string schema [, string table_name]]] )
 *
 * Returns a result set listing the tables and associated privileges in a
 * database.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the tables. This parameter accepts a search
 * pattern containing _ and % as wildcards.
 *
 * ====table_name
 *        The name of the table. This parameter accepts a search pattern
 * containing _ and % as wildcards.
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows describing
 * the privileges for the tables that match the specified parameters. The rows
 * are composed of the following columns:
 *
 * Column name:: Description
 * TABLE_CAT:: The catalog that contains the table. The value is NULL if this
 * table does not have catalogs.
 * TABLE_SCHEM:: Name of the schema that contains the table.
 * TABLE_NAME:: Name of the table.
 * GRANTOR:: Authorization ID of the user who granted the privilege.
 * GRANTEE:: Authorization ID of the user to whom the privilege was granted.
 * PRIVILEGE:: The privilege that has been granted. This can be one of ALTER,
 * CONTROL, DELETE, INDEX, INSERT, REFERENCES, SELECT, or UPDATE.
 * IS_GRANTABLE:: A string value of "YES" or "NO" indicating whether the grantee
 * can grant the privilege to other users.
 */
static PyObject *ibm_db_table_privileges(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry table_privileges()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int rc = SQL_SUCCESS;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "O|OOO", &py_conn_res, &py_qualifier, &py_owner, &py_table_name))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_table_name=%p", py_conn_res, py_qualifier, py_owner, py_table_name);
    LogMsg(DEBUG, messageStr);
    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None)
    {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name))
        {
            py_table_name = PyUnicode_FromObject(py_table_name);
            snprintf(messageStr, sizeof(messageStr), "Converted table_name to Unicode: %s", PyUnicode_AsUTF8(py_table_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "table_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }
        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure created. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLAllocHandle called with parameters SQL_HANDLE_STMT=%d, conn_res->hdbc=%p, stmt_res->hstmt=%p and returned rc=%d",
                 SQL_HANDLE_STMT, (void *)conn_res->hdbc, (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None)
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLTablePrivilegesW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
                                 owner, SQL_NTS, table_name, SQL_NTS);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLTablePrivilegesW called with parameters SQLHSTMT=%p, qualifier=%p, owner=%p, table_name=%p and returned rc=%d",
                 (SQLHSTMT)stmt_res->hstmt, (void *)qualifier, (void *)owner, (void *)table_name, rc);
        LogMsg(DEBUG, messageStr);
        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (table_name)
                PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit table_privileges()");
        return (PyObject *)stmt_res;
    }
    else
    {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        LogMsg(INFO, "exit table_privileges()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.tables
 *
 * ===Description
 * resource ibm_db.tables ( resource connection [, string qualifier [, string
 * schema [, string table-name [, string table-type]]]] )
 *
 * Returns a result set listing the tables and associated metadata in a database
 *
 * ===Parameters
 *
 * ====connection
 *        A valid connection to an IBM DB2, Cloudscape, or Apache Derby database.
 *
 * ====qualifier
 *        A qualifier for DB2 databases running on OS/390 or z/OS servers. For
 * other databases, pass NULL or an empty string.
 *
 * ====schema
 *        The schema which contains the tables. This parameter accepts a search
 * pattern containing _ and % as wildcards.
 *
 * ====table-name
 *        The name of the table. This parameter accepts a search pattern
 * containing _ and % as wildcards.
 *
 * ====table-type
 *        A list of comma-delimited table type identifiers. To match all table
 * types, pass NULL or an empty string.
 *        Valid table type identifiers include: ALIAS, HIERARCHY TABLE,
 * INOPERATIVE VIEW, NICKNAME, MATERIALIZED QUERY TABLE, SYSTEM TABLE, TABLE,
 * TYPED TABLE, TYPED VIEW, and VIEW.
 *
 * ===Return Values
 *
 * Returns a statement resource with a result set containing rows describing
 * the tables that match the specified parameters.
 * The rows are composed of the following columns:
 *
 * Column name:: Description
 * TABLE_CAT:: The catalog that contains the table. The value is NULL if this
 * table does not have catalogs.
 * TABLE_SCHEMA:: Name of the schema that contains the table.
 * TABLE_NAME:: Name of the table.
 * TABLE_TYPE:: Table type identifier for the table.
 * REMARKS:: Description of the table.
 */
static PyObject *ibm_db_tables(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry tables()");
    LogUTF8Msg(args);
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    SQLWCHAR *table_type = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    PyObject *py_table_type = NULL;
    PyObject *py_conn_res;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int rc = SQL_SUCCESS;
    int isNewBuffer = 0;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner, &py_table_name, &py_table_type))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_qualifier=%p, py_owner=%p, py_table_name=%p, py_table_type=%p", py_conn_res, py_qualifier, py_owner, py_table_name, py_table_type);
    LogMsg(DEBUG, messageStr);
    if (py_qualifier != NULL && py_qualifier != Py_None)
    {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier))
        {
            py_qualifier = PyUnicode_FromObject(py_qualifier);
            snprintf(messageStr, sizeof(messageStr), "Converted qualifier to Unicode: %s", PyUnicode_AsUTF8(py_qualifier));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "qualifier must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None)
    {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner))
        {
            py_owner = PyUnicode_FromObject(py_owner);
            snprintf(messageStr, sizeof(messageStr), "Converted owner to Unicode: %s", PyUnicode_AsUTF8(py_owner));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "owner must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None)
    {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name))
        {
            py_table_name = PyUnicode_FromObject(py_table_name);
            snprintf(messageStr, sizeof(messageStr), "Converted table_name to Unicode: %s", PyUnicode_AsUTF8(py_table_name));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "table_name must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_table_type != NULL && py_table_type != Py_None)
    {
        if (PyString_Check(py_table_type) || PyUnicode_Check(py_table_type))
        {
            py_table_type = PyUnicode_FromObject(py_table_type);
            snprintf(messageStr, sizeof(messageStr), "Converted table_type to Unicode: %s", PyUnicode_AsUTF8(py_table_type));
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "table type must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "table type must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_table_type);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure created. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLAllocHandle called with parameters SQL_HANDLE_STMT=%d, conn_res->hdbc=%p, stmt_res->hstmt=%p and returned rc=%d",
                 SQL_HANDLE_STMT, (void *)conn_res->hdbc, (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_table_type);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None)
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner && py_owner != Py_None)
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None)
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);
        if (py_table_type && py_table_type != Py_None)
            table_type = getUnicodeDataAsSQLWCHAR(py_table_type, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLTablesW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS, owner,
                        SQL_NTS, table_name, SQL_NTS, table_type, SQL_NTS);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLTablesW called with parameters SQLHSTMT=%p, qualifier=%p, owner=%p, table_name=%p, table_type=%p and returned rc=%d",
                 (SQLHSTMT)stmt_res->hstmt, (void *)qualifier, (void *)owner, (void *)table_name, (void *)table_type, rc);
        LogMsg(DEBUG, messageStr);
        if (isNewBuffer)
        {
            if (qualifier)
                PyMem_Del(qualifier);
            if (owner)
                PyMem_Del(owner);
            if (table_name)
                PyMem_Del(table_name);
            if (table_type)
                PyMem_Del(table_type);
        }

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_table_type);

            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_table_type);
        LogMsg(INFO, "exit tables()");
        return (PyObject *)stmt_res;
    }
    else
    {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_table_type);
        LogMsg(INFO, "exit tables()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.commit
 * ===Description
 * bool ibm_db.commit ( resource connection )
 *
 * Commits an in-progress transaction on the specified connection resource and
 * begins a new transaction.
 * Python applications normally default to AUTOCOMMIT mode, so ibm_db.commit()
 * is not necessary unless AUTOCOMMIT has been turned off for the connection
 * resource.
 *
 * Note: If the specified connection resource is a persistent connection, all
 * transactions in progress for all applications using that persistent
 * connection will be committed. For this reason, persistent connections are
 * not recommended for use in applications that require transactions.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid database connection resource variable as returned from
 * ibm_db.connect() or ibm_db.pconnect().
 *
 * ===Return Values
 *
 * Returns TRUE on success or FALSE on failure.
 */
static PyObject *ibm_db_commit(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry commit()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res;
    int rc;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed arguments: py_conn_res: %p", (void *)py_conn_res);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(EXCEPTION, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLEndTran(SQL_HANDLE_DBC, conn_res->hdbc, SQL_COMMIT);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLEndTran returned rc=%d", rc);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            LogMsg(ERROR, "SQLEndTran failed");
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_INCREF(Py_False);
            return Py_False;
        }
        else
        {
            LogMsg(INFO, "Transaction committed successfully");
            Py_INCREF(Py_True);
            LogMsg(INFO, "exit commit()");
            return Py_True;
        }
    }
    LogMsg(INFO, "No connection object provided, returning False");
    Py_INCREF(Py_False);
    LogMsg(INFO, "exit commit()");
    return Py_False;
}

/* static int _python_ibm_db_do_prepare(SQLHANDLE hdbc, char *stmt_string, stmt_handle *stmt_res, PyObject *options)
 */
static int _python_ibm_db_do_prepare(SQLHANDLE hdbc, SQLWCHAR *stmt, int stmt_size, stmt_handle *stmt_res, PyObject *options)
{
    LogMsg(INFO, "entry _python_ibm_db_do_prepare()");
    int rc;
    /* alloc handle and return only if it errors */
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &(stmt_res->hstmt));
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle result: %d, stmt_res->hstmt: %p", rc, stmt_res->hstmt);
    LogMsg(INFO, messageStr);
    if (rc == SQL_ERROR)
    {
        _python_ibm_db_check_sql_errors(hdbc, SQL_HANDLE_DBC, rc,
                                        1, NULL, -1, 1);
        return rc;
    }

    /* get the string and its length */
    if (NIL_P(stmt))
    {
        LogMsg(EXCEPTION, "Supplied statement parameter is invalid");
        PyErr_SetString(PyExc_Exception,
                        "Supplied statement parameter is invalid");
        return rc;
    }

    if (rc < SQL_SUCCESS)
    {
        _python_ibm_db_check_sql_errors(hdbc, SQL_HANDLE_DBC, rc, 1, NULL, -1, 1);
        LogMsg(ERROR, "Statement prepare Failed");
        PyErr_SetString(PyExc_Exception, "Statement prepare Failed: ");
        return rc;
    }

    if (!NIL_P(options))
    {
        rc = _python_ibm_db_parse_options(options, SQL_HANDLE_STMT, stmt_res);
        if (rc == SQL_ERROR)
        {
            LogMsg(INFO, "exit _python_ibm_db_do_prepare()");
            return rc;
        }
    }
    snprintf(messageStr, sizeof(messageStr), "Preparing SQL statement: %ls, size: %d", stmt, stmt_size);
    LogMsg(DEBUG, messageStr);
    /* Prepare the stmt. The cursor type requested has already been set in
     * _python_ibm_db_assign_options
     */

    Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
    rc = SQLPrepareW((SQLHSTMT)stmt_res->hstmt, stmt,
                     stmt_size * 2);
    snprintf(messageStr, sizeof(messageStr), "SQLPrepareW called with: hstmt=%p, stmt=%p, stmt_size=%d, and returned rc: %d",
             (void *)stmt_res->hstmt, (void *)stmt, stmt_size * 2, rc);
    LogMsg(DEBUG, messageStr);
#else
    rc = SQLPrepareW((SQLHSTMT)stmt_res->hstmt, stmt,
                     stmt_size);
    snprintf(messageStr, sizeof(messageStr), "SQLPrepareW called with: hstmt=%p, stmt=%p, stmt_size=%d, and returned rc: %d",
             (void *)stmt_res->hstmt, (void *)stmt, stmt_size, rc);
    LogMsg(DEBUG, messageStr);
#endif
    Py_END_ALLOW_THREADS;

    if (rc == SQL_ERROR)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                        1, NULL, -1, 1);
    }
    LogMsg(INFO, "exit _python_ibm_db_do_prepare()");
    return rc;
}

/*!# ibm_db.exec
 *
 * ===Description
 * stmt_handle ibm_db.exec ( IBM_DBConnection connection, string statement
 *                                [, array options] )
 *
 * Prepares and executes an SQL statement.
 *
 * If you plan to interpolate Python variables into the SQL statement,
 * understand that this is one of the more common security exposures. Consider
 * calling ibm_db.prepare() to prepare an SQL statement with parameter markers    * for input values. Then you can call ibm_db.execute() to pass in the input
 * values and avoid SQL injection attacks.
 *
 * If you plan to repeatedly issue the same SQL statement with different
 * parameters, consider calling ibm_db.:prepare() and ibm_db.execute() to
 * enable the database server to reuse its access plan and increase the
 * efficiency of your database access.
 *
 * ===Parameters
 *
 * ====connection
 *
 *        A valid database connection resource variable as returned from
 * ibm_db.connect() or ibm_db.pconnect().
 *
 * ====statement
 *
 *        An SQL statement. The statement cannot contain any parameter markers.
 *
 * ====options
 *
 *        An dictionary containing statement options. You can use this parameter  * to request a scrollable cursor on database servers that support this
 * functionality.
 *
 *        SQL_ATTR_CURSOR_TYPE
 *             Passing the SQL_SCROLL_FORWARD_ONLY value requests a forward-only
 *             cursor for this SQL statement. This is the default type of
 *             cursor, and it is supported by all database servers. It is also
 *             much faster than a scrollable cursor.
 *
 *             Passing the SQL_CURSOR_KEYSET_DRIVEN value requests a scrollable  *             cursor for this SQL statement. This type of cursor enables you to
 *             fetch rows non-sequentially from the database server. However, it
 *             is only supported by DB2 servers, and is much slower than
 *             forward-only cursors.
 *
 * ===Return Values
 *
 * Returns a stmt_handle resource if the SQL statement was issued
 * successfully, or FALSE if the database failed to execute the SQL statement.
 */
static PyObject *ibm_db_exec(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry exec_immediate()");
    LogUTF8Msg(args);
    PyObject *options = NULL;
    PyObject *py_conn_res = NULL;
    stmt_handle *stmt_res;
    conn_handle *conn_res;
    int rc = SQL_SUCCESS;
    int isNewBuffer = 0;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return err
                              * strings
                              */
    SQLWCHAR *stmt = NULL;
    PyObject *py_stmt = NULL;

    /* This function basically is a wrap of the _python_ibm_db_do_prepare and
     * _python_ibm_db_Execute_stmt
     * After completing statement execution, it returns the statement resource
     */

    if (!PyArg_ParseTuple(args, "OO|O", &py_conn_res, &py_stmt, &options))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed arguments: py_conn_res: %p, py_stmt=%p, options=%p", (void *)py_conn_res, (void *)py_stmt, (void *)options);
    LogMsg(DEBUG, messageStr);
    if (py_stmt != NULL && py_stmt != Py_None)
    {
        if (PyString_Check(py_stmt) || PyUnicode_Check(py_stmt))
        {
            py_stmt = PyUnicode_FromObject(py_stmt);
            snprintf(messageStr, sizeof(messageStr), "py_stmt converted to Unicode. py_stmt: %p", (void *)py_stmt);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(EXCEPTION, "statement must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "statement must be a string or unicode");
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(EXCEPTION, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "py_conn_res is valid. Converted to conn_handle. py_conn_res: %p", (void *)py_conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(EXCEPTION, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_stmt);
            return NULL;
        }

        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        if (return_str == NULL)
        {
            snprintf(messageStr, sizeof(messageStr), "Failed to allocate memory. Requested size: %d", DB2_MAX_ERR_MSG_LEN);
            LogMsg(EXCEPTION, messageStr);
            PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
            Py_XDECREF(py_stmt);
            return NULL;
        }

        memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);

        _python_ibm_db_clear_stmt_err_cache();

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        snprintf(messageStr, sizeof(messageStr), "New statement structure allocated. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);

        /* Allocates the stmt handle */
        /* returns the stat_handle back to the calling function */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        snprintf(messageStr, sizeof(messageStr), "SQLAllocHandle result: rc = %d, stmt_res->hstmt: %p", rc, (void *)stmt_res->hstmt);
        LogMsg(DEBUG, messageStr);
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            LogMsg(ERROR, "SQLAllocHandle failed with SQL_ERROR");
            PyMem_Del(return_str);
            Py_XDECREF(py_stmt);
            return NULL;
        }

        if (!NIL_P(options))
        {
            rc = _python_ibm_db_parse_options(options, SQL_HANDLE_STMT, stmt_res);
            snprintf(messageStr, sizeof(messageStr), "Parsed options. rc = %d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "Error parsing options");
                Py_XDECREF(py_stmt);
                return NULL;
            }
        }
        if (py_stmt != NULL && py_stmt != Py_None)
        {
            stmt = getUnicodeDataAsSQLWCHAR(py_stmt, &isNewBuffer);
            snprintf(messageStr, sizeof(messageStr), "Converted py_stmt to SQLWCHAR. stmt: %p", (void *)stmt);
            LogMsg(DEBUG, messageStr);
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLExecDirectW((SQLHSTMT)stmt_res->hstmt, stmt, SQL_NTS);
        snprintf(messageStr, sizeof(messageStr), "SQLExecDirectW result: rc = %d", rc);
        LogMsg(DEBUG, messageStr);
        Py_END_ALLOW_THREADS;
        if (rc < SQL_SUCCESS)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, -1,
                                            1, return_str, DB2_ERRMSG,
                                            stmt_res->errormsg_recno_tracker);
            LogMsg(ERROR, "SQLExecDirectW failed with SQL_ERROR");
            Py_BEGIN_ALLOW_THREADS;
            SQLFreeHandle(SQL_HANDLE_STMT, stmt_res->hstmt);
            Py_END_ALLOW_THREADS;
            /* TODO: Object freeing */
            /* free(stmt_res); */
            if (isNewBuffer)
            {
                if (stmt)
                    PyMem_Del(stmt);
            }
            Py_XDECREF(py_stmt);
            PyMem_Del(return_str);
            return NULL;
        }
        if (rc == SQL_SUCCESS_WITH_INFO)
        {
            LogMsg(INFO, "SQLExecDirectW succeeded with warnings");
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, 1,
                                            1, return_str, DB2_WARNMSG,
                                            stmt_res->errormsg_recno_tracker);
        }
        if (isNewBuffer)
        {
            if (stmt)
                PyMem_Del(stmt);
        }
        PyMem_Del(return_str);
        Py_XDECREF(py_stmt);
        snprintf(messageStr, sizeof(messageStr), "Statement execution successful. Returning stmt_res: %p", (void *)stmt_res);
        LogMsg(INFO, messageStr);
        LogMsg(INFO, "exit exec_immediate()");
        return (PyObject *)stmt_res;
    }
    Py_XDECREF(py_stmt);
    LogMsg(INFO, "exit exec_immediate()");
    return NULL;
}

/*!# ibm_db.free_result
 *
 * ===Description
 * bool ibm_db.free_result ( resource stmt )
 *
 * Frees the system and database resources that are associated with a result
 * set. These resources are freed implicitly when a script finishes, but you
 * can call ibm_db.free_result() to explicitly free the result set resources
 * before the end of the script.
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid statement resource.
 *
 * ===Return Values
 *
 * Returns TRUE on success or FALSE on failure.
 */
static PyObject *ibm_db_free_result(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry free_result()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        if (stmt_res->hstmt)
        {
            /* Free any cursors that might have been allocated in a previous call
             * to SQLExecute
             */
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeStmt((SQLHSTMT)stmt_res->hstmt, SQL_CLOSE);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLFreeStmt called with parameters: stmt_res->hstmt=%p, SQL_CLOSE=%d and returned rc=%d",
                     (void *)stmt_res->hstmt, SQL_CLOSE, rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            if (rc == SQL_ERROR)
            {
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
        }
        _python_ibm_db_free_result_struct(stmt_res);
    }
    else
    {
        LogMsg(EXCEPTION, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
    Py_INCREF(Py_True);
    LogMsg(INFO, "exit free_result()");
    return Py_True;
}

/*
 * static PyObject *_python_ibm_db_prepare_helper(conn_handle *conn_res, PyObject *py_stmt, PyObject *options)
 *
 */
static PyObject *_python_ibm_db_prepare_helper(conn_handle *conn_res, PyObject *py_stmt, PyObject *options)
{
    LogMsg(INFO, "entry _python_ibm_db_prepare_helper()");
    stmt_handle *stmt_res;
    int rc = SQL_SUCCESS;
    char error[DB2_MAX_ERR_MSG_LEN + 50];
    SQLWCHAR *stmt = NULL;
    int stmt_size = 0;
    int isNewBuffer = 0;

    if (!conn_res->handle_active)
    {
        LogMsg(ERROR, "Connection is not active");
        PyErr_SetString(PyExc_Exception, "Connection is not active");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Connection handle active: %d", conn_res->handle_active);
    LogMsg(DEBUG, messageStr);
    if (py_stmt != NULL && PyUnicode_Check(py_stmt))
    {
        snprintf(messageStr, sizeof(messageStr), "Initial py_stmt: %s", PyUnicode_AsUTF8(py_stmt));
        LogMsg(DEBUG, messageStr);
    }
    if (options != NULL && PyUnicode_Check(options))
    {
        snprintf(messageStr, sizeof(messageStr), "Options: %s", PyUnicode_AsUTF8(options));
        LogMsg(DEBUG, messageStr);
    }
    if (py_stmt != NULL && py_stmt != Py_None)
    {
        if (PyString_Check(py_stmt) || PyUnicode_Check(py_stmt))
        {
            py_stmt = PyUnicode_FromObject(py_stmt);
            if (py_stmt != NULL && py_stmt != Py_None)
            {
                stmt_size = PyUnicode_GetLength(py_stmt);
                snprintf(messageStr, sizeof(messageStr), "Converted py_stmt to Unicode, length: %d", stmt_size);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Error occure during processing of statement");
                PyErr_SetString(PyExc_Exception, "Error occure during processing of statement");
                return NULL;
            }
        }
        else
        {
            LogMsg(EXCEPTION, "statement must be a string or unicode");
            PyErr_SetString(PyExc_Exception, "statement must be a string or unicode");
            return NULL;
        }
    }

    _python_ibm_db_clear_stmt_err_cache();

    /* Initialize stmt resource members with default values. */
    /* Parsing will update options if needed */

    stmt_res = _ibm_db_new_stmt_struct(conn_res);
    snprintf(messageStr, sizeof(messageStr), "Initialized new stmt handle: %p", (void *)stmt_res);
    LogMsg(DEBUG, messageStr);

    /* Allocates the stmt handle */
    /* Prepares the statement */
    /* returns the stat_handle back to the calling function */
    if (py_stmt && py_stmt != Py_None)
    {
        stmt = getUnicodeDataAsSQLWCHAR(py_stmt, &isNewBuffer);
        snprintf(messageStr, sizeof(messageStr), "Allocated stmt buffer: %p, isNewBuffer: %d", (void *)stmt, isNewBuffer);
        LogMsg(DEBUG, messageStr);
    }

    rc = _python_ibm_db_do_prepare(conn_res->hdbc, stmt, stmt_size, stmt_res, options);
    snprintf(messageStr, sizeof(messageStr), "Prepared statement, return code: %d", rc);
    LogMsg(DEBUG, messageStr);
    if (isNewBuffer)
    {
        if (stmt)
            PyMem_Del(stmt);
    }

    if (rc < SQL_SUCCESS)
    {
        sprintf(error, "Statement Prepare Failed: %s", IBM_DB_G(__python_stmt_err_msg));
        LogMsg(ERROR, error);
        Py_XDECREF(py_stmt);
        return NULL;
    }
    Py_XDECREF(py_stmt);
    LogMsg(INFO, "exit _python_ibm_db_prepare_helper()");
    return (PyObject *)stmt_res;
}

/*!# ibm_db.prepare
 *
 * ===Description
 * IBMDB_Statement ibm_db.prepare ( IBM_DBConnection connection,
 *                                  string statement [, array options] )
 *
 * ibm_db.prepare() creates a prepared SQL statement which can include 0 or
 * more parameter markers (? characters) representing parameters for input,
 * output, or input/output. You can pass parameters to the prepared statement
 * using ibm_db.bind_param(), or for input values only, as an array passed to
 * ibm_db.execute().
 *
 * There are three main advantages to using prepared statements in your
 * application:
 *        * Performance: when you prepare a statement, the database server
 *         creates an optimized access plan for retrieving data with that
 *         statement. Subsequently issuing the prepared statement with
 *         ibm_db.execute() enables the statements to reuse that access plan
 *         and avoids the overhead of dynamically creating a new access plan
 *         for every statement you issue.
 *        * Security: when you prepare a statement, you can include parameter
 *         markers for input values. When you execute a prepared statement
 *         with input values for placeholders, the database server checks each
 *         input value to ensure that the type matches the column definition or
 *         parameter definition.
 *        * Advanced functionality: Parameter markers not only enable you to
 *         pass input values to prepared SQL statements, they also enable you
 *         to retrieve OUT and INOUT parameters from stored procedures using
 *         ibm_db.bind_param().
 *
 * ===Parameters
 * ====connection
 *
 *        A valid database connection resource variable as returned from
 *        ibm_db.connect() or ibm_db.pconnect().
 *
 * ====statement
 *
 *        An SQL statement, optionally containing one or more parameter markers.
 *
 * ====options
 *
 *        An dictionary containing statement options. You can use this parameter
 *        to request a scrollable cursor on database servers that support this
 *        functionality.
 *
 *        SQL_ATTR_CURSOR_TYPE
 *             Passing the SQL_SCROLL_FORWARD_ONLY value requests a forward-only
 *             cursor for this SQL statement. This is the default type of
 *             cursor, and it is supported by all database servers. It is also
 *             much faster than a scrollable cursor.
 *             Passing the SQL_CURSOR_KEYSET_DRIVEN value requests a scrollable
 *             cursor for this SQL statement. This type of cursor enables you
 *             to fetch rows non-sequentially from the database server. However,
 *             it is only supported by DB2 servers, and is much slower than
 *             forward-only cursors.
 *
 * ===Return Values
 * Returns a IBM_DBStatement object if the SQL statement was successfully
 * parsed and prepared by the database server. Returns FALSE if the database
 * server returned an error. You can determine which error was returned by
 * calling ibm_db.stmt_error() or ibm_db.stmt_errormsg().
 */
static PyObject *ibm_db_prepare(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry prepare()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *options = NULL;
    conn_handle *conn_res;

    PyObject *py_stmt = NULL;

    if (!PyArg_ParseTuple(args, "OO|O", &py_conn_res, &py_stmt, &options))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed arguments: py_conn_res: %p, py_stmt: %p, options: %p",
             (void *)py_conn_res, (void *)py_stmt, (void *)options);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(EXCEPTION, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection object is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        LogMsg(INFO, "Calling and returning _python_ibm_db_prepare_helper");
        LogMsg(INFO, "exit prepare()");
        return _python_ibm_db_prepare_helper(conn_res, py_stmt, options);
    }
    LogMsg(INFO, "exit prepare()");
    return NULL;
}

/*    static param_node* build_list( stmt_res, param_no, data_type, precision, scale, nullable )
 */
static param_node *build_list(stmt_handle *stmt_res, int param_no, SQLSMALLINT data_type, SQLUINTEGER precision, SQLSMALLINT scale, SQLSMALLINT nullable)
{
    param_node *tmp_curr = NULL, *curr = stmt_res->head_cache_list, *prev = NULL;

    /* Allocate memory and make new node to be added */
    tmp_curr = ALLOC(param_node);
    memset(tmp_curr, 0, sizeof(param_node));
    /* assign values */
    tmp_curr->data_type = data_type;
    tmp_curr->param_size = precision;
    tmp_curr->nullable = nullable;
    tmp_curr->scale = scale;
    tmp_curr->param_num = param_no;
    tmp_curr->file_options = SQL_FILE_READ;
    tmp_curr->param_type = SQL_PARAM_INPUT;

    while (curr != NULL)
    {
        prev = curr;
        curr = curr->next;
    }

    if (stmt_res->head_cache_list == NULL)
    {
        stmt_res->head_cache_list = tmp_curr;
    }
    else
    {
        prev->next = tmp_curr;
    }

    tmp_curr->next = curr;

    return tmp_curr;
}

/*    static int _python_ibm_db_bind_data( stmt_handle *stmt_res, param_node *curr, PyObject *bind_data )
 */
static int _python_ibm_db_bind_data(stmt_handle *stmt_res, param_node *curr, PyObject *bind_data)
{
    LogMsg(INFO, "entry _python_ibm_db_bind_data()");
    int rc, i;
    SQLSMALLINT valueType = 0;
    SQLPOINTER paramValuePtr;
    SQLWCHAR *tmp_uvalue = NULL;
    SQLWCHAR *dest_uvalue = NULL;
    char *tmp_svalue = NULL;
    char *dest_svalue = NULL;
#if PY_MAJOR_VERSION < 3
    Py_ssize_t buffer_len = 0;
#endif
    int param_length;
    int type = PYTHON_NIL;
    PyObject *item;
    snprintf(messageStr, sizeof(messageStr), "Parameters: stmt_res=%p, curr->param_type=%d, bind_data=%p",
             (void *)stmt_res, curr->param_type, (void *)bind_data);
    LogMsg(DEBUG, messageStr);
    /* Have to use SQLBindFileToParam if PARAM is type PARAM_FILE */
    /*** Need to fix this***/
    if (curr->param_type == PARAM_FILE)
    {
        PyObject *FileNameObj = NULL;
        /* Only string types can be bound */
        if (PyString_Check(bind_data))
        {
            if (PyUnicode_Check(bind_data))
            {
                FileNameObj = PyUnicode_AsASCIIString(bind_data);
                if (FileNameObj == NULL)
                {
                    LogMsg(ERROR, "PyUnicode_AsASCIIString failed");
                    return SQL_ERROR;
                }
            }
        }
        else
        {
            LogMsg(ERROR, "bind_data is not a string type");
            return SQL_ERROR;
        }
        curr->bind_indicator = 0;
        if (curr->svalue != NULL)
        {
            PyMem_Del(curr->svalue);
            curr->svalue = NULL;
        }
        if (FileNameObj != NULL)
        {
            curr->svalue = PyBytes_AsString(FileNameObj);
        }
        else
        {
            curr->svalue = PyBytes_AsString(bind_data);
        }
        curr->ivalue = strlen(curr->svalue);
        curr->svalue = memcpy(PyMem_Malloc((sizeof(char)) * (curr->ivalue + 1)), curr->svalue, curr->ivalue);
        curr->svalue[curr->ivalue] = '\0';
        Py_XDECREF(FileNameObj);
        valueType = (SQLSMALLINT)curr->ivalue;
        /* Bind file name string */

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLBindFileToParam((SQLHSTMT)stmt_res->hstmt, curr->param_num,
                                curr->data_type, (SQLCHAR *)curr->svalue,
                                (SQLSMALLINT *)&(curr->ivalue), &(curr->file_options),
                                (SQLSMALLINT)curr->ivalue, &(curr->bind_indicator));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLBindFileToParam with parameters: hstmt=%p, param_num=%d, data_type=%d, svalue=%s, ivalue=%d, file_options=%p, ivalue_count=%d, bind_indicator=%p, and returned rc=%d",
                 (void *)stmt_res->hstmt, curr->param_num, curr->data_type, curr->svalue, curr->ivalue, (void *)&(curr->file_options), curr->ivalue, (void *)&(curr->bind_indicator), rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }
        LogMsg(INFO, "exit _python_ibm_db_bind_data()");
        return rc;
    }

    type = TYPE(bind_data);
    snprintf(messageStr, sizeof(messageStr), "Initial bind_data type: %d", type);
    LogMsg(DEBUG, messageStr);
    if (type == PYTHON_LIST)
    {
        Py_ssize_t n = PyList_Size(bind_data);
        snprintf(messageStr, sizeof(messageStr), "Size of bind_data (list): %zd", n);
        LogMsg(DEBUG, messageStr);
        for (i = 0; i < n; i++)
        {
            item = PyList_GetItem(bind_data, i);
            snprintf(messageStr, sizeof(messageStr),
                     "List index %d, item pointer: %p", i, (void *)item);
            LogMsg(DEBUG, messageStr);
            type = TYPE(item);
            snprintf(messageStr, sizeof(messageStr), "Type of item at index %d: %d", i, type);
            LogMsg(DEBUG, messageStr);
            if (type != PYTHON_NIL)
            {
                snprintf(messageStr, sizeof(messageStr), "Found non-NIL item at index %d", i);
                LogMsg(DEBUG, messageStr);
                break;
            }
        }
    }

    switch (type)
    {
    case PYTHON_FIXNUM:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_FIXNUM case with type=%d", type);
        LogMsg(DEBUG, messageStr);
        /* BIGINT_IS_SHORTER_THAN_LONG: Avoid SQLCODE=22005: In xlc with -q64, the size of BIGINT is the same as the size of long */
        if (BIGINT_IS_SHORTER_THAN_LONG && (curr->data_type == SQL_BIGINT || curr->data_type == SQL_DECIMAL))
        {
#if PY_MAJOR_VERSION >= 3
            PyObject *tempobj2 = NULL;
#endif
            PyObject *tempobj = NULL;
            if (TYPE(bind_data) == PYTHON_LIST)
            {
                char *svalue = NULL;
                Py_ssize_t n = PyList_Size(bind_data);
                snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
                LogMsg(DEBUG, messageStr);
                curr->svalue = (char *)ALLOC_N(char, (MAX_PRECISION) * (n));
                curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
                memset(curr->svalue, 0, MAX_PRECISION * n);
                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    snprintf(messageStr, sizeof(messageStr), "List index %d, item pointer: %p", i, (void *)item);
                    LogMsg(DEBUG, messageStr);
                    if (TYPE(item) == PYTHON_NIL)
                    {
                        curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    }
                    else
                    {
                        item = PyObject_Str(item);
#if PY_MAJOR_VERSION >= 3
                        tempobj2 = PyUnicode_AsASCIIString(item);
                        Py_XDECREF(item);
                        item = tempobj2;
#endif
                        svalue = PyBytes_AsString(item);
                        curr->ivalue = strlen(svalue);
                        memcpy(curr->svalue + (i * MAX_PRECISION), svalue, curr->ivalue);
                        svalue = NULL;
                        snprintf(messageStr, sizeof(messageStr),
                                 "Processing list item index %d: svalue=%s, ivalue=%d", i, svalue, curr->ivalue);
                        LogMsg(DEBUG, messageStr);
                        curr->bind_indicator_array[i] = SQL_NTS;
                    }
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                      curr->param_type, SQL_C_CHAR, curr->data_type,
                                      MAX_PRECISION, curr->scale, curr->svalue, MAX_PRECISION, &curr->bind_indicator_array[0]);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, precision=%lu, scale=%d, svalue=%s, buffer_length=%lu, bind_indicator_array=%p and returned rc:%d",
                         (void *)stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_CHAR, curr->data_type, (unsigned long)MAX_PRECISION, curr->scale, curr->svalue, (unsigned long)MAX_PRECISION, (void *)&curr->bind_indicator_array[0], rc);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
#if PY_MAJOR_VERSION >= 3
                PyObject *tempobj2 = NULL;
#endif
                tempobj = PyObject_Str(bind_data);
#if PY_MAJOR_VERSION >= 3
                tempobj2 = PyUnicode_AsASCIIString(tempobj);
                Py_XDECREF(tempobj);
                tempobj = tempobj2;
#endif
                curr->svalue = PyBytes_AsString(tempobj);
                curr->ivalue = strlen(curr->svalue);
                size_t alloc_size = (curr->param_size > curr->ivalue) ? curr->param_size : curr->ivalue;
                curr->svalue = memcpy(PyMem_Malloc((sizeof(char)) * (alloc_size + 1)), curr->svalue, curr->ivalue);
                curr->svalue[curr->ivalue] = '\0';
                snprintf(messageStr, sizeof(messageStr),
                         "Processing single value: svalue=%s, ivalue=%d, bind_indicator=%d", curr->svalue, curr->ivalue, curr->bind_indicator);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                      curr->param_type, SQL_C_CHAR, curr->data_type,
                                      curr->param_size, curr->scale, curr->svalue, curr->param_size, NULL);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, svalue=%s, buffer_length=%lu, bind_indicator=%p and returned rc=%d",
                         (void *)stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_CHAR, curr->data_type, (unsigned long)curr->param_size, curr->scale, curr->svalue, (unsigned long)curr->param_size, (void *)NULL, rc);
                LogMsg(DEBUG, messageStr);
            }

            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            Py_XDECREF(tempobj);
        }
        else
        {
            if (TYPE(bind_data) == PYTHON_LIST)
            {
                Py_ssize_t n = PyList_Size(bind_data);
                snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
                LogMsg(DEBUG, messageStr);
                curr->ivalueArray = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
                curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    snprintf(messageStr, sizeof(messageStr), "List index %d, item pointer: %p", i, (void *)item);
                    LogMsg(DEBUG, messageStr);
                    if (TYPE(item) == PYTHON_NIL)
                    {
                        curr->ivalueArray[i] = 0;
                        curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    }
                    else
                    {
                        curr->ivalueArray[i] = PyLong_AsLong(item);
                        curr->bind_indicator_array[i] = SQL_NTS;
                    }
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                      curr->param_type, SQL_C_LONG, curr->data_type,
                                      curr->param_size, curr->scale, curr->ivalueArray, 0, &curr->bind_indicator_array[0]);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, ivalueArray=%p, buffer_length=%lu, bind_indicator_array=%p, rc=%d", (void *)stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_LONG, curr->data_type,
                         (unsigned long)curr->param_size, curr->scale, (void *)curr->ivalueArray, (unsigned long)0, (void *)&curr->bind_indicator_array[0], rc);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                curr->ivalue = (SQLINTEGER)PyLong_AsLong(bind_data);
                snprintf(messageStr, sizeof(messageStr),
                         "Processing single value: ivalue=%d", curr->ivalue);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                      curr->param_type, SQL_C_LONG, curr->data_type,
                                      curr->param_size, curr->scale, &curr->ivalue, 0, NULL);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, ivalue=%d, buffer_length=%lu, bind_indicator=%p and returned rc=%d", (void *)stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_LONG,
                         curr->data_type, (unsigned long)curr->param_size, curr->scale, curr->ivalue, (unsigned long)0, (void *)NULL, rc);
                LogMsg(DEBUG, messageStr);
            }
            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            curr->data_type = SQL_C_LONG;
        }
        break;

    /* Convert BOOLEAN types to LONG for DB2 / Cloudscape */
    case PYTHON_FALSE:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_FALSE case with type=%d", type);
        LogMsg(DEBUG, messageStr);
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->ivalueArray = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "List index %d, item pointer: %p", i, (void *)item);
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->ivalueArray[i] = 0;
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                }
                else
                {
                    curr->ivalueArray[i] = PyLong_AsLong(item);
                    curr->bind_indicator_array[i] = SQL_NTS;
                    snprintf(messageStr, sizeof(messageStr), "Processed list item index %d: ivalueArray[%d]=%d, bind_indicator_array[%d]=%d",
                             i, i, curr->ivalueArray[i], i, curr->bind_indicator_array[i]);
                    LogMsg(DEBUG, messageStr);
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_LONG, curr->data_type,
                                  curr->param_size, curr->scale, curr->ivalueArray, 0, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, ivalueArray=%p, bind_indicator_array=%p, and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size, curr->scale, curr->ivalueArray, curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            curr->ivalue = 0;
            snprintf(messageStr, sizeof(messageStr), "Processing single value: ivalue=%d", curr->ivalue);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size,
                                  curr->scale, &curr->ivalue, 0, NULL);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, ivalue=%d,  and returne rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size, curr->scale, curr->ivalue, rc);
            LogMsg(DEBUG, messageStr);
        }

        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }

        curr->data_type = SQL_C_LONG;
        LogMsg(DEBUG, "Updated curr->data_type to SQL_C_LONG");
        break;

    case PYTHON_TRUE:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_TRUE case with type=%d", type);
        LogMsg(DEBUG, messageStr);
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->ivalueArray = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "List index %d, item pointer: %p", i, (void *)item);
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->ivalueArray[i] = 0;
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d is PYTHON_NIL: ivalueArray[%d]=NULL, bind_indicator_array[%d]=SQL_NULL_DATA",
                             i, i, i);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    curr->ivalueArray[i] = PyLong_AsLong(item);
                    curr->bind_indicator_array[i] = SQL_NTS;
                    snprintf(messageStr, sizeof(messageStr), "Processed list item index %d: ivalueArray[%d]=%d, bind_indicator_array[%d]=SQL_NTS",
                             i, i, curr->ivalueArray[i], i);
                    LogMsg(DEBUG, messageStr);
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_LONG, curr->data_type,
                                  curr->param_size, curr->scale, curr->ivalueArray, 0, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, ivalueArray=%p, bind_indicator_array=%p, and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size, curr->scale, curr->ivalueArray, curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            curr->ivalue = 1;
            snprintf(messageStr, sizeof(messageStr), "Single value case: ivalue set to %d", curr->ivalue);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size,
                                  curr->scale, &curr->ivalue, 0, NULL);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, ivalue=%d, and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size, curr->scale, curr->ivalue, rc);
            LogMsg(DEBUG, messageStr);
        }

        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }
        curr->data_type = SQL_C_LONG;
        LogMsg(DEBUG, "Updated curr->data_type to SQL_C_LONG");
        break;

    case PYTHON_FLOAT:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_FLOAT case with type=%d", type);
        LogMsg(DEBUG, messageStr);
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->fvalueArray = (double *)ALLOC_N(double, n);
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "List index %d, item pointer: %p", i, (void *)item);
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d is PYTHON_NIL: bind_indicator_array[%d]=SQL_NULL_DATA", i, i);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    curr->fvalueArray[i] = PyFloat_AsDouble(item);
                    curr->bind_indicator_array[i] = SQL_NTS;
                    snprintf(messageStr, sizeof(messageStr), "Processed list item index %d: fvalueArray[%d]=%f, bind_indicator_array[%d]=SQL_NTS",
                             i, i, curr->fvalueArray[i], i);
                    LogMsg(DEBUG, messageStr);
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_DOUBLE, curr->data_type,
                                  curr->param_size, curr->scale, curr->fvalueArray, 0, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, fvalueArray=%p, bind_indicator_array=%p, and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_DOUBLE, curr->data_type, curr->param_size, curr->scale, curr->fvalueArray, curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            curr->fvalue = PyFloat_AsDouble(bind_data);
            snprintf(messageStr, sizeof(messageStr), "Single float value: fvalue=%f", curr->fvalue);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_DOUBLE, curr->data_type, curr->param_size,
                                  curr->scale, &curr->fvalue, 0, NULL);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, c_type=%d, data_type=%d, param_size=%lu, scale=%d, fvalue=%f, and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_DOUBLE, curr->data_type, curr->param_size, curr->scale, curr->fvalue, rc);
            LogMsg(DEBUG, messageStr);
        }

        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }
        curr->data_type = SQL_C_DOUBLE;
        LogMsg(DEBUG, "Updated curr->data_type to SQL_C_DOUBLE");
        break;

    case PYTHON_UNICODE:
    {
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_UNICODE case with type=%d", type);
        LogMsg(DEBUG, messageStr);
        /* To Bind array of values */
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            int isNewBuffer = 0, param_size = 0;
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->uvalue = (SQLWCHAR *)ALLOC_N(SQLWCHAR, curr->param_size * (n));
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            memset(curr->uvalue, 0, sizeof(SQLWCHAR) * curr->param_size * n);

            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "Processing list index %d, item pointer: %p", i, (void *)item);
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    snprintf(messageStr, sizeof(messageStr), "Item at index %d is PYTHON_NIL: bind_indicator_array[%d]=SQL_NULL_DATA", i, i);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    if (PyObject_CheckBuffer(item) && (curr->data_type == SQL_BLOB ||
                                                       curr->data_type == SQL_BINARY ||
                                                       curr->data_type == SQL_VARBINARY))
                    {
#if PY_MAJOR_VERSION >= 3
                        Py_buffer tmp_buffer;
                        PyObject_GetBuffer(item, &tmp_buffer, PyBUF_SIMPLE);
                        tmp_uvalue = tmp_buffer.buf;
                        curr->ivalue = tmp_buffer.len;
#else
                        PyObject_AsReadBuffer(item, (const void **)&tmp_uvalue, &buffer_len);
                        curr->ivalue = buffer_len;
#endif
                        snprintf(messageStr, sizeof(messageStr), "Buffer check: tmp_uvalue=%p, buffer length=%d", (void *)tmp_uvalue, curr->ivalue);
                        LogMsg(DEBUG, messageStr);
                    }
                    else
                    {
                        tmp_uvalue = getUnicodeDataAsSQLWCHAR(item, &isNewBuffer);
                        curr->ivalue = PyUnicode_GetLength(item);
                        curr->ivalue = curr->ivalue * sizeof(SQLWCHAR);
                        snprintf(messageStr, sizeof(messageStr), "Unicode data: tmp_uvalue=%p, ivalue=%d", (void *)tmp_uvalue, curr->ivalue);
                        LogMsg(DEBUG, messageStr);
                    }
                    param_length = curr->ivalue;
                    if (curr->size != 0)
                    {
                        curr->ivalue = (curr->size + 1) * sizeof(SQLWCHAR);
                    }
                    snprintf(messageStr, sizeof(messageStr), "Parameter length adjustment: param_length=%d, curr->ivalue=%d", param_length, curr->ivalue);
                    LogMsg(DEBUG, messageStr);
                    if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                    {
                        if (curr->size == 0)
                        {
                            if ((curr->data_type == SQL_BLOB) || (curr->data_type == SQL_CLOB) || (curr->data_type == SQL_BINARY)
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                                || (curr->data_type == SQL_LONGVARBINARY)
#endif /* PASE */
                                || (curr->data_type == SQL_VARBINARY) || (curr->data_type == SQL_XML))
                            {
                                if (curr->ivalue <= curr->param_size)
                                {
                                    curr->ivalue = curr->param_size + sizeof(SQLWCHAR);
                                }
                            }
                            else
                            {
                                if (curr->ivalue <= (curr->param_size * sizeof(SQLWCHAR)))
                                {
                                    curr->ivalue = (curr->param_size + 1) * sizeof(SQLWCHAR);
                                }
                            }
                            snprintf(messageStr, sizeof(messageStr), "Adjusted ivalue based on SQL_PARAM_OUTPUT/SQL_PARAM_INPUT_OUTPUT: curr->ivalue=%d", curr->ivalue);
                            LogMsg(DEBUG, messageStr);
                        }
                    }

                    if (isNewBuffer == 0 || param_length <= curr->param_size)
                    {
                        dest_uvalue = &curr->uvalue[(curr->param_size / sizeof(SQLWCHAR)) * i];
                        memcpy(dest_uvalue, tmp_uvalue, param_length);
                        param_size = curr->param_size;
                    }
                    else if (curr->data_type == SQL_TYPE_TIMESTAMP || curr->data_type == SQL_TYPE_TIMESTAMP_WITH_TIMEZONE)
                    {
                        dest_uvalue = &curr->uvalue[(param_length / sizeof(SQLWCHAR)) * i];
                        memcpy(dest_uvalue, tmp_uvalue, param_length);
                        param_size = param_length;
                    }
                    PyMem_Del(tmp_uvalue);
                    snprintf(messageStr, sizeof(messageStr), "Memory copy results: dest_uvalue=%p, param_size=%d", (void *)dest_uvalue, param_size);
                    LogMsg(DEBUG, messageStr);
                    switch (curr->data_type)
                    {
                    case SQL_CLOB:
                    case SQL_DBCLOB:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_CLOB or SQL_DBCLOB: param_type=%d, data_type=%d", curr->param_type, curr->data_type);
                        LogMsg(DEBUG, messageStr);
                        if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                        {
                            curr->bind_indicator_array[i] = param_length;
                            paramValuePtr = (SQLPOINTER)curr->uvalue;
                            snprintf(messageStr, sizeof(messageStr), "SQL_PARAM_OUTPUT or SQL_PARAM_INPUT_OUTPUT: bind_indicator_array[%d]=%d, paramValuePtr=%p",
                                     i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        else
                        {
                            curr->bind_indicator_array[i] = curr->ivalue;
#ifndef PASE
                            paramValuePtr = (SQLPOINTER)(curr->uvalue);
#else
                            paramValuePtr = (SQLPOINTER) & (curr->uvalue);
#endif
                            snprintf(messageStr, sizeof(messageStr), "Non SQL_PARAM_OUTPUT/SQL_PARAM_INPUT_OUTPUT: bind_indicator_array[%d]=%d, paramValuePtr=%p",
                                     i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        valueType = SQL_C_WCHAR;
                        LogMsg(DEBUG, "Set valueType to SQL_C_WCHAR");
                        break;

                    case SQL_BLOB:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_BLOB: param_type=%d, data_type=%d", curr->param_type, curr->data_type);
                        LogMsg(DEBUG, messageStr);
                        if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                        {
                            curr->bind_indicator_array[i] = param_length;
                            paramValuePtr = (SQLPOINTER)curr->uvalue;
                            snprintf(messageStr, sizeof(messageStr), "SQL_PARAM_OUTPUT or SQL_PARAM_INPUT_OUTPUT: bind_indicator_array[%d]=%d, paramValuePtr=%p",
                                     i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        else
                        {
                            curr->bind_indicator_array[i] = curr->ivalue;
#ifndef PASE
                            paramValuePtr = (SQLPOINTER)(curr->uvalue);
#else
                            paramValuePtr = (SQLPOINTER) & (curr->uvalue);
#endif
                            snprintf(messageStr, sizeof(messageStr), "Non SQL_PARAM_OUTPUT/SQL_PARAM_INPUT_OUTPUT: bind_indicator_array[%d]=%d, paramValuePtr=%p",
                                     i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        valueType = SQL_C_BINARY;
                        LogMsg(DEBUG, "Set valueType to SQL_C_BINARY");
                        break;

                    case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                    case SQL_LONGVARBINARY:
#endif /* PASE */
                    case SQL_VARBINARY:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_BINARY, SQL_LONGVARBINARY or SQL_VARBINARY: param_length=%d", param_length);
                        LogMsg(DEBUG, messageStr);
                        /* account for bin_mode settings as well */
                        curr->bind_indicator_array[i] = param_length;
                        valueType = SQL_C_BINARY;
                        paramValuePtr = (SQLPOINTER)curr->uvalue;
                        snprintf(messageStr, sizeof(messageStr), "Set bind_indicator_array[%d]=%d, valueType=%d, paramValuePtr=%p",
                                 i, curr->bind_indicator_array[i], valueType, (void *)paramValuePtr);
                        LogMsg(DEBUG, messageStr);
                        break;

                    case SQL_XML:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_XML: param_length=%d", param_length);
                        LogMsg(DEBUG, messageStr);
                        curr->bind_indicator_array[i] = param_length;
                        paramValuePtr = (SQLPOINTER)curr->uvalue;
                        valueType = SQL_C_WCHAR;
                        snprintf(messageStr, sizeof(messageStr), "Set bind_indicator_array[%d]=%d, valueType=%d, paramValuePtr=%p",
                                 i, curr->bind_indicator_array[i], valueType, (void *)paramValuePtr);
                        LogMsg(DEBUG, messageStr);
                        break;

                    case SQL_TYPE_TIMESTAMP:
                    case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_TYPE_TIMESTAMP or SQL_TYPE_TIMESTAMP_WITH_TIMEZONE: param_length=%d", param_length);
                        LogMsg(DEBUG, messageStr);
                        valueType = SQL_C_WCHAR;
                        if (param_length == 0)
                        {
                            curr->bind_indicator_array[i] = SQL_NULL_DATA;
                        }
                        else
                        {
                            curr->bind_indicator_array[i] = param_length;
                        }
                        if (dest_uvalue[20] == 'T')
                        {
                            dest_uvalue[20] = ' ';
                        }
                        paramValuePtr = (SQLPOINTER)(curr->uvalue);
                        snprintf(messageStr, sizeof(messageStr), "Set bind_indicator_array[%d]=%d, valueType=%d, paramValuePtr=%p",
                                 i, curr->bind_indicator_array[i], valueType, (void *)paramValuePtr);
                        LogMsg(DEBUG, messageStr);
                        break;

                    default:
                        snprintf(messageStr, sizeof(messageStr), "Default case for data_type: %d", curr->data_type);
                        LogMsg(DEBUG, messageStr);
                        valueType = SQL_C_WCHAR;
                        curr->bind_indicator_array[i] = param_length;
                        paramValuePtr = (SQLPOINTER)(curr->uvalue);
                        snprintf(messageStr, sizeof(messageStr),
                                 "Set bind_indicator_array[%d]=%d, valueType=%d, paramValuePtr=%p", i, curr->bind_indicator_array[i], valueType, (void *)paramValuePtr);
                        LogMsg(DEBUG, messageStr);
                    }
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType,
                                  curr->data_type, curr->param_size, curr->scale, paramValuePtr,
                                  param_size, curr->bind_indicator_array);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, valueType=%d, data_type=%d, param_size=%d, scale=%d, paramValuePtr=%p, param_size=%d, bind_indicator_array=%p, and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, paramValuePtr, param_size, curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);

            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Updated curr->data_type to %d", curr->data_type);
            LogMsg(DEBUG, messageStr);
            curr->data_type = valueType;
        }
        else /* To bind scalar values */
        {
            int isNewBuffer = 0;
            snprintf(messageStr, sizeof(messageStr), "Binding scalar values: data_type=%d, param_type=%d", curr->data_type, curr->param_type);
            LogMsg(DEBUG, messageStr);
            if (PyObject_CheckBuffer(bind_data) && (curr->data_type == SQL_BLOB || curr->data_type == SQL_BINARY || curr->data_type == SQL_VARBINARY))
            {
#if PY_MAJOR_VERSION >= 3
                Py_buffer tmp_buffer;
                PyObject_GetBuffer(bind_data, &tmp_buffer, PyBUF_SIMPLE);
                curr->uvalue = tmp_buffer.buf;
                curr->ivalue = tmp_buffer.len;
                snprintf(messageStr, sizeof(messageStr), "buffer: uvalue=%p, ivalue=%d", (void *)curr->uvalue, curr->ivalue);
                LogMsg(DEBUG, messageStr);
#else
                PyObject_AsReadBuffer(bind_data, (const void **)&(curr->uvalue), &buffer_len);
                curr->ivalue = buffer_len;
                snprintf(messageStr, sizeof(messageStr), "Python 2 buffer: uvalue=%p, ivalue=%d", (void *)curr->uvalue, curr->ivalue);
                LogMsg(DEBUG, messageStr);
#endif
            }
            else
            {
                if (curr->uvalue != NULL)
                {
                    PyMem_Del(curr->uvalue);
                    curr->uvalue = NULL;
                }
                curr->uvalue = getUnicodeDataAsSQLWCHAR(bind_data, &isNewBuffer);
                curr->ivalue = PyUnicode_GetLength(bind_data);
                curr->ivalue = curr->ivalue * sizeof(SQLWCHAR);
                snprintf(messageStr, sizeof(messageStr), "New uvalue=%p, ivalue=%d", (void *)curr->uvalue, curr->ivalue);
                LogMsg(DEBUG, messageStr);
            }
            param_length = curr->ivalue;
            snprintf(messageStr, sizeof(messageStr), "Calculated param_length=%d", param_length);
            LogMsg(DEBUG, messageStr);
            if (curr->size != 0)
            {
                curr->ivalue = (curr->size + 1) * sizeof(SQLWCHAR);
                snprintf(messageStr, sizeof(messageStr), "Adjusted ivalue for size: %d", curr->ivalue);
                LogMsg(DEBUG, messageStr);
            }

            if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
            {
                if (curr->size == 0)
                {
                    if ((curr->data_type == SQL_BLOB) || (curr->data_type == SQL_CLOB) || (curr->data_type == SQL_BINARY)
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                        || (curr->data_type == SQL_LONGVARBINARY)
#endif /* PASE */
                        || (curr->data_type == SQL_VARBINARY) || (curr->data_type == SQL_XML))
                    {
                        if (curr->ivalue <= curr->param_size)
                        {
                            curr->ivalue = curr->param_size + sizeof(SQLWCHAR);
                            snprintf(messageStr, sizeof(messageStr), "Increased ivalue due to size adjustment: %d", curr->ivalue);
                            LogMsg(DEBUG, messageStr);
                        }
                    }
                    else
                    {
                        if (curr->ivalue <= (curr->param_size * sizeof(SQLWCHAR)))
                        {
                            curr->ivalue = (curr->param_size + 1) * sizeof(SQLWCHAR);
                            snprintf(messageStr, sizeof(messageStr), "Increased ivalue for other data types: %d", curr->ivalue);
                            LogMsg(DEBUG, messageStr);
                        }
                    }
                }
            }

            if (isNewBuffer == 0)
            {
                /* actually make a copy, since this will uvalue will be freed explicitly */
                SQLWCHAR *tmp = (SQLWCHAR *)ALLOC_N(SQLWCHAR, curr->ivalue + 1);
                memcpy(tmp, curr->uvalue, (param_length + sizeof(SQLWCHAR)));
                curr->uvalue = tmp;
                snprintf(messageStr, sizeof(messageStr), "Copied uvalue to new buffer, tmp=%p", (void *)tmp);
                LogMsg(DEBUG, messageStr);
            }
            else if (param_length <= curr->param_size)
            {
                SQLWCHAR *tmp = (SQLWCHAR *)ALLOC_N(SQLWCHAR, curr->ivalue + 1);
                memcpy(tmp, curr->uvalue, (param_length + sizeof(SQLWCHAR)));
                PyMem_Del(curr->uvalue);
                curr->uvalue = tmp;
                snprintf(messageStr, sizeof(messageStr), "Copied uvalue to new buffer with size adjustment, tmp=%p", (void *)tmp);
                LogMsg(DEBUG, messageStr);
            }

            switch (curr->data_type)
            {
            case SQL_CLOB:
            case SQL_DBCLOB:
                snprintf(messageStr, sizeof(messageStr), "Processing SQL_CLOB or SQL_DBCLOB: param_type=%d", curr->param_type);
                LogMsg(DEBUG, messageStr);
                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                {
                    curr->bind_indicator = param_length;
                    paramValuePtr = (SQLPOINTER)curr->uvalue;
                    snprintf(messageStr, sizeof(messageStr), "SQL_PARAM_OUTPUT/SQL_PARAM_INPUT_OUTPUT: bind_indicator=%d, paramValuePtr=%p", curr->bind_indicator, (void *)paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    curr->bind_indicator = SQL_DATA_AT_EXEC;
#ifndef PASE
                    paramValuePtr = (SQLPOINTER)(curr);
#else
                    paramValuePtr = (SQLPOINTER) & (curr);
#endif
                    snprintf(messageStr, sizeof(messageStr), "Non SQL_PARAM_OUTPUT/SQL_PARAM_INPUT_OUTPUT: bind_indicator=%d, paramValuePtr=%p",
                             curr->bind_indicator, (void *)paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                valueType = SQL_C_WCHAR;
                LogMsg(DEBUG, "Set valueType to SQL_C_WCHAR");
                break;

            case SQL_BLOB:
                snprintf(messageStr, sizeof(messageStr), "Handling SQL_BLOB: param_type=%d, param_length=%d", curr->param_type, param_length);
                LogMsg(DEBUG, messageStr);
                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                {
                    curr->bind_indicator = param_length;
                    paramValuePtr = (SQLPOINTER)curr;
                    snprintf(messageStr, sizeof(messageStr), "SQL_PARAM_OUTPUT/SQL_PARAM_INPUT_OUTPUT: bind_indicator=%d, paramValuePtr=%p",
                             curr->bind_indicator, (void *)paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    curr->bind_indicator = SQL_DATA_AT_EXEC;
#ifndef PASE
                    paramValuePtr = (SQLPOINTER)(curr);
#else
                    paramValuePtr = (SQLPOINTER) & (curr);
#endif
                    snprintf(messageStr, sizeof(messageStr), "Non SQL_PARAM_OUTPUT/SQL_PARAM_INPUT_OUTPUT: bind_indicator=%d, paramValuePtr=%p", curr->bind_indicator, (void *)paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                valueType = SQL_C_BINARY;
                LogMsg(DEBUG, "Set valueType to SQL_C_BINARY");
                break;

            case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
            case SQL_LONGVARBINARY:
#endif /* PASE */
            case SQL_VARBINARY:
                snprintf(messageStr, sizeof(messageStr), "Handling SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY: param_length=%d", param_length);
                LogMsg(DEBUG, messageStr);
                /* account for bin_mode settings as well */
                curr->bind_indicator = param_length;
                valueType = SQL_C_BINARY;
                paramValuePtr = (SQLPOINTER)curr->uvalue;
                snprintf(messageStr, sizeof(messageStr), "SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY: bind_indicator=%d, paramValuePtr=%p",
                         curr->bind_indicator, (void *)paramValuePtr);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_XML:
                snprintf(messageStr, sizeof(messageStr), "Handling SQL_XML: param_length=%d", param_length);
                LogMsg(DEBUG, messageStr);
                curr->bind_indicator = param_length;
                paramValuePtr = (SQLPOINTER)curr->uvalue;
                valueType = SQL_C_WCHAR;
                snprintf(messageStr, sizeof(messageStr), "SQL_XML: bind_indicator=%d, paramValuePtr=%p, valueType=%d", curr->bind_indicator, (void *)paramValuePtr, valueType);
                LogMsg(DEBUG, messageStr);
                break;
            case SQL_TYPE_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
                snprintf(messageStr, sizeof(messageStr), "Handling SQL_TYPE_TIMESTAMP or SQL_TYPE_TIMESTAMP_WITH_TIMEZONE: param_length=%d, uvalue[10]=%c", param_length, curr->uvalue[10]);
                LogMsg(DEBUG, messageStr);
                valueType = SQL_C_WCHAR;
                if (param_length == 0)
                {
                    curr->bind_indicator = SQL_NULL_DATA;
                    snprintf(messageStr, sizeof(messageStr), "SQL_TYPE_TIMESTAMP with zero param_length: bind_indicator=%d", curr->bind_indicator);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    curr->bind_indicator = param_length;
                    snprintf(messageStr, sizeof(messageStr), "SQL_TYPE_TIMESTAMP: bind_indicator=%d", curr->bind_indicator);
                    LogMsg(DEBUG, messageStr);
                }
                if (curr->uvalue[10] == 'T')
                {
                    curr->uvalue[10] = ' ';
                }
                paramValuePtr = (SQLPOINTER)(curr->uvalue);
                snprintf(messageStr, sizeof(messageStr), "SQL_TYPE_TIMESTAMP: paramValuePtr=%p", (void *)paramValuePtr);
                LogMsg(DEBUG, messageStr);
                break;
            default:
                snprintf(messageStr, sizeof(messageStr), "Handling default case: param_length=%d", param_length);
                LogMsg(DEBUG, messageStr);
                valueType = SQL_C_WCHAR;
                curr->bind_indicator = param_length;
                paramValuePtr = (SQLPOINTER)(curr->uvalue);
                snprintf(messageStr, sizeof(messageStr), "Default case: bind_indicator=%d, paramValuePtr=%p, valueType=%d", curr->bind_indicator, (void *)paramValuePtr, valueType);
                LogMsg(DEBUG, messageStr);
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, paramValuePtr, curr->ivalue, &(curr->bind_indicator));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter: param_num=%d, param_type=%d, valueType=%d, data_type=%d, param_size=%d, scale=%d, paramValuePtr=%p, ivalue=%d, bind_indicator=%d and returned rc=%d",
                     curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, paramValuePtr, curr->ivalue, curr->bind_indicator, rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            curr->data_type = valueType;
            snprintf(messageStr, sizeof(messageStr), "Updated curr->data_type to %d", curr->data_type);
            LogMsg(DEBUG, messageStr);
        }
    }
    break;

    case PYTHON_STRING:
    {
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_STRING: TYPE(bind_data)=%d, curr->param_size=%d", TYPE(bind_data), curr->param_size);
        LogMsg(DEBUG, messageStr);
        /* To Bind array of values */
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "Binding array of values: n=%zd", n);
            LogMsg(DEBUG, messageStr);
            curr->svalue = (char *)ALLOC_N(char, curr->param_size *(n));
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            memset(curr->svalue, 0, curr->param_size * n);

            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "Processing list item %d: TYPE(item)=%d", i, TYPE(item));
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                }
                else
                {
                    if (PyObject_CheckBuffer(item) && (curr->data_type == SQL_BLOB ||
                                                       curr->data_type == SQL_BINARY ||
                                                       curr->data_type == SQL_VARBINARY))
                    {
#if PY_MAJOR_VERSION >= 3
                        Py_buffer tmp_buffer;
                        PyObject_GetBuffer(item, &tmp_buffer, PyBUF_SIMPLE);
                        tmp_svalue = tmp_buffer.buf;
                        curr->ivalue = tmp_buffer.len;
#else
                        PyObject_AsReadBuffer(item, (const void **)&tmp_svalue, &buffer_len);
                        curr->ivalue = buffer_len;
#endif
                    }
                    else
                    {
                        tmp_svalue = PyBytes_AsString(item); /** It is PyString_AsString() in PY_MAJOR_VERSION<3, and code execution will not come here in PY_MAJOR_VERSION>=3 **/
                        curr->ivalue = strlen(tmp_svalue);
                    }
                    param_length = curr->ivalue;
                    snprintf(messageStr, sizeof(messageStr), "Item %d: tmp_svalue=%p, tmp_svalue_len=%d, param_length=%d", i, (void *)tmp_svalue, curr->ivalue, param_length);
                    LogMsg(DEBUG, messageStr);
                    /*
                     * * An extra parameter is given by the client to pick the size of the
                     * * string returned. The string is then truncate past that size.
                     * * If no size is given then use BUFSIZ to return the string.
                     * */
                    if (curr->size != 0)
                    {
                        curr->ivalue = curr->size;
                    }

                    if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                    {
                        if (curr->size == 0)
                        {
                            if (curr->ivalue <= curr->param_size)
                            {
                                curr->ivalue = curr->param_size + 1;
                            }
                        }
                    }

                    dest_svalue = &curr->svalue[0] + (curr->param_size * i);
                    dest_svalue = memcpy(dest_svalue, tmp_svalue, param_length);
                    snprintf(messageStr, sizeof(messageStr), "Item %d: dest_svalue=%p, valueType=%d", i, (void *)dest_svalue, valueType);
                    LogMsg(DEBUG, messageStr);
                    switch (curr->data_type)
                    {
                    case SQL_CLOB:
                    case SQL_DBCLOB:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_CLOB/SQL_DBCLOB case: curr->param_type=%d, curr->data_type=%d, curr->ivalue=%d, param_length=%d", curr->param_type, curr->data_type, curr->ivalue, param_length);
                        LogMsg(DEBUG, messageStr);
                        if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                        {
                            curr->bind_indicator_array[i] = param_length;
                            paramValuePtr = (SQLPOINTER)curr->svalue;
                            snprintf(messageStr, sizeof(messageStr), "For OUTPUT/INPUT_OUTPUT: curr->bind_indicator_array[%d]=%d, paramValuePtr=%p", i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        else
                        {
                            curr->bind_indicator_array[i] = curr->ivalue;
                            /* The correct dataPtr will be set during SQLPutData with
                             * * the len from this struct
                             * */
#ifndef PASE
                            paramValuePtr = (SQLPOINTER)(curr->svalue);
#else
                            paramValuePtr = (SQLPOINTER) & (curr->svalue);
#endif
                            snprintf(messageStr, sizeof(messageStr), "For other param_type: curr->bind_indicator_array[%d]=%d, paramValuePtr=%p", i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        valueType = SQL_C_CHAR;
                        snprintf(messageStr, sizeof(messageStr), "updated valueType to %d", valueType);
                        LogMsg(DEBUG, messageStr);
                        break;

                    case SQL_BLOB:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_BLOB case: curr->param_type=%d, curr->data_type=%d, curr->ivalue=%d, param_length=%d", curr->param_type, curr->data_type, curr->ivalue, param_length);
                        LogMsg(DEBUG, messageStr);
                        if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                        {
                            curr->ivalue = curr->ivalue - 1;
                            curr->bind_indicator_array[i] = param_length;
                            paramValuePtr = (SQLPOINTER)curr->svalue;
                            snprintf(messageStr, sizeof(messageStr), "For OUTPUT/INPUT_OUTPUT: curr->ivalue=%d, curr->bind_indicator_array[%d]=%d, paramValuePtr=%p",
                                     curr->ivalue, i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        else
                        {
                            curr->bind_indicator_array[i] = curr->ivalue;
#ifndef PASE
                            paramValuePtr = (SQLPOINTER)(curr->svalue);
#else
                            paramValuePtr = (SQLPOINTER) & (curr->svalue);
#endif
                            snprintf(messageStr, sizeof(messageStr), "For other param_type: curr->bind_indicator_array[%d]=%d, paramValuePtr=%p", i, curr->bind_indicator_array[i], (void *)paramValuePtr);
                            LogMsg(DEBUG, messageStr);
                        }
                        valueType = SQL_C_BINARY;
                        snprintf(messageStr, sizeof(messageStr), "Updated valueType to %d", valueType);
                        LogMsg(DEBUG, messageStr);
                        break;

                    case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                    case SQL_LONGVARBINARY:
#endif /* PASE */
                    case SQL_VARBINARY:
                    case SQL_XML:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY/SQL_XML case: curr->param_type=%d, curr->data_type=%d, curr->ivalue=%d, param_length=%d",
                                 curr->param_type, curr->data_type, curr->ivalue, param_length);
                        LogMsg(DEBUG, messageStr);
                        /* account for bin_mode settings as well */
                        curr->bind_indicator_array[i] = curr->ivalue;
                        if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                        {
                            curr->ivalue = curr->ivalue - 1;
                            curr->bind_indicator_array[i] = param_length;
                            snprintf(messageStr, sizeof(messageStr), "For OUTPUT/INPUT_OUTPUT: curr->ivalue=%d, curr->bind_indicator_array[%d]=%d", curr->ivalue, i, curr->bind_indicator_array[i]);
                            LogMsg(DEBUG, messageStr);
                        }

                        valueType = SQL_C_BINARY;
                        paramValuePtr = (SQLPOINTER)curr->svalue;
                        snprintf(messageStr, sizeof(messageStr), "Updated SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY/SQL_XML case: valueType=%d, paramValuePtr=%p",
                                 valueType, (void *)paramValuePtr);
                        LogMsg(DEBUG, messageStr);
                        break;

                        /* This option should handle most other types such as DATE,
                         * * VARCHAR etc
                         * */
                    case SQL_TYPE_TIMESTAMP:
                    case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
                        snprintf(messageStr, sizeof(messageStr), "Processing SQL_TYPE_TIMESTAMP or SQL_TYPE_TIMESTAMP_WITH_TIMEZONE case: curr->param_type=%d, curr->data_type=%d, curr->ivalue=%d, param_length=%d",
                                 curr->param_type, curr->data_type, curr->ivalue, param_length);
                        LogMsg(DEBUG, messageStr);
                        valueType = SQL_C_CHAR;
                        curr->bind_indicator_array[i] = curr->ivalue;
                        if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                        {
                            if (param_length == 0)
                            {
                                curr->bind_indicator_array[i] = SQL_NULL_DATA;
                            }
                            else
                            {
                                curr->bind_indicator_array[i] = SQL_NTS;
                            }
                            snprintf(messageStr, sizeof(messageStr), "For OUTPUT/INPUT_OUTPUT: curr->bind_indicator_array[%d]=%d", i, curr->bind_indicator_array[i]);
                            LogMsg(DEBUG, messageStr);
                        }
                        if (dest_svalue[10] == 'T')
                        {
                            dest_svalue[10] = ' ';
                        }
                        paramValuePtr = (SQLPOINTER)(curr->svalue);
                        snprintf(messageStr, sizeof(messageStr), "updated SQL_TYPE_TIMESTAMP case: valueType to %d, paramValuePtr to %p, dest_svalue[10]=%c",
                                 valueType, (void *)paramValuePtr, dest_svalue[10]);
                        LogMsg(DEBUG, messageStr);
                        break;

                    default:
                        snprintf(messageStr, sizeof(messageStr), "Processing default case: curr->data_type=%d, curr->param_type=%d, curr->ivalue=%d",
                                 curr->data_type, curr->param_type, curr->ivalue);
                        LogMsg(DEBUG, messageStr);
                        valueType = SQL_C_CHAR;
                        curr->bind_indicator_array[i] = curr->ivalue;
                        if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                        {
                            curr->bind_indicator_array[i] = SQL_NTS;
                        }
                        paramValuePtr = (SQLPOINTER)(curr->svalue);
                        snprintf(messageStr, sizeof(messageStr), "Exiting default case: valueType=%d, paramValuePtr=%p", valueType, (void *)paramValuePtr);
                        LogMsg(DEBUG, messageStr);
                    }
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, valueType, curr->data_type, curr->param_size,
                                  curr->scale, paramValuePtr, curr->param_size, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLBindParameter called with parameters: stmt_handle=%p, param_num=%d, param_type=%d, valueType=%d, data_type=%d, param_size=%d, scale=%d, paramValuePtr=%p, param_size=%d, bind_indicator_array[0]=%d, and returned rc=%d",
                     (void *)stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type,
                     curr->param_size, curr->scale, (void *)paramValuePtr, curr->param_size, curr->bind_indicator_array[0], rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            curr->data_type = valueType;
            snprintf(messageStr, sizeof(messageStr), "Updated curr->data_type: valueType=%d", valueType);
            LogMsg(DEBUG, messageStr);
        }
        else /* To bind scalar values */
        {
            char *tmp;
            snprintf(messageStr, sizeof(messageStr), "Binding scalar values: bind_data=%p, curr->data_type=%d", bind_data, curr->data_type);
            LogMsg(DEBUG, messageStr);
            if (PyObject_CheckBuffer(bind_data) && (curr->data_type == SQL_BLOB ||
                                                    curr->data_type == SQL_BINARY ||
                                                    curr->data_type == SQL_VARBINARY))
            {
#if PY_MAJOR_VERSION >= 3
                Py_buffer tmp_buffer;
                PyObject_GetBuffer(bind_data, &tmp_buffer, PyBUF_SIMPLE);
                curr->svalue = tmp_buffer.buf;
                curr->ivalue = tmp_buffer.len;
#else
                PyObject_AsReadBuffer(bind_data, (const void **)&(curr->svalue), &buffer_len);
                curr->ivalue = buffer_len;
#endif
            }
            else
            {
                if (curr->svalue != NULL)
                {
                    PyMem_Del(curr->svalue);
                    curr->svalue = NULL;
                }
                curr->svalue = PyBytes_AsString(bind_data); /** It is PyString_AsString() in PY_MAJOR_VERSION<3, and code execution will not come here in PY_MAJOR_VERSION>=3 **/
                curr->ivalue = strlen(curr->svalue);
            }
            param_length = curr->ivalue;
            snprintf(messageStr, sizeof(messageStr), "param_length=%d, curr->size=%d", param_length, curr->size);
            LogMsg(DEBUG, messageStr);
            /*
             * An extra parameter is given by the client to pick the size of the
             * string returned. The string is then truncate past that size.
             * If no size is given then use BUFSIZ to return the string.
             */
            if (curr->size != 0)
            {
                curr->ivalue = curr->size;
            }

            if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
            {
                if (curr->size == 0)
                {
                    if (curr->ivalue <= curr->param_size)
                    {
                        curr->ivalue = curr->param_size + 1;
                    }
                }
            }

            tmp = ALLOC_N(char, curr->ivalue + 1);
            memset(tmp, 0, curr->ivalue + 1);
            curr->svalue = memcpy(tmp, curr->svalue, param_length);
            curr->svalue[param_length] = '\0';
            snprintf(messageStr, sizeof(messageStr), "After allocation: curr->svalue=%p, curr->ivalue=%d", curr->svalue, curr->ivalue);
            LogMsg(DEBUG, messageStr);
            switch (curr->data_type)
            {
            case SQL_CLOB:
            case SQL_DBCLOB:
                snprintf(messageStr, sizeof(messageStr), "Data type: SQL_CLOB or SQL_DBCLOB. param_type=%d, param_length=%d, curr->svalue=%p", curr->param_type, param_length, curr->svalue);
                LogMsg(DEBUG, messageStr);
                if (curr->param_type == SQL_PARAM_OUTPUT ||
                    curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                {
                    curr->bind_indicator = param_length;
                    paramValuePtr = (SQLPOINTER)curr->svalue;
                    snprintf(messageStr, sizeof(messageStr), "Binding for SQL_CLOB or SQL_DBCLOB. bind_indicator=%d, paramValuePtr=%p", curr->bind_indicator, paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    curr->bind_indicator = SQL_DATA_AT_EXEC;
                    /* The correct dataPtr will be set during SQLPutData with
                     * the len from this struct
                     */
#ifndef PASE
                    paramValuePtr = (SQLPOINTER)(curr);
#else
                    paramValuePtr = (SQLPOINTER) & (curr);
#endif
                    snprintf(messageStr, sizeof(messageStr), "Binding for SQL_CLOB or SQL_DBCLOB at exec. bind_indicator=%d, paramValuePtr=%p", curr->bind_indicator, paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                valueType = SQL_C_CHAR;
                LogMsg(DEBUG, "updated valueType to SQL_C_CHAR");
                break;

            case SQL_BLOB:
                snprintf(messageStr, sizeof(messageStr), "Data type: SQL_BLOB. param_type=%d, curr->ivalue=%d", curr->param_type, curr->ivalue);
                LogMsg(DEBUG, messageStr);
                if (curr->param_type == SQL_PARAM_OUTPUT ||
                    curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                {
                    curr->ivalue = curr->ivalue - 1;
                    curr->bind_indicator = param_length;
                    paramValuePtr = (SQLPOINTER)curr->svalue;
                    snprintf(messageStr, sizeof(messageStr), "Binding for SQL_BLOB. ivalue=%d, bind_indicator=%d, paramValuePtr=%p", curr->ivalue, curr->bind_indicator, paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    curr->bind_indicator = SQL_DATA_AT_EXEC;
#ifndef PASE
                    paramValuePtr = (SQLPOINTER)(curr);
#else
                    paramValuePtr = (SQLPOINTER) & (curr);
#endif
                    snprintf(messageStr, sizeof(messageStr), "Binding for SQL_BLOB at exec. bind_indicator=%d, paramValuePtr=%p", curr->bind_indicator, paramValuePtr);
                    LogMsg(DEBUG, messageStr);
                }
                valueType = SQL_C_BINARY;
                LogMsg(DEBUG, "updated valueType to SQL_C_BINARY");
                break;

            case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
            case SQL_LONGVARBINARY:
#endif /* PASE */
            case SQL_VARBINARY:
            case SQL_XML:
                snprintf(messageStr, sizeof(messageStr), "Data type: SQL_BINARY, SQL_LONGVARBINARY, SQL_VARBINARY, or SQL_XML. param_type=%d, ivalue=%d", curr->param_type, curr->ivalue);
                LogMsg(DEBUG, messageStr);
                /* account for bin_mode settings as well */
                curr->bind_indicator = curr->ivalue;
                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                {
                    curr->ivalue = curr->ivalue - 1;
                    curr->bind_indicator = param_length;
                    snprintf(messageStr, sizeof(messageStr), "Binding for SQL_BINARY or similar. ivalue=%d, bind_indicator=%d", curr->ivalue, curr->bind_indicator);
                    LogMsg(DEBUG, messageStr);
                }

                valueType = SQL_C_BINARY;
                paramValuePtr = (SQLPOINTER)curr->svalue;
                snprintf(messageStr, sizeof(messageStr), "Value type set to SQL_C_BINARY. valueType=%d, paramValuePtr=%p", valueType, paramValuePtr);
                LogMsg(DEBUG, messageStr);
                break;

                /* This option should handle most other types such as DATE,
                 * VARCHAR etc
                 */
            case SQL_TYPE_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
                snprintf(messageStr, sizeof(messageStr), "Data type: SQL_TYPE_TIMESTAMP or SQL_TYPE_TIMESTAMP_WITH_TIMEZONE. param_type=%d, param_length=%d, svalue[10]=%c", curr->param_type, param_length, curr->svalue[10]);
                LogMsg(DEBUG, messageStr);
                valueType = SQL_C_CHAR;
                curr->bind_indicator = curr->ivalue;
                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                {
                    if (param_length == 0)
                    {
                        curr->bind_indicator = SQL_NULL_DATA;
                    }
                    else
                    {
                        curr->bind_indicator = SQL_NTS;
                    }
                    snprintf(messageStr, sizeof(messageStr), "Binding for SQL_TYPE_TIMESTAMP or SQL_TYPE_TIMESTAMP_WITH_TIMEZONE. bind_indicator=%d", curr->bind_indicator);
                    LogMsg(DEBUG, messageStr);
                }
                if (curr->svalue[10] == 'T')
                {
                    curr->svalue[10] = ' ';
                }
                paramValuePtr = (SQLPOINTER)(curr->svalue);
                snprintf(messageStr, sizeof(messageStr), "Value type set to SQL_C_CHAR. valueType=%d, paramValuePtr=%p", valueType, paramValuePtr);
                LogMsg(DEBUG, messageStr);
                break;
            default:
                valueType = SQL_C_CHAR;
                snprintf(messageStr, sizeof(messageStr), "Default case: data_type=%d", curr->data_type);
                LogMsg(DEBUG, messageStr);
                curr->bind_indicator = curr->ivalue;
                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                {
                    curr->bind_indicator = SQL_NTS;
                    snprintf(messageStr, sizeof(messageStr), "Default binding. bind_indicator=%d", curr->bind_indicator);
                    LogMsg(DEBUG, messageStr);
                }
                paramValuePtr = (SQLPOINTER)(curr->svalue);
                snprintf(messageStr, sizeof(messageStr), "Value type set to SQL_C_CHAR. valueType=%d, paramValuePtr=%p", valueType, paramValuePtr);
                LogMsg(DEBUG, messageStr);
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, valueType, curr->data_type, curr->param_size,
                                  curr->scale, paramValuePtr, curr->ivalue, &(curr->bind_indicator));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter with parameters: hstmt=%p, param_num=%d, param_type=%d, valueType=%d, data_type=%d, param_size=%d, scale=%d, paramValuePtr=%p, ivalue=%d, bind_indicator=%p and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, paramValuePtr, curr->ivalue, &(curr->bind_indicator), rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            curr->data_type = valueType;
            snprintf(messageStr, sizeof(messageStr), "Updated curr->data_type to valueType. New data_type=%d", curr->data_type);
            LogMsg(DEBUG, messageStr);
        }
    }
    break;

    case PYTHON_DECIMAL:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_DECIMAL case with current data_type=%d", curr->data_type);
        LogMsg(DEBUG, messageStr);
        if (curr->data_type == SQL_DECIMAL || curr->data_type == SQL_DECFLOAT || curr->data_type == SQL_BIGINT || curr->data_type == SQL_FLOAT || curr->data_type == SQL_DOUBLE)
        {
            if (TYPE(bind_data) == PYTHON_LIST)
            {
                char *svalue = NULL;
                Py_ssize_t n = PyList_Size(bind_data);
                int max_precn = 0;

                if (curr->data_type == SQL_DECIMAL)
                    max_precn = MAX_PRECISION;
                else // SQL_DECFLOAT
                    max_precn = curr->param_size;

                snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd, max_precision=%d", n, max_precn);
                LogMsg(DEBUG, messageStr);
                if (curr->svalue != NULL)
                {
                    PyMem_Del(curr->svalue);
                    curr->svalue = NULL;
                }

                curr->svalue = (char *)ALLOC_N(char, max_precn *n);
                curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
                memset(curr->svalue, 0, max_precn * n);

                for (i = 0; i < n; i++)
                {
                    PyObject *tempobj = NULL;
#if PY_MAJOR_VERSION >= 3
                    PyObject *tempobj2 = NULL;
#endif
                    item = PyList_GetItem(bind_data, i);
                    snprintf(messageStr, sizeof(messageStr), "Processing list item index %d, item pointer: %p", i, (void *)item);
                    LogMsg(DEBUG, messageStr);
                    if (TYPE(item) == PYTHON_NIL)
                    {
                        curr->bind_indicator_array[i] = SQL_NULL_DATA;
                        snprintf(messageStr, sizeof(messageStr), "List item index %d is PYTHON_NIL: bind_indicator_array[%d]=SQL_NULL_DATA", i, i);
                        LogMsg(DEBUG, messageStr);
                    }
                    else
                    {
                        tempobj = PyObject_Str(item);
#if PY_MAJOR_VERSION >= 3
                        tempobj2 = PyUnicode_AsASCIIString(tempobj);
                        Py_XDECREF(tempobj);
                        tempobj = tempobj2;
#endif
                        svalue = PyBytes_AsString(tempobj);
                        curr->ivalue = strlen(svalue);
                        memcpy(curr->svalue + (i * max_precn), svalue, curr->ivalue);
                        valueType = SQL_C_CHAR;
                        paramValuePtr = (SQLPOINTER)(curr->svalue);
                        curr->bind_indicator_array[i] = curr->ivalue;
                        snprintf(messageStr, sizeof(messageStr), "Processed list item index %d: svalue=%s, ivalue=%d, bind_indicator_array[%d]=%d", i, svalue, curr->ivalue, i, curr->bind_indicator_array[i]);
                        LogMsg(DEBUG, messageStr);
                        Py_XDECREF(tempobj);
                    }
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType,
                                      curr->data_type, curr->param_size, curr->scale,
                                      paramValuePtr, max_precn, curr->bind_indicator_array);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for PYTHON_LIST with parameters: hstmt=%p, param_num=%d, param_type=%d, valueType=%d, data_type=%d, param_size=%d, scale=%d, paramValuePtr=%p, max_precn=%d, bind_indicator_array=%p and returned rc=%d",
                         stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, paramValuePtr, max_precn, curr->bind_indicator_array, rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
                {
                    _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                }
                curr->data_type = valueType;
            }
            else /* To bind scalar values */
            {
                PyObject *tempobj = NULL;
#if PY_MAJOR_VERSION >= 3
                PyObject *tempobj2 = NULL;
#endif
                if (curr->svalue != NULL)
                {
                    PyMem_Del(curr->svalue);
                    curr->svalue = NULL;
                }
                tempobj = PyObject_Str(bind_data);
#if PY_MAJOR_VERSION >= 3
                tempobj2 = PyUnicode_AsASCIIString(tempobj);
                Py_XDECREF(tempobj);
                tempobj = tempobj2;
#endif
                curr->svalue = PyBytes_AsString(tempobj);
                curr->ivalue = strlen(curr->svalue);
                curr->svalue = estrdup(curr->svalue);
                curr->svalue[curr->ivalue] = '\0';
                valueType = SQL_C_CHAR;
                paramValuePtr = (SQLPOINTER)(curr->svalue);
                curr->bind_indicator = curr->ivalue;
                snprintf(messageStr, sizeof(messageStr), "Binding scalar value: svalue=%s, ivalue=%d, bind_indicator=%d", curr->svalue, curr->ivalue, curr->bind_indicator);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType,
                                      curr->data_type, curr->param_size, curr->scale,
                                      paramValuePtr, curr->ivalue, &(curr->bind_indicator));
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for scalar value with parameters: hstmt=%p, param_num=%d, param_type=%d, valueType=%d, data_type=%d, param_size=%d, scale=%d, paramValuePtr=%p, ivalue=%d, bind_indicator=%p, and returned rc=%d",
                         stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, paramValuePtr, curr->ivalue, &(curr->bind_indicator), rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
                {
                    _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                }
                curr->data_type = valueType;
                Py_XDECREF(tempobj);
            }
        }
        break;

    case PYTHON_DATE:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_DATE case. Current data_type=%d", curr->data_type);
        LogMsg(DEBUG, messageStr);
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->date_value = ALLOC_N(DATE_STRUCT, n);
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "Processing list item index %d, item pointer: %p", i, (void *)item);
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d is PYTHON_NIL: bind_indicator_array[%d]=SQL_NULL_DATA", i, i);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    (curr->date_value + i)->year = PyDateTime_GET_YEAR(item);
                    (curr->date_value + i)->month = PyDateTime_GET_MONTH(item);
                    (curr->date_value + i)->day = PyDateTime_GET_DAY(item);
                    curr->bind_indicator_array[i] = SQL_NTS;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d: year=%d, month=%d, day=%d, bind_indicator_array[%d]=SQL_NTS",
                             i, (curr->date_value + i)->year, (curr->date_value + i)->month, (curr->date_value + i)->day, i);
                    LogMsg(DEBUG, messageStr);
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_TYPE_DATE, curr->data_type, curr->param_size,
                                  curr->scale, curr->date_value, curr->ivalue, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for PYTHON_LIST with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, date_value=%p, ivalue=%d, bind_indicator_array=%p and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale, curr->date_value, curr->ivalue, curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            curr->date_value = ALLOC(DATE_STRUCT);
            curr->date_value->year = PyDateTime_GET_YEAR(bind_data);
            curr->date_value->month = PyDateTime_GET_MONTH(bind_data);
            curr->date_value->day = PyDateTime_GET_DAY(bind_data);
            snprintf(messageStr, sizeof(messageStr), "Binding scalar value: year=%d, month=%d, day=%d",
                     curr->date_value->year, curr->date_value->month, curr->date_value->day);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_TYPE_DATE, curr->data_type, curr->param_size,
                                  curr->scale, curr->date_value, curr->ivalue, &(curr->bind_indicator));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for scalar value with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, date_value=%p, ivalue=%d, bind_indicator=%p and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale, curr->date_value, curr->ivalue, &(curr->bind_indicator), rc);
            LogMsg(DEBUG, messageStr);
        }
        break;

    case PYTHON_TIME:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_TIME case. Current data_type=%d", curr->data_type);
        LogMsg(DEBUG, messageStr);
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->time_value = ALLOC_N(TIME_STRUCT, n);
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "Processing list item index %d, item pointer: %p", i, (void *)item);
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d is PYTHON_NIL: bind_indicator_array[%d]=SQL_NULL_DATA", i, i);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    (curr->time_value + i)->hour = PyDateTime_TIME_GET_HOUR(item);
                    (curr->time_value + i)->minute = PyDateTime_TIME_GET_MINUTE(item);
                    (curr->time_value + i)->second = PyDateTime_TIME_GET_SECOND(item);
                    curr->bind_indicator_array[i] = SQL_NTS;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d: hour=%d, minute=%d, second=%d, bind_indicator_array[%d]=SQL_NTS", i, (curr->time_value + i)->hour, (curr->time_value + i)->minute, (curr->time_value + i)->second, i);
                    LogMsg(DEBUG, messageStr);
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_TYPE_TIME, curr->data_type, curr->param_size,
                                  curr->scale, curr->time_value, curr->ivalue, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for PYTHON_LIST with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, time_value=%p, ivalue=%d, bind_indicator_array=%p and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale, curr->time_value, curr->ivalue, curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            curr->time_value = ALLOC(TIME_STRUCT);
            curr->time_value->hour = PyDateTime_TIME_GET_HOUR(bind_data);
            curr->time_value->minute = PyDateTime_TIME_GET_MINUTE(bind_data);
            curr->time_value->second = PyDateTime_TIME_GET_SECOND(bind_data);
            snprintf(messageStr, sizeof(messageStr), "Binding scalar value: hour=%d, minute=%d, second=%d",
                     curr->time_value->hour, curr->time_value->minute, curr->time_value->second);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_TYPE_TIME, curr->data_type, curr->param_size,
                                  curr->scale, curr->time_value, curr->ivalue, &(curr->bind_indicator));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for scalar value with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, time_value=%p, ivalue=%d, bind_indicator=%p and returned rc=%d",
                     stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale, curr->time_value, curr->ivalue, &(curr->bind_indicator), rc);
            LogMsg(DEBUG, messageStr);
        }
        break;

    case PYTHON_TIMESTAMP:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_TIMESTAMP case. Current data_type=%d", curr->data_type);
        LogMsg(DEBUG, messageStr);
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->ts_value = ALLOC_N(TIMESTAMP_STRUCT, n);
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);

            for (i = 0; i < n; i++)
            {
                item = PyList_GetItem(bind_data, i);
                snprintf(messageStr, sizeof(messageStr), "Processing list item index %d, item pointer: %p", i, (void *)item);
                LogMsg(DEBUG, messageStr);
                if (TYPE(item) == PYTHON_NIL)
                {
                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d is PYTHON_NIL: bind_indicator_array[%d]=SQL_NULL_DATA", i, i);
                    LogMsg(DEBUG, messageStr);
                }
                else
                {
                    (curr->ts_value + i)->year = PyDateTime_GET_YEAR(item);
                    (curr->ts_value + i)->month = PyDateTime_GET_MONTH(item);
                    (curr->ts_value + i)->day = PyDateTime_GET_DAY(item);
                    (curr->ts_value + i)->hour = PyDateTime_DATE_GET_HOUR(item);
                    (curr->ts_value + i)->minute = PyDateTime_DATE_GET_MINUTE(item);
                    (curr->ts_value + i)->second = PyDateTime_DATE_GET_SECOND(item);
                    (curr->ts_value + i)->fraction = PyDateTime_DATE_GET_MICROSECOND(item) * 1000;
                    curr->bind_indicator_array[i] = SQL_NTS;
                    snprintf(messageStr, sizeof(messageStr), "List item index %d: year=%d, month=%d, day=%d, hour=%d, minute=%d, second=%d, fraction=%d, bind_indicator_array[%d]=SQL_NTS",
                             i, (curr->ts_value + i)->year, (curr->ts_value + i)->month, (curr->ts_value + i)->day,
                             (curr->ts_value + i)->hour, (curr->ts_value + i)->minute, (curr->ts_value + i)->second,
                             (curr->ts_value + i)->fraction, i);
                    LogMsg(DEBUG, messageStr);
                }
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_TYPE_TIMESTAMP, curr->data_type, curr->param_size,
                                  curr->scale, curr->ts_value, curr->ivalue, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for PYTHON_LIST with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, ts_value=%p, ivalue=%d, bind_indicator_array=%p and returned rc=%d",
                     (void *)stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale, (void *)curr->ts_value, curr->ivalue, (void *)curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            curr->ts_value = ALLOC(TIMESTAMP_STRUCT);
            curr->ts_value->year = PyDateTime_GET_YEAR(bind_data);
            curr->ts_value->month = PyDateTime_GET_MONTH(bind_data);
            curr->ts_value->day = PyDateTime_GET_DAY(bind_data);
            curr->ts_value->hour = PyDateTime_DATE_GET_HOUR(bind_data);
            curr->ts_value->minute = PyDateTime_DATE_GET_MINUTE(bind_data);
            curr->ts_value->second = PyDateTime_DATE_GET_SECOND(bind_data);
            curr->ts_value->fraction = PyDateTime_DATE_GET_MICROSECOND(bind_data) * 1000;
            snprintf(messageStr, sizeof(messageStr), "Binding scalar value: year=%d, month=%d, day=%d, hour=%d, minute=%d, second=%d, fraction=%d",
                     curr->ts_value->year, curr->ts_value->month, curr->ts_value->day,
                     curr->ts_value->hour, curr->ts_value->minute, curr->ts_value->second,
                     curr->ts_value->fraction);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_TYPE_TIMESTAMP, curr->data_type, curr->param_size,
                                  curr->scale, curr->ts_value, curr->ivalue, &(curr->bind_indicator));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for scalar value with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, ts_value=%p, ivalue=%d, bind_indicator=%p and returned rc=%d",
                     (void *)stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale,
                     (void *)curr->ts_value, curr->ivalue, (void *)&(curr->bind_indicator), rc);
            LogMsg(DEBUG, messageStr);
        }
        break;

    case PYTHON_TIMESTAMP_TSTZ:
       snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_TIMESTAMP_TSTZ case. Current data_type=%d", curr->data_type);
       LogMsg(DEBUG, messageStr);
       if (TYPE(bind_data) == PYTHON_LIST) {
           Py_ssize_t n = PyList_Size(bind_data);
           snprintf(messageStr, sizeof(messageStr), "Binding array of timezone-aware datetimes, size: %zd", n);
           LogMsg(DEBUG, messageStr);
           curr->tstz_value = ALLOC_N(TIMESTAMP_STRUCT_EXT_TZ, n);
           curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
           for (i = 0; i < n; i++) {
               item = PyList_GetItem(bind_data, i);
               snprintf(messageStr, sizeof(messageStr), "Processing list item index %d, item pointer: %p", i, (void *)item);
               LogMsg(DEBUG, messageStr);
               if (TYPE(item) == PYTHON_NIL) {
                   curr->bind_indicator_array[i] = SQL_NULL_DATA;
               }else{
                    TIMESTAMP_STRUCT_EXT_TZ *ts = &curr->tstz_value[i];
                    ts->year = PyDateTime_GET_YEAR(item);
                    ts->month = PyDateTime_GET_MONTH(item);
                    ts->day = PyDateTime_GET_DAY(item);
                    ts->hour = PyDateTime_DATE_GET_HOUR(item);
                    ts->minute = PyDateTime_DATE_GET_MINUTE(item);
                    ts->second = PyDateTime_DATE_GET_SECOND(item);
                    ts->fraction = PyDateTime_DATE_GET_MICROSECOND(item) * 1000;
                    curr->bind_indicator_array[i] = SQL_NTS;
#if PY_VERSION_HEX >= 0x030A0000  /* Python 3.10+ */
                    PyObject *tzinfo = PyDateTime_DATE_GET_TZINFO(item);
#else
                    PyObject *tzinfo = NULL;  /* Not available in Python < 3.10 */
#endif
                    snprintf(messageStr, sizeof(messageStr), "tzinfo pointer: %p", (void *)tzinfo);
                    LogMsg(DEBUG, messageStr);

                    if (!tzinfo || tzinfo == Py_None) {
                        LogMsg(EXCEPTION,"No tzinfo provided on datetime object");
                        PyErr_SetString(PyExc_ValueError, "No tzinfo provided on datetime object");
                    }

                    PyObject *offset = PyObject_CallMethod(tzinfo, "utcoffset", "O", item);
                    if (offset == NULL || offset == Py_None) {
                        LogMsg(EXCEPTION,"Invalid or missing tzinfo.utcoffset");
                        PyErr_SetString(PyExc_ValueError, "Invalid or missing tzinfo.utcoffset");
                    }

                    PyObject *total_seconds_obj = PyObject_CallMethod(offset, "total_seconds", NULL);
                    Py_XDECREF(offset);
                    if (total_seconds_obj == NULL) {
                        LogMsg(EXCEPTION, "Could not extract total_seconds from utcoffset");
                        PyErr_SetString(PyExc_ValueError, "Could not extract total_seconds from utcoffset");
                    }

                    PyObject *result = PyNumber_Float(total_seconds_obj);
                    Py_XDECREF(total_seconds_obj);
                    if (result == NULL) {
                        LogMsg(EXCEPTION, "Failed to convert total_seconds to float");
                        PyErr_SetString(PyExc_ValueError, "Failed to convert total_seconds to float");
                    }

                    double total_seconds = PyFloat_AsDouble(result);
                    Py_XDECREF(result);
                    ts->timezone_hour = (SQLSMALLINT)(total_seconds / 3600);
                    ts->timezone_minute = (SQLUSMALLINT)((total_seconds - (ts->timezone_hour * 3600)) / 60);
                    if (ts->timezone_hour < 0)
                        ts->timezone_minute = -ts->timezone_minute;

                    if (ts->timezone_hour < -14 || ts->timezone_hour > 14) {
                        snprintf(messageStr, sizeof(messageStr),"Item %zd: timezone hour out of range:%d", i, ts->timezone_hour);
                        LogMsg(EXCEPTION, messageStr);
                        PyErr_Format(PyExc_ValueError, "Item %zd: timezone hour out of range:%d", i, ts->timezone_hour);
                    }

                    if (abs(ts->timezone_minute) > 59) {
                        snprintf(messageStr, sizeof(messageStr),"Item %zd: timezone hour out of range:%d", i, ts->timezone_minute);
                        LogMsg(EXCEPTION, messageStr);
                        PyErr_Format(PyExc_ValueError, "Item %zd: timezone minute out of range:%d", i, ts->timezone_minute);
                    }
                snprintf(messageStr, sizeof(messageStr), "List item index %d: year=%d, month=%d, day=%d, hour=%d, minute=%d, second=%d, fraction=%d, timezone_hour=%d, timezone_minute=%d, curr->bind_indicator_array[%d] = SQL_NTS;", i,
                        ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second, ts->fraction, ts->timezone_hour, ts->timezone_minute,i);
                LogMsg(DEBUG, messageStr);
                }
           }
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_TYPE_TIMESTAMP_EXT_TZ, curr->data_type,
               curr->param_size, curr->scale, curr->tstz_value, curr->ivalue, &curr->bind_indicator_array[0]);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for PYTHON_LIST with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, tstz_value=%p, ivalue=%d, bind_indicator_array=%p and returned rc=%d",
                     (void *)stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale, (void *)curr->tstz_value, curr->ivalue, (void *)curr->bind_indicator_array, rc);
        LogMsg(DEBUG, messageStr);

    }
    else
    {
        curr->tstz_value = ALLOC(TIMESTAMP_STRUCT_EXT_TZ);
        curr->tstz_value->year = PyDateTime_GET_YEAR(bind_data);
        curr->tstz_value->month = PyDateTime_GET_MONTH(bind_data);
        curr->tstz_value->day = PyDateTime_GET_DAY(bind_data);
        curr->tstz_value->hour = PyDateTime_DATE_GET_HOUR(bind_data);
        curr->tstz_value->minute = PyDateTime_DATE_GET_MINUTE(bind_data);
        curr->tstz_value->second = PyDateTime_DATE_GET_SECOND(bind_data);
        curr->tstz_value->fraction = PyDateTime_DATE_GET_MICROSECOND(bind_data) * 1000;
        PyObject *offset = NULL;
        PyObject *total_seconds_obj = NULL;
        PyObject *result = NULL;
        double total_seconds;

#if PY_VERSION_HEX >= 0x030A0000  /* Python 3.10+ */
        PyObject* tzinfo = PyDateTime_DATE_GET_TZINFO(bind_data);
#else
        PyObject* tzinfo = NULL;  /* Not supported in Python < 3.10 */
#endif

        if (!tzinfo || tzinfo == Py_None) {
            LogMsg(EXCEPTION, "No tzinfo provided on datetime object");
            PyErr_SetString(PyExc_ValueError, "No tzinfo provided on datetime object");
        }

        offset = PyObject_CallMethod(tzinfo, "utcoffset", "O", bind_data);
        if (offset == NULL || offset == Py_None) {
            LogMsg(EXCEPTION, "Invalid or missing tzinfo.utcoffset");
            PyErr_SetString(PyExc_ValueError, "Invalid or missing tzinfo.utcoffset");
        }

        total_seconds_obj = PyObject_CallMethod(offset, "total_seconds", NULL);
        Py_XDECREF(offset);
        if (total_seconds_obj == NULL) {
            LogMsg(EXCEPTION, "Could not extract total_seconds from utcoffset");
            PyErr_SetString(PyExc_ValueError, "Could not extract total_seconds from utcoffset");
        }

        result = PyNumber_Float(total_seconds_obj);
        Py_XDECREF(total_seconds_obj);
        if (result == NULL) {
            LogMsg(EXCEPTION, "Failed to convert total_seconds to float");
            PyErr_SetString(PyExc_ValueError, "Failed to convert total_seconds to float");
        }
        total_seconds = PyFloat_AsDouble(result);
        Py_XDECREF(result);

        curr->tstz_value->timezone_hour = (SQLSMALLINT)(total_seconds / 3600);
        curr->tstz_value->timezone_minute = (SQLUSMALLINT)((total_seconds - (curr->tstz_value->timezone_hour * 3600)) / 60);

        if (curr->tstz_value->timezone_hour < 0) {
            curr->tstz_value->timezone_minute = -(curr->tstz_value->timezone_minute);
        }

        if (curr->tstz_value->timezone_hour < -14 || curr->tstz_value->timezone_hour > 14) {
            snprintf(messageStr, sizeof(messageStr),"Timezone hour out of range:%d", curr->tstz_value->timezone_hour);
            LogMsg(EXCEPTION, messageStr);
            PyErr_Format(PyExc_ValueError, "Timezone hour out of range:%d", curr->tstz_value->timezone_hour);
        }
        if (abs(curr->tstz_value->timezone_minute) > 59) {
            snprintf(messageStr, sizeof(messageStr),"Timezone minute out of range:%d",curr->tstz_value->timezone_minute);
            LogMsg(EXCEPTION, messageStr);
            PyErr_Format(PyExc_ValueError, "Timezone minute offset out of range:%d",curr->tstz_value->timezone_minute);
        }

        snprintf(messageStr, sizeof(messageStr), "Binding scalar value: year=%d, month=%d, day=%d, hour=%d, minute=%d, second=%d, fraction=%d, timezone_hour=%d, timezone_minute=%d",
                        curr->tstz_value->year, curr->tstz_value->month, curr->tstz_value->day, curr->tstz_value->hour, curr->tstz_value->minute, curr->tstz_value->second, curr->tstz_value->fraction, curr->tstz_value->timezone_hour, curr->tstz_value->timezone_minute);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, SQL_C_TYPE_TIMESTAMP_EXT_TZ,
                curr->data_type, curr->param_size, curr->scale, curr->tstz_value, curr->ivalue, &(curr->bind_indicator));
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for scalar value with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, tstz_value=%p, ivalue=%d, bind_indicator=%p and returned rc=%d",
                     (void *)stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale,
                     (void *)curr->tstz_value, curr->ivalue, (void *)&(curr->bind_indicator), rc);
        LogMsg(DEBUG, messageStr);
    }
    break;

    case PYTHON_NIL:
        snprintf(messageStr, sizeof(messageStr), "Handling PYTHON_NIL case. Current data_type=%d", curr->data_type);
        LogMsg(DEBUG, messageStr);
        if (TYPE(bind_data) == PYTHON_LIST)
        {
            Py_ssize_t n = PyList_Size(bind_data);
            snprintf(messageStr, sizeof(messageStr), "PYTHON_LIST detected with size: %zd", n);
            LogMsg(DEBUG, messageStr);
            curr->bind_indicator_array = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
            for (i = 0; i < n; i++)
            {
                curr->bind_indicator_array[i] = SQL_NULL_DATA;
                snprintf(messageStr, sizeof(messageStr), "List item index %d set to SQL_NULL_DATA", i);
                LogMsg(DEBUG, messageStr);
            }
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_DEFAULT, curr->data_type, curr->param_size,
                                  curr->scale, curr->ivalueArray, 0, &curr->bind_indicator_array[0]);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for PYTHON_LIST with parameters: hstmt=%p, param_num=%d, param_type=%d, data_type=%d, param_size=%d, scale=%d, ivalueArray=%p, bind_indicator_array=%p and returned rc=%d",
                     (void *)stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale,
                     (void *)curr->ivalueArray, (void *)curr->bind_indicator_array, rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            curr->ivalue = SQL_NULL_DATA;
            snprintf(messageStr, sizeof(messageStr), "Binding scalar value as SQL_NULL_DATA");
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                  curr->param_type, SQL_C_DEFAULT, curr->data_type, curr->param_size,
                                  curr->scale, &curr->ivalue, 0, (SQLLEN *)&(curr->ivalue));
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLBindParameter for scalar PYTHON_NIL with parameters: hstmt=%p, param_num=%d, param_type=%d, SQL_C_DEFAULT, data_type=%d, param_size=%d, scale=%d, ivalue=%d and returned rc=%d",
                     (void *)stmt_res->hstmt, curr->param_num, curr->param_type, curr->data_type, curr->param_size, curr->scale, curr->ivalue, rc);
            LogMsg(DEBUG, messageStr);
        }

        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }

        break;

    default:
        LogMsg(ERROR, "Unhandled case in _python_ibm_db_bind_data(), default case");
        return SQL_ERROR;
    }
    LogMsg(INFO, "exit _python_ibm_db_bind_data()");
    return rc;
}

/* static int _python_ibm_db_execute_helper2(stmt_res, data, int bind_cmp_list)
 */
static int _python_ibm_db_execute_helper2(stmt_handle *stmt_res, PyObject *data, int bind_cmp_list, int bind_params)
{
    LogMsg(INFO, "entry _python_ibm_db_execute_helper2()");
    int rc = SQL_SUCCESS;
    param_node *curr = NULL; /* To traverse the list */
    PyObject *bind_data;     /* Data value from symbol table */
    char error[DB2_MAX_ERR_MSG_LEN + 50];

    /* Used in call to SQLDescribeParam if needed */
    SQLSMALLINT param_no;
    SQLSMALLINT data_type;
    SQLUINTEGER precision;
    SQLSMALLINT scale;
    SQLSMALLINT nullable;
    snprintf(messageStr, sizeof(messageStr), "Parameters: stmt_res->hstmt: %p, data: %p, bind_cmp_list: %d, bind_params: %d",
             (SQLHSTMT)stmt_res->hstmt, data, bind_cmp_list, bind_params);
    LogMsg(DEBUG, messageStr);
    /* This variable means that we bind the complete list of params cached */
    /* The values used are fetched from the active symbol table */
    /* TODO: Enhance this part to check for stmt_res->file_param */
    /* If this flag is set, then use SQLBindParam, else use SQLExtendedBind */
    if (bind_cmp_list)
    {
        /* Bind the complete list sequentially */
        /* Used when no parameters array is passed in */
        curr = stmt_res->head_cache_list;
        snprintf(messageStr, sizeof(messageStr), "Binding complete list. Initial node: %p", curr);
        LogMsg(DEBUG, messageStr);
        while (curr != NULL)
        {
            /* Fetch data from symbol table */
            bind_data = curr->var_pyvalue;
            snprintf(messageStr, sizeof(messageStr), "Processing node: %p, param_type: %d, bind_data: %p",
                     curr, curr->param_type, bind_data);
            LogMsg(DEBUG, messageStr);
            if (bind_data == NULL)
                return -1;

            rc = _python_ibm_db_bind_data(stmt_res, curr, bind_data);
            snprintf(messageStr, sizeof(messageStr), "Called _python_ibm_db_bind_data with stmt_res: %p, curr: %p, bind_data: %p and returned rc: %d", stmt_res, curr, bind_data, rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                sprintf(error, "Binding Error 1: %s",
                        IBM_DB_G(__python_stmt_err_msg));
                LogMsg(ERROR, error);
                PyErr_SetString(PyExc_Exception, error);
                return rc;
            }
            curr = curr->next;
        }
        LogMsg(INFO, "exit _python_ibm_db_execute_helper2()");
        return 0;
    }
    else
    {
        /* Bind only the data value passed in to the Current Node */
        if (data != NULL)
        {
            snprintf(messageStr, sizeof(messageStr), "data provided: %p", data);
            LogMsg(DEBUG, messageStr);
            if (bind_params)
            {
                /* This condition applies if the parameter has not been
                 * bound using ibm_db.bind_param. Need to describe the
                 * parameter and then bind it.
                 */
                param_no = ++stmt_res->num_params;
                snprintf(messageStr, sizeof(messageStr), "Describing param with param_no: %d", param_no);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt, param_no,
                                      (SQLSMALLINT *)&data_type, &precision, (SQLSMALLINT *)&scale,
                                      (SQLSMALLINT *)&nullable);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "Called SQLDescribeParam with hstmt: %p, param_no: %d, data_type: %p, precision: %p, scale: %p, nullable: %p and returned rc: %d",
                         (SQLHSTMT)stmt_res->hstmt, param_no, (SQLSMALLINT *)&data_type, &precision, (SQLSMALLINT *)&scale, (SQLSMALLINT *)&nullable, rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
                {
                    _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                                    SQL_HANDLE_STMT,
                                                    rc, 1, NULL, -1, 1);
                }
                if (rc == SQL_ERROR)
                {
                    sprintf(error, "Describe Param Failed: %s",
                            IBM_DB_G(__python_stmt_err_msg));
                    LogMsg(ERROR, error);
                    PyErr_SetString(PyExc_Exception, error);
                    return rc;
                }
                curr = build_list(stmt_res, param_no, data_type, precision,
                                  scale, nullable);
                snprintf(messageStr, sizeof(messageStr), "Built list with param_no: %d, data_type: %d, precision: %u, scale: %d, nullable: %d",
                         param_no, data_type, precision, scale, nullable);
                LogMsg(DEBUG, messageStr);
                rc = _python_ibm_db_bind_data(stmt_res, curr, data);
                snprintf(messageStr, sizeof(messageStr), "Called _python_ibm_db_bind_data with stmt_res: %p, curr: %p, data: %p and returned rc: %d", stmt_res, curr, data, rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    sprintf(error, "Binding Error 2: %s",
                            IBM_DB_G(__python_stmt_err_msg));
                    LogMsg(ERROR, error);
                    PyErr_SetString(PyExc_Exception, error);
                    return rc;
                }
            }
            else
            {
                /* This is always at least the head_cache_node -- assigned in
                 * ibm_db.execute(), if params have been bound.
                 */
                curr = stmt_res->current_node;
                snprintf(messageStr, sizeof(messageStr), "Binding to current node. Current node: %p", curr);
                LogMsg(DEBUG, messageStr);
                if (curr != NULL)
                {
                    rc = _python_ibm_db_bind_data(stmt_res, curr, data);
                    snprintf(messageStr, sizeof(messageStr), "Called _python_ibm_db_bind_data with stmt_res: %p, curr: %p, data: %p and returned rc: %d", stmt_res, curr, data, rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_ERROR)
                    {
                        sprintf(error, "Binding Error 2: %s",
                                IBM_DB_G(__python_stmt_err_msg));
                        LogMsg(ERROR, error);
                        PyErr_SetString(PyExc_Exception, error);
                        return rc;
                    }
                    stmt_res->current_node = curr->next;
                }
            }
            LogMsg(INFO, "exit _python_ibm_db_execute_helper2()");
            return rc;
        }
    }
    LogMsg(INFO, "exit _python_ibm_db_execute_helper2()");
    return rc;
}

/*
 * static PyObject *_python_ibm_db_execute_helper1(stmt_handle *stmt_res, PyObject *parameters_tuple)
 *
 */
static PyObject *_python_ibm_db_execute_helper1(stmt_handle *stmt_res, PyObject *parameters_tuple)
{
    LogMsg(INFO, "entry _python_ibm_db_execute_helper1()");
    int rc, numOpts, i, bind_params = 0;
    SQLSMALLINT num = 0;
    SQLPOINTER valuePtr;
    PyObject *data;
    char error[DB2_MAX_ERR_MSG_LEN + 50];
    snprintf(messageStr, sizeof(messageStr), "stmt_res: %p, parameters_tuple: %p, bind_params: %d",
             (void *)stmt_res, (void *)parameters_tuple, bind_params);
    LogMsg(DEBUG, messageStr);
    /* Free any cursors that might have been allocated in a previous call to
     * SQLExecute
     */
    Py_BEGIN_ALLOW_THREADS;
    SQLFreeStmt((SQLHSTMT)stmt_res->hstmt, SQL_CLOSE);
    Py_END_ALLOW_THREADS;

    /* This ensures that each call to ibm_db.execute start from scratch */
    stmt_res->current_node = stmt_res->head_cache_list;

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLNumParams((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT *)&num);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLNumParams called with hstmt: %p, num: %p, and returned rc: %d",
             (SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT *)&num, rc);
    LogMsg(DEBUG, messageStr);
    if (num != 0)
    {
        /* Parameter Handling */
        if (!NIL_P(parameters_tuple))
        {
            /* Make sure ibm_db.bind_param has been called */
            /* If the param list is NULL -- ERROR */
            if (stmt_res->head_cache_list == NULL)
            {
                bind_params = 1;
            }

            if (!PyTuple_Check(parameters_tuple))
            {
                LogMsg(EXCEPTION, "Param is not a tuple");
                PyErr_SetString(PyExc_Exception, "Param is not a tuple");
                return NULL;
            }

            numOpts = PyTuple_Size(parameters_tuple);
            snprintf(messageStr, sizeof(messageStr), "parameters_tuple: %p, numOpts: %d, num: %d",
                     (void *)parameters_tuple, numOpts, num);
            LogMsg(DEBUG, messageStr);
            if (numOpts > num)
            {
                /* More are passed in -- Warning - Use the max number present */
                sprintf(error, "%d params bound not matching %d required",
                        numOpts, num);
                LogMsg(WARNING, error);
                PyErr_SetString(PyExc_Exception, error);
                numOpts = stmt_res->num_params;
            }
            else if (numOpts < num)
            {
                /* If there are less params passed in, than are present
                 * -- Error
                 */
                sprintf(error, "%d params bound not matching %d required",
                        numOpts, num);
                LogMsg(EXCEPTION, error);
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }

            for (i = 0; i < numOpts; i++)
            {
                /* Bind values from the parameters_tuple to params */
                data = PyTuple_GetItem(parameters_tuple, i);
                snprintf(messageStr, sizeof(messageStr),
                         "Binding parameter %d with data: %p", i, (void *)data);
                LogMsg(DEBUG, messageStr);
                /* The 0 denotes that you work only with the current node.
                 * The 4th argument specifies whether the data passed in
                 * has been described. So we need to call SQLDescribeParam
                 * before binding depending on this.
                 */
                rc = _python_ibm_db_execute_helper2(stmt_res, data, 0, bind_params);
                snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_execute_helper2 called with stmt_res: %p, data: %p, bind_params: %d, returned rc: %d",
                         (void *)stmt_res, (void *)data, bind_params, rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    sprintf(error, "Binding Error: %s", IBM_DB_G(__python_stmt_err_msg));
                    LogMsg(ERROR, error);
                    PyErr_SetString(PyExc_Exception, error);
                    return NULL;
                }
            }
        }
        else
        {
            /* No additional params passed in. Use values already bound. */
            if (num > stmt_res->num_params)
            {
                /* More parameters than we expected */
                sprintf(error, "%d params bound not matching %d required",
                        stmt_res->num_params, num);
                LogMsg(ERROR, error);
                PyErr_SetString(PyExc_Exception, error);
            }
            else if (num < stmt_res->num_params)
            {
                /* Fewer parameters than we expected */
                sprintf(error, "%d params bound not matching %d required",
                        stmt_res->num_params, num);
                LogMsg(ERROR, error);
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }

            /* Param cache node list is empty -- No params bound */
            if (stmt_res->head_cache_list == NULL)
            {
                LogMsg(EXCEPTION, "Parameters not bound");
                PyErr_SetString(PyExc_Exception, "Parameters not bound");
                return NULL;
            }
            else
            {
                /* The 1 denotes that you work with the whole list
                 * And bind sequentially
                 */
                rc = _python_ibm_db_execute_helper2(stmt_res, NULL, 1, 0);
                snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_execute_helper2 called with stmt_res: %p, data: NULL, bind_params: 0, and returned rc: %d",
                         (void *)stmt_res, rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    sprintf(error, "Binding Error 3: %s", IBM_DB_G(__python_stmt_err_msg));
                    LogMsg(ERROR, error);
                    PyErr_SetString(PyExc_Exception, error);
                    return NULL;
                }
            }
        }
    }
    else
    {
        /* No Parameters
         * We just execute the statement. No additional work needed.
         */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLExecute((SQLHSTMT)stmt_res->hstmt);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLExecute called with hstmt: %p, and returned rc: %d",
                 (SQLHSTMT)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                            SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "Statement Execute Failed: %s", IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        LogMsg(INFO, "exit _python_ibm_db_execute_helper1()");
        Py_INCREF(Py_True);
        return Py_True;
    }

    /* Execute Stmt -- All parameters bound */
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLExecute((SQLHSTMT)stmt_res->hstmt);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLExecute called with hstmt: %p, and returned rc: %d",
             (SQLHSTMT)stmt_res->hstmt, rc);
    LogMsg(DEBUG, messageStr);
    if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                        SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
    }
    if (rc == SQL_ERROR)
    {
        sprintf(error, "Statement Execute Failed: %s", IBM_DB_G(__python_stmt_err_msg));
        PyErr_SetString(PyExc_Exception, error);
        return NULL;
    }
    if (rc == SQL_NEED_DATA)
    {
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLParamData called with hstmt: %p, valuePtr: %p, and returned rc: %d",
                 (SQLHSTMT)stmt_res->hstmt, (void *)valuePtr, rc);
        LogMsg(DEBUG, messageStr);
        while (rc == SQL_NEED_DATA)
        {
            /* passing data value for a parameter */
            if (!NIL_P(((param_node *)valuePtr)->svalue))
            {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node *)valuePtr)->svalue), ((param_node *)valuePtr)->ivalue);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLPutData called with hstmt: %p, svalue: %p, ivalue: %d, and returned rc: %d",
                         (SQLHSTMT)stmt_res->hstmt, (void *)(((param_node *)valuePtr)->svalue), ((param_node *)valuePtr)->ivalue, rc);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node *)valuePtr)->uvalue), ((param_node *)valuePtr)->ivalue);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLPutData called with hstmt: %p, uvalue: %p, ivalue: %d, and returned rc: %d",
                         (SQLHSTMT)stmt_res->hstmt, (void *)(((param_node *)valuePtr)->uvalue), ((param_node *)valuePtr)->ivalue, rc);
                LogMsg(DEBUG, messageStr);
            }

            if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            if (rc == SQL_ERROR)
            {
                sprintf(error, "Sending data failed: %s",
                        IBM_DB_G(__python_stmt_err_msg));
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "SQLParamData called with hstmt: %p, valuePtr: %p, and returned rc: %d",
                     (SQLHSTMT)stmt_res->hstmt, (void *)valuePtr, rc);
            LogMsg(DEBUG, messageStr);
        }

        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "Sending data failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
    }
    /* cleanup dynamic bindings if present */
    if (bind_params == 1)
    {
        _python_ibm_db_clear_param_cache(stmt_res);
    }
    if (rc != SQL_ERROR)
    {
        LogMsg(INFO, "exit _python_ibm_db_execute_helper1()");
        Py_INCREF(Py_True);
        return Py_True;
    }
}

/*!# ibm_db.execute
 *
 * ===Description
 * Py_True/Py_False ibm_db.execute ( IBM_DBStatement stmt [, tuple parameters] )
 *
 * ibm_db.execute() executes an SQL statement that was prepared by
 * ibm_db.prepare().
 *
 * If the SQL statement returns a result set, for example, a SELECT statement
 * or a CALL to a stored procedure that returns one or more result sets, you
 * can retrieve a row as an tuple/dictionary from the stmt resource using
 * ibm_db.fetch_assoc(), ibm_db.fetch_both(), or ibm_db.fetch_tuple().
 * Alternatively, you can use ibm_db.fetch_row() to move the result set pointer
 * to the next row and fetch a column at a time from that row with
 * ibm_db.result().
 *
 * Refer to ibm_db.prepare() for a brief discussion of the advantages of using
 * ibm_db.prepare() and ibm_db.execute() rather than ibm_db.exec().
 *
 * ===Parameters
 * ====stmt
 *
 *        A prepared statement returned from ibm_db.prepare().
 *
 * ====parameters
 *
 *        An tuple of input parameters matching any parameter markers contained
 * in the prepared statement.
 *
 * ===Return Values
 *
 * Returns Py_True on success or Py_False on failure.
 */
static PyObject *ibm_db_execute(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry execute()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *parameters_tuple = NULL;
    stmt_handle *stmt_res;
    if (!PyArg_ParseTuple(args, "O|O", &py_stmt_res, &parameters_tuple))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, parameters_tuple=%p", py_stmt_res, parameters_tuple);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        LogMsg(INFO, "Calling and returning _python_ibm_db_execute_helper1");
        LogMsg(INFO, "exit execute()");
        return _python_ibm_db_execute_helper1(stmt_res, parameters_tuple);
    }
    else
    {
        LogMsg(EXCEPTION, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
}

/*!# ibm_db.conn_errormsg
 *
 * ===Description
 * string ibm_db.conn_errormsg ( [resource connection] )
 *
 * ibm_db.conn_errormsg() returns an error message and SQLCODE value
 * representing the reason the last database connection attempt failed.
 * As ibm_db.connect() returns FALSE in the event of a failed connection
 * attempt, do not pass any parameters to ibm_db.conn_errormsg() to retrieve
 * the associated error message and SQLCODE value.
 *
 * If, however, the connection was successful but becomes invalid over time,
 * you can pass the connection parameter to retrieve the associated error
 * message and SQLCODE value for a specific connection.
 * ===Parameters
 *
 * ====connection
 *        A connection resource associated with a connection that initially
 * succeeded, but which over time became invalid.
 *
 * ===Return Values
 *
 * Returns a string containing the error message and SQLCODE value resulting
 * from a failed connection attempt. If there is no error associated with the
 * last connection attempt, ibm_db.conn_errormsg() returns an empty string.
 */
static PyObject *ibm_db_conn_errormsg(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry conn_errormsg()");
    LogUTF8Msg(args);
    conn_handle *conn_res = NULL;
    PyObject *py_conn_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return err
                              * strings
                              */

    if (!PyArg_ParseTuple(args, "|O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p", py_conn_res);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        if (return_str != NULL)
        {
            snprintf(messageStr, sizeof(messageStr), "Allocated return_str: %p, size: %d", return_str, DB2_MAX_ERR_MSG_LEN);
            LogMsg(DEBUG, messageStr);
            memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);
            LogMsg(DEBUG, "Initialized return_str with zeros");
        }
        else
        {
            LogMsg(ERROR, "Memory allocation for return_str failed");
            return NULL;
        }
        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, -1, 0,
                                        return_str, DB2_ERRMSG,
                                        conn_res->errormsg_recno_tracker);
        snprintf(messageStr, sizeof(messageStr), "SQL errors checked. return_str: %s", return_str);
        LogMsg(DEBUG, messageStr);
        if (conn_res->errormsg_recno_tracker - conn_res->error_recno_tracker >= 1)
        {
            conn_res->error_recno_tracker = conn_res->errormsg_recno_tracker;
            LogMsg(DEBUG, "Updated error_recno_tracker to match errormsg_recno_tracker");
        }
        conn_res->errormsg_recno_tracker++;
        snprintf(messageStr, sizeof(messageStr), "Updated error_recno_tracker: %d, errormsg_recno_tracker: %d", conn_res->error_recno_tracker, conn_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);
        if (return_str != NULL)
        {
            retVal = StringOBJ_FromASCII(return_str);
            PyMem_Del(return_str);
            return_str = NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Created return value: %p", retVal);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit conn_errormsg()");
        return retVal;
    }
    else
    {
        PyObject *defaultErrorMsg = PyUnicode_DecodeUTF8(
            (const char *)IBM_DB_G(__python_conn_err_msg),
            strlen((const char *)IBM_DB_G(__python_conn_err_msg)),
            "replace"
        );
        snprintf(messageStr, sizeof(messageStr), "No statement object provided. Returning default error message: %s", PyUnicode_AsUTF8(defaultErrorMsg));
        Py_DECREF(defaultErrorMsg);
        LogMsg(INFO, messageStr);
        LogMsg(INFO, "exit conn_errormsg()");
        return PyUnicode_DecodeUTF8(
            (const char *)IBM_DB_G(__python_conn_err_msg),
            strlen((const char *)IBM_DB_G(__python_conn_err_msg)),
            "replace"
        );
    }
}
/*!# ibm_db_conn_warn
 *
 * ===Description
 * string ibm_db.conn_warn ( [resource connection] )
 *
 * ibm_db.conn_warn() returns a warning message and SQLCODE value
 * representing the reason the last database connection attempt failed.
 * As ibm_db.connect() returns FALSE in the event of a failed connection
 * attempt, do not pass any parameters to ibm_db.warn_conn_msg() to retrieve
 * the associated warning message and SQLCODE value.
 *
 * If, however, the connection was successful but becomes invalid over time,
 * you can pass the connection parameter to retrieve the associated warning
 * message and SQLCODE value for a specific connection.
 * ===Parameters
 *
 * ====connection
 *      A connection resource associated with a connection that initially
 * succeeded, but which over time became invalid.
 *
 * ===Return Values
 *
 * Returns a string containing the warning message and SQLCODE value resulting
 * from a failed connection attempt. If there is no warning associated with the
 * last connection attempt, ibm_db.warn_conn_msg() returns an empty string.
 */
static PyObject *ibm_db_conn_warn(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry conn_warn()");
    LogUTF8Msg(args);
    conn_handle *conn_res = NULL;
    PyObject *py_conn_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return warning
                              * strings */

    if (!PyArg_ParseTuple(args, "|O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p", py_conn_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res=%p", conn_res);
            LogMsg(DEBUG, messageStr);
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        if (return_str != NULL)
        {
            PyErr_Clear();
            memset(return_str, 0, SQL_SQLSTATE_SIZE + 1);
        }
        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, 1, 0,
                                        return_str, DB2_WARNMSG,
                                        conn_res->error_recno_tracker);
        if (conn_res->error_recno_tracker - conn_res->errormsg_recno_tracker >= 1)
        {
            conn_res->errormsg_recno_tracker = conn_res->error_recno_tracker;
            snprintf(messageStr, sizeof(messageStr), "Updated errormsg_recno_tracker to %d", conn_res->errormsg_recno_tracker);
            LogMsg(DEBUG, messageStr);
        }
        conn_res->error_recno_tracker++;
        if (return_str != NULL)
        {
            retVal = StringOBJ_FromASCII(return_str);
            snprintf(messageStr, sizeof(messageStr), "Returning warning message: %s", return_str);
            LogMsg(INFO, messageStr);
            PyMem_Del(return_str);
            return_str = NULL;
        }
        LogMsg(INFO, "exit conn_warn()");
        return retVal;
    }
    else
    {
        const char *default_warning_msg = IBM_DB_G(__python_conn_warn_msg);
        snprintf(messageStr, sizeof(messageStr), "No connection object provided, returning default warning message: %s", default_warning_msg);
        LogMsg(INFO, messageStr);
        LogMsg(INFO, "exit conn_warn()");
        return StringOBJ_FromASCII(IBM_DB_G(__python_conn_warn_msg));
    }
}

/*!# ibm_db.stmt_warn
 *
 * ===Description
 * string ibm_db.stmt_warn ( [resource stmt] )
 *
 * Returns a string containing the last SQL statement error message.
 *
 * If you do not pass a statement resource as an argument to
 * ibm_db.warn_stmt_msg(), the driver returns the warning message associated with
 * the last attempt to return a statement resource, for example, from
 * ibm_db.prepare() or ibm_db.exec().
 *
 * ===Parameters
 *
 * ====stmt
 *      A valid statement resource.
 *
 * ===Return Values
 *
 * Returns a string containing the warning message and SQLCODE value for the last
 * warning that occurred issuing an SQL statement.
 */
static PyObject *ibm_db_stmt_warn(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry stmt_warn()");
    LogUTF8Msg(args);
    stmt_handle *stmt_res = NULL;
    PyObject *py_stmt_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return err
                              * strings
                              */

    if (!PyArg_ParseTuple(args, "|O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res=%p", stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        if (return_str != NULL)
        {
            memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);
        }
        else
        {
            LogMsg(ERROR, "Memory allocation for return_str failed");
            return NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Calling _python_ibm_db_check_sql_errors with parameters: "
                                                 "hstmt=%p, handle_type=%d, recno_tracker=%d",
                 stmt_res->hstmt, SQL_HANDLE_STMT, stmt_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, 1, 0,
                                        return_str, DB2_WARNMSG,
                                        stmt_res->errormsg_recno_tracker);
        snprintf(messageStr, sizeof(messageStr), "Returned warning message: %s", return_str);
        LogMsg(DEBUG, messageStr);
        if (stmt_res->errormsg_recno_tracker - stmt_res->error_recno_tracker >= 1)
        {
            stmt_res->error_recno_tracker = stmt_res->errormsg_recno_tracker;
        }
        stmt_res->errormsg_recno_tracker++;
        snprintf(messageStr, sizeof(messageStr), "Updated error_recno_tracker=%d, errormsg_recno_tracker=%d",
                 stmt_res->error_recno_tracker, stmt_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);
        if (return_str != NULL)
        {
            retVal = StringOBJ_FromASCII(return_str);
            PyMem_Del(return_str);
            return_str = NULL;
        }
        LogMsg(INFO, "exit stmt_warn()");
        return retVal;
    }
    else
    {
        const char *default_warning_msg = IBM_DB_G(__python_stmt_warn_msg);
        snprintf(messageStr, sizeof(messageStr), "No valid statement handle. Returning default warning message: %s", default_warning_msg);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit stmt_warn()");
        return StringOBJ_FromASCII(IBM_DB_G(__python_stmt_warn_msg));
    }
}

/*!# ibm_db.stmt_errormsg
 *
 * ===Description
 * string ibm_db.stmt_errormsg ( [resource stmt] )
 *
 * Returns a string containing the last SQL statement error message.
 *
 * If you do not pass a statement resource as an argument to
 * ibm_db.stmt_errormsg(), the driver returns the error message associated with
 * the last attempt to return a statement resource, for example, from
 * ibm_db.prepare() or ibm_db.exec().
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid statement resource.
 *
 * ===Return Values
 *
 * Returns a string containing the error message and SQLCODE value for the last
 * error that occurred issuing an SQL statement.
 */
static PyObject *ibm_db_stmt_errormsg(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry stmt_errormsg()");
    LogUTF8Msg(args);
    stmt_handle *stmt_res = NULL;
    PyObject *py_stmt_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return err
                              * strings
                              */

    if (!PyArg_ParseTuple(args, "|O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        if (return_str != NULL)
        {
            snprintf(messageStr, sizeof(messageStr), "Allocated return_str: %p, size: %d", return_str, DB2_MAX_ERR_MSG_LEN);
            LogMsg(DEBUG, messageStr);
            memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);
            LogMsg(DEBUG, "Initialized return_str with zeros");
        }
        else
        {
            LogMsg(ERROR, "Memory allocation for return_str failed");
            return NULL;
        }
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, -1, 0,
                                        return_str, DB2_ERRMSG,
                                        stmt_res->errormsg_recno_tracker);
        snprintf(messageStr, sizeof(messageStr), "SQL errors checked. return_str: %s", return_str);
        LogMsg(DEBUG, messageStr);
        if (stmt_res->errormsg_recno_tracker - stmt_res->error_recno_tracker >= 1)
        {
            LogMsg(DEBUG, "Updated error_recno_tracker to match errormsg_recno_tracker");
            stmt_res->error_recno_tracker = stmt_res->errormsg_recno_tracker;
        }
        stmt_res->errormsg_recno_tracker++;
        snprintf(messageStr, sizeof(messageStr), "Updated error_recno_tracker: %d, errormsg_recno_tracker: %d", stmt_res->error_recno_tracker, stmt_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);

        retVal = StringOBJ_FromStr(return_str);
        if (return_str != NULL)
        {
            PyMem_Del(return_str);
            return_str = NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Created return value: %p", retVal);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit stmt_errormsg()");
        return retVal;
    }
    else
    {
        PyObject *defaultErrorMsg = PyUnicode_DecodeUTF8(
            (const char *)IBM_DB_G(__python_stmt_err_msg),
            strlen((const char *)IBM_DB_G(__python_stmt_err_msg)),
            "replace"
        );
        snprintf(messageStr, sizeof(messageStr), "No statement object provided. Returning default error message: %s", PyUnicode_AsUTF8(defaultErrorMsg));
        Py_DECREF(defaultErrorMsg);
        LogMsg(INFO, messageStr);
        LogMsg(INFO, "exit stmt_errormsg()");
        return PyUnicode_DecodeUTF8(
            (const char *)IBM_DB_G(__python_stmt_err_msg),
            strlen((const char *)IBM_DB_G(__python_stmt_err_msg)),
            "replace"
        );
    }
}

/*!# ibm_db.conn_error
 * ===Description
 * string ibm_db.conn_error ( [resource connection] )
 *
 * ibm_db.conn_error() returns an SQLSTATE value representing the reason the
 * last attempt to connect to a database failed. As ibm_db.connect() returns
 * FALSE in the event of a failed connection attempt, you do not pass any
 * parameters to ibm_db.conn_error() to retrieve the SQLSTATE value.
 *
 * If, however, the connection was successful but becomes invalid over time, you
 * can pass the connection parameter to retrieve the SQLSTATE value for a
 * specific connection.
 *
 * To learn what the SQLSTATE value means, you can issue the following command
 * at a DB2 Command Line Processor prompt: db2 '? sqlstate-value'. You can also
 * call ibm_db.conn_errormsg() to retrieve an explicit error message and the
 * associated SQLCODE value.
 *
 * ===Parameters
 *
 * ====connection
 *        A connection resource associated with a connection that initially
 * succeeded, but which over time became invalid.
 *
 * ===Return Values
 *
 * Returns the SQLSTATE value resulting from a failed connection attempt.
 * Returns an empty string if there is no error associated with the last
 * connection attempt.
 */
static PyObject *ibm_db_conn_error(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry conn_error()");
    LogUTF8Msg(args);
    conn_handle *conn_res = NULL;
    PyObject *py_conn_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return err
                              * strings */

    if (!PyArg_ParseTuple(args, "|O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, ", py_conn_res);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        return_str = ALLOC_N(char, SQL_SQLSTATE_SIZE + 1);
        if (return_str != NULL)
        {
            snprintf(messageStr, sizeof(messageStr), "Allocated return_str: %p, size: %d", return_str, SQL_SQLSTATE_SIZE + 1);
            LogMsg(DEBUG, messageStr);
            memset(return_str, 0, SQL_SQLSTATE_SIZE + 1);
            LogMsg(DEBUG, "Initialized return_str with zeros");
        }
        else
        {
            LogMsg(ERROR, "Memory allocation for return_str failed");
            return NULL;
        }
        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, -1, 0,
                                        return_str, DB2_ERR,
                                        conn_res->error_recno_tracker);
        snprintf(messageStr, sizeof(messageStr), "SQL errors checked. return_str: %s", return_str);
        LogMsg(DEBUG, messageStr);
        if (conn_res->error_recno_tracker - conn_res->errormsg_recno_tracker >= 1)
        {
            LogMsg(DEBUG, "Updating errormsg_recno_tracker");
            conn_res->errormsg_recno_tracker = conn_res->error_recno_tracker;
        }
        conn_res->error_recno_tracker++;
        snprintf(messageStr, sizeof(messageStr), "Updated error_recno_tracker: %d, errormsg_recno_tracker: %d", conn_res->error_recno_tracker, conn_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);
        if (return_str != NULL)
        {
            retVal = StringOBJ_FromASCII(return_str);
            PyMem_Del(return_str);
            return_str = NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Created return value: %p", retVal);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit conn_error()");
        return retVal;
    }
    else
    {
        PyObject *defaultErrorState = StringOBJ_FromASCII(IBM_DB_G(__python_conn_err_state));
        snprintf(messageStr, sizeof(messageStr), "No connection object provided. Returning default error state: %s", PyUnicode_AsUTF8(defaultErrorState));
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit conn_error()");
        return StringOBJ_FromASCII(IBM_DB_G(__python_conn_err_state));
    }
}

/*!# ibm_db.stmt_error
 *
 * ===Description
 * string ibm_db.stmt_error ( [resource stmt] )
 *
 * Returns a string containing the SQLSTATE value returned by an SQL statement.
 *
 * If you do not pass a statement resource as an argument to
 * ibm_db.stmt_error(), the driver returns the SQLSTATE value associated with
 * the last attempt to return a statement resource, for example, from
 * ibm_db.prepare() or ibm_db.exec().
 *
 * To learn what the SQLSTATE value means, you can issue the following command
 * at a DB2 Command Line Processor prompt: db2 '? sqlstate-value'. You can also
 * call ibm_db.stmt_errormsg() to retrieve an explicit error message and the
 * associated SQLCODE value.
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid statement resource.
 *
 * ===Return Values
 *
 * Returns a string containing an SQLSTATE value.
 */
static PyObject *ibm_db_stmt_error(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry stmt_error()");
    LogUTF8Msg(args);
    stmt_handle *stmt_res = NULL;
    PyObject *py_stmt_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                              * _python_ibm_db_check_sql_errors to return err
                              * strings
                              */

    if (!PyArg_ParseTuple(args, "|O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, ", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        snprintf(messageStr, sizeof(messageStr), "Allocated return_str: %p, size: %d", return_str, DB2_MAX_ERR_MSG_LEN);
        LogMsg(DEBUG, messageStr);
        if (return_str != NULL)
        {
            memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);
            LogMsg(DEBUG, "Initialized return_str with zeros");
        }
        else
        {
            LogMsg(ERROR, "Failed to allocate memory for return_str");
            PyErr_SetString(PyExc_MemoryError, "Failed to allocate memory for error message string.");
            return NULL;
        }
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, -1, 0,
                                        return_str, DB2_ERR,
                                        stmt_res->error_recno_tracker);
        snprintf(messageStr, sizeof(messageStr), "SQL errors checked. return_str: %s", return_str);
        LogMsg(DEBUG, messageStr);

        if (stmt_res->error_recno_tracker - stmt_res->errormsg_recno_tracker >= 1)
        {
            LogMsg(DEBUG, "Updating errormsg_recno_tracker");
            stmt_res->errormsg_recno_tracker = stmt_res->error_recno_tracker;
        }
        stmt_res->error_recno_tracker++;
        snprintf(messageStr, sizeof(messageStr), "Updated error_recno_tracker: %d, errormsg_recno_tracker: %d", stmt_res->error_recno_tracker, stmt_res->errormsg_recno_tracker);
        LogMsg(DEBUG, messageStr);
        if (return_str != NULL)
        {
            retVal = StringOBJ_FromASCII(return_str);
            PyMem_Del(return_str);
            return_str = NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Created return value: %p", retVal);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit stmt_error()");
        return retVal;
    }
    else
    {
        PyObject *defaultErrorState = StringOBJ_FromASCII(IBM_DB_G(__python_stmt_err_state));
        snprintf(messageStr, sizeof(messageStr), "No Statement object provided. Returning default error state: %s", PyUnicode_AsUTF8(defaultErrorState));
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit conn_error()");
        return StringOBJ_FromASCII(IBM_DB_G(__python_stmt_err_state));
    }
}

/*!# ibm_db.next_result
 *
 * ===Description
 * resource ibm_db.next_result ( resource stmt )
 *
 * Requests the next result set from a stored procedure.
 *
 * A stored procedure can return zero or more result sets. While you handle the
 * first result set in exactly the same way you would handle the results
 * returned by a simple SELECT statement, to fetch the second and subsequent
 * result sets from a stored procedure you must call the ibm_db.next_result()
 * function and return the result to a uniquely named Python variable.
 *
 * ===Parameters
 * ====stmt
 *        A prepared statement returned from ibm_db.exec() or ibm_db.execute().
 *
 * ===Return Values
 *
 * Returns a new statement resource containing the next result set if the stored
 * procedure returned another result set. Returns FALSE if the stored procedure
 * did not return another result set.
 */
static PyObject *ibm_db_next_result(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry next_result()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res, *new_stmt_res = NULL;
    int rc = 0;
    SQLHANDLE new_hstmt;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        _python_ibm_db_clear_stmt_err_cache();

        /* alloc handle and return only if it errors */
#ifndef __MVS__
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, stmt_res->hdbc, &new_hstmt);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLAllocHandle with parameters SQL_HANDLE_STMT, hdbc=%p, &new_hstmt=%p and returned rc=%d",
                 (void *)stmt_res->hdbc, (void *)new_hstmt, rc);
        LogMsg(DEBUG, messageStr);
#endif
        if (rc < SQL_SUCCESS)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_INCREF(Py_False);
            return Py_False;
        }

        Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
        rc = SQLMoreResults((SQLHSTMT)stmt_res->hstmt);
        snprintf(messageStr, sizeof(messageStr), "Called SQLMoreResults with Parameter hstmt=%p and returned rc=%d", (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
#else
        rc = SQLNextResult((SQLHSTMT)stmt_res->hstmt, (SQLHSTMT)new_hstmt);
        snprintf(messageStr, sizeof(messageStr), "Called SQLNextResult with Parameter hstmt=%p, new_hstmt=%p and returned rc=%d", (void *)stmt_res->hstmt, (void *)new_hstmt, rc);
        LogMsg(DEBUG, messageStr);
#endif
        Py_END_ALLOW_THREADS;

        if (rc != SQL_SUCCESS)
        {
            if (rc < SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
#ifndef __MVS__
            Py_BEGIN_ALLOW_THREADS;
            SQLFreeHandle(SQL_HANDLE_STMT, new_hstmt);
            Py_END_ALLOW_THREADS;
            snprintf(messageStr, sizeof(messageStr), "Called SQLFreeHandle with parameters SQL_HANDLE_STMT, new_hstmt=%p and returned rc=%d", (void *)new_hstmt, rc);
            LogMsg(DEBUG, messageStr);
#endif
            Py_INCREF(Py_False);
            LogMsg(INFO, "exit next_result()");
            return Py_False;
        }

#ifdef __MVS__
        Py_BEGIN_ALLOW_THREADS;
        SQLFreeStmt(stmt_res->hstmt, SQL_UNBIND);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLFreeStmt with parameters hstmt=%p, SQL_UNBIND and returned rc=%d", (void *)stmt_res->hstmt, rc);
        LogMsg(DEBUG, messageStr);
#endif
        /* Initialize stmt resource members with default values. */
        /* Parsing will update options if needed */
        /* NOTE: This needs to match _ibm_db_new_stmt_struct */
        new_stmt_res = PyObject_NEW(stmt_handle, &stmt_handleType);
#ifdef __MVS__
        new_stmt_res->hstmt = stmt_res->hstmt;
#else
        new_stmt_res->hstmt = new_hstmt;
#endif
        new_stmt_res->hdbc = stmt_res->hdbc;

        new_stmt_res->s_bin_mode = stmt_res->s_bin_mode;
        new_stmt_res->cursor_type = stmt_res->cursor_type;
        new_stmt_res->s_case_mode = stmt_res->s_case_mode;
        new_stmt_res->s_use_wchar = stmt_res->s_use_wchar;

        new_stmt_res->head_cache_list = NULL;
        new_stmt_res->current_node = NULL;

        new_stmt_res->num_params = 0;
        new_stmt_res->file_param = 0;

        new_stmt_res->column_info = NULL;
        new_stmt_res->num_columns = 0;

        stmt_res->error_recno_tracker = 1;
        stmt_res->errormsg_recno_tracker = 1;

        new_stmt_res->row_data = NULL;
        LogMsg(INFO, "exit next_result()");
        return (PyObject *)new_stmt_res;
    }
    else
    {
        LogMsg(ERROR, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
}

/*!# ibm_db.num_fields
 *
 * ===Description
 * int ibm_db.num_fields ( resource stmt )
 *
 * Returns the number of fields contained in a result set. This is most useful
 * for handling the result sets returned by dynamically generated queries, or
 * for result sets returned by stored procedures, where your application cannot
 * otherwise know how to retrieve and use the results.
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid statement resource containing a result set.
 *
 * ===Return Values
 *
 * Returns an integer value representing the number of fields in the result set
 * associated with the specified statement resource. Returns FALSE if the
 * statement resource is not a valid input value.
 */
static PyObject *ibm_db_num_fields(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry num_fields()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;
    SQLSMALLINT indx = 0;
    char error[DB2_MAX_ERR_MSG_LEN + 50];

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLNumResultCols((SQLHSTMT)stmt_res->hstmt, &indx);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLNumResultCols return code: %d, Number of columns: %d", rc, indx);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "SQLNumResultCols failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            LogMsg(ERROR, error);
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        LogMsg(INFO, "exit num_fields()");
        return PyInt_FromLong(indx);
    }
    else
    {
        LogMsg(ERROR, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
}

/*!# ibm_db.num_rows
 *
 * ===Description
 * int ibm_db.num_rows ( resource stmt )
 *
 * Returns the number of rows deleted, inserted, or updated by an SQL statement.
 *
 * To determine the number of rows that will be returned by a SELECT statement,
 * issue SELECT COUNT(*) with the same predicates as your intended SELECT
 * statement and retrieve the value. If your application logic checks the number
 * of rows returned by a SELECT statement and branches if the number of rows is
 * 0, consider modifying your application to attempt to return the first row
 * with one of ibm_db.fetch_assoc(), ibm_db.fetch_both(), ibm_db.fetch_array(),
 * or ibm_db.fetch_row(), and branch if the fetch function returns FALSE.
 *
 * Note: If you issue a SELECT statement using a scrollable cursor,
 * ibm_db.num_rows() returns the number of rows returned by the SELECT
 * statement. However, the overhead associated with scrollable cursors
 * significantly degrades the performance of your application, so if this is the
 * only reason you are considering using scrollable cursors, you should use a
 * forward-only cursor and either call SELECT COUNT(*) or rely on the boolean
 * return value of the fetch functions to achieve the equivalent functionality
 * with much better performance.
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid stmt resource containing a result set.
 *
 * ===Return Values
 *
 * Returns the number of rows affected by the last SQL statement issued by the
 * specified statement handle.
 */
static PyObject *ibm_db_num_rows(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry num_rows()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;
    SQLINTEGER count = 0;
    char error[DB2_MAX_ERR_MSG_LEN + 50];

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLRowCount((SQLHSTMT)stmt_res->hstmt, &count);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLRowCount return code: %d, count: %d", rc, count);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            sprintf(error, "SQLRowCount failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            LogMsg(ERROR, error);
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Row count retrieved: %d", count);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit num_rows()");
        return PyInt_FromLong(count);
    }
    else
    {
        LogMsg(ERROR, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
}

/*!# ibm_db.get_num_result
 *
 * ===Description
 * int ibm_db.num_rows ( resource stmt )
 *
 * Returns the number of rows in a current open non-dynamic scrollable cursor.
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid stmt resource containing a result set.
 *
 * ===Return Values
 *
 * True on success or False on failure.
 */
static PyObject *ibm_db_get_num_result(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry get_num_result()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;
    SQLINTEGER count = 0;
    char error[DB2_MAX_ERR_MSG_LEN + 50];
    SQLSMALLINT strLenPtr;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object Parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valie. stmt_res=%p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }

        Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
        /* z/OS DB2 ODBC only seems to have SQLGetDiagRec */
        LogMsg(DEBUG, "This system not have SQLGetDiagRec. rc set to SQL_SUCCESS");
        rc = SQL_SUCCESS;
#else
        rc = SQLGetDiagField(SQL_HANDLE_STMT, stmt_res->hstmt, 0,
                             SQL_DIAG_CURSOR_ROW_COUNT, &count, SQL_IS_INTEGER,
                             &strLenPtr);
#endif
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "Called SQLGetDiagField with parameters: HandleType=SQL_HANDLE_STMT, StatementHandle=%p, RecordNumber=0, DiagField=SQL_DIAG_CURSOR_ROW_COUNT, RowCountPointer=%p, DataType=SQL_IS_INTEGER, StringLengthPointer=%p and returned rc=%d, count=%d",
                 stmt_res->hstmt, &count, &strLenPtr, rc, count);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }
        if (rc == SQL_ERROR)
        {
            sprintf(error, "SQLGetDiagField failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            LogMsg(ERROR, error);
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        snprintf(messageStr, sizeof(messageStr), "Returning row count: %d", count);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit get_num_result()");
        return PyInt_FromLong(count);
    }
    else
    {
        LogMsg(ERROR, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        LogMsg(INFO, "exit get_num_result()");
        return NULL;
    }
}

/* static int _python_ibm_db_get_column_by_name(stmt_handle *stmt_res, char *col_name, int col)
 */
static int _python_ibm_db_get_column_by_name(stmt_handle *stmt_res, char *col_name, int col)
{
    LogMsg(INFO, "entry _python_ibm_db_get_column_by_name()");
    snprintf(messageStr, sizeof(messageStr),
             "stmt_res=%p, col_name=%s, col=%d",
             (void *)stmt_res, col_name ? col_name : "NULL", col);
    LogMsg(DEBUG, messageStr);
    int i;
    /* get column header info */
    snprintf(messageStr, sizeof(messageStr), "Checking column_info: column_info=%p, num_columns=%d",
             (void *)stmt_res->column_info, stmt_res->num_columns);
    LogMsg(DEBUG, messageStr);
    if (stmt_res->column_info == NULL)
    {
        int result = _python_ibm_db_get_result_set_info(stmt_res);
        snprintf(messageStr, sizeof(messageStr),
                 "Result of _python_ibm_db_get_result_set_info: %d", result);
        LogMsg(DEBUG, messageStr);
        if (_python_ibm_db_get_result_set_info(stmt_res) < 0)
        {
            LogMsg(DEBUG, "Failed to get result set info");
            LogMsg(INFO, "exit _python_ibm_db_get_column_by_name()");
            return -1;
        }
    }
    if (col_name == NULL)
    {
        snprintf(messageStr, sizeof(messageStr),
                 "col_name is NULL, col=%d, num_columns=%d", col, stmt_res->num_columns);
        LogMsg(DEBUG, messageStr);
        if (col >= 0 && col < stmt_res->num_columns)
        {
            snprintf(messageStr, sizeof(messageStr), "Returning col=%d", col);
            LogMsg(DEBUG, messageStr);
            LogMsg(INFO, "exit _python_ibm_db_get_column_by_name()");
            return col;
        }
        else
        {
            LogMsg(DEBUG, "Invalid col index");
            LogMsg(INFO, "exit _python_ibm_db_get_column_by_name()");
            return -1;
        }
    }
    /* should start from 0 */
    snprintf(messageStr, sizeof(messageStr),
             "Searching for column name: col_name=%s", col_name);
    LogMsg(DEBUG, messageStr);
    i = 0;
    while (i < stmt_res->num_columns)
    {
        snprintf(messageStr, sizeof(messageStr), "Checking column %d: name=%s", i, stmt_res->column_info[i].name);
        LogMsg(DEBUG, messageStr);
        if (strcmp((char *)stmt_res->column_info[i].name, col_name) == 0)
        {
            snprintf(messageStr, sizeof(messageStr), "Found column: index=%d", i);
            LogMsg(DEBUG, messageStr);
            LogMsg(INFO, "exit _python_ibm_db_get_column_by_name()");
            return i;
        }
        i++;
    }
    LogMsg(DEBUG, "Column not found");
    LogMsg(INFO, "exit _python_ibm_db_get_column_by_name()");
    return -1;
}

/*!# ibm_db.field_name
 *
 * ===Description
 * string ibm_db.field_name ( resource stmt, mixed column )
 *
 * Returns the name of the specified column in the result set.
 *
 * ===Parameters
 *
 * ====stmt
 *        Specifies a statement resource containing a result set.
 *
 * ====column
 *        Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ===Return Values
 *
 * Returns a string containing the name of the specified column. If the
 * specified column does not exist in the result set, ibm_db.field_name()
 * returns FALSE.
 */
static PyObject *ibm_db_field_name(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_name()");
    LogUTF8Msg(args);
    PyObject *column = NULL;
    PyObject *result = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }

    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column index is an integer: %d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert Unicode column name to ASCII");
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column name is a string: %s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(ERROR, "Column argument has to be either an integer or string");
        LogMsg(INFO, "exit field_name()");
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if (col < 0)
    {
        LogMsg(DEBUG, "Column index not found");
        LogMsg(INFO, "exit field_name()");
        Py_INCREF(Py_False);
        return Py_False;
    }
#ifdef _WIN32
    result = PyUnicode_DecodeLocale((char *)stmt_res->column_info[col].name, "surrogateescape");
#else
    result = PyUnicode_FromString((char *)stmt_res->column_info[col].name);
#endif
    if (result)
    {
        const char *result_str = NULL;
#if PY_MAJOR_VERSION >= 3
        result_str = PyUnicode_AsUTF8(result);
#else
        result_str = PyString_AsString(result);
#endif

        if (result_str)
        {
            snprintf(messageStr, sizeof(messageStr), "Successfully retrieved column name: %s", result_str);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "Failed to convert result to UTF-8 string");
        }
        Py_DECREF(result);
    }
    else
    {
        LogMsg(ERROR, "Failed to create Unicode object from column name");
    }
    LogMsg(INFO, "exit field_name()");
#ifdef _WIN32
    return PyUnicode_DecodeLocale((char *)stmt_res->column_info[col].name, "surrogateescape");
#else
    return PyUnicode_FromString((char *)stmt_res->column_info[col].name);
#endif
}

/*!# ibm_db.field_display_size
 *
 * ===Description
 * int ibm_db.field_display_size ( resource stmt, mixed column )
 *
 * Returns the maximum number of bytes required to display a column in a result
 * set.
 *
 * ===Parameters
 * ====stmt
 *        Specifies a statement resource containing a result set.
 *
 * ====column
 *        Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ===Return Values
 *
 * Returns an integer value with the maximum number of bytes required to display
 * the specified column.
 * If the column does not exist in the result set, ibm_db.field_display_size()
 * returns FALSE.
 */
static PyObject *ibm_db_field_display_size(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_display_size()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    int col = -1;
    char *col_name = NULL;
    stmt_handle *stmt_res = NULL;
    int rc;
    SQLINTEGER colDataDisplaySize;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }

    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column is an integer: col=%d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert Unicode to ASCII");
            return NULL;
        }
        column = col_name_py3_tmp;
        snprintf(messageStr, sizeof(messageStr), "Converted column to ASCII: %s", PyBytes_AsString(column));
        LogMsg(DEBUG, messageStr);
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column is a string: col_name=%s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(ERROR, "Column argument has to be either an integer or string");
        LogMsg(INFO, "exit field_display_size()");
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
    snprintf(messageStr, sizeof(messageStr), "Column index after _python_ibm_db_get_column_by_name: %d", col);
    LogMsg(DEBUG, messageStr);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
        LogMsg(DEBUG, "Cleaned up col_name_py3_tmp");
    }
#endif
    if (col < 0)
    {
        LogMsg(ERROR, "Invalid column index");
        LogMsg(INFO, "exit field_display_size()");
        Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLColAttributes((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)col + 1,
                          SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &colDataDisplaySize);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLColAttributes return code: %d, colDataDisplaySize: %d", rc, colDataDisplaySize);
    LogMsg(DEBUG, messageStr);
    if (rc < SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    {
        LogMsg(ERROR, "SQLColAttributes failed");
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                        NULL, -1, 1);
    }
    if (rc < SQL_SUCCESS)
    {
        Py_INCREF(Py_False);
        LogMsg(INFO, "exit field_display_size()");
        return Py_False;
    }
    LogMsg(INFO, "exit field_display_size()");
    return PyInt_FromLong(colDataDisplaySize);
}
/*!# ibm_db.field_nullable
 *
 * ===Description
 * bool ibm_db.field_nullable ( resource stmt, mixed column )
 *
 * Returns True/False based on indicated column in result set is nullable or not.
 *
 * ===Parameters
 *
 * ====stmt
 *              Specifies a statement resource containing a result set.
 *
 * ====column
 *              Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ===Return Values
 *
 * Returns TRUE if indicated column is nullable else returns FALSE.
 * If the specified column does not exist in the result set, ibm_db.field_nullable() returns FALSE
 */
static PyObject *ibm_db_field_nullable(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_nullable()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle *stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;
    int rc;
    SQLINTEGER nullableCol;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res=%p", stmt_res);
        LogMsg(DEBUG, messageStr);
    }

    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column index parsed: col=%d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert column name to ASCII");
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column name parsed: col_name=%s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(INFO, "Invalid column argument. Must be an integer or string");
        Py_RETURN_FALSE;
    }
    snprintf(messageStr, sizeof(messageStr), "Calling _python_ibm_db_get_column_by_name with parameters: stmt_res=%p, col_name=%s, col=%d", stmt_res, col_name, col);
    LogMsg(DEBUG, messageStr);
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
    snprintf(messageStr, sizeof(messageStr),
             "_python_ibm_db_get_column_by_name returned: col=%d", col);
    LogMsg(DEBUG, messageStr);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if (col < 0)
    {
        LogMsg(INFO, "Invalid column index.");
        LogMsg(INFO, "exit field_nullable()");
        Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLColAttributes((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)col + 1,
                          SQL_DESC_NULLABLE, NULL, 0, NULL, &nullableCol);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "Called SQLColAttributes with parameters: StatementHandle=%p, ColumnIndex=%d, and returned rc=%d, NullableCol=%d",
             stmt_res->hstmt, col + 1, rc, nullableCol);
    LogMsg(DEBUG, messageStr);
    if (rc < SQL_SUCCESS)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                        NULL, -1, 1);
        Py_RETURN_FALSE;
    }
    else if (rc == SQL_SUCCESS_WITH_INFO)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                        NULL, -1, 1);
        Py_RETURN_FALSE;
    }
    else if (nullableCol == SQL_NULLABLE)
    {
        LogMsg(INFO, "Column is nullable.");
        LogMsg(INFO, "exit field_nullable()");
        Py_RETURN_TRUE;
    }
    else
    {
        LogMsg(INFO, "Column is not nullable.");
        LogMsg(INFO, "exit field_nullable()");
        Py_RETURN_FALSE;
    }
}
/*!# ibm_db.field_num
 *
 * ===Description
 * int ibm_db.field_num ( resource stmt, mixed column )
 *
 * Returns the position of the named column in a result set.
 *
 * ===Parameters
 *
 * ====stmt
 *        Specifies a statement resource containing a result set.
 *
 * ====column
 *        Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ===Return Values
 *
 * Returns an integer containing the 0-indexed position of the named column in
 * the result set. If the specified column does not exist in the result set,
 * ibm_db.field_num() returns FALSE.
 */
static PyObject *ibm_db_field_num(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_num()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle *stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }

    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column index is an integer: %d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert Unicode column name to ASCII");
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column name is a string: %s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(ERROR, "Column argument has to be either an integer or string");
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if (col < 0)
    {
        LogMsg(DEBUG, "Column index not found");
        Py_INCREF(Py_False);
        LogMsg(INFO, "exit field_num()");
        return Py_False;
    }
    snprintf(messageStr, sizeof(messageStr), "The 0-indexed position of the specified column is: %ld", col);
    LogMsg(INFO, messageStr);
    LogMsg(INFO, "exit field_num()");
    return PyInt_FromLong(col);
}

/*!# ibm_db.field_precision
 *
 * ===Description
 * int ibm_db.field_precision ( resource stmt, mixed column )
 *
 * Returns the precision of the indicated column in a result set.
 *
 * ===Parameters
 *
 * ====stmt
 *        Specifies a statement resource containing a result set.
 *
 * ====column
 *        Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ===Return Values
 *
 * Returns an integer containing the precision of the specified column. If the
 * specified column does not exist in the result set, ibm_db.field_precision()
 * returns FALSE.
 */
static PyObject *ibm_db_field_precision(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_precision()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle *stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }

    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column index is an integer: %d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert Unicode column name to ASCII");
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column name is a string: %s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(ERROR, "Column argument has to be either an integer or string");
        LogMsg(INFO, "exit field_precision()");
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    snprintf(messageStr, sizeof(messageStr), "Column index found: %d", col);
    LogMsg(DEBUG, messageStr);
    if (col < 0)
    {
        LogMsg(DEBUG, "Column index not found");
        LogMsg(INFO, "exit field_precision()");
        Py_RETURN_FALSE;
    }
    snprintf(messageStr, sizeof(messageStr), "Successfully retrieved field precision: %ld", stmt_res->column_info[col].size);
    LogMsg(INFO, messageStr);
    LogMsg(INFO, "exit field_precision()");
    return PyInt_FromLong(stmt_res->column_info[col].size);
}

/*!# ibm_db.field_scale
 *
 * ===Description
 * int ibm_db.field_scale ( resource stmt, mixed column )
 *
 * Returns the scale of the indicated column in a result set.
 *
 * ===Parameters
 * ====stmt
 *        Specifies a statement resource containing a result set.
 *
 * ====column
 *        Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ===Return Values
 *
 * Returns an integer containing the scale of the specified column. If the
 * specified column does not exist in the result set, ibm_db.field_scale()
 * returns FALSE.
 */
static PyObject *ibm_db_field_scale(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_scale()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle *stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }
    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column is an integer. col=%d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert column name to ASCII string");
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column is a string. col_name=%s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(ERROR, "Column argument must be an integer or string");
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
    snprintf(messageStr, sizeof(messageStr), "Column index obtained: col=%d", col);
    LogMsg(DEBUG, messageStr);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if (col < 0)
    {
        LogMsg(ERROR, "Column index is invalid");
        LogMsg(INFO, "exit field_scale()");
        Py_RETURN_FALSE;
    }
    long scale = stmt_res->column_info[col].scale;
    snprintf(messageStr, sizeof(messageStr), "Column scale: %ld", scale);
    LogMsg(INFO, messageStr);
    LogMsg(INFO, "exit field_scale()");
    return PyInt_FromLong(stmt_res->column_info[col].scale);
}

/*!# ibm_db.field_type
 *
 * ===Description
 * string ibm_db.field_type ( resource stmt, mixed column )
 *
 * Returns the data type of the indicated column in a result set.
 *
 * ===Parameters
 * ====stmt
 *        Specifies a statement resource containing a result set.
 *
 * ====column
 *        Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ====Return Values
 *
 * Returns a string containing the defined data type of the specified column.
 * If the specified column does not exist in the result set, ibm_db.field_type()
 * returns FALSE.
 */
static PyObject *ibm_db_field_type(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_type()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle *stmt_res = NULL;
    char *col_name = NULL;
    char *str_val = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }
    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column is an integer: %d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert column name to ASCII string");
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column is a string: %s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(ERROR, "Column argument must be an integer or string");
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    snprintf(messageStr, sizeof(messageStr), "Column index: %d", col);
    LogMsg(DEBUG, messageStr);
    if (col < 0)
    {
        LogMsg(ERROR, "Column index is negative, column not found");
        Py_RETURN_FALSE;
    }
    switch (stmt_res->column_info[col].type)
    {
    case SQL_SMALLINT:
    case SQL_INTEGER:
    case SQL_BIT:
        str_val = "int";
        break;
    case SQL_BIGINT:
        str_val = "bigint";
        break;
    case SQL_REAL:
    case SQL_FLOAT:
    case SQL_DOUBLE:
    case SQL_DECFLOAT:
        str_val = "real";
        break;
    case SQL_DECIMAL:
    case SQL_NUMERIC:
        str_val = "decimal";
        break;
    case SQL_CLOB:
        str_val = "clob";
        break;
    case SQL_DBCLOB:
        str_val = "dbclob";
        break;
    case SQL_BLOB:
        str_val = "blob";
        break;
    case SQL_XML:
        str_val = "xml";
        break;
    case SQL_TYPE_DATE:
        str_val = "date";
        break;
    case SQL_TYPE_TIME:
        str_val = "time";
        break;
    case SQL_TYPE_TIMESTAMP:
        str_val = "timestamp";
        break;
#ifndef __MVS__
    case SQL_BOOLEAN:
        str_val = "boolean";
        break;
#endif
    default:
        str_val = "string";
        break;
    }
    snprintf(messageStr, sizeof(messageStr), "Determined column type: %s", str_val);
    LogMsg(INFO, messageStr);
    LogMsg(INFO, "exit field_type()");
    return StringOBJ_FromASCII(str_val);
}

/*!# ibm_db.field_width
 *
 * ===Description
 * int ibm_db.field_width ( resource stmt, mixed column )
 *
 * Returns the width of the current value of the indicated column in a result
 * set. This is the maximum width of the column for a fixed-length data type, or
 * the actual width of the column for a variable-length data type.
 *
 * ===Parameters
 *
 * ====stmt
 *        Specifies a statement resource containing a result set.
 *
 * ====column
 *        Specifies the column in the result set. This can either be an integer
 * representing the 0-indexed position of the column, or a string containing the
 * name of the column.
 *
 * ===Return Values
 *
 * Returns an integer containing the width of the specified character or binary
 * data type column in a result set. If the specified column does not exist in
 * the result set, ibm_db.field_width() returns FALSE.
 */
static PyObject *ibm_db_field_width(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry field_width()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    int col = -1;
    char *col_name = NULL;
    stmt_handle *stmt_res = NULL;
    int rc;
    SQLINTEGER colDataSize;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }
    if (TYPE(column) == PYTHON_FIXNUM)
    {
        col = PyLong_AsLong(column);
        snprintf(messageStr, sizeof(messageStr), "Column is an integer: %d", col);
        LogMsg(DEBUG, messageStr);
    }
    else if (PyString_Check(column))
    {
#if PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL)
        {
            LogMsg(ERROR, "Failed to convert column name to ASCII string");
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
        snprintf(messageStr, sizeof(messageStr), "Column is a string: %s", col_name);
        LogMsg(DEBUG, messageStr);
    }
    else
    {
        /* Column argument has to be either an integer or string */
        LogMsg(ERROR, "Column argument must be an integer or string");
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL)
    {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    snprintf(messageStr, sizeof(messageStr), "Column index: %d", col);
    LogMsg(DEBUG, messageStr);
    if (col < 0)
    {
        LogMsg(ERROR, "Column index is negative, column not found");
        Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLColAttributes((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)col + 1,
                          SQL_DESC_LENGTH, NULL, 0, NULL, &colDataSize);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr),
             "Called SQLColAttributes with parameters: stmt_res->hstmt=%p, col=%d, SQL_DESC_LENGTH=%d and returned: rc=%d, colDataSize=%ld",
             (void *)stmt_res->hstmt, col + 1, SQL_DESC_LENGTH, rc, (long)colDataSize);
    LogMsg(DEBUG, messageStr);
    if (rc != SQL_SUCCESS)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                        NULL, -1, 1);
        PyErr_Clear();
        Py_RETURN_FALSE;
    }
    snprintf(messageStr, sizeof(messageStr), "Column width: %ld", (long)colDataSize);
    LogMsg(INFO, messageStr);
    LogMsg(INFO, "exit field_width()");
    return PyInt_FromLong(colDataSize);
}

/*!# ibm_db.cursor_type
 *
 * ===Description
 * int ibm_db.cursor_type ( resource stmt )
 *
 * Returns the cursor type used by a statement resource. Use this to determine
 * if you are working with a forward-only cursor or scrollable cursor.
 *
 * ===Parameters
 * ====stmt
 *        A valid statement resource.
 *
 * ===Return Values
 *
 * Returns either SQL_SCROLL_FORWARD_ONLY if the statement resource uses a
 * forward-only cursor or SQL_CURSOR_KEYSET_DRIVEN if the statement resource
 * uses a scrollable cursor.
 */
static PyObject *ibm_db_cursor_type(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry cursor_type()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res = NULL;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }
    long result = (stmt_res->cursor_type != SQL_SCROLL_FORWARD_ONLY);
    snprintf(messageStr, sizeof(messageStr), "Cursor type check result: %ld", result);
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit cursor_type()");
    return PyInt_FromLong(stmt_res->cursor_type != SQL_SCROLL_FORWARD_ONLY);
}

/*!# ibm_db.rollback
 *
 * ===Description
 * bool ibm_db.rollback ( resource connection )
 *
 * Rolls back an in-progress transaction on the specified connection resource
 * and begins a new transaction. Python applications normally default to
 * AUTOCOMMIT mode, so ibm_db.rollback() normally has no effect unless
 * AUTOCOMMIT has been turned off for the connection resource.
 *
 * Note: If the specified connection resource is a persistent connection, all
 * transactions in progress for all applications using that persistent
 * connection will be rolled back. For this reason, persistent connections are
 * not recommended for use in applications that require transactions.
 *
 * ===Parameters
 *
 * ====connection
 *        A valid database connection resource variable as returned from
 * ibm_db.connect() or ibm_db.pconnect().
 *
 * ===Return Values
 *
 * Returns TRUE on success or FALSE on failure.
 */
static PyObject *ibm_db_rollback(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry rollback()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res;
    int rc;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p", py_conn_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLEndTran(SQL_HANDLE_DBC, conn_res->hdbc, SQL_ROLLBACK);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLEndTran called with parambets SQL_HANDLE_DBC=%d, conn_res->hdbc=%p, SQL_ROLLBACK=%d and returned rc=%d",
                 SQL_HANDLE_DBC, (void *)conn_res->hdbc, SQL_ROLLBACK, rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            LogMsg(INFO, "Successfully completed rollback operation");
            LogMsg(INFO, "exit rollback()");
            Py_RETURN_TRUE;
        }
    }
    LogMsg(INFO, "exit rollback()");
    Py_RETURN_FALSE;
}

/*!# ibm_db.free_stmt
 *
 * ===Description
 * bool ibm_db.free_stmt ( resource stmt )
 *
 * Frees the system and database resources that are associated with a statement
 * resource. These resources are freed implicitly when a script finishes, but
 * you can call ibm_db.free_stmt() to explicitly free the statement resources
 * before the end of the script.
 *
 * ===Parameters
 * ====stmt
 *        A valid statement resource.
 *
 * ===Return Values
 *
 * Returns TRUE on success or FALSE on failure.
 *
 * DEPRECATED
 */
static PyObject *ibm_db_free_stmt(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry free_stmt()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    stmt_handle *handle;
    SQLRETURN rc;
    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p", py_stmt_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            handle = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle to be freed: handle->hstmt=%p", (void *)handle->hstmt);
            LogMsg(DEBUG, messageStr);
            if (handle->hstmt != -1)
            {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLFreeHandle(SQL_HANDLE_STMT, handle->hstmt);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLFreeHandle called with SQL_HANDLE_STMT abd handle=%p and returned rc=%d", (void *)handle->hstmt, rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
                {
                    _python_ibm_db_check_sql_errors(handle->hstmt,
                                                    SQL_HANDLE_STMT,
                                                    rc, 1, NULL, -1, 1);
                }
                if (rc == SQL_ERROR)
                {
                    Py_RETURN_FALSE;
                }
                _python_ibm_db_free_result_struct(handle);
                handle->hstmt = -1;
                LogMsg(INFO, "exit free_stmt()");
                Py_RETURN_TRUE;
            }
        }
    }
    LogMsg(INFO, "exit free_stmt()");
    Py_RETURN_NONE;
}

/*    static RETCODE _python_ibm_db_get_data(stmt_handle *stmt_res, int col_num, short ctype, void *buff, int in_length, SQLINTEGER *out_length) */
static RETCODE _python_ibm_db_get_data(stmt_handle *stmt_res, int col_num, short ctype, void *buff, int in_length, SQLINTEGER *out_length)
{
    LogMsg(INFO, "entry _python_ibm_db_get_data()");
    snprintf(messageStr, sizeof(messageStr), "stmt_res=%p, col_num=%d, ctype=%d, buff=%p, in_length=%d, out_length=%p",
             (void *)stmt_res, col_num, ctype, buff, in_length, (void *)out_length);
    LogMsg(DEBUG, messageStr);
    RETCODE rc = SQL_SUCCESS;
    snprintf(messageStr, sizeof(messageStr),
             "Calling SQLGetData: hstmt=%p, col_num=%d, ctype=%d, buff=%p, in_length=%d, out_length=%p",
             (void *)stmt_res->hstmt, col_num, ctype, buff, in_length, (void *)out_length);
    LogMsg(INFO, messageStr);
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLGetData((SQLHSTMT)stmt_res->hstmt, col_num, ctype, buff, in_length,
                    out_length);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLGetData returned: rc=%d", rc);
    if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                        NULL, -1, 1);
    }
    LogMsg(INFO, "exit _python_ibm_db_get_data()");
    return rc;
}

/*!# ibm_db.result
 *
 * ===Description
 * mixed ibm_db.result ( resource stmt, mixed column )
 *
 * Returns a single column from a row in the result set
 *
 * Use ibm_db.result() to return the value of a specified column in the current  * row of a result set. You must call ibm_db.fetch_row() before calling
 * ibm_db.result() to set the location of the result set pointer.
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid stmt resource.
 *
 * ====column
 *        Either an integer mapping to the 0-indexed field in the result set, or  * a string matching the name of the column.
 *
 * ===Return Values
 *
 * Returns the value of the requested field if the field exists in the result
 * set. Returns NULL if the field does not exist, and issues a warning.
 */
static PyObject *ibm_db_result(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry result()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    PyObject *retVal = NULL;
    stmt_handle *stmt_res;
    long col_num;
    RETCODE rc;
    void *out_ptr;
    DATE_STRUCT *date_ptr;
    TIME_STRUCT *time_ptr;
    TIMESTAMP_STRUCT *ts_ptr;
    char error[DB2_MAX_ERR_MSG_LEN + 50];
    SQLINTEGER in_length, out_length = -10; /* Initialize out_length to some
                                             * meaningless value
                                             * */
    SQLSMALLINT column_type, targetCType = SQL_C_CHAR, len_terChar = 0;
    double double_val;
    SQLINTEGER long_val;
    PyObject *return_value = NULL;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, column=%p", py_stmt_res, column);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(EXCEPTION, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }

        if (TYPE(column) == PYTHON_FIXNUM)
        {
            col_num = PyLong_AsLong(column);
            snprintf(messageStr, sizeof(messageStr), "Column number (integer): %ld", col_num);
            LogMsg(DEBUG, messageStr);
        }
        else if (PyString_Check(column))
        {
#if PY_MAJOR_VERSION >= 3
            col_name_py3_tmp = PyUnicode_AsASCIIString(column);
            if (col_name_py3_tmp == NULL)
            {
                LogMsg(ERROR, "Failed to convert column name to ASCII string");
                return NULL;
            }
            column = col_name_py3_tmp;
#endif
            snprintf(messageStr, sizeof(messageStr), "Column name: %s", PyBytes_AsString(column));
            LogMsg(DEBUG, messageStr);
            col_num = _python_ibm_db_get_column_by_name(stmt_res, PyBytes_AsString(column), -1);
#if PY_MAJOR_VERSION >= 3
            if (col_name_py3_tmp != NULL)
            {
                Py_XDECREF(col_name_py3_tmp);
            }
#endif
        }
        else
        {
            /* Column argument has to be either an integer or string */
            LogMsg(ERROR, "Column argument must be an integer or string");
            Py_RETURN_FALSE;
        }

        /* get column header info */
        if (stmt_res->column_info == NULL)
        {
            if (_python_ibm_db_get_result_set_info(stmt_res) < 0)
            {
                sprintf(error, "Column information cannot be retrieved: %s",
                        IBM_DB_G(__python_stmt_err_msg));
                strcpy(IBM_DB_G(__python_stmt_err_msg), error);
                LogMsg(ERROR, error);
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
        }

        if (col_num < 0 || col_num >= stmt_res->num_columns)
        {
            strcpy(IBM_DB_G(__python_stmt_err_msg), "Column ordinal out of range");
            LogMsg(ERROR, "Column ordinal out of range");
            PyErr_Clear();
            Py_RETURN_NONE;
        }

        /* get the data */
        column_type = stmt_res->column_info[col_num].type;
        snprintf(messageStr, sizeof(messageStr), "Processing column type: %d", column_type);
        LogMsg(DEBUG, messageStr);
        switch (column_type)
        {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_GRAPHIC:
        case SQL_VARGRAPHIC:
#ifndef PASE /* i5/OS SQL_LONGVARCHAR is SQL_VARCHAR */
        case SQL_LONGVARCHAR:
        case SQL_LONGVARGRAPHIC:
#endif /* PASE */
        case SQL_BIGINT:
        case SQL_DECIMAL:
        case SQL_NUMERIC:
        case SQL_DECFLOAT:
            if (column_type == SQL_DECIMAL || column_type == SQL_NUMERIC || column_type == SQL_BIGINT)
            {
                in_length = stmt_res->column_info[col_num].size +
                            stmt_res->column_info[col_num].scale + 2 + 1;
            }
            else
            {
                in_length = stmt_res->column_info[col_num].size + 1;
            }
            if (column_type == SQL_DECFLOAT)
            {
                in_length = MAX_DECFLOAT_LENGTH;
            }
            out_ptr = (SQLPOINTER)ALLOC_N(wchar_t, in_length);
            memset(out_ptr, 0, sizeof(wchar_t) * in_length);

            if (out_ptr == NULL)
            {
                LogMsg(ERROR, "Failed to allocate memory for data");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, SQL_C_WCHAR,
                                         out_ptr, in_length * sizeof(wchar_t), &out_length);
            snprintf(messageStr, sizeof(messageStr), "values received rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);

            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "An error occurred while retrieving data");
                if (out_ptr != NULL)
                {
                    PyMem_Del(out_ptr);
                    out_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA)
            {
                Py_INCREF(Py_None);
                return_value = Py_None;
            } // else if (column_type == SQL_BIGINT){
            //    return_value = PyLong_FromString(out_ptr, NULL, 0); }
            // Converting from Wchar string to long leads to data truncation
            // as it treats 00 in 2 bytes for each char as NULL
            else
            {
                return_value = getSQLWCharAsPyUnicodeObject(out_ptr, out_length);
                snprintf(messageStr, sizeof(messageStr), "Data content: %s", PyUnicode_AsUTF8(return_value));
                LogMsg(DEBUG, messageStr);
            }
            PyMem_Del(out_ptr);
            out_ptr = NULL;
            LogMsg(DEBUG, "exit result()");
            return return_value;

        case SQL_TYPE_DATE:
            date_ptr = ALLOC(DATE_STRUCT);
            if (date_ptr == NULL)
            {
                LogMsg(ERROR, "Failed to allocate memory for DATE_STRUCT");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, SQL_C_TYPE_DATE,
                                         date_ptr, sizeof(DATE_STRUCT), &out_length);
            snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data for DATE returned rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);

            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "An error occurred while retrieving DATE data");
                if (date_ptr != NULL)
                {
                    PyMem_Del(date_ptr);
                    date_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA)
            {
                PyMem_Del(date_ptr);
                date_ptr = NULL;
                Py_RETURN_NONE;
            }
            else
            {
                return_value = PyDate_FromDate(date_ptr->year, date_ptr->month, date_ptr->day);
                snprintf(messageStr, sizeof(messageStr), "Retrieved DATE value: %ld-%02ld-%02ld", date_ptr->year, date_ptr->month, date_ptr->day);
                LogMsg(DEBUG, messageStr);
                PyMem_Del(date_ptr);
                date_ptr = NULL;
                LogMsg(DEBUG, "exit result()");
                return return_value;
            }
            break;

        case SQL_TYPE_TIME:
            time_ptr = ALLOC(TIME_STRUCT);
            if (time_ptr == NULL)
            {
                LogMsg(ERROR, "Failed to allocate memory for TIME_STRUCT");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, SQL_C_TYPE_TIME,
                                         time_ptr, sizeof(TIME_STRUCT), &out_length);
            snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data for TIME returned rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);

            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "An error occurred while retrieving TIME data");
                if (time_ptr != NULL)
                {
                    PyMem_Del(time_ptr);
                    time_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }

            if (out_length == SQL_NULL_DATA)
            {
                PyMem_Del(time_ptr);
                time_ptr = NULL;
                Py_RETURN_NONE;
            }
            else
            {
                return_value = PyTime_FromTime(time_ptr->hour % 24, time_ptr->minute, time_ptr->second, 0);
                snprintf(messageStr, sizeof(messageStr), "Retrieved TIME value: %02d:%02d:%02d", time_ptr->hour, time_ptr->minute, time_ptr->second);
                LogMsg(DEBUG, messageStr);
                PyMem_Del(time_ptr);
                time_ptr = NULL;
                LogMsg(INFO, "exit result()");
                return return_value;
            }
            break;

        case SQL_TYPE_TIMESTAMP:
            ts_ptr = ALLOC(TIMESTAMP_STRUCT);
            if (ts_ptr == NULL)
            {
                LogMsg(ERROR, "Failed to allocate memory for TIMESTAMP_STRUCT");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, SQL_C_TYPE_TIMESTAMP,
                                         ts_ptr, sizeof(TIMESTAMP_STRUCT), &out_length);
            snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data for TIMESTAMP returned rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);

            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "An error occurred while retrieving TIMESTAMP data");
                if (ts_ptr != NULL)
                {
                    PyMem_Del(ts_ptr);
                    time_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }

            if (out_length == SQL_NULL_DATA)
            {
                PyMem_Del(ts_ptr);
                ts_ptr = NULL;
                Py_RETURN_NONE;
            }
            else
            {
                return_value = PyDateTime_FromDateAndTime(ts_ptr->year, ts_ptr->month, ts_ptr->day, ts_ptr->hour % 24, ts_ptr->minute, ts_ptr->second, ts_ptr->fraction / 1000);
                snprintf(messageStr, sizeof(messageStr), "Retrieved TIMESTAMP value: %ld-%02ld-%02ld %02d:%02d:%02d.%03d",
                         ts_ptr->year, ts_ptr->month, ts_ptr->day, ts_ptr->hour, ts_ptr->minute, ts_ptr->second, ts_ptr->fraction / 1000);
                LogMsg(DEBUG, messageStr);
                PyMem_Del(ts_ptr);
                ts_ptr = NULL;
                LogMsg(DEBUG, "exit result()");
                return return_value;
            }
            break;

#ifdef __MVS__
        case SQL_SMALLINT:
        case SQL_INTEGER:
#else
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_BOOLEAN:
#endif
            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, SQL_C_LONG,
                                         &long_val, sizeof(long_val),
                                         &out_length);
            snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data returned rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "An error occurred while retrieving BOOLEAN data");
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA)
            {
                Py_RETURN_NONE;
            }
            else
            {
                snprintf(messageStr, sizeof(messageStr), "Retrieved BOOLEAN value: %ld", long_val);
                LogMsg(DEBUG, messageStr);
                LogMsg(INFO, "exit result()");
                return PyInt_FromLong(long_val);
            }
            break;

        case SQL_BIT:
            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, SQL_C_LONG,
                                         &long_val, sizeof(long_val),
                                         &out_length);
            snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data returned rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "An error occurred while retrieving BIT data");
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA)
            {
                Py_RETURN_NONE;
            }
            else
            {
                snprintf(messageStr, sizeof(messageStr), "Retrieved BIT value: %ld", long_val);
                LogMsg(DEBUG, messageStr);
                LogMsg(INFO, "exit result()");
                return PyBool_FromLong(long_val);
            }
            break;

        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, SQL_C_DOUBLE,
                                         &double_val, sizeof(double_val),
                                         &out_length);
            snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data returned rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_ERROR)
            {
                LogMsg(ERROR, "An error occurred while retrieving REAL/FLOAT/DOUBLE data");
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA)
            {
                Py_RETURN_NONE;
            }
            else
            {
                snprintf(messageStr, sizeof(messageStr), "Retrieved REAL/FLOAT/DOUBLE value: %f", double_val);
                LogMsg(DEBUG, messageStr);
                LogMsg(INFO, "exit result()");
                return PyFloat_FromDouble(double_val);
            }
            break;

        case SQL_BLOB:
        case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARCHAR is SQL_VARCHAR */
        case SQL_LONGVARBINARY:
#endif /* PASE */
        case SQL_VARBINARY:
            switch (stmt_res->s_bin_mode)
            {
            case PASSTHRU:
                LogMsg(DEBUG, "SQL_BLOB/SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY - PASSTHRU mode, returning empty bytes");
                LogMsg(INFO, "exit result()");
                return PyBytes_FromStringAndSize("", 0);
                break;
                /* returns here */
            case CONVERT:
                targetCType = SQL_C_CHAR;
                len_terChar = sizeof(char);
                LogMsg(DEBUG, "SQL_BLOB/SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY - CONVERT mode, using SQL_C_CHAR");
                break;
            case BINARY:
                targetCType = SQL_C_BINARY;
                len_terChar = 0;
                LogMsg(DEBUG, "SQL_BLOB/SQL_BINARY/SQL_LONGVARBINARY/SQL_VARBINARY - BINARY mode, using SQL_C_BINARY");
                break;
            default:
                LogMsg(INFO, "exit result()");
                Py_RETURN_FALSE;
            }
            break;
        case SQL_XML:
        case SQL_CLOB:
        case SQL_DBCLOB:
            if (column_type == SQL_CLOB || column_type == SQL_DBCLOB || column_type == SQL_XML)
            {
                len_terChar = sizeof(SQLWCHAR);
                targetCType = SQL_C_WCHAR;
                LogMsg(DEBUG, "Setting len_terChar to sizeof(SQLWCHAR) and targetCType to SQL_C_WCHAR");
            }
            out_ptr = ALLOC_N(char, INIT_BUFSIZ + len_terChar);
            if (out_ptr == NULL)
            {
                LogMsg(ERROR, "Failed to allocate memory for XML data");
                PyErr_SetString(PyExc_Exception,
                                "Failed to Allocate Memory for XML Data");
                return NULL;
            }
            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, targetCType, out_ptr,
                                         INIT_BUFSIZ + len_terChar, &out_length);
            snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data returned rc: %d, out_length: %d", rc, out_length);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                void *tmp_out_ptr = NULL;

                tmp_out_ptr = ALLOC_N(char, out_length + INIT_BUFSIZ + len_terChar);
                memcpy(tmp_out_ptr, out_ptr, INIT_BUFSIZ);
                PyMem_Del(out_ptr);
                out_ptr = tmp_out_ptr;

                rc = _python_ibm_db_get_data(stmt_res, col_num + 1, targetCType, (char *)out_ptr + INIT_BUFSIZ,
                                             out_length + len_terChar, &out_length);
                snprintf(messageStr, sizeof(messageStr), "SQL_Get_Data continued with rc: %d, out_length: %d", rc, out_length);
                LogMsg(DEBUG, messageStr);

                if (rc == SQL_ERROR)
                {
                    LogMsg(ERROR, "An error occurred while retrieving XML/CLOB/DBCLOB data");
                    PyMem_Del(out_ptr);
                    out_ptr = NULL;
                    return NULL;
                }
                if (len_terChar == sizeof(SQLWCHAR))
                {
                    retVal = getSQLWCharAsPyUnicodeObject(out_ptr, INIT_BUFSIZ + out_length);
                }
                else
                {
                    retVal = PyBytes_FromStringAndSize((char *)out_ptr, INIT_BUFSIZ + out_length);
                }
            }
            else if (rc == SQL_ERROR)
            {
                PyMem_Del(out_ptr);
                out_ptr = NULL;
                LogMsg(INFO, "exit result()");
                Py_RETURN_FALSE;
            }
            else
            {
                if (out_length == SQL_NULL_DATA)
                {
                    Py_INCREF(Py_None);
                    retVal = Py_None;
                }
                else
                {
                    if (len_terChar == 0)
                    {
                        LogMsg(INFO, "Processing SQLWCHAR data");
                        retVal = PyBytes_FromStringAndSize((char *)out_ptr, out_length);
                    }
                    else
                    {
                        LogMsg(INFO, "Processing byte data");
                        retVal = getSQLWCharAsPyUnicodeObject(out_ptr, out_length);
                    }
                }
            }
            if (out_ptr != NULL)
            {
                PyMem_Del(out_ptr);
                out_ptr = NULL;
            }
            LogMsg(INFO, "exit result()");
            return retVal;
        default:
            break;
        }
    }
    else
    {
        LogMsg(ERROR, "Supplied parameter is invalid");
        LogMsg(INFO, "exit result()");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
    }
    LogMsg(INFO, "exit result()");
    Py_RETURN_FALSE;
}

/* static void _python_ibm_db_bind_fetch_helper(INTERNAL_FUNCTION_PARAMETERS,
                                                int op)
*/
static PyObject *_python_ibm_db_bind_fetch_helper(PyObject *args, int op)
{
    LogMsg(INFO, "entry _python_ibm_db_bind_fetch_helper()");
    int rc = -1;
    int column_number;
    SQLINTEGER row_number = -1;
    stmt_handle *stmt_res = NULL;
    SQLSMALLINT column_type;
    ibm_db_row_data_type *row_data;
    SQLINTEGER out_length, tmp_length = 0;
    void *out_ptr = NULL;
    SQLWCHAR *wout_ptr = NULL;
    int len_terChar = 0;
    SQLSMALLINT targetCType = SQL_C_CHAR;
    PyObject *py_stmt_res = NULL;
    PyObject *return_value = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    PyObject *py_row_number = NULL;
    char error[DB2_MAX_ERR_MSG_LEN + 50];

    if (!PyArg_ParseTuple(args, "O|O", &py_stmt_res, &py_row_number))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, py_row_number=%p", py_stmt_res, py_row_number);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle set. Address of stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }

    if (!NIL_P(py_row_number))
    {
        if (PyInt_Check(py_row_number))
        {
            row_number = (SQLINTEGER)PyLong_AsLong(py_row_number);
            snprintf(messageStr, sizeof(messageStr), "row_number: %d", row_number);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            LogMsg(EXCEPTION, "Supplied parameter is invalid");
            return NULL;
        }
    }
    if (op == FETCH_INDEX && row_number > 0)
    {
        row_number = 0;
    }
    _python_ibm_db_init_error_info(stmt_res);

    /* get column header info */
    if (stmt_res->column_info == NULL)
    {
        if (_python_ibm_db_get_result_set_info(stmt_res) < 0)
        {
            sprintf(error, "Column information cannot be retrieved: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            LogMsg(ERROR, error);
            return NULL;
        }
    }
    /* bind the data */
    if (stmt_res->row_data == NULL)
    {
        rc = _python_ibm_db_bind_column_helper(stmt_res);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        {
            sprintf(error, "Column binding cannot be done: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            LogMsg(ERROR, error);
            return NULL;
        }
    }
    /* check if row_number is present */
    if (PyTuple_Size(args) == 2 && row_number > 0)
    {
#ifndef PASE /* i5/OS problem with SQL_FETCH_ABSOLUTE (temporary until fixed) */
        if (is_systemi)
        {

            LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_FIRST");
            snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                     row_number);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_FIRST,
                                row_number);
            snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1,
                                                NULL, -1, 1);
            }
            Py_END_ALLOW_THREADS;

            if (row_number > 1 && (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO))
            {
                LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_RELATIVE");
                snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                         row_number - 1);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_RELATIVE,
                                    row_number - 1);
                snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_SUCCESS_WITH_INFO)
                {
                    _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                    rc, 1,
                                                    NULL, -1, 1);
                }
                Py_END_ALLOW_THREADS;
            }
        }
        else
        {
            LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_ABSOLUTE");
            snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                     row_number);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_ABSOLUTE,
                                row_number);
            snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1,
                                                NULL, -1, 1);
            }
            Py_END_ALLOW_THREADS;
        }
#else  /* PASE */
        LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_FIRST");
        snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                 row_number);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_FIRST,
                            row_number);
        snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1,
                                            NULL, -1, 1);
        }
        Py_END_ALLOW_THREADS;

        if (row_number > 1 && (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO))
        {
            LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_RELATIVE");
            snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                     row_number - 1);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;

            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_RELATIVE,
                                row_number - 1);
            snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1,
                                                NULL, -1, 1);
            }
            Py_END_ALLOW_THREADS;
        }
#endif /* PASE */
    }
    else if (PyTuple_Size(args) == 2 && row_number < 0)
    {
        PyErr_SetString(PyExc_Exception,
                        "Requested row number must be a positive value");
        LogMsg(EXCEPTION, "Requested row number must be a positive value");
        return NULL;
    }
    else
    {
        /* row_number is NULL or 0; just fetch next row */
        LogMsg(DEBUG, "Calling SQLFetch, for fetching next row");
        snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p", (void *)stmt_res->hstmt);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;

        rc = SQLFetch((SQLHSTMT)stmt_res->hstmt);
        snprintf(messageStr, sizeof(messageStr), "SQLFetch returned with rc: %d", rc);
        if (rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
        }

        Py_END_ALLOW_THREADS;
    }

    if (rc == SQL_NO_DATA_FOUND)
    {
        Py_INCREF(Py_False);
        LogMsg(INFO, "exit _python_ibm_db_bind_fetch_helper()");
        if (op == FETCH_ASSOC)
        {
            LogMsg(INFO, "exit fetch_assoc()");
        }
        if (op == FETCH_INDEX)
        {
            LogMsg(INFO, "exit fetch_tuple()");
        }
        if (op == FETCH_BOTH)
        {
            LogMsg(INFO, "exit fetch_both()");
        }
        return Py_False;
    }
    else if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                        NULL, -1, 1);
        sprintf(error, "Fetch Failure: %s", IBM_DB_G(__python_stmt_err_msg));
        LogMsg(ERROR, error);
        PyErr_SetString(PyExc_Exception, error);
        return NULL;
    }
    if (rc == SQL_SUCCESS_WITH_INFO)
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                        rc, 1, NULL, -1, 1);
    }
    /* copy the data over return_value */
    if (op & FETCH_ASSOC)
    {
        LogMsg(INFO, "Creating dict for FETCH_ASSOC");
        return_value = PyDict_New();
    }
    else if (op == FETCH_INDEX)
    {
        snprintf(messageStr, sizeof(messageStr), "Creating tuple for FETCH_INDEX with size %d", stmt_res->num_columns);
        LogMsg(INFO, messageStr);
        return_value = PyTuple_New(stmt_res->num_columns);
    }

    for (column_number = 0; column_number < stmt_res->num_columns; column_number++)
    {
        column_type = stmt_res->column_info[column_number].type;
        row_data = &stmt_res->row_data[column_number].data;
        out_length = stmt_res->row_data[column_number].out_length;

        snprintf(messageStr, sizeof(messageStr), "Processing column %d: type=%d, out_length=%d", column_number, column_type, out_length);
        LogMsg(DEBUG, messageStr);
        switch (stmt_res->s_case_mode)
        {
        case CASE_LOWER:
            LogMsg(INFO, "Column name changing to lower case");
            stmt_res->column_info[column_number].name =
                (SQLCHAR *)strtolower((char *)stmt_res->column_info[column_number].name,
                                      strlen((char *)stmt_res->column_info[column_number].name));
            break;
        case CASE_UPPER:
            LogMsg(INFO, "Column name changing to upper case");
            stmt_res->column_info[column_number].name =
                (SQLCHAR *)strtoupper((char *)stmt_res->column_info[column_number].name,
                                      strlen((char *)stmt_res->column_info[column_number].name));
            break;
        case CASE_NATURAL:
        default:
            LogMsg(INFO, "Column name unchanged");
            break;
        }
        if (out_length == SQL_NULL_DATA)
        {
            LogMsg(DEBUG, "Column data is SQL_NULL_DATA");
            Py_INCREF(Py_None);
            value = Py_None;
        }
        else
        {
            switch (column_type)
            {
            case SQL_CHAR:
            case SQL_VARCHAR:
                if (stmt_res->s_use_wchar == WCHAR_NO)
                {
                    tmp_length = stmt_res->column_info[column_number].size;
                    value = PyBytes_FromStringAndSize((char *)row_data->str_val, out_length);
                    snprintf(messageStr, sizeof(messageStr), "Column data converted to bytes, with value: %p", (void *)value);
                    LogMsg(DEBUG, messageStr);
                    break;
                }
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_GRAPHIC:
            case SQL_VARGRAPHIC:
            case SQL_LONGVARGRAPHIC:
                tmp_length = stmt_res->column_info[column_number].size;
                value = getSQLWCharAsPyUnicodeObject(row_data->w_val, out_length);
                snprintf(messageStr, sizeof(messageStr), "Column data converted to unicode, with value: %p", (void *)value);
                LogMsg(DEBUG, messageStr);
                break;

#ifndef PASE /* i5/OS SQL_LONGVARCHAR is SQL_VARCHAR */
            case SQL_LONGVARCHAR:
            case SQL_WLONGVARCHAR:

#endif /* PASE */
                /* i5/OS will xlate from EBCIDIC to ASCII (via SQLGetData) */
                tmp_length = stmt_res->column_info[column_number].size;
                snprintf(messageStr, sizeof(messageStr), "Allocating memory for column %d with size %d", column_number, tmp_length + 1);
                LogMsg(DEBUG, messageStr);
                wout_ptr = (SQLWCHAR *)ALLOC_N(SQLWCHAR, tmp_length + 1);
                if (wout_ptr == NULL)
                {
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    LogMsg(EXCEPTION, "Failed to Allocate Memory");
                    return NULL;
                }

                /*  _python_ibm_db_get_data null terminates all output. */
                LogMsg(DEBUG, "Fetching data for column");
                rc = _python_ibm_db_get_data(stmt_res, column_number + 1, SQL_C_WCHAR, wout_ptr,
                                             (tmp_length * sizeof(SQLWCHAR) + 1), &out_length);
                snprintf(messageStr, sizeof(messageStr), "Fetch result for column %d: rc=%d, out_length=%d", column_number, rc, out_length);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    return NULL;
                }
                if (out_length == SQL_NULL_DATA)
                {
                    Py_INCREF(Py_None);
                    value = Py_None;
                }
                else
                {
                    value = getSQLWCharAsPyUnicodeObject(wout_ptr, out_length);
                    LogMsg(DEBUG, "Column data converted to unicode after fetch");
                }
                if (wout_ptr != NULL)
                {
                    PyMem_Del(wout_ptr);
                    wout_ptr = NULL;
                }
                break;

            case SQL_DECIMAL:
            case SQL_NUMERIC:
            case SQL_DECFLOAT:
                value = StringOBJ_FromASCIIAndSize((char *)row_data->str_val, out_length);
                LogMsg(DEBUG, "Column data converted to decimal");
                break;

            case SQL_TYPE_DATE:
                value = PyDate_FromDate(row_data->date_val->year, row_data->date_val->month, row_data->date_val->day);
                LogMsg(DEBUG, "Column data converted to date");
                break;

            case SQL_TYPE_TIME:
                value = PyTime_FromTime(row_data->time_val->hour % 24, row_data->time_val->minute, row_data->time_val->second, 0);
                LogMsg(DEBUG, "Column data converted to time");
                break;

            case SQL_TYPE_TIMESTAMP:
                value = PyDateTime_FromDateAndTime(row_data->ts_val->year, row_data->ts_val->month, row_data->ts_val->day,
                                                   row_data->ts_val->hour % 24, row_data->ts_val->minute, row_data->ts_val->second,
                                                   row_data->ts_val->fraction / 1000);
                snprintf(messageStr, sizeof(messageStr),
                         "Column data converted to timestamp: year=%d, month=%d, day=%d, hour=%d, minute=%d, second=%d, fraction=%d",
                         row_data->ts_val->year, row_data->ts_val->month, row_data->ts_val->day,
                         row_data->ts_val->hour % 24, row_data->ts_val->minute, row_data->ts_val->second,
                         row_data->ts_val->fraction / 1000);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
                value = format_timestamp_pystr(row_data->tstz_val);
                snprintf(messageStr, sizeof(messageStr),"Column data converted to timestamp_with_timezone: year=%d, month=%d, day=%d, hour=%d, minute=%d,"
                         " second=%d, fraction=%d, timezone_hour=%d, timezone_minute=%d",row_data->tstz_val->year, row_data->tstz_val->month,
                         row_data->tstz_val->day, row_data->tstz_val->hour % 24, row_data->tstz_val->minute, row_data->tstz_val->second,
                         row_data->tstz_val->fraction / 1000, row_data->tstz_val->timezone_hour, row_data->tstz_val->timezone_minute);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_BIGINT:
                value = PyLong_FromString((char *)row_data->str_val, NULL, 10);
                LogMsg(DEBUG, "Column data converted to string");
                break;

#ifdef __MVS__
            case SQL_SMALLINT:
#else
            case SQL_SMALLINT:
            case SQL_BOOLEAN:
#endif
                value = PyInt_FromLong(row_data->s_val);
                snprintf(messageStr, sizeof(messageStr), "SQL_SMALLINT/SQL_BOOLEAN: value=%ld", row_data->s_val);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_INTEGER:
                value = PyInt_FromLong(row_data->i_val);
                snprintf(messageStr, sizeof(messageStr), "SQL_INTEGER: value=%ld", row_data->i_val);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_BIT:
                value = PyBool_FromLong(row_data->i_val);
                snprintf(messageStr, sizeof(messageStr), "SQL_BIT: value=%s", row_data->i_val ? "True" : "False");
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_REAL:
                value = PyFloat_FromDouble(row_data->r_val);
                snprintf(messageStr, sizeof(messageStr), "SQL_REAL: value=%f", row_data->r_val);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_FLOAT:
                value = PyFloat_FromDouble(row_data->f_val);
                snprintf(messageStr, sizeof(messageStr), "SQL_FLOAT: value=%f", row_data->f_val);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_DOUBLE:
                value = PyFloat_FromDouble(row_data->d_val);
                snprintf(messageStr, sizeof(messageStr), "SQL_DOUBLE: value=%f", row_data->d_val);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
            case SQL_LONGVARBINARY:
#endif /* PASE */
            case SQL_VARBINARY:
                if (stmt_res->s_bin_mode == PASSTHRU)
                {
                    value = PyBytes_FromStringAndSize("", 0);
                }
                else
                {
                    value = PyBytes_FromStringAndSize((char *)row_data->str_val, out_length);
                }
                snprintf(messageStr, sizeof(messageStr), "SQL_BINARY/SQL_VARBINARY: value size=%d", out_length);
                LogMsg(DEBUG, messageStr);
                break;

            case SQL_BLOB:
                snprintf(messageStr, sizeof(messageStr), "SQL_BLOB: Binary Mode = %d", stmt_res->s_bin_mode);
                LogMsg(DEBUG, messageStr);
                switch (stmt_res->s_bin_mode)
                {
                case PASSTHRU:
                    LogMsg(DEBUG, "SQL_BLOB: PASSTHRU mode selected");
                    Py_RETURN_NONE;
                    break;
                case CONVERT:
                    len_terChar = sizeof(char);
                    targetCType = SQL_C_CHAR;
                    snprintf(messageStr, sizeof(messageStr), "SQL_BLOB: CONVERT mode selected. len_terChar = %d, targetCType = %d", len_terChar, targetCType);
                    LogMsg(DEBUG, messageStr);
                    break;
                case BINARY:
                    len_terChar = 0;
                    targetCType = SQL_C_BINARY;
                    snprintf(messageStr, sizeof(messageStr), "SQL_BLOB: BINARY mode selected. len_terChar = %d, targetCType = %d", len_terChar, targetCType);
                    LogMsg(DEBUG, messageStr);
                    break;
                default:
                    len_terChar = -1;
                    snprintf(messageStr, sizeof(messageStr), "SQL_BLOB: Default mode selected. len_terChar = %d", len_terChar);
                    LogMsg(DEBUG, messageStr);
                    break;
                }
            case SQL_XML:
            case SQL_CLOB:
            case SQL_DBCLOB:
                if (column_type == SQL_CLOB || column_type == SQL_DBCLOB || column_type == SQL_XML)
                {
                    len_terChar = sizeof(SQLWCHAR);
                    targetCType = SQL_C_WCHAR;
                    snprintf(messageStr, sizeof(messageStr), "SQL_DBCLOB: Conversion settings. len_terChar = %d, targetCType = %d", len_terChar, targetCType);
                    LogMsg(DEBUG, messageStr);
                }
                else if (len_terChar == -1)
                {
                    break;
                }
                out_ptr = (void *)ALLOC_N(char, INIT_BUFSIZ + len_terChar);
                if (out_ptr == NULL)
                {
                    snprintf(messageStr, sizeof(messageStr), "SQL_DBCLOB: Failed to Allocate Memory for LOB Data. INIT_BUFSIZ = %d, len_terChar = %d", INIT_BUFSIZ, len_terChar);
                    LogMsg(ERROR, messageStr);
                    PyErr_SetString(PyExc_Exception,
                                    "Failed to Allocate Memory for LOB Data");
                    return NULL;
                }
                rc = _python_ibm_db_get_data(stmt_res, column_number + 1, targetCType, out_ptr,
                                             INIT_BUFSIZ + len_terChar, &out_length);
                snprintf(messageStr, sizeof(messageStr), "Fetching data: rc = %d, INIT_BUFSIZ = %d, len_terChar = %d, out_length = %d", rc, INIT_BUFSIZ, len_terChar, out_length);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_SUCCESS_WITH_INFO)
                {
                    void *tmp_out_ptr = NULL;

                    snprintf(messageStr, sizeof(messageStr), "SQL_SUCCESS_WITH_INFO received. Preparing to allocate more memory. Initial out_length = %d", out_length);
                    LogMsg(DEBUG, messageStr);
                    tmp_out_ptr = (void *)ALLOC_N(char, out_length + INIT_BUFSIZ + len_terChar);
                    memcpy(tmp_out_ptr, out_ptr, INIT_BUFSIZ);
                    PyMem_Del(out_ptr);
                    out_ptr = tmp_out_ptr;

                    rc = _python_ibm_db_get_data(stmt_res, column_number + 1, targetCType, (char *)out_ptr + INIT_BUFSIZ,
                                                 out_length + len_terChar, &out_length);
                    snprintf(messageStr, sizeof(messageStr), "Second fetch attempt: rc = %d, out_length = %d", rc, out_length);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_ERROR)
                    {
                        if (out_ptr != NULL)
                        {
                            PyMem_Del(out_ptr);
                            out_ptr = NULL;
                        }
                        sprintf(error, "Failed to fetch LOB Data: %s",
                                IBM_DB_G(__python_stmt_err_msg));
                        PyErr_SetString(PyExc_Exception, error);
                        LogMsg(ERROR, error);
                        return NULL;
                    }

                    if (len_terChar == sizeof(SQLWCHAR))
                    {
                        snprintf(messageStr, sizeof(messageStr), "Processing SQLWCHAR data: len_terChar = %d", len_terChar);
                        LogMsg(DEBUG, messageStr);
                        value = getSQLWCharAsPyUnicodeObject(out_ptr, INIT_BUFSIZ + out_length);
                    }
                    else
                    {
                        snprintf(messageStr, sizeof(messageStr), "Processing binary data: len_terChar = %d", len_terChar);
                        LogMsg(DEBUG, messageStr);
                        value = PyBytes_FromStringAndSize((char *)out_ptr, INIT_BUFSIZ + out_length);
                    }
                }
                else if (rc == SQL_ERROR)
                {
                    PyMem_Del(out_ptr);
                    out_ptr = NULL;
                    sprintf(error, "Failed to LOB Data: %s",
                            IBM_DB_G(__python_stmt_err_msg));
                    PyErr_SetString(PyExc_Exception, error);
                    LogMsg(ERROR, error);
                    return NULL;
                }
                else
                {
                    if (out_length == SQL_NULL_DATA)
                    {
                        Py_INCREF(Py_None);
                        value = Py_None;
                    }
                    else
                    {
                        if (len_terChar == sizeof(SQLWCHAR))
                        {
                            snprintf(messageStr, sizeof(messageStr), "Processing SQLWCHAR data: len_terChar = %d", len_terChar);
                            LogMsg(DEBUG, messageStr);
                            value = getSQLWCharAsPyUnicodeObject(out_ptr, out_length);
                        }
                        else
                        {
                            snprintf(messageStr, sizeof(messageStr), "Processing binary data: len_terChar = %d", len_terChar);
                            LogMsg(DEBUG, messageStr);
                            value = PyBytes_FromStringAndSize((char *)out_ptr, out_length);
                        }
                    }
                }
                if (out_ptr != NULL)
                {
                    PyMem_Del(out_ptr);
                    out_ptr = NULL;
                }
                break;

            default:
                Py_INCREF(Py_None);
                value = Py_None;
                break;
            }
        }
        if (op & FETCH_ASSOC)
        {
#ifdef _WIN32
            key = PyUnicode_DecodeLocale((char *)stmt_res->column_info[column_number].name, "surrogateescape");
#else
            key = PyUnicode_FromString((char *)stmt_res->column_info[column_number].name);
#endif
            if (value == NULL)
            {
                Py_XDECREF(key);
                Py_XDECREF(value);
                snprintf(messageStr, sizeof(messageStr), "Error: Value is NULL for FETCH_ASSOC operation. Column: %s", stmt_res->column_info[column_number].name);
                LogMsg(ERROR, messageStr);
                return NULL;
            }
            snprintf(messageStr, sizeof(messageStr), "Fetching column %d as key-value pair", column_number);
            LogMsg(DEBUG, messageStr);
            PyDict_SetItem(return_value, key, value);
            Py_DECREF(key);
        }
        if (op == FETCH_INDEX)
        {
            /* No need to call Py_DECREF as PyTuple_SetItem steals the reference */
            snprintf(messageStr, sizeof(messageStr), "Fetching column %d as index for FETCH_INDEX", column_number);
            LogMsg(DEBUG, messageStr);
            PyTuple_SetItem(return_value, column_number, value);
        }
        else
        {
            if (op == FETCH_BOTH)
            {
                key = PyInt_FromLong(column_number);
                snprintf(messageStr, sizeof(messageStr), "Fetching column %d as key-value pair", column_number);
                LogMsg(DEBUG, messageStr);
                PyDict_SetItem(return_value, key, value);
                Py_DECREF(key);
            }
            Py_DECREF(value);
        }
    }
    if (rc == SQL_SUCCESS_WITH_INFO)
    {
        snprintf(messageStr, sizeof(messageStr), "SQL_SUCCESS_WITH_INFO: Checking SQL errors for statement handle %p", stmt_res->hstmt);
        LogMsg(INFO, messageStr);
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                        rc, 1, NULL, -1, 1);
    }
    LogMsg(INFO, "exit _python_ibm_db_bind_fetch_helper()");
    if (op == FETCH_ASSOC)
    {
        LogMsg(INFO, "exit fetch_assoc()");
    }
    if (op == FETCH_INDEX)
    {
        LogMsg(INFO, "exit fetch_tuple()");
    }
    if (op == FETCH_BOTH)
    {
        LogMsg(INFO, "exit fetch_both()");
    }
    return return_value;
}

/*!# ibm_db.fetch_row
 *
 * ===Description
 * bool ibm_db.fetch_row ( resource stmt [, int row_number] )
 *
 * Sets the result set pointer to the next row or requested row
 *
 * Use ibm_db.fetch_row() to iterate through a result set, or to point to a
 * specific row in a result set if you requested a scrollable cursor.
 *
 * To retrieve individual fields from the result set, call the ibm_db.result()
 * function. Rather than calling ibm_db.fetch_row() and ibm_db.result(), most
 * applications will call one of ibm_db.fetch_assoc(), ibm_db.fetch_both(), or
 * ibm_db.fetch_array() to advance the result set pointer and return a complete
 * row as an array.
 *
 * ===Parameters
 * ====stmt
 *        A valid stmt resource.
 *
 * ====row_number
 *        With scrollable cursors, you can request a specific row number in the
 * result set. Row numbering is 1-indexed.
 *
 * ===Return Values
 *
 * Returns TRUE if the requested row exists in the result set. Returns FALSE if
 * the requested row does not exist in the result set.
 */
static PyObject *ibm_db_fetch_row(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry fetch_row()");
    LogUTF8Msg(args);
    PyObject *py_stmt_res = NULL;
    PyObject *py_row_number = NULL;
    SQLINTEGER row_number = -1;
    stmt_handle *stmt_res = NULL;
    int rc;
    char error[DB2_MAX_ERR_MSG_LEN + 50];

    if (!PyArg_ParseTuple(args, "O|O", &py_stmt_res, &py_row_number))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, py_row_number=%p", py_stmt_res, py_row_number);
    LogMsg(DEBUG, messageStr);
    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)))
    {
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        LogMsg(ERROR, "Supplied statement object parameter is invalid");
        return NULL;
    }
    else
    {
        stmt_res = (stmt_handle *)py_stmt_res;
        snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
        LogMsg(DEBUG, messageStr);
    }

    if (!NIL_P(py_row_number))
    {
        if (PyInt_Check(py_row_number))
        {
            row_number = (SQLINTEGER)PyLong_AsLong(py_row_number);
        }
        else
        {
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            LogMsg(ERROR, "Supplied parameter is invalid");
            return NULL;
        }
    }
    /* get column header info */
    if (stmt_res->column_info == NULL)
    {
        if (_python_ibm_db_get_result_set_info(stmt_res) < 0)
        {
            sprintf(error, "Column information cannot be retrieved: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            LogMsg(ERROR, error);
            return NULL;
        }
    }

    /* check if row_number is present */
    if (PyTuple_Size(args) == 2 && row_number > 0)
    {
#ifndef PASE /* i5/OS problem with SQL_FETCH_ABSOLUTE */

        LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_ABSOLUTE");
        snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                 row_number);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_ABSOLUTE,
                            row_number);
        snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
        LogMsg(DEBUG, messageStr);
        Py_END_ALLOW_THREADS;
        if (rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1, NULL,
                                            -1, 1);
        }
#else  /* PASE */
        LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_FIRST");
        snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                 row_number);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_FIRST,
                            row_number);
        snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1, NULL,
                                            -1, 1);
        }
        Py_END_ALLOW_THREADS;

        if (row_number > 1 && (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO))
        {
            LogMsg(DEBUG, "Calling SQLFetchScroll with SQL_FETCH_RELATIVE");
            snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p, row_number: %d", (void *)stmt_res->hstmt,
                     row_number - 1);
            LogMsg(DEBUG, messageStr);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_RELATIVE,
                                row_number - 1);
            snprintf(messageStr, sizeof(messageStr), "SQLFetchScroll returned with rc: %d", rc);
            LogMsg(DEBUG, messageStr);
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL,
                                                -1, 1);
            }
            Py_END_ALLOW_THREADS;
        }
#endif /* PASE */
    }
    else if (PyTuple_Size(args) == 2 && row_number < 0)
    {
        PyErr_SetString(PyExc_Exception,
                        "Requested row number must be a positive value");
        LogMsg(ERROR, "Requested row number must be a positive value");
        return NULL;
    }
    else
    {
        /* row_number is NULL or 0; just fetch next row */
        LogMsg(DEBUG, "Calling SQLFetch, for fetching next row");
        snprintf(messageStr, sizeof(messageStr), "Statement Handle: %p", (void *)stmt_res->hstmt);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetch((SQLHSTMT)stmt_res->hstmt);
        snprintf(messageStr, sizeof(messageStr), "SQLFetch returned with rc: %d", rc);
        LogMsg(DEBUG, messageStr);
        Py_END_ALLOW_THREADS;
    }

    if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
    {
        if (rc == SQL_SUCCESS_WITH_INFO)
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1,
                                            NULL, -1, 1);
        }
        LogMsg(INFO, "exit fetch_row()");
        Py_RETURN_TRUE;
    }
    else if (rc == SQL_NO_DATA_FOUND)
    {
        LogMsg(INFO, "exit fetch_row()");
        Py_RETURN_FALSE;
    }
    else
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                        NULL, -1, 1);
        PyErr_Clear();
        LogMsg(INFO, "exit fetch_row()");
        Py_RETURN_FALSE;
    }
}

/*!# ibm_db.fetch_assoc
 *
 * ===Description
 * dictionary ibm_db.fetch_assoc ( resource stmt [, int row_number] )
 *
 * Returns a dictionary, indexed by column name, representing a row in a result  * set.
 *
 * ===Parameters
 * ====stmt
 *        A valid stmt resource containing a result set.
 *
 * ====row_number
 *
 *        Requests a specific 1-indexed row from the result set. Passing this
 * parameter results in a
 *        Python warning if the result set uses a forward-only cursor.
 *
 * ===Return Values
 *
 * Returns an associative array with column values indexed by the column name
 * representing the next
 * or requested row in the result set. Returns FALSE if there are no rows left
 * in the result set,
 * or if the row requested by row_number does not exist in the result set.
 */
static PyObject *ibm_db_fetch_assoc(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry fetch_assoc()");
    return _python_ibm_db_bind_fetch_helper(args, FETCH_ASSOC);
}

/*
 * ibm_db.fetch_object --    Returns an object with properties representing columns in the fetched row
 *
 * ===Description
 * object ibm_db.fetch_object ( resource stmt [, int row_number] )
 *
 * Returns an object in which each property represents a column returned in the row fetched from a result set.
 *
 * ===Parameters
 *
 * stmt
 *        A valid stmt resource containing a result set.
 *
 * row_number
 *        Requests a specific 1-indexed row from the result set. Passing this parameter results in a
 *        Python warning if the result set uses a forward-only cursor.
 *
 * ===Return Values
 *
 * Returns an object representing a single row in the result set. The properties of the object map
 * to the names of the columns in the result set.
 *
 * The IBM DB2, Cloudscape, and Apache Derby database servers typically fold column names to upper-case,
 * so the object properties will reflect that case.
 *
 * If your SELECT statement calls a scalar function to modify the value of a column, the database servers
 * return the column number as the name of the column in the result set. If you prefer a more
 * descriptive column name and object property, you can use the AS clause to assign a name
 * to the column in the result set.
 *
 * Returns FALSE if no row was retrieved.
 */
/*
PyObject *ibm_db_fetch_object(int argc, PyObject **argv, PyObject *self)
{
    row_hash_struct *row_res;

    row_res = ALLOC(row_hash_struct);
    row_res->hash = _python_ibm_db_bind_fetch_helper(argc, argv, FETCH_ASSOC);

    if (RTEST(row_res->hash)) {
      return Data_Wrap_Struct(le_row_struct,
            _python_ibm_db_mark_row_struct, _python_ibm_db_free_row_struct,
            row_res);
    } else {
      free(row_res);
      return Py_False;
    }
}
*/

/*!# ibm_db.fetch_array
 *
 * ===Description
 *
 * array ibm_db.fetch_array ( resource stmt [, int row_number] )
 *
 * Returns a tuple, indexed by column position, representing a row in a result
 * set. The columns are 0-indexed.
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid stmt resource containing a result set.
 *
 * ====row_number
 *        Requests a specific 1-indexed row from the result set. Passing this
 * parameter results in a warning if the result set uses a forward-only cursor.
 *
 * ===Return Values
 *
 * Returns a 0-indexed tuple with column values indexed by the column position
 * representing the next or requested row in the result set. Returns FALSE if
 * there are no rows left in the result set, or if the row requested by
 * row_number does not exist in the result set.
 */

static PyObject *ibm_db_fetch_array(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry fetch_tuple()");
    return _python_ibm_db_bind_fetch_helper(args, FETCH_INDEX);
}

/*!# ibm_db.fetch_both
 *
 * ===Description
 * dictionary ibm_db.fetch_both ( resource stmt [, int row_number] )
 *
 * Returns a dictionary, indexed by both column name and position, representing  * a row in a result set. Note that the row returned by ibm_db.fetch_both()
 * requires more memory than the single-indexed dictionaries/arrays returned by  * ibm_db.fetch_assoc() or ibm_db.fetch_tuple().
 *
 * ===Parameters
 *
 * ====stmt
 *        A valid stmt resource containing a result set.
 *
 * ====row_number
 *        Requests a specific 1-indexed row from the result set. Passing this
 * parameter results in a warning if the result set uses a forward-only cursor.
 *
 * ===Return Values
 *
 * Returns a dictionary with column values indexed by both the column name and
 * 0-indexed column number.
 * The dictionary represents the next or requested row in the result set.
 * Returns FALSE if there are no rows left in the result set, or if the row
 * requested by row_number does not exist in the result set.
 */
static PyObject *ibm_db_fetch_both(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry fetch_both()");
    return _python_ibm_db_bind_fetch_helper(args, FETCH_BOTH);
}

/*!# ibm_db.set_option
 *
 * ===Description
 * bool ibm_db.set_option ( resource resc, array options, int type )
 *
 * Sets options for a connection or statement resource. You cannot set options
 * for result set resources.
 *
 * ===Parameters
 *
 * ====resc
 *        A valid connection or statement resource.
 *
 * ====options
 *        The options to be set
 *
 * ====type
 *        A field that specifies the resource type (1 = Connection,
 * NON-1 = Statement)
 *
 * ===Return Values
 *
 * Returns TRUE on success or FALSE on failure
 */
static PyObject *ibm_db_set_option(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry set_option()");
    LogUTF8Msg(args);
    PyObject *conn_or_stmt = NULL;
    PyObject *options = NULL;
    PyObject *py_type = NULL;
    stmt_handle *stmt_res = NULL;
    conn_handle *conn_res;
    int rc = 0;
    long type = 0;

    if (!PyArg_ParseTuple(args, "OOO", &conn_or_stmt, &options, &py_type))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: conn_or_stmt=%p, options=%p, py_type=%p",
             (void *)conn_or_stmt, (void *)options, (void *)py_type);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(conn_or_stmt))
    {
        if (!NIL_P(py_type))
        {
            if (PyInt_Check(py_type))
            {
                type = (int)PyLong_AsLong(py_type);
                snprintf(messageStr, sizeof(messageStr), "type: %ld", type);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Supplied py_type parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if (type == 1)
        {
            if (!PyObject_TypeCheck(conn_or_stmt, &conn_handleType))
            {
                LogMsg(ERROR, "Supplied connection object parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
                return NULL;
            }
            conn_res = (conn_handle *)conn_or_stmt;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);

            if (!NIL_P(options))
            {
                snprintf(messageStr, sizeof(messageStr), "Calling _python_ibm_db_parse_options with: (options=%p, SQL_HANDLE_DBC=%d, conn_res=%p)",
                         (void *)options, SQL_HANDLE_DBC, (void *)conn_res);
                LogMsg(DEBUG, messageStr);
                rc = _python_ibm_db_parse_options(options, SQL_HANDLE_DBC, conn_res);
                snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_parse_options returned rc=%d", rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    LogMsg(ERROR, "Options array must have string indexes");
                    PyErr_SetString(PyExc_Exception,
                                    "Options Array must have string indexes");
                    return NULL;
                }
            }
        }
        else
        {
            if (!PyObject_TypeCheck(conn_or_stmt, &stmt_handleType))
            {
                LogMsg(ERROR, "Supplied statement object parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
                return NULL;
            }
            stmt_res = (stmt_handle *)conn_or_stmt;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
            if (!NIL_P(options))
            {
                snprintf(messageStr, sizeof(messageStr), "Calling _python_ibm_db_parse_options with: (options=%p, SQL_HANDLE_STMT=%d, stmt_res=%p)", (void *)options, SQL_HANDLE_STMT, (void *)stmt_res);
                LogMsg(DEBUG, messageStr);
                rc = _python_ibm_db_parse_options(options, SQL_HANDLE_STMT, stmt_res);
                snprintf(messageStr, sizeof(messageStr), "_python_ibm_db_parse_options returned rc=%d", rc);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR)
                {
                    LogMsg(ERROR, "Options array must have string indexes");
                    PyErr_SetString(PyExc_Exception,
                                    "Options Array must have string indexes");
                    return NULL;
                }
            }
        }
        LogMsg(DEBUG, "successfully set option");
        LogMsg(INFO, "exit set_option()");
        Py_INCREF(Py_True);
        return Py_True;
    }
    else
    {
        LogMsg(DEBUG, "failed to set option");
        LogMsg(INFO, "exit set_option()");
        Py_INCREF(Py_False);
        return Py_False;
    }
}

static PyObject *ibm_db_get_db_info(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry get_db_info()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *return_value = NULL;
    PyObject *py_option = NULL;
    SQLINTEGER option = 0;
    conn_handle *conn_res;
    int rc = 0;
    SQLCHAR *value = NULL;

    if (!PyArg_ParseTuple(args, "OO", &py_conn_res, &py_option))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_option=%p", py_conn_res, py_option);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res=%p", conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (!NIL_P(py_option))
        {
            if (PyInt_Check(py_option))
            {
                option = (SQLINTEGER)PyLong_AsLong(py_option);
                snprintf(messageStr, sizeof(messageStr), "Option parsed: option=%d", option);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Supplied parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        value = (SQLCHAR *)ALLOC_N(char, ACCTSTR_LEN + 1);
        snprintf(messageStr, sizeof(messageStr),
                 "Calling SQLGetInfo with parameters: hdbc=%p, option=%d, buffer=%p, buffer_length=%d",
                 conn_res->hdbc, option, value, ACCTSTR_LEN);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, (SQLSMALLINT)option, (SQLPOINTER)value,
                        ACCTSTR_LEN, NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLGetInfo returned rc=%d, value=%s", rc, value ? (char *)value : "NULL");
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            if (value != NULL)
            {
                PyMem_Del(value);
                value = NULL;
            }
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            if (value != NULL)
            {
                return_value = StringOBJ_FromASCII((char *)value);
                PyMem_Del(value);
                value = NULL;
            }
            LogMsg(INFO, "exit get_db_info()");
            return return_value;
        }
    }
    LogMsg(INFO, "exit get_db_info()");
    Py_INCREF(Py_False);
    return Py_False;
}

/*!# ibm_db.server_info
 *
 * ===Description
 * object ibm_db.server_info ( resource connection )
 *
 * This function returns a read-only object with information about the IBM DB2
 * or Informix Dynamic Server.
 * The following table lists the database server properties:
 *
 * ====Table 1. Database server properties
 * Property name:: Description (Return type)
 *
 * DBMS_NAME:: The name of the database server to which you are connected. For
 * DB2 servers this is a combination of DB2 followed by the operating system on
 * which the database server is running. (string)
 *
 * DBMS_VER:: The version of the database server, in the form of a string
 * "MM.mm.uuuu" where MM is the major version, mm is the minor version, and
 * uuuu is the update. For example, "08.02.0001" represents major version 8,
 * minor version 2, update 1. (string)
 *
 * DB_CODEPAGE:: The code page of the database to which you are connected. (int)
 *
 * DB_NAME:: The name of the database to which you are connected. (string)
 *
 * DFT_ISOLATION:: The default transaction isolation level supported by the
 * server: (string)
 *
 *                         UR:: Uncommitted read: changes are immediately
 * visible by all concurrent transactions.
 *
 *                         CS:: Cursor stability: a row read by one transaction
 * can be altered and committed by a second concurrent transaction.
 *
 *                         RS:: Read stability: a transaction can add or remove
 * rows matching a search condition or a pending transaction.
 *
 *                         RR:: Repeatable read: data affected by pending
 * transaction is not available to other transactions.
 *
 *                         NC:: No commit: any changes are visible at the end of
 * a successful operation. Explicit commits and rollbacks are not allowed.
 *
 * IDENTIFIER_QUOTE_CHAR:: The character used to delimit an identifier. (string)
 *
 * INST_NAME:: The instance on the database server that contains the database.
 * (string)
 *
 * ISOLATION_OPTION:: An array of the isolation options supported by the
 * database server. The isolation options are described in the DFT_ISOLATION
 * property. (array)
 *
 * KEYWORDS:: An array of the keywords reserved by the database server. (array)
 *
 * LIKE_ESCAPE_CLAUSE:: TRUE if the database server supports the use of % and _
 * wildcard characters. FALSE if the database server does not support these
 * wildcard characters. (bool)
 *
 * MAX_COL_NAME_LEN:: Maximum length of a column name supported by the database
 * server, expressed in bytes. (int)
 *
 * MAX_IDENTIFIER_LEN:: Maximum length of an SQL identifier supported by the
 * database server, expressed in characters. (int)
 *
 * MAX_INDEX_SIZE:: Maximum size of columns combined in an index supported by
 * the database server, expressed in bytes. (int)
 *
 * MAX_PROC_NAME_LEN:: Maximum length of a procedure name supported by the
 * database server, expressed in bytes. (int)
 *
 * MAX_ROW_SIZE:: Maximum length of a row in a base table supported by the
 * database server, expressed in bytes. (int)
 *
 * MAX_SCHEMA_NAME_LEN:: Maximum length of a schema name supported by the
 * database server, expressed in bytes. (int)
 *
 * MAX_STATEMENT_LEN:: Maximum length of an SQL statement supported by the
 * database server, expressed in bytes. (int)
 *
 * MAX_TABLE_NAME_LEN:: Maximum length of a table name supported by the
 * database server, expressed in bytes. (bool)
 *
 * NON_NULLABLE_COLUMNS:: TRUE if the database server supports columns that can
 * be defined as NOT NULL, FALSE if the database server does not support columns
 * defined as NOT NULL. (bool)
 *
 * PROCEDURES:: TRUE if the database server supports the use of the CALL
 * statement to call stored procedures, FALSE if the database server does not
 * support the CALL statement. (bool)
 *
 * SPECIAL_CHARS:: A string containing all of the characters other than a-Z,
 * 0-9, and underscore that can be used in an identifier name. (string)
 *
 * SQL_CONFORMANCE:: The level of conformance to the ANSI/ISO SQL-92
 * specification offered by the database server: (string)
 *
 *                            ENTRY:: Entry-level SQL-92 compliance.
 *
 *                            FIPS127:: FIPS-127-2 transitional compliance.
 *
 *                            FULL:: Full level SQL-92 compliance.
 *
 *                            INTERMEDIATE:: Intermediate level SQL-92
 *                                            compliance.
 *
 * ===Parameters
 *
 * ====connection
 *        Specifies an active DB2 client connection.
 *
 * ===Return Values
 *
 * Returns an object on a successful call. Returns FALSE on failure.
 */
static PyObject *ibm_db_server_info(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry server_info()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res;
    int rc = 0;
    char buffer11[11];
    char buffer255[255];
    char buffer2k[2048];
    SQLSMALLINT bufferint16;
    SQLUINTEGER bufferint32;
    SQLINTEGER bitmask;
    char *keyword;
    char *last;
    PyObject *karray;
    int numkw = 0;
    int count = 0;
    PyObject *array;
    PyObject *rv = NULL;

    le_server_info *return_value = PyObject_NEW(le_server_info,
                                                &server_infoType);

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p", py_conn_res);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        /* DBMS_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DBMS_NAME, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_DBMS_NAME=%d, buffer255=%p, buffer255_size=%zu, NULL, and returned rc=%d, buffer255 contains: %s",
                 conn_res->hdbc, SQL_DBMS_NAME, (void *)buffer255, sizeof(buffer255), rc, buffer255);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->DBMS_NAME to: %s", buffer255);
            LogMsg(DEBUG, messageStr);
            return_value->DBMS_NAME = StringOBJ_FromASCII(buffer255);
        }

        /* DBMS_VER */
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DBMS_VER, (SQLPOINTER)buffer11,
                        sizeof(buffer11), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_DBMS_VER=%d, buffer11=%p, buffer11_size=%zu, NULL and returned rc=%d, buffer11 contains: %s",
                 conn_res->hdbc, SQL_DBMS_VER, (void *)buffer11, sizeof(buffer11), rc, buffer11);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->DBMS_VER to: %s", buffer11);
            LogMsg(DEBUG, messageStr);
            return_value->DBMS_VER = StringOBJ_FromASCII(buffer11);
        }

#if defined(__MVS__)
        snprintf(messageStr, sizeof(messageStr), "Setting return_value->DB_CODEPAGE to: %ld", 1208);
        LogMsg(DEBUG, messageStr);
        return_value->DB_CODEPAGE = PyInt_FromLong(1208);
#elif !defined(PASE) /* i5/OS DB_CODEPAGE handled natively */
        /* DB_CODEPAGE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DATABASE_CODEPAGE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_DATABASE_CODEPAGE=%d, bufferint32=%p, bufferint32_size=%zu, NULL and returned rc=%d, bufferint32 contains: %d",
                 conn_res->hdbc, SQL_DATABASE_CODEPAGE, (void *)&bufferint32, sizeof(bufferint32), rc, bufferint32);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->DB_CODEPAGE to: %ld", bufferint32);
            LogMsg(DEBUG, messageStr);
            return_value->DB_CODEPAGE = PyInt_FromLong(bufferint32);
        }
#endif               /* PASE */

        /* DB_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DATABASE_NAME, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_DATABASE_NAME=%d, buffer255=%p, buffer255_size=%zu, NULL and returned rc=%d, buffer255 contains: %s",
                 conn_res->hdbc, SQL_DATABASE_NAME, (void *)buffer255, sizeof(buffer255), rc, buffer255);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_INCREF(Py_False);
            return Py_False;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->DB_NAME to: %s", buffer255);
            LogMsg(DEBUG, messageStr);
            return_value->DB_NAME = StringOBJ_FromASCII(buffer255);
        }

#ifndef PASE /* i5/OS INST_NAME handled natively */
        /* INST_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_SERVER_NAME, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_SERVER_NAME=%d, buffer255=%p, buffer255_size=%zu, NULL and returned rc=%d, buffer255 contains: %s",
                 conn_res->hdbc, SQL_SERVER_NAME, (void *)buffer255, sizeof(buffer255), rc, buffer255);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->INST_NAME to: %s", buffer255);
            LogMsg(DEBUG, messageStr);
            return_value->INST_NAME = StringOBJ_FromASCII(buffer255);
        }

        /* SPECIAL_CHARS */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_SPECIAL_CHARACTERS,
                        (SQLPOINTER)buffer255, sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_SPECIAL_CHARACTERS=%d, buffer255=%p, buffer255_size=%zu, NULL and returned rc=%d, buffer255 contains: %s",
                 conn_res->hdbc, SQL_SPECIAL_CHARACTERS, (void *)buffer255, sizeof(buffer255), rc, buffer255);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->SPECIAL_CHARS to: %s", buffer255);
            LogMsg(DEBUG, messageStr);
            return_value->SPECIAL_CHARS = StringOBJ_FromASCII(buffer255);
        }
#endif /* PASE */

        /* KEYWORDS */
        memset(buffer2k, 0, sizeof(buffer2k));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_KEYWORDS, (SQLPOINTER)buffer2k,
                        sizeof(buffer2k), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_KEYWORDS=%d, buffer2k=%p, buffer2k_size=%zu, NULL and returned rc=%d, buffer2k contains: %s",
                 conn_res->hdbc, SQL_KEYWORDS, (void *)buffer2k, sizeof(buffer2k), rc, buffer2k);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }

            for (last = buffer2k; *last; last++)
            {
                if (*last == ',')
                {
                    numkw++;
                }
            }
            snprintf(messageStr, sizeof(messageStr), "Parsed KEYWORDS buffer. Total keywords count is: %d", numkw);
            LogMsg(DEBUG, messageStr);
            karray = PyTuple_New(numkw + 1);

            for (keyword = last = buffer2k; *last; last++)
            {
                if (*last == ',')
                {
                    *last = '\0';
                    PyTuple_SetItem(karray, count, StringOBJ_FromASCII(keyword));
                    keyword = last + 1;
                    count++;
                }
            }
            if (*keyword)
            {
                PyTuple_SetItem(karray, count, StringOBJ_FromASCII(keyword));
            }
            snprintf(messageStr, sizeof(messageStr), "Set return_value->KEYWORDS to a tuple with %d items.", numkw);
            LogMsg(DEBUG, messageStr);
            return_value->KEYWORDS = karray;
        }

        /* DFT_ISOLATION */
        bitmask = 0;
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DEFAULT_TXN_ISOLATION, &bitmask,
                        sizeof(bitmask), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_DEFAULT_TXN_ISOLATION=%d, &bitmask=%p, bitmask_size=%zu, NULL and returned rc=%d, bitmask=0x%x",
                 conn_res->hdbc, SQL_DEFAULT_TXN_ISOLATION, (void *)&bitmask, sizeof(bitmask), rc, bitmask);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            if (bitmask & SQL_TXN_READ_UNCOMMITTED)
            {
                strcpy((char *)buffer11, "UR");
                LogMsg(DEBUG, "bitmask indicates SQL_TXN_READ_UNCOMMITTED. Setting buffer11 to: UR");
            }
            if (bitmask & SQL_TXN_READ_COMMITTED)
            {
                strcpy((char *)buffer11, "CS");
                LogMsg(DEBUG, "bitmask indicates SQL_TXN_READ_COMMITTED. Setting buffer11 to: CS");
            }
            if (bitmask & SQL_TXN_REPEATABLE_READ)
            {
                strcpy((char *)buffer11, "RS");
                LogMsg(DEBUG, "bitmask indicates SQL_TXN_REPEATABLE_READ. Setting buffer11 to: RS");
            }
            if (bitmask & SQL_TXN_SERIALIZABLE)
            {
                strcpy((char *)buffer11, "RR");
                LogMsg(DEBUG, "bitmask indicates SQL_TXN_SERIALIZABLE. Setting buffer11 to: RR");
            }
            if (bitmask & SQL_TXN_NOCOMMIT)
            {
                strcpy((char *)buffer11, "NC");
                LogMsg(DEBUG, "bitmask indicates SQL_TXN_NOCOMMIT. Setting buffer11 to: NC");
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->DFT_ISOLATION to: %s", buffer11);
            LogMsg(DEBUG, messageStr);
            return_value->DFT_ISOLATION = StringOBJ_FromASCII(buffer11);
        }

#ifndef PASE /* i5/OS ISOLATION_OPTION handled natively */
        /* ISOLATION_OPTION */
        bitmask = 0;
        count = 0;
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_TXN_ISOLATION_OPTION, &bitmask,
                        sizeof(bitmask), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_TXN_ISOLATION_OPTION=%d, &bitmask=%p, bitmask_size=%zu, NULL and returned rc=%d, bitmask=0x%x",
                 conn_res->hdbc, SQL_TXN_ISOLATION_OPTION, (void *)&bitmask, sizeof(bitmask), rc, bitmask);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }

            array = PyTuple_New(5);

            if (bitmask & SQL_TXN_READ_UNCOMMITTED)
            {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("UR"));
                LogMsg(DEBUG, "Bitmask indicates SQL_TXN_READ_UNCOMMITTED. Added 'UR' to the tuple.");
                count++;
            }
            if (bitmask & SQL_TXN_READ_COMMITTED)
            {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("CS"));
                LogMsg(DEBUG, "Bitmask indicates SQL_TXN_READ_COMMITTED. Added 'CS' to the tuple.");
                count++;
            }
            if (bitmask & SQL_TXN_REPEATABLE_READ)
            {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("RS"));
                LogMsg(DEBUG, "Bitmask indicates SQL_TXN_REPEATABLE_READ. Added 'RS' to the tuple.");
                count++;
            }
            if (bitmask & SQL_TXN_SERIALIZABLE)
            {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("RR"));
                LogMsg(DEBUG, "Bitmask indicates SQL_TXN_SERIALIZABLE. Added 'RR' to the tuple.");
                count++;
            }
            if (bitmask & SQL_TXN_NOCOMMIT)
            {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("NC"));
                LogMsg(DEBUG, "Bitmask indicates SQL_TXN_NOCOMMIT. Added 'NC' to the tuple.");
                count++;
            }
            _PyTuple_Resize(&array, count);
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->ISOLATION_OPTION to a tuple with %d items.", count);
            LogMsg(DEBUG, messageStr);
            return_value->ISOLATION_OPTION = array;
        }
#endif /* PASE */

        /* SQL_CONFORMANCE */
        bufferint32 = 0;
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_ODBC_SQL_CONFORMANCE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_ODBC_SQL_CONFORMANCE=%d, &bufferint32=%p, bufferint32_size=%zu, NULL and returned rc=%d, bufferint32=%d",
                 conn_res->hdbc, SQL_ODBC_SQL_CONFORMANCE, (void *)&bufferint32, sizeof(bufferint32), rc, bufferint32);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            switch (bufferint32)
            {
            case SQL_SC_SQL92_ENTRY:
                strcpy((char *)buffer255, "ENTRY");
                LogMsg(DEBUG, "Bitmask indicates SQL_SC_SQL92_ENTRY. Setting buffer255 to 'ENTRY'.");
                break;
            case SQL_SC_FIPS127_2_TRANSITIONAL:
                strcpy((char *)buffer255, "FIPS127");
                LogMsg(DEBUG, "Bitmask indicates SQL_SC_FIPS127_2_TRANSITIONAL. Setting buffer255 to 'FIPS127'.");
                break;
            case SQL_SC_SQL92_FULL:
                strcpy((char *)buffer255, "FULL");
                LogMsg(DEBUG, "Bitmask indicates SQL_SC_SQL92_FULL. Setting buffer255 to 'FULL'.");
                break;
            case SQL_SC_SQL92_INTERMEDIATE:
                strcpy((char *)buffer255, "INTERMEDIATE");
                LogMsg(DEBUG, "Bitmask indicates SQL_SC_SQL92_INTERMEDIATE. Setting buffer255 to 'INTERMEDIATE'.");
                break;
            default:
                break;
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->SQL_CONFORMANCE to: %s", buffer255);
            LogMsg(DEBUG, messageStr);
            return_value->SQL_CONFORMANCE = StringOBJ_FromASCII(buffer255);
        }

        /* PROCEDURES */
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_PROCEDURES, (SQLPOINTER)buffer11,
                        sizeof(buffer11), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_PROCEDURES=%d, (SQLPOINTER)buffer11=%p, buffer11_size=%zu, NULL and returned rc=%d, buffer11 contains: %s",
                 conn_res->hdbc, SQL_PROCEDURES, (void *)buffer11, sizeof(buffer11), rc, buffer11);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            if (strcmp((char *)buffer11, "Y") == 0)
            {
                LogMsg(DEBUG, "SQL_PROCEDURES value is 'Y'. Setting return_value->PROCEDURES to Py_True.");
                Py_INCREF(Py_True);
                return_value->PROCEDURES = Py_True;
            }
            else
            {
                LogMsg(DEBUG, "SQL_PROCEDURES value is not 'Y'. Setting return_value->PROCEDURES to Py_False.");
                Py_INCREF(Py_False);
                return_value->PROCEDURES = Py_False;
            }
        }

        /* IDENTIFIER_QUOTE_CHAR */
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_IDENTIFIER_QUOTE_CHAR,
                        (SQLPOINTER)buffer11, sizeof(buffer11), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_IDENTIFIER_QUOTE_CHAR=%d, (SQLPOINTER)buffer11=%p, buffer11_size=%zu, NULL and returned rc=%d, buffer11 contains: %s",
                 conn_res->hdbc, SQL_IDENTIFIER_QUOTE_CHAR, (void *)buffer11, sizeof(buffer11), rc, buffer11);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->IDENTIFIER_QUOTE_CHAR to: %s", buffer11);
            LogMsg(DEBUG, messageStr);
            return_value->IDENTIFIER_QUOTE_CHAR = StringOBJ_FromASCII(buffer11);
        }

        /* LIKE_ESCAPE_CLAUSE */
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_LIKE_ESCAPE_CLAUSE,
                        (SQLPOINTER)buffer11, sizeof(buffer11), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_LIKE_ESCAPE_CLAUSE=%d, (SQLPOINTER)buffer11=%p, buffer11_size=%zu, NULL and returned rc=%d, buffer11 contains: %s",
                 conn_res->hdbc, SQL_LIKE_ESCAPE_CLAUSE, (void *)buffer11, sizeof(buffer11), rc, buffer11);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            if (strcmp(buffer11, "Y") == 0)
            {
                LogMsg(DEBUG, "SQL_LIKE_ESCAPE_CLAUSE value is 'Y'. Setting return_value->LIKE_ESCAPE_CLAUSE to Py_True.");
                Py_INCREF(Py_True);
                return_value->LIKE_ESCAPE_CLAUSE = Py_True;
            }
            else
            {
                LogMsg(DEBUG, "SQL_LIKE_ESCAPE_CLAUSE value is not 'Y'. Setting return_value->LIKE_ESCAPE_CLAUSE to Py_False.");
                Py_INCREF(Py_False);
                return_value->LIKE_ESCAPE_CLAUSE = Py_False;
            }
        }

        /* MAX_COL_NAME_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_COLUMN_NAME_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_MAX_COLUMN_NAME_LEN=%d, &bufferint16=%p, bufferint16_size=%zu, NULL and returned rc=%d, bufferint16 contains: %d",
                 conn_res->hdbc, SQL_MAX_COLUMN_NAME_LEN, (void *)&bufferint16, sizeof(bufferint16), rc, bufferint16);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_COL_NAME_LEN to: %d", bufferint16);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_COL_NAME_LEN = PyInt_FromLong(bufferint16);
        }

        /* MAX_ROW_SIZE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_ROW_SIZE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr),
                 "SQLGetInfo called with conn_res->hdbc=%p, SQL_MAX_ROW_SIZE=%d, &bufferint32=%p, bufferint32_size=%zu, NULL and returned rc=%d, bufferint32 contains: %d",
                 conn_res->hdbc, SQL_MAX_ROW_SIZE, (void *)&bufferint32, sizeof(bufferint32), rc, bufferint32);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_ROW_SIZE to: %d", bufferint32);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_ROW_SIZE = PyInt_FromLong(bufferint32);
        }

#ifndef PASE /* i5/OS MAX_IDENTIFIER_LEN handled natively */
        /* MAX_IDENTIFIER_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_IDENTIFIER_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_MAX_IDENTIFIER_LEN=%d, &bufferint16=%p, bufferint16_size=%zu, NULL and returned rc=%d, bufferint16 contains: %d",
                 conn_res->hdbc, SQL_MAX_IDENTIFIER_LEN, (void *)&bufferint16, sizeof(bufferint16), rc, bufferint16);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_IDENTIFIER_LEN to: %d", bufferint16);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_IDENTIFIER_LEN = PyInt_FromLong(bufferint16);
        }

        /* MAX_INDEX_SIZE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_INDEX_SIZE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_MAX_INDEX_SIZE=%d, &bufferint32=%p, bufferint32_size=%zu, NULL and returned rc=%d, bufferint32 contains: %d",
                 conn_res->hdbc, SQL_MAX_INDEX_SIZE, (void *)&bufferint32, sizeof(bufferint32), rc, bufferint32);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_INDEX_SIZE to: %d", bufferint32);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_INDEX_SIZE = PyInt_FromLong(bufferint32);
        }

        /* MAX_PROC_NAME_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_PROCEDURE_NAME_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_MAX_PROCEDURE_NAME_LEN=%d, &bufferint16=%p, bufferint16_size=%zu, NULL and returned rc=%d, bufferint16 contains: %d",
                 conn_res->hdbc, SQL_MAX_PROCEDURE_NAME_LEN, (void *)&bufferint16, sizeof(bufferint16), rc, bufferint16);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_PROC_NAME_LEN to: %d", bufferint16);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_PROC_NAME_LEN = PyInt_FromLong(bufferint16);
        }
#endif /* PASE */

        /* MAX_SCHEMA_NAME_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_SCHEMA_NAME_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_MAX_SCHEMA_NAME_LEN=%d, &bufferint16=%p, bufferint16_size=%zu, NULL and returned rc=%d, bufferint16 contains: %d",
                 conn_res->hdbc, SQL_MAX_SCHEMA_NAME_LEN, (void *)&bufferint16, sizeof(bufferint16), rc, bufferint16);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_SCHEMA_NAME_LEN to: %d", bufferint16);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_SCHEMA_NAME_LEN = PyInt_FromLong(bufferint16);
        }

        /* MAX_STATEMENT_LEN */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_STATEMENT_LEN, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_MAX_STATEMENT_LEN=%d, &bufferint32=%p, bufferint32_size=%zu, NULL and returned rc=%d, bufferint32 contains: %d",
                 conn_res->hdbc, SQL_MAX_STATEMENT_LEN, (void *)&bufferint32, sizeof(bufferint32), rc, bufferint32);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_STATEMENT_LEN to: %d", bufferint32);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_STATEMENT_LEN = PyInt_FromLong(bufferint32);
        }

        /* MAX_TABLE_NAME_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_TABLE_NAME_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_MAX_TABLE_NAME_LEN=%d, &bufferint16=%p, bufferint16_size=%zu, NULL and returned rc=%d, bufferint16 contains: %d",
                 conn_res->hdbc, SQL_MAX_TABLE_NAME_LEN, (void *)&bufferint16, sizeof(bufferint16), rc, bufferint16);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            snprintf(messageStr, sizeof(messageStr), "Setting return_value->MAX_TABLE_NAME_LEN to: %d", bufferint16);
            LogMsg(DEBUG, messageStr);
            return_value->MAX_TABLE_NAME_LEN = PyInt_FromLong(bufferint16);
        }

        /* NON_NULLABLE_COLUMNS */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;

        rc = SQLGetInfo(conn_res->hdbc, SQL_NON_NULLABLE_COLUMNS, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo called with parameters conn_res->hdbc=%p, SQL_NON_NULLABLE_COLUMNS=%d, &bufferint16=%p, bufferint16_size=%zu, NULL and returned rc=%d, bufferint16 contains: %d",
                 conn_res->hdbc, SQL_NON_NULLABLE_COLUMNS, (void *)&bufferint16, sizeof(bufferint16), rc, bufferint16);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            switch (bufferint16)
            {
            case SQL_NNC_NON_NULL:
                LogMsg(DEBUG, "SQL_NON_NULLABLE_COLUMNS indicates non-nullable columns. Setting return_value->NON_NULLABLE_COLUMNS to Py_True.");
                Py_INCREF(Py_True);
                rv = Py_True;
                break;
            case SQL_NNC_NULL:
                LogMsg(DEBUG, "SQL_NON_NULLABLE_COLUMNS indicates nullable columns. Setting return_value->NON_NULLABLE_COLUMNS to Py_False.");
                Py_INCREF(Py_False);
                rv = Py_False;
                break;
            default:
                LogMsg(DEBUG, "SQL_NON_NULLABLE_COLUMNS returned an unexpected value. No action taken.");
                break;
            }
            return_value->NON_NULLABLE_COLUMNS = rv;
        }
        snprintf(messageStr, sizeof(messageStr), "Returning Python object at address: %p", (void *)return_value);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit server_info()");
        return (PyObject *)return_value;
    }
    LogMsg(INFO, "exit server_info()");
    Py_RETURN_FALSE;
}

/*!# ibm_db.client_info
 *
 * ===Description
 * object ibm_db.client_info ( resource connection )
 *
 * This function returns a read-only object with information about the IBM Data
 * Server database client. The following table lists the client properties:
 *
 * ====IBM Data Server client properties
 *
 * APPL_CODEPAGE:: The application code page.
 *
 * CONN_CODEPAGE:: The code page for the current connection.
 *
 * DATA_SOURCE_NAME:: The data source name (DSN) used to create the current
 * connection to the database.
 *
 * DRIVER_NAME:: The name of the library that implements the Call Level
 * Interface (CLI) specification.
 *
 * DRIVER_ODBC_VER:: The version of ODBC that the IBM Data Server client
 * supports. This returns a string "MM.mm" where MM is the major version and mm
 * is the minor version. The IBM Data Server client always returns "03.51".
 *
 * DRIVER_VER:: The version of the client, in the form of a string "MM.mm.uuuu"
 * where MM is the major version, mm is the minor version, and uuuu is the
 * update. For example, "08.02.0001" represents major version 8, minor version
 * 2, update 1. (string)
 *
 * ODBC_SQL_CONFORMANCE:: There are three levels of ODBC SQL grammar supported
 * by the client: MINIMAL (Supports the minimum ODBC SQL grammar), CORE
 * (Supports the core ODBC SQL grammar), EXTENDED (Supports extended ODBC SQL
 * grammar).
 *
 * ODBC_VER:: The version of ODBC that the ODBC driver manager supports. This
 * returns a string "MM.mm.rrrr" where MM is the major version, mm is the minor
 * version, and rrrr is the release. The client always returns "03.01.0000".
 *
 * ===Parameters
 *
 * ====connection
 *
 *      Specifies an active IBM Data Server client connection.
 *
 * ===Return Values
 *
 * Returns an object on a successful call. Returns FALSE on failure.
 */
static PyObject *ibm_db_client_info(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry client_info()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res = NULL;
    int rc = 0;
    char buffer255[255];
    SQLSMALLINT bufferint16;
    SQLUINTEGER bufferint32;

    le_client_info *return_value = PyObject_NEW(le_client_info,
                                                &client_infoType);

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p", py_conn_res);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        /* DRIVER_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DRIVER_NAME, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_DRIVER_NAME) rc=%d, buffer255=%s", rc, buffer255);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            return_value->DRIVER_NAME = StringOBJ_FromASCII(buffer255);
        }

        /* DRIVER_VER */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DRIVER_VER, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_DRIVER_VER) rc=%d, buffer255=%s", rc, buffer255);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            return_value->DRIVER_VER = StringOBJ_FromASCII(buffer255);
        }

        /* DATA_SOURCE_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DATA_SOURCE_NAME,
                        (SQLPOINTER)buffer255, sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_DATA_SOURCE_NAME) rc=%d, buffer255=%s", rc, buffer255);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            return_value->DATA_SOURCE_NAME = StringOBJ_FromASCII(buffer255);
        }

        /* DRIVER_ODBC_VER */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DRIVER_ODBC_VER,
                        (SQLPOINTER)buffer255, sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_DRIVER_ODBC_VER) rc=%d, buffer255=%s", rc, buffer255);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            return_value->DRIVER_ODBC_VER = StringOBJ_FromASCII(buffer255);
        }

#ifndef PASE /* i5/OS ODBC_VER handled natively */
        /* ODBC_VER */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_ODBC_VER, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_ODBC_VER) rc=%d, buffer255=%s", rc, buffer255);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            return_value->ODBC_VER = StringOBJ_FromASCII(buffer255);
        }
#endif /* PASE */

        /* ODBC_SQL_CONFORMANCE */
        bufferint16 = 0;
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_ODBC_SQL_CONFORMANCE, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_ODBC_SQL_CONFORMANCE) rc=%d, bufferint16=%d", rc, bufferint16);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            switch (bufferint16)
            {
            case SQL_OSC_MINIMUM:
                strcpy((char *)buffer255, "MINIMUM");
                break;
            case SQL_OSC_CORE:
                strcpy((char *)buffer255, "CORE");
                break;
            case SQL_OSC_EXTENDED:
                strcpy((char *)buffer255, "EXTENDED");
                break;
            default:
                break;
            }
            return_value->ODBC_SQL_CONFORMANCE = StringOBJ_FromASCII(buffer255);
        }

#if defined(__MVS__)
        return_value->APPL_CODEPAGE = PyInt_FromLong(1208);
        return_value->CONN_CODEPAGE = PyInt_FromLong(1208);
        LogMsg(INFO, "For MVS: APPL_CODEPAGE=1208, CONN_CODEPAGE=1208");
#elif !defined(PASE) /* i5/OS APPL_CODEPAGE handled natively */
        /* APPL_CODEPAGE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_APPLICATION_CODEPAGE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_APPLICATION_CODEPAGE) rc=%d, bufferint32=%d", rc, bufferint32);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            return_value->APPL_CODEPAGE = PyInt_FromLong(bufferint32);
        }

        /* CONN_CODEPAGE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_CONNECT_CODEPAGE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;

        snprintf(messageStr, sizeof(messageStr), "SQLGetInfo(SQL_CONNECT_CODEPAGE) rc=%d, bufferint32=%d", rc, bufferint32);
        LogMsg(DEBUG, messageStr);

        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
        else
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc,
                                                SQL_HANDLE_DBC, rc, 1,
                                                NULL, -1, 1);
            }
            return_value->CONN_CODEPAGE = PyInt_FromLong(bufferint32);
        }
#endif               /* PASE */

        LogMsg(INFO, "exit client_info()");
        return (PyObject *)return_value;
    }
    LogMsg(INFO, "exit client_info()");
    PyErr_Clear();
    Py_RETURN_FALSE;
}

/*!# ibm_db.active
 *
 * ===Description
 * Py_True/Py_False ibm_db.active(resource connection)
 *
 * Checks if the specified connection resource is active
 *
 * Returns Py_True if the given connection resource is active
 *
 * ===Parameters
 * ====connection
 *        The connection resource to be validated.
 *
 * ===Return Values
 *
 * Returns Py_True if the given connection resource is active, otherwise it will
 * return Py_False
 */
static PyObject *ibm_db_active(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry active()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    int rc;
    conn_handle *conn_res = NULL;
    SQLINTEGER conn_alive;

    conn_alive = 0;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p", py_conn_res);
    LogMsg(DEBUG, messageStr);
    if (!(NIL_P(py_conn_res) || (py_conn_res == Py_None)))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
#if !defined(PASE) && !defined(__MVS__)
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetConnectAttr(conn_res->hdbc, SQL_ATTR_PING_DB,
                               (SQLPOINTER)&conn_alive, 0, NULL);
        snprintf(messageStr, sizeof(messageStr),
                 "SQLGetConnectAttr executed: rc=%d, conn_alive=%d", rc, conn_alive);
        LogMsg(DEBUG, messageStr);
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR)
        {
            snprintf(messageStr, sizeof(messageStr),
                     "SQL_ERROR occurred: rc=%d, conn_alive=%d", rc, conn_alive);
            LogMsg(ERROR, messageStr);
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
        }
#endif /* PASE */
    }
    /*
     * SQLGetConnectAttr with SQL_ATTR_PING_DB will return 0 on failure but will
     * return the ping time on success.    We only want success or failure.
     */
    if (conn_alive == 0)
    {
        LogMsg(INFO, "exit active()");
        Py_RETURN_FALSE;
    }
    else
    {
        LogMsg(INFO, "exit active()");
        Py_RETURN_TRUE;
    }
}

/*!# ibm_db.get_option
 *
 * ===Description
 * mixed ibm_db.get_option ( resource resc, int options, int type )
 *
 * Returns a value, that is the current setting of a connection or statement
 * attribute.
 *
 * ===Parameters
 *
 * ====resc
 *        A valid connection or statement resource containing a result set.
 *
 * ====options
 *        The options to be retrieved
 *
 * ====type
 *        A field that specifies the resource type (1 = Connection,
 *        non - 1 = Statement)
 *
 * ===Return Values
 *
 * Returns the current setting of the resource attribute provided.
 */
static PyObject *ibm_db_get_option(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry get_option()");
    LogUTF8Msg(args);
    PyObject *conn_or_stmt = NULL;
    PyObject *retVal = NULL;
    PyObject *py_op_integer = NULL;
    PyObject *py_type = NULL;
    SQLCHAR *value = NULL;
    SQLINTEGER value_int = 0;
    conn_handle *conn_res = NULL;
    stmt_handle *stmt_res = NULL;
    SQLINTEGER op_integer = 0;
    SQLINTEGER isInteger = 0;
    long type = 0;
    int rc;

    if (!PyArg_ParseTuple(args, "OOO", &conn_or_stmt, &py_op_integer, &py_type))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: conn_or_stmt=%p, py_op_integer=%p, py_type=%p",
             (void *)conn_or_stmt, (void *)py_op_integer, (void *)py_type);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(conn_or_stmt))
    {
        if (!NIL_P(py_op_integer))
        {
            if (PyInt_Check(py_op_integer))
            {
                op_integer = (SQLINTEGER)PyLong_AsLong(py_op_integer);
                snprintf(messageStr, sizeof(messageStr), "op_integer: %d", op_integer);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Supplied py_op_integer parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if (!NIL_P(py_type))
        {
            if (PyInt_Check(py_type))
            {
                type = PyLong_AsLong(py_type);
                snprintf(messageStr, sizeof(messageStr), "type: %ld", type);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Supplied py_type parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        /* Checking to see if we are getting a connection option (1) or a
         * statement option (non - 1)
         */
        if (type == 1)
        {
            if (!PyObject_TypeCheck(conn_or_stmt, &conn_handleType))
            {
                LogMsg(ERROR, "Supplied connection object Parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
                return NULL;
            }
            conn_res = (conn_handle *)conn_or_stmt;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);

            /* Check to ensure the connection resource given is active */
            if (!conn_res->handle_active)
            {
                LogMsg(ERROR, "Connection is not active");
                PyErr_SetString(PyExc_Exception, "Connection is not active");
                return NULL;
            }
            /* Check that the option given is not null */
            if (!NIL_P(py_op_integer))
            {
                /* ACCTSTR_LEN is the largest possible length of the options to
                 * retrieve
                 */
                switch (op_integer)
                {
                case SQL_ATTR_AUTOCOMMIT:
                case SQL_ATTR_USE_TRUSTED_CONTEXT:
                case SQL_ATTR_TXN_ISOLATION:
                    isInteger = 1;
                    snprintf(messageStr, sizeof(messageStr), "Option %d is considered integer", op_integer);
                    LogMsg(DEBUG, messageStr);
                    break;
                default:
                    isInteger = 0;
                    snprintf(messageStr, sizeof(messageStr), "Option %d is not considered integer", op_integer);
                    LogMsg(DEBUG, messageStr);
                    break;
                }

                if (isInteger == 0)
                {
                    value = (SQLCHAR *)ALLOC_N(char, ACCTSTR_LEN + 1);
                    if (value == NULL)
                    {
                        LogMsg(ERROR, "Failed to Allocate Memory for value");
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return NULL;
                    }
                    memset(value, 0, ACCTSTR_LEN + 1);

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetConnectAttr((SQLHDBC)conn_res->hdbc, op_integer,
                                           (SQLPOINTER)value, ACCTSTR_LEN, NULL);
                    Py_END_ALLOW_THREADS;
                    snprintf(messageStr, sizeof(messageStr), "SQLGetConnectAttr called with parameters SQLHDBC=%p, op_integer=%d, value=%p, ACCTSTR_LEN=%d, and returned rc=%d",
                             (SQLHDBC)conn_res->hdbc, op_integer, (void *)value, ACCTSTR_LEN, rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_ERROR)
                    {
                        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                        rc, 1, NULL, -1, 1);
                        if (value != NULL)
                        {
                            PyMem_Del(value);
                            value = NULL;
                        }
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    retVal = StringOBJ_FromASCII((char *)value);
                    if (value != NULL)
                    {
                        PyMem_Del(value);
                        value = NULL;
                    }
                    snprintf(messageStr, sizeof(messageStr), "Returning string option value: %s", PyUnicode_AsUTF8(retVal));
                    LogMsg(DEBUG, messageStr);
                    LogMsg(INFO, "exit get_option()");
                    return retVal;
                }
                else
                {
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetConnectAttr((SQLHDBC)conn_res->hdbc, op_integer,
                                           &value_int, SQL_IS_INTEGER, NULL);
                    Py_END_ALLOW_THREADS;
                    snprintf(messageStr, sizeof(messageStr), "SQLGetConnectAttr called with parameters SQLHDBC=%p, op_integer=%d, value_int=%p, SQL_IS_INTEGER=%d, and returned rc=%d",
                             (SQLHDBC)conn_res->hdbc, op_integer, (void *)&value_int, SQL_IS_INTEGER, rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_ERROR)
                    {
                        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                        rc, 1, NULL, -1, 1);
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    snprintf(messageStr, sizeof(messageStr), "Returning integer option value: %ld", value_int);
                    LogMsg(DEBUG, messageStr);
                    LogMsg(INFO, "exit get_option()");
                    return PyInt_FromLong(value_int);
                }
            }
            else
            {
                LogMsg(ERROR, "Supplied py_op_integer parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
            /* At this point we know we are to retreive a statement option */
        }
        else
        {
            stmt_res = (stmt_handle *)conn_or_stmt;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
            /* Check that the option given is not null */
            if (!NIL_P(py_op_integer))
            {
                /* Checking that the option to get is the cursor type because that
                 * is what we support here
                 */
                switch (op_integer)
                {
                case SQL_ATTR_CURSOR_TYPE:
                case SQL_ATTR_ROWCOUNT_PREFETCH:
                case SQL_ATTR_QUERY_TIMEOUT:
#ifndef __MVS__
                case SQL_ATTR_CALL_RETURN:
#endif
                    isInteger = 1;
                    snprintf(messageStr, sizeof(messageStr), "Option %d is considered integer", op_integer);
                    LogMsg(DEBUG, messageStr);
                    break;
                default:
                    isInteger = 0;
                    snprintf(messageStr, sizeof(messageStr), "Option %d is not considered integer", op_integer);
                    LogMsg(DEBUG, messageStr);
                    break;
                }

                if (isInteger == 0)
                {
                    // string value pointer
                    value = (SQLCHAR *)ALLOC_N(char, ACCTSTR_LEN + 1);
                    if (value == NULL)
                    {
                        LogMsg(ERROR, "Failed to Allocate Memory for value");
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return NULL;
                    }
                    memset(value, 0, ACCTSTR_LEN + 1);
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetStmtAttr((SQLHSTMT)stmt_res->hstmt, op_integer,
                                        (SQLPOINTER)value, ACCTSTR_LEN, NULL);
                    Py_END_ALLOW_THREADS;
                    snprintf(messageStr, sizeof(messageStr), "SQLGetStmtAttr called with parameters SQLHSTMT=%p, op_integer=%d, value=%p, ACCTSTR_LEN=%d, and returned rc=%d",
                             (SQLHSTMT)stmt_res->hstmt, op_integer, (void *)value, ACCTSTR_LEN, rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_ERROR)
                    {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        if (value != NULL)
                        {
                            PyMem_Del(value);
                            value = NULL;
                        }
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    retVal = StringOBJ_FromASCII((char *)value);
                    if (value != NULL)
                    {
                        PyMem_Del(value);
                        value = NULL;
                    }
                    snprintf(messageStr, sizeof(messageStr), "Returning string option value: %s", PyUnicode_AsUTF8(retVal));
                    LogMsg(DEBUG, messageStr);
                    LogMsg(INFO, "exit get_option()");
                    return retVal;
                }
                else
                {
                    // integer value
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetStmtAttr((SQLHSTMT)stmt_res->hstmt, op_integer,
                                        &value_int, SQL_IS_INTEGER, NULL);
                    Py_END_ALLOW_THREADS;
                    snprintf(messageStr, sizeof(messageStr), "SQLGetStmtAttr called with parameters SQLHSTMT=%p, op_integer=%d, value_int=%p, SQL_IS_INTEGER=%d, and returned rc=%d",
                             (SQLHSTMT)stmt_res->hstmt, op_integer, (void *)&value_int, SQL_IS_INTEGER, rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_ERROR)
                    {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    snprintf(messageStr, sizeof(messageStr), "Returning integer option value: %ld", value_int);
                    LogMsg(DEBUG, messageStr);
                    LogMsg(INFO, "exit get_option()");
                    return PyInt_FromLong(value_int);
                }
            }
            else
            {
                LogMsg(ERROR, "Supplied py_op_integer parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
    }
    LogMsg(INFO, "exit get_option()");
    PyErr_Clear();
    Py_RETURN_FALSE;
}

static int _ibm_db_chaining_flag(stmt_handle *stmt_res, SQLINTEGER flag, error_msg_node *error_list, int client_err_cnt)
{
    LogMsg(INFO, "entry _ibm_db_chaining_flag()");
    snprintf(messageStr, sizeof(messageStr), "stmt_res=%p, flag=%d, error_list=%p, client_err_cnt=%d",
             (void *)stmt_res, flag, (void *)error_list, client_err_cnt);
    LogMsg(DEBUG, messageStr);
#ifdef __MVS__
    /* SQL_ATTR_CHAINING_BEGIN and SQL_ATTR_CHAINING_END are not defined */
    LogMsg(DEBUG, "Returning SQL_SUCCESS as SQL_ATTR_CHAINING_BEGIN and SQL_ATTR_CHAINING_END are not defined.");
    return SQL_SUCCESS;
#else
    int rc;
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLSetStmtAttrW((SQLHSTMT)stmt_res->hstmt, flag, (SQLPOINTER)SQL_TRUE, SQL_IS_INTEGER);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr),
             "Called SQLSetStmtAttrW with parameters hstmt=%p, attr=%d, value=%p, text=%d and returned rc=%d",
             (void *)stmt_res->hstmt, flag, (void *)SQL_TRUE, SQL_IS_INTEGER, rc);
    LogMsg(DEBUG, messageStr);
    if (flag == SQL_ATTR_CHAINING_BEGIN)
    {
        if (rc == SQL_ERROR)
        {
            LogMsg(DEBUG, "SQL_ATTR_CHAINING_BEGIN encountered an error");
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            PyErr_SetString(PyExc_Exception, IBM_DB_G(__python_stmt_err_msg));
        }
    }
    else
    {
        if ((rc != SQL_SUCCESS) || (client_err_cnt != 0))
        {
            SQLINTEGER errNo = 0;
            PyObject *errTuple = NULL;
            SQLINTEGER err_cnt = 0;
            PyObject *err_msg = NULL, *err_fmtObj = NULL;
            char *err_fmt = NULL;
            size_t err_fmt_offset = 0;
            if (rc != SQL_SUCCESS)
            {
#ifdef __MVS__
                /* MVS only seems to have SQLGetDiagRec */
                LogMsg(DEBUG, "MVS only seems to have SQLGetDiagRec: setting rc to SQL_SUCCESS");
                rc = SQL_SUCCESS;
#else
                snprintf(messageStr, sizeof(messageStr), "Calling SQLGetDiagField: handle=%p, field=%d, pointer=%p, text=%d",
                         (void *)stmt_res->hstmt, SQL_DIAG_NUMBER, (void *)&err_cnt, SQL_IS_POINTER);
                LogMsg(DEBUG, messageStr);
                SQLGetDiagField(SQL_HANDLE_STMT, (SQLHSTMT)stmt_res->hstmt, 0, SQL_DIAG_NUMBER, (SQLPOINTER)&err_cnt, SQL_IS_POINTER, NULL);
#endif
            }
            snprintf(messageStr, sizeof(messageStr), "Number of errors detected: err_cnt=%d, client_err_cnt=%d", err_cnt, client_err_cnt);
            LogMsg(DEBUG, messageStr);
            errTuple = PyTuple_New(err_cnt + client_err_cnt);
            /* Allocate enough space for largest possible int value. */
            err_fmt = (char *)PyMem_Malloc(strlen("Error 2147483647: %s\n") * (err_cnt + client_err_cnt) + 1);
            if (err_fmt != NULL)
            {
                err_fmt[0] = '\0';
                errNo = 1;
                while (error_list != NULL)
                {
                    snprintf(messageStr, sizeof(messageStr), "Adding error to tuple: Error %d: %s", errNo, error_list->err_msg);
                    LogMsg(DEBUG, messageStr);
                    err_fmt_offset += sprintf(err_fmt + err_fmt_offset, "Error %d: %s\n", (int)errNo, "%s");
                    PyTuple_SetItem(errTuple, errNo - 1, StringOBJ_FromASCII(error_list->err_msg));
                    error_list = error_list->next;
                    errNo++;
                }
                for (errNo = client_err_cnt + 1; errNo <= (err_cnt + client_err_cnt); errNo++)
                {
                    snprintf(messageStr, sizeof(messageStr), "Adding SQL error to tuple: Error %d", errNo);
                    LogMsg(DEBUG, messageStr);
                    err_fmt_offset += sprintf(err_fmt + err_fmt_offset, "Error %d: %s\n", (int)errNo, "%s");
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt, SQL_HANDLE_STMT, SQL_ERROR, 1, NULL, -1, (errNo - client_err_cnt));
                    PyTuple_SetItem(errTuple, errNo - 1, StringOBJ_FromASCII(IBM_DB_G(__python_stmt_err_msg)));
                }
                err_fmtObj = StringOBJ_FromASCII(err_fmt);
                err_msg = StringObj_Format(err_fmtObj, errTuple);
                if (err_fmtObj != NULL)
                {
                    Py_XDECREF(err_fmtObj);
                }
                if (err_fmt != NULL)
                {
                    PyMem_Free(err_fmt);
                }
                PyErr_SetObject(PyExc_Exception, err_msg);
            }
            else
            {
                LogMsg(EXCEPTION, "Failed to allocate memory for error message format.");
                PyErr_SetString(PyExc_MemoryError, "Failed to allocate memory for error message format.");
                return -1;
            }
        }
    }
    LogMsg(INFO, "exit _ibm_db_chaining_flag()");
    return rc;
#endif
}

static void _build_client_err_list(error_msg_node *head_error_list, char *err_msg)
{
    error_msg_node *tmp_err = NULL, *curr_err = head_error_list->next, *prv_err = NULL;
    tmp_err = ALLOC(error_msg_node);
    memset(tmp_err, 0, sizeof(error_msg_node));
    strcpy(tmp_err->err_msg, err_msg);
    tmp_err->next = NULL;
    while (curr_err != NULL)
    {
        prv_err = curr_err;
        curr_err = curr_err->next;
    }

    if (head_error_list->next == NULL)
    {
        head_error_list->next = tmp_err;
    }
    else
    {
        prv_err->next = tmp_err;
    }
}

/*
 * ibm_db.execute_many -- can be used to execute an SQL with multiple values of parameter marker.
 * ===Description
 * int ibm_db.execute_many(IBM_DBStatement, Parameters[, Options])
 * Returns number of inserted/updated/deleted rows if batch executed successfully.
 * return NULL if batch fully or partialy fails  (All the rows executed except for which error occurs).
 */
static PyObject *ibm_db_execute_many(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry execute_many()");
    LogUTF8Msg(args);
    PyObject *options = NULL;
    PyObject *params = NULL;
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res = NULL;
    char error[DB2_MAX_ERR_MSG_LEN + 50];
    PyObject *data = NULL;
    error_msg_node *head_error_list = NULL;
    int err_count = 0;

    int rc;
    int i = 0;
    SQLSMALLINT numOpts = 0;
    int numOfRows = 0;
    int numOfParam = 0;
    SQLINTEGER row_cnt = 0;
    int chaining_start = 0;

    SQLSMALLINT *data_type;
    SQLUINTEGER precision;
    SQLSMALLINT scale;
    SQLSMALLINT nullable;
    SQLSMALLINT *ref_data_type;

    /* Get the parameters
     *      1. statement handler Object
     *      2. Parameters
     *      3. Options (optional) */
#if defined __MVS__
    LogMsg(ERROR, "Not supported: This function is currently not supported on this platform");
    PyErr_SetString(PyExc_Exception, "Not supported: This function is currently not supported on this platform");
    return NULL;
#endif

    if (!PyArg_ParseTuple(args, "OO|O", &py_stmt_res, &params, &options))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_stmt_res=%p, params=%p, options=%p", py_stmt_res, params, options);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_stmt_res))
    {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_stmt_res;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res: %p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }
        /* Free any cursors that might have been allocated in a previous call to SQLExecute */
        Py_BEGIN_ALLOW_THREADS;
        SQLFreeStmt((SQLHSTMT)stmt_res->hstmt, SQL_CLOSE);
        Py_END_ALLOW_THREADS;

        _python_ibm_db_clear_stmt_err_cache();
        stmt_res->head_cache_list = NULL;
        stmt_res->current_node = NULL;

        /* Bind parameters */
        snprintf(messageStr, sizeof(messageStr), "Calling SQLNumParams with statement handle: %p", (void *)stmt_res->hstmt);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLNumParams((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT *)&numOpts);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLNumParams executed with return code: %d, numOpts: %d", rc, numOpts);
        LogMsg(DEBUG, messageStr);
        data_type = (SQLSMALLINT *)ALLOC_N(SQLSMALLINT, numOpts);
        ref_data_type = (SQLSMALLINT *)ALLOC_N(SQLSMALLINT, numOpts);
        for (i = 0; i < numOpts; i++)
        {
            ref_data_type[i] = -1;
        }
        if (numOpts != 0)
        {
            for (i = 0; i < numOpts; i++)
            {
                snprintf(messageStr, sizeof(messageStr), "Calling SQLDescribeParam with statement handle: %p, parameter index: %d", (void *)stmt_res->hstmt, i + 1);
                LogMsg(DEBUG, messageStr);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt, i + 1,
                                      (SQLSMALLINT *)(data_type + i), &precision, (SQLSMALLINT *)&scale,
                                      (SQLSMALLINT *)&nullable);
                Py_END_ALLOW_THREADS;
                snprintf(messageStr, sizeof(messageStr), "SQLDescribeParam executed with return code: %d, precision: %u, scale: %d, nullable: %d",
                         rc, precision, scale, nullable);
                LogMsg(DEBUG, messageStr);
                if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
                {
                    _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                                                    SQL_HANDLE_STMT,
                                                    rc, 1, NULL, -1, 1);
                }
                if (rc == SQL_ERROR)
                {
                    PyErr_SetString(PyExc_Exception, IBM_DB_G(__python_stmt_err_msg));
                    return NULL;
                }
                build_list(stmt_res, i + 1, data_type[i], precision,
                           scale, nullable);
            }
        }

        /* Execute SQL for all set of parameters */
        numOfRows = PyTuple_Size(params);
        head_error_list = ALLOC(error_msg_node);
        if (head_error_list != NULL)
        {
            memset(head_error_list, 0, sizeof(error_msg_node));
            head_error_list->next = NULL;
        }
        else
        {
            LogMsg(ERROR, "Memory allocation for head_error_list failed");
        }
        if (numOfRows > 0)
        {
            for (i = 0; i < numOfRows; i++)
            {
                int j = 0;
                param_node *curr = NULL;
                PyObject *param = PyTuple_GET_ITEM(params, i);
                error[0] = '\0';
                if (!PyTuple_Check(param))
                {
                    sprintf(error, "Value parameter %d is not a tuple", i + 1);
                    if (head_error_list != NULL)
                    {
                        _build_client_err_list(head_error_list, error);
                        err_count++;
                    }
                    else
                    {
                        LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                    }
                    continue;
                }

                numOfParam = PyTuple_Size(param);
                if (numOpts < numOfParam)
                {
                    /* More are passed in -- Warning - Use the max number present */
                    sprintf(error, "Value parameter tuple %d has more parameters than previous tuple", i + 1);
                    if (head_error_list != NULL)
                    {
                        _build_client_err_list(head_error_list, error);
                        err_count++;
                    }
                    else
                    {
                        LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                    }
                    continue;
                }
                else if (numOpts > numOfParam)
                {
                    /* If there are less params passed in, than are present
                     * -- Error
                     */
                    sprintf(error, "Value parameter tuple %d has fewer parameters than previous tuple", i + 1);
                    if (head_error_list != NULL)
                    {
                        _build_client_err_list(head_error_list, error);
                        err_count++;
                    }
                    else
                    {
                        LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                    }
                    continue;
                }

                /* Bind values from the parameters_tuple to params */
                curr = stmt_res->head_cache_list;

                while (curr != NULL)
                {
                    data = PyTuple_GET_ITEM(param, j);
                    if (data == NULL)
                    {
                        sprintf(error, "NULL value passed for value parameter: %d", i + 1);
                        if (head_error_list != NULL)
                        {
                            _build_client_err_list(head_error_list, error);
                            err_count++;
                        }
                        else
                        {
                            LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                        }
                        break;
                    }

                    if (chaining_start)
                    {
                        // This check is not required for python boolean values True and False as both True and False are homogeneous for boolean.
                        if ((TYPE(data) != PYTHON_NIL) && (TYPE(data) != PYTHON_TRUE) && (TYPE(data) != PYTHON_FALSE) && (ref_data_type[curr->param_num - 1] != TYPE(data)) && (ref_data_type[curr->param_num - 1] != PYTHON_NIL))
                        {
                            sprintf(error, "Value parameter tuple %d has types that are not homogeneous with previous tuple", i + 1);
                            if (head_error_list != NULL)
                            {
                                _build_client_err_list(head_error_list, error);
                                err_count++;
                            }
                            else
                            {
                                LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                            }
                            break;
                        }
                    }
                    else
                    {
                        if (TYPE(data) != PYTHON_NIL)
                        {
                            ref_data_type[curr->param_num - 1] = TYPE(data);
                        }
                        else
                        {
                            int i_tmp;
                            PyObject *param_tmp = NULL;
                            PyObject *data_tmp = NULL;
                            i_tmp = i + 1;
                            for (i_tmp = i + 1; i_tmp < numOfRows; i_tmp++)
                            {
                                param_tmp = PyTuple_GET_ITEM(params, i_tmp);
                                if (!PyTuple_Check(param_tmp))
                                {
                                    continue;
                                }
                                data_tmp = PyTuple_GET_ITEM(param_tmp, j);
                                if (TYPE(data_tmp) != PYTHON_NIL)
                                {
                                    ref_data_type[curr->param_num - 1] = TYPE(data_tmp);
                                    break;
                                }
                                else
                                {
                                    continue;
                                }
                            }
                            if (ref_data_type[curr->param_num - 1] == -1)
                            {
                                ref_data_type[curr->param_num - 1] = PYTHON_NIL;
                            }
                        }
                    }

                    curr->data_type = data_type[curr->param_num - 1];
                    if (TYPE(data) != PYTHON_NIL)
                    {
                        rc = _python_ibm_db_bind_data(stmt_res, curr, data);
                    }
                    else
                    {
                        SQLSMALLINT valueType = 0;
                        switch (ref_data_type[curr->param_num - 1])
                        {
                        case PYTHON_FIXNUM:
                            if (curr->data_type == SQL_BIGINT || curr->data_type == SQL_DECIMAL)
                            {
                                valueType = SQL_C_CHAR;
                            }
                            else
                            {
                                valueType = SQL_C_LONG;
                            }
                            break;
                        case PYTHON_FALSE:
                        case PYTHON_TRUE:
                            valueType = SQL_C_LONG;
                            break;
                        case PYTHON_FLOAT:
                            valueType = SQL_C_DOUBLE;
                            break;
                        case PYTHON_UNICODE:
                            switch (curr->data_type)
                            {
                            case SQL_BLOB:
                            case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                            case SQL_LONGVARBINARY:
#endif /* PASE */
                            case SQL_VARBINARY:
                                valueType = SQL_C_BINARY;
                                break;
                            default:
                                valueType = SQL_C_WCHAR;
                            }
                            break;
                        case PYTHON_STRING:
                            switch (curr->data_type)
                            {
                            case SQL_BLOB:
                            case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                            case SQL_LONGVARBINARY:
#endif /* PASE */
                            case SQL_VARBINARY:
                                valueType = SQL_C_BINARY;
                                break;
                            default:
                                valueType = SQL_C_CHAR;
                            }
                            break;
                        case PYTHON_DATE:
                            valueType = SQL_C_TYPE_DATE;
                            break;
                        case PYTHON_TIME:
                            valueType = SQL_C_TYPE_TIME;
                            break;
                        case PYTHON_TIMESTAMP:
                            valueType = SQL_C_TYPE_TIMESTAMP;
                            break;
                        case PYTHON_DECIMAL:
                            valueType = SQL_C_CHAR;
                            break;
                        case PYTHON_NIL:
                            valueType = SQL_C_DEFAULT;
                            break;
                        }
                        curr->ivalue = SQL_NULL_DATA;

                        Py_BEGIN_ALLOW_THREADS;
                        rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, &curr->ivalue, 0, (SQLLEN *)&(curr->ivalue));
                        Py_END_ALLOW_THREADS;
                        snprintf(messageStr, sizeof(messageStr), "SQLBindParameter return code: %d", rc);
                        LogMsg(DEBUG, messageStr);
                        if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO)
                        {
                            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        }
                    }
                    if (rc != SQL_SUCCESS)
                    {
                        sprintf(error, "Binding Error 1: %s",
                                IBM_DB_G(__python_stmt_err_msg));
                        LogMsg(ERROR, error);
                        if (head_error_list != NULL)
                        {
                            _build_client_err_list(head_error_list, error);
                            err_count++;
                        }
                        else
                        {
                            LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                        }
                        break;
                    }
                    curr = curr->next;
                    j++;
                }

                if (!chaining_start && (error[0] == '\0'))
                {
                    /* Set statement attribute SQL_ATTR_CHAINING_BEGIN */
                    rc = _ibm_db_chaining_flag(stmt_res, SQL_ATTR_CHAINING_BEGIN, NULL, 0);
                    chaining_start = 1;
                    snprintf(messageStr, sizeof(messageStr), "SQL_ATTR_CHAINING_BEGIN flag set. rc: %d", rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc != SQL_SUCCESS)
                    {
                        return NULL;
                    }
                }

                if (error[0] == '\0')
                {
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLExecute((SQLHSTMT)stmt_res->hstmt);
                    Py_END_ALLOW_THREADS;
                    snprintf(messageStr, sizeof(messageStr), "SQLExecute return code: %d", rc);
                    LogMsg(DEBUG, messageStr);
                    if (rc == SQL_NEED_DATA)
                    {
                        SQLPOINTER valuePtr;
                        Py_BEGIN_ALLOW_THREADS;
                        rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
                        Py_END_ALLOW_THREADS;
                        snprintf(messageStr, sizeof(messageStr), "SQLParamData return code: %d", rc);
                        LogMsg(DEBUG, messageStr);
                        while (rc == SQL_NEED_DATA)
                        {
                            /* passing data value for a parameter */
                            if (!NIL_P(((param_node *)valuePtr)->svalue))
                            {
                                Py_BEGIN_ALLOW_THREADS;
                                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node *)valuePtr)->svalue), ((param_node *)valuePtr)->ivalue);
                                Py_END_ALLOW_THREADS;
                            }
                            else
                            {
                                Py_BEGIN_ALLOW_THREADS;
                                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node *)valuePtr)->uvalue), ((param_node *)valuePtr)->ivalue);
                                Py_END_ALLOW_THREADS;
                            }
                            snprintf(messageStr, sizeof(messageStr), "SQLPutData return code: %d", rc);
                            LogMsg(DEBUG, messageStr);
                            if (rc == SQL_ERROR)
                            {
                                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                                sprintf(error, "Sending data failed: %s", IBM_DB_G(__python_stmt_err_msg));
                                LogMsg(ERROR, error);
                                if (head_error_list != NULL)
                                {
                                    _build_client_err_list(head_error_list, error);
                                    err_count++;
                                }
                                else
                                {
                                    LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                                }
                                break;
                            }
                            Py_BEGIN_ALLOW_THREADS;
                            rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
                            Py_END_ALLOW_THREADS;
                            snprintf(messageStr, sizeof(messageStr), "SQLParamData return code: %d", rc);
                            LogMsg(DEBUG, messageStr);
                        }
                    }
                    else if (rc == SQL_ERROR)
                    {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        sprintf(error, "SQLExecute failed: %s", IBM_DB_G(__python_stmt_err_msg));
                        LogMsg(ERROR, error);
                        PyErr_SetString(PyExc_Exception, error);
                        if (head_error_list != NULL)
                        {
                            _build_client_err_list(head_error_list, error);
                            err_count++;
                        }
                        else
                        {
                            LogMsg(ERROR, "head_error_list is NULL, cannot build client error list");
                        }
                        break;
                    }
                }
            }
        }
        else
        {
            LogMsg(DEBUG, "No rows in parameters. Returning 0.");
            LogMsg(INFO, "exit execute_many()");
            return PyInt_FromLong(0);
        }

        /* Set statement attribute SQL_ATTR_CHAINING_END */
        if (head_error_list != NULL)
        {
            rc = _ibm_db_chaining_flag(stmt_res, SQL_ATTR_CHAINING_END, head_error_list->next, err_count);
            snprintf(messageStr, sizeof(messageStr), "SQL_ATTR_CHAINING_END flag set. rc: %d", rc);
            LogMsg(DEBUG, messageStr);
        }
        else
        {
            LogMsg(ERROR, "head_error_list is NULL, cannot process the error list");
        }
        if (head_error_list != NULL)
        {
            error_msg_node *tmp_err = NULL;
            while (head_error_list != NULL)
            {
                tmp_err = head_error_list;
                head_error_list = head_error_list->next;
                PyMem_Del(tmp_err);
            }
        }
        if (rc != SQL_SUCCESS || err_count != 0)
        {
            LogMsg(ERROR, "Errors encountered during execution");
            return NULL;
        }
    }
    else
    {
        LogMsg(ERROR, "Supplied parameter is invalid");
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLRowCount((SQLHSTMT)stmt_res->hstmt, &row_cnt);
    Py_END_ALLOW_THREADS;
    snprintf(messageStr, sizeof(messageStr), "SQLRowCount return code: %d, row_count: %d", rc, row_cnt);
    LogMsg(DEBUG, messageStr);
    if ((rc == SQL_ERROR) && (stmt_res != NULL))
    {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
        sprintf(error, "SQLRowCount failed: %s", IBM_DB_G(__python_stmt_err_msg));
        LogMsg(ERROR, error);
        PyErr_SetString(PyExc_Exception, error);
        return NULL;
    }
    PyMem_Del(data_type);
    PyMem_Del(ref_data_type);
    LogMsg(INFO, "exit execute_many()");
    return PyInt_FromLong(row_cnt);
}

/*
 * ===Description
 *  ibm_db.callproc( conn_handle conn_res, char *procName, (In/INOUT/OUT parameters tuple) )
 *
 * Returns resultset and INOUT/OUT parameters
 *
 * ===Parameters
 * =====  conn_handle
 *        a valid connection resource
 * ===== procedure Name
 *        a valide procedure Name
 *
 * ===== parameters tuple
 *        parameters tuple containing In/OUT/INOUT  variables,
 *
 * ===Returns Values
 * ===== stmt_res
 *        statement resource containning result set
 *
 * ==== INOUT/OUT variables tuple
 *        tuple containing all INOUT/OUT variables
 *
 * If procedure not found than it return NULL
 */
static PyObject *ibm_db_callproc(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry callproc()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *parameters_tuple = NULL;
    PyObject *outTuple = NULL, *pyprocName = NULL, *data = NULL;
    conn_handle *conn_res = NULL;
    stmt_handle *stmt_res = NULL;
    param_node *tmp_curr = NULL;
    int numOfParam = 0;

    if (!PyArg_ParseTuple(args, "OO|O", &py_conn_res, &pyprocName, &parameters_tuple))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }

    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, pyprocName=%p, parameters_tuple=%p", (void *)py_conn_res, (void *)pyprocName, (void *)parameters_tuple);
    LogMsg(DEBUG, messageStr);

    if (!NIL_P(py_conn_res) && pyprocName != Py_None)
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res: %p", (void *)conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (StringObj_Size(pyprocName) == 0)
        {
            LogMsg(ERROR, "Empty Procedure Name");
            PyErr_SetString(PyExc_Exception, "Empty Procedure Name");
            return NULL;
        }

        if (!NIL_P(parameters_tuple))
        {
            PyObject *subsql1 = NULL;
            PyObject *subsql2 = NULL;
            char *strsubsql = NULL;
            PyObject *sql = NULL;
            int i = 0;
            if (!PyTuple_Check(parameters_tuple))
            {
                LogMsg(ERROR, "Param is not a tuple");
                PyErr_SetString(PyExc_Exception, "Param is not a tuple");
                return NULL;
            }
            numOfParam = PyTuple_Size(parameters_tuple);
            snprintf(messageStr, sizeof(messageStr), "Number of parameters: %d", numOfParam);
            LogMsg(DEBUG, messageStr);
            subsql1 = StringOBJ_FromASCII("CALL ");
            subsql2 = PyUnicode_Concat(subsql1, pyprocName);
            Py_XDECREF(subsql1);
            strsubsql = (char *)PyMem_Malloc(sizeof(char) * ((strlen("(  )") + strlen(", ?") * numOfParam) + 2));
            if (strsubsql == NULL)
            {
                LogMsg(ERROR, "Failed to Allocate Memory");
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }
            strsubsql[0] = '\0';
            strcat(strsubsql, "( ");
            for (i = 0; i < numOfParam; i++)
            {
                if (i == 0)
                {
                    strcat(strsubsql, " ?");
                }
                else
                {
                    strcat(strsubsql, ", ?");
                }
            }
            strcat(strsubsql, " )");
            subsql1 = StringOBJ_FromASCII(strsubsql);
            sql = PyUnicode_Concat(subsql2, subsql1);
            Py_XDECREF(subsql1);
            Py_XDECREF(subsql2);
            snprintf(messageStr, sizeof(messageStr), "Constructed SQL statement: %s", PyUnicode_AsUTF8(sql));
            LogMsg(DEBUG, messageStr);
            stmt_res = (stmt_handle *)_python_ibm_db_prepare_helper(conn_res, sql, NULL);
            PyMem_Del(strsubsql);
            Py_XDECREF(sql);
            if (NIL_P(stmt_res))
            {
                LogMsg(ERROR, "Failed to prepare statement");
                return NULL;
            }
            snprintf(messageStr, sizeof(messageStr), "Prepared statement: stmt_res=%p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
            /* Bind values from the parameters_tuple to params */
            for (i = 0; i < numOfParam; i++)
            {
                PyObject *bind_result = NULL;
                data = PyTuple_GET_ITEM(parameters_tuple, i);
                snprintf(messageStr, sizeof(messageStr), "Binding parameter %d: data=%p", i, (void *)data);
                LogMsg(DEBUG, messageStr);
                bind_result = _python_ibm_db_bind_param_helper(4, stmt_res, i + 1, data, SQL_PARAM_INPUT_OUTPUT, 0, 0, 0, 0);
                if (NIL_P(bind_result))
                {
                    LogMsg(ERROR, "Failed to bind parameter");
                    return NULL;
                }
                snprintf(messageStr, sizeof(messageStr), "Bind result for parameter %d: bind_result=%p", i, (void *)bind_result);
                LogMsg(DEBUG, messageStr);
            }
        }
        else
        {
            PyObject *subsql1 = NULL;
            PyObject *subsql2 = NULL;
            PyObject *sql = NULL;
            subsql1 = StringOBJ_FromASCII("CALL ");
            subsql2 = PyUnicode_Concat(subsql1, pyprocName);
            Py_XDECREF(subsql1);
            subsql1 = StringOBJ_FromASCII("( )");
            sql = PyUnicode_Concat(subsql2, subsql1);
            Py_XDECREF(subsql1);
            Py_XDECREF(subsql2);
            snprintf(messageStr, sizeof(messageStr), "Constructed SQL statement (no parameters): %s", PyUnicode_AsUTF8(sql));
            LogMsg(DEBUG, messageStr);
            stmt_res = (stmt_handle *)_python_ibm_db_prepare_helper(conn_res, sql, NULL);
            Py_XDECREF(sql);
            if (NIL_P(stmt_res))
            {
                LogMsg(ERROR, "Failed to prepare statement");
                return NULL;
            }
            snprintf(messageStr, sizeof(messageStr), "Prepared statement (no parameters): stmt_res=%p", (void *)stmt_res);
            LogMsg(DEBUG, messageStr);
        }

        if (!NIL_P(_python_ibm_db_execute_helper1(stmt_res, NULL)))
        {
            tmp_curr = stmt_res->head_cache_list;
            if (numOfParam != 0 && tmp_curr != NULL)
            {
                int paramCount = 1;
                outTuple = PyTuple_New(numOfParam + 1);
                PyTuple_SetItem(outTuple, 0, (PyObject *)stmt_res);
                while (tmp_curr != NULL && (paramCount <= numOfParam))
                {
                    if ((tmp_curr->bind_indicator != SQL_NULL_DATA && tmp_curr->bind_indicator != SQL_NO_TOTAL))
                    {
                        switch (tmp_curr->data_type)
                        {
#ifdef __MVS__
                        case SQL_SMALLINT:
                        case SQL_INTEGER:
#else
                        case SQL_SMALLINT:
                        case SQL_INTEGER:
                        case SQL_BOOLEAN:
#endif
                            PyTuple_SetItem(outTuple, paramCount,
                                            PyInt_FromLong(tmp_curr->ivalue));
                            snprintf(messageStr, sizeof(messageStr), "Parameter %d: int value=%ld", paramCount, tmp_curr->ivalue);
                            LogMsg(DEBUG, messageStr);
                            paramCount++;
                            break;
                        case SQL_REAL:
                        case SQL_FLOAT:
                        case SQL_DOUBLE:
                            PyTuple_SetItem(outTuple, paramCount,
                                            PyFloat_FromDouble(tmp_curr->fvalue));
                            snprintf(messageStr, sizeof(messageStr), "Parameter %d: float value=%f", paramCount, tmp_curr->fvalue);
                            LogMsg(DEBUG, messageStr);
                            paramCount++;
                            break;
                        case SQL_TYPE_DATE:
                            if (!NIL_P(tmp_curr->date_value))
                            {
                                PyTuple_SetItem(outTuple, paramCount,
                                                PyDate_FromDate(tmp_curr->date_value->year,
                                                                tmp_curr->date_value->month,
                                                                tmp_curr->date_value->day));
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: date value=%d-%02d-%02d", paramCount, tmp_curr->date_value->year, tmp_curr->date_value->month, tmp_curr->date_value->day);
                                LogMsg(DEBUG, messageStr);
                            }
                            else
                            {
                                Py_INCREF(Py_None);
                                PyTuple_SetItem(outTuple, paramCount, Py_None);
                            }
                            paramCount++;
                            break;
                        case SQL_TYPE_TIME:
                            if (!NIL_P(tmp_curr->time_value))
                            {
                                PyTuple_SetItem(outTuple, paramCount,
                                                PyTime_FromTime(tmp_curr->time_value->hour % 24,
                                                                tmp_curr->time_value->minute,
                                                                tmp_curr->time_value->second, 0));
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: time value=%02d:%02d:%02d", paramCount, tmp_curr->time_value->hour % 24, tmp_curr->time_value->minute, tmp_curr->time_value->second);
                                LogMsg(DEBUG, messageStr);
                            }
                            else
                            {
                                Py_INCREF(Py_None);
                                PyTuple_SetItem(outTuple, paramCount, Py_None);
                            }
                            paramCount++;
                            break;
                        case SQL_TYPE_TIMESTAMP:
                            if (!NIL_P(tmp_curr->ts_value))
                            {
                                PyTuple_SetItem(outTuple, paramCount,
                                                PyDateTime_FromDateAndTime(tmp_curr->ts_value->year,
                                                                           tmp_curr->ts_value->month, tmp_curr->ts_value->day,
                                                                           tmp_curr->ts_value->hour % 24,
                                                                           tmp_curr->ts_value->minute,
                                                                           tmp_curr->ts_value->second,
                                                                           tmp_curr->ts_value->fraction / 1000));
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: timestamp value=%d-%02d-%02d %02d:%02d:%02d.%03d", paramCount, tmp_curr->ts_value->year, tmp_curr->ts_value->month, tmp_curr->ts_value->day, tmp_curr->ts_value->hour % 24, tmp_curr->ts_value->minute, tmp_curr->ts_value->second, tmp_curr->ts_value->fraction / 1000);
                                LogMsg(DEBUG, messageStr);
                            }
                            else
                            {
                                Py_INCREF(Py_None);
                                PyTuple_SetItem(outTuple, paramCount, Py_None);
                            }
                            paramCount++;
                            break;
                        case SQL_BIGINT:
                            if (!NIL_P(tmp_curr->svalue))
                            {
                                PyTuple_SetItem(outTuple, paramCount,
                                                PyLong_FromString(tmp_curr->svalue,
                                                                  NULL, 0));
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: bigint value=%s", paramCount, tmp_curr->svalue);
                                LogMsg(DEBUG, messageStr);
                            }
                            else
                            {
                                Py_INCREF(Py_None);
                                PyTuple_SetItem(outTuple, paramCount, Py_None);
                            }
                            paramCount++;
                            break;
                        case SQL_BLOB:
                            if (!NIL_P(tmp_curr->svalue))
                            {
                                PyTuple_SetItem(outTuple, paramCount,
                                                PyBytes_FromString(tmp_curr->svalue));
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: blob value=%s", paramCount, tmp_curr->svalue);
                                LogMsg(DEBUG, messageStr);
                            }
                            else
                            {
                                Py_INCREF(Py_None);
                                PyTuple_SetItem(outTuple, paramCount, Py_None);
                            }
                            paramCount++;
                            break;

                        default:
                            if (!NIL_P(tmp_curr->svalue))
                            {
                                PyTuple_SetItem(outTuple, paramCount, StringOBJ_FromASCII(tmp_curr->svalue));
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: String value = %s", paramCount, tmp_curr->svalue);
                                LogMsg(DEBUG, messageStr);
                                paramCount++;
                            }
                            else if (!NIL_P(tmp_curr->uvalue))
                            {
                                PyObject *unicode_value = getSQLWCharAsPyUnicodeObject(tmp_curr->uvalue, tmp_curr->bind_indicator);
                                PyTuple_SetItem(outTuple, paramCount, getSQLWCharAsPyUnicodeObject(tmp_curr->uvalue, tmp_curr->bind_indicator));
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: Unicode value = %s", paramCount, PyUnicode_AsUTF8(unicode_value));
                                LogMsg(DEBUG, messageStr);
                                paramCount++;
                            }
                            else
                            {
                                Py_INCREF(Py_None);
                                PyTuple_SetItem(outTuple, paramCount, Py_None);
                                snprintf(messageStr, sizeof(messageStr), "Parameter %d: None (no value)", paramCount);
                                LogMsg(DEBUG, messageStr);
                                paramCount++;
                            }
                            break;
                        }
                    }
                    else
                    {
                        Py_INCREF(Py_None);
                        PyTuple_SetItem(outTuple, paramCount, Py_None);
                        snprintf(messageStr, sizeof(messageStr), "Parameter %d: None (SQL_NULL_DATA or SQL_NO_TOTAL)", paramCount);
                        LogMsg(DEBUG, messageStr);
                        paramCount++;
                    }
                    tmp_curr = tmp_curr->next;
                }
            }
            else
            {
                outTuple = (PyObject *)stmt_res;
            }
        }
        else
        {
            LogMsg(ERROR, "Unexpected NULL stmt_res. Cannot return statement result.");
            LogMsg(INFO, "exit callproc()");
            return NULL;
        }
        LogMsg(DEBUG, "Parameters processed successfully. Returning result tuple");
        LogMsg(INFO, "exit callproc()");
        return outTuple;
    }
    else
    {
        LogMsg(ERROR, "Connection Resource invalid or procedure name is NULL");
        PyErr_SetString(PyExc_Exception, "Connection Resource invalid or procedure name is NULL");
        return NULL;
    }
}

/*
 * ibm_db.check_function_support-- can be used to query whether a  DB2 CLI or ODBC function is supported
 * ===Description
 * int ibm_db.check_function_support(ConnectionHandle, FunctionId)
 * Returns Py_True if a DB2 CLI or ODBC function is supported
 * return Py_False if a DB2 CLI or ODBC function is not supported
 */
static PyObject *ibm_db_check_function_support(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry check_function_support()");
    LogUTF8Msg(args);
    PyObject *py_conn_res = NULL;
    PyObject *py_funtion_id = NULL;
    int funtion_id = 0;
    conn_handle *conn_res = NULL;
    int supported = 0;
    int rc = 0;

    if (!PyArg_ParseTuple(args, "OO", &py_conn_res, &py_funtion_id))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_conn_res=%p, py_funtion_id=%p", py_conn_res, py_funtion_id);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_conn_res))
    {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType))
        {
            LogMsg(ERROR, "Supplied connection object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied connection object Parameter is invalid");
            return NULL;
        }
        else
        {
            conn_res = (conn_handle *)py_conn_res;
            snprintf(messageStr, sizeof(messageStr), "Connection handle is valid. conn_res=%p", conn_res);
            LogMsg(DEBUG, messageStr);
        }
        if (!NIL_P(py_funtion_id))
        {
            if (PyInt_Check(py_funtion_id))
            {
                funtion_id = (int)PyLong_AsLong(py_funtion_id);
                snprintf(messageStr, sizeof(messageStr), "Function ID parsed: funtion_id=%d", funtion_id);
                LogMsg(DEBUG, messageStr);
            }
            else
            {
                LogMsg(ERROR, "Supplied function ID parameter is invalid");
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        /* Check to ensure the connection resource given is active */
        if (!conn_res->handle_active)
        {
            LogMsg(ERROR, "Connection is not active");
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetFunctions(conn_res->hdbc, (SQLUSMALLINT)funtion_id, (SQLUSMALLINT *)&supported);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "Called SQLGetFunctions with parameters: ConnectionHandle=%p, FunctionID=%d, and returned rc=%d, supported=%d",
                 conn_res->hdbc, funtion_id, rc, supported);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            LogMsg(INFO, "exit check_function_support()");
            Py_RETURN_FALSE;
        }
        else
        {
            if (supported == SQL_TRUE)
            {
                LogMsg(INFO, "exit check_function_support()");
                Py_RETURN_TRUE;
            }
            else
            {
                LogMsg(INFO, "exit check_function_support()");
                Py_RETURN_FALSE;
            }
        }
    }
    LogMsg(INFO, "exit check_function_support()");
    return NULL;
}

/*
 * ibm_db.get_last_serial_value --    Gets the last inserted serial value from IDS
 *
 * ===Description
 * string ibm_db.get_last_serial_value ( resource stmt )
 *
 * Returns a string, that is the last inserted value for a serial column for IDS.
 * The last inserted value could be auto-generated or entered explicitly by the user
 * This function is valid for IDS (Informix Dynamic Server only)
 *
 * ===Parameters
 *
 * stmt
 *        A valid statement resource.
 *
 * ===Return Values
 *
 * Returns a string representation of last inserted serial value on a successful call.
 * Returns FALSE on failure.
 */
PyObject *ibm_db_get_last_serial_value(int argc, PyObject *args, PyObject *self)
{
    LogMsg(INFO, "entry get_last_serial_value()");
    LogUTF8Msg(args);
    SQLCHAR *value = NULL;
    SQLINTEGER pcbValue = 0;
    stmt_handle *stmt_res;
    int rc = 0;

    PyObject *py_qualifier = NULL;
    PyObject *retVal = NULL;

    if (!PyArg_ParseTuple(args, "O", &py_qualifier))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed values: py_qualifier=%p", py_qualifier);
    LogMsg(DEBUG, messageStr);
    if (!NIL_P(py_qualifier))
    {
        if (!PyObject_TypeCheck(py_qualifier, &stmt_handleType))
        {
            LogMsg(ERROR, "Supplied statement object parameter is invalid");
            PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
            return NULL;
        }
        else
        {
            stmt_res = (stmt_handle *)py_qualifier;
            snprintf(messageStr, sizeof(messageStr), "Statement handle is valid. stmt_res=%p", stmt_res);
            LogMsg(DEBUG, messageStr);
        }

        /* We allocate a buffer of size 31 as per recommendations from the CLI IDS team */
        value = ALLOC_N(char, 31);
        if (value == NULL)
        {
            LogMsg(ERROR, "Failed to allocate memory for value");
            PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
            return Py_False;
        }
        snprintf(messageStr, sizeof(messageStr), "Calling SQLGetStmtAttr with parameters: hstmt=%p, attribute=%d, buffer_size=%d",
                 stmt_res->hstmt, SQL_ATTR_GET_GENERATED_VALUE, 31);
        LogMsg(DEBUG, messageStr);
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetStmtAttr((SQLHSTMT)stmt_res->hstmt, SQL_ATTR_GET_GENERATED_VALUE, (SQLPOINTER)value, 31, &pcbValue);
        Py_END_ALLOW_THREADS;
        snprintf(messageStr, sizeof(messageStr), "SQLGetStmtAttr returned rc=%d, pcbValue=%d", rc, pcbValue);
        LogMsg(DEBUG, messageStr);
        if (rc == SQL_ERROR)
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            if (value != NULL)
            {
                PyMem_Del(value);
                value = NULL;
            }
            PyErr_Clear();
            return Py_False;
        }
        snprintf(messageStr, sizeof(messageStr), "Retrieved value: %s", (char *)value);
        LogMsg(DEBUG, messageStr);
        retVal = StringOBJ_FromASCII((char *)value);
        if (value != NULL)
        {
            PyMem_Del(value);
            value = NULL;
        }
        LogMsg(INFO, "exit get_last_serial_value()");
        return retVal;
    }
    else
    {
        LogMsg(ERROR, "Supplied statement handle is invalid");
        LogMsg(INFO, "exit get_last_serial_value()");
        PyErr_SetString(PyExc_Exception, "Supplied statement handle is invalid");
        return Py_False;
    }
}

static int _python_get_variable_type(PyObject *variable_value)
{
    LogMsg(INFO, "entry _python_get_variable_type()");
    if (PyBool_Check(variable_value) && (variable_value == Py_True))
    {
        LogMsg(INFO, "variable_value is a Py_True");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_TRUE");
        return PYTHON_TRUE;
    }
    else if (PyBool_Check(variable_value) && (variable_value == Py_False))
    {
        LogMsg(INFO, "variable_value is Py_False");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_FALSE");
        return PYTHON_FALSE;
    }
    else if (PyInt_Check(variable_value) || PyLong_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is an integer");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_FIXNUM");
        return PYTHON_FIXNUM;
    }
    else if (PyFloat_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a float");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_FLOAT");
        return PYTHON_FLOAT;
    }
    else if (PyUnicode_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a Unicode string");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_UNICODE");
        return PYTHON_UNICODE;
    }
    else if (PyString_Check(variable_value) || PyBytes_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a string or bytes");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_STRING");
        return PYTHON_STRING;
    }
    else if (PyDateTime_Check(variable_value))
	{
        LogMsg(INFO, "variable_value is a datetime object");
        PyObject *tzinfo = PyObject_GetAttrString(variable_value, "tzinfo");

#if defined(__MVS__)
        if (tzinfo && tzinfo != Py_None) {
            Py_DECREF(tzinfo);
            LogMsg(INFO, "datetime object has tzinfo on z/OS");
            LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_TIMESTAMP_TSTZ");
            return PYTHON_TIMESTAMP_TSTZ;
        } else {
            Py_XDECREF(tzinfo);
            LogMsg(INFO, "datetime object has no tzinfo on z/OS");
            LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_TIMESTAMP");
            return PYTHON_TIMESTAMP;
        }
#else
        Py_XDECREF(tzinfo);
        LogMsg(INFO, "datetime object on LUW (tzinfo ignored)");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_TIMESTAMP");
        return PYTHON_TIMESTAMP;
#endif
    }
    else if (PyTime_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a time object");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_TIME");
        return PYTHON_TIME;
    }
    else if (PyDate_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a date object");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_DATE");
        return PYTHON_DATE;
    }
    else if (PyComplex_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a complex number");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_COMPLEX");
        return PYTHON_COMPLEX;
    }
    else if (PyNumber_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a number");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_DECIMAL");
        return PYTHON_DECIMAL;
    }
    else if (PyList_Check(variable_value))
    {
        LogMsg(INFO, "variable_value is a list");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_LIST");
        return PYTHON_LIST;
    }
    else if (variable_value == Py_None)
    {
        LogMsg(INFO, "variable_value is Py_None");
        LogMsg(INFO, "exit _python_get_variable_type() with PYTHON_NIL");
        return PYTHON_NIL;
    }
    else
    {
        LogMsg(INFO, "variable_value does not match any known type");
        LogMsg(INFO, "exit _python_get_variable_type() with 0");
        return 0;
    }
}

static PyObject *ibm_db_debug(PyObject *self, PyObject *args)
{
    _python_ibm_db_debug(self, args);
    Py_RETURN_NONE;
}

// Fetch one row from the result set
static PyObject *ibm_db_fetchone(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry fetchone()");
    LogUTF8Msg(args);
    PyObject *return_value = NULL;
    LogMsg(DEBUG, "calling _python_ibm_db_bind_fetch_helper with FETCH_INDEX");
    return_value = _python_ibm_db_bind_fetch_helper(args, FETCH_INDEX);
    snprintf(messageStr, sizeof(messageStr), "Fetched value: %p", return_value);
    LogMsg(DEBUG, messageStr);
    if (return_value == NULL)
    {
        LogMsg(DEBUG, "No more rows, returning None");
        Py_RETURN_NONE;
    }
    if (PyTuple_Check(return_value) || PyList_Check(return_value))
    {
        snprintf(messageStr, sizeof(messageStr), "Valid row fetched: %p", return_value);
        LogMsg(DEBUG, messageStr);
        LogMsg(INFO, "exit fetchone()");
        return return_value;
    }
    snprintf(messageStr, sizeof(messageStr), "Fetched value is not a tuple or list, returning None");
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit fetchone()");
    Py_RETURN_NONE;
}

// Fetch many rows from the result set
static PyObject *ibm_db_fetchmany(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry fetchmany()");
    LogUTF8Msg(args);
    PyObject *return_value = NULL;
    PyObject *result_list = NULL;
    int num_rows = 0;
    PyObject *stmt = NULL;
    if (!PyArg_ParseTuple(args, "Oi", &stmt, &num_rows))
    {
        LogMsg(ERROR, "Failed to parse arguments");
        LogMsg(EXCEPTION, "fetchmany requires a statement handle and an integer argument for the number of rows");
        PyErr_SetString(PyExc_Exception, "fetchmany requires a statement handle and an integer argument for the number of rows");
        return NULL;
    }
    snprintf(messageStr, sizeof(messageStr), "Parsed statement handle: %p, Number of rows to fetch: %d", stmt, num_rows);
    LogMsg(DEBUG, messageStr);
    if (num_rows <= 0)
    {
        LogMsg(ERROR, "Number of rows must be greater than zero");
        PyErr_SetString(PyExc_Exception, "Number of rows must be greater than zero");
        return NULL;
    }
    result_list = PyList_New(0);
    if (result_list == NULL)
    {
        LogMsg(ERROR, "Memory allocation failed for result list");
        return NULL;
    }
    LogMsg(DEBUG, "Initialized result list");
    int fetch_count = 0;
    while (fetch_count < num_rows && (return_value = _python_ibm_db_bind_fetch_helper(args, FETCH_INDEX)) != NULL)
    {
        snprintf(messageStr, sizeof(messageStr), "Fetched row %d: %p", fetch_count + 1, return_value);
        LogMsg(DEBUG, messageStr);
        if (PyTuple_Check(return_value) || PyList_Check(return_value))
        {
            LogMsg(DEBUG, "Valid row fetched, appending to result list");
            if (PyList_Append(result_list, return_value) == -1)
            {
                LogMsg(ERROR, "Failed to append row to result list");
                Py_XDECREF(result_list);
                return NULL;
            }
            Py_XDECREF(return_value);
            fetch_count++;
        }
        else
        {
            LogMsg(DEBUG, "Fetched value is not a valid row, breaking loop");
            Py_XDECREF(return_value);
            break;
        }
    }
    if (PyList_Size(result_list) == 0)
    {
        LogMsg(DEBUG, "No rows fetched, returning empty list");
        LogMsg(INFO, "exit fetchmany()");
        return result_list;
    }
    snprintf(messageStr, sizeof(messageStr), "Returning %zd rows", PyList_Size(result_list));
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit fetchmany()");
    return result_list;
}

// Fetch all rows from the result set
static PyObject *ibm_db_fetchall(PyObject *self, PyObject *args)
{
    LogMsg(INFO, "entry fetchall()");
    LogUTF8Msg(args);
    PyObject *return_value = NULL;
    PyObject *result_list = NULL;
    result_list = PyList_New(0);
    if (result_list == NULL)
    {
        LogMsg(ERROR, "Memory allocation failed for result list");
        return NULL;
    }
    LogMsg(DEBUG, "Initialized result list");
    while ((return_value = _python_ibm_db_bind_fetch_helper(args, FETCH_INDEX)) != NULL)
    {
        snprintf(messageStr, sizeof(messageStr), "Fetched return value: %p", return_value);
        LogMsg(DEBUG, messageStr);
        if (PyTuple_Check(return_value) || PyList_Check(return_value))
        {
            LogMsg(DEBUG, "Valid row fetched, appending to result list");
            if (PyList_Append(result_list, return_value) == -1)
            {
                LogMsg(ERROR, "Failed to append row to result list");
                Py_XDECREF(result_list);
                return NULL;
            }
            Py_XDECREF(return_value);
        }
        else
        {
            LogMsg(DEBUG, "Fetched value is not a valid row, breaking loop");
            Py_XDECREF(return_value);
            break;
        }
    }
    if (PyList_Size(result_list) == 0)
    {
        LogMsg(DEBUG, "No rows fetched, returning empty list");
        LogMsg(INFO, "exit fetchall()");
        return result_list;
    }
    snprintf(messageStr, sizeof(messageStr), "Returning %zd rows", PyList_Size(result_list));
    LogMsg(DEBUG, messageStr);
    LogMsg(INFO, "exit fetchall()");
    return result_list;
}

/* Listing of ibm_db module functions: */
static PyMethodDef ibm_db_Methods[] = {
    /* name, function, argument type, docstring */
    {"connect", (PyCFunction)ibm_db_connect, METH_VARARGS | METH_KEYWORDS, "Connect to the database"},
    {"pconnect", (PyCFunction)ibm_db_pconnect, METH_VARARGS | METH_KEYWORDS, "Returns a persistent connection to a database"},
    {"exec_immediate", (PyCFunction)ibm_db_exec, METH_VARARGS, "Prepares and executes an SQL statement."},
    {"prepare", (PyCFunction)ibm_db_prepare, METH_VARARGS, "Prepares an SQL statement."},
    {"bind_param", (PyCFunction)ibm_db_bind_param, METH_VARARGS, "Binds a Python variable to an SQL statement parameter"},
    {"execute", (PyCFunction)ibm_db_execute, METH_VARARGS, "Executes an SQL statement that was prepared by ibm_db.prepare()"},
    {"fetch_tuple", (PyCFunction)ibm_db_fetch_array, METH_VARARGS, "Returns an tuple, indexed by column position, representing a row in a result set"},
    {"fetch_assoc", (PyCFunction)ibm_db_fetch_assoc, METH_VARARGS, "Returns a dictionary, indexed by column name, representing a row in a result set"},
    {"fetch_both", (PyCFunction)ibm_db_fetch_both, METH_VARARGS, "Returns a dictionary, indexed by both column name and position, representing a row in a result set"},
    {"fetch_row", (PyCFunction)ibm_db_fetch_row, METH_VARARGS, "Sets the result set pointer to the next row or requested row"},
    {"result", (PyCFunction)ibm_db_result, METH_VARARGS, "Returns a single column from a row in the result set"},
    {"active", (PyCFunction)ibm_db_active, METH_VARARGS, "Checks if the specified connection resource is active"},
    {"autocommit", (PyCFunction)ibm_db_autocommit, METH_VARARGS, "Returns or sets the AUTOCOMMIT state for a database connection"},
    {"callproc", (PyCFunction)ibm_db_callproc, METH_VARARGS, "Returns a tuple containing OUT/INOUT variable value"},
    {"check_function_support", (PyCFunction)ibm_db_check_function_support, METH_VARARGS, "return true if fuction is supported otherwise return false"},
    {"close", (PyCFunction)ibm_db_close, METH_VARARGS, "Close a database connection"},
    {"conn_error", (PyCFunction)ibm_db_conn_error, METH_VARARGS, "Returns a string containing the SQLSTATE returned by the last connection attempt"},
    {"conn_errormsg", (PyCFunction)ibm_db_conn_errormsg, METH_VARARGS, "Returns an error message and SQLCODE value representing the reason the last database connection attempt failed"},
    {"conn_warn", (PyCFunction)ibm_db_conn_warn, METH_VARARGS, "Returns a warning string containing the SQLSTATE returned by the last connection attempt"},
    {"client_info", (PyCFunction)ibm_db_client_info, METH_VARARGS, "Returns a read-only object with information about the DB2 database client"},
    {"column_privileges", (PyCFunction)ibm_db_column_privileges, METH_VARARGS, "Returns a result set listing the columns and associated privileges for a table."},
    {"columns", (PyCFunction)ibm_db_columns, METH_VARARGS, "Returns a result set listing the columns and associated metadata for a table"},
    {"commit", (PyCFunction)ibm_db_commit, METH_VARARGS, "Commits a transaction"},
    {"createdb", (PyCFunction)ibm_db_createdb, METH_VARARGS, "Create db"},
    {"createdbNX", (PyCFunction)ibm_db_createdbNX, METH_VARARGS, "createdbNX"},
    {"cursor_type", (PyCFunction)ibm_db_cursor_type, METH_VARARGS, "Returns the cursor type used by a statement resource"},
    {"dropdb", (PyCFunction)ibm_db_dropdb, METH_VARARGS, "Drop db"},
    {"execute_many", (PyCFunction)ibm_db_execute_many, METH_VARARGS, "Execute SQL with multiple rows."},
    {"field_display_size", (PyCFunction)ibm_db_field_display_size, METH_VARARGS, "Returns the maximum number of bytes required to display a column"},
    {"field_name", (PyCFunction)ibm_db_field_name, METH_VARARGS, "Returns the name of the column in the result set"},
    {"field_nullable", (PyCFunction)ibm_db_field_nullable, METH_VARARGS, "Returns indicated column can contain nulls or not"},
    {"field_num", (PyCFunction)ibm_db_field_num, METH_VARARGS, "Returns the position of the named column in a result set"},
    {"field_precision", (PyCFunction)ibm_db_field_precision, METH_VARARGS, "Returns the precision of the indicated column in a result set"},
    {"field_scale", (PyCFunction)ibm_db_field_scale, METH_VARARGS, "Returns the scale of the indicated column in a result set"},
    {"field_type", (PyCFunction)ibm_db_field_type, METH_VARARGS, "Returns the data type of the indicated column in a result set"},
    {"field_width", (PyCFunction)ibm_db_field_width, METH_VARARGS, "Returns the width of the indicated column in a result set"},
    {"foreign_keys", (PyCFunction)ibm_db_foreign_keys, METH_VARARGS, "Returns a result set listing the foreign keys for a table"},
    {"free_result", (PyCFunction)ibm_db_free_result, METH_VARARGS, "Frees resources associated with a result set"},
    {"free_stmt", (PyCFunction)ibm_db_free_stmt, METH_VARARGS, "Frees resources associated with the indicated statement resource"},
    {"get_option", (PyCFunction)ibm_db_get_option, METH_VARARGS, "Gets the specified option in the resource."},
    {"next_result", (PyCFunction)ibm_db_next_result, METH_VARARGS, "Requests the next result set from a stored procedure"},
    {"num_fields", (PyCFunction)ibm_db_num_fields, METH_VARARGS, "Returns the number of fields contained in a result set"},
    {"num_rows", (PyCFunction)ibm_db_num_rows, METH_VARARGS, "Returns the number of rows affected by an SQL statement"},
    {"get_num_result", (PyCFunction)ibm_db_get_num_result, METH_VARARGS, "Returns the number of rows in a current open non-dynamic scrollable cursor"},
    {"primary_keys", (PyCFunction)ibm_db_primary_keys, METH_VARARGS, "Returns a result set listing primary keys for a table"},
    {"procedure_columns", (PyCFunction)ibm_db_procedure_columns, METH_VARARGS, "Returns a result set listing the parameters for one or more stored procedures."},
    {"procedures", (PyCFunction)ibm_db_procedures, METH_VARARGS, "Returns a result set listing the stored procedures registered in a database"},
    {"recreatedb", (PyCFunction)ibm_db_recreatedb, METH_VARARGS, "recreate db"},
    {"rollback", (PyCFunction)ibm_db_rollback, METH_VARARGS, "Rolls back a transaction"},
    {"server_info", (PyCFunction)ibm_db_server_info, METH_VARARGS, "Returns an object with properties that describe the DB2 database server"},
    {"get_db_info", (PyCFunction)ibm_db_get_db_info, METH_VARARGS, "Returns an object with properties that describe the DB2 database server according to the option passed"},
    {"set_option", (PyCFunction)ibm_db_set_option, METH_VARARGS, "Sets the specified option in the resource"},
    {"special_columns", (PyCFunction)ibm_db_special_columns, METH_VARARGS, "Returns a result set listing the unique row identifier columns for a table"},
    {"statistics", (PyCFunction)ibm_db_statistics, METH_VARARGS, "Returns a result set listing the index and statistics for a table"},
    {"stmt_error", (PyCFunction)ibm_db_stmt_error, METH_VARARGS, "Returns a string containing the SQLSTATE returned by an SQL statement"},
    {"stmt_warn", (PyCFunction)ibm_db_stmt_warn, METH_VARARGS, "Returns a warning string containing the SQLSTATE returned by last SQL statement"},
    {"stmt_errormsg", (PyCFunction)ibm_db_stmt_errormsg, METH_VARARGS, "Returns a string containing the last SQL statement error message"},
    {"table_privileges", (PyCFunction)ibm_db_table_privileges, METH_VARARGS, "Returns a result set listing the tables and associated privileges in a database"},
    {"tables", (PyCFunction)ibm_db_tables, METH_VARARGS, "Returns a result set listing the tables and associated metadata in a database"},
    {"get_last_serial_value", (PyCFunction)ibm_db_get_last_serial_value, METH_VARARGS, "Returns last serial value inserted for identity column"},
    {"debug", (PyCFunction)ibm_db_debug, METH_VARARGS | METH_KEYWORDS, "Enable logging with optional log file or disable logging"},
    {"get_sqlcode", (PyCFunction)ibm_db_get_sqlcode, METH_VARARGS, "Returns a string containing the SQLCODE returned by the last connection attempt/ SQL statement"},
    {"fetchone", (PyCFunction)ibm_db_fetchone, METH_VARARGS, "Fetch a single row from the result set."},
    {"fetchall", (PyCFunction)ibm_db_fetchall, METH_VARARGS, "Fetch all rows from the result set."},
    {"fetchmany", (PyCFunction)ibm_db_fetchmany, METH_VARARGS, "Fetch a specified number of rows from the result set."},
    /* An end-of-listing sentinel: */
    {NULL, NULL, 0, NULL}};

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "ibm_db",
    "IBM DataServer Driver for Python.",
    -1,
    ibm_db_Methods,
};
#endif

/* Module initialization function */
PyMODINIT_FUNC
INIT_ibm_db(void)
{
    PyObject *m;

    PyDateTime_IMPORT;
    ibm_db_globals = ALLOC(struct _ibm_db_globals);
    memset(ibm_db_globals, 0, sizeof(struct _ibm_db_globals));
    python_ibm_db_init_globals(ibm_db_globals);

    persistent_list = PyDict_New();
    os_getpid = PyObject_GetAttrString(PyImport_ImportModule("os"), "getpid");

    conn_handleType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&conn_handleType) < 0)
        return MOD_RETURN_ERROR;

    stmt_handleType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&stmt_handleType) < 0)
        return MOD_RETURN_ERROR;

    client_infoType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&client_infoType) < 0)
        return MOD_RETURN_ERROR;

    server_infoType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&server_infoType) < 0)
        return MOD_RETURN_ERROR;

#if PY_MAJOR_VERSION < 3
    m = Py_InitModule3("ibm_db", ibm_db_Methods, "IBM DataServer Driver for Python.");
#else
    m = PyModule_Create(&moduledef);
#endif

    Py_INCREF(&conn_handleType);
    PyModule_AddObject(m, "IBM_DBConnection", (PyObject *)&conn_handleType);

    PyModule_AddIntConstant(m, "SQL_AUTOCOMMIT_ON", SQL_AUTOCOMMIT_ON);
    PyModule_AddIntConstant(m, "SQL_AUTOCOMMIT_OFF", SQL_AUTOCOMMIT_OFF);
    PyModule_AddIntConstant(m, "SQL_ATTR_AUTOCOMMIT", SQL_ATTR_AUTOCOMMIT);
    PyModule_AddIntConstant(m, "ATTR_CASE", ATTR_CASE);
    PyModule_AddIntConstant(m, "CASE_NATURAL", CASE_NATURAL);
    PyModule_AddIntConstant(m, "CASE_LOWER", CASE_LOWER);
    PyModule_AddIntConstant(m, "CASE_UPPER", CASE_UPPER);
    PyModule_AddIntConstant(m, "USE_WCHAR", USE_WCHAR);
    PyModule_AddIntConstant(m, "WCHAR_YES", WCHAR_YES);
    PyModule_AddIntConstant(m, "WCHAR_NO", WCHAR_NO);
    PyModule_AddIntConstant(m, "SQL_ATTR_CURSOR_TYPE", SQL_ATTR_CURSOR_TYPE);
    PyModule_AddIntConstant(m, "SQL_CURSOR_FORWARD_ONLY", SQL_CURSOR_FORWARD_ONLY);
    PyModule_AddIntConstant(m, "SQL_CURSOR_KEYSET_DRIVEN", SQL_CURSOR_KEYSET_DRIVEN);
    PyModule_AddIntConstant(m, "SQL_CURSOR_DYNAMIC", SQL_CURSOR_DYNAMIC);
    PyModule_AddIntConstant(m, "SQL_CURSOR_STATIC", SQL_CURSOR_STATIC);
    PyModule_AddIntConstant(m, "SQL_PARAM_INPUT", SQL_PARAM_INPUT);
    PyModule_AddIntConstant(m, "SQL_PARAM_OUTPUT", SQL_PARAM_OUTPUT);
    PyModule_AddIntConstant(m, "SQL_PARAM_INPUT_OUTPUT", SQL_PARAM_INPUT_OUTPUT);
    PyModule_AddIntConstant(m, "PARAM_FILE", PARAM_FILE);

    PyModule_AddIntConstant(m, "SQL_BIGINT", SQL_BIGINT);
    PyModule_AddIntConstant(m, "SQL_BINARY", SQL_BINARY);
    PyModule_AddIntConstant(m, "SQL_BLOB", SQL_BLOB);
    PyModule_AddIntConstant(m, "SQL_BLOB_LOCATOR", SQL_BLOB_LOCATOR);
#ifndef __MVS__
    PyModule_AddIntConstant(m, "SQL_BOOLEAN", SQL_BOOLEAN);
#endif
    PyModule_AddIntConstant(m, "SQL_CHAR", SQL_CHAR);
    PyModule_AddIntConstant(m, "SQL_TINYINT", SQL_TINYINT);
    PyModule_AddIntConstant(m, "SQL_BINARY", SQL_BINARY);
    PyModule_AddIntConstant(m, "SQL_BIT", SQL_BIT);
    PyModule_AddIntConstant(m, "SQL_CLOB", SQL_CLOB);
    PyModule_AddIntConstant(m, "SQL_CLOB_LOCATOR", SQL_CLOB_LOCATOR);
    PyModule_AddIntConstant(m, "SQL_TYPE_DATE", SQL_TYPE_DATE);
    PyModule_AddIntConstant(m, "SQL_DBCLOB", SQL_DBCLOB);
    PyModule_AddIntConstant(m, "SQL_DBCLOB_LOCATOR", SQL_DBCLOB_LOCATOR);
    PyModule_AddIntConstant(m, "SQL_DECIMAL", SQL_DECIMAL);
    PyModule_AddIntConstant(m, "SQL_DECFLOAT", SQL_DECFLOAT);
    PyModule_AddIntConstant(m, "SQL_DOUBLE", SQL_DOUBLE);
    PyModule_AddIntConstant(m, "SQL_FLOAT", SQL_FLOAT);
    PyModule_AddIntConstant(m, "SQL_GRAPHIC", SQL_GRAPHIC);
    PyModule_AddIntConstant(m, "SQL_INTEGER", SQL_INTEGER);
    PyModule_AddIntConstant(m, "SQL_LONGVARCHAR", SQL_LONGVARCHAR);
    PyModule_AddIntConstant(m, "SQL_LONGVARBINARY", SQL_LONGVARBINARY);
    PyModule_AddIntConstant(m, "SQL_LONGVARGRAPHIC", SQL_LONGVARGRAPHIC);
    PyModule_AddIntConstant(m, "SQL_WLONGVARCHAR", SQL_WLONGVARCHAR);
    PyModule_AddIntConstant(m, "SQL_NUMERIC", SQL_NUMERIC);
    PyModule_AddIntConstant(m, "SQL_REAL", SQL_REAL);
    PyModule_AddIntConstant(m, "SQL_SMALLINT", SQL_SMALLINT);
    PyModule_AddIntConstant(m, "SQL_TYPE_TIME", SQL_TYPE_TIME);
    PyModule_AddIntConstant(m, "SQL_TYPE_TIMESTAMP", SQL_TYPE_TIMESTAMP);
    PyModule_AddIntConstant(m, "SQL_VARBINARY", SQL_VARBINARY);
    PyModule_AddIntConstant(m, "SQL_VARCHAR", SQL_VARCHAR);
    PyModule_AddIntConstant(m, "SQL_VARBINARY", SQL_VARBINARY);
    PyModule_AddIntConstant(m, "SQL_VARGRAPHIC", SQL_VARGRAPHIC);
    PyModule_AddIntConstant(m, "SQL_WVARCHAR", SQL_WVARCHAR);
    PyModule_AddIntConstant(m, "SQL_WCHAR", SQL_WCHAR);
    PyModule_AddIntConstant(m, "SQL_XML", SQL_XML);
    PyModule_AddIntConstant(m, "SQL_FALSE", SQL_FALSE);
    PyModule_AddIntConstant(m, "SQL_TRUE", SQL_TRUE);
    PyModule_AddIntConstant(m, "SQL_TABLE_STAT", SQL_TABLE_STAT);
    PyModule_AddIntConstant(m, "SQL_INDEX_CLUSTERED", SQL_INDEX_CLUSTERED);
    PyModule_AddIntConstant(m, "SQL_INDEX_OTHER", SQL_INDEX_OTHER);
    PyModule_AddIntConstant(m, "SQL_ATTR_CURRENT_SCHEMA", SQL_ATTR_CURRENT_SCHEMA);
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_USERID", SQL_ATTR_INFO_USERID);
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_WRKSTNNAME", SQL_ATTR_INFO_WRKSTNNAME);
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_ACCTSTR", SQL_ATTR_INFO_ACCTSTR);
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_APPLNAME", SQL_ATTR_INFO_APPLNAME);
    PyModule_AddIntConstant(m, "SQL_ATTR_USE_TRUSTED_CONTEXT", SQL_ATTR_USE_TRUSTED_CONTEXT);
    PyModule_AddIntConstant(m, "SQL_ATTR_TRUSTED_CONTEXT_USERID", SQL_ATTR_TRUSTED_CONTEXT_USERID);
    PyModule_AddIntConstant(m, "SQL_ATTR_TRUSTED_CONTEXT_PASSWORD", SQL_ATTR_TRUSTED_CONTEXT_PASSWORD);
    PyModule_AddIntConstant(m, "SQL_DBMS_NAME", SQL_DBMS_NAME);
    PyModule_AddIntConstant(m, "SQL_DBMS_VER", SQL_DBMS_VER);
    PyModule_AddIntConstant(m, "SQL_ATTR_ROWCOUNT_PREFETCH", SQL_ATTR_ROWCOUNT_PREFETCH);
    PyModule_AddIntConstant(m, "SQL_ROWCOUNT_PREFETCH_ON", SQL_ROWCOUNT_PREFETCH_ON);
    PyModule_AddIntConstant(m, "SQL_ROWCOUNT_PREFETCH_OFF", SQL_ROWCOUNT_PREFETCH_OFF);
    PyModule_AddIntConstant(m, "SQL_API_SQLROWCOUNT", SQL_API_SQLROWCOUNT);
    PyModule_AddIntConstant(m, "QUOTED_LITERAL_REPLACEMENT_ON", SET_QUOTED_LITERAL_REPLACEMENT_ON);
    PyModule_AddIntConstant(m, "QUOTED_LITERAL_REPLACEMENT_OFF", SET_QUOTED_LITERAL_REPLACEMENT_OFF);
#ifndef __MVS__
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_PROGRAMNAME", SQL_ATTR_INFO_PROGRAMNAME);
#endif
    PyModule_AddStringConstant(m, "__version__", MODULE_RELEASE);

    Py_INCREF(&stmt_handleType);
    PyModule_AddObject(m, "IBM_DBStatement", (PyObject *)&stmt_handleType);

    Py_INCREF(&client_infoType);
    PyModule_AddObject(m, "IBM_DBClientInfo", (PyObject *)&client_infoType);

    Py_INCREF(&server_infoType);
    PyModule_AddObject(m, "IBM_DBServerInfo", (PyObject *)&server_infoType);
    PyModule_AddIntConstant(m, "SQL_ATTR_QUERY_TIMEOUT", SQL_ATTR_QUERY_TIMEOUT);
    PyModule_AddIntConstant(m, "SQL_ATTR_PARAMSET_SIZE", SQL_ATTR_PARAMSET_SIZE);
    PyModule_AddIntConstant(m, "SQL_ATTR_PARAM_BIND_TYPE", SQL_ATTR_PARAM_BIND_TYPE);
    PyModule_AddIntConstant(m, "SQL_PARAM_BIND_BY_COLUMN", SQL_PARAM_BIND_BY_COLUMN);
    PyModule_AddIntConstant(m, "SQL_ATTR_XML_DECLARATION", SQL_ATTR_XML_DECLARATION);
#ifndef __MVS__
    PyModule_AddIntConstant(m, "SQL_ATTR_CLIENT_APPLCOMPAT", SQL_ATTR_CLIENT_APPLCOMPAT);
    PyModule_AddIntConstant(m, "SQL_ATTR_CURRENT_PACKAGE_SET", SQL_ATTR_CURRENT_PACKAGE_SET);
    PyModule_AddIntConstant(m, "SQL_ATTR_ACCESS_MODE", SQL_ATTR_ACCESS_MODE);
    PyModule_AddIntConstant(m, "SQL_ATTR_ALLOW_INTERLEAVED_GETDATA", SQL_ATTR_ALLOW_INTERLEAVED_GETDATA);
    PyModule_AddIntConstant(m, "SQL_ATTR_ANSI_APP", SQL_ATTR_ANSI_APP);
    PyModule_AddIntConstant(m, "SQL_ATTR_APP_USES_LOB_LOCATOR", SQL_ATTR_APP_USES_LOB_LOCATOR);
    PyModule_AddIntConstant(m, "SQL_ATTR_APPEND_FOR_FETCH_ONLY", SQL_ATTR_APPEND_FOR_FETCH_ONLY);
    PyModule_AddIntConstant(m, "SQL_ATTR_ASYNC_ENABLE", SQL_ATTR_ASYNC_ENABLE);
    PyModule_AddIntConstant(m, "SQL_ATTR_AUTO_IPD", SQL_ATTR_AUTO_IPD);
    PyModule_AddIntConstant(m, "SQL_ATTR_CACHE_USRLIBL", SQL_ATTR_CACHE_USRLIBL);
    PyModule_AddIntConstant(m, "SQL_ATTR_CLIENT_CODEPAGE", SQL_ATTR_CLIENT_CODEPAGE);
    PyModule_AddIntConstant(m, "SQL_ATTR_COLUMNWISE_MRI", SQL_ATTR_COLUMNWISE_MRI);
    PyModule_AddIntConstant(m, "SQL_ATTR_COMMITONEOF", SQL_ATTR_COMMITONEOF);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONCURRENT_ACCESS_RESOLUTION", SQL_ATTR_CONCURRENT_ACCESS_RESOLUTION);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONFIG_KEYWORDS_ARRAY_SIZE", SQL_ATTR_CONFIG_KEYWORDS_ARRAY_SIZE);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONFIG_KEYWORDS_MAXLEN", SQL_ATTR_CONFIG_KEYWORDS_MAXLEN);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONN_CONTEXT", SQL_ATTR_CONN_CONTEXT);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONNECT_NODE", SQL_ATTR_CONNECT_NODE);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONNECT_PASSIVE", SQL_ATTR_CONNECT_PASSIVE);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONNECTION_DEAD", SQL_ATTR_CONNECTION_DEAD);
    PyModule_AddIntConstant(m, "SQL_ATTR_CONNECTTYPE", SQL_ATTR_CONNECTTYPE);
    PyModule_AddIntConstant(m, "SQL_ATTR_CURRENT_CATALOG", SQL_ATTR_CURRENT_CATALOG);
    PyModule_AddIntConstant(m, "SQL_ATTR_CURRENT_IMPLICIT_XMLPARSE_OPTION", SQL_ATTR_CURRENT_IMPLICIT_XMLPARSE_OPTION);
    PyModule_AddIntConstant(m, "SQL_ATTR_CURRENT_PACKAGE_PATH", SQL_ATTR_CURRENT_PACKAGE_PATH);
    PyModule_AddIntConstant(m, "SQL_ATTR_DATE_FMT", SQL_ATTR_DATE_FMT);
    PyModule_AddIntConstant(m, "SQL_ATTR_DATE_SEP", SQL_ATTR_DATE_SEP);
    PyModule_AddIntConstant(m, "SQL_ATTR_DB2_APPLICATION_HANDLE", SQL_ATTR_DB2_APPLICATION_HANDLE);
    PyModule_AddIntConstant(m, "SQL_ATTR_DB2_APPLICATION_ID", SQL_ATTR_DB2_APPLICATION_ID);
    PyModule_AddIntConstant(m, "SQL_ATTR_DB2_SQLERRP", SQL_ATTR_DB2_SQLERRP);
    PyModule_AddIntConstant(m, "SQL_ATTR_DB2EXPLAIN", SQL_ATTR_DB2EXPLAIN);
    PyModule_AddIntConstant(m, "SQL_ATTR_DECIMAL_SEP", SQL_ATTR_DECIMAL_SEP);
    PyModule_AddIntConstant(m, "SQL_ATTR_DESCRIBE_CALL", SQL_ATTR_DESCRIBE_CALL);
    PyModule_AddIntConstant(m, "SQL_ATTR_DESCRIBE_OUTPUT_LEVEL", SQL_ATTR_DESCRIBE_OUTPUT_LEVEL);
    PyModule_AddIntConstant(m, "SQL_ATTR_DETECT_READ_ONLY_TXN", SQL_ATTR_DETECT_READ_ONLY_TXN);
    PyModule_AddIntConstant(m, "SQL_ATTR_ENLIST_IN_DTC", SQL_ATTR_ENLIST_IN_DTC);
    PyModule_AddIntConstant(m, "SQL_ATTR_EXTENDED_INDICATORS", SQL_ATTR_EXTENDED_INDICATORS);
    PyModule_AddIntConstant(m, "SQL_ATTR_FET_BUF_SIZE", SQL_ATTR_FET_BUF_SIZE);
    PyModule_AddIntConstant(m, "SQL_ATTR_FORCE_ROLLBACK", SQL_ATTR_FORCE_ROLLBACK);
    PyModule_AddIntConstant(m, "SQL_ATTR_FREE_LOCATORS_ON_FETCH", SQL_ATTR_FREE_LOCATORS_ON_FETCH);
    PyModule_AddIntConstant(m, "SQL_ATTR_GET_LATEST_MEMBER", SQL_ATTR_GET_LATEST_MEMBER);
    PyModule_AddIntConstant(m, "SQL_ATTR_GET_LATEST_MEMBER_NAME", SQL_ATTR_GET_LATEST_MEMBER_NAME);
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_ACCTSTR", SQL_ATTR_INFO_ACCTSTR);
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_PROGRAMID", SQL_ATTR_INFO_PROGRAMID);
    PyModule_AddIntConstant(m, "SQL_ATTR_INFO_CRRTKN", SQL_ATTR_INFO_CRRTKN);
    PyModule_AddIntConstant(m, "SQL_ATTR_KEEP_DYNAMIC", SQL_ATTR_KEEP_DYNAMIC);
    PyModule_AddIntConstant(m, "SQL_ATTR_LOB_CACHE_SIZE", SQL_ATTR_LOB_CACHE_SIZE);
    PyModule_AddIntConstant(m, "SQL_ATTR_LOB_FILE_THRESHOLD", SQL_ATTR_LOB_FILE_THRESHOLD);
    PyModule_AddIntConstant(m, "SQL_ATTR_LOGIN_TIMEOUT", SQL_ATTR_LOGIN_TIMEOUT);
    PyModule_AddIntConstant(m, "SQL_ATTR_LONGDATA_COMPAT", SQL_ATTR_LONGDATA_COMPAT);
    PyModule_AddIntConstant(m, "SQL_ATTR_MAPCHAR", SQL_ATTR_MAPCHAR);
    PyModule_AddIntConstant(m, "SQL_ATTR_MAXBLKEXT", SQL_ATTR_MAXBLKEXT);
    PyModule_AddIntConstant(m, "SQL_ATTR_MAX_LOB_BLOCK_SIZE", SQL_ATTR_MAX_LOB_BLOCK_SIZE);
    PyModule_AddIntConstant(m, "SQL_ATTR_NETWORK_STATISTICS", SQL_ATTR_NETWORK_STATISTICS);
    PyModule_AddIntConstant(m, "SQL_ATTR_OVERRIDE_CHARACTER_CODEPAGE", SQL_ATTR_OVERRIDE_CHARACTER_CODEPAGE);
    PyModule_AddIntConstant(m, "SQL_ATTR_OVERRIDE_CODEPAGE", SQL_ATTR_OVERRIDE_CODEPAGE);
    PyModule_AddIntConstant(m, "SQL_ATTR_CALL_RETURN", SQL_ATTR_CALL_RETURN);
    PyModule_AddIntConstant(m, "SQL_ATTR_OVERRIDE_PRIMARY_AFFINITY", SQL_ATTR_OVERRIDE_PRIMARY_AFFINITY);
    PyModule_AddIntConstant(m, "SQL_ATTR_PARC_BATCH", SQL_ATTR_PARC_BATCH);
    PyModule_AddIntConstant(m, "SQL_ATTR_PING_NTIMES", SQL_ATTR_PING_NTIMES);
    PyModule_AddIntConstant(m, "SQL_ATTR_PING_REQUEST_PACKET_SIZE", SQL_ATTR_PING_REQUEST_PACKET_SIZE);
    PyModule_AddIntConstant(m, "SQL_ATTR_QUERY_PREFETCH", SQL_ATTR_QUERY_PREFETCH);
    PyModule_AddIntConstant(m, "SQL_ATTR_QUIET_MODE", SQL_ATTR_QUIET_MODE);
    PyModule_AddIntConstant(m, "SQL_ATTR_READ_ONLY_CONNECTION", SQL_ATTR_READ_ONLY_CONNECTION);
    PyModule_AddIntConstant(m, "SQL_ATTR_RECEIVE_TIMEOUT", SQL_ATTR_RECEIVE_TIMEOUT);
    PyModule_AddIntConstant(m, "SQL_ATTR_REOPT", SQL_ATTR_REOPT);
    PyModule_AddIntConstant(m, "SQL_ATTR_REPORT_ISLONG_FOR_LONGTYPES_OLEDB", SQL_ATTR_REPORT_ISLONG_FOR_LONGTYPES_OLEDB);
    PyModule_AddIntConstant(m, "SQL_ATTR_REPORT_SEAMLESSFAILOVER_WARNING", SQL_ATTR_REPORT_SEAMLESSFAILOVER_WARNING);
    PyModule_AddIntConstant(m, "SQL_ATTR_REPORT_TIMESTAMP_TRUNC_AS_WARN", SQL_ATTR_REPORT_TIMESTAMP_TRUNC_AS_WARN);
    PyModule_AddIntConstant(m, "SQL_ATTR_RETRY_ON_MERGE", SQL_ATTR_RETRY_ON_MERGE);
    PyModule_AddIntConstant(m, "SQL_ATTR_RETRYONERROR", SQL_ATTR_RETRYONERROR);
    PyModule_AddIntConstant(m, "SQL_ATTR_SERVER_MSGTXT_MASK", SQL_ATTR_SERVER_MSGTXT_MASK);
    PyModule_AddIntConstant(m, "SQL_ATTR_SERVER_MSGTXT_SP", SQL_ATTR_SERVER_MSGTXT_SP);
    PyModule_AddIntConstant(m, "SQL_ATTR_SESSION_GLOBAL_VAR", SQL_ATTR_SESSION_GLOBAL_VAR);
    PyModule_AddIntConstant(m, "SQL_ATTR_SESSION_TIME_ZONE", SQL_ATTR_SESSION_TIME_ZONE);
    PyModule_AddIntConstant(m, "SQL_ATTR_SPECIAL_REGISTER", SQL_ATTR_SPECIAL_REGISTER);
    PyModule_AddIntConstant(m, "SQL_ATTR_SQLCOLUMNS_SORT_BY_ORDINAL_OLEDB", SQL_ATTR_SQLCOLUMNS_SORT_BY_ORDINAL_OLEDB);
    PyModule_AddIntConstant(m, "SQL_ATTR_STMT_CONCENTRATOR", SQL_ATTR_STMT_CONCENTRATOR);
    PyModule_AddIntConstant(m, "SQL_ATTR_STREAM_GETDATA", SQL_ATTR_STREAM_GETDATA);
    PyModule_AddIntConstant(m, "SQL_ATTR_STREAM_OUTPUTLOB_ON_CALL", SQL_ATTR_STREAM_OUTPUTLOB_ON_CALL);
    PyModule_AddIntConstant(m, "SQL_ATTR_TIME_FMT", SQL_ATTR_TIME_FMT);
    PyModule_AddIntConstant(m, "SQL_ATTR_TIME_SEP", SQL_ATTR_TIME_SEP);
    PyModule_AddIntConstant(m, "SQL_ATTR_TRUSTED_CONTEXT_ACCESSTOKEN", SQL_ATTR_TRUSTED_CONTEXT_ACCESSTOKEN);
    PyModule_AddIntConstant(m, "SQL_ATTR_USER_REGISTRY_NAME", SQL_ATTR_USER_REGISTRY_NAME);
    PyModule_AddIntConstant(m, "SQL_ATTR_WCHARTYPE", SQL_ATTR_WCHARTYPE);
    PyModule_AddIntConstant(m, "SQL_ATTR_IGNORE_SERVER_LIST", SQL_ATTR_IGNORE_SERVER_LIST);
    PyModule_AddIntConstant(m, "SQL_ATTR_DECFLOAT_ROUNDING_MODE", SQL_ATTR_DECFLOAT_ROUNDING_MODE);
    PyModule_AddIntConstant(m, "SQL_ATTR_PING_DB", SQL_ATTR_PING_DB);
#endif
    PyModule_AddIntConstant(m, "SQL_ATTR_TXN_ISOLATION", SQL_ATTR_TXN_ISOLATION);
    PyModule_AddIntConstant(m, "SQL_TXN_READ_UNCOMMITTED", SQL_TXN_READ_UNCOMMITTED);
    PyModule_AddIntConstant(m, "SQL_TXN_READ_COMMITTED", SQL_TXN_READ_COMMITTED);
    PyModule_AddIntConstant(m, "SQL_TXN_REPEATABLE_READ", SQL_TXN_REPEATABLE_READ);
    PyModule_AddIntConstant(m, "SQL_TXN_SERIALIZABLE", SQL_TXN_SERIALIZABLE);
    PyModule_AddIntConstant(m, "SQL_TXN_NO_COMMIT", SQL_TXN_NOCOMMIT);
    return MOD_RETURN_VAL(m);
}
