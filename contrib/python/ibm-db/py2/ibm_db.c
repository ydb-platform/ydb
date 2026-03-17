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

#define MODULE_RELEASE "3.1.4"

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

static void _python_ibm_db_check_sql_errors( SQLHANDLE handle, SQLSMALLINT hType, int rc, int cpy_to_global, char* ret_str, int API, SQLSMALLINT recno );
static int _python_ibm_db_assign_options( void* handle, int type, long opt_key, PyObject *data );
static SQLWCHAR* getUnicodeDataAsSQLWCHAR(PyObject *pyobj, int *isNewBuffer);
static SQLCHAR* getUnicodeDataAsSQLCHAR(PyObject *pyobj, int *isNewBuffer);
static PyObject* getSQLWCharAsPyUnicodeObject(SQLWCHAR* sqlwcharData, int sqlwcharBytesLen);

const int _check_i = 1;
#define is_bigendian() ( (*(char*)&_check_i) == 0 )
static int is_systemi, is_informix;      /* 1 == TRUE; 0 == FALSE; */
#ifdef _WIN32
#define DLOPEN LoadLibrary
#define DLSYM GetProcAddress
#define DLCLOSE FreeLibrary
#define LIBDB2 "db2cli64.dll"
#elif _AIX
#define DLOPEN dlopen
#define DLSYM dlsym
#define DLCLOSE dlclose
#define LIBDB2 "libdb2.a"
#else
#define DLOPEN dlopen
#define DLSYM dlsym
#define DLCLOSE dlclose
#define LIBDB2 "libdb2.so.1"
#endif

/* Defines a linked list structure for error messages */
typedef struct _error_msg_node {
    char err_msg[DB2_MAX_ERR_MSG_LEN];
    struct _error_msg_node *next;
} error_msg_node;

/* Defines a linked list structure for caching param data */
typedef struct _param_cache_node {
    SQLSMALLINT data_type;           /* Datatype */
    SQLUINTEGER param_size;          /* param size */
    SQLSMALLINT nullable;            /* is Nullable */
    SQLSMALLINT scale;               /* Decimal scale */
    SQLUINTEGER file_options;        /* File options if PARAM_FILE */
    SQLINTEGER  bind_indicator;      /* indicator variable for SQLBindParameter */
    int         param_num;           /* param number in stmt */
    int         param_type;          /* Type of param - INP/OUT/INP-OUT/FILE */
    int         size;                /* Size of param */
    char        *varname;            /* bound variable name */
    PyObject    *var_pyvalue;        /* bound variable value */
    SQLINTEGER  ivalue;              /* Temp storage value */
    double      fvalue;              /* Temp storage value */
    char        *svalue;             /* Temp storage value */
    SQLWCHAR    *uvalue;             /* Temp storage value */
    DATE_STRUCT *date_value;         /* Temp storage value */
    TIME_STRUCT *time_value;         /* Temp storage value */
    TIMESTAMP_STRUCT *ts_value;      /* Temp storage value */
    SQLINTEGER  *ivalueArray;        /* Temp storage array of values */
    double      *fvalueArray;        /* Temp storage array of values */
    SQLINTEGER  *bind_indicator_array; /* Temp storage array of values */
    struct _param_cache_node *next;  /* Pointer to next node */
} param_node;

typedef struct _conn_handle_struct {
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



typedef union {
    SQLINTEGER i_val;
    SQLDOUBLE d_val;
    SQLFLOAT f_val;
    SQLSMALLINT s_val;
    SQLCHAR *str_val;
    SQLREAL r_val;
    SQLWCHAR *w_val;
    TIMESTAMP_STRUCT *ts_val;
    DATE_STRUCT *date_val;
    TIME_STRUCT *time_val;
} ibm_db_row_data_type;


typedef struct {
    SQLINTEGER out_length;
    ibm_db_row_data_type data;
} ibm_db_row_type;

typedef struct _ibm_db_result_set_info_struct {
    SQLCHAR     *name;
    SQLSMALLINT type;
    SQLUINTEGER size;
    SQLSMALLINT scale;
    SQLSMALLINT nullable;
    unsigned char *mem_alloc;  /* Mem free */
} ibm_db_result_set_info;

typedef struct _row_hash_struct {
    PyObject *hash;
} row_hash_struct;

typedef struct _stmt_handle_struct {
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

static void python_ibm_db_init_globals(struct _ibm_db_globals *ibm_db_globals) {
    /* env handle */
    ibm_db_globals->bin_mode = 1;

    memset(ibm_db_globals->__python_conn_err_msg, 0, DB2_MAX_ERR_MSG_LEN);
    memset(ibm_db_globals->__python_stmt_err_msg, 0, DB2_MAX_ERR_MSG_LEN);
    memset(ibm_db_globals->__python_conn_err_state, 0, SQL_SQLSTATE_SIZE + 1);
    memset(ibm_db_globals->__python_stmt_err_state, 0, SQL_SQLSTATE_SIZE + 1);
}

static PyObject *persistent_list;
static PyObject *os_getpid;

char *estrdup(char *data) {
    int len = strlen(data);
    char *dup = ALLOC_N(char, len+1);
    if ( dup == NULL ) {
        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
        return NULL;
    }
    strcpy(dup, data);
    return dup;
}

char *estrndup(char *data, int max) {
    int len = strlen(data);
    char *dup;
    if (len > max){
        len = max;
    }
    dup = ALLOC_N(char, len+1);
    if ( dup == NULL ) {
        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
        return NULL;
    }
    strcpy(dup, data);
    return dup;
}

char *strtolower(char *data, int max) {
    while (max--){
        data[max] = tolower(data[max]);
    }
    return data;
}

char *strtoupper(char *data, int max) {
    while (max--){
        data[max] = toupper(data[max]);
    }
    return data;
}

/*    static void _python_ibm_db_free_conn_struct */
static void _python_ibm_db_free_conn_struct(conn_handle *handle) {

    /* Disconnect from DB. If stmt is allocated, it is freed automatically */
    if ( handle->handle_active && !handle->flag_pconnect) {
        if(handle->auto_commit == 0){
            Py_BEGIN_ALLOW_THREADS;
            SQLEndTran(SQL_HANDLE_DBC, (SQLHDBC)handle->hdbc, SQL_ROLLBACK);
            Py_END_ALLOW_THREADS;
        }
        Py_BEGIN_ALLOW_THREADS;
        SQLDisconnect((SQLHDBC)handle->hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, handle->hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, handle->henv);
        Py_END_ALLOW_THREADS;
    }
    Py_TYPE(handle)->tp_free((PyObject*)handle);
}

/*    static void _python_ibm_db_free_row_struct */
/*
 * static void _python_ibm_db_free_row_struct(row_hash_struct *handle) {
 *  free(handle);
 * }
 */

static void _python_ibm_db_clear_param_cache( stmt_handle *stmt_res )
{
    param_node *temp_ptr, *curr_ptr;

    /* Free param cache list */
    curr_ptr = stmt_res->head_cache_list;

    while (curr_ptr != NULL) {
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
        PyMem_Free(curr_ptr->ivalueArray);
        PyMem_Free(curr_ptr->fvalueArray);
        PyMem_Free(curr_ptr->bind_indicator_array);

        temp_ptr = curr_ptr;
        curr_ptr = curr_ptr->next;

        PyMem_Free(temp_ptr);
    }

    stmt_res->head_cache_list = NULL;
    stmt_res->num_params = 0;
}

/*    static void _python_ibm_db_free_result_struct(stmt_handle* handle) */
static void _python_ibm_db_free_result_struct(stmt_handle* handle) {
    int i;
    param_node *curr_ptr = NULL, *prev_ptr = NULL;

    if ( handle != NULL ) {
        _python_ibm_db_clear_param_cache(handle);

        /* free row data cache */
        if (handle->row_data) {
            for (i = 0; i<handle->num_columns; i++) {
                switch (handle->column_info[i].type) {
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
                        if ( handle->row_data[i].data.str_val != NULL ) {
                            PyMem_Del(handle->row_data[i].data.str_val);
                            handle->row_data[i].data.str_val = NULL;
                        }
                        if ( handle->row_data[i].data.w_val != NULL ) {
                            PyMem_Del(handle->row_data[i].data.w_val);
                            handle->row_data[i].data.w_val = NULL;
                        }
                        break;
                    case SQL_TYPE_TIMESTAMP:
                        if ( handle->row_data[i].data.ts_val != NULL ) {
                            PyMem_Del(handle->row_data[i].data.ts_val);
                            handle->row_data[i].data.ts_val = NULL;
                        }
                        break;
                    case SQL_TYPE_DATE:
                        if ( handle->row_data[i].data.date_val != NULL ) {
                            PyMem_Del(handle->row_data[i].data.date_val);
                            handle->row_data[i].data.date_val = NULL;
                        }
                        break;
                    case SQL_TYPE_TIME:
                        if ( handle->row_data[i].data.time_val != NULL ) {
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
        if ( handle->column_info ) {
            for (i = 0; i<handle->num_columns; i++) {
                PyMem_Del(handle->column_info[i].name);
                /* Mem free */
                if(handle->column_info[i].mem_alloc){
                    PyMem_Del(handle->column_info[i].mem_alloc);
                }
            }
            PyMem_Del(handle->column_info);
            handle->column_info = NULL;
            handle->num_columns = 0;
        }
    }
}

/* static stmt_handle *_ibm_db_new_stmt_struct(conn_handle* conn_res) */
static stmt_handle *_ibm_db_new_stmt_struct(conn_handle* conn_res) {
    stmt_handle *stmt_res;

    stmt_res = PyObject_NEW(stmt_handle, &stmt_handleType);
    /* memset(stmt_res, 0, sizeof(stmt_handle)); */

    /* Initialize stmt resource so parsing assigns updated options if needed */
    stmt_res->hdbc = conn_res->hdbc;
    stmt_res->s_bin_mode = conn_res->c_bin_mode;
    stmt_res->cursor_type = conn_res->c_cursor_type;
    stmt_res->s_case_mode = conn_res->c_case_mode;
    stmt_res->s_use_wchar = conn_res->c_use_wchar;

    stmt_res->head_cache_list = NULL;
    stmt_res->current_node = NULL;

    stmt_res->num_params = 0;
    stmt_res->file_param = 0;

    stmt_res->column_info = NULL;
    stmt_res->num_columns = 0;

    stmt_res->error_recno_tracker = 1;
    stmt_res->errormsg_recno_tracker = 1;

    stmt_res->row_data = NULL;

    return stmt_res;
}

/*    static _python_ibm_db_free_stmt_struct */
static void _python_ibm_db_free_stmt_struct(stmt_handle *handle) {
    if ( handle->hstmt != -1 ) {
        Py_BEGIN_ALLOW_THREADS;
        SQLFreeHandle( SQL_HANDLE_STMT, handle->hstmt);
        Py_END_ALLOW_THREADS;
        if ( handle ) {
            _python_ibm_db_free_result_struct(handle);
        }
    }
    Py_TYPE(handle)->tp_free((PyObject*)handle);
}

/*    static void _python_ibm_db_init_error_info(stmt_handle *stmt_res) */
static void _python_ibm_db_init_error_info(stmt_handle *stmt_res) {
    stmt_res->error_recno_tracker = 1;
    stmt_res->errormsg_recno_tracker = 1;
}

/*    static void _python_ibm_db_check_sql_errors( SQLHANDLE handle, SQLSMALLINT hType, int rc, int cpy_to_global, char* ret_str, int API SQLSMALLINT recno)
*/
static void _python_ibm_db_check_sql_errors( SQLHANDLE handle, SQLSMALLINT hType, int rc, int cpy_to_global, char* ret_str, int API, SQLSMALLINT recno )
{
    SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH + 1] = {0};
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1] = {0};
    SQLCHAR errMsg[DB2_MAX_ERR_MSG_LEN] = {0};
    SQLINTEGER sqlcode = 0;
    SQLSMALLINT length = 0;
    char *p= NULL;
    SQLINTEGER rc1 = SQL_SUCCESS;
    int i = 0;

    memset(errMsg, '\0', DB2_MAX_ERR_MSG_LEN);
    memset(msg, '\0', SQL_MAX_MESSAGE_LENGTH + 1);
    rc1 =  SQLGetDiagRec( hType, handle, recno, sqlstate, &sqlcode, msg,
                          SQL_MAX_MESSAGE_LENGTH + 1, &length );
    if ( rc1 == SQL_SUCCESS )
    {
        while ((p = strchr( (char *)msg, '\n' ))) {
            *p = '\0';
        }
        sprintf((char*)errMsg, "%s SQLCODE=%d", (char*)msg, (int)sqlcode);
#ifdef _WIN32
        for(i = 0; i < strlen(errMsg); i++)
        {
            if(errMsg[i] == '\r')
            {
                errMsg[i] = ' ';
            }
        }
#endif
        if (cpy_to_global != 0 && rc != 1 ) {
            PyErr_SetString(PyExc_Exception, (char *) errMsg);
        }

        switch (rc) {
            case SQL_ERROR:
                /* Need to copy the error msg and sqlstate into the symbol Table
                    * to cache these results */
                if ( cpy_to_global ) {
                    switch (hType) {
                        case SQL_HANDLE_DBC:
                            strncpy( IBM_DB_G(__python_conn_err_state),
                                        (char*)sqlstate, SQL_SQLSTATE_SIZE+1);
                            strncpy( IBM_DB_G(__python_conn_err_msg),
                                        (char*)errMsg, DB2_MAX_ERR_MSG_LEN);
                            break;

                        case SQL_HANDLE_STMT:
                            strncpy( IBM_DB_G(__python_stmt_err_state),
                                        (char*)sqlstate, SQL_SQLSTATE_SIZE+1);
                            strncpy( IBM_DB_G(__python_stmt_err_msg),
                                        (char*)errMsg, DB2_MAX_ERR_MSG_LEN);
                            break;
                    }
                }
                /* This call was made from ibm_db_errmsg or ibm_db_error or ibm_db_warn */
                /* Check for error and return */
                switch (API) {
                    case DB2_ERR:
                        if ( ret_str != NULL ) {
                            strncpy(ret_str, (char*)sqlstate, SQL_SQLSTATE_SIZE+1);
                        }
                        return;
                    case DB2_ERRMSG:
                        if ( ret_str != NULL ) {
                            strncpy(ret_str, (char*)errMsg, DB2_MAX_ERR_MSG_LEN);
                        }
                        return;
                    default:
                        break;
                }
                break;
            case SQL_SUCCESS_WITH_INFO:
                /* Need to copy the warning msg and sqlstate into the symbol Table
                    * to cache these results */
                if ( cpy_to_global ) {
                    switch ( hType ) {
                        case SQL_HANDLE_DBC:
                            strncpy(IBM_DB_G(__python_conn_warn_state),
                                        (char*)sqlstate, SQL_SQLSTATE_SIZE+1);
                            strncpy(IBM_DB_G(__python_conn_warn_msg),
                                        (char*)errMsg, DB2_MAX_ERR_MSG_LEN);
                            break;

                        case SQL_HANDLE_STMT:
                            strncpy(IBM_DB_G(__python_stmt_warn_state),
                                        (char*)sqlstate, SQL_SQLSTATE_SIZE+1);
                            strncpy(IBM_DB_G(__python_stmt_warn_msg),
                                        (char*)errMsg, DB2_MAX_ERR_MSG_LEN);
                            break;
                    }
                }
                /* This call was made from ibm_db_errmsg or ibm_db_error or ibm_db_warn */
                /* Check for error and return */
                if ( (API == DB2_WARNMSG) && (ret_str != NULL) ) {
                    strncpy(ret_str, (char*)errMsg, DB2_MAX_ERR_MSG_LEN);
                }
                return;
            default:
                break;
        }
    }
}

/*    static int _python_ibm_db_assign_options( void *handle, int type, long opt_key, PyObject *data ) */
static int _python_ibm_db_assign_options( void *handle, int type, long opt_key, PyObject *data )
{
    int rc = 0;
    long option_num = 0;
    SQLINTEGER value_int = 0;
#ifdef __MVS__
    SQLCHAR *option_str = NULL;
#else
    SQLWCHAR *option_str = NULL;
#endif
    int isNewBuffer;

    /* First check to see if it is a non-cli attribut */
    if (opt_key == ATTR_CASE) {
        option_num = NUM2LONG(data);
        if (type == SQL_HANDLE_STMT) {
            switch (option_num) {
                case CASE_LOWER:
                    ((stmt_handle*)handle)->s_case_mode = CASE_LOWER;
                    break;
                case CASE_UPPER:
                    ((stmt_handle*)handle)->s_case_mode = CASE_UPPER;
                    break;
                case CASE_NATURAL:
                    ((stmt_handle*)handle)->s_case_mode = CASE_NATURAL;
                    break;
                default:
                    PyErr_SetString(PyExc_Exception, "ATTR_CASE attribute must be one of CASE_LOWER, CASE_UPPER, or CASE_NATURAL");
                    return -1;
            }
        } else if (type == SQL_HANDLE_DBC) {
            switch (option_num) {
                case CASE_LOWER:
                    ((conn_handle*)handle)->c_case_mode = CASE_LOWER;
                    break;
                case CASE_UPPER:
                    ((conn_handle*)handle)->c_case_mode = CASE_UPPER;
                    break;
                case CASE_NATURAL:
                    ((conn_handle*)handle)->c_case_mode = CASE_NATURAL;
                    break;
                default:
                    PyErr_SetString(PyExc_Exception, "ATTR_CASE attribute must be one of CASE_LOWER, CASE_UPPER, or CASE_NATURAL");
                    return -1;
            }
        } else {
            PyErr_SetString(PyExc_Exception, "Connection or statement handle must be passed in.");
            return -1;
        }
    } else if (opt_key == USE_WCHAR) {
        option_num = NUM2LONG(data);
        if (type == SQL_HANDLE_STMT) {
            switch (option_num) {
                case WCHAR_YES:
                    ((stmt_handle*)handle)->s_use_wchar = WCHAR_YES;
                    break;
                case WCHAR_NO:
                    ((stmt_handle*)handle)->s_use_wchar = WCHAR_NO;
                    break;
                default:
                    PyErr_SetString(PyExc_Exception, "USE_WCHAR attribute must be one of WCHAR_YES or WCHAR_NO");
                    return -1;
            }
        }
        else if (type == SQL_HANDLE_DBC) {
            switch (option_num) {
                case WCHAR_YES:
                    ((conn_handle*)handle)->c_use_wchar = WCHAR_YES;
                    break;
                case WCHAR_NO:
                    ((conn_handle*)handle)->c_use_wchar = WCHAR_NO;
                    break;
                default:
                    PyErr_SetString(PyExc_Exception, "USE_WCHAR attribute must be one of WCHAR_YES or WCHAR_NO");
                    return -1;
            }
        }
    } else if (type == SQL_HANDLE_STMT) {
        if (PyString_Check(data)|| PyUnicode_Check(data)) {
            data = PyUnicode_FromObject(data);
#ifdef __MVS__
            option_str = getUnicodeDataAsSQLCHAR(data, &isNewBuffer);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetStmtAttr((SQLHSTMT)((stmt_handle *)handle)->hstmt, opt_key, (SQLPOINTER)option_str, SQL_IS_INTEGER );
            Py_END_ALLOW_THREADS;
#else
            option_str = getUnicodeDataAsSQLWCHAR(data, &isNewBuffer);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetStmtAttrW((SQLHSTMT)((stmt_handle *)handle)->hstmt, opt_key, (SQLPOINTER)option_str, SQL_IS_INTEGER );
            Py_END_ALLOW_THREADS;
#endif
            if ( rc == SQL_ERROR ) {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if (isNewBuffer)
                PyMem_Del(option_str);

        } else {
            option_num = NUM2LONG(data);
            if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_OFF) ((conn_handle*)handle)->auto_commit = 0;
            else if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_ON) ((conn_handle*)handle)->auto_commit = 1;
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetStmtAttr((SQLHSTMT)((stmt_handle *)handle)->hstmt, opt_key, (SQLPOINTER)option_num, SQL_IS_INTEGER );
            Py_END_ALLOW_THREADS;
            if ( rc == SQL_ERROR ) {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if (opt_key == SQL_ATTR_CURSOR_TYPE){
                ((stmt_handle *)handle)->cursor_type = option_num;
                if ( rc == SQL_SUCCESS_WITH_INFO ) {
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetStmtAttr( ((stmt_handle *)handle)->hstmt, opt_key, &value_int, SQL_IS_INTEGER, NULL);
                    Py_END_ALLOW_THREADS;
                    if (rc == SQL_ERROR) {
                        _python_ibm_db_check_sql_errors(((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        PyErr_Clear();
                        return -1;
                    }
                    ((stmt_handle *)handle)->cursor_type = value_int;
                }
            }
        }
    } else if (type == SQL_HANDLE_DBC) {
        if (PyString_Check(data)|| PyUnicode_Check(data)) {
            data = PyUnicode_FromObject(data);
#ifdef __MVS__
            option_str = getUnicodeDataAsSQLCHAR(data, &isNewBuffer);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttr((SQLHSTMT)((conn_handle*)handle)->hdbc, opt_key, (SQLPOINTER)option_str, SQL_NTS);
            Py_END_ALLOW_THREADS;
#else
            option_str = getUnicodeDataAsSQLWCHAR(data, &isNewBuffer);
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttrW((SQLHSTMT)((conn_handle*)handle)->hdbc, opt_key, (SQLPOINTER)option_str, SQL_NTS);
            Py_END_ALLOW_THREADS;
#endif
            if ( rc == SQL_ERROR ) {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if (isNewBuffer)
                PyMem_Del(option_str);

        } else {
            option_num = NUM2LONG(data);
            if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_OFF) ((conn_handle*)handle)->auto_commit = 0;
            else if (opt_key == SQL_ATTR_AUTOCOMMIT && option_num == SQL_AUTOCOMMIT_ON) ((conn_handle*)handle)->auto_commit = 1;
#ifdef __MVS__
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttr((SQLHSTMT)((conn_handle*)handle)->hdbc, opt_key, (SQLPOINTER)option_num, SQL_IS_INTEGER);
            Py_END_ALLOW_THREADS;
#else
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetConnectAttrW((SQLHSTMT)((conn_handle*)handle)->hdbc, opt_key, (SQLPOINTER)option_num, SQL_IS_INTEGER);
            Py_END_ALLOW_THREADS;
#endif
            if ( rc == SQL_ERROR ) {
                _python_ibm_db_check_sql_errors((SQLHSTMT)((stmt_handle *)handle)->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
        }
    } else {
        PyErr_SetString(PyExc_Exception, "Connection or statement handle must be passed in.");
        return -1;
    }
    return 0;
}

/*    static int _python_ibm_db_parse_options( PyObject *options, int type, void *handle)
*/
static int _python_ibm_db_parse_options ( PyObject *options, int type, void *handle )
{
    int numOpts = 0, i = 0;
    PyObject *keys = NULL;
    PyObject *key = NULL; /* Holds the Option Index Key */
    PyObject *data = NULL;
    PyObject *tc_pass = NULL;
    int rc = 0;

    if ( !NIL_P(options) ) {
        keys = PyDict_Keys(options);
        numOpts = PyList_Size(keys);

        for ( i = 0; i < numOpts; i++) {
            key = PyList_GetItem(keys, i);
            data = PyDict_GetItem(options, key);

            if(NUM2LONG(key) == SQL_ATTR_TRUSTED_CONTEXT_PASSWORD) {
                tc_pass = data;
            } else {
                /* Assign options to handle. */
                /* Sets the options in the handle with CLI/ODBC calls */
                rc = _python_ibm_db_assign_options(handle, type, NUM2LONG(key), data);
            }
            if (rc)
                return SQL_ERROR;
        }
        if (!NIL_P(tc_pass) ) {
            rc = _python_ibm_db_assign_options(handle, type, SQL_ATTR_TRUSTED_CONTEXT_PASSWORD, tc_pass);
        }
        if (rc)
            return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

/*    static int _python_ibm_db_get_result_set_info(stmt_handle *stmt_res)
initialize the result set information of each column. This must be done once
*/
static int _python_ibm_db_get_result_set_info(stmt_handle *stmt_res)
{
    int rc = -1, i;
    SQLSMALLINT nResultCols = 0, name_length;
    SQLCHAR tmp_name[BUFSIZ];

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLNumResultCols((SQLHSTMT)stmt_res->hstmt, &nResultCols);
    Py_END_ALLOW_THREADS;

    if ( rc == SQL_ERROR || nResultCols == 0) {
      _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                      SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
      return -1;
    }
      /*  if( rc == SQL_SUCCESS_WITH_INFO )
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                           SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
        } */
    stmt_res->num_columns = nResultCols;
    stmt_res->column_info = ALLOC_N(ibm_db_result_set_info, nResultCols);
    if ( stmt_res->column_info == NULL ) {
      PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
      return -1;
    }
    memset(stmt_res->column_info, 0, sizeof(ibm_db_result_set_info)*nResultCols);
    /* return a set of attributes for a column */
    for (i = 0 ; i < nResultCols; i++) {
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLDescribeCol((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)(i + 1 ),
                            (SQLCHAR *)&tmp_name, BUFSIZ, &name_length,
                            &stmt_res->column_info[i].type,
                            &stmt_res->column_info[i].size,
                            &stmt_res->column_info[i].scale,
                            &stmt_res->column_info[i].nullable);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR  || rc == SQL_SUCCESS_WITH_INFO ) {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                            SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
        }
        if ( rc == SQL_ERROR )
        {
            return -1;
        }
        if ( name_length <= 0 ) {
            stmt_res->column_info[i].name = (SQLCHAR *)estrdup("");
            if ( stmt_res->column_info[i].name == NULL ) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }

        } else if (name_length >= BUFSIZ ) {
            /* column name is longer than BUFSIZ */
            stmt_res->column_info[i].name = (SQLCHAR*)ALLOC_N(char, name_length+1);
            if ( stmt_res->column_info[i].name == NULL ) {
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

            if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            }
            if ( rc == SQL_ERROR )
            {
                return -1;
            }

        } else {
            stmt_res->column_info[i].name = (SQLCHAR*)estrdup((char*)tmp_name);
            if ( stmt_res->column_info[i].name == NULL ) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return -1;
            }

        }
    }
    return 0;
}

/*    static int _python_ibn_bind_column_helper(stmt_handle *stmt_res)
    bind columns to data, this must be done once
*/
static int _python_ibm_db_bind_column_helper(stmt_handle *stmt_res)
{
    SQLINTEGER in_length = 0;
    SQLSMALLINT column_type;
    ibm_db_row_data_type *row_data;
    int i, rc = SQL_SUCCESS;

    stmt_res->row_data = ALLOC_N(ibm_db_row_type, stmt_res->num_columns);
    if ( stmt_res->row_data == NULL ) {
        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
        return -1;
    }
    memset(stmt_res->row_data, 0, sizeof(ibm_db_row_type)*stmt_res->num_columns);

    for (i = 0; i<stmt_res->num_columns; i++) {
        column_type = stmt_res->column_info[i].type;
        row_data = &stmt_res->row_data[i].data;
        switch(column_type) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
                if ( stmt_res->s_use_wchar == WCHAR_NO ) {
                    in_length = stmt_res->column_info[i].size+1;
                    row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                    if ( row_data->str_val == NULL ) {
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return -1;
                    }
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                            SQL_C_CHAR, row_data->str_val, in_length,
                            (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                    Py_END_ALLOW_THREADS;
                    if ( rc == SQL_ERROR ) {
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
                in_length = stmt_res->column_info[i].size+1;
                row_data->w_val = (SQLWCHAR *) ALLOC_N(SQLWCHAR, in_length);
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_WCHAR, row_data->w_val, in_length * sizeof(SQLWCHAR),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;
                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL,
                        -1, 1);
                }
                break;

            case SQL_BINARY:
            case SQL_LONGVARBINARY:
            case SQL_VARBINARY:
                if ( stmt_res->s_bin_mode == CONVERT ) {
                    in_length = 2*(stmt_res->column_info[i].size)+1;
                    row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                    if ( row_data->str_val == NULL ) {
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return -1;
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                        SQL_C_CHAR, row_data->str_val, in_length,
                        (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR ) {
                        _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                            SQL_HANDLE_STMT, rc, 1, NULL,
                            -1, 1);
                    }
                } else {
                    in_length = stmt_res->column_info[i].size+1;
                    row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                    if ( row_data->str_val == NULL ) {
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return -1;
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                        SQL_C_DEFAULT, row_data->str_val, in_length,
                        (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR ) {
                        _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                            SQL_HANDLE_STMT, rc, 1, NULL,
                            -1, 1);
                    }
                }
                break;

            case SQL_BIGINT:
            case SQL_DECFLOAT:
                in_length = stmt_res->column_info[i].size+3;
                row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                if ( row_data->str_val == NULL ) {
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_CHAR, row_data->str_val, in_length,
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                }
                break;

            case SQL_TYPE_DATE:
                row_data->date_val = ALLOC(DATE_STRUCT);
                if ( row_data->date_val == NULL ) {
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_TYPE_DATE, row_data->date_val, sizeof(DATE_STRUCT),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                }
                break;

            case SQL_TYPE_TIME:
                row_data->time_val = ALLOC(TIME_STRUCT);
                if ( row_data->time_val == NULL ) {
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_TYPE_TIME, row_data->time_val, sizeof(TIME_STRUCT),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                }
                break;

            case SQL_TYPE_TIMESTAMP:
                row_data->ts_val = ALLOC(TIMESTAMP_STRUCT);
                if ( row_data->ts_val == NULL ) {
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_TYPE_TIMESTAMP, row_data->time_val, sizeof(TIMESTAMP_STRUCT),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                    return -1;
                }
                break;

#ifdef __MVS__
            case SQL_SMALLINT:
#else
            case SQL_SMALLINT:
            case SQL_BOOLEAN:
#endif

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_DEFAULT, &row_data->s_val,
                    sizeof(row_data->s_val),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1,
                        1);
                }
                break;

            case SQL_INTEGER:

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_DEFAULT, &row_data->i_val,
                    sizeof(row_data->i_val),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1,
                        1);
                }
                break;

            case SQL_BIT:

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_LONG, &row_data->i_val,
                    sizeof(row_data->i_val),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1,
                        1);
                }
                break;

            case SQL_REAL:

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_FLOAT, &row_data->r_val,
                    sizeof(row_data->r_val),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1,
                        1);
                }
                break;

            case SQL_FLOAT:

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_DEFAULT, &row_data->f_val,
                    sizeof(row_data->f_val),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1,
                        1);
                }
                break;

            case SQL_DOUBLE:

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_DEFAULT, &row_data->d_val,
                    sizeof(row_data->d_val),
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1,
                        1);
                }
                break;

            case SQL_DECIMAL:
            case SQL_NUMERIC:
                in_length = stmt_res->column_info[i].size +
                    stmt_res->column_info[i].scale + 2 + 1;
                row_data->str_val = (SQLCHAR *)ALLOC_N(char, in_length);
                if ( row_data->str_val == NULL ) {
                    PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                    return -1;
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindCol((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)(i+1),
                    SQL_C_CHAR, row_data->str_val, in_length,
                    (SQLINTEGER *)(&stmt_res->row_data[i].out_length));
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                        SQL_HANDLE_STMT, rc, 1, NULL, -1,
                        1);
                }
                break;

            case SQL_BLOB:
            case SQL_CLOB:
            case SQL_DBCLOB:
            case SQL_XML:
                stmt_res->row_data[i].out_length = 0;
                break;

            default:
                break;
        }
    }
    return rc;
}

/*    static void _python_ibm_db_clear_stmt_err_cache () */
static void _python_ibm_db_clear_stmt_err_cache(void)
{
    memset(IBM_DB_G(__python_stmt_err_msg), 0, DB2_MAX_ERR_MSG_LEN);
    memset(IBM_DB_G(__python_stmt_err_state), 0, SQL_SQLSTATE_SIZE + 1);
}

/*    static int _python_ibm_db_connect_helper( argc, argv, isPersistent ) */
static PyObject *_python_ibm_db_connect_helper( PyObject *self, PyObject *args, int isPersistent )
{
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
    int rc = 0;
    SQLINTEGER conn_alive;
    conn_handle *conn_res = NULL;
    int reused = 0;
    PyObject *hKey = NULL;
    PyObject *entry = NULL;
    char server[2048];
    int isNewBuffer;
    PyObject *pid = NULL;
    conn_alive = 1;

    if (!PyArg_ParseTuple(args, "OOO|OO", &databaseObj, &uidObj, &passwordObj, &options, &literal_replacementObj)){
        return NULL;
    }
    do {
        databaseObj = PyUnicode_FromObject(databaseObj);
        uidObj = PyUnicode_FromObject(uidObj);
        passwordObj = PyUnicode_FromObject(passwordObj);

        /* Check if we already have a connection for this userID & database
        * combination in this process.
        */
        if (isPersistent) {
	    // we do not want to process a None type and segfault. Better safe than sorry!
	    if (NIL_P(databaseObj)) {
		PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
		return NULL;
	    }
	    else if(!(PyUnicode_Contains(databaseObj, equal) > 0) && ( NIL_P(uidObj) || NIL_P(passwordObj)))
	    {
		PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
		return NULL;
	    }
	    else
	    {
		if (NIL_P(uidObj) || NIL_P(passwordObj))
		{
                    PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
		    return NULL;
		}
	    }

            hKey = PyUnicode_Concat(StringOBJ_FromASCII("__ibm_db_"), uidObj);
            hKey = PyUnicode_Concat(hKey, databaseObj);
            hKey = PyUnicode_Concat(hKey, passwordObj);

            pid = PyObject_CallObject(os_getpid, NULL);
            if (pid == NULL) {
                PyErr_SetString(PyExc_Exception, "Failed to obtain current process id");
                return NULL;
            }
            hKey = PyUnicode_Concat(hKey, PyUnicode_FromFormat("%ld", PyLong_AsLong(pid)));
            Py_DECREF(pid);

            entry = PyDict_GetItem(persistent_list, hKey);

            if (entry != NULL) {
                Py_INCREF(entry);
                conn_res = (conn_handle *)entry;
#if !defined(PASE) && !defined(__MVS__) /* i5/OS server mode is persistant */
                /* Need to reinitialize connection? */
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLGetConnectAttr(conn_res->hdbc, SQL_ATTR_PING_DB,
                    (SQLPOINTER)&conn_alive, 0, NULL);
                Py_END_ALLOW_THREADS;
                if ( (rc == SQL_SUCCESS) && conn_alive ) {
                    _python_ibm_db_check_sql_errors( conn_res->hdbc, SQL_HANDLE_DBC,
                        rc, 1, NULL, -1, 1);
                    reused = 1;
                } /* else will re-connect since connection is dead */
#endif /* PASE */
#if defined(__MVS__)
		/* Since SQL_ATTR_PING_DB is not supported by z ODBC driver,
		 * we will not check for db connection status */
		reused = 1;
#endif
            }
        } else {
            /* Need to check for max pconnections? */
        }

        if ( !NIL_P(literal_replacementObj) ) {
            literal_replacement = (SQLINTEGER) PyInt_AsLong(literal_replacementObj);
        } else {
            literal_replacement = SET_QUOTED_LITERAL_REPLACEMENT_OFF; /*QUOTED LITERAL replacemnt is OFF by default*/
        }

        if (conn_res == NULL) {
            conn_res = PyObject_NEW(conn_handle, &conn_handleType);
            conn_res->henv = 0;
            conn_res->hdbc = 0;
        }

        /* We need to set this early, in case we get an error below,
        so we know how to free the connection */
        conn_res->flag_pconnect = isPersistent;
        /* Allocate ENV handles if not present */
        if ( !conn_res->henv ) {
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &(conn_res->henv));
            Py_END_ALLOW_THREADS;
            if (rc != SQL_SUCCESS) {
                _python_ibm_db_check_sql_errors( conn_res->henv, SQL_HANDLE_ENV, rc,
                    1, NULL, -1, 1);
                break;
            }
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLSetEnvAttr((SQLHENV)conn_res->henv, SQL_ATTR_ODBC_VERSION,
                (void *)SQL_OV_ODBC3, 0);
            Py_END_ALLOW_THREADS;
        }

        if (!reused) {
            /* Alloc CONNECT Handle */
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLAllocHandle(SQL_HANDLE_DBC, conn_res->henv, &(conn_res->hdbc));
            Py_END_ALLOW_THREADS;
            if (rc != SQL_SUCCESS) {
                _python_ibm_db_check_sql_errors(conn_res->henv, SQL_HANDLE_ENV, rc,
                    1, NULL, -1, 1);
                break;
            }
        }

        /* Set this after the connection handle has been allocated to avoid
        unnecessary network flows. Initialize the structure to default values */
        conn_res->auto_commit = SQL_AUTOCOMMIT_ON;
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc, SQL_ATTR_AUTOCOMMIT,
            (SQLPOINTER)(conn_res->auto_commit), SQL_NTS);
        Py_END_ALLOW_THREADS;

        conn_res->c_bin_mode = IBM_DB_G(bin_mode);
        conn_res->c_case_mode = CASE_NATURAL;
        conn_res->c_use_wchar = WCHAR_YES;
        conn_res->c_cursor_type = SQL_SCROLL_FORWARD_ONLY;

        conn_res->error_recno_tracker = 1;
        conn_res->errormsg_recno_tracker = 1;

        /* handle not active as of yet */
        conn_res->handle_active = 0;

        /* Set Options */
        if ( !NIL_P(options) ) {
            if(!PyDict_Check(options)) {
                PyErr_SetString(PyExc_Exception, "options Parameter must be of type dictionay");
                return NULL;
            }
            rc = _python_ibm_db_parse_options( options, SQL_HANDLE_DBC, conn_res );
            if (rc != SQL_SUCCESS) {
                Py_BEGIN_ALLOW_THREADS;
                SQLFreeHandle(SQL_HANDLE_DBC, conn_res->hdbc);
                SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
                Py_END_ALLOW_THREADS;
                break;
            }
        }

        if (! reused) {
            /* Connect */
            /* If the string contains a =, use SQLDriverConnect */
            if (NIL_P(databaseObj)) {
                PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
                return NULL;
            }
            database = getUnicodeDataAsSQLWCHAR(databaseObj, &isNewBuffer);
            if ( PyUnicode_Contains(databaseObj, equal) > 0 ) {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLDriverConnectW((SQLHDBC)conn_res->hdbc, (SQLHWND)NULL,
                    database, SQL_NTS, NULL, 0, NULL,
                    SQL_DRIVER_NOPROMPT );
                Py_END_ALLOW_THREADS;
            } else {
                if (NIL_P(uidObj) || NIL_P(passwordObj)) {
                    PyErr_SetString(PyExc_Exception, "Supplied Parameter is invalid");
                    return NULL;
                }
                uid = getUnicodeDataAsSQLWCHAR(uidObj, &isNewBuffer);
                password = getUnicodeDataAsSQLWCHAR(passwordObj, &isNewBuffer);
                Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
                rc = SQLConnectW((SQLHDBC)conn_res->hdbc,
                    database,
                    PyUnicode_GetSize(databaseObj)*2,
                    uid,
                    PyUnicode_GetSize(uidObj)*2,
                    password,
                    PyUnicode_GetSize(passwordObj)*2);
#else
                rc = SQLConnectW((SQLHDBC)conn_res->hdbc,
                    database,
                    PyUnicode_GetSize(databaseObj),
                    uid,
                    PyUnicode_GetSize(uidObj),
                    password,
                    PyUnicode_GetSize(passwordObj));
#endif
                Py_END_ALLOW_THREADS;
            }
            if( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
            {
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                       rc,1, NULL, -1, 1);
            }
            if ( rc == SQL_ERROR ) {
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

            rc = _python_ibm_db_set_decfloat_rounding_mode_client(conn_res->hdbc);
            if (rc != SQL_SUCCESS){
                  _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc,
                                  1, NULL, -1, 1);
            }
#endif
#endif

            /* Get the server name */
            memset(server, 0, sizeof(server));

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLGetInfo(conn_res->hdbc, SQL_DBMS_NAME, (SQLPOINTER)server,
                2048, NULL);
            Py_END_ALLOW_THREADS;

            if (!strcmp(server, "AS")) is_systemi = 1;
            if (!strncmp(server, "IDS", 3)) is_informix = 1;

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
            if (!is_informix && (literal_replacement == SET_QUOTED_LITERAL_REPLACEMENT_ON)) {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc,
                    SQL_ATTR_REPLACE_QUOTED_LITERALS,
                    (SQLPOINTER) (ENABLE_NUMERIC_LITERALS),
                    SQL_IS_INTEGER);
                Py_END_ALLOW_THREADS;
                if (rc != SQL_SUCCESS)
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc,
                    SQL_ATTR_REPLACE_QUOTED_LITERALS_OLDVALUE,
                    (SQLPOINTER)(ENABLE_NUMERIC_LITERALS),
                    SQL_IS_INTEGER);
                    Py_END_ALLOW_THREADS;
            }
            if (rc != SQL_SUCCESS) {
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc,
                    1, NULL, -1, 1);
            }
        }
        Py_XDECREF(databaseObj);
        Py_XDECREF(uidObj);
        Py_XDECREF(passwordObj);
        conn_res->handle_active = 1;
    } while (0);

    if (hKey != NULL) {
        if (! reused && rc == SQL_SUCCESS) {
            /* If we created a new persistent connection, add it to the
            *  persistent_list
            */
            PyDict_SetItem(persistent_list, hKey, (PyObject *)conn_res);
        }
        Py_DECREF(hKey);
    }

    if (isNewBuffer) {
        PyMem_Del(database);
        PyMem_Del(uid);
        PyMem_Del(password);
    }

    if ( rc != SQL_SUCCESS ) {
        if (conn_res != NULL && conn_res->handle_active) {
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeHandle(SQL_HANDLE_DBC, conn_res->hdbc);
            rc = SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
            Py_END_ALLOW_THREADS;
        }
        if (conn_res != NULL) {
            PyObject_Del(conn_res);
        }
        return NULL;
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
static PyObject* getSQLWCharAsPyUnicodeObject(SQLWCHAR* sqlwcharData, int sqlwcharBytesLen)
{
    PyObject *sysmodule = NULL, *maxuni = NULL;
    long maxuniValue;
    PyObject* u;
    sysmodule = PyImport_ImportModule("sys");
    maxuni = PyObject_GetAttrString(sysmodule, "maxunicode");
    maxuniValue = PyInt_AsLong(maxuni);

    if (maxuniValue <= 65536) {
    /* this is UCS2 python.. nothing to do really */
        return PyUnicode_FromUnicode((Py_UNICODE *)sqlwcharData, sqlwcharBytesLen / sizeof(SQLWCHAR));
        }

    if (is_bigendian()) {
        int bo = 1;
        u = PyUnicode_DecodeUTF16((char *)sqlwcharData, sqlwcharBytesLen, "strict", &bo);
    } else {
        int bo = -1;
        u = PyUnicode_DecodeUTF16((char *)sqlwcharData, sqlwcharBytesLen, "strict", &bo);
    }
    return u;
}


static SQLCHAR* getUnicodeDataAsSQLCHAR(PyObject *pyobj, int *isNewBuffer)
{
    PyObject *sysmodule = NULL, *maxuni = NULL;
    long maxuniValue;
    SQLCHAR* pNewBuffer = NULL;
    PyObject* pyBytesobj = PyUnicode_AsUTF8String(pyobj);
    int nCharLen = PyBytes_GET_SIZE(pyBytesobj);

    *isNewBuffer = 1;
    pNewBuffer = (SQLCHAR *)ALLOC_N(SQLCHAR, nCharLen + 1);
    memset(pNewBuffer, 0, sizeof(SQLCHAR) * (nCharLen + 1));
    memcpy(pNewBuffer, PyBytes_AsString(pyBytesobj), sizeof(SQLCHAR) * (nCharLen) );
    Py_DECREF(pyBytesobj);
    return pNewBuffer;
}

/**
*This function takes value as pyObject and convert it to SQLWCHAR and return it
*
**/
static SQLWCHAR* getUnicodeDataAsSQLWCHAR(PyObject *pyobj, int *isNewBuffer)
{
    PyObject *sysmodule = NULL, *maxuni = NULL;
    long maxuniValue;
    PyObject *pyUTFobj;
    SQLWCHAR* pNewBuffer = NULL;
    int nCharLen = PyUnicode_GET_SIZE(pyobj);

    sysmodule = PyImport_ImportModule("sys");
    maxuni = PyObject_GetAttrString(sysmodule, "maxunicode");
    maxuniValue = PyInt_AsLong(maxuni);

    if (maxuniValue <= 65536) {
        *isNewBuffer = 0;
        return (SQLWCHAR*)PyUnicode_AS_UNICODE(pyobj);
    }

    *isNewBuffer = 1;
    pNewBuffer = (SQLWCHAR *)ALLOC_N(SQLWCHAR, nCharLen + 1);
    memset(pNewBuffer, 0, sizeof(SQLWCHAR) * (nCharLen + 1));
    if (is_bigendian()) {
        pyUTFobj = PyCodec_Encode(pyobj, "utf-16-be", "strict");
    } else {
        pyUTFobj = PyCodec_Encode(pyobj, "utf-16-le", "strict");
    }
    memcpy(pNewBuffer, PyBytes_AsString(pyUTFobj), sizeof(SQLWCHAR) * (nCharLen) );
    Py_DECREF(pyUTFobj);
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
    SQLCHAR decflt_rounding[20];
    SQLHANDLE hstmt;
    int rc = 0;
    int rounding_mode;
    SQLINTEGER decfloat;


    SQLCHAR *stmt = (SQLCHAR *)"values current decfloat rounding mode";

    /* Allocate a Statement Handle */
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    Py_END_ALLOW_THREADS;
    if (rc == SQL_ERROR) {
        _python_ibm_db_check_sql_errors(hdbc, SQL_HANDLE_DBC, rc, 1,
            NULL, -1, 1);
        return rc;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLExecDirect((SQLHSTMT)hstmt, stmt, SQL_NTS);
    Py_END_ALLOW_THREADS;

    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
    {
        _python_ibm_db_check_sql_errors((SQLHSTMT)hstmt,
                SQL_HANDLE_STMT, rc, 1, NULL,
                -1, 1);
    }
    if ( rc == SQL_ERROR ) {
        return rc;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLBindCol((SQLHSTMT)hstmt, 1, SQL_C_DEFAULT, decflt_rounding, 20, NULL);
    Py_END_ALLOW_THREADS;

    if ( rc == SQL_ERROR ) {
        _python_ibm_db_check_sql_errors((SQLHSTMT)hstmt,
            SQL_HANDLE_STMT, rc, 1, NULL,
            -1, 1);
        return rc;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLFetch(hstmt);
    if (rc == SQL_SUCCESS_WITH_INFO )
    {
        _python_ibm_db_check_sql_errors((SQLHSTMT)hstmt,
                SQL_HANDLE_STMT, rc, 1, NULL,
                -1 ,1);
    }
    Py_END_ALLOW_THREADS;

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    Py_END_ALLOW_THREADS;

    /* Now setting up the same rounding mode on the client*/
    if (strcmp(decflt_rounding, "ROUND_HALF_EVEN") == 0) rounding_mode = ROUND_HALF_EVEN;
    if (strcmp(decflt_rounding, "ROUND_HALF_UP") == 0) rounding_mode = ROUND_HALF_UP;
    if (strcmp(decflt_rounding, "ROUND_DOWN") == 0) rounding_mode = ROUND_DOWN;
    if (strcmp(decflt_rounding, "ROUND_CEILING") == 0) rounding_mode = ROUND_CEILING;
    if (strcmp(decflt_rounding, "ROUND_FLOOR") == 0) rounding_mode = ROUND_FLOOR;

    Py_BEGIN_ALLOW_THREADS;
#ifndef PASE
    rc = SQLSetConnectAttr(hdbc, SQL_ATTR_DECFLOAT_ROUNDING_MODE, (SQLPOINTER)rounding_mode, SQL_NTS);
#else
    rc = SQLSetConnectAttr(hdbc, SQL_ATTR_DECFLOAT_ROUNDING_MODE, (SQLPOINTER)&rounding_mode, SQL_NTS);
#endif
    Py_END_ALLOW_THREADS;

    return rc;

}
#endif
#endif

/* static void _python_ibm_db_clear_conn_err_cache () */
static void _python_ibm_db_clear_conn_err_cache(void)
{
    /* Clear out the cached conn messages */
    memset(IBM_DB_G(__python_conn_err_msg), 0, DB2_MAX_ERR_MSG_LEN);
    memset(IBM_DB_G(__python_conn_err_state), 0, SQL_SQLSTATE_SIZE + 1);
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
    _python_ibm_db_clear_conn_err_cache();
    return _python_ibm_db_connect_helper( self, args, 0 );
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
    _python_ibm_db_clear_conn_err_cache();
    return _python_ibm_db_connect_helper( self, args, 1);
}

/*
 * static void _python_clear_local_var(PyObject *dbNameObj, SQLWCHAR *dbName, PyObject *codesetObj, SQLWCHAR *codesetObj, PyObject *modeObj, SQLWCHAR *mode, int isNewBuffer)
 */
static void _python_clear_local_var(PyObject *dbNameObj, SQLWCHAR *dbName, PyObject *codesetObj, SQLWCHAR *codeset, PyObject *modeObj, SQLWCHAR *mode, int isNewBuffer)
{
    if ( !NIL_P( dbNameObj ) ) {
        Py_XDECREF( dbNameObj );
        if ( isNewBuffer ) {
            PyMem_Del( dbName );
        }
    }

    if ( !NIL_P( codesetObj ) ) {
        Py_XDECREF( codesetObj );
        if ( isNewBuffer ) {
            PyMem_Del( codeset );
        }
    }

    if ( !NIL_P( modeObj ) ) {
        Py_XDECREF( modeObj );
        if ( isNewBuffer ) {
            PyMem_Del( mode );
        }
    }
}

/*
 * static int _python_ibm_db_createdb(conn_handle *conn_res, PyObject *dbNameObj, PyObject *codesetObj, PyObject *modeObj, int createNX)
 */
static int _python_ibm_db_createdb(conn_handle *conn_res, PyObject *dbNameObj, PyObject *codesetObj, PyObject *modeObj, int createNX)
{
    SQLWCHAR *dbName = NULL;
    SQLWCHAR *codeset = NULL;
    SQLWCHAR *mode = NULL;
    SQLINTEGER sqlcode;
    SQLSMALLINT length;
    SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
    int isNewBuffer;
    int rc = 0;
#ifdef _WIN32
    HINSTANCE cliLib = NULL;
    FARPROC sqlcreatedb;
#else
    void *cliLib = NULL;
    typedef int (*sqlcreatedbType)( SQLHDBC, SQLWCHAR *, SQLINTEGER, SQLWCHAR *, SQLINTEGER, SQLWCHAR *, SQLINTEGER );
    sqlcreatedbType sqlcreatedb;
#endif

#if defined __APPLE__ || defined _AIX
    PyErr_SetString( PyExc_Exception, "Not supported: This function is currently not supported on this platform" );
    return -1;
#else

    if ( !NIL_P( conn_res ) ) {
        if ( NIL_P( dbNameObj ) ) {
            PyErr_SetString( PyExc_Exception, "Supplied database name Parameter is invalid" );
            return -1;
        }
        /* Check to ensure the connection resource given is active */
        if ( !conn_res->handle_active ) {
            PyErr_SetString( PyExc_Exception, "Connection is not active" );
            return -1;
        }

        dbNameObj = PyUnicode_FromObject( dbNameObj );
        if ( dbNameObj != NULL &&  dbNameObj != Py_None ) {
            dbName = getUnicodeDataAsSQLWCHAR( dbNameObj, &isNewBuffer );
        } else {
            return -1;
        }

        if ( !NIL_P( codesetObj ) ) {
            codesetObj = PyUnicode_FromObject( codesetObj );
            if ( codesetObj != NULL &&  codesetObj != Py_None ) {
                codeset = getUnicodeDataAsSQLWCHAR( codesetObj, &isNewBuffer );
            } else {
                _python_clear_local_var( dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer );
                return -1;
            }

        }
        if ( !NIL_P( modeObj ) ) {
            modeObj = PyUnicode_FromObject( modeObj );
            if ( codesetObj != NULL &&  codesetObj != Py_None ) {
                mode = getUnicodeDataAsSQLWCHAR( modeObj, &isNewBuffer );
            } else {
                _python_clear_local_var( dbNameObj, dbName, codesetObj, codeset, NULL, NULL, isNewBuffer );
                return -1;
            }
        }

#ifdef _WIN32
        cliLib = DLOPEN( LIBDB2 );
#else
        cliLib = DLOPEN( LIBDB2, RTLD_LAZY );
#endif
        if ( !cliLib ) {
            sprintf( (char *)msg, "Error in loading %s library file", LIBDB2 );
            PyErr_SetString( PyExc_Exception,  (char *)msg );
            _python_clear_local_var( dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer );
            return -1;
        }
        Py_BEGIN_ALLOW_THREADS;
#ifdef _WIN32
        sqlcreatedb =  DLSYM( cliLib, "SQLCreateDbW" );
#else
        sqlcreatedb = (sqlcreatedbType) DLSYM( cliLib, "SQLCreateDbW" );
#endif
        Py_END_ALLOW_THREADS;
        if ( sqlcreatedb == NULL )  {
#ifdef _WIN32
            sprintf( (char *)msg, "Not supported: This function is only supported from v97fp4 version of cli on window" );
#else
            sprintf( (char *)msg, "Not supported: This function is only supported from v97fp3 version of cli" );
#endif
            PyErr_SetString( PyExc_Exception, (char *)msg );
            DLCLOSE( cliLib );
            _python_clear_local_var( dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer );
            return -1;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = (*sqlcreatedb)( (SQLHDBC)conn_res->hdbc, dbName, SQL_NTS, codeset, SQL_NTS, mode, SQL_NTS );
        Py_END_ALLOW_THREADS;

        DLCLOSE( cliLib );
        if ( rc != SQL_SUCCESS ) {
            if ( createNX == 1 ) {
                if ( SQLGetDiagRec( SQL_HANDLE_DBC, (SQLHDBC)conn_res->hdbc, 1, sqlstate, &sqlcode, msg, SQL_MAX_MESSAGE_LENGTH + 1, &length ) == SQL_SUCCESS ) {
                    if ( sqlcode == -1005 ) {
                        _python_clear_local_var( dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer );
                        return 0;
                    }
                }
            }
            _python_ibm_db_check_sql_errors( conn_res->hdbc, SQL_HANDLE_DBC, rc, 1, NULL, -1, 1 );
            _python_clear_local_var( dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer );
            return -1;
        }
        _python_clear_local_var( dbNameObj, dbName, codesetObj, codeset, modeObj, mode, isNewBuffer );
        return 0;
    } else {
        PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
        return -1;
    }
#endif
}

/*
 * static int _python_ibm_db_dropdb(conn_handle *conn_res, PyObject *dbNameObj, int recreate)
 */
static int _python_ibm_db_dropdb(conn_handle *conn_res, PyObject *dbNameObj, int recreate)
{
    SQLWCHAR *dbName = NULL;
    SQLINTEGER sqlcode;
    SQLSMALLINT length;
    SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
    int isNewBuffer;
    int rc = 0;
#ifdef _WIN32
    FARPROC sqldropdb;
    HINSTANCE cliLib = NULL;
#else
    typedef int (*sqldropdbType)( SQLHDBC, SQLWCHAR *, SQLINTEGER );
    sqldropdbType sqldropdb;
    void *cliLib;
#endif

#if defined __APPLE__ || defined _AIX
    PyErr_SetString( PyExc_Exception, "Not supported: This function is currently not supported on this platform" );
    return -1;
#else

    if ( !NIL_P( conn_res ) ) {
        if ( NIL_P( dbNameObj ) ) {
            PyErr_SetString( PyExc_Exception, "Supplied database name Parameter is invalid" );
            return -1;
        }
        /* Check to ensure the connection resource given is active */
        if ( !conn_res->handle_active ) {
            PyErr_SetString( PyExc_Exception, "Connection is not active" );
            return -1;
        }

        dbNameObj = PyUnicode_FromObject( dbNameObj );
        if ( dbNameObj != NULL &&  dbNameObj != Py_None ) {
            dbName = getUnicodeDataAsSQLWCHAR( dbNameObj, &isNewBuffer );
        } else {
            return -1;
        }

#ifdef _WIN32
        cliLib = DLOPEN( LIBDB2 );
#else
        cliLib = DLOPEN( LIBDB2, RTLD_LAZY );
#endif
        if ( !cliLib ) {
            sprintf( (char *)msg, "Error in loading %s library file", LIBDB2 );
            PyErr_SetString( PyExc_Exception, (char *)msg );
            _python_clear_local_var( dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer );
            return -1;
        }
        Py_BEGIN_ALLOW_THREADS;
#ifdef _WIN32
        sqldropdb = DLSYM( cliLib, "SQLDropDbW" );
#else
        sqldropdb = (sqldropdbType)DLSYM( cliLib, "SQLDropDbW" );
#endif
        Py_END_ALLOW_THREADS;
        if ( sqldropdb == NULL)  {
#ifdef _WIN32
            sprintf( (char *)msg, "Not supported: This function is only supported from v97fp4 version of cli on window" );
#else
            sprintf( (char *)msg, "Not supported: This function is only supported from v97fp3 version of cli" );
#endif
            PyErr_SetString( PyExc_Exception, (char *)msg );
            DLCLOSE( cliLib );
            _python_clear_local_var( dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer );
            return -1;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = sqldropdb( conn_res->hdbc, dbName, SQL_NTS );
        Py_END_ALLOW_THREADS;

        DLCLOSE( cliLib );
        if ( rc != SQL_SUCCESS ) {
            if ( recreate ) {
                if ( SQLGetDiagRec( SQL_HANDLE_DBC, (SQLHDBC)conn_res->hdbc, 1, sqlstate, &sqlcode, msg, SQL_MAX_MESSAGE_LENGTH + 1, &length ) == SQL_SUCCESS ) {
                    if ( sqlcode == -1013 ) {
                        _python_clear_local_var( dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer );
                        return 0;
                    }
                }
            }
            _python_ibm_db_check_sql_errors( conn_res->hdbc, SQL_HANDLE_DBC, rc, 1, NULL, -1, 1 );
            return -1;
        }
        _python_clear_local_var( dbNameObj, dbName, NULL, NULL, NULL, NULL, isNewBuffer );
        return 0;
    } else {
        PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
        return -1;
    }
#endif
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
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    PyObject *codesetObj = NULL;
    PyObject *modeObj = NULL;
    int rc = -1;

    if ( !PyArg_ParseTuple( args, "OO|OO", &py_conn_res, &dbNameObj, &codesetObj, &modeObj ) ) {
        return NULL;
    }
    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
        PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
        return NULL;
    }
    rc = _python_ibm_db_createdb((conn_handle *)py_conn_res, dbNameObj, codesetObj, modeObj, 0);
    if ( rc == 0 ) {
        Py_RETURN_TRUE;
    } else {
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
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    int rc = -1;

    if ( !PyArg_ParseTuple( args, "OO", &py_conn_res, &dbNameObj ) ) {
        return NULL;
    }
    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
        PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
        return NULL;
    }
    rc = _python_ibm_db_dropdb( (conn_handle *)py_conn_res, dbNameObj, 0 );
    if ( rc == 0 ) {
        Py_RETURN_TRUE;
    } else {
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
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    PyObject *codesetObj = NULL;
    PyObject *modeObj = NULL;
    int rc = -1;

    if ( !PyArg_ParseTuple( args, "OO|OO", &py_conn_res, &dbNameObj, &codesetObj, &modeObj ) ) {
        return NULL;
    }
    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
        PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
        return NULL;
    }
    rc = _python_ibm_db_dropdb((conn_handle *)py_conn_res, dbNameObj, 1 );
    if ( rc != 0 ) {
        return NULL;
    }

    rc = _python_ibm_db_createdb((conn_handle *)py_conn_res, dbNameObj, codesetObj, modeObj, 0);
    if ( rc == 0 ) {
        Py_RETURN_TRUE;
    } else {
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
    PyObject *py_conn_res = NULL;
    PyObject *dbNameObj = NULL;
    PyObject *codesetObj = NULL;
    PyObject *modeObj = NULL;
    int rc = -1;

    if ( !PyArg_ParseTuple( args, "OO|OO", &py_conn_res, &dbNameObj, &codesetObj, &modeObj ) ) {
        return NULL;
    }
    if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
        PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
        return NULL;
    }
    rc = _python_ibm_db_createdb((conn_handle *)py_conn_res, dbNameObj, codesetObj, modeObj, 1);
    if ( rc == 0 ) {
        Py_RETURN_TRUE;
    } else {
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
    PyObject *py_autocommit = NULL;
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res = NULL;
    int rc;
    SQLINTEGER autocommit = -1;

    if (!PyArg_ParseTuple(args, "O|O", &py_conn_res, &py_autocommit)){
        return NULL;
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        if (!NIL_P(py_autocommit)) {
            if (PyInt_Check(py_autocommit)) {
                autocommit = (SQLINTEGER)PyInt_AsLong(py_autocommit);
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

      /* If value in handle is different from value passed in */
        if (PyTuple_Size(args) == 2) {
            if(autocommit != (conn_res->auto_commit)) {
                Py_BEGIN_ALLOW_THREADS;
#ifndef PASE
                rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) (autocommit == 0 ? SQL_AUTOCOMMIT_OFF : SQL_AUTOCOMMIT_ON), SQL_IS_INTEGER);
#else
                rc = SQLSetConnectAttr((SQLHDBC)conn_res->hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)&autocommit, SQL_IS_INTEGER);
#endif
                Py_END_ALLOW_THREADS;
                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                rc, 1, NULL, -1, 1);
                }
                conn_res->auto_commit = autocommit;
            }
            Py_INCREF(Py_True);
            return Py_True;
        } else {
            return PyInt_FromLong(conn_res->auto_commit);
        }
    }
    return NULL;
}

/*    static void _python_ibm_db_add_param_cache( stmt_handle *stmt_res, int param_no, PyObject *var_pyvalue, char *varname, int varname_len, int param_type, int size, SQLSMALLINT data_type, SQLSMALLINT precision, SQLSMALLINT scale, SQLSMALLINT nullable )
*/
static void _python_ibm_db_add_param_cache( stmt_handle *stmt_res, int param_no, PyObject *var_pyvalue, int param_type, int size, SQLSMALLINT data_type, SQLUINTEGER precision, SQLSMALLINT scale, SQLSMALLINT nullable )
{
    param_node *tmp_curr = NULL, *prev = stmt_res->head_cache_list, *curr = stmt_res->head_cache_list;

    while ( (curr != NULL) && (curr->param_num < param_no) ) {
        prev = curr;
        curr = curr->next;
    }

    if ( curr == NULL || curr->param_num != param_no ) {
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
        if ( param_type == PARAM_FILE) {
            stmt_res->file_param = 1;
        }

        if ( var_pyvalue != NULL) {
            Py_INCREF(var_pyvalue);
            tmp_curr->var_pyvalue = var_pyvalue;
        }

        /* link pointers for the list */
        if ( prev == NULL ) {
            stmt_res->head_cache_list = tmp_curr;
        } else {
            prev->next = tmp_curr;
        }
        tmp_curr->next = curr;

        /* Increment num params added */
        stmt_res->num_params++;
    } else {
        /* Both the nodes are for the same param no */
        /* Replace Information */
        curr->data_type = data_type;
        curr->param_size = precision;
        curr->nullable = nullable;
        curr->scale = scale;
        curr->param_num = param_no;
        curr->file_options = SQL_FILE_READ;
        curr->param_type = param_type;
        curr->size = size;

        /* Set this flag in stmt_res if a FILE INPUT is present */
        if ( param_type == PARAM_FILE) {
            stmt_res->file_param = 1;
        }

        if ( var_pyvalue != NULL) {
            Py_DECREF(curr->var_pyvalue);
            Py_INCREF(var_pyvalue);
            curr->var_pyvalue = var_pyvalue;
        }

    }
}

/*
 * static PyObject *_python_ibm_db_bind_param_helper(int argc, stmt_handle *stmt_res, SQLUSMALLINT param_no, PyObject *var_pyvalue, long param_type,
 *                      long data_type, long precision, long scale, long size)
 */
static PyObject *_python_ibm_db_bind_param_helper(int argc, stmt_handle *stmt_res, SQLUSMALLINT param_no, PyObject *var_pyvalue, long param_type, long data_type, long precision, long scale, long size)
{
    SQLSMALLINT sql_data_type = 0;
    SQLUINTEGER sql_precision = 0;
    SQLSMALLINT sql_scale = 0;
    SQLSMALLINT sql_nullable = SQL_NO_NULLS;
    char error[DB2_MAX_ERR_MSG_LEN];
    int rc = 0;

    /* Check for Param options */
    switch (argc) {
        /* if argc == 3, then the default value for param_type will be used */
        case 3:
            param_type = SQL_PARAM_INPUT;

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt, (SQLUSMALLINT)param_no, &sql_data_type, &sql_precision, &sql_scale, &sql_nullable);
            Py_END_ALLOW_THREADS;

            if( rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR )
            {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                                 SQL_HANDLE_STMT, rc, 1,
                                                 NULL, -1, 1);
            }
            if ( rc == SQL_ERROR ) {
                sprintf(error, "Describe Param Failed: %s",
                IBM_DB_G(__python_stmt_err_msg));
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }
            /* Add to cache */
            _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                        param_type, size,
                                        sql_data_type, sql_precision,
                                        sql_scale, sql_nullable );
            break;

        case 4:
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt,
                                (SQLUSMALLINT)param_no, &sql_data_type,
                                &sql_precision, &sql_scale, &sql_nullable);
            Py_END_ALLOW_THREADS;

            if( rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR )
            {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                                 SQL_HANDLE_STMT, rc, 1,
                                                 NULL, -1, 1);
            }
            if ( rc == SQL_ERROR ) {
                sprintf(error, "Describe Param Failed: %s",
                        IBM_DB_G(__python_stmt_err_msg));
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }
            /* Add to cache */
            _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                            param_type, size,
                                            sql_data_type, sql_precision,
                                            sql_scale, sql_nullable );
            break;

        case 5:
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt,
                                (SQLUSMALLINT)param_no, &sql_data_type,
                                &sql_precision, &sql_scale, &sql_nullable);
            Py_END_ALLOW_THREADS;

            if( rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR )
            {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                                 SQL_HANDLE_STMT, rc, 1,
                                                 NULL, -1, 1);
            }
            if ( rc == SQL_ERROR ) {
                sprintf(error, "Describe Param Failed: %s",
                                IBM_DB_G(__python_stmt_err_msg));
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }
            sql_data_type = (SQLSMALLINT)data_type;
            /* Add to cache */
            _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                            param_type, size,
                                            sql_data_type, sql_precision,
                                            sql_scale, sql_nullable );
            break;

        case 6:
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt,
                                (SQLUSMALLINT)param_no, &sql_data_type,
                            &sql_precision, &sql_scale, &sql_nullable);
            Py_END_ALLOW_THREADS;

            if( rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR )
            {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                                 SQL_HANDLE_STMT, rc, 1,
                                                 NULL, -1, 1);
            }
            if ( rc == SQL_ERROR ) {
                sprintf(error, "Describe Param Failed: %s",
                        IBM_DB_G(__python_stmt_err_msg));
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }
            sql_data_type = (SQLSMALLINT)data_type;
            sql_precision = (SQLUINTEGER)precision;
            /* Add to cache */
            _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                            param_type, size,
                                            sql_data_type, sql_precision,
                                            sql_scale, sql_nullable );
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
            sql_data_type = (SQLSMALLINT)data_type;
            sql_precision = (SQLUINTEGER)precision;
            sql_scale = (SQLSMALLINT)scale;
            _python_ibm_db_add_param_cache(stmt_res, param_no, var_pyvalue,
                                            param_type, size,
                                            sql_data_type, sql_precision,
                                            sql_scale, sql_nullable );
            break;

        default:
            /* WRONG_PARAM_COUNT; */
            return NULL;
    }
    /* end Switch */

    /* We bind data with DB2 CLI in ibm_db.execute() */
    /* This will save network flow if we need to override params in it */

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
                        &py_scale, &py_size)) {

        return NULL;
    }

    if (!NIL_P(py_param_no)) {
        if (PyInt_Check(py_param_no)) {
            param_no = (SQLUSMALLINT) PyInt_AsLong(py_param_no);
        } else {
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            return NULL;
        }
    }
    if (py_param_type != NULL && py_param_type != Py_None &&
        TYPE(py_param_type) == PYTHON_FIXNUM) {
        param_type = PyInt_AS_LONG(py_param_type);
    }

    if (py_data_type != NULL && py_data_type != Py_None &&
        TYPE(py_data_type) == PYTHON_FIXNUM) {
        data_type = PyInt_AS_LONG(py_data_type);
    }

    if (py_precision != NULL && py_precision != Py_None &&
        TYPE(py_precision) == PYTHON_FIXNUM) {
        precision = PyInt_AS_LONG(py_precision);
    }

    if (py_scale != NULL && py_scale != Py_None &&
        TYPE(py_scale) == PYTHON_FIXNUM) {
        scale = PyInt_AS_LONG(py_scale);
    }

    if (py_size != NULL && py_size != Py_None &&
        TYPE(py_size) == PYTHON_FIXNUM) {
        size = PyInt_AS_LONG(py_size);
    }

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        return _python_ibm_db_bind_param_helper(PyTuple_Size(args), stmt_res, param_no, var_pyvalue, param_type, data_type, precision, scale, size);
    } else {
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
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res = NULL;
    int rc;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        /* Check to see if it's a persistent connection;
         * if so, just return true
        */

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        if ( conn_res->handle_active && !conn_res->flag_pconnect ) {
            /* Disconnect from DB. If stmt is allocated,
            * it is freed automatically
            */
            if (conn_res->auto_commit == 0) {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLEndTran(SQL_HANDLE_DBC, (SQLHDBC)conn_res->hdbc,
                                SQL_ROLLBACK);
                Py_END_ALLOW_THREADS;
                if ( rc == SQL_ERROR ) {
                    _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                                                    rc, 1, NULL, -1, 1);
                    return NULL;
                }
            }
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLDisconnect((SQLHDBC)conn_res->hdbc);
            Py_END_ALLOW_THREADS;
            if( rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR )
            {
                _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                 SQL_HANDLE_DBC, rc, 1,
                                                 NULL, -1, 1);
            }
            if ( rc == SQL_ERROR ) {
                return NULL;
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeHandle(SQL_HANDLE_DBC, conn_res->hdbc);
            Py_END_ALLOW_THREADS;

            if( rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR )
            {
                _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                 SQL_HANDLE_DBC, rc, 1,
                                                 NULL, -1, 1);
            }

            if ( rc == SQL_ERROR ) {

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
                Py_END_ALLOW_THREADS;
                return NULL;
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeHandle(SQL_HANDLE_ENV, conn_res->henv);
            Py_END_ALLOW_THREADS;
            if( rc == SQL_SUCCESS_WITH_INFO || rc == SQL_ERROR )
            {
                _python_ibm_db_check_sql_errors( conn_res->henv,
                                                 SQL_HANDLE_ENV, rc, 1,
                                                 NULL, -1, 1);
            }

            if ( rc == SQL_ERROR ) {
                return NULL;
            }

            conn_res->handle_active = 0;
            Py_INCREF(Py_True);
            return Py_True;
        } else if ( conn_res->flag_pconnect ) {
            /* Do we need to call FreeStmt or something to close cursors? */
            Py_INCREF(Py_True);
            return Py_True;
        } else {
            return NULL;
        }
    } else {
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
    int rc;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_table_name, &py_column_name))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None) {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name)){
            py_table_name = PyUnicode_FromObject(py_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_column_name != NULL && py_column_name != Py_None) {
        if (PyString_Check(py_column_name) || PyUnicode_Check(py_table_name)){
            py_column_name = PyUnicode_FromObject(py_column_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "column_name must be a string");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
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
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None )
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLColumnPrivilegesW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
                            owner, SQL_NTS, table_name, SQL_NTS, column_name,
                            SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(table_name) PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                            SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
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

        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_column_name);

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
    int rc;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_table_name, &py_column_name))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None) {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name)){
            py_table_name = PyUnicode_FromObject(py_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_column_name != NULL && py_column_name != Py_None) {
        if (PyString_Check(py_column_name) || PyUnicode_Check(py_table_name)){
            py_column_name = PyUnicode_FromObject(py_column_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "column_name must be a string");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
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
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }

        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None )
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);
        if (py_column_name && py_column_name != Py_None )
            column_name = getUnicodeDataAsSQLWCHAR(py_column_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLColumnsW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
            owner,SQL_NTS, table_name, SQL_NTS, column_name, SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(table_name) PyMem_Del(table_name);
            if(column_name) PyMem_Del(column_name);
        }

        if (rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
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
        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_column_name);

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
    SQLWCHAR *pk_qualifier = NULL;
    SQLWCHAR *pk_owner = NULL;
    SQLWCHAR *pk_table_name = NULL;
    SQLWCHAR *fk_qualifier = NULL;
    SQLWCHAR *fk_owner = NULL;
    SQLWCHAR *fk_table_name = NULL;
    int rc;
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
        return NULL;

    if (py_pk_qualifier != NULL && py_pk_qualifier != Py_None) {
        if (PyString_Check(py_pk_qualifier) || PyUnicode_Check(py_pk_qualifier)){
            py_pk_qualifier = PyUnicode_FromObject(py_pk_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception,
                "qualifier for table containing primary key must be a string or unicode");
            return NULL;
        }
    }

    if (py_pk_owner != NULL && py_pk_owner != Py_None) {
        if (PyString_Check(py_pk_owner) || PyUnicode_Check(py_pk_owner)){
            py_pk_owner = PyUnicode_FromObject(py_pk_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception,
                "owner of table containing primary key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            return NULL;
        }
    }

    if (py_pk_table_name != NULL && py_pk_table_name != Py_None) {
        if (PyString_Check(py_pk_table_name) || PyUnicode_Check(py_pk_table_name)){
            py_pk_table_name = PyUnicode_FromObject(py_pk_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception,
                "name of the table that contains primary key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            return NULL;
        }
    }

    if (py_fk_qualifier != NULL && py_fk_qualifier != Py_None) {
        if (PyString_Check(py_fk_qualifier) || PyUnicode_Check(py_fk_qualifier)){
            py_fk_qualifier = PyUnicode_FromObject(py_fk_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception,
                "qualifier for table containing the foreign key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            return NULL;
        }
    }

    if (py_fk_owner != NULL && py_fk_owner != Py_None) {
        if (PyString_Check(py_fk_owner) || PyUnicode_Check(py_fk_owner)){
            py_fk_owner = PyUnicode_FromObject(py_fk_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception,
                "owner of table containing the foreign key must be a string or unicode");
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            Py_XDECREF(py_fk_qualifier);
            return NULL;
        }
    }

    if (py_fk_table_name != NULL && py_fk_table_name != Py_None) {
        if (PyString_Check(py_fk_table_name) || PyUnicode_Check(py_fk_table_name)){
            py_fk_table_name = PyUnicode_FromObject(py_fk_table_name);
        }
        else {
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

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
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

        if (rc == SQL_ERROR) {
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

        if(py_pk_qualifier && py_pk_qualifier != Py_None)
            pk_qualifier = getUnicodeDataAsSQLWCHAR(py_pk_qualifier, &isNewBuffer);
        if(py_pk_owner && py_pk_owner != Py_None)
            pk_owner = getUnicodeDataAsSQLWCHAR(py_pk_owner, &isNewBuffer);
        if(py_pk_table_name && py_pk_table_name != Py_None)
            pk_table_name = getUnicodeDataAsSQLWCHAR(py_pk_table_name, &isNewBuffer);
        if(py_fk_qualifier && py_fk_qualifier != Py_None)
            fk_qualifier = getUnicodeDataAsSQLWCHAR(py_fk_qualifier, &isNewBuffer);
        if(py_fk_owner && py_fk_owner != Py_None)
            fk_owner = getUnicodeDataAsSQLWCHAR(py_fk_owner, &isNewBuffer);
        if(py_fk_table_name && py_fk_table_name != Py_None)
            fk_table_name = getUnicodeDataAsSQLWCHAR(py_fk_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLForeignKeysW((SQLHSTMT)stmt_res->hstmt, pk_qualifier, SQL_NTS,
                        pk_owner, SQL_NTS, pk_table_name, SQL_NTS, fk_qualifier, SQL_NTS,
                        fk_owner, SQL_NTS, fk_table_name, SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(pk_qualifier) PyMem_Del(pk_qualifier);
            if(pk_owner) PyMem_Del(pk_owner);
            if(pk_table_name) PyMem_Del(pk_table_name);
            if(fk_qualifier) PyMem_Del(fk_qualifier);
            if(fk_owner) PyMem_Del(fk_owner);
            if(fk_table_name) PyMem_Del(fk_table_name);
        }

        if (rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            Py_XDECREF(py_pk_qualifier);
            Py_XDECREF(py_pk_owner);
            Py_XDECREF(py_pk_table_name);
            Py_XDECREF(py_fk_qualifier);
            Py_XDECREF(py_fk_owner);
            Py_XDECREF(py_fk_table_name);
            PyErr_Clear( );
            Py_RETURN_FALSE;
        }
        Py_XDECREF(py_pk_qualifier);
        Py_XDECREF(py_pk_owner);
        Py_XDECREF(py_pk_table_name);
        Py_XDECREF(py_fk_qualifier);
        Py_XDECREF(py_fk_owner);
        Py_XDECREF(py_fk_table_name);
        return (PyObject *)stmt_res;

    } else {
        Py_XDECREF(py_pk_qualifier);
        Py_XDECREF(py_pk_owner);
        Py_XDECREF(py_pk_table_name);
        Py_XDECREF(py_fk_qualifier);
        Py_XDECREF(py_fk_owner);
        Py_XDECREF(py_fk_table_name);
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
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    int rc;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "OOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_table_name))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None) {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name)){
            py_table_name = PyUnicode_FromObject(py_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None )
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLPrimaryKeysW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
            owner, SQL_NTS, table_name, SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(table_name) PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR ) {
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

        return (PyObject *)stmt_res;

    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);

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
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *proc_name = NULL;
    SQLWCHAR *column_name = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_proc_name = NULL;
    PyObject *py_column_name = NULL;
    PyObject *py_conn_res = NULL;
    int rc = 0;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_proc_name, &py_column_name))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_proc_name != NULL && py_proc_name != Py_None) {
        if (PyString_Check(py_proc_name) || PyUnicode_Check(py_proc_name)){
            py_proc_name = PyUnicode_FromObject(py_proc_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_column_name != NULL && py_column_name != Py_None) {
        if (PyString_Check(py_column_name) || PyUnicode_Check(py_column_name)){
            py_column_name = PyUnicode_FromObject(py_column_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "column_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            Py_XDECREF(py_column_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            Py_XDECREF(py_column_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_proc_name && py_proc_name != Py_None )
            proc_name = getUnicodeDataAsSQLWCHAR(py_proc_name, &isNewBuffer);
        if (py_column_name && py_column_name != Py_None )
            column_name = getUnicodeDataAsSQLWCHAR(py_column_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLProcedureColumnsW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
            owner, SQL_NTS, proc_name, SQL_NTS, column_name,
            SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(proc_name) PyMem_Del(proc_name);
            if(column_name) PyMem_Del(column_name);
        }

        if (rc == SQL_ERROR ) {
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

        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_proc_name);
        Py_XDECREF(py_column_name);

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
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *proc_name = NULL;
    int rc = 0;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_proc_name = NULL;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "OOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_proc_name))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_proc_name != NULL && py_proc_name != Py_None) {
        if (PyString_Check(py_proc_name) || PyUnicode_Check(py_proc_name)){
            py_proc_name = PyUnicode_FromObject(py_proc_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }


    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_proc_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_proc_name && py_proc_name != Py_None )
            proc_name = getUnicodeDataAsSQLWCHAR(py_proc_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLProceduresW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS, owner,
            SQL_NTS, proc_name, SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(proc_name) PyMem_Del(proc_name);
        }

        if (rc == SQL_ERROR ) {
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
        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_proc_name);

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
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    int scope = 0;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int rc;
    PyObject *py_conn_res = NULL;
    PyObject *py_scope = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "OOOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_table_name, &py_scope))
        return NULL;

    if (!NIL_P(py_scope)) {
        if (PyInt_Check(py_scope)) {
            scope = (int) PyInt_AsLong(py_scope);
        } else {
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            return NULL;
        }
    }
    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None) {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name)){
            py_table_name = PyUnicode_FromObject(py_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None )
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLSpecialColumnsW((SQLHSTMT)stmt_res->hstmt, SQL_BEST_ROWID,
            qualifier, SQL_NTS, owner, SQL_NTS, table_name,
            SQL_NTS, scope, SQL_NULLABLE);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(table_name) PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR ) {
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

        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);

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
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    int unique = 0;
    int rc = 0;
    SQLUSMALLINT sql_unique;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    PyObject *py_unique = NULL;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "OOOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_table_name, &py_unique))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None) {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name)){
            py_table_name = PyUnicode_FromObject(py_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_unique != NULL && py_unique != Py_None) {
        if (PyBool_Check(py_unique)) {
            if (py_unique == Py_True)
                unique = 1;
            else
                unique = 0;
        }
        else {
            PyErr_SetString(PyExc_Exception, "unique must be a boolean");
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);
        sql_unique = unique;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None )
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLStatisticsW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS, owner,
            SQL_NTS, table_name, SQL_NTS, sql_unique, SQL_QUICK);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(table_name) PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR ) {
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

        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);

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
    SQLWCHAR *qualifier = NULL;
    SQLWCHAR *owner = NULL;
    SQLWCHAR *table_name = NULL;
    conn_handle *conn_res;
    stmt_handle *stmt_res;
    int rc;
    PyObject *py_conn_res = NULL;
    PyObject *py_qualifier = NULL;
    PyObject *py_owner = NULL;
    PyObject *py_table_name = NULL;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "O|OOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_table_name))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None) {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name)){
            py_table_name = PyUnicode_FromObject(py_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }

        if (!conn_res) {
            PyErr_SetString(PyExc_Exception,"Connection Resource cannot be found");
            Py_RETURN_FALSE;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None )
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLTablePrivilegesW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS,
            owner, SQL_NTS, table_name, SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(table_name) PyMem_Del(table_name);
        }

        if (rc == SQL_ERROR ) {
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

        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);

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
    int rc;
    int isNewBuffer;

    if (!PyArg_ParseTuple(args, "O|OOOO", &py_conn_res, &py_qualifier, &py_owner,
        &py_table_name, &py_table_type))
        return NULL;

    if (py_qualifier != NULL && py_qualifier != Py_None) {
        if (PyString_Check(py_qualifier) || PyUnicode_Check(py_qualifier)){
            py_qualifier = PyUnicode_FromObject(py_qualifier);
        }
        else {
            PyErr_SetString(PyExc_Exception, "qualifier must be a string or unicode");
            return NULL;
        }
    }

    if (py_owner != NULL && py_owner != Py_None) {
        if (PyString_Check(py_owner) || PyUnicode_Check(py_owner)){
            py_owner = PyUnicode_FromObject(py_owner);
        }
        else {
            PyErr_SetString(PyExc_Exception, "owner must be a string or unicode");
            Py_XDECREF(py_qualifier);
            return NULL;
        }
    }

    if (py_table_name != NULL && py_table_name != Py_None) {
        if (PyString_Check(py_table_name) || PyUnicode_Check(py_table_name)){
            py_table_name = PyUnicode_FromObject(py_table_name);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table_name must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            return NULL;
        }
    }

    if (py_table_type != NULL && py_table_type != Py_None) {
        if (PyString_Check(py_table_type) || PyUnicode_Check(py_table_type)){
            py_table_type = PyUnicode_FromObject(py_table_type);
        }
        else {
            PyErr_SetString(PyExc_Exception, "table type must be a string or unicode");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_table_type);
            return NULL;
        }

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if (rc == SQL_ERROR) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            Py_XDECREF(py_qualifier);
            Py_XDECREF(py_owner);
            Py_XDECREF(py_table_name);
            Py_XDECREF(py_table_type);

            Py_RETURN_FALSE;
        }
        if (py_qualifier && py_qualifier != Py_None )
            qualifier = getUnicodeDataAsSQLWCHAR(py_qualifier, &isNewBuffer);
        if (py_owner &&  py_owner != Py_None )
            owner = getUnicodeDataAsSQLWCHAR(py_owner, &isNewBuffer);
        if (py_table_name && py_table_name != Py_None )
            table_name = getUnicodeDataAsSQLWCHAR(py_table_name, &isNewBuffer);
        if(py_table_type && py_table_type != Py_None)
            table_type = getUnicodeDataAsSQLWCHAR(py_table_type, &isNewBuffer);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLTablesW((SQLHSTMT)stmt_res->hstmt, qualifier, SQL_NTS, owner,
            SQL_NTS, table_name, SQL_NTS, table_type, SQL_NTS);
        Py_END_ALLOW_THREADS;

        if (isNewBuffer) {
            if(qualifier) PyMem_Del(qualifier);
            if(owner) PyMem_Del(owner);
            if(table_name) PyMem_Del(table_name);
            if(table_type) PyMem_Del(table_type);
        }

        if (rc == SQL_ERROR ) {
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

        return (PyObject *)stmt_res;
    } else {
        Py_XDECREF(py_qualifier);
        Py_XDECREF(py_owner);
        Py_XDECREF(py_table_name);
        Py_XDECREF(py_table_type);

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
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res;
    int rc;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLEndTran(SQL_HANDLE_DBC, conn_res->hdbc, SQL_COMMIT);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                             NULL, -1, 1);
            Py_INCREF(Py_False);
            return Py_False;
        } else {
            Py_INCREF(Py_True);
            return Py_True;
        }
    }
    Py_INCREF(Py_False);
    return Py_False;
}

/* static int _python_ibm_db_do_prepare(SQLHANDLE hdbc, char *stmt_string, stmt_handle *stmt_res, PyObject *options)
*/
static int _python_ibm_db_do_prepare(SQLHANDLE hdbc, SQLWCHAR *stmt, int stmt_size, stmt_handle *stmt_res, PyObject *options)
{
    int rc;
    /* alloc handle and return only if it errors */
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &(stmt_res->hstmt));
    Py_END_ALLOW_THREADS;
    if ( rc == SQL_ERROR ) {
         _python_ibm_db_check_sql_errors(hdbc, SQL_HANDLE_DBC, rc,
                                        1, NULL, -1, 1);
        return rc;
    }

    /* get the string and its length */
    if (NIL_P(stmt)) {
        PyErr_SetString(PyExc_Exception,
            "Supplied statement parameter is invalid");
        return rc;
    }

    if ( rc < SQL_SUCCESS ) {
        _python_ibm_db_check_sql_errors(hdbc, SQL_HANDLE_DBC, rc, 1, NULL, -1, 1);
        PyErr_SetString(PyExc_Exception, "Statement prepare Failed: ");
        return rc;
    }

    if (!NIL_P(options)) {
        rc = _python_ibm_db_parse_options( options, SQL_HANDLE_STMT, stmt_res );
        if ( rc == SQL_ERROR ) {
            return rc;
        }
    }

    /* Prepare the stmt. The cursor type requested has already been set in
    * _python_ibm_db_assign_options
    */

    Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
    rc = SQLPrepareW((SQLHSTMT)stmt_res->hstmt, stmt,
                stmt_size*2);
#else
    rc = SQLPrepareW((SQLHSTMT)stmt_res->hstmt, stmt,
		stmt_size);
#endif
    Py_END_ALLOW_THREADS;

    if ( rc == SQL_ERROR ) {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                        1, NULL, -1, 1);
    }
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
    PyObject *options = NULL;
    PyObject *py_conn_res = NULL;
    stmt_handle *stmt_res;
    conn_handle *conn_res;
    int rc;
    int isNewBuffer;
    char* return_str = NULL; /* This variable is used by
                             * _python_ibm_db_check_sql_errors to return err
                             * strings
                             */
    SQLWCHAR *stmt = NULL;
    PyObject *py_stmt = NULL;

    /* This function basically is a wrap of the _python_ibm_db_do_prepare and
    * _python_ibm_db_Execute_stmt
    * After completing statement execution, it returns the statement resource
    */

    if (!PyArg_ParseTuple(args, "OO|O", &py_conn_res, &py_stmt,  &options))
        return NULL;

    if (py_stmt != NULL && py_stmt != Py_None) {
        if (PyString_Check(py_stmt) || PyUnicode_Check(py_stmt)){
            py_stmt = PyUnicode_FromObject(py_stmt);
        }
        else {
            PyErr_SetString(PyExc_Exception, "statement must be a string or unicode");
            return NULL;
        }
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            Py_XDECREF(py_stmt);
            return NULL;
        }

        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        if ( return_str == NULL ) {
            PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
            Py_XDECREF(py_stmt);
            return NULL;
        }

        memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);

        _python_ibm_db_clear_stmt_err_cache();

        stmt_res = _ibm_db_new_stmt_struct(conn_res);

        /* Allocates the stmt handle */
        /* returns the stat_handle back to the calling function */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, conn_res->hdbc, &(stmt_res->hstmt));
        Py_END_ALLOW_THREADS;
        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            PyMem_Del(return_str);
            Py_XDECREF(py_stmt);
            return NULL;
        }

        if (!NIL_P(options)) {
            rc = _python_ibm_db_parse_options(options, SQL_HANDLE_STMT, stmt_res);
            if ( rc == SQL_ERROR ) {
                Py_XDECREF(py_stmt);
                return NULL;
            }
        }
        if (py_stmt != NULL && py_stmt != Py_None){
            stmt = getUnicodeDataAsSQLWCHAR(py_stmt, &isNewBuffer);
             }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLExecDirectW((SQLHSTMT)stmt_res->hstmt, stmt, SQL_NTS);
        Py_END_ALLOW_THREADS;
        if ( rc < SQL_SUCCESS ) {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, -1,
                1, return_str, DB2_ERRMSG,
                stmt_res->errormsg_recno_tracker);
            Py_BEGIN_ALLOW_THREADS;
            SQLFreeHandle( SQL_HANDLE_STMT, stmt_res->hstmt );
            Py_END_ALLOW_THREADS;
            /* TODO: Object freeing */
            /* free(stmt_res); */
            if (isNewBuffer) {
                if(stmt) PyMem_Del(stmt);
            }
            Py_XDECREF(py_stmt);
            PyMem_Del(return_str);
            return NULL;
        }
        if ( rc == SQL_SUCCESS_WITH_INFO )
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, 1,
                                             1, return_str, DB2_WARNMSG,
                                            stmt_res->errormsg_recno_tracker);
        }
        if (isNewBuffer) {
            if(stmt) PyMem_Del(stmt);
        }
        PyMem_Del(return_str);
        Py_XDECREF(py_stmt);
        return (PyObject *)stmt_res;
    }
    Py_XDECREF(py_stmt);
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
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        if ( stmt_res->hstmt ) {
            /* Free any cursors that might have been allocated in a previous call
            * to SQLExecute
            */
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFreeStmt((SQLHSTMT)stmt_res->hstmt, SQL_CLOSE);
            Py_END_ALLOW_THREADS;
            if ( rc == SQL_ERROR ||  rc == SQL_SUCCESS_WITH_INFO )
            {
               _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            if ( rc == SQL_ERROR ) {
                PyErr_Clear( );
                Py_RETURN_FALSE;
            }
        }
        _python_ibm_db_free_result_struct(stmt_res);
    } else {
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
    Py_INCREF(Py_True);
    return Py_True;
}

/*
 * static PyObject *_python_ibm_db_prepare_helper(conn_handle *conn_res, PyObject *py_stmt, PyObject *options)
 *
 */
static PyObject *_python_ibm_db_prepare_helper(conn_handle *conn_res, PyObject *py_stmt, PyObject *options)
{
    stmt_handle *stmt_res;
    int rc;
    char error[DB2_MAX_ERR_MSG_LEN];
    SQLWCHAR *stmt = NULL;
    int stmt_size = 0;
    int isNewBuffer;

    if (!conn_res->handle_active) {
        PyErr_SetString(PyExc_Exception, "Connection is not active");
        return NULL;
    }

    if (py_stmt != NULL && py_stmt != Py_None) {
        if (PyString_Check(py_stmt) || PyUnicode_Check(py_stmt)) {
            py_stmt = PyUnicode_FromObject(py_stmt);
            if (py_stmt != NULL &&  py_stmt != Py_None) {
                stmt_size = PyUnicode_GetSize(py_stmt);
            } else {
                PyErr_SetString(PyExc_Exception, "Error occure during processing of statement");
                return NULL;
            }
        }
        else {
            PyErr_SetString(PyExc_Exception, "statement must be a string or unicode");
            return NULL;
        }
    }

    _python_ibm_db_clear_stmt_err_cache();

    /* Initialize stmt resource members with default values. */
    /* Parsing will update options if needed */

    stmt_res = _ibm_db_new_stmt_struct(conn_res);

    /* Allocates the stmt handle */
    /* Prepares the statement */
    /* returns the stat_handle back to the calling function */
    if( py_stmt && py_stmt != Py_None)
        stmt = getUnicodeDataAsSQLWCHAR(py_stmt, &isNewBuffer);

    rc = _python_ibm_db_do_prepare(conn_res->hdbc, stmt, stmt_size, stmt_res, options);
    if (isNewBuffer) {
        if(stmt) PyMem_Del(stmt);
    }

    if ( rc < SQL_SUCCESS ) {
        sprintf(error, "Statement Prepare Failed: %s", IBM_DB_G(__python_stmt_err_msg));
        Py_XDECREF(py_stmt);
        return NULL;
    }
    Py_XDECREF(py_stmt);
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
    PyObject *py_conn_res = NULL;
    PyObject *options = NULL;
    conn_handle *conn_res;

    PyObject *py_stmt = NULL;

    if (!PyArg_ParseTuple(args, "OO|O", &py_conn_res, &py_stmt, &options))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        return _python_ibm_db_prepare_helper(conn_res, py_stmt, options);
    }

    return NULL;
}

/*    static param_node* build_list( stmt_res, param_no, data_type, precision, scale, nullable )
*/
static param_node* build_list( stmt_handle *stmt_res, int param_no, SQLSMALLINT data_type, SQLUINTEGER precision, SQLSMALLINT scale, SQLSMALLINT nullable )
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

    while ( curr != NULL ) {
        prev = curr;
        curr = curr->next;
    }

    if (stmt_res->head_cache_list == NULL) {
        stmt_res->head_cache_list = tmp_curr;
    } else {
        prev->next = tmp_curr;
    }

    tmp_curr->next = curr;

    return tmp_curr;
}

/*    static int _python_ibm_db_bind_data( stmt_handle *stmt_res, param_node *curr, PyObject *bind_data )
*/
static int _python_ibm_db_bind_data( stmt_handle *stmt_res, param_node *curr, PyObject *bind_data)
{
    int rc,i;
    SQLSMALLINT valueType = 0;
    SQLPOINTER    paramValuePtr;
    SQLWCHAR *tmp_uvalue = NULL;
    SQLWCHAR *dest_uvalue = NULL;
    char *tmp_svalue = NULL;
    char *dest_svalue = NULL;
#if  PY_MAJOR_VERSION < 3
    Py_ssize_t buffer_len = 0;
#endif
    int param_length;
    int type = PYTHON_NIL;
    PyObject* item;

    /* Have to use SQLBindFileToParam if PARAM is type PARAM_FILE */
    /*** Need to fix this***/
    if ( curr->param_type == PARAM_FILE) {
        PyObject *FileNameObj = NULL;
        /* Only string types can be bound */
        if (PyString_Check(bind_data)) {
            if (PyUnicode_Check(bind_data)) {
                FileNameObj = PyUnicode_AsASCIIString(bind_data);
                if (FileNameObj == NULL) {
                    return SQL_ERROR;
                }
            }
        } else {
            return SQL_ERROR;
        }
        curr->bind_indicator = 0;
        if(curr->svalue != NULL) {
            PyMem_Del(curr->svalue);
            curr->svalue = NULL;
        }
        if (FileNameObj != NULL) {
            curr->svalue = PyBytes_AsString(FileNameObj);
        } else {
            curr->svalue = PyBytes_AsString(bind_data);
        }
        curr->ivalue = strlen(curr->svalue);
        curr->svalue = memcpy(PyMem_Malloc((sizeof(char))*(curr->ivalue+1)), curr->svalue, curr->ivalue);
        curr->svalue[curr->ivalue] = '\0';
        Py_XDECREF(FileNameObj);
        valueType = (SQLSMALLINT) curr->ivalue;
        /* Bind file name string */

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLBindFileToParam((SQLHSTMT)stmt_res->hstmt, curr->param_num,
                    curr->data_type, (SQLCHAR*)curr->svalue,
                    (SQLSMALLINT*)&(curr->ivalue), &(curr->file_options),
                    (SQLSMALLINT) curr->ivalue, &(curr->bind_indicator));
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
            _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                             rc, 1, NULL, -1, 1);
        }
        return rc;
    }

    type = TYPE(bind_data);
    if(type == PYTHON_LIST)
    {
    	item = PyList_GetItem(bind_data, 0);
        type = TYPE(item);
    }

    switch(type) {
        case PYTHON_FIXNUM:
	  /* BIGINT_IS_SHORTER_THAN_LONG: Avoid SQLCODE=22005: In xlc with -q64, the size of BIGINT is the same as the size of long */
	  if(BIGINT_IS_SHORTER_THAN_LONG && (curr->data_type == SQL_BIGINT || curr->data_type == SQL_DECIMAL )){
#if  PY_MAJOR_VERSION >= 3
                PyObject *tempobj2 = NULL;
#endif
                PyObject *tempobj = NULL;
                if(TYPE(bind_data) == PYTHON_LIST)
                {
                    char *svalue = NULL;
                    Py_ssize_t n = PyList_Size(bind_data);
                    curr->svalue = (char *)ALLOC_N(char, (MAX_PRECISION) * (n));
                    memset(curr->svalue , 0, MAX_PRECISION * n);
                    for (i = 0; i < n; i++)
                    {
                        item = PyList_GetItem(bind_data, i);
                        item = PyObject_Str(item);
#if  PY_MAJOR_VERSION >= 3
                        tempobj2 = PyUnicode_AsASCIIString(item);
                        Py_XDECREF(item);
                        item = tempobj2;
#endif
                        svalue = PyBytes_AsString(item);
                        curr->ivalue = strlen(svalue);
                        memcpy(curr->svalue + (i * MAX_PRECISION), svalue, curr->ivalue);
                        svalue = NULL;
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                curr->param_type, SQL_C_CHAR, curr->data_type,
                                MAX_PRECISION, curr->scale, curr->svalue, MAX_PRECISION, NULL);
                    Py_END_ALLOW_THREADS;

                }
                else
                {
#if  PY_MAJOR_VERSION >= 3
                       PyObject *tempobj2 = NULL;
#endif
                    tempobj = PyObject_Str(bind_data);
#if  PY_MAJOR_VERSION >= 3
                    tempobj2 = PyUnicode_AsASCIIString(tempobj);
                    Py_XDECREF(tempobj);
                    tempobj = tempobj2;
#endif
                    curr->svalue = PyBytes_AsString(tempobj);
                    curr->ivalue = strlen(curr->svalue);
                    curr->svalue = memcpy(PyMem_Malloc((sizeof(char))*(curr->ivalue+1)), curr->svalue, curr->ivalue);
                    curr->svalue[curr->ivalue] = '\0';
                    curr->bind_indicator = curr->ivalue;

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                curr->param_type, SQL_C_CHAR, curr->data_type,
                                curr->param_size, curr->scale, curr->svalue, curr->param_size, NULL);
                    Py_END_ALLOW_THREADS;
                }

                if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ){
                    _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
                }
                Py_XDECREF(tempobj);
            }
            else{
                if(TYPE(bind_data) == PYTHON_LIST)
                {
                    Py_ssize_t n = PyList_Size(bind_data);
                    curr->ivalueArray = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
                    for (i = 0; i < n; i++)
                    {
                        item = PyList_GetItem(bind_data, i);
                        curr->ivalueArray[i] = PyLong_AsLong(item);
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                curr->param_type, SQL_C_LONG, curr->data_type,
                                curr->param_size, curr->scale, curr->ivalueArray, 0, NULL);
                    Py_END_ALLOW_THREADS;
                }
                else
                {
                    curr->ivalue = (SQLINTEGER) PyLong_AsLong(bind_data);

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                                curr->param_type, SQL_C_LONG, curr->data_type,
                                curr->param_size, curr->scale, &curr->ivalue, 0, NULL);
                    Py_END_ALLOW_THREADS;
                }

                if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                    _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
                }
                curr->data_type = SQL_C_LONG;
            }
            break;

        /* Convert BOOLEAN types to LONG for DB2 / Cloudscape */
        case PYTHON_FALSE:
            if(TYPE(bind_data) == PYTHON_LIST)
            {
                Py_ssize_t n = PyList_Size(bind_data);
                curr->ivalueArray = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    curr->ivalueArray[i] = PyLong_AsLong(item);
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                        curr->param_type, SQL_C_LONG, curr->data_type,
                        curr->param_size, curr->scale, curr->ivalueArray, 0, NULL);
                Py_END_ALLOW_THREADS;
            }
            else
            {
                curr->ivalue = 0;

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                            curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size,
                            curr->scale, &curr->ivalue, 0, NULL);
                Py_END_ALLOW_THREADS;
            }

            if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                    rc, 1, NULL, -1, 1);
            }

            curr->data_type = SQL_C_LONG;
            break;

        case PYTHON_TRUE:
            if(TYPE(bind_data) == PYTHON_LIST)
            {
                Py_ssize_t n = PyList_Size(bind_data);
                curr->ivalueArray = (SQLINTEGER *)ALLOC_N(SQLINTEGER, n);
                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    curr->ivalueArray[i] = PyLong_AsLong(item);
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                            curr->param_type, SQL_C_LONG, curr->data_type,
                            curr->param_size, curr->scale, curr->ivalueArray, 0, NULL);
                Py_END_ALLOW_THREADS;
            }
            else
            {
                curr->ivalue = 1;

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                            curr->param_type, SQL_C_LONG, curr->data_type, curr->param_size,
                            curr->scale, &curr->ivalue, 0, NULL);
                Py_END_ALLOW_THREADS;
            }

            if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            curr->data_type = SQL_C_LONG;
            break;

        case PYTHON_FLOAT:
            if(TYPE(bind_data) == PYTHON_LIST)
            {
                Py_ssize_t n = PyList_Size(bind_data);
                curr->fvalueArray = (double *)ALLOC_N(double, n);
                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    curr->fvalueArray[i] = PyFloat_AsDouble(item);
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                        curr->param_type, SQL_C_DOUBLE, curr->data_type,
                        curr->param_size, curr->scale, curr->fvalueArray, 0, NULL);
                Py_END_ALLOW_THREADS;
            }
            else
            {
                curr->fvalue = PyFloat_AsDouble(bind_data);

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                            curr->param_type, SQL_C_DOUBLE, curr->data_type, curr->param_size,
                            curr->scale, &curr->fvalue, 0, NULL);
                Py_END_ALLOW_THREADS;
            }

            if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
            curr->data_type = SQL_C_DOUBLE;
            break;

        case PYTHON_UNICODE:
            {
                /* To Bind array of values */
                if(TYPE(bind_data) == PYTHON_LIST)
                {
                    int isNewBuffer, param_size = 0;
                    Py_ssize_t n = PyList_Size(bind_data);
                    curr->uvalue = (SQLWCHAR *)ALLOC_N(SQLWCHAR, curr->param_size * (n));
                    curr->bind_indicator_array = (SQLINTEGER *) ALLOC_N(SQLINTEGER, n);
                    memset(curr->uvalue , 0, sizeof(SQLWCHAR) * curr->param_size * n);

                    for (i = 0; i < n; i++)
                    {
                        item = PyList_GetItem(bind_data, i);

                        tmp_uvalue = NULL;
                        dest_uvalue = NULL;

                        if(PyObject_CheckBuffer(item) && (curr->data_type == SQL_BLOB   ||
                                                          curr->data_type == SQL_BINARY ||
                                                          curr->data_type == SQL_VARBINARY) )
                        {
#if  PY_MAJOR_VERSION >= 3
                            Py_buffer tmp_buffer;
                            PyObject_GetBuffer(item, &tmp_buffer, PyBUF_SIMPLE);
                            tmp_uvalue = tmp_buffer.buf;
                            curr->ivalue = tmp_buffer.len;
#else
                            PyObject_AsReadBuffer(item, (const void **) &tmp_uvalue, &buffer_len);
                            curr->ivalue = buffer_len;
#endif
                        }
                        else
                        {
                            if(tmp_uvalue != NULL)
                            {
                                PyMem_Del(tmp_uvalue);
                                tmp_uvalue = NULL;
                            }
                            tmp_uvalue = getUnicodeDataAsSQLWCHAR(item, &isNewBuffer);
                            curr->ivalue = PyUnicode_GetSize(item);
                            curr->ivalue = curr->ivalue * sizeof(SQLWCHAR);
                        }
                        param_length = curr->ivalue;
                        if (curr->size != 0)
                        {
                            curr->ivalue = (curr->size + 1) * sizeof(SQLWCHAR);
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
                                    }
                                }
                                else
                                {
                                    if (curr->ivalue <= (curr->param_size * sizeof(SQLWCHAR)))
                                    {
                                        curr->ivalue = (curr->param_size + 1) * sizeof(SQLWCHAR);
                                    }
                                }
                            }
                        }

                        if (isNewBuffer == 0 )
                        {
                            dest_uvalue = (char *)&curr->uvalue[0] + (curr->param_size * i);
                            dest_uvalue = memcpy(dest_uvalue, tmp_uvalue, (param_length + sizeof(SQLWCHAR)));
                            param_size = curr->param_size;
                        }
                        else if (param_length <= curr->param_size)
                        {
                            dest_uvalue = (char *)&curr->uvalue[0] + (curr->param_size * i);
                            dest_uvalue = memcpy(dest_uvalue, tmp_uvalue, (param_length + sizeof(SQLWCHAR)));
                            PyMem_Del(tmp_uvalue);
                            param_size = curr->param_size;
                        }
                        else if(curr->data_type == SQL_TYPE_TIMESTAMP)
                        {
                            dest_uvalue = (char *)&curr->uvalue[0] + (param_length * i);
                            dest_uvalue = memcpy(dest_uvalue, tmp_uvalue, param_length);
                            PyMem_Del(tmp_uvalue);
                            param_size = param_length;
                        }

                        switch( curr->data_type )
                        {
                            case SQL_CLOB:
                            case SQL_DBCLOB:
                                if(curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                                {
                                    curr->bind_indicator_array[i] =  param_length;
                                    paramValuePtr = (SQLPOINTER)curr->uvalue;
                                }
                                else
                                {
                                    curr->bind_indicator_array[i] = curr->ivalue;
#ifndef PASE
                                    paramValuePtr = (SQLPOINTER)(curr->uvalue);
#else
                                    paramValuePtr = (SQLPOINTER)&(curr->uvalue);
#endif
                                }
                                valueType = SQL_C_WCHAR;
                                break;

                            case SQL_BLOB:
                                if (curr->param_type == SQL_PARAM_OUTPUT ||curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                                {
                                    curr->bind_indicator_array[i] = param_length;
                                    paramValuePtr = (SQLPOINTER)curr->uvalue;
                                }
                                else
                                {
                                    curr->bind_indicator_array[i] = curr->ivalue;
#ifndef PASE
                                    paramValuePtr = (SQLPOINTER)(curr->uvalue);
#else
                                    paramValuePtr = (SQLPOINTER)&(curr->uvalue);
#endif
                                }
                                valueType = SQL_C_BINARY;
                                break;

                            case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                            case SQL_LONGVARBINARY:
#endif /* PASE */
                            case SQL_VARBINARY:
                                /* account for bin_mode settings as well */
                                curr->bind_indicator_array[i] = param_length;
                                valueType = SQL_C_BINARY;
                                paramValuePtr = (SQLPOINTER)curr->uvalue;
                                break;

                            case SQL_XML:
                                curr->bind_indicator_array[i] = param_length;
                                paramValuePtr = (SQLPOINTER)curr->uvalue;
                                valueType = SQL_C_WCHAR;
                                break;

                            case SQL_TYPE_TIMESTAMP:
                                valueType = SQL_C_WCHAR;
                                if( param_length == 0)
                                {
                                    curr->bind_indicator_array[i] = SQL_NULL_DATA;
                                }
                                else
                                {
                                    curr->bind_indicator_array[i] = param_length;
                                }
                                if(dest_uvalue[20] == 'T')
                                {
                                    dest_uvalue[20] = ' ';
                                }
                                paramValuePtr = (SQLPOINTER)(curr->uvalue);
                                break;

                            default:
                                valueType = SQL_C_WCHAR;
                                curr->bind_indicator_array[i] = param_length;
                                paramValuePtr = (SQLPOINTER)(curr->uvalue);
                        }
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType,
                            curr->data_type, curr->param_size, curr->scale, paramValuePtr,
                            param_size, &curr->bind_indicator_array[0]);
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
                    {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                    }
                    curr->data_type = valueType;
                }
                else /* To bind scalar values */
                {
                    int isNewBuffer;
                    if(PyObject_CheckBuffer(bind_data) && (curr->data_type == SQL_BLOB || curr->data_type == SQL_BINARY || curr->data_type == SQL_VARBINARY)) {
#if  PY_MAJOR_VERSION >= 3
                        Py_buffer tmp_buffer;
                        PyObject_GetBuffer(bind_data, &tmp_buffer, PyBUF_SIMPLE);
                        curr->uvalue = tmp_buffer.buf;
                        curr->ivalue = tmp_buffer.len;
#else
                        PyObject_AsReadBuffer(bind_data, (const void **) &(curr->uvalue), &buffer_len);
                        curr->ivalue = buffer_len;
#endif
                    } else {
                        if(curr->uvalue != NULL) {
                            PyMem_Del(curr->uvalue);
                            curr->uvalue = NULL;
                        }
                        curr->uvalue = getUnicodeDataAsSQLWCHAR(bind_data, &isNewBuffer);
                        curr->ivalue = PyUnicode_GetSize(bind_data);
                        curr->ivalue = curr->ivalue * sizeof(SQLWCHAR);
                    }
                    param_length = curr->ivalue;
                    if (curr->size != 0) {
                        curr->ivalue = (curr->size + 1) * sizeof(SQLWCHAR);
                    }

                    if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT) {
                        if (curr->size == 0) {
                            if ((curr->data_type == SQL_BLOB) || (curr->data_type == SQL_CLOB) || (curr->data_type == SQL_BINARY)
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                                    || (curr->data_type == SQL_LONGVARBINARY)
#endif /* PASE */
                                    || (curr->data_type == SQL_VARBINARY) || (curr->data_type == SQL_XML)) {
                                if (curr->ivalue <= curr->param_size) {
                                    curr->ivalue = curr->param_size + sizeof(SQLWCHAR);
                                }
                            } else {
                                if (curr->ivalue <= (curr->param_size * sizeof(SQLWCHAR))) {
                                    curr->ivalue = (curr->param_size + 1) * sizeof(SQLWCHAR);
                                }
                            }
                        }
                    }

                    if (isNewBuffer == 0 ){
                        /* actually make a copy, since this will uvalue will be freed explicitly */
                        SQLWCHAR* tmp = (SQLWCHAR*)ALLOC_N(SQLWCHAR, curr->ivalue + 1);
                        memcpy(tmp, curr->uvalue, (param_length + sizeof(SQLWCHAR)));
                        curr->uvalue = tmp;
                    } else if (param_length <= curr->param_size) {
                        SQLWCHAR* tmp = (SQLWCHAR*)ALLOC_N(SQLWCHAR, curr->ivalue + 1);
                        memcpy(tmp, curr->uvalue, (param_length + sizeof(SQLWCHAR)));
                        PyMem_Del(curr->uvalue);
                        curr->uvalue = tmp;
                    }

                    switch( curr->data_type){
                        case SQL_CLOB:
                        case SQL_DBCLOB:
                            if(curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT){
                                curr->bind_indicator =  param_length;
                                paramValuePtr = (SQLPOINTER)curr->uvalue;
                            } else {
                                curr->bind_indicator = SQL_DATA_AT_EXEC;
#ifndef PASE
                                paramValuePtr = (SQLPOINTER)(curr);
#else
                                paramValuePtr = (SQLPOINTER)&(curr);
#endif
                            }
                            valueType = SQL_C_WCHAR;
                            break;

                        case SQL_BLOB:
                            if (curr->param_type == SQL_PARAM_OUTPUT ||curr->param_type == SQL_PARAM_INPUT_OUTPUT) {
                                curr->bind_indicator = param_length;
                                paramValuePtr = (SQLPOINTER)curr;
                            } else {
                                curr->bind_indicator = SQL_DATA_AT_EXEC;
#ifndef PASE
                                paramValuePtr = (SQLPOINTER)(curr);
#else
                                paramValuePtr = (SQLPOINTER)&(curr);
#endif
                            }
                            valueType = SQL_C_BINARY;
                            break;

                        case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                        case SQL_LONGVARBINARY:
#endif /* PASE */
                        case SQL_VARBINARY:
                            /* account for bin_mode settings as well */
                            curr->bind_indicator = param_length;
                            valueType = SQL_C_BINARY;
                            paramValuePtr = (SQLPOINTER)curr->uvalue;
                            break;

                        case SQL_XML:
                            curr->bind_indicator = param_length;
                            paramValuePtr = (SQLPOINTER)curr->uvalue;
                            valueType = SQL_C_WCHAR;
                            break;
                        case SQL_TYPE_TIMESTAMP:
                            valueType = SQL_C_WCHAR;
                            if( param_length == 0)
                            {
                                curr->bind_indicator = SQL_NULL_DATA;
                            }
                            else
                            {
                                curr->bind_indicator = param_length;
                            }
                            if(curr->uvalue[10] == 'T'){
                                curr->uvalue[10] = ' ';
                            }
                            paramValuePtr = (SQLPOINTER)(curr->uvalue);
                            break;
                        default:
                            valueType = SQL_C_WCHAR;
                            curr->bind_indicator = param_length;
                            paramValuePtr = (SQLPOINTER)(curr->uvalue);
                   }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType, curr->data_type, curr->param_size, curr->scale, paramValuePtr, curr->ivalue, &(curr->bind_indicator));
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                    }
                    curr->data_type = valueType;
                }
            }
            break;

        case PYTHON_STRING:
            {
                /* To Bind array of values */
                if(TYPE(bind_data) == PYTHON_LIST)
                {
                    Py_ssize_t n = PyList_Size(bind_data);
                    curr->svalue = (char *)ALLOC_N(char, curr->param_size * (n));
                    curr->bind_indicator_array = (SQLINTEGER *) ALLOC_N(SQLINTEGER, n);
                    memset(curr->svalue , 0, curr->param_size * n);

                    for (i = 0; i < n; i++)
                    {
                        item = PyList_GetItem(bind_data, i);

                        tmp_svalue = NULL;
                        dest_svalue = NULL;
                        if (PyObject_CheckBuffer(item) && (curr->data_type == SQL_BLOB      ||
                                                           curr->data_type == SQL_BINARY   ||
                                                           curr->data_type == SQL_VARBINARY) )
                        {
#if  PY_MAJOR_VERSION >= 3
                            Py_buffer tmp_buffer;
                            PyObject_GetBuffer(item, &tmp_buffer, PyBUF_SIMPLE);
                            tmp_svalue = tmp_buffer.buf;
                            curr->ivalue = tmp_buffer.len;
#else
                            PyObject_AsReadBuffer(item, (const void **) &tmp_svalue, &buffer_len);
                            curr->ivalue = buffer_len;
#endif
                        }
                        else
                        {
                            if(tmp_svalue != NULL)
                            {
                                PyMem_Del(tmp_svalue);
                                tmp_svalue = NULL;
                            }
                            tmp_svalue = PyBytes_AsString(item);   /** It is PyString_AsString() in PY_MAJOR_VERSION<3, and code execution will not come here in PY_MAJOR_VERSION>=3 **/
                            curr->ivalue = strlen(tmp_svalue);
                        }
                        param_length = curr->ivalue;
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

                        switch ( curr->data_type )
                        {
                            case SQL_CLOB:
                            case SQL_DBCLOB:
                                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                                {
                                    curr->bind_indicator_array[i] = param_length;
                                    paramValuePtr = (SQLPOINTER)curr->svalue;
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
                                    paramValuePtr = (SQLPOINTER)&(curr->svalue);
#endif
                                }
                                valueType = SQL_C_CHAR;
                                break;

                            case SQL_BLOB:
                                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                                {
                                    curr->ivalue = curr->ivalue - 1;
                                    curr->bind_indicator_array[i] = param_length;
                                    paramValuePtr = (SQLPOINTER)curr->svalue;
                                }
                                else
                                {
                                    curr->bind_indicator_array[i] = curr->ivalue;
#ifndef PASE
                                    paramValuePtr = (SQLPOINTER)(curr->svalue);
#else
                                    paramValuePtr = (SQLPOINTER)&(curr->svalue);
#endif
                                }
                                valueType = SQL_C_BINARY;
                                break;

                            case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                            case SQL_LONGVARBINARY:
#endif /* PASE */
                            case SQL_VARBINARY:
                            case SQL_XML:
                                /* account for bin_mode settings as well */
                                curr->bind_indicator_array[i] = curr->ivalue;
                                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                                {
                                    curr->ivalue = curr->ivalue - 1;
                                    curr->bind_indicator_array[i] = param_length;
                                }

                                valueType = SQL_C_BINARY;
                                paramValuePtr = (SQLPOINTER)curr->svalue;
                                break;

                                /* This option should handle most other types such as DATE,
                                 * * VARCHAR etc
                                 * */
                            case SQL_TYPE_TIMESTAMP:
                                valueType = SQL_C_CHAR;
                                curr->bind_indicator_array[i] = curr->ivalue;
                                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                                {
                                    if( param_length == 0)
                                    {
                                        curr->bind_indicator_array[i] = SQL_NULL_DATA;
                                    }
                                    else
                                    {
                                        curr->bind_indicator_array[i] = SQL_NTS;
                                    }
                                }
                                if(dest_svalue[10] == 'T')
                                {
                                    dest_svalue[10] = ' ';
                                }
                                paramValuePtr = (SQLPOINTER)(curr->svalue);
                                break;

                            default:
                                valueType = SQL_C_CHAR;
                                curr->bind_indicator_array[i] = curr->ivalue;
                                if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                                {
                                    curr->bind_indicator_array[i] = SQL_NTS;
                                }
                                paramValuePtr = (SQLPOINTER)(curr->svalue);
                        }
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                            curr->param_type, valueType, curr->data_type, curr->param_size,
                            curr->scale, paramValuePtr, curr->param_size, &curr->bind_indicator_array[0]);
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                rc, 1, NULL, -1, 1);
                    }
                    curr->data_type = valueType;
                }
                else /* To bind scalar values */
                {
                    char* tmp;
                    if (PyObject_CheckBuffer(bind_data) && (curr->data_type == SQL_BLOB   ||
                                                            curr->data_type == SQL_BINARY ||
                                                            curr->data_type == SQL_VARBINARY) )
                    {
#if  PY_MAJOR_VERSION >= 3
                        Py_buffer tmp_buffer;
                        PyObject_GetBuffer(bind_data, &tmp_buffer, PyBUF_SIMPLE);
                        curr->svalue = tmp_buffer.buf;
                        curr->ivalue = tmp_buffer.len;
#else
                       PyObject_AsReadBuffer(bind_data, (const void **) &(curr->svalue), &buffer_len);
                        curr->ivalue = buffer_len;
#endif
                    }
                    else
                    {
                        if(curr->svalue != NULL)
                        {
                            PyMem_Del(curr->svalue);
                            curr->svalue = NULL;
                        }
                        curr->svalue = PyBytes_AsString(bind_data);   /** It is PyString_AsString() in PY_MAJOR_VERSION<3, and code execution will not come here in PY_MAJOR_VERSION>=3 **/
                        curr->ivalue = strlen(curr->svalue);
                    }
                    param_length = curr->ivalue;
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

                    tmp = ALLOC_N(char, curr->ivalue+1);
                    memset(tmp, 0, curr->ivalue+1);
                    curr->svalue = memcpy(tmp, curr->svalue, param_length);
                    curr->svalue[param_length] = '\0';

                    switch ( curr->data_type )
                    {
                        case SQL_CLOB:
                        case SQL_DBCLOB:
                            if (curr->param_type == SQL_PARAM_OUTPUT ||
                                curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                            {
                                curr->bind_indicator = param_length;
                                paramValuePtr = (SQLPOINTER)curr->svalue;
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
                                paramValuePtr = (SQLPOINTER)&(curr);
#endif
                            }
                            valueType = SQL_C_CHAR;
                            break;

                        case SQL_BLOB:
                            if (curr->param_type == SQL_PARAM_OUTPUT ||
                                curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                            {
                                curr->ivalue = curr->ivalue -1;
                                curr->bind_indicator = param_length;
                                paramValuePtr = (SQLPOINTER)curr->svalue;
                            }
                            else
                            {
                                curr->bind_indicator = SQL_DATA_AT_EXEC;
#ifndef PASE
                                paramValuePtr = (SQLPOINTER)(curr);
#else
                                paramValuePtr = (SQLPOINTER)&(curr);
#endif
                            }
                            valueType = SQL_C_BINARY;
                            break;

                        case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                        case SQL_LONGVARBINARY:
#endif /* PASE */
                        case SQL_VARBINARY:
                        case SQL_XML:
                            /* account for bin_mode settings as well */
                            curr->bind_indicator = curr->ivalue;
                            if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                            {
                                curr->ivalue = curr->ivalue - 1;
                                curr->bind_indicator = param_length;
                            }

                            valueType = SQL_C_BINARY;
                            paramValuePtr = (SQLPOINTER)curr->svalue;
                            break;

                            /* This option should handle most other types such as DATE,
                            * VARCHAR etc
                            */
                        case SQL_TYPE_TIMESTAMP:
                            valueType = SQL_C_CHAR;
                            curr->bind_indicator = curr->ivalue;
                            if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                            {
                                if( param_length == 0)
                                {
                                    curr->bind_indicator = SQL_NULL_DATA;
                                }
                                else
                                {
                                    curr->bind_indicator = SQL_NTS;
                                }
                            }
                            if(curr->svalue[10] == 'T')
                            {
                                curr->svalue[10] = ' ';
                            }
                            paramValuePtr = (SQLPOINTER)(curr->svalue);
                            break;
                        default:
                            valueType = SQL_C_CHAR;
                            curr->bind_indicator = curr->ivalue;
                            if (curr->param_type == SQL_PARAM_OUTPUT || curr->param_type == SQL_PARAM_INPUT_OUTPUT)
                            {
                                curr->bind_indicator = SQL_NTS;
                            }
                            paramValuePtr = (SQLPOINTER)(curr->svalue);
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                        curr->param_type, valueType, curr->data_type, curr->param_size,
                        curr->scale, paramValuePtr, curr->ivalue, &(curr->bind_indicator));
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                                    rc, 1, NULL, -1, 1);
                    }
                    curr->data_type = valueType;
                }
            }
            break;

        case PYTHON_DECIMAL:
            if (curr->data_type == SQL_DECIMAL || curr->data_type == SQL_DECFLOAT)
            {
                if(TYPE(bind_data) == PYTHON_LIST)
                {
                    char *svalue = NULL;
                    Py_ssize_t n = PyList_Size(bind_data);
                    int max_precn = 0;

                    if(curr->data_type == SQL_DECIMAL)
                        max_precn = MAX_PRECISION;
                    else // SQL_DECFLOAT
                        max_precn = curr->param_size;

                    if(curr->svalue != NULL)
                    {
                        PyMem_Del(curr->svalue);
                        curr->svalue = NULL;
                    }

                    curr->svalue = (char *)ALLOC_N(char, max_precn * n);
                    curr->bind_indicator_array = (SQLINTEGER *) ALLOC_N(SQLINTEGER, n);
                    memset(curr->svalue , 0, max_precn * n);

                    for (i = 0; i < n; i++)
                    {
                        PyObject *tempobj = NULL;
#if  PY_MAJOR_VERSION >= 3
                        PyObject *tempobj2 = NULL;
#endif
                        item = PyList_GetItem(bind_data, i);
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
                        Py_XDECREF(tempobj);
                    }

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType,
                            curr->data_type, curr->param_size, curr->scale,
                            paramValuePtr, max_precn, curr->bind_indicator_array);
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
                    {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,rc, 1, NULL, -1, 1);
                    }
                    curr->data_type = valueType;
                }
                else /* To bind scalar values */
                {
                    PyObject *tempobj = NULL;
#if  PY_MAJOR_VERSION >= 3
                    PyObject *tempobj2 = NULL;
#endif
                    if(curr->svalue != NULL) {
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

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLBindParameter(stmt_res->hstmt, curr->param_num, curr->param_type, valueType,
                            curr->data_type, curr->param_size, curr->scale,
                            paramValuePtr, curr->ivalue, &(curr->bind_indicator));
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,rc, 1, NULL, -1, 1);
                    }
                    curr->data_type = valueType;
                    Py_XDECREF(tempobj);
                }
                break;
            }


        case PYTHON_DATE:
            if(TYPE(bind_data) == PYTHON_LIST)
            {
                Py_ssize_t n = PyList_Size(bind_data);
                curr->date_value = ALLOC_N(DATE_STRUCT, n);

                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    (curr->date_value + i)->year = PyDateTime_GET_YEAR(item);
                    (curr->date_value + i)->month = PyDateTime_GET_MONTH(item);
                    (curr->date_value + i)->day = PyDateTime_GET_DAY(item);
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                        curr->param_type, SQL_C_TYPE_DATE, curr->data_type, curr->param_size,
                        curr->scale, curr->date_value, curr->ivalue, curr->bind_indicator_array);
                Py_END_ALLOW_THREADS;
            }
            else
            {
                curr->date_value = ALLOC(DATE_STRUCT);
                curr->date_value->year = PyDateTime_GET_YEAR(bind_data);
                curr->date_value->month = PyDateTime_GET_MONTH(bind_data);
                curr->date_value->day = PyDateTime_GET_DAY(bind_data);

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                    curr->param_type, SQL_C_TYPE_DATE, curr->data_type, curr->param_size,
                    curr->scale, curr->date_value, curr->ivalue, &(curr->bind_indicator));
                Py_END_ALLOW_THREADS;
            }
            break;

        case PYTHON_TIME:
            if(TYPE(bind_data) == PYTHON_LIST)
            {
                Py_ssize_t n = PyList_Size(bind_data);
                curr->time_value = ALLOC_N(TIME_STRUCT, n);

                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    (curr->time_value + i)->hour = PyDateTime_TIME_GET_HOUR(item);
                    (curr->time_value + i)->minute = PyDateTime_TIME_GET_MINUTE(item);
                    (curr->time_value + i)->second = PyDateTime_TIME_GET_SECOND(item);
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                        curr->param_type, SQL_C_TYPE_TIME, curr->data_type, curr->param_size,
                        curr->scale, curr->time_value, curr->ivalue, curr->bind_indicator_array);
                Py_END_ALLOW_THREADS;
            }
            else
            {
                curr->time_value = ALLOC(TIME_STRUCT);
                curr->time_value->hour = PyDateTime_TIME_GET_HOUR(bind_data);
                curr->time_value->minute = PyDateTime_TIME_GET_MINUTE(bind_data);
                curr->time_value->second = PyDateTime_TIME_GET_SECOND(bind_data);

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                    curr->param_type, SQL_C_TYPE_TIME, curr->data_type, curr->param_size,
                    curr->scale, curr->time_value, curr->ivalue, &(curr->bind_indicator));
                Py_END_ALLOW_THREADS;
            }
            break;

        case PYTHON_TIMESTAMP:
            if(TYPE(bind_data) == PYTHON_LIST)
            {
                Py_ssize_t n = PyList_Size(bind_data);
                curr->ts_value = ALLOC_N(TIMESTAMP_STRUCT, n);

                for (i = 0; i < n; i++)
                {
                    item = PyList_GetItem(bind_data, i);
                    (curr->ts_value + i)->year = PyDateTime_GET_YEAR(item);
                    (curr->ts_value + i)->month = PyDateTime_GET_MONTH(item);
                    (curr->ts_value + i)->day = PyDateTime_GET_DAY(item);
                    (curr->ts_value + i)->hour = PyDateTime_DATE_GET_HOUR(item);
                    (curr->ts_value + i)->minute = PyDateTime_DATE_GET_MINUTE(item);
                    (curr->ts_value + i)->second = PyDateTime_DATE_GET_SECOND(item);
                    (curr->ts_value + i)->fraction = PyDateTime_DATE_GET_MICROSECOND(item) * 1000;
                }

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                        curr->param_type, SQL_C_TYPE_TIMESTAMP, curr->data_type, curr->param_size,
                        curr->scale, curr->ts_value, curr->ivalue, curr->bind_indicator_array);
                Py_END_ALLOW_THREADS;
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

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                    curr->param_type, SQL_C_TYPE_TIMESTAMP, curr->data_type, curr->param_size,
                    curr->scale, curr->ts_value, curr->ivalue, &(curr->bind_indicator));
                Py_END_ALLOW_THREADS;
            }
            break;

        case PYTHON_NIL:
            curr->ivalue = SQL_NULL_DATA;

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLBindParameter(stmt_res->hstmt, curr->param_num,
                curr->param_type, SQL_C_DEFAULT, curr->data_type, curr->param_size,
                curr->scale, &curr->ivalue, 0, (SQLLEN *)&(curr->ivalue));
            Py_END_ALLOW_THREADS;

            if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                                            rc, 1, NULL, -1, 1);
            }
            break;

        default:
            return SQL_ERROR;
    }
    return rc;
}

/* static int _python_ibm_db_execute_helper2(stmt_res, data, int bind_cmp_list)
    */
static int _python_ibm_db_execute_helper2(stmt_handle *stmt_res, PyObject *data, int bind_cmp_list, int bind_params)
{
    int rc = SQL_SUCCESS;
    param_node *curr = NULL;    /* To traverse the list */
    PyObject *bind_data;         /* Data value from symbol table */
    char error[DB2_MAX_ERR_MSG_LEN];

    /* Used in call to SQLDescribeParam if needed */
    SQLSMALLINT param_no;
    SQLSMALLINT data_type;
    SQLUINTEGER precision;
    SQLSMALLINT scale;
    SQLSMALLINT nullable;

    /* This variable means that we bind the complete list of params cached */
    /* The values used are fetched from the active symbol table */
    /* TODO: Enhance this part to check for stmt_res->file_param */
    /* If this flag is set, then use SQLBindParam, else use SQLExtendedBind */
    if ( bind_cmp_list ) {
        /* Bind the complete list sequentially */
        /* Used when no parameters array is passed in */
        curr = stmt_res->head_cache_list;

        while (curr != NULL ) {
            /* Fetch data from symbol table */
            if (curr->param_type == PARAM_FILE)
                bind_data = curr->var_pyvalue;
            else {
                bind_data = curr->var_pyvalue;
            }
            if (bind_data == NULL)
                return -1;

            rc = _python_ibm_db_bind_data( stmt_res, curr, bind_data);
            if ( rc == SQL_ERROR ) {
                sprintf(error, "Binding Error 1: %s",
                        IBM_DB_G(__python_stmt_err_msg));
                PyErr_SetString(PyExc_Exception, error);
                return rc;
            }
            curr = curr->next;
        }
        return 0;
    } else {
        /* Bind only the data value passed in to the Current Node */
        if ( data != NULL ) {
            if ( bind_params ) {
                /* This condition applies if the parameter has not been
                * bound using ibm_db.bind_param. Need to describe the
                * parameter and then bind it.
                */
                param_no = ++stmt_res->num_params;

                Py_BEGIN_ALLOW_THREADS;
                rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt, param_no,
                    (SQLSMALLINT*)&data_type, &precision, (SQLSMALLINT*)&scale,
                    (SQLSMALLINT*)&nullable);
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
                {
                     _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                                      SQL_HANDLE_STMT,
                                                      rc, 1, NULL, -1, 1);
                }
                if ( rc == SQL_ERROR ) {
                    sprintf(error, "Describe Param Failed: %s",
                            IBM_DB_G(__python_stmt_err_msg));
                    PyErr_SetString(PyExc_Exception, error);
                    return rc;
                }
                curr = build_list(stmt_res, param_no, data_type, precision,
                              scale, nullable);
                rc = _python_ibm_db_bind_data( stmt_res, curr, data);
                if ( rc == SQL_ERROR ) {
                    sprintf(error, "Binding Error 2: %s",
                            IBM_DB_G(__python_stmt_err_msg));
                    PyErr_SetString(PyExc_Exception, error);
                    return rc;
                }
            } else {
                /* This is always at least the head_cache_node -- assigned in
                * ibm_db.execute(), if params have been bound.
                */
                curr = stmt_res->current_node;
                if ( curr != NULL ) {
                    rc = _python_ibm_db_bind_data( stmt_res, curr, data);
                    if ( rc == SQL_ERROR ) {
                        sprintf(error, "Binding Error 2: %s",
                            IBM_DB_G(__python_stmt_err_msg));
                        PyErr_SetString(PyExc_Exception, error);
                        return rc;
                    }
                    stmt_res->current_node = curr->next;
                }
            }
            return rc;
        }
    }
    return rc;
}

/*
 * static PyObject *_python_ibm_db_execute_helper1(stmt_handle *stmt_res, PyObject *parameters_tuple)
 *
 */
static PyObject *_python_ibm_db_execute_helper1(stmt_handle *stmt_res, PyObject *parameters_tuple)
{
    int rc, numOpts, i, bind_params = 0;
    SQLSMALLINT num;
    SQLPOINTER valuePtr;
    PyObject *data;
    char error[DB2_MAX_ERR_MSG_LEN];
    /* This is used to loop over the param cache */
    param_node *prev_ptr, *curr_ptr;
    /* Free any cursors that might have been allocated in a previous call to
    * SQLExecute
    */
    Py_BEGIN_ALLOW_THREADS;
    SQLFreeStmt((SQLHSTMT)stmt_res->hstmt, SQL_CLOSE);
    Py_END_ALLOW_THREADS;

    /* This ensures that each call to ibm_db.execute start from scratch */
    stmt_res->current_node = stmt_res->head_cache_list;

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLNumParams((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT*)&num);
    Py_END_ALLOW_THREADS;

    if ( num != 0 ) {
        /* Parameter Handling */
        if ( !NIL_P(parameters_tuple) ) {
            /* Make sure ibm_db.bind_param has been called */
            /* If the param list is NULL -- ERROR */
            if ( stmt_res->head_cache_list == NULL ) {
                bind_params = 1;
            }

            if (!PyTuple_Check(parameters_tuple)) {
                PyErr_SetString(PyExc_Exception, "Param is not a tuple");
                return NULL;
            }

            numOpts = PyTuple_Size(parameters_tuple);

            if (numOpts > num) {
                /* More are passed in -- Warning - Use the max number present */
                sprintf(error, "%d params bound not matching %d required",
                        numOpts, num);
                PyErr_SetString(PyExc_Exception, error);
                numOpts = stmt_res->num_params;
            } else if (numOpts < num) {
                /* If there are less params passed in, than are present
                * -- Error
                */
                sprintf(error, "%d params bound not matching %d required",
                        numOpts, num);
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }

            for ( i = 0; i < numOpts; i++) {
                /* Bind values from the parameters_tuple to params */
                data = PyTuple_GetItem(parameters_tuple, i);

                /* The 0 denotes that you work only with the current node.
                * The 4th argument specifies whether the data passed in
                * has been described. So we need to call SQLDescribeParam
                * before binding depending on this.
                */
                rc = _python_ibm_db_execute_helper2(stmt_res, data, 0, bind_params);
                if ( rc == SQL_ERROR) {
                    sprintf(error, "Binding Error: %s", IBM_DB_G(__python_stmt_err_msg));
                    PyErr_SetString(PyExc_Exception, error);
                    return NULL;
                }
            }
        } else {
            /* No additional params passed in. Use values already bound. */
            if ( num > stmt_res->num_params ) {
                /* More parameters than we expected */
                sprintf(error, "%d params bound not matching %d required",
                        stmt_res->num_params, num);
                PyErr_SetString(PyExc_Exception, error);
            } else if ( num < stmt_res->num_params ) {
                /* Fewer parameters than we expected */
                sprintf(error, "%d params bound not matching %d required",
                        stmt_res->num_params, num);
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }

            /* Param cache node list is empty -- No params bound */
            if ( stmt_res->head_cache_list == NULL ) {
                PyErr_SetString(PyExc_Exception, "Parameters not bound");
                return NULL;
            } else {
                /* The 1 denotes that you work with the whole list
                 * And bind sequentially
                 */
                rc = _python_ibm_db_execute_helper2(stmt_res, NULL, 1, 0);
                if ( rc == SQL_ERROR ) {
                    sprintf(error, "Binding Error 3: %s", IBM_DB_G(__python_stmt_err_msg));
                    PyErr_SetString(PyExc_Exception, error);
                    return NULL;
                }
            }
        }
    } else {
        /* No Parameters
         * We just execute the statement. No additional work needed.
         */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLExecute((SQLHSTMT)stmt_res->hstmt);
        Py_END_ALLOW_THREADS;

        if( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
        {
            _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                             SQL_HANDLE_STMT,
                                              rc, 1, NULL, -1, 1);
        }
        if ( rc == SQL_ERROR ) {
            sprintf(error, "Statement Execute Failed: %s", IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        Py_INCREF(Py_True);
        return Py_True;
    }

    /* Execute Stmt -- All parameters bound */
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLExecute((SQLHSTMT)stmt_res->hstmt);
    Py_END_ALLOW_THREADS;
    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
    {
         _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                          SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
    }
    if ( rc == SQL_ERROR ) {
        sprintf(error, "Statement Execute Failed: %s", IBM_DB_G(__python_stmt_err_msg));
        PyErr_SetString(PyExc_Exception, error);
        return NULL;
    }
    if ( rc == SQL_NEED_DATA ) {
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
        Py_END_ALLOW_THREADS;
        while ( rc == SQL_NEED_DATA ) {
            /* passing data value for a parameter */
            if ( !NIL_P(((param_node*)valuePtr)->svalue)) {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node*)valuePtr)->svalue), ((param_node*)valuePtr)->ivalue);
                Py_END_ALLOW_THREADS;
            } else {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node*)valuePtr)->uvalue), ((param_node*)valuePtr)->ivalue);
                Py_END_ALLOW_THREADS;
            }

            if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                        rc, 1, NULL, -1, 1);
            }
            if ( rc == SQL_ERROR)
            {
                sprintf(error, "Sending data failed: %s",
                        IBM_DB_G(__python_stmt_err_msg));
                PyErr_SetString(PyExc_Exception, error);
                return NULL;
            }

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
            Py_END_ALLOW_THREADS;
        }

        if ( rc == SQL_ERROR  || rc == SQL_SUCCESS_WITH_INFO ) {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt,
                               SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
        }
        if( rc == SQL_ERROR )
        {
            sprintf(error, "Sending data failed: %s",
                        IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
    }

    /* cleanup dynamic bindings if present */
    if ( bind_params == 1 ) {
        _python_ibm_db_clear_param_cache(stmt_res);
    }

    if ( rc != SQL_ERROR ) {
        Py_INCREF(Py_True);
        return Py_True;
    }
    return NULL;
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
    PyObject *py_stmt_res = NULL;
    PyObject *parameters_tuple = NULL;
    stmt_handle *stmt_res;
    if (!PyArg_ParseTuple(args, "O|O", &py_stmt_res, &parameters_tuple))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        return _python_ibm_db_execute_helper1(stmt_res, parameters_tuple);
    } else {
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
    conn_handle *conn_res = NULL;
    PyObject *py_conn_res = NULL;
    PyObject *retVal = NULL;
    char* return_str = NULL;    /* This variable is used by
                    * _python_ibm_db_check_sql_errors to return err
                    * strings
                    */

    if (!PyArg_ParseTuple(args, "|O", &py_conn_res))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
        }

        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);

        memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);

        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, -1, 0,
                                          return_str, DB2_ERRMSG,
                                          conn_res->errormsg_recno_tracker);
        if(conn_res->errormsg_recno_tracker - conn_res->error_recno_tracker >= 1)
             conn_res->error_recno_tracker = conn_res->errormsg_recno_tracker;
        conn_res->errormsg_recno_tracker++;

        retVal =  StringOBJ_FromASCII(return_str);
        if(return_str != NULL) {
            PyMem_Del(return_str);
            return_str = NULL;
        }
        return retVal;
    } else {
        return StringOBJ_FromASCII(IBM_DB_G(__python_conn_err_msg));
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
    conn_handle *conn_res = NULL;
    PyObject *py_conn_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                             * _python_ibm_db_check_sql_errors to return warning
                             * strings */

    if (!PyArg_ParseTuple(args, "|O", &py_conn_res))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);
        PyErr_Clear();
        memset(return_str, 0, SQL_SQLSTATE_SIZE + 1);

        _python_ibm_db_check_sql_errors( conn_res->hdbc, SQL_HANDLE_DBC, 1, 0,
                                         return_str, DB2_WARNMSG,
                                         conn_res->error_recno_tracker);
        if (conn_res->error_recno_tracker-conn_res->errormsg_recno_tracker >= 1) {
            conn_res->errormsg_recno_tracker = conn_res->error_recno_tracker;
        }
        conn_res->error_recno_tracker++;

        retVal = StringOBJ_FromASCII( return_str );
        if( return_str != NULL ) {
            PyMem_Del( return_str);
            return_str = NULL;
        }
        return retVal;
    } else {
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
    stmt_handle *stmt_res = NULL;
    PyObject *py_stmt_res = NULL;
    PyObject *retVal = NULL;
    char* return_str = NULL;    /* This variable is used by
                    * _python_ibm_db_check_sql_errors to return err
                    * strings
                    */

    if (!PyArg_ParseTuple(args, "|O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);

        memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);

        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, 1, 0,
                                        return_str, DB2_WARNMSG,
                                        stmt_res->errormsg_recno_tracker);
        if(stmt_res->errormsg_recno_tracker - stmt_res->error_recno_tracker >= 1)
            stmt_res->error_recno_tracker = stmt_res->errormsg_recno_tracker;
        stmt_res->errormsg_recno_tracker++;

        retVal = StringOBJ_FromASCII(return_str);
        if(return_str != NULL) {
            PyMem_Del(return_str);
            return_str = NULL;
        }
        return retVal;
    } else {
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
    stmt_handle *stmt_res = NULL;
    PyObject *py_stmt_res = NULL;
    PyObject *retVal = NULL;
    char* return_str = NULL;    /* This variable is used by
                    * _python_ibm_db_check_sql_errors to return err
                    * strings
                    */

    if (!PyArg_ParseTuple(args, "|O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);

        memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);

        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, -1, 0,
                                        return_str, DB2_ERRMSG,
                                        stmt_res->errormsg_recno_tracker);
        if(stmt_res->errormsg_recno_tracker - stmt_res->error_recno_tracker >= 1)
            stmt_res->error_recno_tracker = stmt_res->errormsg_recno_tracker;
        stmt_res->errormsg_recno_tracker++;

        retVal = StringOBJ_FromStr(return_str);
        if(return_str != NULL) {
            PyMem_Del(return_str);
            return_str = NULL;
        }
        return retVal;
    } else {
        return StringOBJ_FromStr(IBM_DB_G(__python_stmt_err_msg));
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
    conn_handle *conn_res = NULL;
    PyObject *py_conn_res = NULL;
    PyObject *retVal = NULL;
    char *return_str = NULL; /* This variable is used by
                             * _python_ibm_db_check_sql_errors to return err
                             * strings */

    if (!PyArg_ParseTuple(args, "|O", &py_conn_res))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        return_str = ALLOC_N(char, SQL_SQLSTATE_SIZE + 1);

        memset(return_str, 0, SQL_SQLSTATE_SIZE + 1);

        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, -1, 0,
                                        return_str, DB2_ERR,
                                        conn_res->error_recno_tracker);
        if (conn_res->error_recno_tracker-conn_res->errormsg_recno_tracker >= 1) {
            conn_res->errormsg_recno_tracker = conn_res->error_recno_tracker;
        }
        conn_res->error_recno_tracker++;

        retVal = StringOBJ_FromASCII(return_str);
        if(return_str != NULL) {
            PyMem_Del(return_str);
            return_str = NULL;
        }
        return retVal;
    } else {
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
    stmt_handle *stmt_res = NULL;
    PyObject *py_stmt_res = NULL;
    PyObject *retVal = NULL;
    char* return_str = NULL; /* This variable is used by
                             * _python_ibm_db_check_sql_errors to return err
                             * strings
                             */

    if (!PyArg_ParseTuple(args, "|O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        return_str = ALLOC_N(char, DB2_MAX_ERR_MSG_LEN);

        memset(return_str, 0, DB2_MAX_ERR_MSG_LEN);

        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, -1, 0,
                                        return_str, DB2_ERR,
                                        stmt_res->error_recno_tracker);

        if (stmt_res->error_recno_tracker-stmt_res->errormsg_recno_tracker >= 1) {
            stmt_res->errormsg_recno_tracker = stmt_res->error_recno_tracker;
        }
        stmt_res->error_recno_tracker++;

        retVal = StringOBJ_FromASCII(return_str);
        if(return_str != NULL) {
            PyMem_Del(return_str);
            return_str = NULL;
        }
        return retVal;
    } else {
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
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res, *new_stmt_res = NULL;
    int rc = 0;
    SQLHANDLE new_hstmt;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        _python_ibm_db_clear_stmt_err_cache();

        /* alloc handle and return only if it errors */
#ifndef __MVS__
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLAllocHandle(SQL_HANDLE_STMT, stmt_res->hdbc, &new_hstmt);
        Py_END_ALLOW_THREADS;
#endif
        if ( rc < SQL_SUCCESS ) {
            _python_ibm_db_check_sql_errors(stmt_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_INCREF(Py_False);
            return Py_False;
        }

        Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
	rc = SQLMoreResults((SQLHSTMT)stmt_res->hstmt);
#else
	rc = SQLNextResult((SQLHSTMT)stmt_res->hstmt, (SQLHSTMT)new_hstmt);
#endif
        Py_END_ALLOW_THREADS;

        if( rc != SQL_SUCCESS ) {
            if(rc < SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO ) {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1, NULL, -1, 1);
            }
#ifndef __MVS__
	    Py_BEGIN_ALLOW_THREADS;
		SQLFreeHandle(SQL_HANDLE_STMT, new_hstmt);
		Py_END_ALLOW_THREADS;
#endif
            Py_INCREF(Py_False);
            return Py_False;
        }

#ifdef __MVS__
	Py_BEGIN_ALLOW_THREADS;
	SQLFreeStmt(stmt_res->hstmt, SQL_UNBIND);
	Py_END_ALLOW_THREADS;
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

        return (PyObject *)new_stmt_res;
    } else {
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
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;
    SQLSMALLINT indx = 0;
    char error[DB2_MAX_ERR_MSG_LEN];

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLNumResultCols((SQLHSTMT)stmt_res->hstmt, &indx);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
            _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                             1, NULL, -1, 1);
                }
                if( rc == SQL_ERROR)
                {
            sprintf(error, "SQLNumResultCols failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        return PyInt_FromLong(indx);
    } else {
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
    Py_INCREF(Py_False);
    return Py_False;
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
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;
    SQLINTEGER count = 0;
    char error[DB2_MAX_ERR_MSG_LEN];

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLRowCount((SQLHSTMT)stmt_res->hstmt, &count);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,
                                            1, NULL, -1, 1);
            sprintf(error, "SQLRowCount failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        return PyInt_FromLong(count);
    } else {
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
    Py_INCREF(Py_False);
    return Py_False;
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
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res;
    int rc = 0;
    SQLINTEGER count = 0;
    char error[DB2_MAX_ERR_MSG_LEN];
    SQLSMALLINT strLenPtr;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }

        Py_BEGIN_ALLOW_THREADS;
#ifdef __MVS__
	/* z/OS DB2 ODBC only seems to have SQLGetDiagRec */
	rc = SQL_SUCCESS;
#else
	rc = SQLGetDiagField(SQL_HANDLE_STMT, stmt_res->hstmt, 0,
                                SQL_DIAG_CURSOR_ROW_COUNT, &count, SQL_IS_INTEGER,
                                &strLenPtr);
#endif
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ||  rc == SQL_SUCCESS_WITH_INFO )
        {
            _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                             rc,1, NULL, -1, 1);
        }
        if ( rc == SQL_ERROR ) {
            sprintf(error, "SQLGetDiagField failed: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
        return PyInt_FromLong(count);
    } else {
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;
    }
    Py_INCREF(Py_False);
    return Py_False;
}

/* static int _python_ibm_db_get_column_by_name(stmt_handle *stmt_res, char *col_name, int col)
 */
static int _python_ibm_db_get_column_by_name(stmt_handle *stmt_res, char *col_name, int col)
{
    int i;
    /* get column header info */
    if ( stmt_res->column_info == NULL ) {
        if (_python_ibm_db_get_result_set_info(stmt_res)<0) {
            return -1;
        }
    }
    if ( col_name == NULL ) {
        if ( col >= 0 && col < stmt_res->num_columns) {
            return col;
        } else {
            return -1;
        }
    }
    /* should start from 0 */
    i = 0;
    while (i < stmt_res->num_columns) {
        if (strcmp((char*)stmt_res->column_info[i].name, col_name) == 0) {
            return i;
        }
        i++;
    }
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
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    PyObject *py_stmt_res = NULL;
    stmt_handle* stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if (col_name_py3_tmp != NULL) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
        Py_INCREF(Py_False);
        return Py_False;
    }
#ifdef _WIN32
    return PyUnicode_DecodeLocale((char*)stmt_res->column_info[col].name,"surrogateescape");
#else
	return PyUnicode_FromString((char*)stmt_res->column_info[col].name);
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    int col = -1;
    char *col_name = NULL;
    stmt_handle *stmt_res = NULL;
    int rc;
    SQLINTEGER colDataDisplaySize;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if ( col_name_py3_tmp != NULL ) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
        Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLColAttributes((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)col+1,
         SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &colDataDisplaySize);
    Py_END_ALLOW_THREADS;

    if ( rc < SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO ) {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                      NULL, -1, 1);
    }
    if( rc < SQL_SUCCESS )
    {
        Py_INCREF(Py_False);
        return Py_False;
    }
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle* stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;
    int rc;
    SQLINTEGER nullableCol;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if ( col_name_py3_tmp != NULL ) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
         Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLColAttributes((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)col+1,
        SQL_DESC_NULLABLE, NULL, 0, NULL, &nullableCol);
    Py_END_ALLOW_THREADS;

    if ( rc <  SQL_SUCCESS ) {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                    NULL, -1, 1);
        Py_RETURN_FALSE;
    }
    else if ( rc == SQL_SUCCESS_WITH_INFO )
    {
        _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT, rc,1,
                                          NULL, -1, 1);
        Py_RETURN_FALSE;
    }
    else if ( nullableCol == SQL_NULLABLE ) {
        Py_RETURN_TRUE;
    } else {
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle* stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if ( col_name_py3_tmp != NULL ) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
        Py_INCREF(Py_False);
        return Py_False;
    }
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle* stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if ( col_name_py3_tmp != NULL ) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
        Py_RETURN_FALSE;
    }
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle* stmt_res = NULL;
    char *col_name = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }
    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if ( col_name_py3_tmp != NULL ) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
        Py_RETURN_FALSE;
    }
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    stmt_handle* stmt_res = NULL;
    char *col_name = NULL;
    char *str_val = NULL;
    int col = -1;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }
    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if ( col_name_py3_tmp != NULL ) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
        Py_RETURN_FALSE;
    }
    switch (stmt_res->column_info[col].type) {
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    int col = -1;
    char *col_name = NULL;
    stmt_handle *stmt_res = NULL;
    int rc;
    SQLINTEGER colDataSize;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }
    if ( TYPE(column) == PYTHON_FIXNUM ) {
        col = PyInt_AsLong(column);
    } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
        col_name_py3_tmp = PyUnicode_AsASCIIString(column);
        if (col_name_py3_tmp == NULL) {
            return NULL;
        }
        column = col_name_py3_tmp;
#endif
        col_name = PyBytes_AsString(column);
    } else {
        /* Column argument has to be either an integer or string */
        Py_RETURN_FALSE;
    }
    col = _python_ibm_db_get_column_by_name(stmt_res, col_name, col);
#if  PY_MAJOR_VERSION >= 3
    if ( col_name_py3_tmp != NULL ) {
        Py_XDECREF(col_name_py3_tmp);
    }
#endif
    if ( col < 0 ) {
        Py_RETURN_FALSE;
    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLColAttributes((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT)col + 1,
        SQL_DESC_LENGTH, NULL, 0, NULL, &colDataSize);
    Py_END_ALLOW_THREADS;

    if ( rc != SQL_SUCCESS ) {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
            NULL, -1, 1);
        PyErr_Clear();
        Py_RETURN_FALSE;
    }
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
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res = NULL;

    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

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
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res;
    int rc;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLEndTran(SQL_HANDLE_DBC, conn_res->hdbc, SQL_ROLLBACK);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            Py_RETURN_TRUE;
        }
    }
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
    PyObject *py_stmt_res = NULL;
    stmt_handle *handle;
    SQLRETURN rc;
    if (!PyArg_ParseTuple(args, "O", &py_stmt_res))
        return NULL;
    if (!NIL_P(py_stmt_res)) {
        if (PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            handle = (stmt_handle *)py_stmt_res;
            if (handle->hstmt != -1) {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLFreeHandle( SQL_HANDLE_STMT, handle->hstmt);
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
                {
                    _python_ibm_db_check_sql_errors( handle->hstmt,
                                                     SQL_HANDLE_STMT,
                                                     rc, 1, NULL, -1, 1);
                }
                if ( rc == SQL_ERROR ){
                    Py_RETURN_FALSE;
                }
                  _python_ibm_db_free_result_struct(handle);
                handle->hstmt = -1;
                Py_RETURN_TRUE;
            }
        }
    }
    Py_RETURN_NONE;
}

/*    static RETCODE _python_ibm_db_get_data(stmt_handle *stmt_res, int col_num, short ctype, void *buff, int in_length, SQLINTEGER *out_length) */
static RETCODE _python_ibm_db_get_data(stmt_handle *stmt_res, int col_num, short ctype, void *buff, int in_length, SQLINTEGER *out_length)
{
    RETCODE rc = SQL_SUCCESS;

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLGetData((SQLHSTMT)stmt_res->hstmt, col_num, ctype, buff, in_length,
        out_length);
    Py_END_ALLOW_THREADS;

    if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
            NULL, -1, 1);
    }
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
    PyObject *py_stmt_res = NULL;
    PyObject *column = NULL;
#if  PY_MAJOR_VERSION >= 3
    PyObject *col_name_py3_tmp = NULL;
#endif
    PyObject *retVal = NULL;
    stmt_handle *stmt_res;
    long col_num;
    RETCODE rc;
    void    *out_ptr;
    DATE_STRUCT *date_ptr;
    TIME_STRUCT *time_ptr;
    TIMESTAMP_STRUCT *ts_ptr;
    char error[DB2_MAX_ERR_MSG_LEN];
    SQLINTEGER in_length, out_length = -10; /* Initialize out_length to some
                        * meaningless value
                        * */
    SQLSMALLINT column_type, targetCType = SQL_C_CHAR, len_terChar = 0 ;
    double double_val;
    SQLINTEGER long_val;
    PyObject *return_value = NULL;

    if (!PyArg_ParseTuple(args, "OO", &py_stmt_res, &column))
        return NULL;

    if (!NIL_P(py_stmt_res)) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }

        if ( TYPE(column) == PYTHON_FIXNUM ) {
            col_num = PyLong_AsLong(column);

        } else if (PyString_Check(column)) {
#if  PY_MAJOR_VERSION >= 3
            col_name_py3_tmp = PyUnicode_AsASCIIString(column);
            if (col_name_py3_tmp == NULL) {
                return NULL;
            }
            column = col_name_py3_tmp;
#endif
            col_num = _python_ibm_db_get_column_by_name(stmt_res, PyBytes_AsString(column), -1);
#if  PY_MAJOR_VERSION >= 3
            if ( col_name_py3_tmp != NULL ) {
                Py_XDECREF(col_name_py3_tmp);
            }
#endif
        } else {
            /* Column argument has to be either an integer or string */
            Py_RETURN_FALSE;
        }

    /* get column header info */
    if ( stmt_res->column_info == NULL ) {
        if (_python_ibm_db_get_result_set_info(stmt_res)<0) {
            sprintf(error, "Column information cannot be retrieved: %s",
                    IBM_DB_G(__python_stmt_err_msg));
            strcpy(IBM_DB_G(__python_stmt_err_msg), error);
            PyErr_Clear();
            Py_RETURN_FALSE;
        }
    }

    if(col_num < 0 || col_num >= stmt_res->num_columns) {
        strcpy(IBM_DB_G(__python_stmt_err_msg), "Column ordinal out of range");
        PyErr_Clear();
        Py_RETURN_NONE;
    }

    /* get the data */
    column_type = stmt_res->column_info[col_num].type;
    switch(column_type) {
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
            if (column_type == SQL_DECIMAL || column_type == SQL_NUMERIC || column_type == SQL_BIGINT){
                in_length = stmt_res->column_info[col_num].size +
                            stmt_res->column_info[col_num].scale + 2 + 1;
            }
            else{
                in_length = stmt_res->column_info[col_num].size+1;
            }
            if (column_type == SQL_DECFLOAT){
                in_length = MAX_DECFLOAT_LENGTH;
            }
            out_ptr = (SQLPOINTER)ALLOC_N(Py_UNICODE, in_length);
            memset(out_ptr,0,sizeof(Py_UNICODE)*in_length);

            if ( out_ptr == NULL ) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num+1, SQL_C_WCHAR,
                         out_ptr, in_length * sizeof(Py_UNICODE), &out_length);

            if ( rc == SQL_ERROR ) {
                if(out_ptr != NULL) {
                    PyMem_Del(out_ptr);
                    out_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA) {
                Py_INCREF(Py_None);
                return_value = Py_None;
            } //else if (column_type == SQL_BIGINT){
            //    return_value = PyLong_FromString(out_ptr, NULL, 0); }
            // Converting from Wchar string to long leads to data truncation
            // as it treats 00 in 2 bytes for each char as NULL
            else {
                return_value = getSQLWCharAsPyUnicodeObject(out_ptr, out_length);
            }
            PyMem_Del(out_ptr);
            out_ptr = NULL;
            return return_value;

        case SQL_TYPE_DATE:
            date_ptr = ALLOC(DATE_STRUCT);
            if (date_ptr == NULL) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num+1, SQL_C_TYPE_DATE,
                        date_ptr, sizeof(DATE_STRUCT), &out_length);

            if ( rc == SQL_ERROR ) {
                if(date_ptr != NULL) {
                    PyMem_Del(date_ptr);
                    date_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA) {
                PyMem_Del(date_ptr);
                date_ptr = NULL;
                Py_RETURN_NONE;
            } else {
                return_value = PyDate_FromDate(date_ptr->year, date_ptr->month, date_ptr->day);
                PyMem_Del(date_ptr);
                date_ptr = NULL;
                return return_value;
            }
            break;

        case SQL_TYPE_TIME:
            time_ptr = ALLOC(TIME_STRUCT);
            if (time_ptr == NULL) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num+1, SQL_C_TYPE_TIME,
                        time_ptr, sizeof(TIME_STRUCT), &out_length);

            if ( rc == SQL_ERROR ) {
                if(time_ptr != NULL) {
                    PyMem_Del(time_ptr);
                    time_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }

            if (out_length == SQL_NULL_DATA) {
                PyMem_Del(time_ptr);
                time_ptr = NULL;
                Py_RETURN_NONE;
            } else {
                return_value = PyTime_FromTime(time_ptr->hour % 24, time_ptr->minute, time_ptr->second, 0);
                PyMem_Del(time_ptr);
                time_ptr = NULL;
                return return_value;
            }
            break;

        case SQL_TYPE_TIMESTAMP:
            ts_ptr = ALLOC(TIMESTAMP_STRUCT);
            if (ts_ptr == NULL) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }

            rc = _python_ibm_db_get_data(stmt_res, col_num+1, SQL_C_TYPE_TIMESTAMP,
                        ts_ptr, sizeof(TIMESTAMP_STRUCT), &out_length);

            if ( rc == SQL_ERROR ) {
                if(ts_ptr != NULL) {
                    PyMem_Del(ts_ptr);
                    time_ptr = NULL;
                }
                PyErr_Clear();
                Py_RETURN_FALSE;
            }

            if (out_length == SQL_NULL_DATA) {
                PyMem_Del(ts_ptr);
                ts_ptr = NULL;
                Py_RETURN_NONE;
            } else {
                return_value = PyDateTime_FromDateAndTime(ts_ptr->year, ts_ptr->month, ts_ptr->day, ts_ptr->hour % 24, ts_ptr->minute, ts_ptr->second, ts_ptr->fraction / 1000);
                PyMem_Del(ts_ptr);
                ts_ptr = NULL;
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
            rc = _python_ibm_db_get_data(stmt_res, col_num+1, SQL_C_LONG,
                         &long_val, sizeof(long_val),
                         &out_length);
            if ( rc == SQL_ERROR ) {
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA) {
                Py_RETURN_NONE;
            } else {
                return PyInt_FromLong(long_val);
            }
            break;

        case SQL_BIT:
            rc = _python_ibm_db_get_data(stmt_res, col_num+1, SQL_C_LONG,
                         &long_val, sizeof(long_val),
                         &out_length);
            if ( rc == SQL_ERROR ) {
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA) {
                Py_RETURN_NONE;
            } else {
                return PyBool_FromLong(long_val);
            }
            break;


        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            rc = _python_ibm_db_get_data(stmt_res, col_num+1, SQL_C_DOUBLE,
                         &double_val, sizeof(double_val),
                         &out_length);
            if ( rc == SQL_ERROR ) {
                PyErr_Clear();
                Py_RETURN_FALSE;
            }
            if (out_length == SQL_NULL_DATA) {
                Py_RETURN_NONE;
            } else {
                return PyFloat_FromDouble(double_val);
            }
            break;

        case SQL_BLOB:
        case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARCHAR is SQL_VARCHAR */
        case SQL_LONGVARBINARY:
#endif /* PASE */
        case SQL_VARBINARY:
            switch (stmt_res->s_bin_mode) {
                case PASSTHRU:
                    return PyBytes_FromStringAndSize("", 0);
                    break;
                    /* returns here */
                case CONVERT:
                    targetCType = SQL_C_CHAR;
                    len_terChar = sizeof(char);
                    break;
                case BINARY:
                    targetCType = SQL_C_BINARY;
                    len_terChar = 0;
                    break;
                default:
                    Py_RETURN_FALSE;
            }
        case SQL_XML:
        case SQL_CLOB:
        case SQL_DBCLOB:
            if (column_type == SQL_CLOB || column_type == SQL_DBCLOB || column_type == SQL_XML) {
                len_terChar = sizeof(SQLWCHAR);
                targetCType = SQL_C_WCHAR;
            }
            out_ptr = ALLOC_N(char, INIT_BUFSIZ + len_terChar);
            if ( out_ptr == NULL ) {
                 PyErr_SetString(PyExc_Exception,
                        "Failed to Allocate Memory for XML Data");
                return NULL;
            }
            rc = _python_ibm_db_get_data(stmt_res, col_num + 1, targetCType, out_ptr,
                    INIT_BUFSIZ + len_terChar, &out_length);
            if ( rc == SQL_SUCCESS_WITH_INFO ) {
                void *tmp_out_ptr = NULL;

                tmp_out_ptr = ALLOC_N(char, out_length + INIT_BUFSIZ + len_terChar);
                memcpy(tmp_out_ptr, out_ptr, INIT_BUFSIZ);
                PyMem_Del(out_ptr);
                out_ptr = tmp_out_ptr;

                rc = _python_ibm_db_get_data(stmt_res, col_num + 1, targetCType, (char *)out_ptr + INIT_BUFSIZ,
                    out_length + len_terChar, &out_length);
                if (rc == SQL_ERROR) {
                    PyMem_Del(out_ptr);
                    out_ptr = NULL;
                    return NULL;
                }
                if (len_terChar == sizeof(SQLWCHAR)) {
                    retVal = getSQLWCharAsPyUnicodeObject(out_ptr, INIT_BUFSIZ + out_length);
                } else {
                    retVal = PyBytes_FromStringAndSize((char *)out_ptr, INIT_BUFSIZ + out_length);
                }
            } else if ( rc == SQL_ERROR ) {
                PyMem_Del(out_ptr);
                out_ptr = NULL;
                Py_RETURN_FALSE;
            } else {
                if (out_length == SQL_NULL_DATA) {
                    Py_INCREF(Py_None);
                    retVal = Py_None;
                } else {
                    if (len_terChar == 0) {
                        retVal = PyBytes_FromStringAndSize((char *)out_ptr, out_length);
                    } else {
                        retVal = getSQLWCharAsPyUnicodeObject(out_ptr, out_length);
                    }
                }

            }
            if (out_ptr != NULL) {
                PyMem_Del(out_ptr);
                out_ptr = NULL;
            }
            return retVal;
        default:
            break;
        }
    } else {
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
    }
    Py_RETURN_FALSE;
}

/* static void _python_ibm_db_bind_fetch_helper(INTERNAL_FUNCTION_PARAMETERS,
                                                int op)
*/
static PyObject *_python_ibm_db_bind_fetch_helper(PyObject *args, int op)
{
    int rc = -1;
    int column_number;
    SQLINTEGER row_number = -1;
    stmt_handle *stmt_res = NULL;
    SQLSMALLINT column_type ;
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
    char error[DB2_MAX_ERR_MSG_LEN];

    if (!PyArg_ParseTuple(args, "O|O", &py_stmt_res, &py_row_number))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString(PyExc_Exception, "Supplied statement object parameter is invalid");
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

    if (!NIL_P(py_row_number)) {
        if (PyInt_Check(py_row_number)) {
            row_number = (SQLINTEGER) PyInt_AsLong(py_row_number);
        } else {
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            return NULL;
        }
    }
    _python_ibm_db_init_error_info(stmt_res);

    /* get column header info */
    if ( stmt_res->column_info == NULL ) {
        if (_python_ibm_db_get_result_set_info(stmt_res)<0) {
            sprintf(error, "Column information cannot be retrieved: %s",
                IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
    }
    /* bind the data */
    if ( stmt_res->row_data == NULL ) {
        rc = _python_ibm_db_bind_column_helper(stmt_res);
        if ( rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO ) {
            sprintf(error, "Column binding cannot be done: %s",
                IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
    }
    /* check if row_number is present */
    if (PyTuple_Size(args) == 2 && row_number > 0) {
#ifndef PASE /* i5/OS problem with SQL_FETCH_ABSOLUTE (temporary until fixed) */
        if (is_systemi) {

            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_FIRST,
                row_number);
            if( rc == SQL_SUCCESS_WITH_INFO )
            {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                                 rc ,1 ,
                                                 NULL, -1 ,1);
            }
            Py_END_ALLOW_THREADS;

            if (row_number>1 && (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO))
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_RELATIVE,
                row_number-1);
                if( rc == SQL_SUCCESS_WITH_INFO )
                {
                    _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                                     rc ,1,
                                                     NULL, -1 ,1);
                }
                Py_END_ALLOW_THREADS;
        } else {
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_ABSOLUTE,
                row_number);
            if( rc == SQL_SUCCESS_WITH_INFO )
            {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                                 rc,1,
                                                 NULL,-1,1);
            }
            Py_END_ALLOW_THREADS;
        }
#else /* PASE */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_FIRST,
            row_number);
         if ( rc == SQL_SUCCESS_WITH_INFO )
         {
               _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                                rc, 1,
                                                NULL, -1,1);
         }
        Py_END_ALLOW_THREADS;

        if (row_number>1 && (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO))
            Py_BEGIN_ALLOW_THREADS;

            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_RELATIVE,
            row_number-1);
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                _python_ibm_db_check_sql_errors( stmt_res->hstmt, SQL_HANDLE_STMT,
                                                 rc, 1,
                                                 NULL, -1, 1);
            }
            Py_END_ALLOW_THREADS;
#endif /* PASE */
    } else if (PyTuple_Size(args) == 2 && row_number < 0) {
        PyErr_SetString(PyExc_Exception,
            "Requested row number must be a positive value");
        return NULL;
    } else {
        /* row_number is NULL or 0; just fetch next row */
        Py_BEGIN_ALLOW_THREADS;

        rc = SQLFetch((SQLHSTMT)stmt_res->hstmt);
        if( rc == SQL_SUCCESS_WITH_INFO )
        {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                      rc, 1,NULL, -1, 1);
        }

        Py_END_ALLOW_THREADS;
    }

    if (rc == SQL_NO_DATA_FOUND) {
        Py_INCREF(Py_False);
        return Py_False;
    } else if ( rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
            NULL, -1, 1);
        sprintf(error, "Fetch Failure: %s", IBM_DB_G(__python_stmt_err_msg));
        PyErr_SetString(PyExc_Exception, error);
        return NULL;
    }
       if( rc == SQL_SUCCESS_WITH_INFO )
       {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                    rc, 1,NULL, -1, 1);
       }
    /* copy the data over return_value */
    if ( op & FETCH_ASSOC ) {
        return_value = PyDict_New();
    } else if ( op == FETCH_INDEX ) {
        return_value = PyTuple_New(stmt_res->num_columns);
    }

    for (column_number = 0; column_number < stmt_res->num_columns; column_number++) {
        column_type = stmt_res->column_info[column_number].type;
        row_data = &stmt_res->row_data[column_number].data;
        out_length = stmt_res->row_data[column_number].out_length;

        switch(stmt_res->s_case_mode) {
            case CASE_LOWER:
                stmt_res->column_info[column_number].name =
                    (SQLCHAR*)strtolower((char*)stmt_res->column_info[column_number].name,
                    strlen((char*)stmt_res->column_info[column_number].name));
                break;
            case CASE_UPPER:
                stmt_res->column_info[column_number].name =
                    (SQLCHAR*)strtoupper((char*)stmt_res->column_info[column_number].name,
                    strlen((char*)stmt_res->column_info[column_number].name));
                break;
            case CASE_NATURAL:
            default:
                    break;
        }
        if (out_length == SQL_NULL_DATA) {
            Py_INCREF(Py_None);
            value = Py_None;
        } else {
            switch(column_type) {
                case SQL_CHAR:
                case SQL_VARCHAR:
                    if ( stmt_res->s_use_wchar == WCHAR_NO ) {
                        tmp_length = stmt_res->column_info[column_number].size;
                        value = PyBytes_FromStringAndSize((char *)row_data->str_val, out_length);
                        break;
                    }
                case SQL_WCHAR:
                case SQL_WVARCHAR:
                case SQL_GRAPHIC:
                case SQL_VARGRAPHIC:
                case SQL_LONGVARGRAPHIC:
                    tmp_length = stmt_res->column_info[column_number].size;
                    value = getSQLWCharAsPyUnicodeObject(row_data->w_val, out_length);
                    break;

#ifndef PASE /* i5/OS SQL_LONGVARCHAR is SQL_VARCHAR */
                case SQL_LONGVARCHAR:
                case SQL_WLONGVARCHAR:

#endif /* PASE */
                    /* i5/OS will xlate from EBCIDIC to ASCII (via SQLGetData) */
                    tmp_length = stmt_res->column_info[column_number].size;

                    wout_ptr = (SQLWCHAR *)ALLOC_N(SQLWCHAR, tmp_length + 1);
                    if ( wout_ptr == NULL ) {
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return NULL;
                    }

                    /*  _python_ibm_db_get_data null terminates all output. */
                    rc = _python_ibm_db_get_data(stmt_res, column_number + 1, SQL_C_WCHAR, wout_ptr,
                        (tmp_length * sizeof(SQLWCHAR) + 1), &out_length);
                    if ( rc == SQL_ERROR ) {
                        return NULL;
                    }
                    if (out_length == SQL_NULL_DATA) {
                        Py_INCREF(Py_None);
                        value = Py_None;
                    } else {
                        value = getSQLWCharAsPyUnicodeObject(wout_ptr, out_length);
                    }
                    if (wout_ptr != NULL) {
                        PyMem_Del(wout_ptr);
                        wout_ptr = NULL;
                    }
                    break;

                case SQL_DECIMAL:
                case SQL_NUMERIC:
                case SQL_DECFLOAT:
		    value = StringOBJ_FromASCIIAndSize((char *)row_data->str_val, out_length);
                    break;

                case SQL_TYPE_DATE:
                    value = PyDate_FromDate(row_data->date_val->year, row_data->date_val->month, row_data->date_val->day);
                    break;

                case SQL_TYPE_TIME:
                    value = PyTime_FromTime(row_data->time_val->hour % 24, row_data->time_val->minute, row_data->time_val->second, 0);
                    break;

                case SQL_TYPE_TIMESTAMP:
                    value = PyDateTime_FromDateAndTime(row_data->ts_val->year, row_data->ts_val->month, row_data->ts_val->day,
                                    row_data->ts_val->hour % 24, row_data->ts_val->minute, row_data->ts_val->second,
                                    row_data->ts_val->fraction / 1000);
                    break;

                case SQL_BIGINT:
                    value = PyLong_FromString((char *)row_data->str_val, NULL, 10);
                    break;

#ifdef __MVS__
                case SQL_SMALLINT:
#else
                case SQL_SMALLINT:
                case SQL_BOOLEAN:
#endif
                    value = PyInt_FromLong(row_data->s_val);
                    break;

                case SQL_INTEGER:
                    value = PyInt_FromLong(row_data->i_val);
                    break;

                case SQL_BIT:
                    value = PyBool_FromLong(row_data->i_val);
                    break;

                case SQL_REAL:
                    value = PyFloat_FromDouble(row_data->r_val);
                    break;

                case SQL_FLOAT:
                    value = PyFloat_FromDouble(row_data->f_val);
                    break;

                case SQL_DOUBLE:
                    value = PyFloat_FromDouble(row_data->d_val);
                    break;

                case SQL_BINARY:
#ifndef PASE /* i5/OS SQL_LONGVARBINARY is SQL_VARBINARY */
                case SQL_LONGVARBINARY:
#endif /* PASE */
                case SQL_VARBINARY:
                    if ( stmt_res->s_bin_mode == PASSTHRU ) {
                        value = PyBytes_FromStringAndSize("", 0);
                    } else {
                        value = PyBytes_FromStringAndSize((char *)row_data->str_val, out_length);
                    }
                    break;

                case SQL_BLOB:
                    switch (stmt_res->s_bin_mode) {
                        case PASSTHRU:
                            Py_RETURN_NONE;
                            break;
                        case CONVERT:
                            len_terChar = sizeof(char);
                            targetCType = SQL_C_CHAR;
                            break;
                        case BINARY:
                            len_terChar = 0;
                            targetCType = SQL_C_BINARY;
                            break;
                        default:
                            len_terChar = -1;
                            break;
                    }
                case SQL_XML:
                case SQL_CLOB:
                case SQL_DBCLOB:
                    if (column_type == SQL_CLOB || column_type == SQL_DBCLOB || column_type == SQL_XML) {
                        len_terChar = sizeof(SQLWCHAR);
                        targetCType = SQL_C_WCHAR;
                    } else if (len_terChar == -1) {
                        break;
                    }
                    out_ptr = (void *)ALLOC_N(char, INIT_BUFSIZ + len_terChar);
                    if (out_ptr == NULL) {
                        PyErr_SetString(PyExc_Exception,
                            "Failed to Allocate Memory for LOB Data");
                        return NULL;
                    }
                    rc = _python_ibm_db_get_data(stmt_res, column_number + 1, targetCType, out_ptr,
                        INIT_BUFSIZ + len_terChar, &out_length);
                    if (rc == SQL_SUCCESS_WITH_INFO) {
                        void *tmp_out_ptr = NULL;

                        tmp_out_ptr = (void *)ALLOC_N(char, out_length + INIT_BUFSIZ + len_terChar);
                        memcpy(tmp_out_ptr, out_ptr, INIT_BUFSIZ);
                        PyMem_Del(out_ptr);
                        out_ptr = tmp_out_ptr;

                        rc = _python_ibm_db_get_data(stmt_res, column_number + 1, targetCType, (char *)out_ptr + INIT_BUFSIZ,
                            out_length + len_terChar, &out_length);
                        if (rc == SQL_ERROR) {
                            if (out_ptr != NULL) {
                                PyMem_Del(out_ptr);
                                out_ptr = NULL;
                            }
                            sprintf(error, "Failed to fetch LOB Data: %s",
                                IBM_DB_G(__python_stmt_err_msg));
                            PyErr_SetString(PyExc_Exception, error);
                            return NULL;
                        }

                        if (len_terChar == sizeof(SQLWCHAR)) {
                            value = getSQLWCharAsPyUnicodeObject(out_ptr, INIT_BUFSIZ + out_length);
                        } else {
                            value = PyBytes_FromStringAndSize((char*)out_ptr, INIT_BUFSIZ + out_length);
                        }
                    } else if ( rc == SQL_ERROR ) {
                        PyMem_Del(out_ptr);
                        out_ptr = NULL;
                        sprintf(error, "Failed to LOB Data: %s",
                            IBM_DB_G(__python_stmt_err_msg));
                        PyErr_SetString(PyExc_Exception, error);
                        return NULL;
                    } else {
                        if (out_length == SQL_NULL_DATA) {
                            Py_INCREF(Py_None);
                            value = Py_None;
                        } else {
                            if (len_terChar == sizeof(SQLWCHAR)) {
                                value =  getSQLWCharAsPyUnicodeObject(out_ptr, out_length);
                            } else {
                                value = PyBytes_FromStringAndSize((char*)out_ptr, out_length);
                            }
                        }
                    }
                    if (out_ptr != NULL) {
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
        if (op & FETCH_ASSOC) {
#ifdef _WIN32
            key = PyUnicode_DecodeLocale((char*)stmt_res->column_info[column_number].name,"surrogateescape");
#else
			key = PyUnicode_FromString((char*)stmt_res->column_info[column_number].name);
#endif
            if (value == NULL) {
                Py_XDECREF(key);
                Py_XDECREF(value);
                return NULL;
            }
            PyDict_SetItem(return_value, key, value);
            Py_DECREF(key);
        }
        if (op == FETCH_INDEX) {
            /* No need to call Py_DECREF as PyTuple_SetItem steals the reference */
            PyTuple_SetItem(return_value, column_number, value);
        } else {
            if (op == FETCH_BOTH) {
                key = PyInt_FromLong(column_number);
                PyDict_SetItem(return_value, key, value);
                Py_DECREF(key);
            }
            Py_DECREF(value);
        }
    }
       if( rc == SQL_SUCCESS_WITH_INFO )
       {
            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT,
                    rc, 1,NULL, -1, 1);
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
    PyObject *py_stmt_res = NULL;
    PyObject *py_row_number = NULL;
    SQLINTEGER row_number = -1;
    stmt_handle* stmt_res = NULL;
    int rc;
    char error[DB2_MAX_ERR_MSG_LEN];

    if (!PyArg_ParseTuple(args, "O|O", &py_stmt_res, &py_row_number))
        return NULL;

    if (NIL_P(py_stmt_res) || (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType))) {
        PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
        return NULL;
    } else {
        stmt_res = (stmt_handle *)py_stmt_res;
    }

    if (!NIL_P(py_row_number)) {
        if (PyInt_Check(py_row_number)) {
            row_number = (SQLINTEGER) PyInt_AsLong(py_row_number);
        } else {
            PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
            return NULL;
        }
    }
    /* get column header info */
    if ( stmt_res->column_info == NULL ) {
        if (_python_ibm_db_get_result_set_info(stmt_res)<0) {
            sprintf(error, "Column information cannot be retrieved: %s",
                 IBM_DB_G(__python_stmt_err_msg));
            PyErr_SetString(PyExc_Exception, error);
            return NULL;
        }
    }

    /* check if row_number is present */
    if (PyTuple_Size(args) == 2 && row_number > 0) {
#ifndef PASE /* i5/OS problem with SQL_FETCH_ABSOLUTE */

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_ABSOLUTE,
                          row_number);
        Py_END_ALLOW_THREADS;
        if( rc == SQL_SUCCESS_WITH_INFO )
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                             SQL_HANDLE_STMT, rc, 1, NULL,
                                             -1,1);
        }
#else /* PASE */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_FIRST,
                          row_number);
        if( rc == SQL_SUCCESS_WITH_INFO )
        {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                             SQL_HANDLE_STMT, rc, 1, NULL,
                                             -1,1);
        }
        Py_END_ALLOW_THREADS;

        if (row_number>1 && (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO))
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLFetchScroll((SQLHSTMT)stmt_res->hstmt, SQL_FETCH_RELATIVE,
                             row_number-1);
            if( rc == SQL_SUCCESS_WITH_INFO )
            {
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt,
                                                 SQL_HANDLE_STMT, rc, 1, NULL,
                                                 -1,1);
            }
            Py_END_ALLOW_THREADS;
#endif /* PASE */
    } else if (PyTuple_Size(args) == 2 && row_number < 0) {
        PyErr_SetString(PyExc_Exception,
                  "Requested row number must be a positive value");
        return NULL;
    } else {
        /* row_number is NULL or 0; just fetch next row */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLFetch((SQLHSTMT)stmt_res->hstmt);
        Py_END_ALLOW_THREADS;
    }

    if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
        if( rc == SQL_SUCCESS_WITH_INFO )
        {
             _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                              SQL_HANDLE_STMT, rc, 1,
                                              NULL, -1, 1);
        }
        Py_RETURN_TRUE;
    } else if (rc == SQL_NO_DATA_FOUND) {
        Py_RETURN_FALSE;
    } else {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1,
                                  NULL, -1, 1);
        PyErr_Clear();
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
    PyObject *conn_or_stmt = NULL;
    PyObject *options = NULL;
    PyObject *py_type = NULL;
    stmt_handle *stmt_res = NULL;
    conn_handle *conn_res;
    int rc = 0;
    long type = 0;

    if (!PyArg_ParseTuple(args, "OOO", &conn_or_stmt, &options, &py_type))
        return NULL;

    if (!NIL_P(conn_or_stmt)) {
        if (!NIL_P(py_type)) {
            if (PyInt_Check(py_type)) {
                type = (int) PyInt_AsLong(py_type);
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if ( type == 1 ) {
            if (!PyObject_TypeCheck(conn_or_stmt, &conn_handleType)) {
                PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
                return NULL;
            }
            conn_res = (conn_handle *)conn_or_stmt;

            if ( !NIL_P(options) ) {
                rc = _python_ibm_db_parse_options(options, SQL_HANDLE_DBC,
                    conn_res);
                if (rc == SQL_ERROR) {
                    PyErr_SetString(PyExc_Exception,
                        "Options Array must have string indexes");
                    return NULL;
                }
            }
        } else {
            if (!PyObject_TypeCheck(conn_or_stmt, &stmt_handleType)) {
                PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
                return NULL;
            }
            stmt_res = (stmt_handle *)conn_or_stmt;

            if ( !NIL_P(options) ) {
                rc = _python_ibm_db_parse_options(options, SQL_HANDLE_STMT,
                    stmt_res);
                if (rc == SQL_ERROR) {
                    PyErr_SetString(PyExc_Exception,
                        "Options Array must have string indexes");
                    return NULL;
                }
            }
        }
        Py_INCREF(Py_True);
        return Py_True;
    } else {
        Py_INCREF(Py_False);
        return Py_False;
    }
}

static PyObject *ibm_db_get_db_info(PyObject *self, PyObject *args)
{
    PyObject *py_conn_res = NULL;
    PyObject *return_value = NULL;
    PyObject *py_option = NULL;
    SQLINTEGER option = 0;
    conn_handle *conn_res;
    int rc = 0;
    SQLCHAR *value=NULL;

    if (!PyArg_ParseTuple(args, "OO", &py_conn_res, &py_option))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        if (!NIL_P(py_option)) {
            if (PyInt_Check(py_option)) {
                option = (SQLINTEGER) PyInt_AsLong(py_option);
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        value = (SQLCHAR*)ALLOC_N(char, ACCTSTR_LEN + 1);

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, (SQLSMALLINT)option, (SQLPOINTER)value,
                        ACCTSTR_LEN, NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            if(value != NULL) {
                PyMem_Del(value);
                value = NULL;
            }
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value = StringOBJ_FromASCII((char *)value);
            if(value != NULL) {
                PyMem_Del(value);
                value = NULL;
            }
            return return_value;
        }
    }
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
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }

        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        /* DBMS_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DBMS_NAME, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            return_value->DBMS_NAME = StringOBJ_FromASCII(buffer255);
        }

        /* DBMS_VER */
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DBMS_VER, (SQLPOINTER)buffer11,
                    sizeof(buffer11), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if( rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                 SQL_HANDLE_DBC, rc, 1,
                                                 NULL, -1, 1);
            }
                return_value->DBMS_VER = StringOBJ_FromASCII(buffer11);
        }

#if defined(__MVS__)
	return_value->DB_CODEPAGE = PyInt_FromLong(1208);
#elif !defined(PASE)  /* i5/OS DB_CODEPAGE handled natively */
        /* DB_CODEPAGE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DATABASE_CODEPAGE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
             if( rc == SQL_SUCCESS_WITH_INFO)
             {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
             }
            return_value->DB_CODEPAGE = PyInt_FromLong(bufferint32);
        }
#endif /* PASE */

        /* DB_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DATABASE_NAME, (SQLPOINTER)buffer255,
                        sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            Py_INCREF(Py_False);
            return Py_False;
        } else {
            if( rc == SQL_SUCCESS_WITH_INFO)
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->DB_NAME = StringOBJ_FromASCII(buffer255);
        }

#ifndef PASE      /* i5/OS INST_NAME handled natively */
        /* INST_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_SERVER_NAME, (SQLPOINTER)buffer255,
                    sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if( rc == SQL_SUCCESS_WITH_INFO)
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->INST_NAME = StringOBJ_FromASCII(buffer255);
        }

        /* SPECIAL_CHARS */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_SPECIAL_CHARACTERS,
                    (SQLPOINTER)buffer255, sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if( rc == SQL_SUCCESS_WITH_INFO)
            {
                _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                 SQL_HANDLE_DBC, rc, 1,
                                                 NULL, -1, 1);
            }
            return_value->SPECIAL_CHARS = StringOBJ_FromASCII(buffer255);
        }
#endif /* PASE */

        /* KEYWORDS */
        memset(buffer2k, 0, sizeof(buffer2k));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_KEYWORDS, (SQLPOINTER)buffer2k,
                        sizeof(buffer2k), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if( rc == SQL_SUCCESS_WITH_INFO)
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }

            for (last = buffer2k; *last; last++) {
                if (*last == ',') {
                    numkw++;
                }
            }
            karray = PyTuple_New(numkw+1);

            for (keyword = last = buffer2k; *last; last++) {
                if (*last == ',') {
                    *last = '\0';
                    PyTuple_SetItem(karray, count, StringOBJ_FromASCII(keyword));
                    keyword = last+1;
                    count++;
                }
            }
            if (*keyword)
                PyTuple_SetItem(karray, count, StringOBJ_FromASCII(keyword));
            return_value->KEYWORDS = karray;
        }

        /* DFT_ISOLATION */
        bitmask = 0;
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DEFAULT_TXN_ISOLATION, &bitmask,
                    sizeof(bitmask), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            if( bitmask & SQL_TXN_READ_UNCOMMITTED ) {
                strcpy((char *)buffer11, "UR");
            }
            if( bitmask & SQL_TXN_READ_COMMITTED ) {
                strcpy((char *)buffer11, "CS");
            }
            if( bitmask & SQL_TXN_REPEATABLE_READ ) {
                strcpy((char *)buffer11, "RS");
            }
            if( bitmask & SQL_TXN_SERIALIZABLE ) {
                strcpy((char *)buffer11, "RR");
            }
            if( bitmask & SQL_TXN_NOCOMMIT ) {
                strcpy((char *)buffer11, "NC");
            }
            return_value->DFT_ISOLATION = StringOBJ_FromASCII(buffer11);
        }

#ifndef PASE      /* i5/OS ISOLATION_OPTION handled natively */
        /* ISOLATION_OPTION */
        bitmask = 0;
        count = 0;
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_TXN_ISOLATION_OPTION, &bitmask,
                        sizeof(bitmask), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1 , 1);
            }

            array = PyTuple_New(5);

            if( bitmask & SQL_TXN_READ_UNCOMMITTED ) {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("UR"));
                count++;
            }
            if( bitmask & SQL_TXN_READ_COMMITTED ) {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("CS"));
                count++;
            }
            if( bitmask & SQL_TXN_REPEATABLE_READ ) {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("RS"));
                count++;
            }
            if( bitmask & SQL_TXN_SERIALIZABLE ) {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("RR"));
                count++;
            }
            if( bitmask & SQL_TXN_NOCOMMIT ) {
                PyTuple_SetItem(array, count, StringOBJ_FromASCII("NC"));
                count++;
            }
            _PyTuple_Resize(&array, count);

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

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            switch (bufferint32) {
                case SQL_SC_SQL92_ENTRY:
                    strcpy((char *)buffer255, "ENTRY");
                    break;
                case SQL_SC_FIPS127_2_TRANSITIONAL:
                    strcpy((char *)buffer255, "FIPS127");
                    break;
                case SQL_SC_SQL92_FULL:
                    strcpy((char *)buffer255, "FULL");
                    break;
                case SQL_SC_SQL92_INTERMEDIATE:
                    strcpy((char *)buffer255, "INTERMEDIATE");
                    break;
                default:
                    break;
            }
            return_value->SQL_CONFORMANCE = StringOBJ_FromASCII(buffer255);
        }

        /* PROCEDURES */
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_PROCEDURES, (SQLPOINTER)buffer11,
                        sizeof(buffer11), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            if( strcmp((char *)buffer11, "Y") == 0 ) {
                Py_INCREF(Py_True);
                return_value->PROCEDURES = Py_True;
            } else {
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

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->IDENTIFIER_QUOTE_CHAR = StringOBJ_FromASCII(buffer11);
        }

        /* LIKE_ESCAPE_CLAUSE */
        memset(buffer11, 0, sizeof(buffer11));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_LIKE_ESCAPE_CLAUSE,
                        (SQLPOINTER)buffer11, sizeof(buffer11), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            if( strcmp(buffer11, "Y") == 0 ) {
                Py_INCREF(Py_True);
                return_value->LIKE_ESCAPE_CLAUSE = Py_True;
            } else {
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

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_COL_NAME_LEN = PyInt_FromLong(bufferint16);
        }

        /* MAX_ROW_SIZE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_ROW_SIZE, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_ROW_SIZE = PyInt_FromLong(bufferint32);
        }

#ifndef PASE      /* i5/OS MAX_IDENTIFIER_LEN handled natively */
        /* MAX_IDENTIFIER_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_IDENTIFIER_LEN, &bufferint16,
                    sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_IDENTIFIER_LEN = PyInt_FromLong(bufferint16);
        }

        /* MAX_INDEX_SIZE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_INDEX_SIZE, &bufferint32,
                    sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_INDEX_SIZE = PyInt_FromLong(bufferint32);
        }

        /* MAX_PROC_NAME_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_PROCEDURE_NAME_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_PROC_NAME_LEN = PyInt_FromLong(bufferint16);
        }
#endif /* PASE */

        /* MAX_SCHEMA_NAME_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_SCHEMA_NAME_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_SCHEMA_NAME_LEN = PyInt_FromLong(bufferint16);
        }

        /* MAX_STATEMENT_LEN */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_STATEMENT_LEN, &bufferint32,
                        sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                        NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_STATEMENT_LEN = PyInt_FromLong(bufferint32);
        }

        /* MAX_TABLE_NAME_LEN */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_MAX_TABLE_NAME_LEN, &bufferint16,
                        sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->MAX_TABLE_NAME_LEN = PyInt_FromLong(bufferint16);
        }

        /* NON_NULLABLE_COLUMNS */
        bufferint16 = 0;

        Py_BEGIN_ALLOW_THREADS;

        rc = SQLGetInfo(conn_res->hdbc, SQL_NON_NULLABLE_COLUMNS, &bufferint16,
                    sizeof(bufferint16), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                            NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            switch (bufferint16) {
                case SQL_NNC_NON_NULL:
                    Py_INCREF(Py_True);
                    rv = Py_True;
                    break;
                case SQL_NNC_NULL:
                    Py_INCREF(Py_False);
                    rv = Py_False;
                    break;
                default:
                    break;
            }
            return_value->NON_NULLABLE_COLUMNS = rv;
        }
        return (PyObject *)return_value;
    }
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
    PyObject *py_conn_res = NULL;
    conn_handle *conn_res = NULL;
    int rc = 0;
    char buffer255[255];
    SQLSMALLINT bufferint16;
    SQLUINTEGER bufferint32;

    le_client_info *return_value = PyObject_NEW(le_client_info,
                                         &client_infoType);

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
        return NULL;

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
        }

        /* DRIVER_NAME */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_DRIVER_NAME, (SQLPOINTER)buffer255,
                      sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                             NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
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

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                             NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
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

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                         NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
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

        if ( rc == SQL_ERROR ) {
                _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                                    NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->DRIVER_ODBC_VER = StringOBJ_FromASCII(buffer255);
        }

#ifndef PASE      /* i5/OS ODBC_VER handled natively */
        /* ODBC_VER */
        memset(buffer255, 0, sizeof(buffer255));

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_ODBC_VER, (SQLPOINTER)buffer255,
                          sizeof(buffer255), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                             NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
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

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                             NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            switch (bufferint16) {
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
#elif !defined(PASE)   /* i5/OS APPL_CODEPAGE handled natively */
        /* APPL_CODEPAGE */
        bufferint32 = 0;

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetInfo(conn_res->hdbc, SQL_APPLICATION_CODEPAGE, &bufferint32,
                      sizeof(bufferint32), NULL);
        Py_END_ALLOW_THREADS;

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                             NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
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

        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC, rc, 1,
                                         NULL, -1, 1);
            PyErr_Clear();
            Py_RETURN_FALSE;
        } else {
            if ( rc == SQL_SUCCESS_WITH_INFO )
            {
                 _python_ibm_db_check_sql_errors( conn_res->hdbc,
                                                  SQL_HANDLE_DBC, rc, 1,
                                                  NULL, -1, 1);
            }
            return_value->CONN_CODEPAGE = PyInt_FromLong(bufferint32);
        }
#endif /* PASE */

        return (PyObject *)return_value;
    }
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
    PyObject *py_conn_res = NULL;
    int rc;
    conn_handle *conn_res = NULL;
    SQLINTEGER conn_alive;

    conn_alive = 0;

    if (!PyArg_ParseTuple(args, "O", &py_conn_res))
        return NULL;

    if (!(NIL_P(py_conn_res) || (py_conn_res == Py_None))) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
#if !defined(PASE) && !defined(__MVS__)
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetConnectAttr(conn_res->hdbc, SQL_ATTR_PING_DB,
            (SQLPOINTER)&conn_alive, 0, NULL);
        Py_END_ALLOW_THREADS;
        if ( rc == SQL_ERROR ) {
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
    if (conn_alive == 0) {
        Py_RETURN_FALSE;
    } else {
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
        return NULL;

    if (!NIL_P(conn_or_stmt)) {
        if (!NIL_P(py_op_integer)) {
            if (PyInt_Check(py_op_integer)) {
                op_integer = (SQLINTEGER) PyInt_AsLong(py_op_integer);
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        if (!NIL_P(py_type)) {
            if (PyInt_Check(py_type)) {
                type = PyInt_AsLong(py_type);
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        /* Checking to see if we are getting a connection option (1) or a
        * statement option (non - 1)
        */
        if (type == 1) {
            if (!PyObject_TypeCheck(conn_or_stmt, &conn_handleType)) {
                PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
                return NULL;
            }
            conn_res = (conn_handle *)conn_or_stmt;

            /* Check to ensure the connection resource given is active */
            if (!conn_res->handle_active) {
                PyErr_SetString(PyExc_Exception, "Connection is not active");
                return NULL;
             }
            /* Check that the option given is not null */
            if (!NIL_P(py_op_integer)) {
                /* ACCTSTR_LEN is the largest possible length of the options to
                * retrieve
             */
                switch(op_integer)
                {
                    case SQL_ATTR_AUTOCOMMIT:
                    case SQL_ATTR_USE_TRUSTED_CONTEXT:
                    case SQL_ATTR_TXN_ISOLATION:
                        isInteger = 1;
                        break;
                    default:
                        isInteger = 0;
                        break;
                }

                if ( isInteger == 0 )
                {
                    value = (SQLCHAR*)ALLOC_N(char, ACCTSTR_LEN + 1);
                    if ( value == NULL ) {
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return NULL;
                    }
                    memset(value, 0, ACCTSTR_LEN + 1);

                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetConnectAttr((SQLHDBC)conn_res->hdbc, op_integer,
                        (SQLPOINTER)value, ACCTSTR_LEN, NULL);
                    Py_END_ALLOW_THREADS;
                    if (rc == SQL_ERROR) {
                        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                            rc, 1, NULL, -1, 1);
                        if(value != NULL) {
                            PyMem_Del(value);
                            value = NULL;
                        }
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    retVal = StringOBJ_FromASCII((char *)value);
                    if(value != NULL) {
                        PyMem_Del(value);
                        value = NULL;
                    }
                    return retVal;
                }
                else {
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetConnectAttr((SQLHDBC)conn_res->hdbc, op_integer,
                            &value_int, SQL_IS_INTEGER, NULL);
                    Py_END_ALLOW_THREADS;
                    if (rc == SQL_ERROR) {
                        _python_ibm_db_check_sql_errors(conn_res->hdbc, SQL_HANDLE_DBC,
                            rc, 1, NULL, -1, 1);
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    return PyInt_FromLong(value_int);
                }
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
            /* At this point we know we are to retreive a statement option */
        } else {
            stmt_res = (stmt_handle *)conn_or_stmt;

            /* Check that the option given is not null */
            if (!NIL_P(py_op_integer)) {
                /* Checking that the option to get is the cursor type because that
                * is what we support here
                */
                switch(op_integer)
                {
                    case SQL_ATTR_CURSOR_TYPE:
                    case SQL_ATTR_ROWCOUNT_PREFETCH:
                    case SQL_ATTR_QUERY_TIMEOUT:
                        isInteger = 1;
                        break;
                    default:
                        isInteger = 0;
                        break;
                }

                if( isInteger == 0 )
                {
                    // string value pointer
                    value = (SQLCHAR*)ALLOC_N(char, ACCTSTR_LEN + 1);
                    if ( value == NULL ) {
                        PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                        return NULL;
                    }
                    memset(value, 0, ACCTSTR_LEN + 1);
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetStmtAttr((SQLHSTMT)stmt_res->hstmt, op_integer,
                        (SQLPOINTER)value, ACCTSTR_LEN, NULL);
                    Py_END_ALLOW_THREADS;
                    if (rc == SQL_ERROR) {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        if(value != NULL) {
                            PyMem_Del(value);
                            value = NULL;
                        }
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    retVal = StringOBJ_FromASCII((char *)value);
                    if(value != NULL) {
                        PyMem_Del(value);
                        value = NULL;
                    }
                    return retVal;
                } else {
                    // integer value
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLGetStmtAttr((SQLHSTMT)stmt_res->hstmt, op_integer,
                            &value_int, SQL_IS_INTEGER, NULL);
                    Py_END_ALLOW_THREADS;
                    if (rc == SQL_ERROR) {
                        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        PyErr_Clear();
                        Py_RETURN_FALSE;
                    }
                    return PyInt_FromLong(value_int);
                }
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
    }
    PyErr_Clear();
    Py_RETURN_FALSE;
}

static int _ibm_db_chaining_flag(stmt_handle *stmt_res, SQLINTEGER flag, error_msg_node *error_list, int client_err_cnt) {
#ifdef __MVS__
        /* SQL_ATTR_CHAINING_BEGIN and SQL_ATTR_CHAINING_END are not defined */
        return SQL_SUCCESS;
#else
  int rc;
    Py_BEGIN_ALLOW_THREADS;
    rc = SQLSetStmtAttrW((SQLHSTMT)stmt_res->hstmt, flag, (SQLPOINTER)SQL_TRUE, SQL_IS_INTEGER);
    Py_END_ALLOW_THREADS;
    if ( flag == SQL_ATTR_CHAINING_BEGIN ) {
        if ( rc == SQL_ERROR ) {
            _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
            PyErr_SetString(PyExc_Exception, IBM_DB_G(__python_stmt_err_msg));
        }
    } else {
        if ( (rc != SQL_SUCCESS) || (client_err_cnt != 0) ) {
            SQLINTEGER errNo = 0;
            PyObject *errTuple = NULL;
            SQLINTEGER err_cnt = 0;
            PyObject *err_msg = NULL, *err_fmtObj = NULL;
            char *err_fmt = NULL;
            size_t err_fmt_offset = 0;
            if ( rc != SQL_SUCCESS ) {
#ifdef __MVS__
	        /* MVS only seems to have SQLGetDiagRec */
	        rc = SQL_SUCCESS;
#else
		SQLGetDiagField(SQL_HANDLE_STMT, (SQLHSTMT)stmt_res->hstmt, 0, SQL_DIAG_NUMBER, (SQLPOINTER) &err_cnt, SQL_IS_POINTER, NULL);
#endif
            }
            errTuple = PyTuple_New(err_cnt + client_err_cnt);
            /* Allocate enough space for largest possible int value. */
            err_fmt = (char *)PyMem_Malloc(strlen("Error 2147483647: %s\n") * (err_cnt + client_err_cnt) + 1);
            err_fmt[0] = '\0';
            errNo = 1;
            while( error_list != NULL ) {
                err_fmt_offset += sprintf(err_fmt+err_fmt_offset, "Error %d: %s\n", (int)errNo, "%s");
                PyTuple_SetItem(errTuple, errNo - 1, StringOBJ_FromASCII(error_list->err_msg));
                error_list = error_list->next;
                errNo++;
            }
            for ( errNo = client_err_cnt + 1; errNo <= (err_cnt + client_err_cnt); errNo++ ) {
                err_fmt_offset += sprintf(err_fmt+err_fmt_offset, "Error %d: %s\n", (int)errNo, "%s");
                _python_ibm_db_check_sql_errors((SQLHSTMT)stmt_res->hstmt, SQL_HANDLE_STMT, SQL_ERROR, 1, NULL, -1, (errNo - client_err_cnt));
                PyTuple_SetItem(errTuple, errNo - 1, StringOBJ_FromASCII(IBM_DB_G(__python_stmt_err_msg)));
            }
            err_fmtObj = StringOBJ_FromASCII(err_fmt);
            err_msg = StringObj_Format(err_fmtObj, errTuple);
            if ( err_fmtObj != NULL ) { Py_XDECREF(err_fmtObj); }
            if ( err_fmt != NULL ) { PyMem_Free(err_fmt); }
            PyErr_SetObject(PyExc_Exception, err_msg);
        }
    }
    return rc;
#endif
}

static void _build_client_err_list(error_msg_node *head_error_list, char *err_msg) {
    error_msg_node *tmp_err = NULL, *curr_err = head_error_list->next, *prv_err = NULL;
    tmp_err = ALLOC(error_msg_node);
    memset(tmp_err, 0, sizeof(error_msg_node));
    strcpy(tmp_err->err_msg, err_msg);
    tmp_err->next = NULL;
    while( curr_err != NULL ) {
        prv_err = curr_err;
        curr_err = curr_err->next;
    }

    if ( head_error_list->next == NULL ) {
        head_error_list->next = tmp_err;
    } else {
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
static PyObject* ibm_db_execute_many (PyObject *self, PyObject *args) {
    PyObject *options = NULL;
    PyObject *params = NULL;
    PyObject *py_stmt_res = NULL;
    stmt_handle *stmt_res = NULL;
    char error[DB2_MAX_ERR_MSG_LEN];
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
    PyErr_SetString( PyExc_Exception, "Not supported: This function is currently not supported on this platform" );
    return NULL;
#endif

    if ( !PyArg_ParseTuple(args, "OO|O", &py_stmt_res, &params, &options) )
        return NULL;

    if ( !NIL_P(py_stmt_res) ) {
        if (!PyObject_TypeCheck(py_stmt_res, &stmt_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
            return NULL;
        } else {
            stmt_res = (stmt_handle *)py_stmt_res;
        }
        /* Free any cursors that might have been allocated in a previous call to SQLExecute */
        Py_BEGIN_ALLOW_THREADS;
        SQLFreeStmt((SQLHSTMT)stmt_res->hstmt, SQL_CLOSE);
        Py_END_ALLOW_THREADS;

        _python_ibm_db_clear_stmt_err_cache();
        stmt_res->head_cache_list = NULL;
        stmt_res->current_node = NULL;

        /* Bind parameters */
        Py_BEGIN_ALLOW_THREADS;
        rc = SQLNumParams((SQLHSTMT)stmt_res->hstmt, (SQLSMALLINT*)&numOpts);
        Py_END_ALLOW_THREADS;

        data_type = (SQLSMALLINT*)ALLOC_N(SQLSMALLINT, numOpts);
        ref_data_type = (SQLSMALLINT*)ALLOC_N(SQLSMALLINT, numOpts);
        for ( i = 0; i < numOpts; i++) {
            ref_data_type[i] = -1;
        }
        if ( numOpts != 0 ) {
            for ( i = 0; i < numOpts; i++) {
                Py_BEGIN_ALLOW_THREADS;
                rc = SQLDescribeParam((SQLHSTMT)stmt_res->hstmt, i + 1,
                    (SQLSMALLINT*)(data_type + i), &precision, (SQLSMALLINT*)&scale,
                    (SQLSMALLINT*)&nullable);
                Py_END_ALLOW_THREADS;

                if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO )
                {
                    _python_ibm_db_check_sql_errors( stmt_res->hstmt,
                                                     SQL_HANDLE_STMT,
                                                     rc, 1, NULL, -1, 1);
                }
                if ( rc == SQL_ERROR ) {
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
        memset(head_error_list, 0, sizeof(error_msg_node));
        head_error_list->next = NULL;
        if ( numOfRows > 0 ) {
            for ( i = 0; i < numOfRows; i++ ) {
                int j = 0;
                param_node *curr = NULL;
                PyObject *param = PyTuple_GET_ITEM(params, i);
                error[0] = '\0';
                if ( !PyTuple_Check(param) ) {
                    sprintf(error, "Value parameter %d is not a tuple", i + 1);
                    _build_client_err_list(head_error_list, error);
                    err_count++;
                    continue;
                }

                numOfParam = PyTuple_Size(param);
                if ( numOpts < numOfParam ) {
                    /* More are passed in -- Warning - Use the max number present */
                    sprintf(error, "Value parameter tuple %d has more parameters than previous tuple", i + 1);
                    _build_client_err_list(head_error_list, error);
                    err_count++;
                    continue;
                } else if ( numOpts > numOfParam ) {
                    /* If there are less params passed in, than are present
                    * -- Error
                    */
                    sprintf(error, "Value parameter tuple %d has fewer parameters than previous tuple", i + 1);
                    _build_client_err_list(head_error_list, error);
                    err_count++;
                    continue;
                }

                /* Bind values from the parameters_tuple to params */
                curr = stmt_res->head_cache_list;

                while ( curr != NULL ) {
                    data = PyTuple_GET_ITEM(param, j);
                    if ( data == NULL ) {
                        sprintf(error, "NULL value passed for value parameter: %d", i + 1);
                        _build_client_err_list(head_error_list, error);
                        err_count++;
                        break;
                    }

                    if ( chaining_start ) {
                        // This check is not required for python boolean values True and False as both True and False are homogeneous for boolean.
                        if ( ( TYPE(data) != PYTHON_NIL ) && (TYPE(data) != PYTHON_TRUE) && (TYPE(data) != PYTHON_FALSE) && ( ref_data_type[curr->param_num - 1] != TYPE(data) ) && ( ref_data_type[curr->param_num - 1] != PYTHON_NIL) ) {
                            sprintf(error, "Value parameter tuple %d has types that are not homogeneous with previous tuple", i + 1);
                            _build_client_err_list(head_error_list, error);
                            err_count++;
                            break;
                        }
                    } else {
                        if ( TYPE(data) != PYTHON_NIL ) {
                            ref_data_type[curr->param_num -1] = TYPE(data);
                        } else {
                            int i_tmp;
                            PyObject *param_tmp = NULL;
                            PyObject *data_tmp = NULL;
                            i_tmp = i + 1;
                            for ( i_tmp = i + 1; i_tmp < numOfRows; i_tmp++ ) {
                                param_tmp = PyTuple_GET_ITEM(params, i_tmp);
                                if ( !PyTuple_Check(param_tmp) ) {
                                    continue;
                                }
                                data_tmp = PyTuple_GET_ITEM(param_tmp, j);
                                if ( TYPE(data_tmp) != PYTHON_NIL ) {
                                    ref_data_type[curr->param_num -1] = TYPE(data_tmp);
                                    break;
                                } else {
                                    continue;
                                }
                            }
                            if ( ref_data_type[curr->param_num -1] == -1 ) {
                                ref_data_type[curr->param_num -1] = PYTHON_NIL;
                            }
                        }

                    }

                    curr->data_type = data_type[curr->param_num - 1];
                    if ( TYPE(data) != PYTHON_NIL ) {
                        rc = _python_ibm_db_bind_data(stmt_res, curr, data);
                    } else {
                        SQLSMALLINT valueType = 0;
                        switch( ref_data_type[curr->param_num -1] ) {
                            case PYTHON_FIXNUM:
                                if(curr->data_type == SQL_BIGINT || curr->data_type == SQL_DECIMAL ) {
                                    valueType = SQL_C_CHAR;
                                } else {
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
                                switch( curr->data_type ) {
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
                                switch( curr->data_type ) {
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
                        if ( rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO ) {
                            _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                        }
                    }
                    if ( rc != SQL_SUCCESS ) {
                        sprintf(error, "Binding Error 1: %s",
                                IBM_DB_G(__python_stmt_err_msg));
                        _build_client_err_list(head_error_list, error);
                        err_count++;
                        break;
                    }
                    curr = curr->next;
                    j++;
                }

                if ( !chaining_start && ( error[0] == '\0' ) ) {
                    /* Set statement attribute SQL_ATTR_CHAINING_BEGIN */
                    rc = _ibm_db_chaining_flag(stmt_res, SQL_ATTR_CHAINING_BEGIN, NULL, 0);
                    chaining_start = 1;
                    if ( rc != SQL_SUCCESS ) {
                        return NULL;
                    }
                }

                if ( error[0] == '\0' ) {
                    Py_BEGIN_ALLOW_THREADS;
                    rc = SQLExecute((SQLHSTMT)stmt_res->hstmt);
                    Py_END_ALLOW_THREADS;

                    if ( rc == SQL_NEED_DATA ) {
                        SQLPOINTER valuePtr;
                        Py_BEGIN_ALLOW_THREADS;
                        rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
                        Py_END_ALLOW_THREADS;
                        while ( rc == SQL_NEED_DATA ) {
                            /* passing data value for a parameter */
                            if ( !NIL_P(((param_node*)valuePtr)->svalue)) {
                                Py_BEGIN_ALLOW_THREADS;
                                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node*)valuePtr)->svalue), ((param_node*)valuePtr)->ivalue);
                                Py_END_ALLOW_THREADS;
                            } else {
                                Py_BEGIN_ALLOW_THREADS;
                                rc = SQLPutData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER)(((param_node*)valuePtr)->uvalue), ((param_node*)valuePtr)->ivalue);
                                Py_END_ALLOW_THREADS;
                            }
                            if ( rc == SQL_ERROR ) {
                                _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
                                sprintf(error, "Sending data failed: %s", IBM_DB_G(__python_stmt_err_msg));
                                _build_client_err_list(head_error_list, error);
                                err_count++;
                                break;
                            }
                            Py_BEGIN_ALLOW_THREADS;
                            rc = SQLParamData((SQLHSTMT)stmt_res->hstmt, (SQLPOINTER *)&valuePtr);
                            Py_END_ALLOW_THREADS;
                        }
                    }
                    else if (rc == SQL_ERROR)
                    {
                       _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,1, NULL, -1, 1);
                       sprintf(error, "SQLExecute failed: %s", IBM_DB_G(__python_stmt_err_msg));
                       PyErr_SetString(PyExc_Exception, error);
                       _build_client_err_list(head_error_list, error);
                       err_count++;
                       break;
                    }
                }
            }
        } else {
            return PyInt_FromLong(0);

        }

        /* Set statement attribute SQL_ATTR_CHAINING_END */
        rc = _ibm_db_chaining_flag(stmt_res, SQL_ATTR_CHAINING_END, head_error_list->next, err_count);
        if ( head_error_list != NULL ) {
            error_msg_node *tmp_err = NULL;
            while ( head_error_list != NULL ) {
                tmp_err = head_error_list;
                head_error_list = head_error_list->next;
                PyMem_Del(tmp_err);
            }
        }
        if ( rc != SQL_SUCCESS || err_count != 0 ) {
            return NULL;
        }
    } else {
        PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
        return NULL;

    }

    Py_BEGIN_ALLOW_THREADS;
    rc = SQLRowCount((SQLHSTMT)stmt_res->hstmt, &row_cnt);
    Py_END_ALLOW_THREADS;

    if ( (rc == SQL_ERROR) && (stmt_res != NULL) ) {
        _python_ibm_db_check_sql_errors(stmt_res->hstmt, SQL_HANDLE_STMT, rc,1, NULL, -1, 1);
        sprintf(error, "SQLRowCount failed: %s",IBM_DB_G(__python_stmt_err_msg));
        PyErr_SetString(PyExc_Exception, error);
        return NULL;
    }
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
static PyObject* ibm_db_callproc(PyObject *self, PyObject *args){
    PyObject *py_conn_res = NULL;
    PyObject *parameters_tuple = NULL;
    PyObject *outTuple = NULL, *pyprocName = NULL, *data = NULL;
    conn_handle *conn_res = NULL;
    stmt_handle *stmt_res = NULL;
    param_node *tmp_curr = NULL;
    int numOfParam = 0;

    if (!PyArg_ParseTuple(args, "OO|O", &py_conn_res, &pyprocName, &parameters_tuple)) {
        return NULL;
    }

    if (!NIL_P(py_conn_res) && pyprocName != Py_None) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        if (StringObj_Size(pyprocName) == 0) {
            PyErr_SetString(PyExc_Exception, "Empty Procedure Name");
            return NULL;
        }

        if (!NIL_P(parameters_tuple) ) {
            PyObject *subsql1 = NULL;
            PyObject *subsql2 = NULL;
            char *strsubsql = NULL;
            PyObject *sql = NULL;
            int i=0;
            if (!PyTuple_Check(parameters_tuple)) {
                PyErr_SetString(PyExc_Exception, "Param is not a tuple");
                return NULL;
            }
            numOfParam = PyTuple_Size(parameters_tuple);
            subsql1 = StringOBJ_FromASCII("CALL ");
            subsql2 = PyUnicode_Concat(subsql1, pyprocName);
            Py_XDECREF(subsql1);
            strsubsql = (char *)PyMem_Malloc(sizeof(char)*((strlen("(  )") + strlen(", ?")*numOfParam) + 2));
            if (strsubsql == NULL) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return NULL;
            }
            strsubsql[0] = '\0';
            strcat(strsubsql, "( ");
            for (i = 0; i < numOfParam; i++) {
                if (i == 0) {
                    strcat(strsubsql, " ?");
                } else {
                    strcat(strsubsql, ", ?");
                }
            }
            strcat(strsubsql, " )");
            subsql1 = StringOBJ_FromASCII(strsubsql);
            sql = PyUnicode_Concat(subsql2, subsql1);
            Py_XDECREF(subsql1);
            Py_XDECREF(subsql2);
            stmt_res = (stmt_handle *)_python_ibm_db_prepare_helper(conn_res, sql, NULL);
            PyMem_Del(strsubsql);
            Py_XDECREF(sql);
            if(NIL_P(stmt_res)) {
                return NULL;
            }
            /* Bind values from the parameters_tuple to params */
            for (i = 0; i < numOfParam; i++ ) {
                PyObject *bind_result = NULL;
                data = PyTuple_GET_ITEM(parameters_tuple, i);
                bind_result = _python_ibm_db_bind_param_helper(4, stmt_res, i+1, data, SQL_PARAM_INPUT_OUTPUT, 0, 0, 0, 0);
                if (NIL_P(bind_result)){
                    return NULL;
                }
            }
        } else {
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
            stmt_res = (stmt_handle *)_python_ibm_db_prepare_helper(conn_res, sql, NULL);
            Py_XDECREF(sql);
            if(NIL_P(stmt_res)) {
                return NULL;
            }
        }

        if (!NIL_P(_python_ibm_db_execute_helper1(stmt_res, NULL))) {
            tmp_curr = stmt_res->head_cache_list;
            if(numOfParam != 0 && tmp_curr != NULL) {
                int paramCount = 1;
                outTuple = PyTuple_New(numOfParam + 1);
                PyTuple_SetItem(outTuple, 0, (PyObject*)stmt_res);
                while(tmp_curr != NULL && (paramCount <= numOfParam)) {
                    if ( (tmp_curr->bind_indicator != SQL_NULL_DATA && tmp_curr->bind_indicator != SQL_NO_TOTAL )) {
                        switch (tmp_curr->data_type) {
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
                                paramCount++;
                                break;
                            case SQL_REAL:
                            case SQL_FLOAT:
                            case SQL_DOUBLE:
                                PyTuple_SetItem(outTuple, paramCount,
                                            PyFloat_FromDouble(tmp_curr->fvalue));
                                  paramCount++;
                                break;
                            case SQL_TYPE_DATE:
                                if(!NIL_P(tmp_curr->date_value))
                                {
                                    PyTuple_SetItem(outTuple, paramCount,
                                     PyDate_FromDate(tmp_curr->date_value->year,
                                     tmp_curr->date_value->month,
                                     tmp_curr->date_value->day));
                                }
                                else
                                {
                                    Py_INCREF(Py_None);
                                    PyTuple_SetItem(outTuple, paramCount, Py_None);
                                }
                                paramCount++;
                                break;
                            case SQL_TYPE_TIME:
                                if( !NIL_P(tmp_curr->time_value))
                                {
                                    PyTuple_SetItem(outTuple, paramCount,
                                     PyTime_FromTime(tmp_curr->time_value->hour % 24,
                                     tmp_curr->time_value->minute,
                                     tmp_curr->time_value->second, 0));
                                }
                                else
                                {
                                    Py_INCREF(Py_None);
                                    PyTuple_SetItem(outTuple, paramCount, Py_None);
                                }
                                paramCount++;
                                break;
                            case SQL_TYPE_TIMESTAMP:
                                if( !NIL_P(tmp_curr->ts_value ))
                                {
                                    PyTuple_SetItem(outTuple, paramCount,
                                     PyDateTime_FromDateAndTime(tmp_curr->ts_value->year,
                                     tmp_curr->ts_value->month, tmp_curr->ts_value->day,
                                     tmp_curr->ts_value->hour % 24,
                                     tmp_curr->ts_value->minute,
                                     tmp_curr->ts_value->second,
                                     tmp_curr->ts_value->fraction / 1000));
                                }
                                else
                                {
                                    Py_INCREF(Py_None);
                                    PyTuple_SetItem(outTuple, paramCount, Py_None);
                                }
                                paramCount++;
                                break;
                            case SQL_BIGINT:
                                if( !NIL_P(tmp_curr->svalue ))
                                {
                                    PyTuple_SetItem( outTuple, paramCount,
                                     PyLong_FromString(tmp_curr->svalue,
                                     NULL, 0));
                                }
                                else
                                {
                                    Py_INCREF(Py_None);
                                    PyTuple_SetItem(outTuple, paramCount, Py_None);
                                }
                                paramCount++;
                                break;
                            case SQL_BLOB:
                                if( !NIL_P(tmp_curr->svalue ))
                                {
                                    PyTuple_SetItem( outTuple, paramCount,
                                                     PyBytes_FromString(tmp_curr->svalue));
                                }
                                else
                                {
                                    Py_INCREF(Py_None);
                                    PyTuple_SetItem(outTuple, paramCount, Py_None);
                                }
                                paramCount++;
                                break;

                            default:
                                if (!NIL_P(tmp_curr->svalue)) {
                                    PyTuple_SetItem(outTuple, paramCount, StringOBJ_FromASCII(tmp_curr->svalue));
                                    paramCount++;
                                } else if (!NIL_P(tmp_curr->uvalue)) {
                                    PyTuple_SetItem(outTuple, paramCount, getSQLWCharAsPyUnicodeObject(tmp_curr->uvalue, tmp_curr->bind_indicator));
                                    paramCount++;
                                } else {
                                    Py_INCREF(Py_None);
                                    PyTuple_SetItem(outTuple, paramCount, Py_None);
                                    paramCount++;
                                }
                                break;
                        }
                    } else {
                        Py_INCREF(Py_None);
                        PyTuple_SetItem(outTuple, paramCount, Py_None);
                        paramCount++;
                    }
                    tmp_curr = tmp_curr->next;
                }
            } else {
                outTuple = (PyObject *)stmt_res;
            }
        } else {
            return NULL;
        }
        return outTuple;
    } else {
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
static PyObject* ibm_db_check_function_support(PyObject *self, PyObject *args)
{
    PyObject *py_conn_res = NULL;
    PyObject *py_funtion_id = NULL;
    int funtion_id = 0;
    conn_handle *conn_res = NULL;
    int supported = 0;
    int rc = 0;

    if (!PyArg_ParseTuple(args, "OO", &py_conn_res, &py_funtion_id)) {
        return NULL;
    }

    if (!NIL_P(py_conn_res)) {
        if (!PyObject_TypeCheck(py_conn_res, &conn_handleType)) {
            PyErr_SetString( PyExc_Exception, "Supplied connection object Parameter is invalid" );
            return NULL;
        } else {
            conn_res = (conn_handle *)py_conn_res;
        }
        if (!NIL_P(py_funtion_id)) {
            if (PyInt_Check(py_funtion_id)){
                funtion_id = (int) PyInt_AsLong(py_funtion_id);
            } else {
                PyErr_SetString(PyExc_Exception, "Supplied parameter is invalid");
                return NULL;
            }
        }
        /* Check to ensure the connection resource given is active */
        if (!conn_res->handle_active) {
            PyErr_SetString(PyExc_Exception, "Connection is not active");
            return NULL;
         }

        Py_BEGIN_ALLOW_THREADS;
        rc = SQLGetFunctions(conn_res->hdbc, (SQLUSMALLINT) funtion_id, (SQLUSMALLINT*) &supported);
        Py_END_ALLOW_THREADS;

        if (rc == SQL_ERROR) {
            Py_RETURN_FALSE;
        }
        else {
            if(supported == SQL_TRUE) {
                Py_RETURN_TRUE;
            }
            else {
                Py_RETURN_FALSE;
            }
        }

    }
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
    PyObject *stmt = NULL;
    SQLCHAR *value = NULL;
        PyObject *return_value = NULL;
        SQLINTEGER pcbValue = 0;
    stmt_handle *stmt_res;
    int rc = 0;


        PyObject *py_qualifier = NULL;
        PyObject *retVal = NULL;

        if(!PyArg_ParseTuple(args, "O",&py_qualifier))
            return NULL;

        if (!NIL_P(py_qualifier)) {
            if (!PyObject_TypeCheck(py_qualifier, &stmt_handleType)) {
                PyErr_SetString( PyExc_Exception, "Supplied statement object parameter is invalid" );
                return NULL;
            } else {
                stmt_res = (stmt_handle *)py_qualifier;
            }

            /* We allocate a buffer of size 31 as per recommendations from the CLI IDS team */
            value = ALLOC_N(char,31);
            if ( value == NULL ) {
                PyErr_SetString(PyExc_Exception, "Failed to Allocate Memory");
                return Py_False;
            }
            Py_BEGIN_ALLOW_THREADS;
            rc = SQLGetStmtAttr((SQLHSTMT)stmt_res->hstmt, SQL_ATTR_GET_GENERATED_VALUE,(SQLPOINTER)value, 31,&pcbValue);
            Py_END_ALLOW_THREADS;
            if ( rc == SQL_ERROR ) {
               _python_ibm_db_check_sql_errors( (SQLHSTMT)stmt_res->hstmt, SQL_HANDLE_STMT, rc, 1, NULL, -1, 1);
               if(value != NULL) {
                   PyMem_Del(value);
                   value = NULL;
               }
               PyErr_Clear();
               return Py_False;
            }
            retVal = StringOBJ_FromASCII((char *)value);
            if(value != NULL) {
                PyMem_Del(value);
                value = NULL;
            }
            return retVal;
        }
    else {
      PyErr_SetString(PyExc_Exception, "Supplied statement handle is invalid");
      return Py_False;
    }
}

static int _python_get_variable_type(PyObject *variable_value)
{
    if (PyBool_Check(variable_value) && (variable_value == Py_True)){
        return PYTHON_TRUE;
    }
    else if (PyBool_Check(variable_value) && (variable_value == Py_False)){
        return PYTHON_FALSE;
    }
    else if (PyInt_Check(variable_value) || PyLong_Check(variable_value)){
        return PYTHON_FIXNUM;
    }
    else if (PyFloat_Check(variable_value)){
        return PYTHON_FLOAT;
    }
    else if (PyUnicode_Check(variable_value)){
        return PYTHON_UNICODE;
    }
    else if (PyString_Check(variable_value) || PyBytes_Check(variable_value)){
        return PYTHON_STRING;
    }
    else if (PyDateTime_Check(variable_value)){
        return PYTHON_TIMESTAMP;
    }
    else if (PyTime_Check(variable_value)){
        return PYTHON_TIME;
    }
    else if (PyDate_Check(variable_value)){
        return PYTHON_DATE;
    }
    else if (PyComplex_Check(variable_value)){
        return PYTHON_COMPLEX;
    }
    else if (PyNumber_Check(variable_value)){
        return PYTHON_DECIMAL;
    }
    else if (PyList_Check(variable_value)){
            return PYTHON_LIST;
        }
    else if (variable_value == Py_None){
        return PYTHON_NIL;
    }
    else return 0;
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
    {"createdbNX", (PyCFunction)ibm_db_createdbNX, METH_VARARGS, "createdbNX" },
    {"cursor_type", (PyCFunction)ibm_db_cursor_type, METH_VARARGS, "Returns the cursor type used by a statement resource"},
    {"dropdb", (PyCFunction)ibm_db_dropdb, METH_VARARGS, "Drop db"},
    {"execute_many", (PyCFunction)ibm_db_execute_many, METH_VARARGS, "Execute SQL with multiple rows."},
    {"field_display_size", (PyCFunction)ibm_db_field_display_size, METH_VARARGS, "Returns the maximum number of bytes required to display a column"},
    {"field_name", (PyCFunction)ibm_db_field_name, METH_VARARGS, "Returns the name of the column in the result set"},
    {"field_nullable", (PyCFunction)ibm_db_field_nullable, METH_VARARGS, "Returns indicated column can contain nulls or not"},
    {"field_num", (PyCFunction)ibm_db_field_num, METH_VARARGS, "Returns the position of the named column in a result set"},
    {"field_precision", (PyCFunction)ibm_db_field_precision, METH_VARARGS, "Returns the precision of the indicated column in a result set"},
    {"field_scale", (PyCFunction)ibm_db_field_scale , METH_VARARGS, "Returns the scale of the indicated column in a result set"},
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
    {"stmt_warn",(PyCFunction)ibm_db_stmt_warn, METH_VARARGS, "Returns a warning string containing the SQLSTATE returned by last SQL statement"},
    {"stmt_errormsg", (PyCFunction)ibm_db_stmt_errormsg, METH_VARARGS, "Returns a string containing the last SQL statement error message"},
    {"table_privileges", (PyCFunction)ibm_db_table_privileges, METH_VARARGS, "Returns a result set listing the tables and associated privileges in a database"},
    {"tables", (PyCFunction)ibm_db_tables, METH_VARARGS, "Returns a result set listing the tables and associated metadata in a database"},
    {"get_last_serial_value", (PyCFunction)ibm_db_get_last_serial_value, METH_VARARGS, "Returns last serial value inserted for identity column"},
    /* An end-of-listing sentinel: */
    {NULL, NULL, 0, NULL}
};

#ifndef PyMODINIT_FUNC    /* declarations for DLL import/export */
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
INIT_ibm_db(void) {
    PyObject* m;

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
    m = Py_InitModule3("ibm_db", ibm_db_Methods,  "IBM DataServer Driver for Python.");
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
    return MOD_RETURN_VAL(m);
}
