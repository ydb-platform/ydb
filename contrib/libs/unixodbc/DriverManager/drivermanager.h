#ifndef _DRIVERMANAGER_H
#define _DRIVERMANAGER_H

#define ODBCVER 0x0380

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif
#include <ltdl.h>
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h> 
#endif

#ifdef HAVE_SYNCH_H
#error #include <synch.h>
#endif
    
#ifdef HAVE_LIBPTH
#include <pth.h>
#elif HAVE_LIBPTHREAD
#include <pthread.h>
#elif HAVE_LIBTHREAD
#include <thread.h>
#endif

#define SQL_NOUNICODEMAP
#define  UNICODE

#include <log.h>
#include <ini.h>
#include <odbcinstext.h>
#include <sqlext.h>                     /* THIS WILL BRING IN sql.h and
                                           sqltypes.h AS WELL AS PROVIDE
                                           MS EXTENSIONS */
#include <sqlucode.h>
#include "__stats.h"

/*
 * iconv support
 */

#ifdef HAVE_ICONV
#include <stdlib.h>
#include <iconv.h>
#endif

#ifdef UNICODE_ENCODING
#define DEFAULT_ICONV_ENCODING      UNICODE_ENCODING 
#else
#define DEFAULT_ICONV_ENCODING      "auto-search"
#endif

#define ERROR_PREFIX        "[unixODBC]"
#define DM_ERROR_PREFIX     "[Driver Manager]"
#define LOG_MESSAGE_LEN     128         /* length of string to display in log */

/*
 * SQLSetStmt/ConnectionAttr limits
 */

#define SQL_CONN_DRIVER_MIN     20000
#define SQL_STMT_DRIVER_MIN     20000

/*
 * its possible that the driver has a different definition of a handle to the driver
 * manager, DB2 64bit is a example of this
 */

#define DRV_SQLHANDLE			SQLHANDLE
#define DRV_SQLHDESC			SQLHDESC

/*
 * DEFAULT FILE NAMES
 *
 */

/*
 * magic numbers
 */

#define HENV_MAGIC      19289
#define HDBC_MAGIC      19290
#define HSTMT_MAGIC     19291
#define HDESC_MAGIC     19292

/*
 * states
 */

#define STATE_E0        0
#define STATE_E1        1
#define STATE_E2        2

#define STATE_C0        0
#define STATE_C1        1
#define STATE_C2        2
#define STATE_C3        3
#define STATE_C4        4
#define STATE_C5        5
#define STATE_C6        6

#define STATE_S0        0
#define STATE_S1        1
#define STATE_S2        2
#define STATE_S3        3
#define STATE_S4        4
#define STATE_S5        5
#define STATE_S6        6
#define STATE_S7        7
#define STATE_S8        8
#define STATE_S9        9
#define STATE_S10       10
#define STATE_S11       11
#define STATE_S12       12

#define STATE_S13       13          /* SQLExecute/SQLExecDirect/SQLMoreResult  returned SQL_PARAM_DATA_AVAILABLE */
#define STATE_S14       14          /* SQL_PARAM_DATA_AVAILABLE version of S9, Must Get */
#define STATE_S15       15          /* SQL_PARAM_DATA_AVAILABLE version of S10, Can Get */

#define STATE_D0        0
#define STATE_D1i       1
#define STATE_D1e       2

/*
 * rules to extract diag/error record from driver
 */
#define DEFER_R0        0            /* Not defer extracting diag/err record from driver. Use this as default */
#define DEFER_R1        1            /* defer extracting diag/err record from driver when SQL_SUCCESS_WITH_INFO is returned */
#define DEFER_R2        2            /* defer extracting diag/err record from driver when SQL_ERROR is returned */
#define DEFER_R3        3            /* defer extracting diag/err record from driver when SQL_SUCCESS_WITH_INFO or SQL_ERROR is returned */

/*
 * structure to contain the loaded lib entry points
 */

struct driver_func
{
    int         ordinal;
    char        *name;
    void        *dm_func;               /* this is to fix what seems a bug in */
			                            /* some dlopen implemnations where dlsym */
					                    /* will return the driver manager func */
					                    /* not the driver one */
    void        *dm_funcW;
    SQLRETURN   (*func)();
    SQLRETURN   (*funcW)();             /* function with a unicode W */
    SQLRETURN   (*funcA)();             /* function with a unicode A */
    int         can_supply;             /* this is used to indicate that */
                                        /* the DM can execute the function */
                                        /* even if the driver does not */
                                        /* supply it */
};

typedef struct error
{
    SQLWCHAR    sqlstate[ 6 ];
    SQLWCHAR    *msg;
    SQLINTEGER  native_error;
    int         return_val;
    SQLRETURN   diag_column_number_ret;
    SQLRETURN   diag_row_number_ret;
    SQLRETURN   diag_class_origin_ret;
    SQLRETURN   diag_subclass_origin_ret;
    SQLRETURN   diag_connection_name_ret;
    SQLRETURN   diag_server_name_ret;
    SQLINTEGER  diag_column_number;
    SQLLEN      diag_row_number;
    SQLWCHAR    diag_class_origin[ 128 ];
    SQLWCHAR    diag_subclass_origin[ 128 ];
    SQLWCHAR    diag_connection_name[ 128 ];
    SQLWCHAR    diag_server_name[ 128 ];
    struct error *next;
    struct error *prev;

} ERROR;

typedef struct error_header
{
    int         error_count;
    ERROR       *error_list_head;
    ERROR       *error_list_tail;
    int         internal_count;
    ERROR       *internal_list_head;
    ERROR       *internal_list_tail;
} EHEADER;

typedef struct error_head
{
    EHEADER     sql_error_head;
    EHEADER     sql_diag_head;
    void        *owning_handle;
    int         handle_type;
    SQLRETURN   return_code;
    SQLINTEGER  header_set;
    SQLRETURN   diag_cursor_row_count_ret;
    SQLRETURN   diag_dynamic_function_ret;
    SQLRETURN   diag_dynamic_function_code_ret;
    SQLRETURN   diag_number_ret;
    SQLRETURN   diag_row_count_ret;
    SQLLEN      diag_cursor_row_count;
    SQLWCHAR    diag_dynamic_function[ 128 ];
    SQLINTEGER  diag_dynamic_function_code;
    SQLLEN      diag_number;
    SQLLEN      diag_row_count;
    int         defer_extract;   /* determine the extraction of driver's message for
                                           SQLGetDiagRec or SQLGetDiagField.
                                           0 by default is not deferred */
    SQLRETURN   ret_code_deferred; /* used for deferring extraction */
} EHEAD;

struct log_structure
{
    char    *program_name;
    char    *log_file_name;
    int     log_flag;
    int     pid_logging;            /* the log path specifies a directory, and a */
                                    /* log file per pid is created */
    int     ref_count;              /* number of times dm_log_open()'d without dm_log_close() */
};

extern struct log_structure log_info;

/*
 * save connection attr untill after the connect, and then pass on
 */

struct save_attr
{
    int                 attr_type;
    char                *str_attr;
    int                 str_len;
    intptr_t            intptr_attr;
    struct save_attr    *next;
};

/*
 * attribute extension support
 */

struct attr_set
{
    char            *keyword;
    char            *value;
    int             override;
    int             attribute;
    int             is_int_type;
    int             int_value;
    struct attr_set *next;
};

struct attr_struct
{
    int             count;
    struct attr_set *list;
};

int __parse_attribute_string( struct attr_struct *attr_str,
    char *str, int str_len );
void __release_attr_str( struct attr_struct *attr_str );
void __set_attributes( void *handle, int type );
void __set_local_attributes( void *handle, int type );
void *__attr_override( void *handle, int type, int attribute, void * value, SQLINTEGER *string_length );
void *__attr_override_wide( void *handle, int type, int attribute, void * value, SQLINTEGER *string_length, SQLWCHAR *buffer );

/*
 * use this to maintain a list of the drivers that are loaded under this env,
 * and to decide if we want to call SQLAllocHandle( SQL_ENV ) om them
 */

struct env_lib_struct
{
    char            *lib_name;
    DRV_SQLHANDLE   env_handle;
    int             count;
    int             driver_act_ver;     /* real version of the driver, keep in the list instead of the env, as 
                                         * we may have both V2 and V3 drivers in use */
    struct env_lib_struct   *next;
};

typedef struct environment
{
    int             type;               /* magic number */
    struct environment *next_class_list;/* static list of all env handles */
    char            msg[ LOG_MSG_MAX*2 ];	/* buff to format msgs */
    int             state;              /* state of environment */
    int             version_set;        /* whether ODBC version has been set */
    SQLINTEGER      requested_version;  /* SQL_OV_ODBC2 or SQL_OV_ODBC3 */
    int             connection_count;   /* number of hdbc of this env */
    int             sql_driver_count;   /* used for SQLDrivers */
    EHEAD           error;              /* keep track of errors */
    SQLINTEGER      connection_pooling; /* does connection pooling operate */
    SQLINTEGER      cp_match;
    int             fetch_mode;         /* for SQLDataSources */
    int             entry;
    void            *sh;                /* statistics handle */
    int             released;           /* Catch a race condition in SQLAPI lib */
    struct env_lib_struct *env_lib_list;/* use this to avoid multiple AllocEnv in the driver */
} *DMHENV;


#ifdef FAST_HANDLE_VALIDATE
    struct statement;
#endif

/*
 * connection pooling attributes
 */

typedef struct connection
{
    int             type;               /* magic number */
    struct connection *next_class_list; /* static list of all dbc handles */
    char            msg[ LOG_MSG_MAX*2 ]; /* buff to format msgs */
    int             state;              /* state of connection */
    DMHENV          environment;        /* environment that own's the
                                           connection */
#ifdef FAST_HANDLE_VALIDATE
    struct statement *statements;       /* List of statements owned by this 
                                           connection */
#endif
    
    void            *dl_handle;         /* handle of the loaded lib */
    char            dl_name[ 256 ];     /* name of loaded lib */
    struct driver_func *functions;      /* entry points */
    struct driver_func  ini_func;       /* optinal start end functions */
    struct driver_func  fini_func;
    int             unicode_driver;     /* do we use the W functions in the */
                                        /* driver ? */
    DRV_SQLHANDLE 	driver_env;         /* environment handle in client */
    DRV_SQLHANDLE   driver_dbc;         /* connection handle in client */
    int             driver_version;     /* required version of the connected */
                                        /* driver */
    int             driver_act_ver;     /* real version of the driver */
    int             statement_count;    /* number of statements on this dbc */
    EHEAD           error;              /* keep track of errors */
    char            dsn[ SQL_MAX_DSN_LENGTH + 1 ];  /* where we are connected */
    int             access_mode;        /* variables set via SQLSetConnectAttr */
    int             access_mode_set;      
    int             login_timeout;
    int             login_timeout_set;
    int             auto_commit;
    int             auto_commit_set;
    int             async_enable;
    int             async_enable_set;
    int             auto_ipd;
    int             auto_ipd_set;
    int             connection_timeout;
    int             connection_timeout_set;
    int             metadata_id;
    int             metadata_id_set;
    int             packet_size;
    int             packet_size_set;
    SQLLEN          quite_mode;
    int             quite_mode_set;
    int             txn_isolation;
    int             txn_isolation_set;

    SQLINTEGER      cursors;
    void            *cl_handle;         /* handle to the cursor lib */
    int             trace;
    char            tracefile[ INI_MAX_PROPERTY_VALUE + 1 ];
#ifdef HAVE_LIBPTH
    pth_mutex_t     mutex;              /* protect the object */
    int             protection_level;
#elif HAVE_LIBPTHREAD
    pthread_mutex_t mutex;              /* protect the object */
    int             protection_level;
#elif HAVE_LIBTHREAD
    mutex_t mutex;              		/* protect the object */
    int             protection_level;
#endif
    int             ex_fetch_mapping;   /* disable SQLFetch -> SQLExtendedFetch */
    int             disable_gf;         /* dont call SQLGetFunctions in the driver */
    int             dont_dlclose;       /* disable dlclosing of the handle */
    int             bookmarks_on;       /* bookmarks are set on */
    void            *pooled_connection; /* points to t connection pool structure */
    int             pooling_timeout;
    int             ttl;
    char            *_driver_connect_string;
    int             dsn_length;
    char            *_server;
    int             server_length;
    char            *_user;
    int             user_length;
    char            *_password;
    int             password_length;
    char            cli_year[ 5 ];
    struct attr_struct  env_attribute;      /* Extended attribute set info */
    struct attr_struct  dbc_attribute;
    struct attr_struct  stmt_attribute;
    struct save_attr    *save_attr;         /* SQLConnectAttr before connect */
#ifdef HAVE_ICONV
    iconv_t         iconv_cd_uc_to_ascii;   /* in and out conversion descriptor */
    iconv_t         iconv_cd_ascii_to_uc;
    char            unicode_string[ 64 ];   /* name of unicode conversion */
#endif
    struct env_lib_struct *env_list_ent;    /* pointer to reference in the env list */
    char            probe_sql[ 512 ];       /* SQL to use to check a pool is valid */
	int				threading_level;		/* level of thread protection the DM proves */
	int				cbs_found;				/* Have we queried the driver for the effect of a */
	SQLSMALLINT		ccb_value;				/* COMMIT or a ROLLBACK */
	SQLSMALLINT		crb_value;
} *DMHDBC;

struct connection_pool_head;

typedef struct connection_pool_entry
{
    time_t  expiry_time;
    int     ttl;
    int     timeout;
    int     in_use;
    struct  connection_pool_entry *next;
    struct  connection_pool_head *head;
    struct  connection connection;
    int     cursors;
} CPOOLENT;

typedef struct connection_pool_head
{
    struct connection_pool_head *next;

    char    *_driver_connect_string;
    int     dsn_length;
    char    *_server;
    int     server_length;
    char    *_user;
    int     user_length;
    char    *_password;
    int     password_length;

    volatile int num_entries;                /* always at least 1 */
    CPOOLENT *entries;
} CPOOLHEAD;

void pool_unreserve( CPOOLHEAD *pooh );

typedef struct descriptor
{
    int             type;               /* magic number */
    struct descriptor *next_class_list; /* static list of all desc handles */
    char            msg[ LOG_MSG_MAX*2 ]; /* buff to format msgs */
    int             state;              /* state of descriptor */

#ifdef FAST_HANDLE_VALIDATE
    struct descriptor *prev_class_list;/* static list of all desc handles */
#endif    

    EHEAD           error;              /* keep track of errors */
    DRV_SQLHDESC    driver_desc;        /* driver descriptor */
    DMHDBC          connection;         /* DM connection that owns this */
    int             implicit;           /* created by a AllocStmt */
    void            *associated_with;   /* statement that this is a descriptor of */
#ifdef HAVE_LIBPTH
    pth_mutex_t     mutex;              /* protect the object */
#elif HAVE_LIBPTHREAD
    pthread_mutex_t mutex;              /* protect the object */
#elif HAVE_LIBTHREAD
    mutex_t mutex;              		/* protect the object */
#endif
} *DMHDESC;

typedef struct statement
{
    int             type;               /* magic number */
    struct statement *next_class_list;  /* static list of all stmt handles */
    char            msg[ LOG_MSG_MAX*2 ]; /* buff to format msgs */
    int             state;              /* state of statement */
#ifdef FAST_HANDLE_VALIDATE
    struct statement *prev_class_list;  /* static list of all stmt handles */
    struct statement *next_conn_list;   /* Single linked list storing statements 
                                           owned by "connection" connection */
#endif
    DMHDBC          connection;         /* DM connection that owns this */
    DRV_SQLHANDLE   driver_stmt;        /* statement in the driver */
    SQLSMALLINT     hascols;            /* is there a result set */
    int             prepared;           /* the statement has been prepared */
    int             interupted_func;    /* current function running async */
                                        /* or NEED_DATA */
    int             interupted_state;   /* state we went into need data or */
                                        /* still executing from */
    int             bookmarks_on;       /* bookmarks are set on */
    EHEAD           error;              /* keep track of errors */
    SQLINTEGER      metadata_id;
    DMHDESC         ipd;                /* current descriptors */
    DMHDESC         apd;
    DMHDESC         ird;
    DMHDESC         ard;
    DMHDESC         implicit_ipd;       /* implicit descriptors */
    DMHDESC         implicit_apd;
    DMHDESC         implicit_ird;
    DMHDESC         implicit_ard;
    SQLULEN		    *fetch_bm_ptr;      /* Saved for ODBC3 to ODBC2 mapping */ 
    SQLULEN     	*row_ct_ptr;        /* row count ptr */
    SQLUSMALLINT    *row_st_arr;        /* row status array */
    SQLULEN     	row_array_size;
	SQLPOINTER      valueptr;           /* Default buffer for SQLParamData() */

#ifdef HAVE_LIBPTH
    pth_mutex_t     mutex;              /* protect the object */
#elif HAVE_LIBPTHREAD
    pthread_mutex_t mutex;              /* protect the object */
#elif HAVE_LIBTHREAD
    mutex_t mutex;              		/* protect the object */

#endif

    int             eod;                /* when in S6 has EOD been returned */
} *DMHSTMT;

#if defined ( HAVE_LIBPTHREAD ) || defined ( HAVE_LIBTHREAD ) || defined ( HAVE_LIBPTH )
#define TS_LEVEL0   0           /* no implicit protection, only for */
                                /* dm internal structures */
#define TS_LEVEL1   1           /* protection on a statement level */
#define TS_LEVEL2   2           /* protection on a connection level */
#define TS_LEVEL3   3           /* protection on a environment level */
#endif

void mutex_lib_entry( void );
void mutex_lib_exit( void );

void mutex_pool_entry( void );
void mutex_pool_exit( void );

void mutex_iconv_entry( void );
void mutex_iconv_exit( void );

typedef struct connection_pair
{
    char            *name;
    char            *value;
    struct connection_pair *next;
} *connection_attribute;

/*
 * defined down here to get the DMHDBC definition
 */

void __handle_attr_extensions( DMHDBC connection, char *dsn, char *driver_name );

/*
 * handle allocation functions
 */

DMHENV __share_env( int *first );
DMHENV __alloc_env( void );
int __validate_env( DMHENV );
void __release_env( DMHENV environment );
int __validate_env_mark_released( DMHENV env );

DMHDBC __alloc_dbc( void );
int __validate_dbc( DMHDBC );
void __release_dbc( DMHDBC connection );

DMHSTMT __alloc_stmt( void );
void __register_stmt ( DMHDBC connection, DMHSTMT statement );
void __set_stmt_state ( DMHDBC connection, SQLSMALLINT cb_value );
int __validate_stmt( DMHSTMT );
void __release_stmt( DMHSTMT );

DMHDESC __alloc_desc( void );
int __validate_desc( DMHDESC );
void __release_desc( DMHDESC );

/*
 * generic functions
 */

SQLRETURN __SQLAllocHandle( SQLSMALLINT handle_type,
           SQLHANDLE input_handle,
           SQLHANDLE *output_handle,
           SQLINTEGER requested_version );

SQLRETURN __SQLFreeHandle( SQLSMALLINT handle_type,
           SQLHANDLE handle );

SQLRETURN __SQLGetInfo( SQLHDBC connection_handle,
		 	SQLUSMALLINT info_type,
			SQLPOINTER info_value,
			SQLSMALLINT buffer_length,
			SQLSMALLINT *string_length );

int __connect_part_one( DMHDBC connection, char *driver_lib, char *driver_name, int *warnings );
void __disconnect_part_one( DMHDBC connection );
int __connect_part_two( DMHDBC connection );
void __disconnect_part_two( DMHDBC connection );
void __disconnect_part_three( DMHDBC connection );
void __disconnect_part_four( DMHDBC connection );
DMHDBC __get_dbc_root( void );

void  __check_for_function( DMHDBC connection,
        SQLUSMALLINT function_id,
        SQLUSMALLINT *supported );

int __clean_stmt_from_dbc( DMHDBC connection );
int __clean_desc_from_dbc( DMHDBC connection );
void __map_error_state( char * state, int requested_version );
void __map_error_state_w( SQLWCHAR * wstate, int requested_version );

/*
 * mapping from ODBC 2 <-> 3 datetime types
 */

#define MAP_SQL_DM2D 	0
#define MAP_SQL_D2DM 	1
#define MAP_C_DM2D 	2
#define MAP_C_D2DM 	3

SQLSMALLINT __map_type( int map, DMHDBC connection, SQLSMALLINT type);

/*
 * error functions
 */

typedef enum error_id
{
    ERROR_01000,
    ERROR_01004,
    ERROR_01S02,
    ERROR_01S06,
    ERROR_07005,
    ERROR_07009,
    ERROR_08002,
    ERROR_08003,
    ERROR_24000,
    ERROR_25000,
    ERROR_25S01,
    ERROR_S1000,
    ERROR_S1003,
    ERROR_S1010,
    ERROR_S1011,
    ERROR_S1107,
    ERROR_S1108,
    ERROR_S1C00,
    ERROR_HY001,
    ERROR_HY003,
    ERROR_HY004,
    ERROR_HY007,
    ERROR_HY009,
    ERROR_HY010,
    ERROR_HY011,
    ERROR_HY012,
    ERROR_HY013,
    ERROR_HY017,
    ERROR_HY024,
    ERROR_HY090,
    ERROR_HY092,
    ERROR_HY095,
    ERROR_HY097,
    ERROR_HY098,
    ERROR_HY099,
    ERROR_HY100,
    ERROR_HY101,
    ERROR_HY103,
    ERROR_HY105,
    ERROR_HY106,
    ERROR_HY110,
    ERROR_HY111,
    ERROR_HYC00,
    ERROR_IM001,
    ERROR_IM002,
    ERROR_IM003,
    ERROR_IM004,
    ERROR_IM005,
    ERROR_IM010,
    ERROR_IM012,
    ERROR_SL004,
    ERROR_SL009,
    ERROR_SL010,
    ERROR_SL008,
    ERROR_HY000,
    ERROR_IM011,
    ERROR_HYT02
} error_id;

#define IGNORE_THREAD       (-1)

#define function_return(l,h,r,d)    function_return_ex(l,h,r,FALSE,d)

#define SUBCLASS_ODBC           0
#define SUBCLASS_ISO            1

void __post_internal_error( EHEAD *error_handle,
        error_id, char *txt, int connection_mode );
void __post_internal_error_api( EHEAD *error_handle,
        error_id, char *txt, int connection_mode, int calling_api );
void __post_internal_error_ex( EHEAD *error_handle,
        SQLCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLCHAR *message_text,
        int class_origin,
        int subclass_origin );
void __post_internal_error_ex_noprefix( EHEAD *error_handle,
        SQLCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLCHAR *message_text,
        int class_origin,
        int subclass_origin );
void __post_internal_error_ex_w( EHEAD *error_handle,
        SQLWCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLWCHAR *message_text,
        int class_origin,
        int subclass_origin );
void __post_internal_error_ex_w_noprefix( EHEAD *error_handle,
        SQLWCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLWCHAR *message_text,
        int class_origin,
        int subclass_origin );

void extract_error_from_driver( EHEAD * error_handle,
        DMHDBC hdbc,
        int ret_code,
        int save_to_diag );

int function_return_nodrv( int level, void *handle, int ret_code );
int function_return_ex( int level, void * handle, int ret_code, int save_to_diag, int defer_type );
void function_entry( void *handle );
void setup_error_head( EHEAD *error_header, void *handle, int handle_type );
void clear_error_head( EHEAD *error_header );
SQLWCHAR *ansi_to_unicode_copy( SQLWCHAR * dest, char *src, SQLINTEGER buffer_len, DMHDBC connection, int *wlen );
SQLWCHAR *ansi_to_unicode_alloc( SQLCHAR *str, SQLINTEGER len, DMHDBC connection, int *wlen );
char *unicode_to_ansi_copy( char* dest, int dest_len, SQLWCHAR *src, SQLINTEGER len, DMHDBC connection, int *clen );
char *unicode_to_ansi_alloc( SQLWCHAR *str, SQLINTEGER len, DMHDBC connection, int *clen );
int unicode_setup( DMHDBC connection );
void unicode_shutdown( DMHDBC connection );
char * __get_return_status( SQLRETURN ret, SQLCHAR *buffer );
char * __sql_as_text( SQLINTEGER type );
char * __c_as_text( SQLINTEGER type );
char * __string_with_length( SQLCHAR *out, SQLCHAR *str, SQLINTEGER len );
char * __string_with_length_pass( SQLCHAR *out, SQLCHAR *str, SQLINTEGER len );
char * __string_with_length_hide_pwd( SQLCHAR *out, SQLCHAR *str, SQLINTEGER len );
char * __wstring_with_length( SQLCHAR *out, SQLWCHAR *str, SQLINTEGER len );
char * __wstring_with_length_pass( SQLCHAR *out, SQLWCHAR *str, SQLINTEGER len );
char * __wstring_with_length_hide_pwd( SQLCHAR *out, SQLWCHAR *str, SQLINTEGER len );
SQLWCHAR *wide_strcpy( SQLWCHAR *str1, SQLWCHAR *str2 );
SQLWCHAR *wide_strncpy( SQLWCHAR *str1, SQLWCHAR *str2, int buffer_length );
SQLWCHAR *wide_strcat( SQLWCHAR *str1, SQLWCHAR *str2 );
SQLWCHAR *wide_strdup( SQLWCHAR *str1 );
int wide_strlen( SQLWCHAR *str1 );
int wide_ansi_strncmp( SQLWCHAR *str1, char *str2, int len );
char * __get_pid( SQLCHAR *str );
char * __iptr_as_string( SQLCHAR *s, SQLINTEGER *ptr );
char * __ptr_as_string( SQLCHAR *s, SQLLEN *ptr );
char * __sptr_as_string( SQLCHAR *s, SQLSMALLINT *ptr );
char * __info_as_string( SQLCHAR *s, SQLINTEGER typ );
void __clear_internal_error( struct error *error_handle );
char * __data_as_string( SQLCHAR *s, SQLINTEGER type, 
        SQLLEN *ptr, SQLPOINTER buf );
char * __sdata_as_string( SQLCHAR *s, SQLINTEGER type, 
        SQLSMALLINT *ptr, SQLPOINTER buf );
char * __idata_as_string( SQLCHAR *s, SQLINTEGER type, 
        SQLINTEGER *ptr, SQLPOINTER buf );
char * __col_attr_as_string( SQLCHAR *s, SQLINTEGER type );
char * __fid_as_string( SQLCHAR *s, SQLINTEGER fid );
char * __con_attr_as_string( SQLCHAR *s, SQLINTEGER type );
char * __env_attr_as_string( SQLCHAR *s, SQLINTEGER type );
char * __stmt_attr_as_string( SQLCHAR *s, SQLINTEGER type );
char * __desc_attr_as_string( SQLCHAR *s, SQLINTEGER type );
char * __diag_attr_as_string( SQLCHAR *s, SQLINTEGER type );
char * __type_as_string( SQLCHAR *s, SQLSMALLINT type );
DMHDBC __get_connection( EHEAD * head );
DRV_SQLHANDLE __get_driver_handle( EHEAD * head );
int __get_version( EHEAD * head );
int dm_check_connection_attrs( DMHDBC connection, SQLINTEGER attribute, SQLPOINTER value );
int dm_check_statement_attrs( DMHSTMT statement, SQLINTEGER attribute, SQLPOINTER value );
int __check_stmt_from_dbc( DMHDBC connection, int state );
#define MAX_STATE_ARGS  8
int __check_stmt_from_dbc_v( DMHDBC connection, int statecount, ... );
int __check_stmt_from_desc( DMHDESC desc, int state );
int __check_stmt_from_desc_ird( DMHDESC desc, int state );

/* 
 * These are passed to the cursor lib as helper functions
 */

struct driver_helper_funcs
{
    void (*__post_internal_error_ex)( EHEAD *error_header,
            SQLCHAR *sqlstate,
            SQLINTEGER native_error,
            SQLCHAR *message_text,
            int class_origin,
            int subclass_origin );

    void (*__post_internal_error)( EHEAD *error_handle,
        error_id id, char *txt, int connection_mode );
    void (*dm_log_write)( char *function_name, int line, int type, int severity,
        char *message );
};

/*
 * thread protection funcs
 */

#if defined ( HAVE_LIBPTHREAD ) || defined ( HAVE_LIBTHREAD ) || defined ( HAVE_LIBPTH )

void thread_protect( int type, void *handle );
void thread_release( int type, void *handle );
int pool_timedwait( DMHDBC );
void pool_signal();

#else

#define thread_protect(a,b)
#define thread_release(a,b)
#define pool_timedwait(a)
#define pool_signal()

#endif

void dbc_change_thread_support( DMHDBC connection, int level );

#ifdef WITH_HANDLE_REDIRECT

void *find_parent_handle( DRV_SQLHANDLE hand, int type );

#endif

/*
 * lookup functions
 */

char *__find_lib_name( char *dsn, char *lib_name, char *driver_name );

/*
 * setup the cursor library
 */

SQLRETURN SQL_API CLConnect( DMHDBC connection, struct driver_helper_funcs * );

/*
 * connection string functions
 */

struct con_pair
{
    char            *keyword;
    char            *attribute;
    char            *identifier;
    struct con_pair *next;
};

struct con_struct
{
    int             count;
    struct con_pair *list;
};

void __generate_connection_string( struct con_struct *con_str, char *str, int str_len );
int __parse_connection_string( struct con_struct *con_str,
    char *str, int str_len );
int __parse_connection_string_w( struct con_struct *con_str,
    SQLWCHAR *str, int str_len );
char * __get_attribute_value( struct con_struct * con_str, char * keyword );
void __release_conn( struct con_struct *con_str );
void __get_attr( char ** cp, char ** keyword, char ** value );
struct con_pair * __get_pair( char ** cp );
int __append_pair( struct con_struct *con_str, char *kword, char *value );
void __handle_attr_extensions_cs( DMHDBC connection, struct con_struct *con_str );
void __strip_from_pool( DMHENV env );

void extract_diag_error_w( int htype,
                            DRV_SQLHANDLE handle,
                            DMHDBC connection,
                            EHEAD *head,
                            int return_code,
                            int save_to_diag );

void extract_diag_error( int htype,
                            DRV_SQLHANDLE handle,
                            DMHDBC connection,
                            EHEAD *head,
                            int return_code,
                            int save_to_diag );

void extract_sql_error_w( DRV_SQLHANDLE henv,
                            DRV_SQLHANDLE hdbc,
                            DRV_SQLHANDLE hstmt,
                            DMHDBC connection,
                            EHEAD *head, 
                            int return_code );

void extract_sql_error( DRV_SQLHANDLE henv,
                            DRV_SQLHANDLE hdbc,
                            DRV_SQLHANDLE hstmt,
                            DMHDBC connection,
                            EHEAD *head, 
                            int return_code );
/*
 * the following two are part of a effort to get a particular unicode driver working
 */

SQLINTEGER map_ca_odbc3_to_2( SQLINTEGER field_identifier );
SQLINTEGER map_ca_odbc2_to_3( SQLINTEGER field_identifier );

/*
 * check the type passed to SQLBindCol is a valid C_TYPE
 */

int check_target_type( int c_type, int connection_mode);

/*
 * entry exit functions in drivers
 */

#define ODBC_INI_FUNCTION           "SQLDriverLoad"
#define ODBC_FINI_FUNCTION          "SQLDriverUnload"

/*
 * driver manager logging functions
 */

void dm_log_open( char *program_name, char *log_file, int pid_logging );

void dm_log_write( char *function_name, int line, int type, int severity, char *message );
void dm_log_write_diag( char *message );

void dm_log_close( void );

/*
 * connection pooling functions
 */

int search_for_pool( DMHDBC connection,
           SQLCHAR *server_name,
           SQLSMALLINT name_length1,
           SQLCHAR *user_name,
           SQLSMALLINT name_length2,
           SQLCHAR *authentication,
           SQLSMALLINT name_length3,
           SQLCHAR *connect_string,
           SQLSMALLINT connect_string_length,
           CPOOLHEAD **pooh,
           int retrying );

void return_to_pool( DMHDBC connection );

int add_to_pool( DMHDBC connection, CPOOLHEAD *pooh );

/*
 * Macros to check and call functions in the driver
 */

#define DM_SQLALLOCCONNECT          0
#define CHECK_SQLALLOCCONNECT(con)  (con->functions[0].func!=NULL)
#define SQLALLOCCONNECT(con,env,oh)\
                                    ((SQLRETURN (*)(SQLHENV, SQLHDBC*))\
                                    con->functions[0].func)(env,oh)

#define DM_SQLALLOCENV              1
#define CHECK_SQLALLOCENV(con)      (con->functions[1].func!=NULL)
#define SQLALLOCENV(con,oh)\
                                    ((SQLRETURN (*)(SQLHENV*))\
                                    con->functions[1].func)(oh)

#define DM_SQLALLOCHANDLE           2
#define CHECK_SQLALLOCHANDLE(con)   (con->functions[2].func!=NULL)
    /*
     * if the function is in the cursor lib, pass a additional
     * arg that allows the cursor lib to get the dm handle
     */
#define SQLALLOCHANDLE(con,ht,ih,oh,dmh)\
            (con->cl_handle?\
                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE, SQLHANDLE*, SQLHANDLE))\
                     con->functions[2].func)(ht,ih,oh,dmh):\
                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE, SQLHANDLE*))\
                     con->functions[2].func)(ht,ih,oh))

#define DM_SQLALLOCSTMT             3
#define CHECK_SQLALLOCSTMT(con)     (con->functions[3].func!=NULL)
#define SQLALLOCSTMT(con,dbc,oh,dmh)\
            (con->cl_handle?\
                    ((SQLRETURN (*)(SQLHDBC, SQLHSTMT*, SQLHANDLE))\
                     con->functions[3].func)(dbc,oh,dmh):\
                    ((SQLRETURN (*)(SQLHDBC, SQLHSTMT*))\
                     con->functions[3].func)(dbc,oh))

#define DM_SQLALLOCHANDLESTD        4

#define DM_SQLBINDCOL               5
#define CHECK_SQLBINDCOL(con)       (con->functions[5].func!=NULL)
#define SQLBINDCOL(con,stmt,cn,tt,tvp,bl,sli)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*))\
                                    con->functions[5].func)\
                                        (stmt,cn,tt,tvp,bl,sli)

#define DM_SQLBINDPARAM             6
#define CHECK_SQLBINDPARAM(con)     (con->functions[6].func!=NULL)
#define SQLBINDPARAM(con,stmt,pn,vt,pt,cs,dd,pvp,ind)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLUSMALLINT, SQLSMALLINT,\
                                      SQLSMALLINT, SQLULEN, SQLSMALLINT,\
                                      SQLPOINTER, SQLLEN*))\
                                    con->functions[6].func)\
                                        (stmt,pn,vt,pt,cs,dd,pvp,ind)

#define DM_SQLBINDPARAMETER         7
#define CHECK_SQLBINDPARAMETER(con) (con->functions[7].func!=NULL)
#define SQLBINDPARAMETER(con,stmt,pn,typ,vt,pt,cs,dd,pvp,bl,ind)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLSMALLINT, SQLSMALLINT, SQLSMALLINT,\
                                      SQLULEN, SQLSMALLINT, SQLPOINTER,\
                                      SQLLEN, SQLLEN*))\
                                    con->functions[7].func)\
                                        (stmt,pn,typ,vt,pt,cs,dd,pvp,bl,ind)

#define DM_SQLBROWSECONNECT         8
#define CHECK_SQLBROWSECONNECT(con) (con->functions[8].func!=NULL)
#define SQLBROWSECONNECT(con,dbc,ics,sl1,ocs,bl,sl2)\
                                    ((SQLRETURN (*)(SQLHDBC,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[8].func)\
                                    (dbc,ics,sl1,ocs,bl,sl2)
#define CHECK_SQLBROWSECONNECTW(con) (con->functions[8].funcW!=NULL)
#define SQLBROWSECONNECTW(con,dbc,ics,sl1,ocs,bl,sl2)\
                                    ((SQLRETURN (*)(SQLHDBC,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[8].funcW)\
                                    (dbc,ics,sl1,ocs,bl,sl2)
    
#define DM_SQLBULKOPERATIONS        9
#define CHECK_SQLBULKOPERATIONS(con)    (con->functions[9].func!=NULL)
#define SQLBULKOPERATIONS(con,stmt,op)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLSMALLINT))\
                                    con->functions[9].func)(stmt,op)

#define DM_SQLCANCEL                10
#define CHECK_SQLCANCEL(con)        (con->functions[10].func!=NULL)
#define SQLCANCEL(con,stmt)\
                                    ((SQLRETURN (*)(SQLHSTMT))\
                                    con->functions[10].func)(stmt)

#define DM_SQLCLOSECURSOR           11
#define CHECK_SQLCLOSECURSOR(con)   (con->functions[11].func!=NULL)
#define SQLCLOSECURSOR(con,stmt)\
                                    ((SQLRETURN (*)(SQLHSTMT))\
                                    con->functions[11].func)(stmt)

#define DM_SQLCOLATTRIBUTE          12
#define CHECK_SQLCOLATTRIBUTE(con)  (con->functions[12].func!=NULL)
#define SQLCOLATTRIBUTE(con,stmt,cn,fi,cap,bl,slp,nap)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLUSMALLINT, SQLPOINTER, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLLEN*))\
                                    con->functions[12].func)\
                                        (stmt,cn,fi,cap,bl,slp,nap)
#define CHECK_SQLCOLATTRIBUTEW(con)  (con->functions[12].funcW!=NULL)
#define SQLCOLATTRIBUTEW(con,stmt,cn,fi,cap,bl,slp,nap)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLUSMALLINT, SQLPOINTER, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLLEN*))\
                                    con->functions[12].funcW)\
                                        (stmt,cn,fi,cap,bl,slp,nap)

#define DM_SQLCOLATTRIBUTES         13
#define CHECK_SQLCOLATTRIBUTES(con) (con->functions[13].func!=NULL)
#define SQLCOLATTRIBUTES(con,stmt,cn,fi,cap,bl,slp,nap)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLUSMALLINT, SQLPOINTER, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLLEN*))\
                                    con->functions[13].func)\
                                        (stmt,cn,fi,cap,bl,slp,nap)
#define CHECK_SQLCOLATTRIBUTESW(con) (con->functions[13].funcW!=NULL)
#define SQLCOLATTRIBUTESW(con,stmt,cn,fi,cap,bl,slp,nap)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLUSMALLINT, SQLPOINTER, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLLEN*))\
                                    con->functions[13].funcW)\
                                        (stmt,cn,fi,cap,bl,slp,nap)

#define DM_SQLCOLUMNPRIVILEGES      14 
#define CHECK_SQLCOLUMNPRIVILEGES(con)  (con->functions[14].func!=NULL)
#define SQLCOLUMNPRIVILEGES(con,stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[14].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)
#define CHECK_SQLCOLUMNPRIVILEGESW(con)  (con->functions[14].funcW!=NULL)
#define SQLCOLUMNPRIVILEGESW(con,stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[14].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)

#define DM_SQLCOLUMNS               15
#define CHECK_SQLCOLUMNS(con)       (con->functions[15].func!=NULL)
#define SQLCOLUMNS(con,stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[15].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)
#define CHECK_SQLCOLUMNSW(con)       (con->functions[15].funcW!=NULL)
#define SQLCOLUMNSW(con,stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[15].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)

#define DM_SQLCONNECT               16
#define CHECK_SQLCONNECT(con)       (con->functions[16].func!=NULL)
#define SQLCONNECT(con,dbc,dsn,l1,uid,l2,at,l3)\
                                    ((SQLRETURN (*)(SQLHDBC,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[16].func)\
                                    (dbc,dsn,l1,uid,l2,at,l3)
#define CHECK_SQLCONNECTW(con)       (con->functions[16].funcW!=NULL)
#define SQLCONNECTW(con,dbc,dsn,l1,uid,l2,at,l3)\
                                    ((SQLRETURN (*)(SQLHDBC,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[16].funcW)\
                                    (dbc,dsn,l1,uid,l2,at,l3)

#define DM_SQLCOPYDESC              17
#define CHECK_SQLCOPYDESC(con)      (con->functions[17].func!=NULL)
#define SQLCOPYDESC(con,sd,td)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLHDESC))\
                                    con->functions[17].func)(sd,td)

#define DM_SQLDATASOURCES           18

#define DM_SQLDESCRIBECOL           19
#define CHECK_SQLDESCRIBECOL(con)   (con->functions[19].func!=NULL)
#define SQLDESCRIBECOL(con,stmt,cnum,cn,bli,nl,dt,cs,dd,n)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLUSMALLINT, SQLCHAR*, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLSMALLINT*, SQLULEN*,\
                                      SQLSMALLINT*, SQLSMALLINT*))\
                                    con->functions[19].func)\
                                        (stmt,cnum,cn,bli,nl,dt,cs,dd,n)
#define CHECK_SQLDESCRIBECOLW(con)   (con->functions[19].funcW!=NULL)
#define SQLDESCRIBECOLW(con,stmt,cnum,cn,bli,nl,dt,cs,dd,n)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLUSMALLINT, SQLWCHAR*, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLSMALLINT*, SQLULEN*,\
                                      SQLSMALLINT*, SQLSMALLINT*))\
                                    con->functions[19].funcW)\
                                        (stmt,cnum,cn,bli,nl,dt,cs,dd,n)

#define DM_SQLDESCRIBEPARAM         20
#define CHECK_SQLDESCRIBEPARAM(con) (con->functions[20].func!=NULL)
#define SQLDESCRIBEPARAM(con,stmt,pn,dtp,psp,ddp,np)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLUSMALLINT, SQLSMALLINT*, SQLULEN*,\
                                      SQLSMALLINT*, SQLSMALLINT*))\
                                    con->functions[20].func)\
                                        (stmt,pn,dtp,psp,ddp,np)

#define DM_SQLDISCONNECT            21
#define CHECK_SQLDISCONNECT(con)    (con->functions!=NULL && con->functions[21].func!=NULL)
#define SQLDISCONNECT(con,dbc)\
                                    ((SQLRETURN (*)(SQLHDBC))\
                                    con->functions[21].func)(dbc)

#define DM_SQLDRIVERCONNECT         22
#define CHECK_SQLDRIVERCONNECT(con) (con->functions[22].func!=NULL)
#define SQLDRIVERCONNECT(con,dbc,wh,ics,sl1,ocs,bl,sl2p,dc)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLHWND,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLUSMALLINT))\
                                    con->functions[22].func)\
                                        (dbc,wh,ics,sl1,ocs,bl,sl2p,dc)

#define CHECK_SQLDRIVERCONNECTW(con) (con->functions[22].funcW!=NULL)
#define SQLDRIVERCONNECTW(con,dbc,wh,ics,sl1,ocs,bl,sl2p,dc)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLHWND,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLSMALLINT*, SQLUSMALLINT))\
                                     con->functions[22].funcW)\
                                        (dbc,wh,ics,sl1,ocs,bl,sl2p,dc)

#define DM_SQLDRIVERS               23

#define DM_SQLENDTRAN               24
#define CHECK_SQLENDTRAN(con)       (con->functions[24].func!=NULL)
#define SQLENDTRAN(con,ht,h,op)\
                                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT))\
                                    con->functions[24].func)(ht,h,op)

#define DM_SQLERROR                 25
#define CHECK_SQLERROR(con)         (con->functions[25].func!=NULL)
#define SQLERROR(con,env,dbc,stmt,st,nat,msg,mm,pcb)\
                                    ((SQLRETURN (*)(SQLHENV, SQLHDBC, SQLHSTMT,\
                                      SQLCHAR*, SQLINTEGER*,\
                                      SQLCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[25].func)\
                                        (env,dbc,stmt,st,nat,msg,mm,pcb)
#define CHECK_SQLERRORW(con)         (con->functions[25].funcW!=NULL)
#define SQLERRORW(con,env,dbc,stmt,st,nat,msg,mm,pcb)\
                                    ((SQLRETURN (*)(SQLHENV, SQLHDBC, SQLHSTMT,\
                                      SQLWCHAR*, SQLINTEGER*,\
                                      SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[25].funcW)\
                                        (env,dbc,stmt,st,nat,msg,mm,pcb)

#define DM_SQLEXECDIRECT            26
#define CHECK_SQLEXECDIRECT(con)    (con->functions[26].func!=NULL)
#define SQLEXECDIRECT(con,stmt,sql,len)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLCHAR*, SQLINTEGER))\
                                    con->functions[26].func)(stmt,sql,len)
#define CHECK_SQLEXECDIRECTW(con)    (con->functions[26].funcW!=NULL)
#define SQLEXECDIRECTW(con,stmt,sql,len)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLWCHAR*, SQLINTEGER))\
                                    con->functions[26].funcW)(stmt,sql,len)

#define DM_SQLEXECUTE               27
#define CHECK_SQLEXECUTE(con)       (con->functions[27].func!=NULL)
#define SQLEXECUTE(con,stmt)\
                                    ((SQLRETURN (*)(SQLHSTMT))\
                                    con->functions[27].func)(stmt)

#define DM_SQLEXTENDEDFETCH         28
#define CHECK_SQLEXTENDEDFETCH(con) (con->functions[28].func!=NULL)
#define SQLEXTENDEDFETCH(con,stmt,fo,of,rcp,ssa)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                     SQLLEN, SQLULEN*, SQLUSMALLINT*))\
                                    con->functions[28].func)\
                                        (stmt,fo,of,rcp,ssa)

#define DM_FETCH                    29
#define CHECK_SQLFETCH(con)         (con->functions[29].func!=NULL)
#define SQLFETCH(con,stmt)\
                                    ((SQLRETURN (*)(SQLHSTMT))\
                                    con->functions[29].func)(stmt)

#define DM_SQLFETCHSCROLL           30
#define CHECK_SQLFETCHSCROLL(con)   (con->functions[30].func!=NULL)
#define SQLFETCHSCROLL(con,stmt,or,of)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLSMALLINT, SQLLEN))\
                                    con->functions[30].func)\
                                        (stmt,or,of)

#define DM_SQLFOREIGNKEYS           31
#define CHECK_SQLFOREIGNKEYS(con)   (con->functions[31].func!=NULL)
#define SQLFOREIGNKEYS(con,stmt,cn,nl1,sn,nl2,tn,nl3,fcn,nl4,fsn,nl5,ftn,nl6)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[31].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,fcn,nl4,fsn,nl5,ftn,nl6)
#define CHECK_SQLFOREIGNKEYSW(con)   (con->functions[31].funcW!=NULL)
#define SQLFOREIGNKEYSW(con,stmt,cn,nl1,sn,nl2,tn,nl3,fcn,nl4,fsn,nl5,ftn,nl6)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[31].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,fcn,nl4,fsn,nl5,ftn,nl6)

#define DM_SQLFREEENV               32
#define CHECK_SQLFREEENV(con)       (con->functions[32].func!=NULL)
#define SQLFREEENV(con,env)\
                                    ((SQLRETURN (*)(SQLHENV))\
                                    con->functions[32].func)(env)

#define DM_SQLFREEHANDLE            33
#define CHECK_SQLFREEHANDLE(con)    (con->functions[33].func!=NULL)
#define SQLFREEHANDLE(con,typ,env)\
                                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE))\
                                    con->functions[33].func)(typ,env)

#define DM_SQLFREESTMT              34
#define CHECK_SQLFREESTMT(con)      (con->functions[34].func!=NULL)
#define SQLFREESTMT(con,stmt,opt)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT))\
                                    con->functions[34].func)(stmt,opt)

#define DM_SQLFREECONNECT           35
#define CHECK_SQLFREECONNECT(con)   (con->functions[35].func!=NULL)
#define SQLFREECONNECT(con,dbc)\
                                    ((SQLRETURN (*)(SQLHDBC))\
                                    con->functions[35].func)(dbc)

#define DM_SQLGETCONNECTATTR        36
#define CHECK_SQLGETCONNECTATTR(con)    (con->functions[36].func!=NULL)
#define SQLGETCONNECTATTR(con,dbc,at,vp,bl,slp)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLINTEGER,\
                                      SQLPOINTER, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[36].func)\
                                        (dbc,at,vp,bl,slp)
#define CHECK_SQLGETCONNECTATTRW(con)    (con->functions[36].funcW!=NULL)
#define SQLGETCONNECTATTRW(con,dbc,at,vp,bl,slp)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLINTEGER,\
                                      SQLPOINTER, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[36].funcW)\
                                        (dbc,at,vp,bl,slp)

#define DM_SQLGETCONNECTOPTION      37
#define CHECK_SQLGETCONNECTOPTION(con)  (con->functions[37].func!=NULL)
#define SQLGETCONNECTOPTION(con,dbc,at,val)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLUSMALLINT, SQLPOINTER))\
                                    con->functions[37].func)\
                                        (dbc,at,val)
#define CHECK_SQLGETCONNECTOPTIONW(con)  (con->functions[37].funcW!=NULL)
#define SQLGETCONNECTOPTIONW(con,dbc,at,val)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLUSMALLINT, SQLPOINTER))\
                                    con->functions[37].funcW)\
                                        (dbc,at,val)

#define DM_SQLGETCURSORNAME         38
#define CHECK_SQLGETCURSORNAME(con) (con->functions[38].func!=NULL)
#define SQLGETCURSORNAME(con,stmt,cn,bl,nlp)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[38].func)\
                                        (stmt,cn,bl,nlp)
#define CHECK_SQLGETCURSORNAMEW(con) (con->functions[38].funcW!=NULL)
#define SQLGETCURSORNAMEW(con,stmt,cn,bl,nlp)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[38].funcW)\
                                        (stmt,cn,bl,nlp)

#define DM_SQLGETDATA               39
#define CHECK_SQLGETDATA(con)       (con->functions[39].func!=NULL)
#define SQLGETDATA(con,stmt,cn,tt,tvp,bl,sli)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*))\
                                    con->functions[39].func)\
                                        (stmt,cn,tt,tvp,bl,sli)

#define DM_SQLGETDESCFIELD          40
#define CHECK_SQLGETDESCFIELD(con)  (con->functions[40].func!=NULL)
#define SQLGETDESCFIELD(con,des,rn,fi,vp,bl,slp)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLSMALLINT,\
                                      SQLSMALLINT, SQLPOINTER, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[40].func)\
                                        (des,rn,fi,vp,bl,slp)
#define CHECK_SQLGETDESCFIELDW(con)  (con->functions[40].funcW!=NULL)
#define SQLGETDESCFIELDW(con,des,rn,fi,vp,bl,slp)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLSMALLINT,\
                                      SQLSMALLINT, SQLPOINTER, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[40].funcW)\
                                        (des,rn,fi,vp,bl,slp)

#define DM_SQLGETDESCREC            41
#define CHECK_SQLGETDESCREC(con)    (con->functions[41].func!=NULL)
#define SQLGETDESCREC(con,des,rn,n,bl,slp,tp,stp,lp,pp,sp,np)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT, SQLSMALLINT*,\
                                      SQLSMALLINT*, SQLSMALLINT*, SQLLEN*,\
                                      SQLSMALLINT*, SQLSMALLINT*, SQLSMALLINT*))\
                                    con->functions[41].func)\
                                        (des,rn,n,bl,slp,tp,stp,lp,pp,sp,np)
#define CHECK_SQLGETDESCRECW(con)    (con->functions[41].funcW!=NULL)
#define SQLGETDESCRECW(con,des,rn,n,bl,slp,tp,stp,lp,pp,sp,np)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*,\
                                      SQLSMALLINT*, SQLSMALLINT*, SQLLEN*,\
                                      SQLSMALLINT*, SQLSMALLINT*, SQLSMALLINT*))\
                                    con->functions[41].funcW)\
                                        (des,rn,n,bl,slp,tp,stp,lp,pp,sp,np)

#define DM_SQLGETDIAGFIELD          42
#define CHECK_SQLGETDIAGFIELD(con)  (con->functions[42].func!=NULL)
#define SQLGETDIAGFIELD(con,typ,han,rn,di,dip,bl,slp)\
                                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE,\
                                      SQLSMALLINT, SQLSMALLINT, SQLPOINTER,\
                                      SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[42].func)\
                                        (typ,han,rn,di,dip,bl,slp)
#define CHECK_SQLGETDIAGFIELDW(con)  (con->functions[42].funcW!=NULL)
#define SQLGETDIAGFIELDW(con,typ,han,rn,di,dip,bl,slp)\
                                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE,\
                                      SQLSMALLINT, SQLSMALLINT, SQLPOINTER,\
                                      SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[42].funcW)\
                                        (typ,han,rn,di,dip,bl,slp)

#define DM_SQLGETENVATTR            43
#define CHECK_SQLGETENVATTR(con)    (con->functions[43].func!=NULL)
#define SQLGETENVATTR(con,env,attr,val,len,ol)\
                                    ((SQLRETURN (*)(SQLHENV, SQLINTEGER,\
                                      SQLPOINTER, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[43].func)\
                                    (env,attr,val,len,ol)

#define DM_SQLGETFUNCTIONS          44
#define CHECK_SQLGETFUNCTIONS(con)  (con->functions[44].func!=NULL)
#define SQLGETFUNCTIONS(con,dbc,id,ptr)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT*))\
                                    con->functions[44].func)\
                                        (dbc,id,ptr)

#define DM_SQLGETINFO               45
#define CHECK_SQLGETINFO(con)       (con->functions[45].func!=NULL)
#define SQLGETINFO(con,dbc,it,ivo,bl,slp)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLUSMALLINT,\
                                      SQLPOINTER, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[45].func)\
                                        (dbc,it,ivo,bl,slp)
#define CHECK_SQLGETINFOW(con)       (con->functions[45].funcW!=NULL)
#define SQLGETINFOW(con,dbc,it,ivo,bl,slp)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLUSMALLINT,\
                                      SQLPOINTER, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[45].funcW)\
                                        (dbc,it,ivo,bl,slp)

#define DM_SQLGETSTMTATTR           46
#define CHECK_SQLGETSTMTATTR(con)   (con->functions[46].func!=NULL)
#define SQLGETSTMTATTR(con,stmt,at,vp,bl,slp)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLINTEGER,\
                                      SQLPOINTER, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[46].func)\
                                        (stmt,at,vp,bl,slp)
#define CHECK_SQLGETSTMTATTRW(con)   (con->functions[46].funcW!=NULL)
#define SQLGETSTMTATTRW(con,stmt,at,vp,bl,slp)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLINTEGER,\
                                      SQLPOINTER, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[46].funcW)\
                                        (stmt,at,vp,bl,slp)
#define DM_SQLGETSTMTOPTION         47
#define CHECK_SQLGETSTMTOPTION(con) (con->functions[47].func!=NULL)
#define SQLGETSTMTOPTION(con,stmt,op,val)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT, SQLPOINTER))\
                                    con->functions[47].func)\
                                        (stmt,op,val)
#define CHECK_SQLGETSTMTOPTIONW(con) (con->functions[47].funcW!=NULL)
#define SQLGETSTMTOPTIONW(con,stmt,op,val)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT, SQLPOINTER))\
                                    con->functions[47].funcW)\
                                        (stmt,op,val)

#define DM_SQLGETTYPEINFO           48
#define CHECK_SQLGETTYPEINFO(con)   (con->functions[48].func!=NULL)
#define SQLGETTYPEINFO(con,stmt,typ)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLSMALLINT))\
                                    con->functions[48].func)(stmt,typ)
#define CHECK_SQLGETTYPEINFOW(con)   (con->functions[48].funcW!=NULL)
#define SQLGETTYPEINFOW(con,stmt,typ)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLSMALLINT))\
                                    con->functions[48].funcW)(stmt,typ)

#define DM_SQLMORERESULTS           49
#define CHECK_SQLMORERESULTS(con)   (con->functions[49].func!=NULL)
#define SQLMORERESULTS(con,stmt)\
                                    ((SQLRETURN (*)(SQLHSTMT))\
                                    con->functions[49].func)(stmt)

#define DM_SQLNATIVESQL             50
#define CHECK_SQLNATIVESQL(con)     (con->functions[50].func!=NULL)
#define SQLNATIVESQL(con,dbc,ist,tl,ost,bl,tlp)\
                                    ((SQLRETURN (*)(SQLHDBC,\
                                      SQLCHAR*, SQLINTEGER,\
                                      SQLCHAR*, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[50].func)\
                                        (dbc,ist,tl,ost,bl,tlp)
#define CHECK_SQLNATIVESQLW(con)     (con->functions[50].funcW!=NULL)
#define SQLNATIVESQLW(con,dbc,ist,tl,ost,bl,tlp)\
                                    ((SQLRETURN (*)(SQLHDBC,\
                                      SQLWCHAR*, SQLINTEGER,\
                                      SQLWCHAR*, SQLINTEGER, SQLINTEGER*))\
                                    con->functions[50].funcW)\
                                        (dbc,ist,tl,ost,bl,tlp)

#define DM_SQLNUMPARAMS             51
#define CHECK_SQLNUMPARAMS(con)     (con->functions[51].func!=NULL)
#define SQLNUMPARAMS(con,stmt,cnt)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLSMALLINT*))\
                                    con->functions[51].func)(stmt,cnt)

#define DM_SQLNUMRESULTCOLS         52
#define CHECK_SQLNUMRESULTCOLS(con) (con->functions[52].func!=NULL)
#define SQLNUMRESULTCOLS(con,stmt,cnt)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLSMALLINT*))\
                                    con->functions[52].func)(stmt,cnt)

#define DM_SQLPARAMDATA             53
#define CHECK_SQLPARAMDATA(con)     (con->functions[53].func!=NULL)
#define SQLPARAMDATA(con,stmt,val)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLPOINTER*))\
                                    con->functions[53].func)(stmt,val)

#define DM_SQLPARAMOPTIONS          54
#define CHECK_SQLPARAMOPTIONS(con)  (con->functions[54].func!=NULL)
#define SQLPARAMOPTIONS(con,stmt,cr,pi)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLULEN, SQLULEN*))\
                                    con->functions[54].func)(stmt,cr,pi)

#define DM_SQLPREPARE               55
#define CHECK_SQLPREPARE(con)       (con->functions[55].func!=NULL)
#define SQLPREPARE(con,stmt,sql,len)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLCHAR*, SQLINTEGER))\
                                    con->functions[55].func)(stmt,sql,len)
#define CHECK_SQLPREPAREW(con)       (con->functions[55].funcW!=NULL)
#define SQLPREPAREW(con,stmt,sql,len)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLWCHAR*, SQLINTEGER))\
                                    con->functions[55].funcW)(stmt,sql,len)

#define DM_SQLPRIMARYKEYS           56
#define CHECK_SQLPRIMARYKEYS(con)   (con->functions[56].func!=NULL)
#define SQLPRIMARYKEYS(con,stmt,cn,nl1,sn,nl2,tn,nl3)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[56].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3)
#define CHECK_SQLPRIMARYKEYSW(con)   (con->functions[56].funcW!=NULL)
#define SQLPRIMARYKEYSW(con,stmt,cn,nl1,sn,nl2,tn,nl3)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[56].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3)

#define DM_SQLPROCEDURECOLUMNS      57
#define CHECK_SQLPROCEDURECOLUMNS(con)  (con->functions[57].func!=NULL)
#define SQLPROCEDURECOLUMNS(con,stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[57].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)
#define CHECK_SQLPROCEDURECOLUMNSW(con)  (con->functions[57].funcW!=NULL)
#define SQLPROCEDURECOLUMNSW(con,stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[57].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,col,nl4)

#define DM_SQLPROCEDURES            58
#define CHECK_SQLPROCEDURES(con)    (con->functions[58].func!=NULL)
#define SQLPROCEDURES(con,stmt,cn,nl1,sn,nl2,tn,nl3)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[58].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3)
#define CHECK_SQLPROCEDURESW(con)    (con->functions[58].funcW!=NULL)
#define SQLPROCEDURESW(con,stmt,cn,nl1,sn,nl2,tn,nl3)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[58].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3)

#define DM_SQLPUTDATA               59
#define CHECK_SQLPUTDATA(con)       (con->functions[59].func!=NULL)
#define SQLPUTDATA(con,stmt,d,p)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLPOINTER, SQLLEN))\
                                    con->functions[59].func)(stmt,d,p)

#define DM_SQLROWCOUNT              60
#define CHECK_SQLROWCOUNT(con)      (con->functions[60].func!=NULL)
#define DEF_SQLROWCOUNT(con,stmt,cnt)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLLEN*))\
                                    con->functions[60].func)(stmt,cnt)

#define DM_SQLSETCONNECTATTR        61
#define CHECK_SQLSETCONNECTATTR(con)    (con->functions[61].func!=NULL)
#define SQLSETCONNECTATTR(con,dbc,at,vp,sl)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLINTEGER,\
                                      SQLPOINTER, SQLINTEGER))\
                                    con->functions[61].func)\
                                        (dbc,at,vp,sl)
#define CHECK_SQLSETCONNECTATTRW(con)    (con->functions[61].funcW!=NULL)
#define SQLSETCONNECTATTRW(con,dbc,at,vp,sl)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLINTEGER,\
                                      SQLPOINTER, SQLINTEGER))\
                                    con->functions[61].funcW)\
                                        (dbc,at,vp,sl)

#define DM_SQLSETCONNECTOPTION      62
#define CHECK_SQLSETCONNECTOPTION(con)  (con->functions[62].func!=NULL)
#define SQLSETCONNECTOPTION(con,dbc,op,p)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLUSMALLINT, SQLULEN))\
                                    con->functions[62].func)\
                                        (dbc,op,p)
#define CHECK_SQLSETCONNECTOPTIONW(con)  (con->functions[62].funcW!=NULL)
#define SQLSETCONNECTOPTIONW(con,dbc,op,p)\
                                    ((SQLRETURN (*)(SQLHDBC, SQLUSMALLINT, SQLULEN))\
                                    con->functions[62].funcW)\
                                        (dbc,op,p)

#define DM_SQLSETCURSORNAME         63
#define CHECK_SQLSETCURSORNAME(con) (con->functions[63].func!=NULL)
#define SQLSETCURSORNAME(con,stmt,nam,len)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLCHAR*, SQLSMALLINT))\
                                    con->functions[63].func)(stmt,nam,len)
#define CHECK_SQLSETCURSORNAMEW(con) (con->functions[63].funcW!=NULL)
#define SQLSETCURSORNAMEW(con,stmt,nam,len)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[63].funcW)(stmt,nam,len)

#define DM_SQLSETDESCFIELD          64
#define CHECK_SQLSETDESCFIELD(con)  (con->functions[64].func!=NULL)
#define SQLSETDESCFIELD(con,des,rn,fi,vp,bl)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLSMALLINT,\
                                      SQLSMALLINT, SQLPOINTER, SQLINTEGER))\
                                    con->functions[64].func)\
                                    (des,rn,fi,vp,bl)
#define CHECK_SQLSETDESCFIELDW(con)  (con->functions[64].funcW!=NULL)
#define SQLSETDESCFIELDW(con,des,rn,fi,vp,bl)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLSMALLINT,\
                                     SQLSMALLINT, SQLPOINTER, SQLINTEGER))\
                                    con->functions[64].funcW)\
                                    (des,rn,fi,vp,bl)

#define DM_SQLSETDESCREC            65
#define CHECK_SQLSETDESCREC(con)    (con->functions[65].func!=NULL)
#define SQLSETDESCREC(con,des,rn,t,st,l,p,sc,dp,slp,ip)\
                                    ((SQLRETURN (*)(SQLHDESC, SQLSMALLINT,\
                                      SQLSMALLINT, SQLSMALLINT, SQLLEN, SQLSMALLINT,\
                                      SQLSMALLINT, SQLPOINTER, SQLLEN*, SQLLEN*))\
                                    con->functions[65].func)\
                                        (des,rn,t,st,l,p,sc,dp,slp,ip)

#define DM_SQLSETENVATTR            66
#define CHECK_SQLSETENVATTR(con)    (con->functions[66].func!=NULL)
#define SQLSETENVATTR(con,env,attr,val,len)\
                                    ((SQLRETURN (*)(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER))\
                                    con->functions[66].func)(env,attr,val,len)

#define DM_SQLSETPARAM              67
#define CHECK_SQLSETPARAM(con)      (con->functions[67].func!=NULL)
#define SQLSETPARAM(con,stmt,pn,vt,pt,lp,ps,pv,sli)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLSMALLINT, SQLSMALLINT, SQLULEN,\
                                      SQLSMALLINT, SQLPOINTER, SQLLEN*))\
                                    con->functions[67].func)\
                                        (stmt,pn,vt,pt,lp,ps,pv,sli)

#define DM_SQLSETPOS                68
#define CHECK_SQLSETPOS(con)        (con->functions[68].func!=NULL)
#define SQLSETPOS(con,stmt,rn,op,lt)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLSETPOSIROW,\
                                      SQLUSMALLINT, SQLUSMALLINT))\
                                    con->functions[68].func)\
                                        (stmt,rn,op,lt)

#define DM_SQLSETSCROLLOPTIONS      69
#define CHECK_SQLSETSCROLLOPTIONS(con)  (con->functions[69].func!=NULL)
#define SQLSETSCROLLOPTIONS(con,stmt,fc,cr,rs)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT,\
                                      SQLLEN, SQLUSMALLINT))\
                                    con->functions[69].func)\
                                        (stmt,fc,cr,rs)
#define DM_SQLSETSTMTATTR           70
#define CHECK_SQLSETSTMTATTR(con)   (con->functions[70].func!=NULL)
#define SQLSETSTMTATTR(con,stmt,attr,vp,sl)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER))\
                                    con->functions[70].func)\
                                        (stmt,attr,vp,sl)
#define CHECK_SQLSETSTMTATTRW(con)   (con->functions[70].funcW!=NULL)
#define SQLSETSTMTATTRW(con,stmt,attr,vp,sl)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER))\
                                    con->functions[70].funcW)\
                                        (stmt,attr,vp,sl)

#define DM_SQLSETSTMTOPTION         71
#define CHECK_SQLSETSTMTOPTION(con) (con->functions[71].func!=NULL)
#define SQLSETSTMTOPTION(con,stmt,op,val)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT, SQLULEN))\
                                    con->functions[71].func)\
                                        (stmt,op,val)

#define CHECK_SQLSETSTMTOPTIONW(con)   (con->functions[71].funcW!=NULL)
#define SQLSETSTMTOPTIONW(con,stmt,op,val)\
                                    ((SQLRETURN (*)(SQLHSTMT, SQLUSMALLINT, SQLULEN))\
                                    con->functions[71].funcW)\
                                        (stmt,op,val)

#define DM_SQLSPECIALCOLUMNS        72
#define CHECK_SQLSPECIALCOLUMNS(con)    (con->functions[72].func!=NULL)
#define SQLSPECIALCOLUMNS(con,stmt,it,cn,nl1,sn,nl2,tn,nl3,s,n)\
                                    ((SQLRETURN (*) (\
                                           SQLHSTMT, SQLUSMALLINT,\
                                           SQLCHAR*, SQLSMALLINT,\
                                           SQLCHAR*, SQLSMALLINT,\
                                           SQLCHAR*, SQLSMALLINT,\
                                           SQLUSMALLINT, SQLUSMALLINT))\
                                    con->functions[72].func)\
                                        (stmt,it,cn,nl1,sn,nl2,tn,nl3,s,n)
#define CHECK_SQLSPECIALCOLUMNSW(con)    (con->functions[72].funcW!=NULL)
#define SQLSPECIALCOLUMNSW(con,stmt,it,cn,nl1,sn,nl2,tn,nl3,s,n)\
                                    ((SQLRETURN (*) (\
                                        SQLHSTMT, SQLUSMALLINT,\
                                        SQLWCHAR*, SQLSMALLINT,\
                                        SQLWCHAR*, SQLSMALLINT,\
                                        SQLWCHAR*, SQLSMALLINT,\
                                        SQLUSMALLINT, SQLUSMALLINT))\
                                    con->functions[72].funcW)\
                                        (stmt,it,cn,nl1,sn,nl2,tn,nl3,s,n)

#define DM_SQLSTATISTICS            73
#define CHECK_SQLSTATISTICS(con)    (con->functions[73].func!=NULL)
#define SQLSTATISTICS(con,stmt,cn,nl1,sn,nl2,tn,nl3,un,res)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLUSMALLINT, SQLUSMALLINT))\
                                    con->functions[73].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,un,res)
#define CHECK_SQLSTATISTICSW(con)    (con->functions[73].funcW!=NULL)
#define SQLSTATISTICSW(con,stmt,cn,nl1,sn,nl2,tn,nl3,un,res)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLUSMALLINT, SQLUSMALLINT))\
                                    con->functions[73].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,un,res)

#define DM_SQLTABLEPRIVILEGES       74
#define CHECK_SQLTABLEPRIVILEGES(con)   (con->functions[74].func!=NULL)
#define SQLTABLEPRIVILEGES(con,stmt,cn,nl1,sn,nl2,tn,nl3)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[74].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3)
#define CHECK_SQLTABLEPRIVILEGESW(con)   (con->functions[74].funcW!=NULL)
#define SQLTABLEPRIVILEGESW(con,stmt,cn,nl1,sn,nl2,tn,nl3)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[74].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3)

#define DM_SQLTABLES                75
#define CHECK_SQLTABLES(con)        (con->functions[75].func!=NULL)
#define SQLTABLES(con,stmt,cn,nl1,sn,nl2,tn,nl3,tt,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT,\
                                      SQLCHAR*, SQLSMALLINT))\
                                    con->functions[75].func)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,tt,nl4)
#define CHECK_SQLTABLESW(con)        (con->functions[75].funcW!=NULL)
#define SQLTABLESW(con,stmt,cn,nl1,sn,nl2,tn,nl3,tt,nl4)\
                                    ((SQLRETURN (*)(SQLHSTMT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT,\
                                      SQLWCHAR*, SQLSMALLINT))\
                                    con->functions[75].funcW)\
                                        (stmt,cn,nl1,sn,nl2,tn,nl3,tt,nl4)

#define DM_SQLTRANSACT              76
#define CHECK_SQLTRANSACT(con)      (con->functions[76].func!=NULL)
#define SQLTRANSACT(con,eh,ch,op)\
                                    ((SQLRETURN (*)(SQLHENV, SQLHDBC, SQLUSMALLINT))\
                                    con->functions[76].func)(eh,ch,op)


#define DM_SQLGETDIAGREC            77
#define CHECK_SQLGETDIAGREC(con)    (con->functions[77].func!=NULL)
#define SQLGETDIAGREC(con,typ,han,rn,st,nat,msg,bl,tlp)\
                                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE,\
                                      SQLSMALLINT, SQLCHAR*, SQLINTEGER*,\
                                      SQLCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[77].func)\
                                        (typ,han,rn,st,nat,msg,bl,tlp)
#define CHECK_SQLGETDIAGRECW(con)    (con->functions[77].funcW!=NULL)
#define SQLGETDIAGRECW(con,typ,han,rn,st,nat,msg,bl,tlp)\
                                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE,\
                                      SQLSMALLINT, SQLWCHAR*, SQLINTEGER*,\
                                      SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*))\
                                    con->functions[77].funcW)\
                                        (typ,han,rn,st,nat,msg,bl,tlp)

#define DM_SQLCANCELHANDLE          78
#define CHECK_SQLCANCELHANDLE(con)  (con->functions[78].func!=NULL)
#define SQLCANCELHANDLE(con,typ,han)\
                                    ((SQLRETURN (*)(SQLSMALLINT, SQLHANDLE))\
                                    con->functions[78].func)\
                                        (typ,han)

#endif
