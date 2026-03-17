/*********************************************************************
 *
 * unixODBC Cursor Library
 *
 * Created by Nick Gorham
 * (nick@lurcher.org).
 *
 * copyright (c) 1999 Nick Gorham
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 **********************************************************************
 *
 * $Id: SQLConnect.c,v 1.7 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLConnect.c,v $
 * Revision 1.7  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.6  2007/02/12 11:49:35  lurcher
 * Add QT4 support to existing GUI parts
 *
 * Revision 1.5  2005/07/08 12:11:23  lurcher
 *
 * Fix a cursor lib problem (it was broken if you did metadata calls)
 * Alter the params to SQLParamOptions to use SQLULEN
 *
 * Revision 1.4  2005/05/03 17:16:49  lurcher
 * Backport a couple of changes from the Debian build
 *
 * Revision 1.3  2002/11/25 15:37:54  lurcher
 *
 * Fix problems in the cursor lib
 *
 * Revision 1.2  2002/11/19 18:52:28  lurcher
 *
 * Alter the cursor lib to not require linking to the driver manager.
 *
 * Revision 1.1.1.1  2001/10/17 16:40:15  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/03/28 14:57:22  nick
 *
 * Fix bugs in corsor lib introduced bu UNCODE and other changes
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.3  1999/11/20 20:54:00  ngorham
 *
 * Asorted portability fixes
 *
 * Revision 1.2  1999/11/10 03:51:35  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.1  1999/09/19 22:22:50  ngorham
 *
 *
 * Added first cursor library work, read only at the moment and only works
 * with selects with no where clause
 *
 *
 **********************************************************************/

#include <config.h>
#include "cursorlibrary.h"

static struct driver_func  cl_template_func[] =
{
    /* 00 */ { SQL_API_SQLALLOCCONNECT,      "SQLAllocConnect", NULL, NULL,
                            NULL },
    /* 01 */ { SQL_API_SQLALLOCENV,          "SQLAllocEnv", NULL, NULL,
                            NULL },
    /* 02 */ { SQL_API_SQLALLOCHANDLE,       "SQLAllocHandle",  NULL, NULL,
                            (SQLRETURN (*)())CLAllocHandle },
    /* 03 */ { SQL_API_SQLALLOCSTMT,         "SQLAllocStmt", NULL, NULL,
                            (SQLRETURN (*)())CLAllocStmt },
    /* 04 */ { SQL_API_SQLALLOCHANDLESTD,    "SQLAllocHandleStd", NULL, NULL,
                            (SQLRETURN (*)())CLAllocHandleStd },
    /* 05 */ { SQL_API_SQLBINDCOL,           "SQLBindCol", NULL, NULL,
                            (SQLRETURN (*)())CLBindCol },
    /* 06 */ { SQL_API_SQLBINDPARAM,         "SQLBindParam", NULL, NULL,
                            (SQLRETURN (*)())CLBindParam },
    /* 07 */ { SQL_API_SQLBINDPARAMETER,     "SQLBindParameter", NULL, NULL,
                            (SQLRETURN (*)())CLBindParameter },
    /* 08 */ { SQL_API_SQLBROWSECONNECT,     "SQLBrowseConnect", NULL, NULL,
                            NULL },
    /* 09 */ { SQL_API_SQLBULKOPERATIONS,    "SQLBulkOperations", NULL, NULL,
                            NULL },
    /* 10 */ { SQL_API_SQLCANCEL,            "SQLCancel", NULL, NULL,
                            (SQLRETURN (*)())CLCancel },
    /* 11 */ { SQL_API_SQLCLOSECURSOR,       "SQLCloseCursor", NULL, NULL,
                            (SQLRETURN (*)())CLCloseCursor },
    /* 12 */ { SQL_API_SQLCOLATTRIBUTE,      "SQLColAttribute", NULL, NULL,
                            (SQLRETURN (*)())CLColAttribute },
    /* 13 */ { SQL_API_SQLCOLATTRIBUTES,     "SQLColAttributes", NULL, NULL,
                            (SQLRETURN (*)())CLColAttributes },
    /* 14 */ { SQL_API_SQLCOLUMNPRIVILEGES,  "SQLColumnPrivileges", NULL, NULL,
                            (SQLRETURN (*)())CLColumnPrivileges },
    /* 15 */ { SQL_API_SQLCOLUMNS,           "SQLColumns", NULL, NULL,
                            (SQLRETURN (*)())CLColumns },
    /* 16 */ { SQL_API_SQLCONNECT,           "SQLConnect", NULL, NULL,
                            NULL },
    /* 17 */ { SQL_API_SQLCOPYDESC,          "SQLCopyDesc", NULL, NULL,
                            (SQLRETURN (*)())CLCopyDesc },
    /* 18 */ { SQL_API_SQLDATASOURCES,       "SQLDataSources", NULL, NULL,
                            NULL },
    /* 19 */ { SQL_API_SQLDESCRIBECOL,       "SQLDescribeCol", NULL, NULL,
                            (SQLRETURN (*)())CLDescribeCol },
    /* 20 */ { SQL_API_SQLDESCRIBEPARAM,     "SQLDescribeParam", NULL, NULL,
                            (SQLRETURN (*)())CLDescribeParam },
    /* 21 */ { SQL_API_SQLDISCONNECT,        "SQLDisconnect", NULL, NULL,
                            (SQLRETURN (*)())CLDisconnect },
    /* 22 */ { SQL_API_SQLDRIVERCONNECT,     "SQLDriverConnect", NULL, NULL,
                            NULL },
    /* 23 */ { SQL_API_SQLDRIVERS,           "SQLDrivers", NULL, NULL,
                            NULL },
    /* 24 */ { SQL_API_SQLENDTRAN,           "SQLEndTran", NULL, NULL,
                            (SQLRETURN (*)())CLEndTran },
    /* 25 */ { SQL_API_SQLERROR,             "SQLError", NULL, NULL,
                            (SQLRETURN (*)())CLError },
    /* 26 */ { SQL_API_SQLEXECDIRECT,        "SQLExecDirect", NULL, NULL,
                            (SQLRETURN (*)())CLExecDirect },
    /* 27 */ { SQL_API_SQLEXECUTE,           "SQLExecute", NULL, NULL,
                            (SQLRETURN (*)())CLExecute },
    /* 28 */ { SQL_API_SQLEXTENDEDFETCH,     "SQLExtendedFetch", NULL, NULL,
                            (SQLRETURN (*)())CLExtendedFetch },
    /* 29 */ { SQL_API_SQLFETCH,             "SQLFetch", NULL, NULL,
                            (SQLRETURN (*)())CLFetch },
    /* 30 */ { SQL_API_SQLFETCHSCROLL,       "SQLFetchScroll", NULL, NULL,
                            (SQLRETURN (*)())CLFetchScroll },
    /* 31 */ { SQL_API_SQLFOREIGNKEYS,       "SQLForeignKeys", NULL, NULL,
                            (SQLRETURN (*)())CLForeignKeys },
    /* 32 */ { SQL_API_SQLFREEENV,           "SQLFreeEnv", NULL, NULL,
                            NULL },
    /* 33 */ { SQL_API_SQLFREEHANDLE,        "SQLFreeHandle", NULL, NULL,
                            (SQLRETURN (*)())CLFreeHandle },
    /* 34 */ { SQL_API_SQLFREESTMT,          "SQLFreeStmt", NULL, NULL,
                            (SQLRETURN (*)())CLFreeStmt },
    /* 35 */ { SQL_API_SQLFREECONNECT,       "SQLFreeConnect", NULL, NULL,
                            NULL },
    /* 36 */ { SQL_API_SQLGETCONNECTATTR,    "SQLGetConnectAttr", NULL, NULL,
                            (SQLRETURN (*)())CLGetConnectAttr },
    /* 37 */ { SQL_API_SQLGETCONNECTOPTION,  "SQLGetConnectOption", NULL, NULL,
                            (SQLRETURN (*)())CLGetConnectOption },
    /* 38 */ { SQL_API_SQLGETCURSORNAME,     "SQLGetCursorName", NULL, NULL,
                            (SQLRETURN (*)())CLGetCursorName },
    /* 39 */ { SQL_API_SQLGETDATA,           "SQLGetData", NULL, NULL,
                            (SQLRETURN (*)())CLGetData },
    /* 40 */ { SQL_API_SQLGETDESCFIELD,      "SQLGetDescField", NULL, NULL,
                            (SQLRETURN (*)())CLGetDescField },
    /* 41 */ { SQL_API_SQLGETDESCREC,        "SQLGetDescRec", NULL, NULL,
                            (SQLRETURN (*)())CLGetDescRec },
    /* 42 */ { SQL_API_SQLGETDIAGFIELD,      "SQLGetDiagField", NULL, NULL,
                            (SQLRETURN (*)())CLGetDiagField },
    /* 43 */ { SQL_API_SQLGETENVATTR,        "SQLGetEnvAttr", NULL, NULL,
                            NULL },
    /* 44 */ { SQL_API_SQLGETFUNCTIONS,      "SQLGetFunctions", NULL, NULL,
                            NULL },
    /* 45 */ { SQL_API_SQLGETINFO,           "SQLGetInfo", NULL, NULL,
                            (SQLRETURN (*)())CLGetInfo },
    /* 46 */ { SQL_API_SQLGETSTMTATTR,       "SQLGetStmtAttr", NULL, NULL,
                            (SQLRETURN (*)())CLGetStmtAttr },
    /* 47 */ { SQL_API_SQLGETSTMTOPTION,     "SQLGetStmtOption", NULL, NULL,
                            (SQLRETURN (*)())CLGetStmtOption },
    /* 48 */ { SQL_API_SQLGETTYPEINFO,       "SQLGetTypeInfo", NULL, NULL,
                            (SQLRETURN (*)())CLGetTypeInfo },
    /* 49 */ { SQL_API_SQLMORERESULTS,       "SQLMoreResults", NULL, NULL,
                            (SQLRETURN (*)())CLMoreResults },
    /* 50 */ { SQL_API_SQLNATIVESQL,         "SQLNativeSql", NULL, NULL,
                            (SQLRETURN (*)())CLNativeSql },
    /* 51 */ { SQL_API_SQLNUMPARAMS,         "SQLNumParams", NULL, NULL,
                            (SQLRETURN (*)())CLNumParams },
    /* 52 */ { SQL_API_SQLNUMRESULTCOLS,     "SQLNumResultCols", NULL, NULL,
                            (SQLRETURN (*)())CLNumResultCols },
    /* 53 */ { SQL_API_SQLPARAMDATA,         "SQLParamData", NULL, NULL,
                            (SQLRETURN (*)())CLParamData },
    /* 54 */ { SQL_API_SQLPARAMOPTIONS,      "SQLParamOptions", NULL, NULL,
                            (SQLRETURN (*)())CLParamOptions },
    /* 55 */ { SQL_API_SQLPREPARE,           "SQLPrepare", NULL, NULL,
                            (SQLRETURN (*)())CLPrepare },
    /* 56 */ { SQL_API_SQLPRIMARYKEYS,       "SQLPrimaryKeys", NULL, NULL,
                            (SQLRETURN (*)())CLPrimaryKeys },
    /* 57 */ { SQL_API_SQLPROCEDURECOLUMNS,  "SQLProcedureColumns", NULL, NULL,
                            (SQLRETURN (*)())CLProcedureColumns },
    /* 58 */ { SQL_API_SQLPROCEDURES,        "SQLProcedures", NULL, NULL,
                            (SQLRETURN (*)())CLProcedures },
    /* 59 */ { SQL_API_SQLPUTDATA,           "SQLPutData", NULL, NULL,
                            (SQLRETURN (*)())CLPutData },
    /* 60 */ { SQL_API_SQLROWCOUNT,          "SQLRowCount", NULL, NULL,
                            (SQLRETURN (*)())CLRowCount },
    /* 61 */ { SQL_API_SQLSETCONNECTATTR,    "SQLSetConnectAttr", NULL, NULL,
                            (SQLRETURN (*)())CLSetConnectAttr },
    /* 62 */ { SQL_API_SQLSETCONNECTOPTION,  "SQLSetConnectOption", NULL, NULL,
                            (SQLRETURN (*)())CLSetConnectOption },
    /* 63 */ { SQL_API_SQLSETCURSORNAME,     "SQLSetCursorName", NULL, NULL,
                            (SQLRETURN (*)())CLSetCursorName },
    /* 64 */ { SQL_API_SQLSETDESCFIELD,      "SQLSetDescField", NULL, NULL,
                            (SQLRETURN (*)())CLSetDescField },
    /* 65 */ { SQL_API_SQLSETDESCREC,        "SQLSetDescRec",  NULL, NULL,
                            (SQLRETURN (*)())CLSetDescRec },
    /* 66 */ { SQL_API_SQLSETENVATTR,        "SQLSetEnvAttr", NULL, NULL,
                            NULL },
    /* 67 */ { SQL_API_SQLSETPARAM,          "SQLSetParam", NULL, NULL,
                            (SQLRETURN (*)())CLSetParam },
    /* 68 */ { SQL_API_SQLSETPOS,            "SQLSetPos", NULL, NULL,
                            (SQLRETURN (*)())CLSetPos },
    /* 69 */ { SQL_API_SQLSETSCROLLOPTIONS,  "SQLSetScrollOptions", NULL, NULL,
                            (SQLRETURN (*)())CLSetScrollOptions },
    /* 70 */ { SQL_API_SQLSETSTMTATTR,       "SQLSetStmtAttr", NULL, NULL,
                            (SQLRETURN (*)())CLSetStmtAttr },
    /* 71 */ { SQL_API_SQLSETSTMTOPTION,     "SQLSetStmtOption", NULL, NULL,
                            (SQLRETURN (*)())CLSetStmtOption },
    /* 72 */ { SQL_API_SQLSPECIALCOLUMNS,    "SQLSpecialColumns", NULL, NULL,
                            (SQLRETURN (*)())CLSpecialColumns },
    /* 73 */ { SQL_API_SQLSTATISTICS,        "SQLStatistics", NULL, NULL,
                            (SQLRETURN (*)())CLStatistics },
    /* 74 */ { SQL_API_SQLTABLEPRIVILEGES,   "SQLTablePrivileges", NULL, NULL,
                            (SQLRETURN (*)())CLTablePrivileges },
    /* 75 */ { SQL_API_SQLTABLES,            "SQLTables", NULL, NULL,
                            (SQLRETURN (*)())CLTables },
    /* 76 */ { SQL_API_SQLTRANSACT,          "SQLTransact", NULL, NULL,
                            (SQLRETURN (*)())CLTransact },
    /* 77 */ { SQL_API_SQLGETDIAGREC,        "SQLGetDiagRec", NULL, NULL,
                            (SQLRETURN (*)())CLGetDiagRec },
};

/*
 * connect is done by the driver manager, the is called to put the
 * cursor lib in the call chain
 */

SQLRETURN CLConnect( DMHDBC connection, struct driver_helper_funcs *dh )
{
    int i;
    CLHDBC cl_connection;
    SQLRETURN ret;

    /*
     * Allocated a cursor connection structure 
     */

    cl_connection = malloc( sizeof( struct cl_connection ));

    if ( !cl_connection )
    {
        dh ->dm_log_write( "CL " __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: IM001" );

        dh ->__post_internal_error( &connection -> error,
                ERROR_HY001, NULL,
                connection -> environment -> requested_version );
        return SQL_ERROR;
    }

    memset( cl_connection, 0, sizeof( struct cl_connection ));

    cl_connection -> functions = connection -> functions;
    cl_connection -> dm_connection = connection;

    cl_connection -> dh.__post_internal_error_ex = dh -> __post_internal_error_ex;
    cl_connection -> dh.__post_internal_error = dh -> __post_internal_error;
    cl_connection -> dh.dm_log_write = dh -> dm_log_write;

    /*
     * allocated a copy of the functions
     */

    if ( !( cl_connection -> functions = malloc( sizeof( cl_template_func ))))
    {
        dh ->dm_log_write( "CL " __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: IM001" );

        cl_connection -> dh.__post_internal_error( &connection -> error,
                ERROR_HY001, NULL,
                connection -> environment -> requested_version );

        free( cl_connection );

        return SQL_ERROR;
    }

    /*
     * replace the function pointers with the ones in the
     * cursor lib
     */

    for ( i = 0; 
            i < sizeof( cl_template_func ) / sizeof( cl_template_func[ 0 ] ); 
            i ++ )
    {
        cl_connection -> functions[ i ] =
            connection -> functions[ i ];

        /*
         * if set replace the driver function with the function in 
         * the template
         */

        if ( cl_template_func[ i ].func && 
                connection -> functions[ i ].func )
        {
            connection -> functions[ i ] =
                cl_template_func[ i ];

            /*
             * copy the can_supply from the drivers list
             */

            connection -> functions[ i ].can_supply =
                cl_connection -> functions[ i ].can_supply;
        }
        /*
         * prevent the DM from tring to get via the W functions, the cursor lib is 
         * ascii only
         */

        connection -> functions[ i ].funcW = NULL;
    }

    /*
     * add some functions the cursor lib will supply
     */

    connection -> functions[ DM_SQLSETPOS ].can_supply = 1;
    connection -> functions[ DM_SQLSETPOS ].func = 
        cl_template_func[ DM_SQLSETPOS ].func;

    connection -> functions[ DM_SQLSETSCROLLOPTIONS ].can_supply = 1;
    connection -> functions[ DM_SQLSETSCROLLOPTIONS ].func = 
        cl_template_func[ DM_SQLSETSCROLLOPTIONS ].func;

    connection -> functions[ DM_SQLFETCHSCROLL ].can_supply = 1;
    connection -> functions[ DM_SQLFETCHSCROLL ].func = 
        cl_template_func[ DM_SQLFETCHSCROLL ].func;

    connection -> functions[ DM_SQLEXTENDEDFETCH ].can_supply = 1;
    connection -> functions[ DM_SQLEXTENDEDFETCH ].func = 
        cl_template_func[ DM_SQLEXTENDEDFETCH ].func;

    /*
     * blank off what we don't do
     */

    connection -> functions[ DM_SQLBULKOPERATIONS ].can_supply = 0;
    connection -> functions[ DM_SQLBULKOPERATIONS ].func = NULL;


    /*
     * intercept the driver dbc
     */

    cl_connection -> driver_dbc = connection -> driver_dbc;
    connection -> driver_dbc = ( DRV_SQLHANDLE ) cl_connection;

    /*
     * check the number of alowed active statements
     */

    if ( CHECK_SQLGETINFO( cl_connection ))
    {
        ret = SQLGETINFO( cl_connection,
                cl_connection -> driver_dbc,
                SQL_MAX_CONCURRENT_ACTIVITIES,
                &cl_connection -> active_statement_allowed,
                sizeof( cl_connection -> active_statement_allowed ),
                NULL );
        /*
         * assume the worst
         */

        if ( !SQL_SUCCEEDED( ret ))
        {
            cl_connection -> active_statement_allowed = 1;
        }
    }
    else
    {
        cl_connection -> active_statement_allowed = 1;
    }

    return SQL_SUCCESS;
}

SQLRETURN CLDisconnect( SQLHDBC connection_handle )
{
    SQLRETURN ret;
    int i;
    CLHDBC cl_connection = (CLHDBC)connection_handle;
    DMHDBC connection = cl_connection -> dm_connection;

    /* 
     * disconnect from the driver
     */

    ret = cl_connection -> functions ?
            SQLDISCONNECT( cl_connection, cl_connection -> driver_dbc ) :
            -1;

    if ( SQL_SUCCEEDED( ret ))
    {
        /*
         * replace the function pointers with the ones from the
         * cursor lib, this may be cleared if its a pooled connection
         */

        if ( connection ) {
            for ( i = 0; 
                i < sizeof( cl_template_func ) / sizeof( cl_template_func[ 0 ] ); 
                i ++ )
            {
                connection -> functions[ i ] =
                    cl_connection -> functions[ i ];
            }

            /*
             * replace the driver dbc
             */
    
            connection -> driver_dbc = 
                cl_connection -> driver_dbc;
        }

        /*
         * release the allocated memory
         */

        if ( cl_connection -> functions ) {
            free( cl_connection -> functions );
        }

        free( cl_connection );
    }

    return ret;
}

