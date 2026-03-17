/*********************************************************************
 *
 * Written by Nick Gorham
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
 * $Id: __attribute.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: __attribute.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.7  2007/07/13 09:01:08  lurcher
 * Add isql option to quote field data
 *
 * Revision 1.6  2006/04/18 10:24:47  lurcher
 * Add a couple of changes from Mark Vanderwiel
 *
 * Revision 1.5  2004/10/27 08:57:57  lurcher
 * Remove -module from cur Makefile.am, it seems to stop the lib building on HPUX...
 *
 * Revision 1.4  2004/06/21 10:01:12  lurcher
 *
 * Fix a couple of 64 bit issues
 *
 * Revision 1.3  2003/01/23 15:33:25  lurcher
 *
 * Fix problems with using putenv()
 *
 * Revision 1.2  2002/02/21 18:44:09  lurcher
 *
 * Fix bug on 32 bit platforms without long long support
 * Add option to set environment variables from the ini file
 *
 * Revision 1.1.1.1  2001/10/17 16:40:09  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 *
 **********************************************************************/

#include <config.h>
#include <string.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: __attribute.c,v $";

/*
 * these are taken directly from odbctest/attr.cpp
 * so any bugs or additions, should be added there also
 */

typedef struct attr_value
{
	char	*text;
	int 	value;
	char 	*version;
	int		data_type;
} attr_value;

typedef struct attr_options
{
	char	*text;
	int		attr;
	attr_value values[ 6 ];
	char 	*version;
	int		data_type;
	int		is_bitmap;
	int		is_pointer;
} attr_options;

static attr_options stmt_options[] = 
{
	{ "SQL_ATTR_APP_PARAM_DESC", SQL_ATTR_APP_PARAM_DESC, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_APP_ROW_DESC", SQL_ATTR_APP_ROW_DESC, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ASYNC_ENABLE", SQL_ATTR_ASYNC_ENABLE, 
		{
			{ "SQL_ASYNC_ENABLE_OFF", SQL_ASYNC_ENABLE_OFF }, 
			{ "SQL_ASYNC_ENABLE_ON", SQL_ASYNC_ENABLE_ON }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CONCURRENCY", SQL_ATTR_CONCURRENCY, 
		{
			{ "SQL_CONCUR_READ_ONLY", SQL_CONCUR_READ_ONLY }, 
			{ "SQL_CONCUR_LOCK", SQL_CONCUR_LOCK }, 
			{ "SQL_CONCUR_ROWVER", SQL_CONCUR_ROWVER }, 
			{ "SQL_CONCUR_VALUES", SQL_CONCUR_VALUES }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CURSOR_SCROLLABLE", SQL_ATTR_CURSOR_SCROLLABLE, 
		{
			{ "SQL_NONSCROLLABLE", SQL_NONSCROLLABLE }, 
			{ "SQL_SCROLLABLE", SQL_SCROLLABLE }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CURSOR_SENSITIVITY", SQL_ATTR_CURSOR_SENSITIVITY, 
		{
			{ "SQL_UNSPECIFIED", SQL_UNSPECIFIED }, 
			{ "SQL_INSENSITIVE", SQL_INSENSITIVE }, 
			{ "SQL_SENSITIVE", SQL_SENSITIVE }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CURSOR_TYPE", SQL_ATTR_CURSOR_TYPE, 
		{
			{ "SQL_CURSOR_FORWARD_ONLY", SQL_CURSOR_FORWARD_ONLY }, 
			{ "SQL_CURSOR_STATIC", SQL_CURSOR_STATIC }, 
			{ "SQL_CURSOR_KEYSET_DRIVEN", SQL_CURSOR_KEYSET_DRIVEN }, 
			{ "SQL_CURSOR_DYNAMIC", SQL_CURSOR_DYNAMIC }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ENABLE_AUTO_IPD", SQL_ATTR_ENABLE_AUTO_IPD, 
		{
			{ "SQL_FALSE", SQL_FALSE }, 
			{ "SQL_TRUE", SQL_TRUE }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_FETCH_BOOKMARK_PTR", SQL_ATTR_FETCH_BOOKMARK_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER, FALSE, TRUE
	},
	{ "SQL_ATTR_FETCH_IMP_PARAM_DESC", SQL_ATTR_IMP_PARAM_DESC, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_FETCH_IMP_ROW_DESC", SQL_ATTR_IMP_ROW_DESC, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_KEYSET_SIZE", SQL_ATTR_KEYSET_SIZE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_MAX_LENGTH", SQL_ATTR_MAX_LENGTH, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_MAX_ROWS", SQL_ATTR_MAX_ROWS, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_METADATA_ID", SQL_ATTR_METADATA_ID, 
		{
			{ "SQL_FALSE", SQL_FALSE }, 
			{ "SQL_TRUE", SQL_TRUE }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_NOSCAN", SQL_ATTR_NOSCAN, 
		{
			{ "SQL_NOSCAN_OFF", SQL_NOSCAN_OFF }, 
			{ "SQL_NOSCAN_ON", SQL_NOSCAN_ON }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_PARAM_BIND_OFFSET_PTR", SQL_ATTR_PARAM_BIND_OFFSET_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER, FALSE, TRUE
	},
	{ "SQL_ATTR_PARAM_BIND_TYPE", SQL_ATTR_PARAM_BIND_TYPE, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_PARAM_OPERATION_PTR", SQL_ATTR_PARAM_OPERATION_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_SMALLINT, FALSE, TRUE
	},
	{ "SQL_ATTR_PARAM_STATUS_PTR", SQL_ATTR_PARAM_STATUS_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_SMALLINT, FALSE, TRUE
	},
	{ "SQL_ATTR_PARAMS_PROCESSED_PTR", SQL_ATTR_PARAMS_PROCESSED_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_SMALLINT, FALSE, TRUE
	},
	{ "SQL_ATTR_PARAMSET_SIZE", SQL_ATTR_PARAMSET_SIZE, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_QUERY_TIMEOUT", SQL_ATTR_QUERY_TIMEOUT, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_RETRIEVE_DATA", SQL_ATTR_RETRIEVE_DATA, 
		{
			{ "SQL_RD_ON", SQL_RD_ON }, 
			{ "SQL_RD_OFF", SQL_RD_OFF }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ROW_ARRAY_SIZE", SQL_ATTR_ROW_ARRAY_SIZE, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ROW_BIND_OFFSET_PTR", SQL_ATTR_ROW_BIND_OFFSET_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER, FALSE, TRUE
	},
	{ "SQL_ATTR_ROW_BIND_TYPE", SQL_ATTR_ROW_BIND_TYPE, 
		{
			{ "SQL_BIND_BY_COLUMN", SQL_BIND_BY_COLUMN }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ROW_NUMBER", SQL_ATTR_ROW_NUMBER, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ROW_OPERATION_PTR", SQL_ATTR_ROW_OPERATION_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_SMALLINT, FALSE, TRUE
	},
	{ "SQL_ATTR_ROW_STATUS_PTR", SQL_ATTR_ROW_STATUS_PTR,
		{
			{ NULL }
		}, "3.0", SQL_SMALLINT, FALSE, TRUE
	},
	{ "SQL_ATTR_ROWS_FETCHED_PTR", SQL_ATTR_ROWS_FETCHED_PTR, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_SIMULATE_CURSOR", SQL_ATTR_SIMULATE_CURSOR, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_USE_BOOKMARKS", SQL_ATTR_USE_BOOKMARKS,
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ NULL 
	}
};

static attr_options stmt_opt_options[] = 
{
	{ "SQL_ASYNC_ENABLE", SQL_ASYNC_ENABLE, 
		{
			{ "SQL_ASYNC_ENABLE_OFF", SQL_ASYNC_ENABLE_OFF }, 
			{ "SQL_ASYNC_ENABLE_ON", SQL_ASYNC_ENABLE_ON }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_BIND_TYPE", SQL_BIND_TYPE, 
		{
			{ "SQL_BIND_BY_COLUMN", SQL_BIND_BY_COLUMN }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_CONCURRENCY", SQL_CONCURRENCY, 
		{
			{ "SQL_CONCUR_READ_ONLY", SQL_CONCUR_READ_ONLY }, 
			{ "SQL_CONCUR_LOCK", SQL_CONCUR_LOCK }, 
			{ "SQL_CONCUR_ROWVER", SQL_CONCUR_ROWVER }, 
			{ "SQL_CONCUR_VALUES", SQL_CONCUR_VALUES }, 
			{ "SQL_CONCUR_READ_ONLY", SQL_CONCUR_READ_ONLY }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_CURSOR_TYPE", SQL_CURSOR_TYPE, 
		{
			{ "SQL_CURSOR_FORWARD_ONLY", SQL_CURSOR_FORWARD_ONLY }, 
			{ "SQL_CURSOR_STATIC", SQL_CURSOR_STATIC }, 
			{ "SQL_CURSOR_KEYSET_DRIVEN", SQL_CURSOR_KEYSET_DRIVEN }, 
			{ "SQL_CURSOR_DYNAMIC", SQL_CURSOR_DYNAMIC }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_KEYSET_SIZE", SQL_KEYSET_SIZE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_MAX_LENGTH", SQL_MAX_LENGTH, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_MAX_ROWS", SQL_MAX_ROWS, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_NOSCAN", SQL_NOSCAN, 
		{
			{ "SQL_NOSCAN_OFF", SQL_NOSCAN_OFF }, 
			{ "SQL_NOSCAN_ON", SQL_NOSCAN_ON }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_QUERY_TIMEOUT", SQL_QUERY_TIMEOUT, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_RETRIEVE_DATA", SQL_RETRIEVE_DATA, 
		{
			{ "SQL_RD_ON", SQL_RD_ON }, 
			{ "SQL_RD_OFF", SQL_RD_OFF }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ROWSET_SIZE", SQL_ROWSET_SIZE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_SIMULATE_CURSOR", SQL_SIMULATE_CURSOR, 
		{
			{ "SQL_SC_NON_UNIQUE", SQL_SC_NON_UNIQUE }, 
			{ "SQL_SC_TRY_UNIQUE", SQL_SC_TRY_UNIQUE }, 
			{ "SQL_SC_UNIQUE", SQL_SC_UNIQUE }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_USE_BOOKMARKS", SQL_USE_BOOKMARKS, 
		{
			{ "SQL_UB_ON", SQL_UB_ON }, 
			{ "SQL_UB_OFF", SQL_UB_OFF }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ NULL 
	}
};

static attr_options conn_options[] = 
{
	{ "SQL_ATTR_ACCESS_MODE", SQL_ATTR_ACCESS_MODE, 
		{
			{ "SQL_MODE_READ_WRITE", SQL_MODE_READ_WRITE }, 
			{ "SQL_MODE_READ_ONLY", SQL_MODE_READ_ONLY }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ASYNC_ENABLE", SQL_ATTR_ASYNC_ENABLE, 
		{
			{ "SQL_ASYNC_ENABLE_OFF", SQL_ASYNC_ENABLE_OFF }, 
			{ "SQL_ASYNC_ENABLE_ON", SQL_ASYNC_ENABLE_ON }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_AUTO_IPD", SQL_ATTR_AUTO_IPD, 
		{
			{ "SQL_TRUE", SQL_TRUE }, 
			{ "SQL_FALSE", SQL_FALSE }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_AUTOCOMMIT", SQL_ATTR_AUTOCOMMIT, 
		{
			{ "SQL_AUTOCOMMIT_ON", SQL_AUTOCOMMIT_ON }, 
			{ "SQL_AUTOCOMMIT_OFF", SQL_AUTOCOMMIT_OFF }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CONNECTION_TIMEOUT", SQL_ATTR_CONNECTION_TIMEOUT, 
		{
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CURRENT_CATALOG", SQL_ATTR_CURRENT_CATALOG, 
		{
			{ NULL }
		}, "2.0", SQL_CHAR
	},
	{ "SQL_ATTR_LOGIN_TIMEOUT", SQL_ATTR_LOGIN_TIMEOUT, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_METADATA_ID", SQL_ATTR_METADATA_ID, 
		{
			{ "SQL_TRUE", SQL_TRUE }, 
			{ "SQL_FALSE", SQL_FALSE }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_ODBC_CURSORS", SQL_ATTR_ODBC_CURSORS, 
		{
			{ "SQL_CUR_USE_IF_NEEDED", SQL_CUR_USE_IF_NEEDED }, 
			{ "SQL_CUR_USE_ODBC", SQL_CUR_USE_ODBC }, 
			{ "SQL_CUR_USE_DRIVER", SQL_CUR_USE_DRIVER }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_PACKET_SIZE", SQL_ATTR_PACKET_SIZE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_QUIET_MODE", SQL_ATTR_QUIET_MODE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "SQL_ATTR_TRACE", SQL_ATTR_TRACE, 
		{
			{ "SQL_OPT_TRACE_OFF", SQL_OPT_TRACE_OFF }, 
			{ "SQL_OPT_TRACE_ON", SQL_OPT_TRACE_ON }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_TRACEFILE", SQL_ATTR_TRACEFILE, 
		{
			{ NULL }
		}, "1.0", SQL_CHAR
	},
	{ "SQL_ATTR_TRANSLATE_LIB", SQL_ATTR_TRANSLATE_LIB, 
		{
			{ NULL }
		}, "1.0", SQL_CHAR
	},
	{ "SQL_ATTR_TRANSLATE_OPTION", SQL_ATTR_TRANSLATE_OPTION, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "SQL_ATTR_TXN_ISOLATION", SQL_ATTR_TXN_ISOLATION, 
		{
			{ "SQL_TXN_READ_UNCOMMITTED", SQL_TXN_READ_UNCOMMITTED },
			{ "SQL_TXN_READ_COMMITTED", SQL_TXN_READ_COMMITTED },
			{ "SQL_TXN_REPEATABLE_READ", SQL_TXN_REPEATABLE_READ },
			{ "SQL_TXN_SERIALIZABLE", SQL_TXN_SERIALIZABLE },
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ NULL 
	}
};

static attr_options conn_opt_options[] = 
{
	{ "conn: SQL_ACCESS_MODE", SQL_ACCESS_MODE, 
		{
			{ "SQL_MODE_READ_ONLY", SQL_MODE_READ_ONLY }, 
			{ "SQL_MODE_READ_WRITE", SQL_MODE_READ_WRITE }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "conn: SQL_AUTOCOMMIT", SQL_AUTOCOMMIT, 
		{
			{ "SQL_AUTOCOMMIT_ON", SQL_AUTOCOMMIT_ON }, 
			{ "SQL_AUTOCOMMIT_OFF", SQL_AUTOCOMMIT_OFF }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "conn: SQL_CURRENT_QUALIFIER", SQL_CURRENT_QUALIFIER, 
		{
			{ NULL }
		}, "2.0", SQL_CHAR
	},
	{ "conn: SQL_LOGIN_TIMEOUT", SQL_LOGIN_TIMEOUT, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "conn: SQL_ODBC_CURSORS", SQL_ODBC_CURSORS, 
		{
			{ "SQL_CUR_USE_IF_NEEDED", SQL_CUR_USE_IF_NEEDED }, 
			{ "SQL_CUR_USE_ODBC", SQL_CUR_USE_ODBC }, 
			{ "SQL_CUR_USE_DRIVER", SQL_CUR_USE_DRIVER }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "conn: SQL_OPT_TRACE", SQL_OPT_TRACE, 
		{
			{ "SQL_OPT_TRACE_ON", SQL_OPT_TRACE_ON }, 
			{ "SQL_OPT_TRACE_OFF", SQL_OPT_TRACE_OFF }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "conn: SQL_OPT_TRACEFILE", SQL_OPT_TRACEFILE, 
		{
			{ NULL }
		}, "1.0", SQL_CHAR
	},
	{ "conn: SQL_PACKET_SIZE", SQL_PACKET_SIZE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "conn: SQL_QUIET_MODE", SQL_QUIET_MODE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "conn: SQL_TRANSLATE_DLL", SQL_TRANSLATE_DLL, 
		{
			{ NULL }
		}, "1.0", SQL_CHAR
	},
	{ "conn: SQL_TRANSLATE_OPTION", SQL_TRANSLATE_OPTION, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "conn: SQL_TXN_ISOLATION", SQL_TXN_ISOLATION, 
		{
			{ "SQL_TXN_READ_UNCOMMITED", SQL_TXN_READ_UNCOMMITTED }, 
			{ "SQL_TXN_READ_COMMITED", SQL_TXN_READ_COMMITTED }, 
			{ "SQL_TXN_REPEATABLE_READ", SQL_TXN_REPEATABLE_READ }, 
			{ "SQL_TXN_SERIALIZABLE", SQL_TXN_SERIALIZABLE }, 
			{ "SQL_TXN_VERSIONING", 0x00000010L }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "stmt: SQL_ASYNC_ENABLE", SQL_ASYNC_ENABLE, 
		{
			{ "SQL_ASYNC_ENABLE_OFF", SQL_ASYNC_ENABLE_OFF }, 
			{ "SQL_ASYNC_ENABLE_ON", SQL_ASYNC_ENABLE_ON }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "stmt: SQL_BIND_TYPE", SQL_BIND_TYPE, 
		{
			{ "SQL_BIND_BY_COLUMN", SQL_BIND_BY_COLUMN }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "stmt: SQL_CONCURRENCY", SQL_CONCURRENCY, 
		{
			{ "SQL_CONCUR_READ_ONLY", SQL_CONCUR_READ_ONLY }, 
			{ "SQL_CONCUR_LOCK", SQL_CONCUR_LOCK }, 
			{ "SQL_CONCUR_ROWVER", SQL_CONCUR_ROWVER }, 
			{ "SQL_CONCUR_VALUES", SQL_CONCUR_VALUES }, 
			{ "SQL_CONCUR_READ_ONLY", SQL_CONCUR_READ_ONLY }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "stmt: SQL_CURSOR_TYPE", SQL_CURSOR_TYPE, 
		{
			{ "SQL_CURSOR_FORWARD_ONLY", SQL_CURSOR_FORWARD_ONLY }, 
			{ "SQL_CURSOR_STATIC", SQL_CURSOR_STATIC }, 
			{ "SQL_CURSOR_KEYSET_DRIVEN", SQL_CURSOR_KEYSET_DRIVEN }, 
			{ "SQL_CURSOR_DYNAMIC", SQL_CURSOR_DYNAMIC }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "stmt: SQL_KEYSET_SIZE", SQL_KEYSET_SIZE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "stmt: SQL_MAX_LENGTH", SQL_MAX_LENGTH, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "stmt: SQL_MAX_ROWS", SQL_MAX_ROWS, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "stmt: SQL_NOSCAN", SQL_NOSCAN, 
		{
			{ "SQL_NOSCAN_OFF", SQL_NOSCAN_OFF }, 
			{ "SQL_NOSCAN_ON", SQL_NOSCAN_ON }, 
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "stmt: SQL_QUERY_TIMEOUT", SQL_QUERY_TIMEOUT, 
		{
			{ NULL }
		}, "1.0", SQL_INTEGER
	},
	{ "stmt: SQL_RETRIEVE_DATA", SQL_RETRIEVE_DATA, 
		{
			{ "SQL_RD_ON", SQL_RD_ON }, 
			{ "SQL_RD_OFF", SQL_RD_OFF }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "stmt: SQL_ROWSET_SIZE", SQL_ROWSET_SIZE, 
		{
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "stmt: SQL_SIMULATE_CURSOR", SQL_SIMULATE_CURSOR, 
		{
			{ "SQL_SC_NON_UNIQUE", SQL_SC_NON_UNIQUE }, 
			{ "SQL_SC_TRY_UNIQUE", SQL_SC_TRY_UNIQUE }, 
			{ "SQL_SC_UNIQUE", SQL_SC_UNIQUE }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ "stmt: SQL_USE_BOOKMARKS", SQL_USE_BOOKMARKS, 
		{
			{ "SQL_UB_ON", SQL_UB_ON }, 
			{ "SQL_UB_OFF", SQL_UB_OFF }, 
			{ NULL }
		}, "2.0", SQL_INTEGER
	},
	{ NULL 
	}
};

static attr_options env_options[] = 
{
	{ "SQL_ATTR_ODBC_VERSION", SQL_ATTR_ODBC_VERSION, 
		{
			{ "SQL_OV_ODBC2", SQL_OV_ODBC2 }, 
			{ "SQL_OV_ODBC3", SQL_OV_ODBC3 }, 
			{ "SQL_OV_ODBC3_80", SQL_OV_ODBC3_80 }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CP_MATCH", SQL_ATTR_CP_MATCH,
		{
			{ "SQL_CP_STRICT_MATCH", SQL_CP_STRICT_MATCH },
			{ "SQL_CP_RELAXED_MATCH", SQL_CP_RELAXED_MATCH },
			{ "SQL_CP_MATCH_DEFAULT", SQL_CP_MATCH_DEFAULT },
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_CONNECTION_POOLING", SQL_ATTR_CONNECTION_POOLING, 
		{
			{ "SQL_CP_OFF", SQL_OV_ODBC2 }, 
			{ "SQL_CP_ONE_PER_DRIVER", SQL_CP_ONE_PER_DRIVER }, 
			{ "SQL_CP_ONE_PER_HENV", SQL_CP_ONE_PER_HENV }, 
			{ "SQL_CP_DEFAULT", SQL_CP_DEFAULT }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
	{ "SQL_ATTR_OUTPUT_NTS", SQL_ATTR_OUTPUT_NTS, 
		{
			{ "SQL_TRUE", SQL_TRUE }, 
			{ "SQL_FALSE", SQL_FALSE }, 
			{ NULL }
		}, "3.0", SQL_INTEGER
	},
    { "SQL_ATTR_UNIXODBC_ENVATTR", SQL_ATTR_UNIXODBC_ENVATTR,
        {
            { NULL }
        }, "3.0", SQL_CHAR
            
    },
    {
        NULL 
	}
};

static int find_option( char *kw, struct attr_set *as, struct attr_options *opt )
{
struct attr_value *val;
int found = 0;

    while( opt -> text && !found )
    {
        if ( strcasecmp( kw, opt -> text ) == 0 )
        {
            found = 1;
            val = opt -> values;
            as -> attribute = opt -> attr;

            while ( val -> text )
            {
                if ( strcasecmp( as -> value, val -> text ) == 0 )
                {
                    break;
                }
                val ++;
            }

            if ( val -> text )
            {
                as -> is_int_type = 1;
                as -> int_value = val -> value;
            }
            else
            {
                if ( opt -> data_type != SQL_CHAR )
                {
                    as -> is_int_type = 1;
                    as -> int_value = atoi( as -> value );
                }
            }
        }
        opt ++;
    }

    /*
     * Handle non standard attributes by [numeric_value]={char value} or [numeric_value]=\int_value
     */
    if ( !found ) 
    {
        if ( kw[ 0 ] == '[' ) {
            as -> attribute = atoi( kw + 1 );
            if ( as -> value[ 0 ] == '\\' ) {
                as -> is_int_type = 1;
                as -> int_value = atoi( as -> value + 1 );
            }
            found = 1;
        }
    }

    return found;
}

struct attr_set * __get_set( char ** cp, int *skip )
{
char *ptr, *kw;
int len;
struct attr_set *as;

    /*
     * flag to indicate a non valid option
     */

    *skip = 0;

    ptr = *cp;

    if ( !**cp )
        return NULL;

    while ( **cp && **cp != '=' )
    {
        (*cp)++;
    }

    if ( !**cp )
        return NULL;

    as = malloc( sizeof( struct attr_set ));
    if ( !as )
    {
        return NULL;
    }

    memset( as, 0, sizeof( struct attr_set ));

    len = *cp - ptr;
    as -> keyword = malloc( len + 1 );
    memcpy( as -> keyword, ptr, len );
    as -> keyword[ len ] = '\0';

    (*cp)++;
    ptr = *cp;

    if ( **cp && **cp == '{' )
    {
        (*cp)++;
        ptr ++;
        while ( **cp && **cp != '}' )
            (*cp)++;

        len = *cp - ptr;
        as -> value = malloc( len + 1 );
        memcpy( as -> value, ptr , len );
        as -> value[ len ] = '\0';
        (*cp)++;
    }
    else
    {
        while ( **cp && **cp != ';' )
            (*cp)++;

        len = *cp - ptr;
        as -> value = malloc( len + 1 );
        memcpy( as -> value, ptr, len );
        as -> value[ len ] = '\0';
    }

    /*
     * now we translate the keyword and attribute values
     */

    if ( as -> keyword[ 0 ] == '*' )
    {
        kw = as -> keyword + 1;
        as -> override = 1;
    }
    else
    {
        kw = as -> keyword;
    }


    if ( !find_option( kw, as, env_options ) &&
        !find_option( kw, as, conn_options ) &&
        !find_option( kw, as, conn_opt_options ) &&
        !find_option( kw, as, stmt_options ) &&
        !find_option( kw, as, stmt_opt_options ))
    {
        *skip = 1;
    }

    if ( **cp )
        (*cp)++;

    return as;
}

int __append_set( struct attr_struct *attr_str, struct attr_set *ap )
{
struct attr_set *ptr, *end, *nap;

    /* check that the attribute is not already in the list */

    end = NULL;
    if ( attr_str -> count > 0 )
    {
        ptr = attr_str -> list;
        while( ptr )
        {
            if( ap -> attribute ==  ptr -> attribute )
            {
                return 0;
            }
            end = ptr;
            ptr = ptr -> next;
        }
    }

    nap = malloc( sizeof( *ptr ));
    *nap = *ap;

    nap -> keyword = malloc( strlen( ap -> keyword ) + 1 );
    strcpy( nap -> keyword, ap -> keyword );

    nap -> value = malloc( strlen( ap -> value ) + 1 );
    strcpy( nap -> value, ap -> value );

    attr_str -> count ++;

    if ( attr_str -> list )
    {
        end -> next = nap;
        nap -> next = NULL;
    }
    else
    {
        nap -> next = NULL;
        attr_str -> list = nap;
    }

    return 0;
}

int __parse_attribute_string( struct attr_struct *attr_str,
    char *str, int str_len )
{
struct attr_set *cp;
char *local_str, *ptr;
int skip;

    attr_str -> count = 0;
    attr_str -> list = NULL;

    if ( str_len != SQL_NTS )
    {
        local_str = malloc( str_len + 1 );
        memcpy( local_str, str, str_len );
        local_str[ str_len ] = '\0';
    }
    else
    {
        local_str = str;
    }

    ptr = local_str;

    while(( cp = __get_set( &ptr, &skip )) != NULL )
    {
        if ( !skip )
        {
            __append_set( attr_str, cp );
        }
        free( cp -> keyword );
        free( cp -> value );
        free( cp );
    }

    if ( str_len != SQL_NTS )
        free( local_str );

    return 0;
}

void __release_attr_str( struct attr_struct *attr_str )
{
    struct attr_set *set, *ptr;

    if ( !attr_str )
    {
        return;
    }

    set = attr_str -> list;

    while ( set )
    {
        ptr = set -> next;

        free( set -> keyword );
        free( set -> value );
        free( set );

        set = ptr;
    }

    attr_str -> list = NULL;
    attr_str -> count = 0;
}


static void __set_local_attribute( void *handle, int type, struct attr_set *as )
{
    SQLRETURN ret = SQL_SUCCESS;

    if ( type == SQL_HANDLE_ENV )
    {
        DMHDBC connection = (DMHDBC) handle;

        if ( as -> attribute == SQL_ATTR_UNIXODBC_ENVATTR )
        {
            /*
             * its a memory leak, but not much I can do, see "man putenv"
             */
            putenv( strdup( as -> value ));
        }
        else
        {
            return;
        }

        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, "\t\tENV ATTR [%s=%s] ret = %d",
                    as -> keyword, as -> value, ret );

            dm_log_write_diag( connection -> msg );
        }
    }
}

static void __set_attribute( void *handle, int type, struct attr_set *as )
{
    SQLRETURN ret = SQL_ERROR;

    if ( type == SQL_HANDLE_ENV )
    {
        DMHDBC connection = (DMHDBC) handle;

        if ( as -> attribute == SQL_ATTR_UNIXODBC_ENVATTR )
        {
            return;
        }

        if ( connection -> driver_version >= SQL_OV_ODBC3 || CHECK_SQLSETENVATTR( connection ))
        {
            if ( CHECK_SQLSETENVATTR( connection ))
            {
                if ( as -> is_int_type )
                {
                    ret = SQLSETENVATTR( connection,
                            connection -> driver_env,
                            as -> attribute,
                            (SQLPOINTER)(intptr_t) as -> int_value,
                            0 );
                }
                else
                {
                    ret = SQLSETENVATTR( connection,
                            connection -> driver_env,
                            as -> attribute,
                            as -> value,
                            strlen( as -> value ));
                }
            }
        }
        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, "\t\tENV ATTR [%s=%s] ret = %d",
                    as -> keyword, as -> value, ret );

            dm_log_write_diag( connection -> msg );
        }
    }
    else if ( type == SQL_HANDLE_DBC )
    {
        DMHDBC connection = (DMHDBC) handle;

        if ( connection -> driver_version >= SQL_OV_ODBC3 )
        {
            if ( CHECK_SQLSETCONNECTATTR( connection ))
            {
                if ( as -> is_int_type )
                {
                    ret = SQLSETCONNECTATTR( connection,
                            connection -> driver_dbc,
                            as -> attribute,
                            (SQLPOINTER)(intptr_t) as -> int_value,
                            0 );
                }
                else
                {
                    ret = SQLSETCONNECTATTR( connection,
                            connection -> driver_dbc,
                            as -> attribute,
                            as -> value,
                            strlen( as -> value ));
                }
            }
            else if ( CHECK_SQLSETCONNECTOPTION( connection ))
            {
                if ( as -> is_int_type )
                {
                    ret = SQLSETCONNECTOPTION( connection,
                            connection -> driver_dbc,
                            as -> attribute,
                            as -> int_value );
                }
                else
                {
                    ret = SQLSETCONNECTOPTION( connection,
                            connection -> driver_dbc,
                            as -> attribute,
                            (SQLULEN) as -> value );
                }
            }
        }
        else
        {
            if ( CHECK_SQLSETCONNECTOPTION( connection ))
            {
                if ( as -> is_int_type )
                {
                    ret = SQLSETCONNECTOPTION( connection,
                            connection -> driver_dbc,
                            as -> attribute,
                            as -> int_value );
                }
                else
                {
                    ret = SQLSETCONNECTOPTION( connection,
                            connection -> driver_dbc,
                            as -> attribute,
                            (SQLULEN) as -> value );
                }
            }
        }
        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, "\t\tCONN ATTR [%s=%s] ret = %d",
                    as -> keyword, as -> value, ret );

            dm_log_write_diag( connection -> msg );
        }
    }
    else if ( type == SQL_HANDLE_STMT )
    {
        DMHSTMT statement = (DMHSTMT) handle;
        DMHDBC connection = statement -> connection;

        if ( connection -> driver_version >= SQL_OV_ODBC3 )
        {
            if ( CHECK_SQLSETSTMTATTR( connection ))
            {
                if ( as -> is_int_type )
                {
                    ret = SQLSETSTMTATTR( connection,
                            statement -> driver_stmt,
                            as -> attribute,
                            (SQLPOINTER)(intptr_t) as -> int_value,
                            0 );
                }
                else
                {
                    ret = SQLSETSTMTATTR( connection,
                            statement -> driver_stmt,
                            as -> attribute,
                            as -> value,
                            strlen( as -> value ));
                }
            }
            else if ( CHECK_SQLSETSTMTOPTION( connection ))
            {
                if ( as -> is_int_type )
                {
                    ret = SQLSETSTMTOPTION( connection,
                            statement -> driver_stmt,
                            as -> attribute,
                            as -> int_value );
                }
                else
                {
                    ret = SQLSETSTMTOPTION( connection,
                            statement -> driver_stmt,
                            as -> attribute,
                            (SQLULEN) as -> value );
                }
            }
        }
        else
        {
            if ( CHECK_SQLSETSTMTOPTION( connection ))
            {
                if ( as -> is_int_type )
                {
                    ret = SQLSETSTMTOPTION( connection,
                            statement -> driver_stmt,
                            as -> attribute,
                            as -> int_value );
                }
                else
                {
                    ret = SQLSETSTMTOPTION( connection,
                            statement -> driver_stmt,
                            as -> attribute,
                            (SQLULEN) as -> value );
                }
            }

            /*
             * Fall back, the attribute may be ODBC 3 only
             */

            if ( ret == SQL_ERROR ) {

                if ( CHECK_SQLSETSTMTATTR( connection ))
                {
                    if ( as -> is_int_type )
                    {
                        ret = SQLSETSTMTATTR( connection,
                                statement -> driver_stmt,
                                as -> attribute,
                                (SQLPOINTER)(intptr_t) as -> int_value,
                                0 );
                    }
                    else
                    {
                        ret = SQLSETSTMTATTR( connection,
                                statement -> driver_stmt,
                                as -> attribute,
                                as -> value,
                                strlen( as -> value ));
                    }
                }
            }
        }
        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, "\t\tSTMT ATTR [%s=%s] ret = %d",
                    as -> keyword, as -> value, ret );

            dm_log_write_diag( connection -> msg );
        }
    }
}

void __set_local_attributes( void * handle, int type )
{
    struct attr_set *as;

    switch( type )
    {
      case SQL_HANDLE_ENV:
        as = ((DMHDBC) handle ) -> env_attribute.list;
        break;

      default:
        as = NULL;
        break;
    }

    while( as )
    {
        __set_local_attribute( handle, type, as );
        as = as -> next;
    }
}

void __set_attributes( void * handle, int type )
{
    struct attr_set *as;

    switch( type )
    {
      case SQL_HANDLE_ENV:
        as = ((DMHDBC) handle ) -> env_attribute.list;
        break;

      case SQL_HANDLE_DBC:
        as = ((DMHDBC) handle ) -> dbc_attribute.list;
        break;

      case SQL_HANDLE_STMT:
        as = ((DMHSTMT) handle ) -> connection -> stmt_attribute.list;
        break;

      default:
        as = NULL;
        break;
    }

    while( as )
    {
        __set_attribute( handle, type, as );
        as = as -> next;
    }
}

void *__attr_override( void *handle, int type, int attribute, void *value, SQLINTEGER *string_length )
{
    struct attr_set *as;
    char *msg;

    switch( type )
    {
      case SQL_HANDLE_DBC:
        as = ((DMHDBC) handle ) -> dbc_attribute.list;
        msg = ((DMHDBC) handle ) -> msg;
        break;

      case SQL_HANDLE_STMT:
        as = ((DMHSTMT) handle ) -> connection -> stmt_attribute.list;
        msg = ((DMHSTMT) handle ) -> msg;
        break;

      default:
        as = NULL;
		msg = NULL;
        break;
    }

    while( as )
    {
        if ( as -> override && as -> attribute == attribute )
        {
            break;
        }
        as = as -> next;
    }

    if ( as )
    {
        if ( log_info.log_flag )
        {
            sprintf( msg, "\t\tATTR OVERRIDE [%s=%s]",
                    as -> keyword + 1, as -> value );

            dm_log_write_diag( msg );
        }

        if ( as -> is_int_type )
        {
#ifdef HAVE_PTRDIFF_T
            return (void*)(ptrdiff_t) as -> int_value;
#else
            return (void*)(long) as -> int_value;
#endif
        }
        else
        {
            if ( string_length )
            {
                *string_length = strlen( as -> value );
            }
            return as -> value;
        }
    }
    else
    {
        return value;
    }
}

void *__attr_override_wide( void *handle, int type, int attribute, void *value, SQLINTEGER *string_length, 
		SQLWCHAR *buffer )
{
    struct attr_set *as;
    char *msg;

    switch( type )
    {
      case SQL_HANDLE_DBC:
        as = ((DMHDBC) handle ) -> dbc_attribute.list;
        msg = ((DMHDBC) handle ) -> msg;
        break;

      case SQL_HANDLE_STMT:
        as = ((DMHSTMT) handle ) -> connection -> stmt_attribute.list;
        msg = ((DMHSTMT) handle ) -> msg;
        break;

      default:
        as = NULL;
		msg = NULL;
        break;
    }

    while( as )
    {
        if ( as -> override && as -> attribute == attribute )
        {
            break;
        }
        as = as -> next;
    }

    if ( as )
    {
        if ( log_info.log_flag )
        {
            sprintf( msg, "\t\tATTR OVERRIDE [%s=%s]",
                    as -> keyword + 1, as -> value );

            dm_log_write_diag( msg );
        }

        if ( as -> is_int_type )
        {
#ifdef HAVE_PTRDIFF_T
            return (void*)(ptrdiff_t) as -> int_value;
#else
            return (void*)(long) as -> int_value;
#endif
        }
        else
        {
            if ( string_length )
            {
                *string_length = strlen( as -> value ) * sizeof( SQLWCHAR );
            }
			switch( type ) 
			{
      			case SQL_HANDLE_DBC:
					ansi_to_unicode_copy( buffer, as->value, SQL_NTS, (DMHDBC) handle, NULL );
					break;

      			case SQL_HANDLE_STMT:
					ansi_to_unicode_copy( buffer, as->value, SQL_NTS, ((DMHSTMT) handle ) -> connection, NULL );
					break;
			}
            return buffer;
        }
    }
    else
    {
        return value;
    }
}

/*
 * check for valid attributes in the setting functions
 */

int dm_check_connection_attrs( DMHDBC connection, SQLINTEGER attribute, SQLPOINTER value )
{
#ifdef HAVE_PTRDIFF_T
    ptrdiff_t ival;
#else
    SQLINTEGER ival;
#endif

#ifdef HAVE_PTRDIFF_T
    ival = (ptrdiff_t) value;
#else
    ival = (SQLINTEGER) value;
#endif

    switch( attribute ) 
    {
        case SQL_ACCESS_MODE:
            if ( ival != SQL_MODE_READ_ONLY && ival != SQL_MODE_READ_WRITE ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_ASYNC_ENABLE:
            if ( ival != SQL_ASYNC_ENABLE_OFF && ival != SQL_ASYNC_ENABLE_ON ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_AUTO_IPD:
            if ( ival != SQL_TRUE && ival != SQL_FALSE ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_AUTOCOMMIT:
            if ( ival != SQL_AUTOCOMMIT_ON && ival != SQL_AUTOCOMMIT_OFF ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_METADATA_ID:
            if ( ival != SQL_TRUE && ival != SQL_FALSE ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_ODBC_CURSORS:
            if ( ival != SQL_CUR_USE_IF_NEEDED && ival != SQL_CUR_USE_ODBC &&
                ival != SQL_CUR_USE_DRIVER ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_TRACE:
            if ( ival != SQL_OPT_TRACE_ON && ival != SQL_OPT_TRACE_OFF ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_TXN_ISOLATION:
            if ( ival != SQL_TXN_READ_UNCOMMITTED && ival != SQL_TXN_READ_COMMITTED
                && ival != SQL_TXN_REPEATABLE_READ && ival != SQL_TXN_SERIALIZABLE )
            {
                return SQL_ERROR;
            }
            break;

        /*
         * include statement attributes as well
         */

        case SQL_ATTR_CONCURRENCY:
            if ( ival != SQL_CONCUR_READ_ONLY && ival != SQL_CONCUR_LOCK &&
                ival != SQL_CONCUR_ROWVER && ival != SQL_CONCUR_VALUES ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CURSOR_SCROLLABLE:
            if ( ival != SQL_NONSCROLLABLE && ival != SQL_SCROLLABLE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CURSOR_SENSITIVITY:
            if ( ival != SQL_UNSPECIFIED && ival != SQL_INSENSITIVE 
                && ival != SQL_SENSITIVE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CURSOR_TYPE:
            if ( ival != SQL_CURSOR_FORWARD_ONLY && ival != SQL_CURSOR_STATIC &&
                ival != SQL_CURSOR_KEYSET_DRIVEN && ival != SQL_CURSOR_DYNAMIC ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_ENABLE_AUTO_IPD:
            if ( ival != SQL_TRUE && ival != SQL_FALSE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_NOSCAN:
            if ( ival != SQL_NOSCAN_ON && ival != SQL_NOSCAN_OFF )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_RETRIEVE_DATA:
            if ( ival != SQL_RD_ON && ival != SQL_RD_OFF )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_SIMULATE_CURSOR:
            if ( ival != SQL_SC_NON_UNIQUE && ival != SQL_SC_TRY_UNIQUE &&
                ival != SQL_SC_UNIQUE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_USE_BOOKMARKS:
            if ( ival != SQL_UB_OFF && ival != SQL_UB_VARIABLE &&
                ival != SQL_UB_FIXED )
            {
                return SQL_ERROR;
            }
            break;

        default:
            return SQL_SUCCESS;
    }

    return SQL_SUCCESS;
}

int dm_check_statement_attrs( DMHSTMT statement, SQLINTEGER attribute, SQLPOINTER value )
{
#ifdef HAVE_PTRDIFF_T
    ptrdiff_t ival;
#else
    SQLUINTEGER ival;
#endif

#ifdef HAVE_PTRDIFF_T
    ival = (ptrdiff_t) value;
#else
    ival = (SQLUINTEGER) value;
#endif

    switch( attribute ) 
    {
        case SQL_ATTR_ASYNC_ENABLE:
            if ( ival != SQL_ASYNC_ENABLE_OFF && ival != SQL_ASYNC_ENABLE_ON ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CONCURRENCY:
            if ( ival != SQL_CONCUR_READ_ONLY && ival != SQL_CONCUR_LOCK &&
                ival != SQL_CONCUR_ROWVER && ival != SQL_CONCUR_VALUES ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CURSOR_SCROLLABLE:
            if ( ival != SQL_NONSCROLLABLE && ival != SQL_SCROLLABLE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CURSOR_SENSITIVITY:
            if ( ival != SQL_UNSPECIFIED && ival != SQL_INSENSITIVE 
                && ival != SQL_SENSITIVE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CURSOR_TYPE:
            if ( ival != SQL_CURSOR_FORWARD_ONLY && ival != SQL_CURSOR_STATIC &&
                ival != SQL_CURSOR_KEYSET_DRIVEN && ival != SQL_CURSOR_DYNAMIC ) 
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_ENABLE_AUTO_IPD:
            if ( ival != SQL_TRUE && ival != SQL_FALSE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_NOSCAN:
            if ( ival != SQL_NOSCAN_ON && ival != SQL_NOSCAN_OFF )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_RETRIEVE_DATA:
            if ( ival != SQL_RD_ON && ival != SQL_RD_OFF )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_SIMULATE_CURSOR:
            if ( ival != SQL_SC_NON_UNIQUE && ival != SQL_SC_TRY_UNIQUE &&
                ival != SQL_SC_UNIQUE )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_USE_BOOKMARKS:
            if ( ival != SQL_UB_OFF && ival != SQL_UB_VARIABLE &&
                ival != SQL_UB_FIXED )
            {
                return SQL_ERROR;
            }
            break;

        case SQL_ROWSET_SIZE:
            return ival > 0 ? SQL_SUCCESS : SQL_ERROR;

        default:
            return SQL_SUCCESS;
    }

    return SQL_SUCCESS;
}

