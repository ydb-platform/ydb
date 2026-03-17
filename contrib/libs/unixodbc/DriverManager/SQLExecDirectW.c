/*********************************************************************
 *
 * (pharvey@codebydesign.com).
 *
 * Modified and extended by Nick Gorham
 * (nick@lurcher.org).
 *
 * copyright (c) 1999 Nick Gorham
 *
 * Any bugs or problems should be considered the fault of Nick and not
 * Peter.
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
 * $Id: SQLExecDirectW.c,v 1.10 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLExecDirectW.c,v $
 * Revision 1.10  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.9  2008/08/29 08:01:38  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.8  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.7  2006/04/11 10:22:56  lurcher
 * Fix a data type check
 *
 * Revision 1.6  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.4  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.3  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.2  2002/01/15 14:47:44  lurcher
 *
 * Reset stmt->prepared flag after entering a SQLParamData state from
 * SQLExecDirect
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/01/04 13:16:25  nick
 *
 * Add support for GNU portable threads and tidy up some UNICODE compile
 * warnings
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLExecDirectW.c,v $";

SQLRETURN SQLExecDirectW( SQLHSTMT statement_handle,
           SQLWCHAR *statement_text,
           SQLINTEGER text_length )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
    SQLCHAR *s1;
    SQLCHAR s2[ 100 + LOG_MESSAGE_LEN ];

    /*
     * check statement
     */

    if ( !__validate_stmt( statement ))
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

#ifdef WITH_HANDLE_REDIRECT
		{
			DMHSTMT parent_statement;

			parent_statement = find_parent_handle( statement, SQL_HANDLE_STMT );

			if ( parent_statement ) {
        		dm_log_write( __FILE__, 
                	__LINE__, 
                    	LOG_INFO, 
                    	LOG_INFO, 
                    	"Info: found parent handle" );

				if ( CHECK_SQLEXECDIRECTW( parent_statement -> connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

                	return  SQLEXECDIRECTW( parent_statement -> connection,
							statement,
							statement_text,
							text_length );
				}
			}
		}
#endif
        return SQL_INVALID_HANDLE;
    }

    function_entry( statement );

    if ( log_info.log_flag )
    {
        /*
         * allocate some space for the buffer
         */

        if ( statement_text && text_length == SQL_NTS )
        {
             s1 = malloc( wide_strlen( statement_text ) * 2 + LOG_MESSAGE_LEN * 2 );
        }
        else if ( statement_text )
        {
            s1 = malloc( text_length + LOG_MESSAGE_LEN * 2 );
        }
        else
        {
            s1 = malloc( LOG_MESSAGE_LEN * 2 );
        }

        sprintf( statement -> msg, "\n\t\tEntry:\
\n\t\t\tStatement = %p\
\n\t\t\tSQL = %s",
                statement,
                __wstring_with_length( s1, statement_text, text_length ));

        free( s1 );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    if ( !statement_text )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY009" );

        __post_internal_error( &statement -> error,
                ERROR_HY009, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( text_length <= 0 && text_length != SQL_NTS )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY090" );

        __post_internal_error( &statement -> error,
                ERROR_HY090, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    /*
     * check states
     */

#ifdef NR_PROBE
    if ( statement -> state == STATE_S5 ||
            statement -> state == STATE_S6 ||
            statement -> state == STATE_S7 )
#else
    if (( statement -> state == STATE_S6 && statement -> eod == 0 ) ||
            statement -> state == STATE_S7 )
#endif
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 24000" );

        __post_internal_error( &statement -> error,
                ERROR_24000, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    else if ( statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &statement -> error,
                ERROR_HY010, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 )
    {
        if ( statement -> interupted_func != SQL_API_SQLEXECDIRECT )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY010" );

            __post_internal_error( &statement -> error,
                    ERROR_HY010, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
    }

    if ( statement -> connection -> unicode_driver ||
		    CHECK_SQLEXECDIRECTW( statement -> connection ))
    {
#ifdef NR_PROBE
        if ( !CHECK_SQLEXECDIRECTW( statement -> connection ) ||
                !CHECK_SQLNUMRESULTCOLS( statement -> connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &statement -> error,
                    ERROR_IM001, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
#else
        if ( !CHECK_SQLEXECDIRECTW( statement -> connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &statement -> error,
                    ERROR_IM001, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
#endif

        ret = SQLEXECDIRECTW( statement -> connection,
                statement -> driver_stmt,
                statement_text,
                text_length );
    }
    else
    {
        SQLCHAR *as1 = NULL;
        int clen;

#ifdef NR_PROBE
        if ( !CHECK_SQLEXECDIRECT( statement -> connection ) ||
                !CHECK_SQLNUMRESULTCOLS( statement -> connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &statement -> error,
                    ERROR_IM001, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
#else
        if ( !CHECK_SQLEXECDIRECT( statement -> connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &statement -> error,
                    ERROR_IM001, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
#endif

        as1 = (SQLCHAR*) unicode_to_ansi_alloc( statement_text, text_length, statement -> connection, &clen );

        text_length = clen;

        ret = SQLEXECDIRECT( statement -> connection,
                statement -> driver_stmt,
                as1,
                text_length );

        if ( as1 )
            free( as1 );
    }

    if ( SQL_SUCCEEDED( ret ))
    {
#ifdef NR_PROBE
        SQLRETURN local_ret;
  
        /*
         * grab any errors
         */

        if ( ret == SQL_SUCCESS_WITH_INFO )
        {
            function_return_ex( IGNORE_THREAD, statement, ret, TRUE, DEFER_R1 );
        }

        local_ret = SQLNUMRESULTCOLS( statement -> connection,
                statement -> driver_stmt, &statement -> numcols );

        if ( statement -> numcols > 0 )
        {
            statement -> state = STATE_S5;
        }
        else
        {
            statement -> state = STATE_S4;
        }
#else
        /*
         * We don't know for sure
         */
        statement -> hascols = 1;
        statement -> state = STATE_S5;
#endif

        statement -> prepared = 0;

        /*
         * there is a issue here with transactions, but for the
         * moment
         *
        statement -> connection -> state = STATE_C6;
         */
    }
    else if ( ret == SQL_NO_DATA )
    {
        statement -> state = STATE_S4;
        statement -> prepared = 0;
    }
    else if ( ret == SQL_NEED_DATA )
    {
        statement -> interupted_func = SQL_API_SQLEXECDIRECT;
        statement -> interupted_state = statement -> state;
        statement -> state = STATE_S8;

        statement -> prepared = 0;
    }
    else if ( ret == SQL_PARAM_DATA_AVAILABLE )
    {
        statement -> interupted_func = SQL_API_SQLEXECDIRECT;
        statement -> interupted_state = statement -> state;
        statement -> state = STATE_S13;
    }
    else if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLEXECDIRECT;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;

        statement -> prepared = 0;
    }
    else if (( statement -> state >= STATE_S2 && statement -> state <= STATE_S4 ) ||
              ( statement -> state >= STATE_S11 && statement -> state <= STATE_S12 &&
              statement -> interupted_state >= STATE_S2 && statement -> interupted_state <= STATE_S4 ))
    {
        statement -> state = STATE_S1;
    }
    else if ( statement -> state >= STATE_S11 && statement -> state <= STATE_S12 )
    {
        statement -> state = statement -> interupted_state;
    }

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s2 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R1 );
}
