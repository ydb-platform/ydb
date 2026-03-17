/*********************************************************************
 *
 * This is based on code created by Peter Harvey,
 * (pharvey@codebydesign.com).
 *
 * Modified and extended by Nick Gorham
 * (nick@lurcher.org).
 *
 * Any bugs or problems should be considered the fault of Nick and not
 * Peter.
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
 * $Id: __info.c,v 1.50 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: __info.c,v $
 * Revision 1.50  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.49  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.48  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.47  2008/01/02 15:10:33  lurcher
 * Fix problems trying to use the cursor lib on a non select statement
 *
 * Revision 1.46  2007/11/26 11:37:23  lurcher
 * Sync up before tag
 *
 * Revision 1.45  2007/09/28 13:20:22  lurcher
 * Add timestamp to logging
 *
 * Revision 1.44  2007/04/02 10:50:19  lurcher
 * Fix some 64bit problems (only when sizeof(SQLLEN) == 8 )
 *
 * Revision 1.43  2007/03/05 09:49:24  lurcher
 * Get it to build on VMS again
 *
 * Revision 1.42  2006/11/27 14:08:34  lurcher
 * Sync up dirs
 *
 * Revision 1.41  2006/03/08 11:22:13  lurcher
 * Add check for valid C_TYPE
 *
 * Revision 1.40  2005/12/19 18:43:26  lurcher
 * Add new parts to contrib and alter how the errors are returned from the driver
 *
 * Revision 1.39  2005/11/08 09:37:10  lurcher
 * Allow the driver and application to have different length handles
 *
 * Revision 1.38  2005/02/07 11:46:45  lurcher
 * Add missing ODBC2 installer stubs
 *
 * Revision 1.37  2004/07/24 17:55:37  lurcher
 * Sync up CVS
 *
 * Revision 1.36  2004/05/17 08:25:00  lurcher
 * Update the way the libltso is used, and fix a problem with gODBCConfig
 * not closeing correctly.
 *
 * Revision 1.35  2004/03/30 13:20:11  lurcher
 *
 *
 * Fix problem with SQLCopyDesc
 * Add additional target for iconv
 *
 * Revision 1.34  2003/12/01 16:37:17  lurcher
 *
 * Fix a bug in SQLWritePrivateProfileString
 *
 * Revision 1.33  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.32  2003/09/08 15:34:29  lurcher
 *
 * A couple of small but perfectly formed fixes
 *
 * Revision 1.31  2003/07/21 11:12:59  lurcher
 *
 * Fix corruption in Postgre7.1 driver
 * Tidy up gODBCconfig
 *
 * Revision 1.30  2003/06/24 09:40:58  lurcher
 *
 * Extra UNICODE stuff
 *
 * Revision 1.29  2003/06/04 12:49:45  lurcher
 *
 * Further PID logging tweeks
 *
 * Revision 1.28  2003/06/03 13:52:13  lurcher
 *
 * Change the mode of PID logfiles to allow the process to change to another
 * user
 *
 * Revision 1.27  2003/06/02 16:51:36  lurcher
 *
 * Add TracePid option
 *
 * Revision 1.26  2003/02/25 13:28:31  lurcher
 *
 * Allow errors on the drivers AllocHandle to be reported
 * Fix a problem that caused errors to not be reported in the log
 * Remove a redundant line from the spec file
 *
 * Revision 1.25  2003/02/06 12:58:25  lurcher
 *
 * Fix a speeling problem :-)
 *
 * Revision 1.24  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.23  2002/11/11 17:10:20  lurcher
 *
 * VMS changes
 *
 * Revision 1.22  2002/11/06 16:08:01  lurcher
 *
 * Update missing
 *
 * Revision 1.21  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.20  2002/08/20 12:41:07  lurcher
 *
 * Fix incorrect return state from SQLEndTran/SQLTransact
 *
 * Revision 1.19  2002/08/19 09:11:49  lurcher
 *
 * Fix Maxor ineffiecny in Postgres Drivers, and fix a return state
 *
 * Revision 1.18  2002/08/15 08:10:33  lurcher
 *
 * Couple of small fixes from John L Miller
 *
 * Revision 1.17  2002/08/12 16:20:44  lurcher
 *
 * Make it try and find a working iconv set of encodings
 *
 * Revision 1.16  2002/08/12 13:17:52  lurcher
 *
 * Replicate the way the MS DM handles loading of driver libs, and allocating
 * handles in the driver. usage counting in the driver means that dlopen is
 * only called for the first use, and dlclose for the last. AllocHandle for
 * the driver environment is only called for the first time per driver
 * per application environment.
 *
 * Revision 1.15  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.14  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.13  2002/07/10 15:05:57  lurcher
 *
 * Alter the return code in the Postgres driver, for a warning, it should be
 * 01000 it was 00000
 * Fix a problem in DriverManagerII with the postgres driver as the driver
 * doesn't return a propper list of schemas
 * Allow the delimiter to be set in isql to a hex/octal char not just a
 * printable one
 *
 * Revision 1.12  2002/07/08 16:37:35  lurcher
 *
 * Fix bug in unicode_to_ansi_copy
 *
 * Revision 1.11  2002/07/04 17:27:56  lurcher
 *
 * Small bug fixes
 *
 * Revision 1.9  2002/05/28 13:30:34  lurcher
 *
 * Tidy up for AIX
 *
 * Revision 1.8  2002/05/21 14:19:44  lurcher
 *
 * * Update libtool to escape from AIX build problem
 * * Add fix to avoid file handle limitations
 * * Add more UNICODE changes, it looks like it is native 16 representation
 *   the old way can be reproduced by defining UCS16BE
 * * Add iusql, its just the same as isql but uses the wide functions
 *
 * Revision 1.7  2002/04/10 11:04:36  lurcher
 *
 * Fix endian issue with 4 byte unicode support
 *
 * Revision 1.6  2002/02/27 11:27:14  lurcher
 *
 * Fix bug in error reporting
 *
 * Revision 1.5  2002/01/21 18:00:51  lurcher
 *
 * Assorted fixed and changes, mainly UNICODE/bug fixes
 *
 * Revision 1.4  2001/12/13 13:56:31  lurcher
 *
 * init a global for Peter
 *
 * Revision 1.3  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.2  2001/11/15 18:38:21  lurcher
 *
 * Make the errors returned from SQLError reset after each API call
 * if the app is expecting ODBC 3 operation
 *
 * Revision 1.1.1.1  2001/10/17 16:40:09  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.23  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.22  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.21  2001/07/02 17:09:37  nick
 *
 * Add some portability changes
 *
 * Revision 1.20  2001/06/20 17:25:32  pat
 * Correct msg1 length in 4 extract diag functions
 *
 * Revision 1.19  2001/06/20 08:19:25  nick
 *
 * Fix buffer overflow in error handling
 *
 * Revision 1.18  2001/04/23 13:58:43  nick
 *
 * Assorted tweeks to text driver to get it to work with StarOffice
 *
 * Revision 1.17  2001/04/20 16:57:25  nick
 *
 * Add extra mapping of data types
 *
 * Revision 1.16  2001/04/18 15:03:37  nick
 *
 * Fix problem when going to DB2 unicode driver
 *
 * Revision 1.15  2001/04/16 22:35:10  nick
 *
 * More tweeks to the AutoTest code
 *
 * Revision 1.14  2001/04/14 10:42:03  nick
 *
 * Extra work on the autotest feature of odbctest
 *
 * Revision 1.13  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.12  2001/04/03 16:34:12  nick
 *
 * Add support for strangly broken unicode drivers
 *
 * Revision 1.11  2001/01/09 23:15:18  nick
 *
 * More unicode error fixes
 *
 * Revision 1.10  2001/01/09 22:33:13  nick
 *
 * Stop passing NULL into SQLExtendedFetch
 * Further fixes to unicode to ansi conversions
 *
 * Revision 1.9  2001/01/09 11:03:32  nick
 *
 * Fixed overrun bug
 *
 * Revision 1.8  2001/01/06 15:00:12  nick
 *
 * Fix bug in SQLError introduced with UNICODE
 *
 * Revision 1.7  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.6  2000/10/25 12:45:51  nick
 *
 * Add mapping for both ODBC 2 - 3 and ODBC 3 - 2 error states
 *
 * Revision 1.5  2000/10/25 12:32:41  nick
 *
 * The mapping was the wrong way around for errors, ODBC3 error are mapped
 * to ODBC 2 not the other way around
 *
 * Revision 1.4  2000/10/25 09:13:26  nick
 *
 * Remove some invalid ODBC2-ODBC3 error mappings
 *
 * Revision 1.3  2000/10/13 15:18:49  nick
 *
 * Change string length parameter from SQLINTEGER to SQLSMALLINT
 *
 * Revision 1.2  2000/09/19 13:13:13  nick
 *
 * Add display of returned error text in log file
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.28  2000/07/31 08:46:11  ngorham
 *
 * Avoid potential buffer overrun
 *
 * Revision 1.27  2000/06/23 16:11:38  ngorham
 *
 * Map ODBC 2 SQLSTATE values to ODBC 3
 *
 * Revision 1.25  2000/06/21 11:07:36  ngorham
 *
 * Stop Errors from SQLAllocHandle being lost
 *
 * Revision 1.24  2000/06/20 12:44:01  ngorham
 *
 * Fix bug that caused a success with info message from SQLExecute or
 * SQLExecDirect to be lost if used with a ODBC 3 driver and the application
 * called SQLGetDiagRec
 *
 * Revision 1.23  2000/06/01 11:00:51  ngorham
 *
 * return errors from descriptor functions
 *
 * Revision 1.22  2001/05/31 23:24:20  ngorham
 *
 * Update timestamps
 *
 * Revision 1.21  2000/05/21 21:49:19  ngorham
 *
 * Assorted fixes
 *
 * Revision 1.20  2001/04/11 09:00:05  ngorham
 *
 * remove stray printf
 *
 * Revision 1.19  2001/04/01 00:06:50  ngorham
 *
 * Dont use stderr, if the log file fails to open.
 *
 * Revision 1.18  2000/03/14 07:45:35  ngorham
 *
 * Fix bug that discarded connection errors
 *
 * Revision 1.17  2000/01/18 17:24:50  ngorham
 *
 * Add missing [unixODBC] prefix in front of error messages.
 *
 * Revision 1.16  1999/12/14 19:02:25  ngorham
 *
 * Mask out the password fields in the logging
 *
 * Revision 1.15  1999/12/04 17:01:23  ngorham
 *
 * Remove C++ comments from the Postgres code
 *
 * Revision 1.14  1999/12/01 09:20:07  ngorham
 *
 * Fix some threading problems
 *
 * Revision 1.13  1999/11/17 21:08:58  ngorham
 *
 * Fix Bug with the ODBC 3 error handling
 *
 * Revision 1.12  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.11  1999/11/10 22:15:48  ngorham
 *
 * Fix some bugs with the DM and error reporting.
 *
 * Revision 1.10  1999/11/10 03:51:34  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.9  1999/10/24 23:54:19  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.8  1999/10/03 23:05:16  ngorham
 *
 * First public outing of the cursor lib
 *
 * Revision 1.7  1999/09/19 22:24:34  ngorham
 *
 * Added support for the cursor library
 *
 * Revision 1.6  1999/08/03 21:47:39  shandyb
 * Moving to automake: changed files in DriverManager
 *
 * Revision 1.5  1999/07/10 21:10:17  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.4  1999/07/05 19:54:05  ngorham
 *
 * Fix a problem where a long string could crash the DM
 *
 * Revision 1.3  1999/07/04 21:05:08  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/19 17:51:41  ngorham
 *
 * Applied assorted minor bug fixes
 *
 * Revision 1.1.1.1  1999/05/29 13:41:09  sShandyb
 * first go at it
 *
 * Revision 1.2  1999/06/03 22:20:25  ngorham
 *
 * Finished off the ODBC3-2 mapping
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 *
 *
 **********************************************************************/

#include <config.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined( HAVE_GETTIMEOFDAY ) && defined( HAVE_SYS_TIME_H )
#include <sys/time.h>
#elif defined( HAVE_FTIME ) && defined( HAVE_SYS_TIMEB_H )
#include <sys/timeb.h>
#elif defined( DHAVE_TIME ) && defined( HAVE_TIME_H ) 
#include <time.h>
#endif

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>

#elif defined(_WIN32) && !defined(__CYGWIN__)

#include <locale.h>
#include <string.h>

// Can't include <windows.h> here
unsigned int __stdcall GetACP (void);

typedef struct { char *win_enc; char *canonical_enc; } t_enc;

static t_enc lookuptable[] = {
  { "CP1361",  "JOHAB" },
  { "CP20127", "ASCII" },
  { "CP20866", "KOI8-R" },
  { "CP20936", "GB2312" },
  { "CP21866", "KOI8-RU" },
  { "CP28591", "ISO-8859-1" },
  { "CP28592", "ISO-8859-2" },
  { "CP28593", "ISO-8859-3" },
  { "CP28594", "ISO-8859-4" },
  { "CP28595", "ISO-8859-5" },
  { "CP28596", "ISO-8859-6" },
  { "CP28597", "ISO-8859-7" },
  { "CP28598", "ISO-8859-8" },
  { "CP28599", "ISO-8859-9" },
  { "CP28605", "ISO-8859-15" },
  { "CP38598", "ISO-8859-8" },
  { "CP51932", "EUC-JP" },
  { "CP51936", "GB2312" },
  { "CP51949", "EUC-KR" },
  { "CP51950", "EUC-TW" },
  { "CP54936", "GB18030" },
  { "CP65001", "UTF-8" },
  { "CP936",   "GBK" }
};

#endif

#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: __info.c,v $ $Revision: 1.50 $";

struct log_structure log_info = { NULL, NULL, 0, 0 };

SQLINTEGER ODBCSharedTraceFlag = 0;

/*
 * unicode setup functions, do them on a connection basis.
 */

int unicode_setup( DMHDBC connection )
{
#ifdef HAVE_ICONV
    char ascii[ 256 ], unicode[ 256 ];
    char *be_ucode[] = { "UCS-2-INTERNAL", "UCS-2BE", "UCS-2", "ucs2", NULL };
    char *le_ucode[] = { "UCS-2-INTERNAL", "UCS-2LE", NULL };
    char *asc[] = { "char", "char", "ISO8859-1", "ISO-8859-1", "8859-1", "iso8859_1", "ASCII", NULL };
    union { long l; char c[sizeof (long)]; } u;
    int be;

    if ( connection -> iconv_cd_uc_to_ascii != (iconv_t)(-1) &&
         connection -> iconv_cd_ascii_to_uc != (iconv_t)(-1))
    {
        return 1;
    }

    /*
     * is this a bigendian machine ?
     */

    u.l = 1; 
    be = (u.c[sizeof (long) - 1] == 1);

    mutex_iconv_entry();

#if defined( HAVE_NL_LANGINFO ) && defined(HAVE_LANGINFO_CODESET)
    /* 
     * Try with current locale settings first 
     */
    asc[ 0 ] = nl_langinfo(CODESET);
#elif defined(_WIN32) && !defined(__CYGWIN__)
    static char resultbuf[2 + 10 + 1];
    {
      // Heavily inspired by the localcharset module of gnulib (LGPL-2.1)
      char buf[2 + 10 + 1];

      char *current_locale = setlocale (LC_CTYPE, NULL);
      char *pdot = strrchr (current_locale, '.');

      if (pdot && 2 + strlen (pdot + 1) + 1 <= sizeof (buf))
        sprintf (buf, "CP%s", pdot + 1);
      else
        sprintf (buf, "CP%u", GetACP ());

      if (strcmp (buf + 2, "65001") == 0 || strcmp (buf + 2, "utf8") == 0)
        asc[ 0 ] = "UTF-8";
      else
        {
          strcpy (resultbuf, buf);
          asc[ 0 ] = resultbuf;
        }

      int i;
      for (i = 0; i < sizeof (lookuptable) / sizeof (t_enc); i++)
      {
        t_enc sym = lookuptable[i];
        if (strcmp(sym.win_enc, asc[ 0 ]) == 0)
        {
          strcpy (resultbuf, sym.canonical_enc);
          asc[ 0 ] = resultbuf;
          break;
        }
      }
    }
#endif

    /*
     * if required find a match
     */

    if ( strcmp( ASCII_ENCODING, "auto-search" ) == 0 && strcmp( connection -> unicode_string, "auto-search" ) == 0 )
    {
        /*
         * look for both
         */
        int i, j, found;
        iconv_t icvt;

        ascii[ 0 ] = '\0';
        unicode[ 0 ] = '\0';

        for ( i = found = 0; ( be ? be_ucode[ i ] : le_ucode[ i ] ) != NULL && !found; i ++ )
        {
            for ( j = 0; asc[ j ] && !found; j ++ )
            {
                if (( icvt = iconv_open( asc[ j ], be ? be_ucode[ i ] : le_ucode[ i ] )) != ((iconv_t) -1 ) )
                {
                    strcpy( ascii, asc[ j ] );
                    strcpy( unicode, be ? be_ucode[ i ] : le_ucode[ i ] );
                    iconv_close( icvt );
                    found = 1;
                }
            }
        }
    }
    else if ( strcmp( ASCII_ENCODING, "auto-search" ) == 0 )
    {
        /*
         * look for ascii
         */
        int j;
        iconv_t icvt;

        strcpy( unicode, connection -> unicode_string );

        for ( j = 0; asc[ j ]; j ++ )
        {
            if (( icvt = iconv_open( asc[ j ], unicode )) != ((iconv_t) -1 ) )
            {
                strcpy( ascii, asc[ j ] );
                iconv_close( icvt );
                break;
            }
        }
    }
    else if ( strcmp( connection -> unicode_string, "auto-search" ) == 0 )
    {
        /*
         * look for unicode
         */
        int i;
        iconv_t icvt;

        strcpy( ascii, ASCII_ENCODING );

        for ( i = 0; be ? be_ucode[ i ] : le_ucode[ i ]; i ++ )
        {
            if (( icvt = iconv_open( ascii, be ? be_ucode[ i ] : le_ucode[ i ] )) != ((iconv_t) -1 ) )
            {
                strcpy( unicode, be ? be_ucode[ i ] : le_ucode[ i ] );
                iconv_close( icvt );
                break;
            }
        }
    }
    else
    {
        strcpy( ascii, ASCII_ENCODING );
        strcpy( unicode, connection -> unicode_string );
    }

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, "\t\tUNICODE Using encoding ASCII '%s' and UNICODE '%s'",
                        ascii, unicode );

        dm_log_write_diag( connection -> msg );
    }

    connection -> iconv_cd_uc_to_ascii = iconv_open( ascii, unicode );
    connection -> iconv_cd_ascii_to_uc = iconv_open( unicode, ascii );

    mutex_iconv_exit();

    if ( connection -> iconv_cd_uc_to_ascii == (iconv_t)(-1) ||
            connection -> iconv_cd_ascii_to_uc ==  (iconv_t)(-1))
    {
        return 0;
    }
    else
    {
        return 1;
    }

#else
    return 1;
#endif
}

void unicode_shutdown( DMHDBC connection )
{
#ifdef HAVE_ICONV

    mutex_iconv_entry();

    if ( connection -> iconv_cd_ascii_to_uc != (iconv_t)(-1) )
    {
        iconv_close( connection -> iconv_cd_ascii_to_uc );
    }

    if ( connection -> iconv_cd_uc_to_ascii != (iconv_t)(-1))
    {
        iconv_close( connection -> iconv_cd_uc_to_ascii );
    }

    connection -> iconv_cd_uc_to_ascii = (iconv_t)(-1);
    connection -> iconv_cd_ascii_to_uc = (iconv_t)(-1);

    mutex_iconv_exit();
#endif
}

/*
 * returned a malloc'd buffer in unicode converted from the ansi buffer
 */

SQLWCHAR *ansi_to_unicode_alloc( SQLCHAR *str, SQLINTEGER len, DMHDBC connection, int *wlen )
{
    SQLWCHAR *ustr;

    if ( wlen ) 
    {
        *wlen = len;
    }

    if( !str )
    {
        return NULL;
    }

    if ( len == SQL_NTS )
    {
        len = strlen((char*) str );
    }
    else if ( len < 0 ) 
    {
        len = 0;
    }

    ustr = malloc( sizeof( SQLWCHAR ) * ( len + 1 ));
    if ( !ustr )
    {
        return NULL;
    }

    return ansi_to_unicode_copy( ustr, (char*) str, len, connection, wlen );
}

/*
 * return a ansi representation of a unicode buffer, according to
 * the chosen conversion method
 */

char *unicode_to_ansi_alloc( SQLWCHAR *str, SQLINTEGER len, DMHDBC connection, int *clen )
{
    char *aptr;

    if ( clen ) 
    {
        *clen = len;
    }

    if ( !str )
    {
        return NULL;
    }

    if ( len == SQL_NTS )
    {
        len = wide_strlen( str );
    }

    aptr = malloc(( len * 4 ) + 1 );       /* There may be UTF8 */
    if ( !aptr )
    {
        return NULL;
    }

    return unicode_to_ansi_copy( aptr, len * 4, str, len, connection, clen );
}

/*
 * copy from a unicode buffer to a ansi buffer using the chosen conversion
 */

char *unicode_to_ansi_copy( char * dest, int dest_len, SQLWCHAR *src, SQLINTEGER buffer_len, DMHDBC connection, int *clen )
{
    int i;

    if ( !src || !dest )
    {
        return NULL;
    }

    if ( buffer_len == SQL_NTS )
    {
        buffer_len = wide_strlen( src );
    }
#ifdef HAVE_ICONV

    mutex_iconv_entry();

    if ( connection && connection -> iconv_cd_uc_to_ascii != (iconv_t)(-1))
    {
        size_t ret;
        size_t inbl = buffer_len * sizeof( SQLWCHAR );
        size_t obl = dest_len;
        char *ipt = (char*)src;
        char *opt = (char*)dest;

        if (( ret = iconv( connection -> iconv_cd_uc_to_ascii,
                    (ICONV_CONST char**)&ipt, &inbl,
                    &opt, &obl )) != (size_t)(-1))
        {
   		mutex_iconv_exit();

            if ( clen ) 
            {
                *clen = opt - dest;
            }
	        /* Null terminate outside of iconv, so that the length returned does not include the null terminator. */
            if ( obl )
            {
	            *opt = '\0';
            }
            return dest;
        }
    }

   	mutex_iconv_exit();

#endif

    for ( i = 0; i < buffer_len && i < dest_len && src[ i ] != 0; i ++ )
    {
#ifdef SQL_WCHART_CONVERT
        dest[ i ] = (char)(src[ i ] & 0x000000ff);
#else
        dest[ i ] = src[ i ] & 0x00FF;
#endif
    }

    if ( clen ) 
    {
        *clen = i;
    }

    if (dest_len)
    {
        dest[ i < dest_len ? i : i-1 ] = '\0';
    }

    return dest;
}

/*
 * copy from a ansi buffer to a unicode buffer using the chosen conversion
 */

SQLWCHAR *ansi_to_unicode_copy( SQLWCHAR * dest, char *src, SQLINTEGER buffer_len, DMHDBC connection, int *wlen )
{
    int i;

    if ( !src || !dest )
    {
        return NULL;
    }

    if ( buffer_len == SQL_NTS )
    {
        buffer_len = strlen( src );
    }
    else if ( buffer_len < 0 ) 
    {
        buffer_len = 0;
    }

#ifdef HAVE_ICONV

    if ( connection && connection -> iconv_cd_ascii_to_uc != (iconv_t)(-1))
    {
        size_t inbl = buffer_len;
        size_t obl = buffer_len * sizeof( SQLWCHAR );
        char *ipt = (char*)src;
        char *opt = (char*)dest;

		mutex_iconv_entry();

        if ( iconv( connection -> iconv_cd_ascii_to_uc,
                    (ICONV_CONST char**)&ipt, &inbl,
                    &opt, &obl ) != (size_t)(-1))
        {
			mutex_iconv_exit();

            if ( wlen ) 
            {
                *wlen = ( opt - ((char*)dest)) / sizeof( SQLWCHAR );
            }
	        /* Null terminate outside of iconv, so that the length returned does not include the null terminator. */
	        dest[( opt - ((char*)dest)) / sizeof( SQLWCHAR )] = 0;
            return dest;
        }

		mutex_iconv_exit();
    }

#endif

    for ( i = 0; i < buffer_len && src[ i ] != 0; i ++ )
    {
#ifdef SQL_WCHART_CONVERT
        dest[ i ] = src[ i ] & 0x000000ff;
#else
        dest[ i ] = src[ i ] & 0x00FF;
#endif
    }

    if ( wlen ) 
    {
        *wlen = i;
    }

    dest[ i ] = 0;


    return dest;
}


/*
 * display a SQLGetTypeInfo type as a astring
 */

char * __type_as_string( SQLCHAR *s, SQLSMALLINT type )
{
    switch( type )
    {
      case SQL_DOUBLE:
        sprintf((char*) s, "SQL_DOUBLE" );
        break;

      case SQL_FLOAT:
        sprintf((char*) s, "SQL_FLOAT" );
        break;

      case SQL_REAL:
        sprintf((char*) s, "SQL_REAL" );
        break;

      case SQL_BIT:
        sprintf((char*) s, "SQL_BIT" );
        break;

      case SQL_CHAR:
        sprintf((char*) s, "SQL_CHAR" );
        break;

      case SQL_VARCHAR:
        sprintf((char*) s, "SQL_VARCHAR" );
        break;

      case SQL_LONGVARCHAR:
        sprintf((char*) s, "SQL_LONGVARCHAR" );
        break;

      case SQL_BINARY:
        sprintf((char*) s, "SQL_BINARY" );
        break;

      case SQL_VARBINARY:
        sprintf((char*) s, "SQL_VARBINARY" );
        break;

      case SQL_LONGVARBINARY:
        sprintf((char*) s, "SQL_LONGVARBINARY" );
        break;

      case SQL_DECIMAL:
        sprintf((char*) s, "SQL_DECIMAL" );
        break;

      case SQL_NUMERIC:
        sprintf((char*) s, "SQL_NUMERIC" );
        break;

      case SQL_BIGINT:
        sprintf((char*) s, "SQL_BIGINT" );
        break;

      case SQL_INTEGER:
        sprintf((char*) s, "SQL_INTEGER" );
        break;

      case SQL_SMALLINT:
        sprintf((char*) s, "SQL_SMALLINT" );
        break;

      case SQL_TINYINT:
        sprintf((char*) s, "SQL_TINYINT" );
        break;

      case SQL_TYPE_DATE:
        sprintf((char*) s, "SQL_TYPE_DATE" );
        break;

      case SQL_TYPE_TIME:
        sprintf((char*) s, "SQL_TYPE_TIME" );
        break;

      case SQL_TYPE_TIMESTAMP:
        sprintf((char*) s, "SQL_TYPE_TIMESTAMP" );
        break;

      case SQL_DATE:
        sprintf((char*) s, "SQL_DATE" );
        break;

      case SQL_TIME:
        sprintf((char*) s, "SQL_TIME" );
        break;

      case SQL_TIMESTAMP:
        sprintf((char*) s, "SQL_TIMESTAMP" );
        break;

      case SQL_INTERVAL_YEAR:
        sprintf((char*) s, "SQL_INTERVAL_YEAR" );
        break;

      case SQL_INTERVAL_YEAR_TO_MONTH:
        sprintf((char*) s, "SQL_INTERVAL_YEAR_TO_MONTH" );
        break;

      case SQL_INTERVAL_MONTH:
        sprintf((char*) s, "SQL_INTERVAL_MONTH" );
        break;

      case SQL_INTERVAL_DAY_TO_SECOND:
        sprintf((char*) s, "SQL_INTERVAL_DAY_TO_SECOND" );
        break;

      case SQL_INTERVAL_DAY_TO_MINUTE:
        sprintf((char*) s, "SQL_INTERVAL_DAY_TO_MINUTE" );
        break;

      case SQL_INTERVAL_DAY:
        sprintf((char*) s, "SQL_INTERVAL_DAY" );
        break;

      case SQL_INTERVAL_HOUR_TO_SECOND:
        sprintf((char*) s, "SQL_INTERVAL_HOUR_TO_SECOND" );
        break;

      case SQL_INTERVAL_HOUR_TO_MINUTE:
        sprintf((char*) s, "SQL_INTERVAL_HOUR_TO_MINUTE" );
        break;

      case SQL_INTERVAL_HOUR:
        sprintf((char*) s, "SQL_INTERVAL_HOUR" );
        break;

      case SQL_INTERVAL_MINUTE_TO_SECOND:
        sprintf((char*) s, "SQL_INTERVAL_MINUTE_TO_SECOND" );
        break;

      case SQL_INTERVAL_MINUTE:
        sprintf((char*) s, "SQL_INTERVAL_MINUTE" );
        break;

      case SQL_INTERVAL_SECOND:
        sprintf((char*) s, "SQL_INTERVAL_SECOND" );
        break;

      case SQL_ALL_TYPES:
        sprintf((char*) s, "SQL_ALL_TYPES" );
        break;

      default:
        sprintf((char*) s, "Unknown(%d)", (int)type );
        break;
    }

    return (char*) s;
}

/*
 * display a data field as a string
 */

char * __sdata_as_string( SQLCHAR *s, SQLINTEGER type, 
        SQLSMALLINT *ptr, SQLPOINTER buf )
{
    SQLLEN iptr;

    if ( ptr )
    {
        iptr = *ptr;
        return __data_as_string( s, type, &iptr, buf );
    }
    else
    {
        return __data_as_string( s, type, NULL, buf );
    }

    return (char*) s;
}

char * __idata_as_string( SQLCHAR *s, SQLINTEGER type, 
        SQLINTEGER *ptr, SQLPOINTER buf )
{
    SQLLEN iptr;

    if ( ptr )
    {
        iptr = *ptr;
        return __data_as_string( s, type, &iptr, buf );
    }
    else
    {
        return __data_as_string( s, type, NULL, buf );
    }

    return (char*) s;
}

char * __data_as_string( SQLCHAR *s, SQLINTEGER type, 
        SQLLEN *ptr, SQLPOINTER buf )
{
    if ( ptr && *ptr == SQL_NULL_DATA )
    {
        sprintf((char*) s, "SQL_NULL_DATA" );
    }
    else if ( ptr && *ptr < 0 )
    {
        sprintf((char*) s, "Indicator = %d", (int)*ptr );
    } 
    else if ( !buf )
    {
        sprintf((char*) s, "[NULLPTR]" );
    }
    else
    {
        switch ( type )
        {
          case SQL_INTEGER:
            {
                SQLINTEGER val;

                memcpy( &val, buf, sizeof( SQLINTEGER ));
                sprintf((char*) s, "[%d]", (int)val );
            }
            break;

          case SQL_CHAR:
          case SQL_VARCHAR:
            sprintf((char*) s, "[%.*s]", LOG_MESSAGE_LEN, (char*)buf );
            break;

          case SQL_WCHAR:
          case SQL_WVARCHAR:
            {
                int len = LOG_MESSAGE_LEN;
                signed short *ptr = (signed short*)buf;
                char *optr;

                optr = (char*) s;
                sprintf((char*)  s, "[" );

                optr ++;

                while( len > 0 )
                {
                    if ( *ptr == 0x0000 )
                        break;
                    sprintf( optr, "%c", *ptr & 0x00FF );
                    optr ++;
                    len --;
                    ptr ++;
                }
                sprintf( optr, "](unicode)" );
            }
            break;

          case SQL_DOUBLE:
            {
                double val;

                memcpy( &val, buf, sizeof( double ));
                sprintf((char*) s, "[%g]", val );
            }
            break;

          case SQL_FLOAT:
          case SQL_REAL:
            {
                float val;

                memcpy( &val, buf, sizeof( float ));
                sprintf((char*) s, "[%g]", val );
            }
            break;

          case SQL_BIT:
            {
                SQLCHAR val;

                memcpy( &val, buf, sizeof( SQLCHAR ));
                sprintf((char*) s, "[%d]", (int)val );
            }
            break;

          case SQL_LONGVARCHAR:
            sprintf((char*) s, "[LONGVARCHARDATA...]" );
            break;

          case SQL_BINARY:
            sprintf((char*) s, "[BINARYDATA...]" );
            break;

          case SQL_VARBINARY:
            sprintf((char*) s, "[VARBINARYDATA...]" );
            break;

          case SQL_LONGVARBINARY:
            sprintf((char*) s, "[LONGVARBINARYDATA...]" );
            break;

          case SQL_DECIMAL:
            sprintf((char*) s, "[DECIMAL...]" );
            break;

          case SQL_NUMERIC:
            sprintf((char*) s, "[NUMERIC...]" );
            break;

          case SQL_BIGINT:
            sprintf((char*) s, "[BIGINT...]" );
            break;

          case SQL_SMALLINT:
            {
                short val;

                memcpy( &val, buf, sizeof( short ));
                sprintf((char*) s, "[%d]", (int)val );
            }
            break;

          case SQL_TINYINT:
            {
                char val;

                memcpy( &val, buf, sizeof( char ));
                sprintf((char*) s, "[%d]", (int)val );
            }
            break;

          case SQL_TYPE_DATE:
          case SQL_DATE:
            sprintf((char*) s, "[DATE...]" );
            break;

          case SQL_TYPE_TIME:
          case SQL_TIME:
            sprintf((char*) s, "[TIME...]" );
            break;

          case SQL_TYPE_TIMESTAMP:
          case SQL_TIMESTAMP:
            sprintf((char*) s, "[TIMESTAMP...]" );
            break;

          case SQL_INTERVAL_YEAR:
          case SQL_INTERVAL_YEAR_TO_MONTH:
          case SQL_INTERVAL_MONTH:
          case SQL_INTERVAL_DAY_TO_SECOND:
          case SQL_INTERVAL_DAY_TO_MINUTE:
          case SQL_INTERVAL_DAY:
          case SQL_INTERVAL_HOUR_TO_SECOND:
          case SQL_INTERVAL_HOUR_TO_MINUTE:
          case SQL_INTERVAL_HOUR:
          case SQL_INTERVAL_MINUTE_TO_SECOND:
          case SQL_INTERVAL_MINUTE:
          case SQL_INTERVAL_SECOND:
            sprintf((char*) s, "[INTERVAL...]" );
            break;

          default:
            sprintf((char*) s, "[Data...]" );
            break;
        }
    }

    return (char*) s;
}

/*
 * display a pointer to a int
 */

char * __iptr_as_string( SQLCHAR *s, SQLINTEGER *ptr )
{
    if ( ptr )
    {
        sprintf((char*) s, "%p -> %ld (%d bits)", (void*)ptr, (long)*ptr, (int)sizeof( SQLINTEGER ) * 8 );
    }
    else
    {
        sprintf((char*) s, "NULLPTR" );
    }

    return (char*) s;
}

char * __ptr_as_string( SQLCHAR *s, SQLLEN *ptr )
{
    if ( ptr )
    {
        sprintf((char*) s, "%p -> %ld (%d bits)", (void*)ptr, (long)*ptr, (int)sizeof( SQLLEN ) * 8 );
    }
    else
    {
        sprintf((char*) s, "NULLPTR" );
    }

    return (char*) s;
}

/*
 * display a pointer to a int
 */

char * __sptr_as_string( SQLCHAR *s, SQLSMALLINT *ptr )
{
    if ( ptr )
    {
        sprintf((char*) s, "%p -> %d", (void*)ptr, (int)*ptr );
    }
    else
    {
        sprintf((char*) s, "NULLPTR" );
    }

    return (char*) s;
}

/*
 * convert a function id to a string
 */

char * __fid_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_API_SQLALLOCCONNECT:
        sprintf((char*) s, "SQLAllocConnect" );
        break;

     case SQL_API_SQLALLOCENV:
        sprintf((char*) s, "SQLAllocEnv" );
        break;

      case SQL_API_SQLALLOCHANDLE:
        sprintf((char*) s, "SQLAllocHandle" );
        break;

      case SQL_API_SQLALLOCSTMT:
        sprintf((char*) s, "SQLAllocStmt" );
        break;

      case SQL_API_SQLALLOCHANDLESTD:
        sprintf((char*) s, "SQLAllochandleStd" );
        break;

      case SQL_API_SQLBINDCOL:
        sprintf((char*) s, "SQLBindCol" );
        break;

      case SQL_API_SQLBINDPARAM:
        sprintf((char*) s, "SQLBindParam" );
        break;

      case SQL_API_SQLBINDPARAMETER:
        sprintf((char*) s, "SQLBindParameter" );
        break;

      case SQL_API_SQLBROWSECONNECT:
        sprintf((char*) s, "SQLBrowseConnect" );
        break;

      case SQL_API_SQLBULKOPERATIONS:
        sprintf((char*) s, "SQLBulkOperations" );
        break;

      case SQL_API_SQLCANCEL:
        sprintf((char*) s, "SQLCancel" );
        break;

      case SQL_API_SQLCLOSECURSOR:
        sprintf((char*) s, "SQLCloseCursor" );
        break;

      case SQL_API_SQLCOLATTRIBUTES:
        sprintf((char*) s, "SQLColAttribute(s)" );
        break;

      case SQL_API_SQLCOLUMNPRIVILEGES:
        sprintf((char*) s, "SQLColumnPrivileges" );
        break;

      case SQL_API_SQLCOLUMNS:
        sprintf((char*) s, "SQLColumns" );
        break;

      case SQL_API_SQLCONNECT:
        sprintf((char*) s, "SQLConnect" );
        break;

      case SQL_API_SQLCOPYDESC:
        sprintf((char*) s, "SQLCopyDesc" );
        break;

      case SQL_API_SQLDATASOURCES:
        sprintf((char*) s, "SQLDataSources" );
        break;

      case SQL_API_SQLDESCRIBECOL:
        sprintf((char*) s, "SQLDescribeCol" );
        break;

      case SQL_API_SQLDESCRIBEPARAM:
        sprintf((char*) s, "SQLDescribeParam" );
        break;

      case SQL_API_SQLDISCONNECT:
        sprintf((char*) s, "SQLDisconnect" );
        break;

      case SQL_API_SQLDRIVERCONNECT:
        sprintf((char*) s, "SQLDriverConnect" );
        break;

      case SQL_API_SQLDRIVERS:
        sprintf((char*) s, "SQLDrivers" );
        break;

      case SQL_API_SQLENDTRAN:
        sprintf((char*) s, "SQLEndTran" );
        break;

      case SQL_API_SQLERROR:
        sprintf((char*) s, "SQLError" );
        break;

      case SQL_API_SQLEXECDIRECT:
        sprintf((char*) s, "SQLExecDirect" );
        break;

      case SQL_API_SQLEXECUTE:
        sprintf((char*) s, "SQLExecute" );
        break;

      case SQL_API_SQLEXTENDEDFETCH:
        sprintf((char*) s, "SQLExtendedFetch" );
        break;

      case SQL_API_SQLFETCH:
        sprintf((char*) s, "SQLFetch" );
        break;

      case SQL_API_SQLFETCHSCROLL:
        sprintf((char*) s, "SQLFetchScroll" );
        break;

      case SQL_API_SQLFOREIGNKEYS:
        sprintf((char*) s, "SQLForeignKeys" );
        break;

      case SQL_API_SQLFREEENV:
        sprintf((char*) s, "SQLFreeEnv" );
        break;

      case SQL_API_SQLFREEHANDLE:
        sprintf((char*) s, "SQLFreeHandle" );
        break;

      case SQL_API_SQLFREESTMT:
        sprintf((char*) s, "SQLFreeStmt" );
        break;

      case SQL_API_SQLFREECONNECT:
        sprintf((char*) s, "SQLFreeConnect" );
        break;

       case SQL_API_SQLGETCONNECTATTR:
        sprintf((char*) s, "SQLGetConnectAttr" );
        break;

      case SQL_API_SQLGETCONNECTOPTION:
        sprintf((char*) s, "SQLGetConnectOption" );
        break;

      case SQL_API_SQLGETCURSORNAME:
        sprintf((char*) s, "SQLGetCursorName" );
        break;

      case SQL_API_SQLGETDATA:
        sprintf((char*) s, "SQLGetData" );
        break;

      case SQL_API_SQLGETDESCFIELD:
        sprintf((char*) s, "SQLGetDescField" );
        break;

      case SQL_API_SQLGETDESCREC:
        sprintf((char*) s, "SQLGetDescRec" );
        break;

      case SQL_API_SQLGETDIAGFIELD:
        sprintf((char*) s, "SQLGetDiagField" );
        break;

      case SQL_API_SQLGETENVATTR:
        sprintf((char*) s, "SQLGetEnvAttr" );
        break;

      case SQL_API_SQLGETFUNCTIONS:
        sprintf((char*) s, "SQLGetFunctions" );
        break;

      case SQL_API_SQLGETINFO:
        sprintf((char*) s, "SQLGetInfo" );
        break;

      case SQL_API_SQLGETSTMTATTR:
        sprintf((char*) s, "SQLGetStmtAttr" );
        break;

      case SQL_API_SQLGETSTMTOPTION:
        sprintf((char*) s, "SQLGetStmtOption" );
        break;

      case SQL_API_SQLGETTYPEINFO:
        sprintf((char*) s, "SQLGetTypeInfo" );
        break;

      case SQL_API_SQLMORERESULTS:
        sprintf((char*) s, "SQLMoreResults" );
        break;

      case SQL_API_SQLNATIVESQL:
        sprintf((char*) s, "SQLNativeSql" );
        break;

      case SQL_API_SQLNUMPARAMS:
        sprintf((char*) s, "SQLNumParams" );
        break;

      case SQL_API_SQLNUMRESULTCOLS:
        sprintf((char*) s, "SQLNumResultCols" );
        break;

      case SQL_API_SQLPARAMDATA:
        sprintf((char*) s, "SQLParamData" );
        break;

      case SQL_API_SQLPARAMOPTIONS:
        sprintf((char*) s, "SQLParamOptions" );
        break;

      case SQL_API_SQLPREPARE:
        sprintf((char*) s, "SQLPrepare" );
        break;

      case SQL_API_SQLPRIMARYKEYS:
        sprintf((char*) s, "SQLPrimaryKeys" );
        break;

      case SQL_API_SQLPROCEDURECOLUMNS:
        sprintf((char*) s, "SQLProcedureColumns" );
        break;

      case SQL_API_SQLPROCEDURES:
        sprintf((char*) s, "SQLProcedures" );
        break;

      case SQL_API_SQLPUTDATA:
        sprintf((char*) s, "SQLPutData" );
        break;

      case SQL_API_SQLROWCOUNT:
        sprintf((char*) s, "SQLRowCount" );
        break;

      case SQL_API_SQLSETCONNECTATTR:
        sprintf((char*) s, "SQLSetConnectAttr" );
        break;

      case SQL_API_SQLSETCONNECTOPTION:
        sprintf((char*) s, "SQLSetConnectOption" );
        break;

      case SQL_API_SQLSETCURSORNAME:
        sprintf((char*) s, "SQLSetCursorName" );
        break;

      case SQL_API_SQLSETDESCFIELD:
        sprintf((char*) s, "SQLSetDescField" );
        break;

      case SQL_API_SQLSETDESCREC:
        sprintf((char*) s, "SQLSetDescRec" );
        break;

      case SQL_API_SQLSETENVATTR:
        sprintf((char*) s, "SQLSetEnvAttr" );
        break;

      case SQL_API_SQLSETPARAM:
        sprintf((char*) s, "SQLSetParam" );
        break;

      case SQL_API_SQLSETPOS:
        sprintf((char*) s, "SQLSetPos" );
        break;

      case SQL_API_SQLSETSCROLLOPTIONS:
        sprintf((char*) s, "SQLSetScrollOptions" );
        break;

      case SQL_API_SQLSETSTMTATTR:
        sprintf((char*) s, "SQLSetStmtAttr" );
        break;

      case SQL_API_SQLSETSTMTOPTION:
        sprintf((char*) s, "SQLSetStmtOption" );
        break;

      case SQL_API_SQLSPECIALCOLUMNS:
        sprintf((char*) s, "SQLSpecialColumns" );
        break;

      case SQL_API_SQLSTATISTICS:
        sprintf((char*) s, "SQLStatistics" );
        break;

      case SQL_API_SQLTABLEPRIVILEGES:
        sprintf((char*) s, "SQLTablePrivileges" );
        break;

      case SQL_API_SQLTABLES:
        sprintf((char*) s, "SQLTables" );
        break;

      case SQL_API_SQLTRANSACT:
        sprintf((char*) s, "SQLTransact" );
        break;

      case SQL_API_SQLGETDIAGREC:
        sprintf((char*) s, "SQLGetDiagRec" );
        break;

      default:
        sprintf((char*) s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * convert a column attribute to a string
 */

char * __col_attr_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_DESC_AUTO_UNIQUE_VALUE:
        sprintf((char*) s, "SQL_DESC_AUTO_UNIQUE_VALUE" );
        break;

      case SQL_DESC_BASE_COLUMN_NAME:
        sprintf((char*) s, "SQL_DESC_BASE_COLUMN_NAME" );
        break;

      case SQL_DESC_BASE_TABLE_NAME:
        sprintf((char*) s, "SQL_DESC_BASE_TABLE_NAME" );
        break;

      case SQL_DESC_CASE_SENSITIVE:
        sprintf((char*) s, "SQL_DESC_CASE_SENSITIVE" );
        break;

      case SQL_DESC_CATALOG_NAME:
        sprintf((char*)  s, "SQL_DESC_CATALOG_NAME" );
        break;

      case SQL_DESC_CONCISE_TYPE:
        sprintf((char*)  s, "SQL_DESC_CONCISE_TYPE" );
        break;

      case SQL_DESC_DISPLAY_SIZE:
        sprintf((char*)  s, "SQL_DESC_DISPLAY_SIZE" );
        break;

      case SQL_DESC_FIXED_PREC_SCALE:
        sprintf((char*)  s, "SQL_DESC_FIXED_PREC_SCALE" );
        break;

      case SQL_DESC_LABEL:
        sprintf((char*) s, "SQL_DESC_LABEL" );
        break;

      case SQL_COLUMN_NAME:
        sprintf((char*) s, "SQL_COLUMN_NAME" );
        break;

      case SQL_DESC_LENGTH:
        sprintf((char*) s, "SQL_DESC_LENGTH" );
        break;

      case SQL_COLUMN_LENGTH:
        sprintf((char*) s, "SQL_COLUMN_LENGTH" );
        break;

      case SQL_DESC_LITERAL_PREFIX:
        sprintf((char*) s, "SQL_DESC_LITERAL_PREFIX" );
        break;

      case SQL_DESC_LITERAL_SUFFIX:
        sprintf((char*) s, "SQL_DESC_LITERAL_SUFFIX" );
        break;

      case SQL_DESC_LOCAL_TYPE_NAME:
        sprintf((char*) s, "SQL_DESC_LOCAL_TYPE_NAME" );
        break;

      case SQL_DESC_NAME:
        sprintf((char*) s, "SQL_DESC_NAME" );
        break;

      case SQL_DESC_NULLABLE:
        sprintf((char*) s, "SQL_DESC_NULLABLE" );
        break;

      case SQL_COLUMN_NULLABLE:
        sprintf((char*) s, "SQL_COLUMN_NULLABLE" );
        break;

      case SQL_DESC_NUM_PREC_RADIX:
        sprintf((char*) s, "SQL_DESC_NUM_PREC_RADIX" );
        break;

      case SQL_DESC_OCTET_LENGTH:
        sprintf((char*) s, "SQL_DESC_OCTET_LENGTH" );
        break;

      case SQL_DESC_PRECISION:
        sprintf((char*) s, "SQL_DESC_PRECISION" );
        break;

      case SQL_COLUMN_PRECISION:
        sprintf((char*) s, "SQL_COLUMN_PRECISION" );
        break;

      case SQL_DESC_SCALE:
        sprintf((char*) s, "SQL_DESC_SCALE" );
        break;

      case SQL_COLUMN_SCALE:
        sprintf((char*) s, "SQL_COLUMN_SCALE" );
        break;

      case SQL_DESC_SCHEMA_NAME:
        sprintf((char*) s, "SQL_DESC_SCHEMA_NAME" );
        break;

      case SQL_DESC_SEARCHABLE:
        sprintf((char*) s, "SQL_DESC_SEARCHABLE" );
        break;

      case SQL_DESC_TABLE_NAME:
        sprintf((char*) s, "SQL_DESC_TABLE_NAME" );
        break;

      case SQL_DESC_TYPE:
        sprintf((char*) s, "SQL_DESC_TYPE" );
        break;

      case SQL_DESC_TYPE_NAME:
        sprintf((char*) s, "SQL_DESC_TYPE_NAME" );
        break;

      case SQL_DESC_UNNAMED:
        sprintf((char*) s, "SQL_DESC_UNNAMED" );
        break;

      case SQL_DESC_UNSIGNED:
        sprintf((char*) s, "SQL_DESC_UNSIGNED" );
        break;

      case SQL_DESC_UPDATABLE:
        sprintf((char*) s, "SQL_DESC_UPDATABLE" );
        break;

      default:
        sprintf((char*) s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * convert a connect attribute to a string
 */

char * __env_attr_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_ATTR_CONNECTION_POOLING:
        sprintf((char*) s, "SQL_ATTR_CONNECTION_POOLING" );
        break;

      case SQL_ATTR_CP_MATCH:
        sprintf((char*) s, "SQL_ATTR_CP_MATCH" );
        break;

      case SQL_ATTR_ODBC_VERSION:
        sprintf((char*) s, "SQL_ATTR_ODBC_VERSION" );
        break;

      case SQL_ATTR_OUTPUT_NTS:
        sprintf((char*) s, "SQL_ATTR_OUTPUT_NTS" );
        break;

      default:
        sprintf((char*) s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * convert a connect attribute to a string
 */

char * __con_attr_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_ATTR_ACCESS_MODE:
        sprintf((char*) s, "SQL_ATTR_ACCESS_MODE" );
        break;

      case SQL_ATTR_ASYNC_ENABLE:
        sprintf((char*) s, "SQL_ATTR_ASYNC_ENABLE" );
        break;

      case SQL_ATTR_AUTO_IPD:
        sprintf((char*) s, "SQL_ATTR_AUTO_IPD" );
        break;

      case SQL_ATTR_AUTOCOMMIT:
        sprintf((char*) s, "SQL_ATTR_AUTOCOMMIT" );
        break;

      case SQL_ATTR_CONNECTION_TIMEOUT:
        sprintf((char*) s, "SQL_ATTR_CONNECTION_TIMEOUT" );
        break;

      case SQL_ATTR_CURRENT_CATALOG:
        sprintf((char*) s, "SQL_ATTR_CURRENT_CATALOG" );
        break;

      case SQL_ATTR_LOGIN_TIMEOUT:
        sprintf((char*) s, "SQL_ATTR_LOGIN_TIMEOUT" );
        break;

      case SQL_ATTR_METADATA_ID:
        sprintf((char*) s, "SQL_ATTR_METADATA_ID" );
        break;

      case SQL_ATTR_ODBC_CURSORS:
        sprintf((char*) s, "SQL_ATTR_ODBC_CURSORS" );
        break;

      case SQL_ATTR_PACKET_SIZE:
        sprintf((char*) s, "SQL_ATTR_PACKET_SIZE" );
        break;

      case SQL_ATTR_QUIET_MODE:
        sprintf((char*) s, "SQL_ATTR_QUIET_MODE" );
        break;

      case SQL_ATTR_TRACE:
        sprintf((char*) s, "SQL_ATTR_TRACE" );
        break;

      case SQL_ATTR_TRACEFILE:
        sprintf((char*) s, "SQL_ATTR_TRACEFILE" );
        break;

      case SQL_ATTR_TRANSLATE_LIB:
        sprintf((char*) s, "SQL_ATTR_TRANSLATE_LIB" );
        break;

      case SQL_ATTR_TRANSLATE_OPTION:
        sprintf((char*) s, "SQL_ATTR_TRANSLATE_OPTION" );
        break;

      case SQL_ATTR_TXN_ISOLATION:
        sprintf((char*) s, "SQL_ATTR_TXN_ISOLATION" );
        break;

      default:
        sprintf((char*)  s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * convert a diagnostic attribute to a string
 */

char * __diag_attr_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_DIAG_CURSOR_ROW_COUNT:
        sprintf((char*) s, "SQL_DIAG_CURSOR_ROW_COUNT" );
        break;

      case SQL_DIAG_DYNAMIC_FUNCTION:
        sprintf((char*) s, "SQL_DIAG_DYNAMIC_FUNCTION" );
        break;

      case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
        sprintf((char*) s, "SQL_DIAG_DYNAMIC_FUNCTION_CODE" );
        break;

      case SQL_DIAG_NUMBER:
        sprintf((char*) s, "SQL_DIAG_NUMBER" );
        break;

      case SQL_DIAG_RETURNCODE:
        sprintf((char*) s, "SQL_DIAG_RETURNCODE" );
        break;

      case SQL_DIAG_ROW_COUNT:
        sprintf((char*) s, "SQL_DIAG_ROW_COUNT" );
        break;

      case SQL_DIAG_CLASS_ORIGIN:
        sprintf((char*) s, "SQL_DIAG_CLASS_ORIGIN" );
        break;

      case SQL_DIAG_COLUMN_NUMBER:
        sprintf((char*) s, "SQL_DIAG_COLUMN_NUMBER" );
        break;

      case SQL_DIAG_CONNECTION_NAME:
        sprintf((char*) s, "SQL_DIAG_CONNECTION_NAME" );
        break;

      case SQL_DIAG_MESSAGE_TEXT:
        sprintf((char*) s, "SQL_DIAG_MESSAGE_TEXT" );
        break;

      case SQL_DIAG_NATIVE:
        sprintf((char*) s, "SQL_DIAG_NATIVE" );
        break;

      case SQL_DIAG_ROW_NUMBER:
        sprintf((char*) s, "SQL_DIAG_ROW_NUMBER" );
        break;

      case SQL_DIAG_SERVER_NAME:
        sprintf((char*) s, "SQL_DIAG_SERVER_NAME" );
        break;

      case SQL_DIAG_SQLSTATE:
        sprintf((char*) s, "SQL_DIAG_SQLSTATE" );
        break;

      case SQL_DIAG_SUBCLASS_ORIGIN:
        sprintf((char*) s, "SQL_DIAG_SUBCLASS_ORIGIN" );
        break;

      default:
        sprintf((char*)  s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * convert a descriptor attribute to a string
 */

char * __desc_attr_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_DESC_ALLOC_TYPE:
        sprintf((char*)  s, "SQL_DESC_ALLOC_TYPE" );
        break;

      case SQL_DESC_ARRAY_SIZE:
        sprintf((char*)  s, "SQL_DESC_ARRAY_SIZE" );
        break;

      case SQL_DESC_ARRAY_STATUS_PTR:
        sprintf((char*)  s, "SQL_DESC_ARRAY_STATUS_PTR" );
        break;

      case SQL_DESC_BIND_OFFSET_PTR:
        sprintf((char*)  s, "SQL_DESC_BIND_OFFSET_PTR" );
        break;

      case SQL_DESC_BIND_TYPE:
        sprintf((char*)  s, "SQL_DESC_BIND_TYPE" );
        break;

      case SQL_DESC_COUNT:
        sprintf((char*)  s, "SQL_DESC_COUNT" );
        break;

      case SQL_DESC_ROWS_PROCESSED_PTR:
        sprintf((char*)  s, "SQL_DESC_ROWS_PROCESSED_PTR" );
        break;

      case SQL_DESC_AUTO_UNIQUE_VALUE:
        sprintf((char*)  s, "SQL_DESC_AUTO_UNIQUE_VALUE" );
        break;

      case SQL_DESC_BASE_COLUMN_NAME:
        sprintf((char*)  s, "SQL_DESC_BASE_COLUMN_NAME" );
        break;

      case SQL_DESC_BASE_TABLE_NAME:
        sprintf((char*)  s, "SQL_DESC_BASE_TABLE_NAME" );
        break;

      case SQL_DESC_CASE_SENSITIVE:
        sprintf((char*)  s, "SQL_DESC_CASE_SENSITIVE" );
        break;

      case SQL_DESC_CATALOG_NAME:
        sprintf((char*)  s, "SQL_DESC_CATALOG_NAME" );
        break;

      case SQL_DESC_CONCISE_TYPE:
        sprintf((char*)  s, "SQL_DESC_CONCISE_TYPE" );
        break;

      case SQL_DESC_DATA_PTR:
        sprintf((char*)  s, "SQL_DESC_DATA_PTR" );
        break;

      case SQL_DESC_DATETIME_INTERVAL_CODE:
        sprintf((char*)  s, "SQL_DESC_DATETIME_INTERVAL_CODE" );
        break;

      case SQL_DESC_DATETIME_INTERVAL_PRECISION:
        sprintf((char*)  s, "SQL_DESC_DATETIME_INTERVAL_PRECISION" );
        break;

      case SQL_DESC_DISPLAY_SIZE:
        sprintf((char*)  s, "SQL_DESC_DISPLAY_SIZE" );
        break;

      case SQL_DESC_FIXED_PREC_SCALE:
        sprintf((char*)  s, "SQL_DESC_FIXED_PREC_SCALE" );
        break;

      case SQL_DESC_INDICATOR_PTR:
        sprintf((char*)  s, "SQL_DESC_INDICATOR_PTR" );
        break;

      case SQL_DESC_LABEL:
        sprintf((char*)  s, "SQL_DESC_LABEL" );
        break;

      case SQL_DESC_LENGTH:
        sprintf((char*)  s, "SQL_DESC_LENGTH" );
        break;

      case SQL_DESC_LITERAL_PREFIX:
        sprintf((char*)  s, "SQL_DESC_LITERAL_PREFIX" );
        break;

      case SQL_DESC_LITERAL_SUFFIX:
        sprintf((char*)  s, "SQL_DESC_LITERAL_SUFFIX" );
        break;

      case SQL_DESC_LOCAL_TYPE_NAME:
        sprintf((char*)  s, "SQL_DESC_LOCAL_TYPE_NAME" );
        break;

      case SQL_DESC_NAME:
        sprintf((char*)  s, "SQL_DESC_NAME" );
        break;

      case SQL_DESC_NULLABLE:
        sprintf((char*)  s, "SQL_DESC_NULLABLE" );
        break;

      case SQL_DESC_NUM_PREC_RADIX:
        sprintf((char*)  s, "SQL_DESC_NUM_PREC_RADIX" );
        break;

      case SQL_DESC_OCTET_LENGTH:
        sprintf((char*)  s, "SQL_DESC_OCTET_LENGTH" );
        break;

      case SQL_DESC_OCTET_LENGTH_PTR:
        sprintf((char*)  s, "SQL_DESC_OCTET_LENGTH_PTR" );
        break;

      case SQL_DESC_PARAMETER_TYPE:
        sprintf((char*)  s, "SQL_DESC_PARAMETER_TYPE" );
        break;

      case SQL_DESC_PRECISION:
        sprintf((char*)  s, "SQL_DESC_PRECISION" );
        break;

      case SQL_DESC_SCALE:
        sprintf((char*)  s, "SQL_DESC_SCALE" );
        break;

      case SQL_DESC_SCHEMA_NAME:
        sprintf((char*)  s, "SQL_DESC_SCHEMA_NAME" );
        break;

      case SQL_DESC_SEARCHABLE:
        sprintf((char*)  s, "SQL_DESC_SEARCHABLE" );
        break;

      case SQL_DESC_TABLE_NAME:
        sprintf((char*)  s, "SQL_DESC_TABLE_NAME" );
        break;

      case SQL_DESC_TYPE:
        sprintf((char*)  s, "SQL_DESC_TYPE" );
        break;

      case SQL_DESC_TYPE_NAME:
        sprintf((char*)  s, "SQL_DESC_TYPE_NAME" );
        break;

      case SQL_DESC_UNNAMED:
        sprintf((char*)  s, "SQL_DESC_UNNAMED" );
        break;

      case SQL_DESC_UNSIGNED:
        sprintf((char*)  s, "SQL_DESC_UNSIGNED" );
        break;

      case SQL_DESC_UPDATABLE:
        sprintf((char*)  s, "SQL_DESC_UPDATABLE" );
        break;

      default:
        sprintf((char*)  s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * convert a statement attribute to a string
 */

char * __stmt_attr_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_ATTR_APP_PARAM_DESC:
        sprintf((char*)  s, "SQL_ATTR_APP_PARAM_DESC" );
        break;

      case SQL_ATTR_APP_ROW_DESC:
        sprintf((char*)  s, "SQL_ATTR_APP_ROW_DESC" );
        break;

      case SQL_ATTR_ASYNC_ENABLE:
        sprintf((char*)  s, "SQL_ATTR_ASYNC_ENABLE" );
        break;

      case SQL_ATTR_CONCURRENCY:
        sprintf((char*)  s, "SQL_ATTR_CONCURRENCY" );
        break;

      case SQL_ATTR_CURSOR_SCROLLABLE:
        sprintf((char*)  s, "SQL_ATTR_CURSOR_SCROLLABLE" );
        break;

      case SQL_ATTR_CURSOR_SENSITIVITY:
        sprintf((char*)  s, "SQL_ATTR_CURSOR_SENSITIVITY" );
        break;

      case SQL_ATTR_CURSOR_TYPE:
        sprintf((char*)  s, "SQL_ATTR_CURSOR_TYPE" );
        break;

      case SQL_ATTR_ENABLE_AUTO_IPD:
        sprintf((char*)  s, "SQL_ATTR_ENABLE_AUTO_IPD" );
        break;

      case SQL_ATTR_FETCH_BOOKMARK_PTR:
        sprintf((char*)  s, "SQL_ATTR_FETCH_BOOKMARK_PTR" );
        break;

      case SQL_ATTR_IMP_PARAM_DESC:
        sprintf((char*)  s, "SQL_ATTR_IMP_PARAM_DESC" );
        break;

      case SQL_ATTR_IMP_ROW_DESC:
        sprintf((char*)  s, "SQL_ATTR_IMP_ROW_DESC" );
        break;

      case SQL_ATTR_KEYSET_SIZE:
        sprintf((char*)  s, "SQL_ATTR_KEYSET_SIZE" );
        break;

      case SQL_ATTR_MAX_LENGTH:
        sprintf((char*)  s, "SQL_ATTR_MAX_LENGTH" );
        break;

      case SQL_ATTR_MAX_ROWS:
        sprintf((char*)  s, "SQL_ATTR_MAX_ROWS" );
        break;

      case SQL_ATTR_METADATA_ID:
        sprintf((char*)  s, "SQL_ATTR_METADATA_ID" );
        break;

      case SQL_ATTR_NOSCAN:
        sprintf((char*)  s, "SQL_ATTR_NOSCAN" );
        break;

      case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        sprintf((char*)  s, "SQL_ATTR_PARAM_BIND_OFFSET_PTR" );
        break;

      case SQL_ATTR_PARAM_BIND_TYPE:
        sprintf((char*)  s, "SQL_ATTR_PARAM_BIND_TYPE" );
        break;

      case SQL_ATTR_PARAM_OPERATION_PTR:
        sprintf((char*)  s, "SQL_ATTR_PARAM_OPERATION_PTR" );
        break;

      case SQL_ATTR_PARAM_STATUS_PTR:
        sprintf((char*)  s, "SQL_ATTR_PARAM_STATUS_PTR" );
        break;

      case SQL_ATTR_PARAMS_PROCESSED_PTR:
        sprintf((char*)  s, "SQL_ATTR_PARAMS_PROCESSED_PTR" );
        break;

      case SQL_ATTR_PARAMSET_SIZE:
        sprintf((char*)  s, "SQL_ATTR_PARAMSET_SIZE" );
        break;

      case SQL_ATTR_QUERY_TIMEOUT:
        sprintf((char*)  s, "SQL_ATTR_QUERY_TIMEOUT" );
        break;

      case SQL_ATTR_RETRIEVE_DATA:
        sprintf((char*)  s, "SQL_ATTR_RETRIEVE_DATA" );
        break;

      case SQL_ROWSET_SIZE:
        sprintf((char*)  s, "SQL_ROWSET_SIZE" );
        break;

      case SQL_ATTR_ROW_ARRAY_SIZE:
        sprintf((char*)  s, "SQL_ATTR_ROW_ARRAY_SIZE" );
        break;

      case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        sprintf((char*)  s, "SQL_ATTR_ROW_BIND_OFFSET_PTR" );
        break;

      case SQL_ATTR_ROW_BIND_TYPE:
        sprintf((char*)  s, "SQL_ATTR_ROW_BIND_TYPE" );
        break;

      case SQL_ATTR_ROW_NUMBER:
        sprintf((char*)  s, "SQL_ATTR_ROW_NUMBER" );
        break;

      case SQL_ATTR_ROW_OPERATION_PTR:
        sprintf((char*)  s, "SQL_ATTR_ROW_OPERATION_PTR" );
        break;

      case SQL_ATTR_ROW_STATUS_PTR:
        sprintf((char*)  s, "SQL_ATTR_ROW_STATUS_PTR" );
        break;

      case SQL_ATTR_ROWS_FETCHED_PTR:
        sprintf((char*)  s, "SQL_ATTR_ROWS_FETCHED_PTR" );
        break;

      case SQL_ATTR_SIMULATE_CURSOR:
        sprintf((char*)  s, "SQL_ATTR_SIMULATE_CURSOR" );
        break;

      case SQL_ATTR_USE_BOOKMARKS:
        sprintf((char*)  s, "SQL_ATTR_USE_BOOKMARKS" );
        break;

      default:
        sprintf((char*)  s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * return a SQLGetInfo type as a string
 */

char * __info_as_string( SQLCHAR *s, SQLINTEGER type )
{
    switch( type )
    {
      case SQL_ACCESSIBLE_PROCEDURES:
        sprintf((char*)  s, "SQL_ACCESSIBLE_PROCEDURES" );
        break;

      case SQL_ACCESSIBLE_TABLES:
        sprintf((char*)  s, "SQL_ACCESSIBLE_TABLES" );
        break;

      case SQL_ACTIVE_ENVIRONMENTS:
        sprintf((char*)  s, "SQL_ACTIVE_ENVIRONMENTS" );
        break;

      case SQL_AGGREGATE_FUNCTIONS:
        sprintf((char*)  s, "SQL_AGGREGATE_FUNCTIONS" );
        break;

      case SQL_ALTER_DOMAIN:
        sprintf((char*)  s, "SQL_ALTER_DOMAIN" );
        break;

      case SQL_ALTER_TABLE:
        sprintf((char*)  s, "SQL_ALTER_TABLE" );
        break;

      case SQL_ASYNC_MODE:
        sprintf((char*)  s, "SQL_ASYNC_MODE" );
        break;

      case SQL_BATCH_ROW_COUNT:
        sprintf((char*)  s, "SQL_BATCH_ROW_COUNT" );
        break;

      case SQL_BATCH_SUPPORT:
        sprintf((char*)  s, "SQL_BATCH_SUPPORT" );
        break;

      case SQL_BOOKMARK_PERSISTENCE:
        sprintf((char*)  s, "SQL_BOOKMARK_PERSISTENCE" );
        break;

      case SQL_CATALOG_LOCATION:
        sprintf((char*)  s, "SQL_CATALOG_LOCATION" );
        break;
    
      case SQL_CATALOG_NAME:
        sprintf((char*)  s, "SQL_CATALOG_NAME" );
        break;
    
      case SQL_CATALOG_NAME_SEPARATOR:
        sprintf((char*)  s, "SQL_CATALOG_NAME_SEPARATOR" );
        break;

      case SQL_CATALOG_TERM:
        sprintf((char*)  s, "SQL_CATALOG_TERM" );
        break;

      case SQL_CATALOG_USAGE:
        sprintf((char*)  s, "SQL_CATALOG_USAGE" );
        break;

      case SQL_COLLATION_SEQ:
        sprintf((char*)  s, "SQL_COLLATION_SEQ" );
        break;

      case SQL_COLUMN_ALIAS:
        sprintf((char*)  s, "SQL_COLUMN_ALIAS" );
        break;
    
      case SQL_CONCAT_NULL_BEHAVIOR:    
        sprintf((char*)  s, "SQL_CONCAT_NULL_BEHAVIOR" );
        break;

      case SQL_CONVERT_BIGINT:
        sprintf((char*)  s, "SQL_CONVERT_BIGINT" );
        break;

      case SQL_CONVERT_BINARY:
        sprintf((char*)  s, "SQL_CONVERT_BINARY" );
        break;

      case SQL_CONVERT_BIT:
        sprintf((char*)  s, "SQL_CONVERT_BIT" );
        break;

      case SQL_CONVERT_CHAR:
        sprintf((char*)  s, "SQL_CONVERT_CHAR" );
        break;

      case SQL_CONVERT_DATE:
        sprintf((char*)  s, "SQL_CONVERT_DATE" );
        break;

      case SQL_CONVERT_DECIMAL:
        sprintf((char*)  s, "SQL_CONVERT_DECIMAL" );
        break;

      case SQL_CONVERT_DOUBLE:
        sprintf((char*)  s, "SQL_CONVERT_DOUBLE" );
        break;

      case SQL_CONVERT_FLOAT:
        sprintf((char*)  s, "SQL_CONVERT_FLOAT" );
        break;

      case SQL_CONVERT_INTEGER:
        sprintf((char*)  s, "SQL_CONVERT_INTEGER" );
        break;

      case SQL_CONVERT_INTERVAL_YEAR_MONTH:
        sprintf((char*)  s, "SQL_CONVERT_INTERVAL_YEAR_MONTH" );
        break;

      case SQL_CONVERT_INTERVAL_DAY_TIME:
        sprintf((char*)  s, "SQL_CONVERT_INTERVAL_DAY_TIME" );
        break;

      case SQL_CONVERT_LONGVARBINARY:
        sprintf((char*)  s, "SQL_CONVERT_LONGVARBINARY" );
        break;

      case SQL_CONVERT_LONGVARCHAR:
        sprintf((char*)  s, "SQL_CONVERT_LONGVARCHAR" );
        break;

      case SQL_CONVERT_NUMERIC:
        sprintf((char*)  s, "SQL_CONVERT_NUMERIC" );
        break;

      case SQL_CONVERT_REAL:
        sprintf((char*)  s, "SQL_CONVERT_REAL" );
        break;

      case SQL_CONVERT_SMALLINT:
        sprintf((char*)  s, "SQL_CONVERT_SMALLINT" );
        break;

      case SQL_CONVERT_TIME:
        sprintf((char*)  s, "SQL_CONVERT_TIME" );
        break;

      case SQL_CONVERT_TIMESTAMP:
        sprintf((char*)  s, "SQL_CONVERT_TIMESTAMP" );
        break;

      case SQL_CONVERT_TINYINT:
        sprintf((char*)  s, "SQL_CONVERT_TINYINT" );
        break;

      case SQL_CONVERT_VARBINARY:
        sprintf((char*)  s, "SQL_CONVERT_VARBINARY" );
        break;

      case SQL_CONVERT_VARCHAR:
        sprintf((char*)  s, "SQL_CONVERT_VARCHAR" );
        break;

      case SQL_CONVERT_FUNCTIONS:
        sprintf((char*)  s, "SQL_CONVERT_FUNCTIONS" );
        break;

      case SQL_CORRELATION_NAME:
        sprintf((char*)  s, "SQL_CORRELATION_NAME" );
        break;

      case SQL_CREATE_ASSERTION:
        sprintf((char*)  s, "SQL_CREATE_ASSERTION" );
        break;

      case SQL_CREATE_CHARACTER_SET:
        sprintf((char*)  s, "SQL_CREATE_CHARACTER_SET" );
        break;

      case SQL_CREATE_COLLATION:
        sprintf((char*)  s, "SQL_CREATE_COLLATION" );
        break;

      case SQL_CREATE_DOMAIN:
        sprintf((char*)  s, "SQL_CREATE_DOMAIN" );
        break;

      case SQL_CREATE_SCHEMA:
        sprintf((char*)  s, "SQL_CREATE_SCHEMA" );
        break;

      case SQL_CREATE_TABLE:
        sprintf((char*)  s, "SQL_CREATE_TABLE" );
        break;

      case SQL_CREATE_TRANSLATION:
        sprintf((char*)  s, "SQL_CREATE_TRANSLATION" );
        break;

      case SQL_CREATE_VIEW:
        sprintf((char*)  s, "SQL_CREATE_VIEW" );
        break;

      case SQL_CURSOR_COMMIT_BEHAVIOR:
        sprintf((char*)  s, "SQL_CURSOR_COMMIT_BEHAVIOR" );
        break;

      case SQL_CURSOR_ROLLBACK_BEHAVIOR:
        sprintf((char*)  s, "SQL_CURSOR_ROLLBACK_BEHAVIOR" );
        break;

      case SQL_CURSOR_SENSITIVITY:
        sprintf((char*)  s, "SQL_CURSOR_SENSITIVITY" );
        break;

      case SQL_DATA_SOURCE_NAME:
        sprintf((char*)  s, "SQL_DATA_SOURCE_NAME" );
        break;

      case SQL_DATA_SOURCE_READ_ONLY:
        sprintf((char*)  s, "SQL_DATA_SOURCE_READ_ONLY" );
        break;

      case SQL_DATABASE_NAME:
        sprintf((char*)  s, "SQL_DATABASE_NAME" );
        break;

      case SQL_DATETIME_LITERALS:
        sprintf((char*)  s, "SQL_DATETIME_LITERALS" );
        break;

      case SQL_DBMS_NAME:
        sprintf((char*)  s, "SQL_DBMS_NAME" );
        break;

      case SQL_DBMS_VER:
        sprintf((char*)  s, "SQL_DBMS_VER" );
        break;

      case SQL_DDL_INDEX:
        sprintf((char*)  s, "SQL_DDL_INDEX" );
        break;

      case SQL_DEFAULT_TXN_ISOLATION:
        sprintf((char*)  s, "SQL_DEFAULT_TXN_ISOLATION" );
        break;

      case SQL_DESCRIBE_PARAMETER:
        sprintf((char*)  s, "SQL_DESCRIBE_PARAMETER" );
        break;

      case SQL_DRIVER_NAME:
        sprintf((char*)  s, "SQL_DRIVER_NAME" );
        break;

      case SQL_DRIVER_HLIB:
        sprintf((char*)  s, "SQL_DRIVER_HLIB" );
        break;

      case SQL_DRIVER_HSTMT:
        sprintf((char*)  s, "SQL_DRIVER_HSTMT" );
        break;

      case SQL_DRIVER_ODBC_VER:
        sprintf((char*)  s, "SQL_DRIVER_ODBC_VER" );
        break;

      case SQL_DRIVER_VER:
        sprintf((char*)  s, "SQL_DRIVER_VER" );
        break;

      case SQL_ODBC_VER:
        sprintf((char*)  s, "SQL_ODBC_VER" );
        break;
  
      case SQL_DROP_ASSERTION:
        sprintf((char*)  s, "SQL_DROP_ASSERTION" );
        break;

      case SQL_DROP_CHARACTER_SET:
        sprintf((char*)  s, "SQL_DROP_CHARACTER_SET" );
        break;

      case SQL_DROP_COLLATION:
        sprintf((char*)  s, "SQL_DROP_COLLATION" );
        break;

      case SQL_DROP_DOMAIN:
        sprintf((char*)  s, "SQL_DROP_DOMAIN" );
        break;

      case SQL_DROP_SCHEMA:
        sprintf((char*)  s, "SQL_DROP_SCHEMA" );
        break;

      case SQL_DROP_TABLE:
        sprintf((char*)  s, "SQL_DROP_TABLE" );
        break;

      case SQL_DROP_TRANSLATION:
        sprintf((char*)  s, "SQL_DROP_TRANSLATION" );
        break;

      case SQL_DROP_VIEW:
        sprintf((char*)  s, "SQL_DROP_VIEW" );
        break;

      case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
        sprintf((char*)  s, "SQL_DYNAMIC_CURSOR_ATTRIBUTES1" );
        break;
    
      case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
        sprintf((char*)  s, "SQL_EXPRESSIONS_IN_ORDERBY" );
        break;

      case SQL_EXPRESSIONS_IN_ORDERBY:
        sprintf((char*)  s, "SQL_EXPRESSIONS_IN_ORDERBY" );
        break;

      case SQL_FILE_USAGE:
        sprintf((char*)  s, "SQL_FILE_USAGE" );
        break;

      case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
        sprintf((char*)  s, "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1" );
        break;

      case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
        sprintf((char*)  s, "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2" );
        break;

      case SQL_GETDATA_EXTENSIONS:
        sprintf((char*)  s, "SQL_GETDATA_EXTENSIONS" );
        break;

      case SQL_GROUP_BY:
        sprintf((char*)  s, "SQL_GROUP_BY" );
        break;

      case SQL_IDENTIFIER_CASE:
        sprintf((char*)  s, "SQL_IDENTIFIER_CASE" );
        break;

      case SQL_IDENTIFIER_QUOTE_CHAR:
        sprintf((char*)  s, "SQL_IDENTIFIER_QUOTE_CHAR" );
        break;

      case SQL_INDEX_KEYWORDS:
        sprintf((char*)  s, "SQL_INDEX_KEYWORDS" );
        break;

      case SQL_INFO_SCHEMA_VIEWS:
        sprintf((char*)  s, "SQL_INFO_SCHEMA_VIEWS" );
        break;

      case SQL_INSERT_STATEMENT:
        sprintf((char*)  s, "SQL_INSERT_STATEMENT" );
        break;

      case SQL_INTEGRITY:    
        sprintf((char*)  s, "SQL_INTEGRITY" );
        break;

      case SQL_KEYSET_CURSOR_ATTRIBUTES1:
        sprintf((char*)  s, "SQL_KEYSET_CURSOR_ATTRIBUTES1" );
        break;

      case SQL_KEYSET_CURSOR_ATTRIBUTES2:
        sprintf((char*)  s, "SQL_KEYSET_CURSOR_ATTRIBUTES2" );
        break;

      case SQL_KEYWORDS:
        sprintf((char*)  s, "SQL_KEYWORDS" );
        break;

      case SQL_LIKE_ESCAPE_CLAUSE:
        sprintf((char*)  s, "SQL_LIKE_ESCAPE_CLAUSE" );
        break;

      case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
        sprintf((char*)  s, "SQL_MAX_ASYNC_CONCURRENT_STATEMENTS" );
        break;

      case SQL_MAX_BINARY_LITERAL_LEN:
        sprintf((char*)  s, "SQL_MAX_BINARY_LITERAL_LEN" );
        break;

      case SQL_MAX_CATALOG_NAME_LEN:
        sprintf((char*)  s, "SQL_MAX_CATALOG_NAME_LEN" );
        break;
    
      case SQL_MAX_CHAR_LITERAL_LEN:
        sprintf((char*)  s, "SQL_MAX_CHAR_LITERAL_LEN" );
        break;

      case SQL_MAX_COLUMN_NAME_LEN:
        sprintf((char*)  s, "SQL_MAX_COLUMN_NAME_LEN" );
        break;

      case SQL_MAX_COLUMNS_IN_GROUP_BY:    
        sprintf((char*)  s, "SQL_MAX_COLUMNS_IN_GROUP_BY" );
        break;

      case SQL_MAX_COLUMNS_IN_INDEX:
        sprintf((char*)  s, "SQL_MAX_COLUMNS_IN_INDEX" );
        break;

      case SQL_MAX_COLUMNS_IN_SELECT:
        sprintf((char*)  s, "SQL_MAX_COLUMNS_IN_SELECT" );
        break;

      case SQL_MAX_COLUMNS_IN_ORDER_BY:
        sprintf((char*)  s, "SQL_MAX_COLUMNS_IN_ORDER_BY" );
        break;

      case SQL_MAX_COLUMNS_IN_TABLE:
        sprintf((char*)  s, "SQL_MAX_COLUMNS_IN_TABLE" );
        break;

      case SQL_MAX_CONCURRENT_ACTIVITIES:
        sprintf((char*)  s, "SQL_MAX_CONCURRENT_ACTIVITIES" );
        break;

      case SQL_MAX_CURSOR_NAME_LEN:    
        sprintf((char*)  s, "SQL_MAX_CURSOR_NAME_LEN" );
        break;

      case SQL_MAX_DRIVER_CONNECTIONS:    
        sprintf((char*)  s, "SQL_MAX_DRIVER_CONNECTIONS" );
        break;

      case SQL_MAX_IDENTIFIER_LEN:    
        sprintf((char*)  s, "SQL_MAX_IDENTIFIER_LEN" );
        break;

      case SQL_MAX_INDEX_SIZE:    
        sprintf((char*)  s, "SQL_MAX_INDEX_SIZE" );
        break;

      case SQL_MAX_PROCEDURE_NAME_LEN:
        sprintf((char*)  s, "SQL_MAX_PROCEDURE_NAME_LEN" );
        break;

      case SQL_MAX_ROW_SIZE:    
        sprintf((char*)  s, "SQL_MAX_ROW_SIZE" );
        break;
    
      case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
        sprintf((char*)  s, "SQL_MAX_ROW_SIZE_INCLUDES_LONG" );
        break;

      case SQL_MAX_SCHEMA_NAME_LEN:
        sprintf((char*)  s, "SQL_MAX_SCHEMA_NAME_LEN" );
        break;

      case SQL_MAX_STATEMENT_LEN:
        sprintf((char*)  s, "SQL_MAX_STATEMENT_LEN" );
        break;

      case SQL_MAX_TABLE_NAME_LEN:    
        sprintf((char*)  s, "SQL_MAX_TABLE_NAME_LEN" );
        break;

      case SQL_MAX_TABLES_IN_SELECT:
        sprintf((char*)  s, "SQL_MAX_TABLES_IN_SELECT" );
        break;

      case SQL_MAX_USER_NAME_LEN:
        sprintf((char*)  s, "SQL_MAX_USER_NAME_LEN" );
        break;

      case SQL_MULT_RESULT_SETS:
        sprintf((char*)  s, "SQL_MULT_RESULT_SETS" );
        break;

      case SQL_MULTIPLE_ACTIVE_TXN:
        sprintf((char*)  s, "SQL_MULTIPLE_ACTIVE_TXN" );
        break;

      case SQL_NEED_LONG_DATA_LEN:
        sprintf((char*)  s, "SQL_NEED_LONG_DATA_LEN" );
        break;

      case SQL_NON_NULLABLE_COLUMNS:
        sprintf((char*)  s, "SQL_NON_NULLABLE_COLUMNS" );
        break;

      case SQL_NULL_COLLATION:
        sprintf((char*)  s, "SQL_NULL_COLLATION" );
        break;

      case SQL_NUMERIC_FUNCTIONS:        
        sprintf((char*)  s, "SQL_NUMERIC_FUNCTIONS" );
        break;

      case SQL_ODBC_INTERFACE_CONFORMANCE:
        sprintf((char*)  s, "SQL_ODBC_INTERFACE_CONFORMANCE" );
        break;

      case SQL_OJ_CAPABILITIES:
        sprintf((char*)  s, "SQL_OJ_CAPABILITIES" );
        break;

      case SQL_ORDER_BY_COLUMNS_IN_SELECT:
        sprintf((char*)  s, "SQL_ORDER_BY_COLUMNS_IN_SELECT" );
        break;

      case SQL_PARAM_ARRAY_ROW_COUNTS:
        sprintf((char*)  s, "SQL_PARAM_ARRAY_ROW_COUNTS" );
        break;

      case SQL_PARAM_ARRAY_SELECTS:
        sprintf((char*)  s, "SQL_PARAM_ARRAY_SELECTS" );
        break;

      case SQL_PROCEDURE_TERM:
        sprintf((char*)  s, "SQL_PROCEDURE_TERM" );
        break;

      case SQL_PROCEDURES:
        sprintf((char*)  s, "SQL_PROCEDURES" );
        break;

      case SQL_QUOTED_IDENTIFIER_CASE:
        sprintf((char*)  s, "SQL_QUOTED_IDENTIFIER_CASE" );
        break;

      case SQL_ROW_UPDATES:
        sprintf((char*)  s, "SQL_ROW_UPDATES" );
        break;

      case SQL_SCHEMA_TERM:
        sprintf((char*)  s, "SQL_SCHEMA_TERM" );
        break;

      case SQL_SCHEMA_USAGE:
        sprintf((char*)  s, "SQL_SCHEMA_USAGE" );
        break;

      case SQL_SCROLL_OPTIONS:
        sprintf((char*)  s, "SQL_SCROLL_OPTIONS" );
        break;

      case SQL_SEARCH_PATTERN_ESCAPE:
        sprintf((char*)  s, "SQL_SEARCH_PATTERN_ESCAPE" );
        break;

      case SQL_SERVER_NAME:
        sprintf((char*)  s, "SQL_SERVER_NAME" );
        break;

      case SQL_SPECIAL_CHARACTERS:
        sprintf((char*)  s, "SQL_SPECIAL_CHARACTERS" );
        break;

      case SQL_SQL_CONFORMANCE:
        sprintf((char*)  s, "SQL_SQL_CONFORMANCE" );
        break;

      case SQL_SQL92_DATETIME_FUNCTIONS:
        sprintf((char*)  s, "SQL_SQL92_DATETIME_FUNCTIONS" );
        break;

      case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
        sprintf((char*)  s, "SQL_SQL92_FOREIGN_KEY_DELETE_RULE" );
        break;

      case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
        sprintf((char*)  s, "SQL_SQL92_FOREIGN_KEY_UPDATE_RULE" );
        break;

      case SQL_SQL92_GRANT:
        sprintf((char*)  s, "SQL_SQL92_GRANT" );
        break;

      case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
        sprintf((char*)  s, "SQL_SQL92_NUMERIC_VALUE_FUNCTIONS" );
        break;

      case SQL_SQL92_PREDICATES:
        sprintf((char*)  s, "SQL_SQL92_PREDICATES" );
        break;

      case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
        sprintf((char*)  s, "SQL_SQL92_RELATIONAL_JOIN_OPERATORS" );
        break;

      case SQL_SQL92_REVOKE:
        sprintf((char*)  s, "SQL_SQL92_REVOKE" );
        break;

      case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
        sprintf((char*)  s, "SQL_SQL92_ROW_VALUE_CONSTRUCTOR" );
        break;

      case SQL_SQL92_STRING_FUNCTIONS:
        sprintf((char*)  s, "SQL_SQL92_STRING_EXPRESSIONS" );
        break;

      case SQL_SQL92_VALUE_EXPRESSIONS:
        sprintf((char*)  s, "SQL_SQL92_VALUE_EXPRESSIONS" );
        break;

      case SQL_STANDARD_CLI_CONFORMANCE:
        sprintf((char*)  s, "SQL_STANDARD_CLI_CONFORMANCE" );
        break;

      case SQL_STATIC_CURSOR_ATTRIBUTES1:
        sprintf((char*)  s, "SQL_STATIC_CURSOR_ATTRIBUTES1" );
        break;

      case SQL_STATIC_CURSOR_ATTRIBUTES2:
        sprintf((char*)  s, "SQL_STATIC_CURSOR_ATTRIBUTES2" );
        break;

      case SQL_STRING_FUNCTIONS:
        sprintf((char*)  s, "SQL_STRING_FUNCTIONS" );
        break;

      case SQL_SUBQUERIES:
        sprintf((char*)  s, "SQL_SUBQUERIES" );
        break;

      case SQL_SYSTEM_FUNCTIONS:
        sprintf((char*)  s, "SQL_SYSTEM_FUNCTIONS" );
        break;

      case SQL_TABLE_TERM:
        sprintf((char*)  s, "SQL_TABLE_TERM" );
        break;

      case SQL_TIMEDATE_ADD_INTERVALS:
        sprintf((char*)  s, "SQL_TIMEDATE_ADD_INTERVALS" );
        break;

      case SQL_TIMEDATE_DIFF_INTERVALS:
        sprintf((char*)  s, "SQL_TIMEDATE_DIFF_INTERVALS" );
        break;

      case SQL_TIMEDATE_FUNCTIONS:
        sprintf((char*)  s, "SQL_TIMEDATE_FUNCTIONS" );
        break;

      case SQL_TXN_CAPABLE:
        sprintf((char*)  s, "SQL_TXN_CAPABLE" );
        break;

      case SQL_TXN_ISOLATION_OPTION:
        sprintf((char*)  s, "SQL_TXN_ISOLATION_OPTION" );
        break;

      case SQL_UNION:
        sprintf((char*)  s, "SQL_UNION" );
        break;

      case SQL_USER_NAME:
        sprintf((char*)  s, "SQL_USER_NAME" );
        break;

      case SQL_XOPEN_CLI_YEAR:
        sprintf((char*)  s, "SQL_XOPEN_CLI_YEAR" );
        break;

      case SQL_FETCH_DIRECTION:
        sprintf((char*)  s, "SQL_FETCH_DIRECTION" );
        break;

      case SQL_LOCK_TYPES:
        sprintf((char*)  s, "SQL_LOCK_TYPES" );
        break;

      case SQL_ODBC_API_CONFORMANCE:
        sprintf((char*)  s, "SQL_ODBC_API_CONFORMANCE" );
        break;

      case SQL_ODBC_SQL_CONFORMANCE:
        sprintf((char*)  s, "SQL_ODBC_SQL_CONFORMANCE" );
        break;

      case SQL_POS_OPERATIONS:
        sprintf((char*)  s, "SQL_POS_OPERATIONS" );
        break;

      case SQL_POSITIONED_STATEMENTS:
        sprintf((char*)  s, "SQL_POSITIONED_STATEMENTS" );
        break;

      case SQL_SCROLL_CONCURRENCY:
        sprintf((char*)  s, "SQL_SCROLL_CONCURRENCY" );
        break;

      case SQL_STATIC_SENSITIVITY:
        sprintf((char*)  s, "SQL_STATIC_SENSITIVITY" );
        break;

      case SQL_OUTER_JOINS:
        sprintf((char*)  s, "SQL_OUTER_JOINS" );
        break;

      case SQL_DRIVER_AWARE_POOLING_SUPPORTED:
        sprintf((char*)  s, "SQL_DRIVER_AWARE_POOLING_SUPPORTED" );
        break;

      default:
        sprintf((char*)  s, "%d", (int)type );
    }

    return (char*) s;
}

/*
 * convert from type 3 error states to type 2
 */

struct state_map
{
    char ver2[6];
    char ver3[6];
};

static const struct state_map state_mapping_3_2[] = {
    { "01S03", "01001" },
    { "01S04", "01001" },
    { "22003", "HY019" },
    { "22005", "22018" },
    { "22008", "22007" },
    { "24000", "07005" },
    { "37000", "42000" },
    { "70100", "HY018" },
    { "S0001", "42S01" },
    { "S0002", "42S02" },
    { "S0011", "42S11" },
    { "S0012", "42S12" },
    { "S0021", "42S21" },
    { "S0022", "42S22" },
    { "S0023", "42S23" },
    { "S1000", "HY000" },
    { "S1001", "HY001" },
    { "S1002", "07009" },
    { "S1003", "HY003" },
    { "S1004", "HY004" },
    { "S1007", "HY007" },
    { "S1008", "HY008" },
    { "S1009", "HY009" },
    { "S1010", "HY010" },
    { "S1011", "HY011" },
    { "S1012", "HY012" },
    { "S1090", "HY090" },
    { "S1091", "HY091" },
    { "S1092", "HY092" },
    { "S1093", "07009" },
    { "S1096", "HY096" },
    { "S1097", "HY097" },
    { "S1098", "HY098" },
    { "S1099", "HY099" },
    { "S1100", "HY100" },
    { "S1101", "HY101" },
    { "S1103", "HY103" },
    { "S1104", "HY104" },
    { "S1105", "HY105" },
    { "S1106", "HY106" },
    { "S1107", "HY107" },
    { "S1108", "HY108" },
    { "S1109", "HY109" },
    { "S1110", "HY110" },
    { "S1111", "HY111" },
    { "S1C00", "HYC00" },
    { "S1T00", "HYT00" },
    { "", "" }
};

/*
 * the book doesn't say that it should map ODBC 2 states to ODBC 3
 * but the MS Windows DM can be seen to do just that
 */

static const struct state_map state_mapping_2_3[] = {
    { "01S03", "01001" },
    { "01S04", "01001" },
    { "22005", "22018" },
    { "37000", "42000" },
    { "70100", "HY018" },
    { "S0001", "42S01" },
    { "S0002", "42S02" },
    { "S0011", "42S11" },
    { "S0012", "42S12" },
    { "S0021", "42S21" },
    { "S0022", "42S22" },
    { "S0023", "42S23" },
    { "S1000", "HY000" },
    { "S1001", "HY001" },
    { "S1002", "07009" },
    { "S1003", "HY003" },
    { "S1004", "HY004" },
    { "S1007", "HY007" },
    { "S1008", "HY008" },
    { "S1009", "HY009" },
    { "S1010", "HY010" },
    { "S1011", "HY011" },
    { "S1012", "HY012" },
    { "S1090", "HY090" },
    { "S1091", "HY091" },
    { "S1092", "HY092" },
    { "S1093", "07009" },
    { "S1096", "HY096" },
    { "S1097", "HY097" },
    { "S1098", "HY098" },
    { "S1099", "HY099" },
    { "S1100", "HY100" },
    { "S1101", "HY101" },
    { "S1103", "HY103" },
    { "S1104", "HY104" },
    { "S1105", "HY105" },
    { "S1106", "HY106" },
    { "S1107", "HY107" },
    { "S1108", "HY108" },
    { "S1109", "HY109" },
    { "S1110", "HY110" },
    { "S1111", "HY111" },
    { "S1C00", "HYC00" },
    { "S1T00", "HYT00" },
    { "", "" }
};

/*
 * map ODBC3 states to/from ODBC 2
 */

void __map_error_state( char * state, int requested_version )
{
    const struct state_map *ptr;

    if ( !state )
        return;


    if ( requested_version == SQL_OV_ODBC2 )
    {
        ptr = state_mapping_3_2;

        while( ptr -> ver3[0] )
        {
            if ( strcmp( ptr -> ver3, state ) == 0 )
            {
                strcpy( state, ptr -> ver2 );
                return;
            }
            ptr ++;
        }
    }
    else if ( requested_version >= SQL_OV_ODBC3 )
    {
        ptr = state_mapping_2_3;

        while( ptr -> ver2[0] )
        {
            if ( strcmp( ptr -> ver2, state ) == 0 )
            {
                strcpy( state, ptr -> ver3 );
                return;
            }
            ptr ++;
        }
    }
}

void __map_error_state_w( SQLWCHAR * wstate, int requested_version )
{
    char state[ 6 ];

    unicode_to_ansi_copy( state, 6, wstate, SQL_NTS, NULL, NULL );

    __map_error_state( state, requested_version );

    ansi_to_unicode_copy( wstate, state, SQL_NTS, NULL, NULL );
}

/*
 * return the process id as a string
 */

char * __get_pid( SQLCHAR * str )
{
    sprintf((char *) str, "%d", getpid());

    return (char*)str;
}

/*
 * take a SQL string and its length indicator and format it for
 * display
 */

char * __string_with_length( SQLCHAR *ostr, SQLCHAR *instr, SQLINTEGER len )
{
    if ( instr == NULL )
    {
        sprintf((char*) ostr, "[NULL]" );
    }
    else if ( len == SQL_NTS )
    {
        if ( strlen((char*) instr ) > LOG_MESSAGE_LEN )
        {
            sprintf((char*) ostr, "[%.*s...][length = %ld (SQL_NTS)]",
                LOG_MESSAGE_LEN, instr, (long int)strlen((char*) instr ));
        }
        else
        {
            sprintf((char*) ostr, "[%s][length = %ld (SQL_NTS)]",
                instr, (long int)strlen((char*) instr ));
        }

    }
    else
    {
        if ( len < LOG_MESSAGE_LEN )
            sprintf((char*) ostr, "[%.*s][length = %d]", (int)len, instr, (int)len );
        else
            sprintf((char*) ostr, "[%.*s...][length = %d]", LOG_MESSAGE_LEN, instr, (int)len );
    }

    return (char*)ostr;
}

char * __wstring_with_length( SQLCHAR *ostr, SQLWCHAR *instr, SQLINTEGER len )
{
    int i = 0;
    char tmp[ LOG_MESSAGE_LEN ];

    if ( instr == NULL )
    {
        sprintf((char*) ostr, "[NULL]" );
    }
    else if ( len == SQL_NTS )
    {
        if ( ( i = wide_strlen( instr ) ) < LOG_MESSAGE_LEN )
        {
            strcpy((char*) ostr, "[" );
            unicode_to_ansi_copy((char*) ostr + 1, LOG_MESSAGE_LEN, instr, i, NULL, NULL );
            strcat((char*) ostr, "]" );
        }
        else
        {
            strcpy((char*) ostr, "[" );
            unicode_to_ansi_copy((char*) ostr + 1, LOG_MESSAGE_LEN, instr, LOG_MESSAGE_LEN, NULL, NULL );
            strcat((char*) ostr, "...]" );
        }
        sprintf( tmp, "[length = %d (SQL_NTS)]", i );
        strcat((char*) ostr, tmp );
    }
    else
    {
        if ( len < LOG_MESSAGE_LEN )
        {
            strcpy((char*) ostr, "[" );
            unicode_to_ansi_copy((char*) ostr + 1, LOG_MESSAGE_LEN, instr, len, NULL, NULL );
            strcat((char*) ostr, "]" );
        }
        else
        {
            strcpy((char*) ostr, "[" );
            unicode_to_ansi_copy((char*) ostr + 1, LOG_MESSAGE_LEN, instr, LOG_MESSAGE_LEN, NULL, NULL );
            strcat((char*) ostr, "...]" );
        }
        sprintf( tmp, "[length = %d]", (int)len );
        strcat((char*) ostr, tmp );
    }

    return (char*)ostr;
}

/*
 * replace password with ****
 */

char * __string_with_length_pass( SQLCHAR *out, SQLCHAR *str, SQLINTEGER len )
{
    char *p = __string_with_length( out, str, len );

    /*
     * the string will be of the form [text]
     */

    if ( str )
    {
        char * ptr = p + 1;

        while ( *ptr && *ptr != ']' )
        {
            *ptr = '*';
            ptr ++;
        }
    }

    return p;
}

char * __wstring_with_length_pass( SQLCHAR *out, SQLWCHAR *str, SQLINTEGER len )
{
    char *p = __wstring_with_length( out, str, len );

    /*
     * the string will be of the form [text]
     */

    if ( str )
    {
        char * ptr = p + 1;

        while ( *ptr && *ptr != ']' )
        {
            *ptr = '*';
            ptr ++;
        }
    }

    return p;
}

/*
 * mask out PWD=str;
 * wont work on lower case pwd but there you go
 */

char * __string_with_length_hide_pwd( SQLCHAR *out, SQLCHAR *str, SQLINTEGER len )
{
    char *p = __string_with_length( out, str, len );

    if ( str )
    {
        char *ptr;

        ptr = strstr( p, "PWD=" );
        while ( ptr )
        {
            ptr += 4;
            while ( *ptr && *ptr != ';' && *ptr != ']' )
            {
                *ptr = '*';
                ptr ++;
            }
            ptr = strstr( ptr, "PWD=" );
        }
    }

    return p;
}

char * __wstring_with_length_hide_pwd( SQLCHAR *out, SQLWCHAR *str, SQLINTEGER len )
{
    char *p = __wstring_with_length( out, str, len );

    if ( str )
    {
        char *ptr;

        ptr = strstr( p, "PWD=" );
        while ( ptr )
        {
            ptr += 4;
            while ( *ptr && *ptr != ';' && *ptr != ']' )
            {
                *ptr = '*';
                ptr ++;
            }
            ptr = strstr( ptr, "PWD=" );
        }
    }

    return p;
}

/*
 * display a C type as a string
 */
 
char * __c_as_text( SQLINTEGER type )
{
    switch( type )
    {
      case SQL_C_CHAR:
        return "SQL_C_CHAR";

      case SQL_C_LONG:
        return "SQL_C_LONG";

      case SQL_C_SHORT:
        return "SQL_C_SHORT";

      case SQL_C_FLOAT:
        return "SQL_C_FLOAT";

      case SQL_C_DOUBLE:
        return "SQL_C_DOUBLE";

      case SQL_C_NUMERIC:
        return "SQL_C_NUMERIC";

      case SQL_C_DEFAULT:
        return "SQL_C_DEFAULT";

      case SQL_C_DATE:
        return "SQL_C_DATE";

      case SQL_C_TIME:
        return "SQL_C_TIME";

      case SQL_C_TIMESTAMP:
        return "SQL_C_TIMESTAMP";

      case SQL_C_TYPE_DATE:
        return "SQL_C_TYPE_DATE";

      case SQL_C_TYPE_TIME:
        return "SQL_C_TYPE_TIME";

      case SQL_C_TYPE_TIMESTAMP:
        return "SQL_C_TYPE_TIMESTAMP ";

      case SQL_C_INTERVAL_YEAR:
        return "SQL_C_INTERVAL_YEAR ";

      case SQL_C_INTERVAL_MONTH:
        return "SQL_C_INTERVAL_MONTH";

      case SQL_C_INTERVAL_DAY:
        return "SQL_C_INTERVAL_DAY ";

      case SQL_C_INTERVAL_HOUR:
        return "SQL_C_INTERVAL_HOUR";

      case SQL_C_INTERVAL_MINUTE:
        return "SQL_C_INTERVAL_MINUTE";

      case SQL_C_INTERVAL_SECOND:
        return "SQL_C_INTERVAL_SECOND";

      case SQL_C_INTERVAL_YEAR_TO_MONTH:
        return "SQL_C_INTERVAL_YEAR_TO_MONTH";

      case SQL_C_INTERVAL_DAY_TO_HOUR:
        return "SQL_C_INTERVAL_DAY_TO_HOUR ";

      case SQL_C_INTERVAL_DAY_TO_MINUTE:
        return "SQL_C_INTERVAL_DAY_TO_MINUTE";

      case SQL_C_INTERVAL_DAY_TO_SECOND:
        return "SQL_C_INTERVAL_DAY_TO_SECOND";

      case SQL_C_INTERVAL_HOUR_TO_MINUTE:
        return "SQL_C_INTERVAL_HOUR_TO_MINUTE";

      case SQL_C_INTERVAL_HOUR_TO_SECOND:
        return "SQL_C_INTERVAL_HOUR_TO_SECOND";

      case SQL_C_INTERVAL_MINUTE_TO_SECOND:
        return "SQL_C_INTERVAL_MINUTE_TO_SECOND";

      case SQL_C_BINARY:
        return "SQL_C_BINARY";

      case SQL_C_BIT:
        return "SQL_C_BIT";

      case SQL_C_SBIGINT:
        return "SQL_C_SBIGINT";

      case SQL_C_UBIGINT:
        return "SQL_C_UBIGINT";

      case SQL_C_TINYINT:
        return "SQL_C_TINYINT";

      case SQL_C_SLONG:
        return "SQL_C_SLONG";

      case SQL_C_SSHORT:
        return "SQL_C_SSHORT";

      case SQL_C_STINYINT:
        return "SQL_C_STINYINT";

      case SQL_C_ULONG:
        return "SQL_C_ULONG";

      case SQL_C_USHORT:
        return "SQL_C_USHORT";

      case SQL_C_UTINYINT:
        return "SQL_C_UTINYINT";

      case SQL_C_GUID:
        return "SQL_C_GUID";

      case SQL_C_WCHAR:
        return "SQL_C_WCHAR";

      default:
        return "";
    }
}

/*
 * display a SQL type as a string
 */
 
char * __sql_as_text( SQLINTEGER type )
{
    switch( type )
    {
      case SQL_DECIMAL:
        return "SQL_DECIMAL";

      case SQL_VARCHAR:
        return "SQL_VARCHAR";

      case SQL_LONGVARCHAR:
        return "SQL_LONGVARCHAR";

      case SQL_LONGVARBINARY:
        return "SQL_LONGVARBINARY";

      case SQL_C_BINARY:
        return "SQL_C_BINARY";

      case SQL_VARBINARY:
        return "SQL_VARBINARY";

      case SQL_CHAR:
        return "SQL_CHAR";

      case SQL_WCHAR:
        return "SQL_WCHAR";

      case SQL_WVARCHAR:
        return "SQL_WVARCHAR";

      case SQL_INTEGER:
        return "SQL_INTEGER";

      case SQL_C_ULONG:
        return "SQL_C_ULONG";

      case SQL_C_SLONG:
        return "SQL_C_SLONG";

      case SQL_BIGINT:
        return "SQL_BIGINT";

      case SQL_C_UBIGINT:
        return "SQL_C_SBIGINT";

      case SQL_C_SBIGINT:
        return "SQL_C_SBIGINT";

      case SQL_SMALLINT:
        return "SQL_SMALLINT";

      case SQL_C_USHORT:
        return "SQL_C_USHORT";

      case SQL_C_SSHORT:
        return "SQL_C_SSHORT";

      case SQL_TINYINT:
        return "SQL_TINYINT";

      case SQL_C_UTINYINT:
        return "SQL_C_UTINYINT";

      case SQL_C_STINYINT:
        return "SQL_C_STINYINT";

      case SQL_BIT:
        return "SQL_BIT";

      case SQL_NUMERIC:
        return "SQL_NUMERIC";

      case SQL_REAL:
        return "SQL_REAL";

      case SQL_DOUBLE:
        return "SQL_DOUBLE";

      case SQL_FLOAT:
        return "SQL_FLOAT";

      case SQL_TYPE_DATE:
        return "SQL_TYPE_DATE";

      case SQL_DATE:
        return "SQL_DATE";

      case SQL_TYPE_TIME:
        return "SQL_TYPE_TIME";

      case SQL_TIME:
        return "SQL_TIME";

      case SQL_TYPE_TIMESTAMP:
        return "SQL_TYPE_TIMESTAMP";

      case SQL_TIMESTAMP:
        return "SQL_TIMESTAMP";

      case SQL_INTERVAL_YEAR:
        return "SQL_INTERVAL_YEAR ";

      case SQL_INTERVAL_MONTH:
        return "SQL_INTERVAL_MONTH";

      case SQL_INTERVAL_DAY:
        return "SQL_INTERVAL_DAY ";

      case SQL_INTERVAL_HOUR:
        return "SQL_INTERVAL_HOUR";

      case SQL_INTERVAL_MINUTE:
        return "SQL_INTERVAL_MINUTE";

      case SQL_INTERVAL_SECOND:
        return "SQL_INTERVAL_SECOND";

      case SQL_INTERVAL_YEAR_TO_MONTH:
        return "SQL_INTERVAL_YEAR_TO_MONTH";

      case SQL_INTERVAL_DAY_TO_HOUR:
        return "SQL_INTERVAL_DAY_TO_HOUR ";

      case SQL_INTERVAL_DAY_TO_MINUTE:
        return "SQL_INTERVAL_DAY_TO_MINUTE";

      case SQL_INTERVAL_DAY_TO_SECOND:
        return "SQL_INTERVAL_DAY_TO_SECOND";

      case SQL_INTERVAL_HOUR_TO_MINUTE:
        return "SQL_INTERVAL_HOUR_TO_MINUTE";

      case SQL_INTERVAL_HOUR_TO_SECOND:
        return "SQL_INTERVAL_HOUR_TO_SECOND";

      case SQL_INTERVAL_MINUTE_TO_SECOND:
        return "SQL_INTERVAL_MINUTE_TO_SECOND";

      default:
        return "";
    }
}

/*
 * convert a return type as a string
 */

char * __get_return_status( SQLRETURN ret, SQLCHAR *buffer )
{
    switch ( ret )
    {
      case SQL_SUCCESS:
        return "SQL_SUCCESS";

      case SQL_ERROR:
        return "SQL_ERROR";

      case SQL_SUCCESS_WITH_INFO:
        return "SQL_SUCCESS_WITH_INFO";

      case SQL_NO_DATA:
        return "SQL_NO_DATA";

      case SQL_STILL_EXECUTING:
        return "SQL_STILL_EXECUTING";

      case SQL_INVALID_HANDLE:
        return "SQL_INVALID_HANDLE";

      case SQL_NEED_DATA:
        return "SQL_NEED_DATA";

      case SQL_PARAM_DATA_AVAILABLE:
        return "SQL_PARAM_DATA_AVAILABLE";

      default:
        sprintf((char*) buffer, "UNKNOWN(%d)", ret );
        return (char*)buffer;
    }
}

int wide_ansi_strncmp( SQLWCHAR *str1, char *str2, int len )
{
    char c;

    while( len > 0 )
    {
        if ( *str1 == 0 || *str2 == 0 )
            break;

        c = (char) *str1;
        if ( c != *str2 )
            return *str2 - c;

        str1 ++;
        str2 ++;
        len --;
    }

    c = (char) *str1;

    return *str2 - c;
}

SQLWCHAR *wide_strcpy( SQLWCHAR *str1, SQLWCHAR *str2 )
{
    SQLWCHAR *retp = str1;

    if ( !str1 )
        return NULL;

    while( *str2 )
    {
        *str1 = *str2;
        str1 ++;
        str2 ++;
    }
    *str1 = 0;

    return retp;
}

SQLWCHAR *wide_strncpy( SQLWCHAR *str1, SQLWCHAR *str2, int buffer_length )
{
    SQLWCHAR *retp = str1;

    if ( !str1 )
        return NULL;

    while( *str2 && buffer_length > 0 )
    {
        *str1 = *str2;
        str1 ++;
        str2 ++;
        buffer_length --;
    }
    *str1 = 0;

    return retp;
}

SQLWCHAR *wide_strcat( SQLWCHAR *str1, SQLWCHAR *str2 )
{
    SQLWCHAR *retp = str1;

    while( *str1 )
    {
        str1 ++;
    }

    while( *str2 )
    {
        *str1 = *str2;
        str1 ++;
        str2 ++;
    }
    *str1 = 0;

    return retp;
}

SQLWCHAR *wide_strdup( SQLWCHAR *str1 )
{
    SQLWCHAR *ptr;
    int len = 0;

    while( str1[ len ] )
        len ++;

    ptr = malloc( sizeof( SQLWCHAR ) * ( len + 1 ));
    if ( !ptr )
        return NULL;

    return wide_strcpy( ptr, str1 );
}

int wide_strlen( SQLWCHAR *str1 )
{
    int len = 0;

    while( str1[ len ] ) {
        len ++;
    }

    return len;
}

static int check_error_order( ERROR *e1, ERROR *e2, EHEAD *head )
{
    char *s1, *s2;
    int ret;

    /*
     * as far as I can see, a simple strcmp gives the order we need 
     */

    s1 = unicode_to_ansi_alloc( e1 -> sqlstate, SQL_NTS, __get_connection( head ), NULL);
    s2 = unicode_to_ansi_alloc( e2 -> sqlstate, SQL_NTS, __get_connection( head ), NULL );

    ret = strcmp( s1, s2 );

    free( s1 );
    free( s2 );

    return ret;
}

/* 
 * insert the error into the list, making sure its in the correct
 * order
 */

static void insert_into_error_list( EHEAD *error_header, ERROR *e1 )
{
    error_header -> sql_error_head.error_count ++;

    if ( error_header -> sql_error_head.error_list_head )
    {
        /*
         * find where in the list it needs to go
         */

        ERROR *curr, *prev;

        prev = NULL;
        curr = error_header -> sql_error_head.error_list_head;
        while ( curr && check_error_order( curr, e1, error_header ) >= 0 )
        {
            prev = curr;
            curr = curr -> next;
        }

        if ( curr )
        {
            if ( prev )
            {
                /*
                 * in the middle
                 */
                e1 -> next = curr;
                e1 -> prev = curr -> prev;
                curr -> prev -> next = e1;
                curr -> prev = e1;
            }
            else
            {
                /*
                 * at the beginning
                 */
                e1 -> next = error_header -> sql_error_head.error_list_head;
                e1 -> prev = NULL;
                e1 -> next -> prev = e1;
                error_header -> sql_error_head.error_list_head = e1;
            }
        }
        else
        {
            /*
             * at the end
             */

            e1 -> next = NULL;
            e1 -> prev = error_header -> sql_error_head.error_list_tail;
            e1 -> prev -> next = e1;
            error_header -> sql_error_head.error_list_tail = e1;
        }
    }
    else
    {
        e1 -> next = e1 -> prev = NULL;
        error_header -> sql_error_head.error_list_tail = e1;
        error_header -> sql_error_head.error_list_head = e1;
    }
}

static void insert_into_diag_list( EHEAD *error_header, ERROR *e2 )
{
    error_header -> sql_diag_head.internal_count ++;

    if ( error_header -> sql_diag_head.internal_list_head )
    {
        /*
         * find where in the list it needs to go
         */

        ERROR *curr, *prev;

        prev = NULL;
        curr = error_header -> sql_diag_head.internal_list_head;
        while ( curr && check_error_order( curr, e2, error_header ) >= 0 )
        {
            prev = curr;
            curr = curr -> next;
        }

        if ( curr )
        {
            if ( prev )
            {
                /*
                 * in the middle
                 */
                e2 -> next = curr;
                e2 -> prev = curr -> prev;
                curr -> prev -> next = e2;
                curr -> prev = e2;
            }
            else
            {
                /*
                 * at the beginning
                 */
                e2 -> next = error_header -> sql_diag_head.internal_list_head;
                e2 -> prev = NULL;
                e2 -> next -> prev = e2;
                error_header -> sql_diag_head.internal_list_head = e2;
            }
        }
        else
        {
            /*
             * at the end
             */

            e2 -> next = NULL;
            e2 -> prev = error_header -> sql_diag_head.internal_list_tail;
            e2 -> prev -> next = e2;
            error_header -> sql_diag_head.internal_list_tail = e2;
        }
    }
    else
    {
        e2 -> next = e2 -> prev = NULL;
        error_header -> sql_diag_head.internal_list_tail = e2;
        error_header -> sql_diag_head.internal_list_head = e2;
    }
}

void __post_internal_error_ex( EHEAD *error_header,
        SQLCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLCHAR *message_text,
        int class_origin,
        int subclass_origin )
{
    SQLCHAR msg[ SQL_MAX_MESSAGE_LENGTH + 32 ];

    /*
     * add our prefix
     */

    strcpy((char*) msg, ERROR_PREFIX );
    strncat((char*) msg, (char*) message_text, sizeof( msg ) - ( 1 + strlen( ERROR_PREFIX )));

    __post_internal_error_ex_noprefix(
        error_header,
        sqlstate,
        native_error,
        msg,
        class_origin,
        subclass_origin );
}

void __post_internal_error_ex_noprefix( EHEAD *error_header,
        SQLCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLCHAR *msg,
        int class_origin,
        int subclass_origin )
{
    /*
     * create a error block and add to the lists,
     * leave space for the error prefix
     */

    ERROR *e1, *e2;
    DMHDBC conn = __get_connection( error_header );

    e1 = malloc( sizeof( ERROR ));
    if (e1 == NULL)
        return;
    e2 = malloc( sizeof( ERROR ));
    if (e2 == NULL)
    {
        free(e1);
        return;
    }

    memset( e1, 0, sizeof( *e1 ));
    memset( e2, 0, sizeof( *e2 ));

    e1 -> native_error = native_error;
    e2 -> native_error = native_error;
    ansi_to_unicode_copy(e1 -> sqlstate,
                         (char*)sqlstate, SQL_NTS, conn, NULL );
    wide_strcpy( e2 -> sqlstate, e1 -> sqlstate );

    e1 -> msg = ansi_to_unicode_alloc( msg, SQL_NTS, conn, NULL );
    if ( !e1 -> msg )
    {
        free( e1 );
        free( e2 );
        return;
    }
    e2 -> msg = wide_strdup( e1 -> msg );
    if ( !e2 -> msg )
    {
        free( e1 -> msg);
        free( e1 );
        free( e2 );
        return;
    }

    e1 -> return_val = SQL_ERROR;
    e2 -> return_val = SQL_ERROR;

    e1 -> diag_column_number_ret = SQL_NO_COLUMN_NUMBER;
    e1 -> diag_row_number_ret = SQL_NO_ROW_NUMBER;
    e1 -> diag_class_origin_ret = SQL_SUCCESS;
    e1 -> diag_subclass_origin_ret = SQL_SUCCESS;
    e1 -> diag_connection_name_ret = SQL_SUCCESS;
    e1 -> diag_server_name_ret = SQL_SUCCESS;
    e1 -> diag_column_number = 0;
    e1 -> diag_row_number = 0;

    e2 -> diag_column_number_ret = SQL_NO_COLUMN_NUMBER;
    e2 -> diag_row_number_ret = SQL_NO_ROW_NUMBER;
    e2 -> diag_class_origin_ret = SQL_SUCCESS;
    e2 -> diag_subclass_origin_ret = SQL_SUCCESS;
    e2 -> diag_connection_name_ret = SQL_SUCCESS;
    e2 -> diag_server_name_ret = SQL_SUCCESS;
    e2 -> diag_column_number = 0;
    e2 -> diag_row_number = 0;

    if ( class_origin == SUBCLASS_ODBC )
    	ansi_to_unicode_copy( e1 -> diag_class_origin, (char*) "ODBC 3.0",
			      SQL_NTS, conn, NULL );
    else
    	ansi_to_unicode_copy( e1 -> diag_class_origin, (char*) "ISO 9075",
			      SQL_NTS, conn, NULL );
    wide_strcpy( e2 -> diag_class_origin, e1 -> diag_class_origin );

    if ( subclass_origin == SUBCLASS_ODBC )
    	ansi_to_unicode_copy( e1 -> diag_subclass_origin, (char*) "ODBC 3.0",
			      SQL_NTS, conn, NULL );
    else
    	ansi_to_unicode_copy( e1 -> diag_subclass_origin, (char*) "ISO 9075",
			      SQL_NTS, conn, NULL );
    wide_strcpy( e2 -> diag_subclass_origin, e1 -> diag_subclass_origin );

    ansi_to_unicode_copy( e1 -> diag_connection_name, (char*) "", SQL_NTS,
			  conn, NULL );
    wide_strcpy( e2 -> diag_connection_name, e1 -> diag_connection_name );

    ansi_to_unicode_copy( e1 -> diag_server_name, conn ? conn->dsn : (char*) "", SQL_NTS,
			  conn, NULL );
    wide_strcpy( e2 -> diag_server_name, e1 -> diag_server_name );

    /*
     * the list for SQLError puts both local and driver 
     * errors in the same list
     */

    insert_into_error_list( error_header, e1 );
    insert_into_diag_list( error_header, e2 );
}

void __post_internal_error_ex_w( EHEAD *error_header,
        SQLWCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLWCHAR *message_text,
        int class_origin,
        int subclass_origin )
{
    SQLWCHAR msg[ SQL_MAX_MESSAGE_LENGTH + 32 ];

    /*
     * add our prefix
     */

    ansi_to_unicode_copy(msg, (char*) ERROR_PREFIX, SQL_NTS,
			 __get_connection( error_header ), NULL);
    wide_strcat( msg, message_text );

    __post_internal_error_ex_w_noprefix(
        error_header,
        sqlstate,
        native_error,
        msg,
        class_origin,
        subclass_origin );
}

void __post_internal_error_ex_w_noprefix( EHEAD *error_header,
        SQLWCHAR *sqlstate,
        SQLINTEGER native_error,
        SQLWCHAR *msg,
        int class_origin,
        int subclass_origin )
{
    /*
     * create a error block and add to the lists,
     * leave space for the error prefix
     */

    ERROR *e1, *e2;

    e1 = malloc( sizeof( ERROR ));
    if ( !e1 )
        return;
    e2 = malloc( sizeof( ERROR ));
    if ( !e2 )
    {
        free(e1);
        return;
    }

    memset( e1, 0, sizeof( *e1 ));
    memset( e2, 0, sizeof( *e2 ));

    e1 -> native_error = native_error;
    e2 -> native_error = native_error;
    wide_strcpy( e1 -> sqlstate, sqlstate );
    wide_strcpy( e2 -> sqlstate, sqlstate );
    e1 -> msg = wide_strdup( msg );
    e2 -> msg = wide_strdup( msg );
    e1 -> return_val = SQL_ERROR;
    e2 -> return_val = SQL_ERROR;

    e1 -> diag_column_number_ret = SQL_NO_COLUMN_NUMBER;
    e1 -> diag_row_number_ret = SQL_NO_ROW_NUMBER;
    e1 -> diag_class_origin_ret = SQL_SUCCESS;
    e1 -> diag_subclass_origin_ret = SQL_SUCCESS;
    e1 -> diag_connection_name_ret = SQL_SUCCESS;
    e1 -> diag_server_name_ret = SQL_SUCCESS;
    e1 -> diag_column_number = 0;
    e1 -> diag_row_number = 0;

    e2 -> diag_column_number_ret = SQL_NO_COLUMN_NUMBER;
    e2 -> diag_row_number_ret = SQL_NO_ROW_NUMBER;
    e2 -> diag_class_origin_ret = SQL_SUCCESS;
    e2 -> diag_subclass_origin_ret = SQL_SUCCESS;
    e2 -> diag_connection_name_ret = SQL_SUCCESS;
    e2 -> diag_server_name_ret = SQL_SUCCESS;
    e2 -> diag_column_number = 0;
    e2 -> diag_row_number = 0;

    if ( class_origin == SUBCLASS_ODBC )
        ansi_to_unicode_copy( e1 -> diag_class_origin, (char*) "ODBC 3.0",
							  SQL_NTS, __get_connection( error_header ), NULL );
    else
        ansi_to_unicode_copy( e1 -> diag_class_origin, (char*) "ISO 9075",
							  SQL_NTS, __get_connection( error_header ), NULL );
    wide_strcpy( e2 -> diag_class_origin, e1 -> diag_class_origin );

    if ( subclass_origin == SUBCLASS_ODBC )
        ansi_to_unicode_copy( e1 -> diag_subclass_origin, (char*) "ODBC 3.0",
							  SQL_NTS, __get_connection( error_header ), NULL );
    else
        ansi_to_unicode_copy( e1 ->diag_subclass_origin, (char*) "ISO 9075",
							  SQL_NTS, __get_connection( error_header ), NULL );
    wide_strcpy( e2 -> diag_subclass_origin, e1 -> diag_subclass_origin );

    e1 -> diag_connection_name[ 0 ] = 0;
    e2 -> diag_connection_name[ 0 ] = 0;

    e1 -> diag_server_name[ 0 ] = 0;
    e2 -> diag_server_name[ 0 ] = 0;

    error_header -> return_code = SQL_ERROR;

    /*
     * the list for SQLError puts both local and driver 
     * errors in the same list
     */

    insert_into_error_list( error_header, e1 );
    insert_into_diag_list( error_header, e2 );
}

/*
 * initialise a error header and take note what it belongs to
 */

void setup_error_head( EHEAD *error_header, void *handle, int type )
{
    memset( error_header, 0, sizeof( *error_header ));

    error_header -> owning_handle = handle;
    error_header -> handle_type = type;
}

/*
 * free any resources used but the error headers
 */

void clear_error_head( EHEAD *error_header )
{
    ERROR *cur, *prev;

    prev = NULL;
    cur = error_header -> sql_error_head.error_list_head;

    while( cur )
    {
        prev = cur;

        free( prev -> msg );
        cur = prev -> next;
        free( prev );
    }

    error_header -> sql_error_head.error_list_head = NULL;
    error_header -> sql_error_head.error_list_tail = NULL;

    prev = NULL;
    cur = error_header -> sql_diag_head.error_list_head;

    while( cur )
    {
        prev = cur;

        free( prev -> msg );
        cur = prev -> next;
        free( prev );
    }

    error_header -> sql_diag_head.error_list_head = NULL;
    error_header -> sql_diag_head.error_list_tail = NULL;

    prev = NULL;
    cur = error_header -> sql_diag_head.internal_list_head;

    while( cur )
    {
        prev = cur;

        free( prev -> msg );
        cur = prev -> next;
        free( prev );
    }

    error_header -> sql_diag_head.internal_list_head = NULL;
    error_header -> sql_diag_head.internal_list_tail = NULL;
}

/*
 * get the error values from the handle
 */

void extract_diag_error( int htype,
                            DRV_SQLHANDLE handle,
                            DMHDBC connection,
                            EHEAD *head,
                            int return_code,
                            int save_to_diag )
{
    SQLRETURN ret;
    SQLCHAR *msg;
    SQLCHAR *msg1;
    int msg_len, msg1_len;
    SQLCHAR sqlstate[ 6 ];
    SQLINTEGER native;
    SQLINTEGER rec_number;
    SQLSMALLINT len;
    
    head -> return_code = return_code;
    head -> header_set = 0;
    head -> diag_cursor_row_count_ret = SQL_ERROR;
    head -> diag_dynamic_function_ret = SQL_ERROR;
    head -> diag_dynamic_function_code_ret = SQL_ERROR;
    head -> diag_number_ret = SQL_ERROR;
    head -> diag_row_count_ret = SQL_ERROR;

    rec_number = 1;
    do
    {
        len = 0;

        msg1_len = SQL_MAX_MESSAGE_LENGTH + 1;
        msg1 = malloc( msg1_len );

        ret = SQLGETDIAGREC( connection,
                head -> handle_type,
                handle,
                rec_number,
                sqlstate,
                &native,
                msg1,
                msg1_len,
                &len );

        if ( SQL_SUCCEEDED( ret ))
        {
            ERROR *e = malloc( sizeof( ERROR ));
            SQLWCHAR *tmp;

            /* 
             * make sure we are truncated in the right place
             */

            if ( ret == SQL_SUCCESS_WITH_INFO || len >= SQL_MAX_MESSAGE_LENGTH ) {
                msg1 = realloc( msg1, len + 1 );
                msg1_len = len + 1;

                ret = SQLGETDIAGREC( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        sqlstate,
                        &native,
                        msg1,
                        msg1_len,
                        &len );
            }

            msg_len = len + 32;
            msg = malloc( msg_len );

#ifdef STRICT_ODBC_ERROR
            strcpy((char*) msg, (char*)msg1 );
#else
            strcpy((char*) msg, ERROR_PREFIX );
            strcat((char*) msg, (char*)msg1 );
#endif

            /*
             * add to the SQLError list
             */

            e -> native_error = native;
            tmp = ansi_to_unicode_alloc( sqlstate, SQL_NTS, connection, NULL );
            wide_strcpy( e -> sqlstate, tmp );
            free( tmp );
            e -> msg = ansi_to_unicode_alloc( msg, SQL_NTS, connection, NULL );
            e -> return_val = return_code;

            insert_into_error_list( head, e );

            /*
             * we do this if called from a DM function that goes on to call
             * a further driver function before returning
             */

            if ( save_to_diag )
            {
                SQLWCHAR *tmp;

                e = malloc( sizeof( ERROR ));
                e -> native_error = native;
                tmp = ansi_to_unicode_alloc( sqlstate, SQL_NTS, connection, NULL );
                wide_strcpy( e -> sqlstate, tmp );
                free( tmp );
                e -> msg = ansi_to_unicode_alloc( msg, SQL_NTS, connection, NULL );
                e -> return_val = return_code;

                insert_into_diag_list( head, e );

                /*
                 * now we need to do some extra calls to get
                 * extended info
                 */

                e -> diag_column_number_ret = SQL_ERROR;
                e -> diag_row_number_ret = SQL_ERROR;
                e -> diag_class_origin_ret = SQL_ERROR;
                e -> diag_subclass_origin_ret = SQL_ERROR;
                e -> diag_connection_name_ret = SQL_ERROR;
                e -> diag_server_name_ret= SQL_ERROR;


                if ( head -> handle_type == SQL_HANDLE_STMT )
                {
                    if ( rec_number == 1 )
                    {
                        head -> header_set = 1;
                        head -> diag_cursor_row_count_ret = SQLGETDIAGFIELD( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_CURSOR_ROW_COUNT,
                            &head->diag_cursor_row_count,
                            0,
                            NULL );

                        if ( SQL_SUCCEEDED( head -> diag_dynamic_function_ret = SQLGETDIAGFIELD( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_DYNAMIC_FUNCTION,
                            msg,
                            msg_len,
                            &len )))
                        {
                            tmp = ansi_to_unicode_alloc(msg, SQL_NTS, connection, NULL );
                            wide_strcpy( head->diag_dynamic_function, tmp );
                            free( tmp );
                        }

                        head -> diag_dynamic_function_code_ret = SQLGETDIAGFIELD( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_DYNAMIC_FUNCTION_CODE,
                            &head->diag_dynamic_function_code,
                            0,
                            NULL );

                        head -> diag_number_ret = SQLGETDIAGFIELD( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_NUMBER,
                            &head->diag_number,
                            0,
                            NULL );

                        head -> diag_row_count_ret = SQLGETDIAGFIELD( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_ROW_COUNT,
                            &head->diag_row_count,
                            0,
                            NULL );
                    }

                    e -> diag_column_number_ret = SQLGETDIAGFIELD( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_COLUMN_NUMBER,
                        &e->diag_column_number,
                        0,
                        NULL );

                    e -> diag_row_number_ret = SQLGETDIAGFIELD( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_ROW_NUMBER,
                        &e->diag_row_number,
                        0,
                        NULL );
                }
                else
                {
                    e -> diag_column_number_ret = SQL_ERROR;
                    e -> diag_row_number_ret = SQL_ERROR;
                    e -> diag_class_origin_ret = SQL_ERROR;
                    e -> diag_subclass_origin_ret = SQL_ERROR;
                    e -> diag_connection_name_ret = SQL_ERROR;
                    e -> diag_server_name_ret= SQL_ERROR;

                    if ( SQL_SUCCEEDED( e -> diag_class_origin_ret = SQLGETDIAGFIELD( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_CLASS_ORIGIN,
                        msg,
                        msg_len,
                        &len )))
                    {
                        tmp = ansi_to_unicode_alloc( msg, SQL_NTS, connection, NULL );
                        wide_strcpy( e->diag_class_origin, tmp );
                        free( tmp );
                    }

                    if ( SQL_SUCCEEDED( e -> diag_subclass_origin_ret = SQLGETDIAGFIELD( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_SUBCLASS_ORIGIN,
                        msg,
                        msg_len,
                        &len )))
                    {
                        tmp = ansi_to_unicode_alloc(msg, SQL_NTS, connection, NULL );
                        wide_strcpy( e->diag_subclass_origin, tmp );
                        free( tmp );
                    }

                    if ( SQL_SUCCEEDED( e -> diag_connection_name_ret = SQLGETDIAGFIELD( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_CONNECTION_NAME,
                        msg,
                        msg_len,
                        &len )))
                    {
                        tmp = ansi_to_unicode_alloc( msg, SQL_NTS, connection, NULL );
                        wide_strcpy( e->diag_connection_name, tmp );
                        free( tmp );
                    }

                    if ( SQL_SUCCEEDED( e -> diag_server_name_ret = SQLGETDIAGFIELD( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_SERVER_NAME,
                        msg,
                        msg_len,
                        &len )))
                    {
                        tmp = ansi_to_unicode_alloc( msg, SQL_NTS, connection, NULL );
                        wide_strcpy( e -> diag_server_name, tmp );
                        free( tmp );
                    }
                }
            }
            else
            {
                head -> sql_diag_head.error_count ++;
            }

            rec_number ++;

            /*
             * add to logfile
             */

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                        sqlstate, msg1 );

                dm_log_write_diag( connection -> msg );
            }

            free( msg );
            free( msg1 );
        }
        else {
            free( msg1 );
        }
    }
    while( SQL_SUCCEEDED( ret ));
}

void extract_sql_error( DRV_SQLHANDLE henv,
                            DRV_SQLHANDLE hdbc,
                            DRV_SQLHANDLE hstmt,
                            DMHDBC connection,
                            EHEAD *head, 
                            int return_code )
{
    SQLRETURN ret;
    SQLCHAR msg[ SQL_MAX_MESSAGE_LENGTH + 32 ];
    SQLCHAR msg1[ SQL_MAX_MESSAGE_LENGTH + 1 ];
    SQLCHAR sqlstate[ 6 ];
    SQLINTEGER native;
    SQLSMALLINT len;

    head -> return_code = return_code;
    head -> header_set = 0;
    head -> diag_cursor_row_count_ret = SQL_ERROR;
    head -> diag_dynamic_function_ret = SQL_ERROR;
    head -> diag_dynamic_function_code_ret = SQL_ERROR;
    head -> diag_number_ret = SQL_ERROR;
    head -> diag_row_count_ret = SQL_ERROR;

    do
    {
        len = 0;

        ret = SQLERROR( connection,
                henv, 
                hdbc,
                hstmt,
                sqlstate,
                &native,
                msg1,
                sizeof( msg1 ),
                &len );

        if ( SQL_SUCCEEDED( ret ))
        {
            SQLWCHAR *tmp;
            ERROR *e = malloc( sizeof( ERROR ));

            /*
             * add to the lists, SQLError list first
             */

            /*
             * add our prefix
             */

            /* 
             * make sure we are truncated in the right place
             */

            if ( ret == SQL_SUCCESS_WITH_INFO || len >= SQL_MAX_MESSAGE_LENGTH ) {
                msg1[ SQL_MAX_MESSAGE_LENGTH ] = '\0';
            }

#ifdef STRICT_ODBC_ERROR
            strcpy((char*) msg, (char*)msg1 );
#else
            strcpy((char*) msg, ERROR_PREFIX );
            strcat((char*) msg, (char*)msg1 );
#endif

            e -> native_error = native;
            tmp = ansi_to_unicode_alloc( sqlstate, SQL_NTS, connection, NULL );
            wide_strcpy( e -> sqlstate, tmp );
            free( tmp );
            e -> msg = ansi_to_unicode_alloc( msg, SQL_NTS, connection, NULL );
            e -> return_val = return_code;

            insert_into_error_list( head, e );

            /*
             * SQLGetDiagRec list next
             */

            e = malloc( sizeof( ERROR ));

            e -> diag_column_number_ret = SQL_ERROR;
            e -> diag_row_number_ret = SQL_ERROR;
            e -> diag_class_origin_ret = SQL_ERROR;
            e -> diag_subclass_origin_ret = SQL_ERROR;
            e -> diag_connection_name_ret = SQL_ERROR;
            e -> diag_server_name_ret= SQL_ERROR;

            e -> native_error = native;
            tmp = ansi_to_unicode_alloc( sqlstate, SQL_NTS, connection, NULL );
            wide_strcpy( e -> sqlstate, tmp );
            free( tmp );
            e -> msg = ansi_to_unicode_alloc( msg, SQL_NTS, connection, NULL );
            e -> return_val = return_code;

            insert_into_diag_list( head, e );

            /*
             * add to logfile
             */

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                        sqlstate, msg1 );

                dm_log_write_diag( connection -> msg );
            }
        }
    }
    while( SQL_SUCCEEDED( ret ));
}

void extract_diag_error_w( int htype,
                            DRV_SQLHANDLE handle,
                            DMHDBC connection,
                            EHEAD *head,
                            int return_code,
                            int save_to_diag )
{
    SQLRETURN ret;
    SQLWCHAR *msg;
    SQLWCHAR *msg1;
    SQLWCHAR sqlstate[ 6 ];
    SQLINTEGER native;
    SQLINTEGER rec_number;
    SQLSMALLINT len;

    head -> return_code = return_code;
    head -> header_set = 0;
    head -> diag_cursor_row_count_ret = SQL_ERROR;
    head -> diag_dynamic_function_ret = SQL_ERROR;
    head -> diag_dynamic_function_code_ret = SQL_ERROR;
    head -> diag_number_ret = SQL_ERROR;
    head -> diag_row_count_ret = SQL_ERROR;

    rec_number = 1;
    do
    {
        len = 0;

        msg1 = malloc(( SQL_MAX_MESSAGE_LENGTH + 1 ) * sizeof( SQLWCHAR ));

        ret = SQLGETDIAGRECW( connection,
                head -> handle_type,
                handle,
                rec_number,
                sqlstate,
                &native,
                msg1,
                SQL_MAX_MESSAGE_LENGTH + 1,
                &len );

        if ( SQL_SUCCEEDED( ret ))
        {
            ERROR *e = malloc( sizeof( ERROR ));
#ifndef STRICT_ODBC_ERROR
            SQLWCHAR *tmp;
#endif

            /* 
             * make sure we are truncated in the right place
             */

            if ( ret == SQL_SUCCESS_WITH_INFO || len >= SQL_MAX_MESSAGE_LENGTH ) {
                msg1 = realloc( msg1, ( len + 1 ) * sizeof( SQLWCHAR ));
                ret = SQLGETDIAGRECW( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        sqlstate,
                        &native,
                        msg1,
                        len + 1,
                        &len );
            }

            msg = malloc(( len + 32 ) * sizeof( SQLWCHAR ));

#ifdef STRICT_ODBC_ERROR
            wide_strcpy( msg, msg1 );
#else
            tmp = ansi_to_unicode_alloc((SQLCHAR*) ERROR_PREFIX, SQL_NTS, connection, NULL );
            wide_strcpy( msg, tmp );
            free( tmp );
            wide_strcat( msg, msg1 );
#endif

            /*
             * add to the SQLError list
             */

            e -> native_error = native;
            wide_strcpy( e -> sqlstate, sqlstate );
            e -> msg = wide_strdup( msg );
            e -> return_val = return_code;

            insert_into_error_list( head, e );

            /*
             * we do this if called from a DM function that goes on to call
             * a further driver function before returning
             */

            if ( save_to_diag )
            {
                e = malloc( sizeof( ERROR ));
                e -> native_error = native;
                wide_strcpy( e -> sqlstate, sqlstate );
                e -> msg = wide_strdup( msg );
                e -> return_val = return_code;

                insert_into_diag_list( head, e );

                /*
                 * now we need to do some extra calls to get
                 * extended info
                 */

                e -> diag_column_number_ret = SQL_ERROR;
                e -> diag_row_number_ret = SQL_ERROR;
                e -> diag_class_origin_ret = SQL_ERROR;
                e -> diag_subclass_origin_ret = SQL_ERROR;
                e -> diag_connection_name_ret = SQL_ERROR;
                e -> diag_server_name_ret= SQL_ERROR;

                if ( head -> handle_type == SQL_HANDLE_STMT )
                {
                    if ( rec_number == 1 )
                    {
                        head -> header_set = 1;

                        head -> diag_cursor_row_count_ret = SQLGETDIAGFIELDW( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_CURSOR_ROW_COUNT,
                            &head->diag_cursor_row_count,
                            0,
                            NULL );

                        head -> diag_dynamic_function_ret = SQLGETDIAGFIELDW( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_DYNAMIC_FUNCTION,
                            head->diag_dynamic_function,
                            sizeof( head->diag_dynamic_function ),
                            &len );

                        head -> diag_dynamic_function_code_ret = SQLGETDIAGFIELDW( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_DYNAMIC_FUNCTION_CODE,
                            &head->diag_dynamic_function_code,
                            0,
                            NULL );

                        head -> diag_number_ret = SQLGETDIAGFIELDW( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_NUMBER,
                            &head->diag_number,
                            0,
                            NULL );

                        head -> diag_row_count_ret = SQLGETDIAGFIELDW( connection,
                            head -> handle_type,
                            handle,
                            0,
                            SQL_DIAG_ROW_COUNT,
                            &head->diag_row_count,
                            0,
                            NULL );
                    }

                    e -> diag_column_number_ret = SQLGETDIAGFIELDW( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_COLUMN_NUMBER,
                        &e->diag_column_number,
                        0,
                        NULL );

                    e -> diag_row_number_ret = SQLGETDIAGFIELDW( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_ROW_NUMBER,
                        &e->diag_row_number,
                        0,
                        NULL );
                }
                else
                {
                    e -> diag_column_number_ret = SQL_ERROR;
                    e -> diag_row_number_ret = SQL_ERROR;
                    e -> diag_class_origin_ret = SQL_ERROR;
                    e -> diag_subclass_origin_ret = SQL_ERROR;
                    e -> diag_connection_name_ret = SQL_ERROR;
                    e -> diag_server_name_ret= SQL_ERROR;

                    e -> diag_class_origin_ret = SQLGETDIAGFIELDW( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_CLASS_ORIGIN,
                        e->diag_class_origin,
                        sizeof( e->diag_class_origin ),
                        &len );

                    e -> diag_subclass_origin_ret = SQLGETDIAGFIELDW( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_SUBCLASS_ORIGIN,
                        e->diag_subclass_origin,
                        sizeof( e->diag_subclass_origin ),
                        &len );

                    e -> diag_connection_name_ret = SQLGETDIAGFIELDW( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_CONNECTION_NAME,
                        e->diag_connection_name,
                        sizeof( e->diag_connection_name ),
                        &len );

                    e -> diag_server_name_ret = SQLGETDIAGFIELDW( connection,
                        head -> handle_type,
                        handle,
                        rec_number,
                        SQL_DIAG_SERVER_NAME,
                        e->diag_server_name,
                        sizeof( e->diag_server_name ),
                        &len );
                }
            }
            else
            {
                head -> sql_diag_head.error_count ++;
            }

            rec_number ++;

            /*
             * add to logfile
             */

            if ( log_info.log_flag )
            {
                SQLCHAR *as1, *as2;

                as1 = (SQLCHAR*) unicode_to_ansi_alloc( sqlstate, SQL_NTS, connection, NULL );
                as2 = (SQLCHAR*) unicode_to_ansi_alloc( msg1, SQL_NTS, connection, NULL );

                sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                        as1 ? as1 : (SQLCHAR*)"NULL", as2 ? as2 : (SQLCHAR*)"NULL" );

                if( as1 ) free( as1 );
                if( as2 ) free( as2 );

                dm_log_write_diag( connection -> msg );
            }

            free( msg );
            free( msg1 );
        }
        else {
            free( msg1 );
        }
    }
    while( SQL_SUCCEEDED( ret ));
}

void extract_sql_error_w( DRV_SQLHANDLE henv,
                            DRV_SQLHANDLE hdbc,
                            DRV_SQLHANDLE hstmt,
                            DMHDBC connection,
                            EHEAD *head, 
                            int return_code )
{
    SQLRETURN ret;
    SQLWCHAR msg[ SQL_MAX_MESSAGE_LENGTH + 32 ];
    SQLWCHAR msg1[ SQL_MAX_MESSAGE_LENGTH + 1 ];
    SQLWCHAR sqlstate[ 6 ];
    SQLINTEGER native;
    SQLSMALLINT len;

    head -> return_code = return_code;

    do
    {
        len = 0;

        ret = SQLERRORW( connection,
                henv, 
                hdbc,
                hstmt,
                sqlstate,
                &native,
                msg1,
                SQL_MAX_MESSAGE_LENGTH,
                &len );

        if ( SQL_SUCCEEDED( ret ))
        {
#ifndef STRICT_ODBC_ERROR
            SQLWCHAR *tmp;
#endif

            /*
             * add to the lists, SQLError list first
             */

            ERROR *e = malloc( sizeof( ERROR ));

            /*
             * add our prefix
             */

            /* 
             * make sure we are truncated in the right place
             */

            if ( ret == SQL_SUCCESS_WITH_INFO || len >= SQL_MAX_MESSAGE_LENGTH ) {
                msg1[ SQL_MAX_MESSAGE_LENGTH ] = 0;
            }

#ifdef STRICT_ODBC_ERROR
            wide_strcpy( msg, msg1 );
#else
            tmp = ansi_to_unicode_alloc((SQLCHAR*) ERROR_PREFIX, SQL_NTS, connection, NULL );
            wide_strcpy( msg, tmp );
            free( tmp );
            wide_strcat( msg, msg1 );
#endif

            e -> native_error = native;
            wide_strcpy( e -> sqlstate, sqlstate );
            e -> msg = wide_strdup( msg );
            e -> return_val = return_code;

            insert_into_error_list( head, e );

            /*
             * SQLGetDiagRec list next
             */

            e = malloc( sizeof( ERROR ));
            e -> native_error = native;
            wide_strcpy( e -> sqlstate, sqlstate );
            e -> msg = wide_strdup( msg );
            e -> return_val = return_code;

            insert_into_diag_list( head, e );

            /*
             * add to logfile
             */

            if ( log_info.log_flag )
            {
                SQLCHAR *as1, *as2;

                as1 = (SQLCHAR*) unicode_to_ansi_alloc( sqlstate, SQL_NTS, connection, NULL );
                as2 = (SQLCHAR*) unicode_to_ansi_alloc( msg1, SQL_NTS, connection, NULL );

                sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                        as1 ? as1 : (SQLCHAR*)"NULL", as2 ? as2 : (SQLCHAR*)"NULL");

                if( as1 ) free( as1 );
                if( as2 ) free( as2 );

                dm_log_write_diag( connection -> msg );
            }
        }
    }
    while( SQL_SUCCEEDED( ret ));
}

/*
 * Extract diag information from driver
 */
void extract_error_from_driver( EHEAD * error_handle,
                                DMHDBC hdbc,
                                int ret_code,
                                int save_to_diag )
{
    void (*extracterrorfunc)( DRV_SQLHANDLE, DRV_SQLHANDLE, DRV_SQLHANDLE, DMHDBC, EHEAD *, int ) = 0;
    void (*extractdiagfunc)( int, DRV_SQLHANDLE, DMHDBC, EHEAD*, int, int ) = 0;

    DRV_SQLHANDLE hdbc_drv = SQL_NULL_HDBC;
    DRV_SQLHANDLE hstmt_drv = SQL_NULL_HSTMT;
    DRV_SQLHANDLE handle_diag_extract = __get_driver_handle( error_handle );


    if ( error_handle->handle_type == SQL_HANDLE_ENV )
    {
        return;
    }

    if ( error_handle->handle_type == SQL_HANDLE_DBC )
    {
        hdbc_drv = handle_diag_extract;
    }
    else if ( error_handle->handle_type == SQL_HANDLE_STMT )
    {
        hstmt_drv = handle_diag_extract;
    }

    /* If we have the W functions may as well use them */

    if ( CHECK_SQLGETDIAGFIELDW( hdbc ) &&
        CHECK_SQLGETDIAGRECW( hdbc ))
    {
        extractdiagfunc = extract_diag_error_w;
    }
    else if ( CHECK_SQLERRORW( hdbc ))
    {
        extracterrorfunc = extract_sql_error_w;
    }
    else if ( CHECK_SQLGETDIAGFIELD( hdbc ) &&
             CHECK_SQLGETDIAGREC( hdbc ))
    {
        extractdiagfunc = extract_diag_error;
    }
    else if ( CHECK_SQLERROR( hdbc ))
    {
        extracterrorfunc = extract_sql_error;
    }

    if ( extractdiagfunc )
    {
        extractdiagfunc( error_handle->handle_type,
                handle_diag_extract,
                hdbc,
                error_handle,
                ret_code,
                save_to_diag );
    }
    else if ( error_handle->handle_type != SQL_HANDLE_DESC && extracterrorfunc )
    {
        extracterrorfunc( SQL_NULL_HENV,
                hdbc_drv,
                hstmt_drv,
                hdbc,
                error_handle,
                ret_code );
    }
    else
    {
        __post_internal_error( error_handle,
            ERROR_HY000, "Driver returned SQL_ERROR or SQL_SUCCESS_WITH_INFO but no error reporting API found",
            hdbc->environment->requested_version );
    }
}


/* Return without collecting diag recs from the handle - to be called if the
   DM function is returning before calling the driver function. */
int function_return_nodrv( int level, void *handle, int ret_code) 
{
    if ( level != IGNORE_THREAD )
    {
        thread_release( level, handle );
    }
    return ret_code;
}

/*
 * capture function returns and check error's if necessary
 */

int function_return_ex( int level, void * handle, int ret_code, int save_to_diag, int defer_type )
{
    DMHENV  henv;
    DMHDBC  hdbc;
    DMHSTMT hstmt;
    DMHDESC hdesc;
    EHEAD   *herror = NULL;

    if ( ret_code == SQL_SUCCESS_WITH_INFO || ret_code == SQL_ERROR || ret_code == SQL_NO_DATA)
    {
        /*
         * find what type of handle it is
         */
        henv = handle;

        switch ( henv -> type )
        {
          case HENV_MAGIC:
            {
                /*
                 * do nothing, it must be local
                 */
            }
            break;

          case HDBC_MAGIC:
            {
                hdbc = handle;

                /*
                 * are we connected ?
                 */

                if ( hdbc -> state >= STATE_C4 )
                {
                    herror = &hdbc->error;
                }
            }
            break;

          case HSTMT_MAGIC:
            {
                hstmt = handle;
                herror = &hstmt->error;
                hdbc = hstmt->connection;
            }
            break;

          case HDESC_MAGIC:
            {
                hdesc = handle;
                herror = &hdesc->error;
                hdbc = hdesc->connection;
            }
            break;
        }

        if ( herror )
        {
            /*
             * set defer flag
             */
            herror->defer_extract = ( ret_code == SQL_ERROR ? defer_type >> 1 : defer_type ) & 1;

            if ( herror->defer_extract )
            {
                herror->ret_code_deferred = ret_code;
            }
            else
            {
                extract_error_from_driver( herror, hdbc, ret_code, save_to_diag );
            }
        }
    }

    /*
     * release any threads
     */

    if ( level != IGNORE_THREAD )
    {
        thread_release( level, handle );
    }

    return ret_code;
}

/*
 * clear errors down at the start of a new statement
 * only clear for the ODBC lists, the rest stay
 */

void function_entry( void *handle )
{
    ERROR *cur, *prev;
    EHEAD *error_header;
    DMHENV henv;
    DMHDBC hdbc;
    DMHSTMT hstmt;
    DMHDESC hdesc;
    int version;

    /*
     * find what the handle is
     */

    henv = handle;
    switch( henv -> type )
    {
      case HENV_MAGIC:
        error_header = &henv -> error;
        version = henv -> requested_version;
        break;

      case HDBC_MAGIC:
        hdbc = handle;
        error_header = &hdbc -> error;
        version = hdbc -> environment -> requested_version;
        break;

      case HSTMT_MAGIC:
        hstmt = handle;
        error_header = &hstmt -> error;
        version = hstmt -> connection -> environment -> requested_version;
        break;

      case HDESC_MAGIC:
        hdesc = handle;
        error_header = &hdesc -> error;
        version = hdesc -> connection -> environment -> requested_version;
        break;
    }

    error_header->defer_extract = 0;
    error_header->ret_code_deferred = 0;

    prev = NULL;
    cur = error_header -> sql_diag_head.error_list_head;

    while( cur )
    {
        prev = cur;

        free( prev -> msg );
        cur = prev -> next;
        free( prev );
    }

    error_header -> sql_diag_head.error_list_head = NULL;
    error_header -> sql_diag_head.error_list_tail = NULL;
    error_header -> sql_diag_head.error_count = 0;
    error_header -> header_set = 0;

    prev = NULL;
    cur = error_header -> sql_diag_head.internal_list_head;

    while( cur )
    {
        prev = cur;

        free( prev -> msg );
        cur = prev -> next;
        free( prev );
    }

    error_header -> sql_diag_head.internal_list_head = NULL;
    error_header -> sql_diag_head.internal_list_tail = NULL;
    error_header -> sql_diag_head.internal_count = 0;

    /*
     * if version is SQL_OV_ODBC3 then clear the SQLError list
     * as well
     */

#ifdef USE_OLD_ODBC2_ERROR_CLEARING
    if ( version >= SQL_OV_ODBC3 )
#endif
    {
        prev = NULL;
        cur = error_header -> sql_error_head.error_list_head;

        while( cur )
        {
            prev = cur;

            free( prev -> msg );
            cur = prev -> next;
            free( prev );
        }

        error_header -> sql_error_head.error_list_head = NULL;
        error_header -> sql_error_head.error_list_tail = NULL;
        error_header -> sql_error_head.error_count = 0;
    }
}

void __post_internal_error( EHEAD *error_handle,
        error_id id, char *txt, int connection_mode )
{
    __post_internal_error_api( error_handle, id, txt, connection_mode, 0 );

}

void __post_internal_error_api( EHEAD *error_handle,
        error_id id, char *txt, int connection_mode, int calling_api )
{
    char sqlstate[ 6 ];
    char *message;
    SQLCHAR msg[ SQL_MAX_MESSAGE_LENGTH ];
    SQLRETURN ret = SQL_ERROR;
    int class, subclass;

    class = SUBCLASS_ISO;
    subclass = SUBCLASS_ISO;

    switch( id )
    {
      case ERROR_01000:
        strcpy( sqlstate, "01000" );
        message = "General warning";
        break;

      case ERROR_01004:
        strcpy( sqlstate, "01004" );
        message = "String data, right truncated";
        break;

      case ERROR_01S02:
        strcpy( sqlstate, "01S02" );
        message = "Option value changed";
        subclass = SUBCLASS_ODBC;
        break;

      case ERROR_01S06:
        strcpy( sqlstate, "01S06" );
        message = "Attempt to fetch before the result set returned the first rowset";
        subclass = SUBCLASS_ODBC;
        break;

      case ERROR_07005:
        strcpy( sqlstate, "07005" );
        message = "Prepared statement not a cursor-specification";
        break;

      case ERROR_07009:
        switch( calling_api )
        {
          case SQL_API_SQLDESCRIBEPARAM:
          case SQL_API_SQLBINDPARAMETER:
          case SQL_API_SQLSETPARAM:
                if ( connection_mode >= SQL_OV_ODBC3 )
                    strcpy( sqlstate, "07009" );
                else
                    strcpy( sqlstate, "S1093" );
                message = "Invalid parameter index";
                break;

          default:
                if ( connection_mode >= SQL_OV_ODBC3 )
                    strcpy( sqlstate, "07009" );
                else
                    strcpy( sqlstate, "S1002" );
                message = "Invalid descriptor index";
                break;
        }
        break;

      case ERROR_08002:
        strcpy( sqlstate, "08002" );
        message = "Connection in use";
        break;

      case ERROR_08003:
        strcpy( sqlstate, "08003" );
        message = "Connection not open";
        break;

      case ERROR_24000:
        strcpy( sqlstate, "24000" );
        message = "Invalid cursor state";
        break;

      case ERROR_25000:
        message = "Invalid transaction state";
        strcpy( sqlstate, "25000" );
        break;

      case ERROR_25S01:
        message = "Transaction state unknown";
        strcpy( sqlstate, "25S01" );
        subclass = SUBCLASS_ODBC;
        break;

      case ERROR_S1000:
        message = "General error";
        strcpy( sqlstate, "S1000" );
        break;

      case ERROR_S1003:
        message = "Program type out of range";
        strcpy( sqlstate, "S1003" );
        break;

      case ERROR_S1010:
        message = "Function sequence error";
        strcpy( sqlstate, "S1010" );
        break;

      case ERROR_S1011:
        message = "Operation invalid at this time";
        strcpy( sqlstate, "S1011" );
        break;

      case ERROR_S1107:
        message = "Row value out of range";
        strcpy( sqlstate, "S1107" );
        break;

      case ERROR_S1108:
        message = "Concurrency option out of range";
        strcpy( sqlstate, "S1108" );
        break;

      case ERROR_S1C00:
        message = "Driver not capable";
        strcpy( sqlstate, "S1C00" );
        break;

      case ERROR_HY001:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY001" );
        else
            strcpy( sqlstate, "S1011" );
        message = "Memory allocation error";
        break;

      case ERROR_HY003:
        if ( connection_mode >= SQL_OV_ODBC3 )
        {
            strcpy( sqlstate, "HY003" );
            /* Windows DM returns " Program type out of range" instead of 
               "Invalid application buffer type" */
            message = "Program type out of range";
        }
        else
        {
            strcpy( sqlstate, "S1003" );
        message = "Invalid application buffer type";
        }
        break;

      case ERROR_HY004:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY004" );
        else
            strcpy( sqlstate, "S1004" );
        message = "Invalid SQL data type";
        break;

      case ERROR_HY007:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY007" );
        else
            strcpy( sqlstate, "S1007" );
        message = "Associated statement is not prepared";
        break;

      case ERROR_HY009:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY009" );
        else
            strcpy( sqlstate, "S1009" );
        message = "Invalid use of null pointer";
        break;

      case ERROR_HY010:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY010" );
        else
            strcpy( sqlstate, "S1010" );
        message = "Function sequence error";
        break;

      case ERROR_HY011:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY011" );
        else
            strcpy( sqlstate, "S1011" );
        message = "Attribute cannot be set now";
        break;

      case ERROR_HY012:
	    if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY012" );
        else
            strcpy( sqlstate, "S1012" );
        message = "Invalid transaction operation code";
        break;

      case ERROR_HY013:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY013" );
        else
            strcpy( sqlstate, "S1013" );
        message = "Memory management error";
        break;

      case ERROR_HY017:
        strcpy( sqlstate, "HY017" );
        message = "Invalid use of an automatically allocated descriptor handle";
        break;

      case ERROR_HY024:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY024" );
        else
            strcpy( sqlstate, "S1009" );
        message = "Invalid attribute value";
        break;

      case ERROR_HY090:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY090" );
        else
            strcpy( sqlstate, "S1090" );
        message = "Invalid string or buffer length";
        break;

      case ERROR_HY092:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY092" );
        else
            strcpy( sqlstate, "S1092" );
        message = "Invalid attribute/option identifier";
        break;

      case ERROR_HY095:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY095" );
        else
            strcpy( sqlstate, "S1095" );
        message = "Function type out of range";
        break;
        
      case ERROR_HY097:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY097" );
        else
            strcpy( sqlstate, "S1097" );
        message = "Column type out of range";
        break;

      case ERROR_HY098:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY098" );
        else
            strcpy( sqlstate, "S1098" );
        message = "Scope type out of range";
        break;

      case ERROR_HY099:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY099" );
        else
            strcpy( sqlstate, "S1099" );
        message = "Nullable type out of range";
        break;

      case ERROR_HY100:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY100" );
        else
            strcpy( sqlstate, "S1100" );
        message = "Uniqueness option type out of range";
        break;

      case ERROR_HY101:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY101" );
        else
            strcpy( sqlstate, "S1101" );
        message = "Accuracy option type out of range";
        break;

      case ERROR_HY103:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY103" );
        else
            strcpy( sqlstate, "S1103" );
        message = "Invalid retrieval code";
        break;

      case ERROR_HY105:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY105" );
        else
            strcpy( sqlstate, "S1105" );
        message = "Invalid parameter type";
        break;

      case ERROR_HY106:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY106" );
        else
            strcpy( sqlstate, "S1106" );
        message = "Fetch type out of range";
        break;

      case ERROR_HY110:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY110" );
        else
            strcpy( sqlstate, "S1110" );
        message = "Invalid driver completion";
        break;

      case ERROR_HY111:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY111" );
        else
            strcpy( sqlstate, "S1111" );
        message = "Invalid bookmark value";
        break;

      case ERROR_HYC00:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HYC00" );
        else
            strcpy( sqlstate, "S1C00" );
        message = "Optional feature not implemented";
        break;

      case ERROR_IM001:
        strcpy( sqlstate, "IM001" );
        message = "Driver does not support this function";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_IM002:
        strcpy( sqlstate, "IM002" );
        message = "Data source name not found and no default driver specified";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_IM003:
        strcpy( sqlstate, "IM003" );
        message = "Specified driver could not be loaded";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_IM004:
        strcpy( sqlstate, "IM004" );
        message = "Driver's SQLAllocHandle on SQL_HANDLE_HENV failed";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_IM005:
        strcpy( sqlstate, "IM005" );
        message = "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_IM010:
        strcpy( sqlstate, "IM010" );
        message = "Data source name too long";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_IM011:
        strcpy( sqlstate, "IM011" );
        message = "Driver name too long";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_IM012:
        strcpy( sqlstate, "IM012" );
        message = "DRIVER keyword syntax error";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_SL004:
        strcpy( sqlstate, "SL004" );
        message = "Result set not generated by a SELECT statement";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_SL009:
        strcpy( sqlstate, "SL009" );
        message = "No columns were bound prior to calling SQLFetch or SQLFetchScroll";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_SL010:
        strcpy( sqlstate, "SL010" );
        message = "SQLBindCol returned SQL_ERROR on a attempt to bind a internal buffer";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_SL008:
        strcpy( sqlstate, "SL008" );
        message = "SQLGetData is not allowed on a forward only (non-buffered) cursor";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

      case ERROR_HY000:
        if ( connection_mode >= SQL_OV_ODBC3 )
            strcpy( sqlstate, "HY000" );
        else
            strcpy( sqlstate, "S1000" );
        message = "General error";
        break;

      case ERROR_HYT02:
        strcpy( sqlstate, "HYT02");
        message = "Connection pool at capacity and the wait has timed out";
        subclass = SUBCLASS_ODBC;
        class = SUBCLASS_ODBC;
        break;

	  default:
        strcpy( sqlstate, "?????" );
        message = "Unknown";
    }

    if ( txt )
        message = txt;

    strcpy((char*) msg, DM_ERROR_PREFIX );
    strncat((char*) msg, message, sizeof(msg) - sizeof(DM_ERROR_PREFIX) );

    error_handle -> return_code = ret;

    __post_internal_error_ex( error_handle,
        (SQLCHAR*)sqlstate, 0, msg, class, subclass );
}

/*
 * open a log file
 */

void  dm_log_open( char *program_name,
        char *log_file_name, int pid_logging )
{
    if ( log_info.program_name )
    {
        free( log_info.program_name );
    }
    if ( log_info.log_file_name )
    {
        free( log_info.log_file_name );
    }
    log_info.program_name = strdup( program_name );
    log_info.log_file_name = strdup( log_file_name );
    log_info.log_flag = 1;

    /*
     * are we doing perprocess logging
     */

    log_info.pid_logging = pid_logging;
    log_info.ref_count++;
}

void dm_log_write( char *function_name, int line, int type, int severity,
        char *message )
{
    FILE *fp;
    char tmp[ 24 ];

    if ( !log_info.log_flag && !ODBCSharedTraceFlag )
        return;

    if ( log_info.pid_logging )
    {
        char file_name[ 256 ], str[ 20 ];

        if ( !log_info.log_file_name )
        {
            strcpy( file_name, "/tmp/sql.log" );
        }
        else
        {
            sprintf( file_name, "%s/%s", log_info.log_file_name, __get_pid((SQLCHAR*) str ));
        }
        fp = uo_fopen( file_name, "a" );

        /*
         * Change the mode to be rw for all
         */
        chmod( file_name, 0666 );
    }
    else
    {
        if ( !log_info.log_file_name )
        {
            fp = uo_fopen( "/tmp/sql.log", "a" );
        }
        else
        {
            fp = uo_fopen( log_info.log_file_name, "a" );
        }
    }

    if ( fp )
    {
		char tstamp_str[ 128 ];

#if defined( HAVE_GETTIMEOFDAY ) && defined( HAVE_SYS_TIME_H )
		{
			struct timeval tv;
			void* tz = NULL;

			gettimeofday( &tv, tz );

			sprintf( tstamp_str, "[%ld.%06ld]", tv.tv_sec, tv.tv_usec );
		}
#elif defined( HAVE_FTIME ) && defined( HAVE_SYS_TIMEB_H )
		{
			struct timeb tp;

			ftime( &tp );

			sprintf( tstamp_str, "[%ld.%03d]", tp.time, tp.millitm );
		}
#elif defined( DHAVE_TIME ) && defined( HAVE_TIME_H ) 
		{
			time_t tv;

			time( &tv );
			sprintf( tstamp_str, "[%ld]", tv );
		}
#else
		tstamp_str[ 0 ] = '\0';
#endif
        if ( !log_info.program_name )
        {
            uo_fprintf( fp, "[ODBC][%s]%s[%s][%d]%s\n", __get_pid((SQLCHAR*) tmp ), 
					tstamp_str,
                    function_name, line, message );
        }
        else
        {
            uo_fprintf( fp, "[%s][%s]%s[%s][%d]%s\n", log_info.program_name,
                __get_pid((SQLCHAR*) tmp ), 
				tstamp_str,
				function_name, line, message );
        }

        uo_fclose( fp );
    }
}

void dm_log_write_diag( char *message )
{
    FILE *fp;

    if ( !log_info.log_flag && !ODBCSharedTraceFlag )
        return;

    if ( log_info.pid_logging )
    {
        char file_name[ 256 ], str[ 20 ];

        if ( !log_info.log_file_name )
        {
            strcpy( file_name, "/tmp/sql.log" );
        }
        else
        {
            sprintf( file_name, "%s/%s", log_info.log_file_name, __get_pid((SQLCHAR*) str ));
        }
        fp = uo_fopen( file_name, "a" );

        /*
         * Change the mode to be rw for all
         */
        chmod( file_name, 0666 );
    }
    else
    {
        if ( !log_info.log_file_name )
        {
            fp = uo_fopen( "/tmp/sql.log", "a" );
        }
        else
        {
            fp = uo_fopen( log_info.log_file_name, "a" );
        }
    }

    if ( fp )
    {
        uo_fprintf( fp, "%s\n\n", message );

        uo_fclose( fp );
    }
}

void dm_log_close( void )
{
    if ( !log_info.ref_count )
        return;

    log_info.ref_count--;
    if ( !log_info.ref_count )
    {
        free( log_info.program_name );
        free( log_info.log_file_name );
        log_info.program_name = NULL;
        log_info.log_file_name = NULL;
        log_info.log_flag = 0;
    }
}
