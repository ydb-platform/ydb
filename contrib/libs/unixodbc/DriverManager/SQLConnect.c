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
 * $Id: SQLConnect.c,v 1.66 2009/05/15 15:23:56 lurcher Exp $
 *
 * $Log: SQLConnect.c,v $
 * Revision 1.66  2009/05/15 15:23:56  lurcher
 * Fix pooled connection thread problems
 *
 * Revision 1.65  2009/03/26 14:39:21  lurcher
 * Fix typo in isql
 *
 * Revision 1.64  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.63  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.62  2009/01/13 10:54:13  lurcher
 * Allow setting of default Threading level
 *
 * Revision 1.61  2008/11/24 12:44:23  lurcher
 * Try and tidu up the connection version checking
 *
 * Revision 1.60  2008/09/29 14:02:43  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.59  2008/08/29 08:01:38  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.58  2008/06/17 16:14:13  lurcher
 * Fix for iconv memory leak and some fixes for CYGWIN
 *
 * Revision 1.57  2008/05/30 12:04:55  lurcher
 * Fix a couple of build problems and get ready for the next release
 *
 * Revision 1.56  2007/07/13 14:01:18  lurcher
 * Fix problem when not using iconv
 *
 * Revision 1.55  2007/03/13 10:35:38  lurcher
 * clear the iconv handles after use
 *
 * Revision 1.54  2007/03/07 22:53:29  lurcher
 * Fix pooling iconv leak, and try and allow the W entry point in a setup lib to be used
 *
 * Revision 1.53  2007/01/02 10:27:50  lurcher
 * Fix descriptor leak with unicode only driver
 *
 * Revision 1.52  2006/10/13 08:43:10  lurcher
 *
 *
 * Remove debug printf
 *
 * Revision 1.51  2006/06/28 08:08:41  lurcher
 * Add timestamp with timezone to Postgres7.1 driver
 *
 * Revision 1.50  2006/04/11 10:22:56  lurcher
 * Fix a data type check
 *
 * Revision 1.49  2005/11/08 09:37:10  lurcher
 * Allow the driver and application to have different length handles
 *
 * Revision 1.48  2005/10/06 08:50:58  lurcher
 * Fix problem with SQLDrivers not returning first entry
 *
 * Revision 1.47  2005/07/08 12:11:23  lurcher
 *
 * Fix a cursor lib problem (it was broken if you did metadata calls)
 * Alter the params to SQLParamOptions to use SQLULEN
 *
 * Revision 1.46  2005/05/24 16:51:57  lurcher
 * Fix potential for the driver to no have its handle closed
 *
 * Revision 1.45  2005/03/01 14:24:40  lurcher
 * Change DontDLClose default
 *
 * Revision 1.44  2005/02/01 10:24:23  lurcher
 * Cope if SHLIBEXT is not set
 *
 * Revision 1.43  2004/12/20 18:06:13  lurcher
 * Fix small typo in SQLConnect
 *
 * Revision 1.42  2004/09/22 09:13:38  lurcher
 * Replaced crypt auth in postgres with md5 for 7.1 Postgres driver
 *
 * Revision 1.41  2004/09/08 16:38:53  lurcher
 *
 * Get ready for a 2.2.10 release
 *
 * Revision 1.40  2004/07/25 00:42:02  peteralexharvey
 * for OS2 port
 *
 * Revision 1.39  2004/07/24 17:55:37  lurcher
 * Sync up CVS
 *
 * Revision 1.38  2004/06/16 14:42:03  lurcher
 *
 *
 * Fix potential corruption with threaded use and SQLEndTran
 *
 * Revision 1.37  2004/05/10 15:58:52  lurcher
 *
 * Stop the driver manager calling free handle twice
 *
 * Revision 1.36  2004/04/01 12:34:26  lurcher
 *
 * Fix minor memory leak
 * Add support for 64bit HPUX
 *
 * Revision 1.35  2004/02/26 15:52:03  lurcher
 *
 * Fix potential to call SQLFreeEnv in driver twice
 * Set default value if call to SQLGetPrivateProfileString fails because
 * the odbcinst.ini file is not found, and can't be created
 *
 * Revision 1.34  2004/02/18 15:47:44  lurcher
 *
 * Fix a leak in the iconv code
 *
 * Revision 1.33  2004/02/17 11:05:35  lurcher
 *
 * 2.2.8 release
 *
 * Revision 1.32  2004/02/02 10:10:45  lurcher
 *
 * Fix some connection pooling problems
 * Include sqlucode in sqlext
 *
 * Revision 1.31  2003/12/01 16:37:17  lurcher
 *
 * Fix a bug in SQLWritePrivateProfileString
 *
 * Revision 1.30  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.29  2003/10/06 15:43:46  lurcher
 *
 * Fix cursor lib to work with SQLFetch as well as the other fetch calls
 * Update README.OSX to detail building the cursor lib
 *
 * Revision 1.28  2003/09/08 15:34:29  lurcher
 *
 * A couple of small but perfectly formed fixes
 *
 * Revision 1.27  2003/08/15 17:34:43  lurcher
 *
 * Remove some unneeded ODBC2->3 attribute conversions
 *
 * Revision 1.26  2003/08/08 11:14:21  lurcher
 *
 * Fix UNICODE problem in SQLDriverConnectW
 *
 * Revision 1.25  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.24  2003/02/26 13:05:42  lurcher
 *
 * Update for new autoconf
 *
 * Revision 1.23  2003/02/25 13:28:28  lurcher
 *
 * Allow errors on the drivers AllocHandle to be reported
 * Fix a problem that caused errors to not be reported in the log
 * Remove a redundant line from the spec file
 *
 * Revision 1.22  2003/02/06 18:13:01  lurcher
 *
 * Another HP_UX twiddle
 *
 * Revision 1.21  2003/02/06 12:58:25  lurcher
 *
 * Fix a speeling problem :-)
 *
 * Revision 1.20  2002/12/20 11:36:46  lurcher
 *
 * Update DMEnvAttr code to allow setting in the odbcinst.ini entry
 *
 * Revision 1.19  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.18  2002/11/19 18:52:27  lurcher
 *
 * Alter the cursor lib to not require linking to the driver manager.
 *
 * Revision 1.17  2002/11/13 15:59:20  lurcher
 *
 * More VMS changes
 *
 * Revision 1.16  2002/08/27 08:49:02  lurcher
 *
 * New version number and fix for cursor lib loading
 *
 * Revision 1.15  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.14  2002/08/12 13:17:52  lurcher
 *
 * Replicate the way the MS DM handles loading of driver libs, and allocating
 * handles in the driver. usage counting in the driver means that dlopen is
 * only called for the first use, and dlclose for the last. AllocHandle for
 * the driver environment is only called for the first time per driver
 * per application environment.
 *
 * Revision 1.13  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.12  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.11  2002/07/12 09:01:37  lurcher
 *
 * Fix problem, with SAPDB where if the connection specifies ODBC 2, the
 * don't make use of the ODBC 3 method of SQLGetFunctions
 *
 * Revision 1.10  2002/07/04 17:27:56  lurcher
 *
 * Small bug fixes
 *
 * Revision 1.8  2002/05/24 12:42:49  lurcher
 *
 * Alter NEWS and ChangeLog to match their correct usage
 * Additional UNICODE tweeks
 *
 * Revision 1.7  2002/03/26 09:35:46  lurcher
 *
 * Extend naming of cursor lib to work on non linux platforms
 * (it expected a .so)
 *
 * Revision 1.6  2002/02/21 18:44:09  lurcher
 *
 * Fix bug on 32 bit platforms without long long support
 * Add option to set environment variables from the ini file
 *
 * Revision 1.5  2002/01/21 18:00:51  lurcher
 *
 * Assorted fixed and changes, mainly UNICODE/bug fixes
 *
 * Revision 1.4  2001/12/19 15:55:53  lurcher
 *
 * Add option to disable calling of SQLGetFunctions in driver
 *
 * Revision 1.3  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.2  2001/11/21 16:58:25  lurcher
 *
 * Assorted fixes to make the MAX OSX build work nicer
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.31  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.30  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.29  2001/08/03 15:19:00  nick
 *
 * Add changes to set values before connect
 *
 * Revision 1.28  2001/07/31 12:03:46  nick
 *
 * Fix how the DM gets the CLI year for SQLGetInfo
 * Fix small bug in strncasecmp
 *
 * Revision 1.27  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.26  2001/06/25 12:55:15  nick
 *
 * Fix threading problem with multiple ENV's
 *
 * Revision 1.25  2001/06/13 11:23:11  nick
 *
 * Fix a couple of portability problems
 *
 * Revision 1.24  2001/05/31 16:05:55  nick
 *
 * Fix problems with postgres closing local sockets
 * Make odbctest build with QT 3 (it doesn't work due to what I think are bugs
 * in QT 3)
 * Fix a couple of problems in the cursor lib
 *
 * Revision 1.23  2001/05/23 11:44:44  nick
 *
 * Fix typo
 *
 * Revision 1.22  2001/05/09 11:56:47  nick
 *
 * Add support for libtool 1.4
 *
 * Revision 1.21  2001/04/18 15:03:37  nick
 *
 * Fix problem when going to DB2 unicode driver
 *
 * Revision 1.20  2001/04/16 22:35:10  nick
 *
 * More tweeks to the AutoTest code
 *
 * Revision 1.19  2001/04/16 15:41:24  nick
 *
 * Fix some problems calling non existing error funcs
 *
 * Revision 1.18  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.17  2001/04/04 11:30:38  nick
 *
 * Fix a memory leak in Postgre7.1
 * Fix a problem with timed out pooled connections
 * Add time to live option for pooled connections
 *
 * Revision 1.16  2001/04/03 16:34:12  nick
 *
 * Add support for strangly broken unicode drivers
 *
 * Revision 1.15  2001/03/30 08:35:39  nick
 *
 * Fix a couple of pooling problems
 *
 * Revision 1.14  2001/03/02 14:24:23  nick
 *
 * Fix thread detection for Solaris
 *
 * Revision 1.13  2001/02/12 11:20:22  nick
 *
 * Add supoort for calling SQLDriverLoad and SQLDriverUnload
 *
 * Revision 1.12  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.11  2000/12/18 13:02:13  nick
 *
 * More buf fixes
 *
 * Revision 1.10  2000/12/17 11:02:37  nick
 *
 * Fix extra '*'
 *
 * Revision 1.9  2000/12/17 11:00:32  nick
 *
 * Add thread safe bits to pooling
 *
 * Revision 1.8  2000/12/14 18:10:19  nick
 *
 * Add connection pooling
 *
 * Revision 1.7  2000/11/29 11:26:18  nick
 *
 * Add unicode bits
 *
 * Revision 1.6  2000/11/22 18:35:43  nick
 *
 * Check input handle before touching output handle
 *
 * Revision 1.5  2000/11/22 17:19:32  nick
 *
 * Fix tracing problem in SQLConnect
 *
 * Revision 1.4  2000/11/14 10:15:27  nick
 *
 * Add test for localtime_r
 *
 * Revision 1.3  2000/10/25 08:58:55  nick
 *
 * Fix crash when null server and SQL_NTS is passed in
 *
 * Revision 1.2  2000/10/13 15:18:49  nick
 *
 * Change string length parameter from SQLINTEGER to SQLSMALLINT
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.30  2000/07/28 14:57:29  ngorham
 *
 * Don't copy the function pointers for ColAttribute, ColAttributes just
 * set can_supply
 *
 * Revision 1.29  2000/06/27 17:34:09  ngorham
 *
 * Fix a problem when the second part of the connect failed a seg fault
 * was generated in the error reporting
 *
 * Revision 1.28  2001/05/26 19:11:37  ngorham
 *
 * Add SQLCopyDesc functionality and fix bug that was stopping messages
 * coming out of SQLConnect
 *
 * Revision 1.27  2000/05/21 21:49:19  ngorham
 *
 * Assorted fixes
 *
 * Revision 1.26  2000/04/27 20:49:03  ngorham
 *
 * Fixes to work with Star Office 5.2
 *
 * Revision 1.25  2000/04/19 22:00:57  ngorham
 *
 * We can always supply SQLGetFunctions
 *
 * Revision 1.24  2000/03/11 15:55:47  ngorham
 *
 * A few more changes and bug fixes (see NEWS)
 *
 * Revision 1.23  2000/02/25 00:02:00  ngorham
 *
 * Add a patch to support IBM DB2, and Solaris threads
 *
 * Revision 1.22  2000/02/02 07:55:20  ngorham
 *
 * Add flag to disable SQLFetch -> SQLExtendedFetch mapping
 *
 * Revision 1.21  1999/12/28 15:05:00  ngorham
 *
 * Fix bug that caused StarOffice to fail. A SQLConnect, SQLDisconnect,
 * followed by another SQLConnect on the same DBC would fail.
 *
 * Revision 1.20  1999/12/17 09:40:30  ngorham
 *
 * Change a error return from HY004 to IM004
 *
 * Revision 1.19  1999/12/14 19:02:25  ngorham
 *
 * Mask out the password fields in the logging
 *
 * Revision 1.18  1999/11/13 23:40:58  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.17  1999/11/10 03:51:33  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.16  1999/10/24 23:54:17  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.15  1999/10/14 06:49:24  ngorham
 *
 * Remove @all_includes@ from Drivers/MiniSQL/Makefile.am
 *
 * Revision 1.14  1999/10/09 00:15:58  ngorham
 *
 * Add mapping from SQL_TYPE_X to SQL_X and SQL_C_TYPE_X to SQL_C_X
 * when the driver is a ODBC 2 one
 *
 * Revision 1.13  1999/10/07 20:39:25  ngorham
 *
 * Added .cvsignore files and fixed a couple of bugs in the DM
 *
 * Revision 1.12  1999/10/06 07:10:46  ngorham
 *
 * As the book says check dlerror after a dl func
 *
 * Revision 1.11  1999/10/06 07:01:25  ngorham
 *
 * Added more support for non linux platforms
 *
 * Revision 1.10  1999/09/26 18:55:03  ngorham
 *
 * Fixed a problem where the cursor lib was being used by default
 *
 * Revision 1.9  1999/09/24 22:54:52  ngorham
 *
 * Fixed some unchanged dlopen,dlsym,dlclose functions
 *
 * Revision 1.8  1999/09/21 22:34:24  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.7  1999/09/20 21:46:49  ngorham
 *
 * Added support for libtld dlopen replace
 *
 * Revision 1.6  1999/09/19 22:24:33  ngorham
 *
 * Added support for the cursor library
 *
 * Revision 1.5  1999/08/03 21:47:39  shandyb
 * Moving to automake: changed files in DriverManager
 *
 * Revision 1.4  1999/07/10 21:10:15  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:07  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:54  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:05  sShandyb
 * first go at it
 *
 * Revision 1.4  1999/06/07 01:29:30  pharvey
 * *** empty log message ***
 *
 * Revision 1.3  1999/06/02 20:12:10  ngorham
 *
 * Fixed botched log entry, and removed the dos \r from the sql header files.
 *
 * Revision 1.2  1999/06/02 19:57:20  ngorham
 *
 * Added code to check if a attempt is being made to compile with a C++
 * Compiler, and issue a message.
 * Start work on the ODBC2-3 conversions.
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.7  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.6  1999/05/04 22:41:12  nick
 * and another night ends
 *
 * Revision 1.5  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.4  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.3  1999/04/29 21:40:58  nick
 * End of another night :-)
 *
 * Revision 1.2  1999/04/29 20:47:37  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#elif defined(HAVE_TIME_H)
#include <time.h>
#endif
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLConnect.c,v $ $Revision: 1.66 $";

#ifdef __OS2__
#define CURSOR_LIB	"ODBCCR"
#else
#define CURSOR_LIB      "libodbccr"
#endif

#ifndef CURSOR_LIB_VER
#ifdef  DEFINE_CURSOR_LIB_VER
#define CURSOR_LIB_VER  "2"
#endif
#endif

/*
 * structure to contain the loaded lib entry points
 */

static struct driver_func  template_func[] =
{
    /* 00 */ { SQL_API_SQLALLOCCONNECT,      "SQLAllocConnect", (void*)SQLAllocConnect },
    /* 01 */ { SQL_API_SQLALLOCENV,          "SQLAllocEnv", (void*)SQLAllocEnv  },
    /* 02 */ { SQL_API_SQLALLOCHANDLE,       "SQLAllocHandle", (void*)SQLAllocHandle },
    /* 03 */ { SQL_API_SQLALLOCSTMT,         "SQLAllocStmt", (void*)SQLAllocStmt },
    /* 04 */ { SQL_API_SQLALLOCHANDLESTD,    "SQLAllocHandleStd", (void*)SQLAllocHandleStd },
    /* 05 */ { SQL_API_SQLBINDCOL,           "SQLBindCol", (void*)SQLBindCol },
    /* 06 */ { SQL_API_SQLBINDPARAM,         "SQLBindParam", (void*)SQLBindParam },
    /* 07 */ { SQL_API_SQLBINDPARAMETER,     "SQLBindParameter", (void*)SQLBindParameter },
    /* 08 */ { SQL_API_SQLBROWSECONNECT,     "SQLBrowseConnect", 
                (void*)SQLBrowseConnect, (void*)SQLBrowseConnectW },
    /* 09 */ { SQL_API_SQLBULKOPERATIONS,    "SQLBulkOperations", (void*)SQLBulkOperations },
    /* 10 */ { SQL_API_SQLCANCEL,            "SQLCancel", (void*)SQLCancel },
    /* 11 */ { SQL_API_SQLCLOSECURSOR,       "SQLCloseCursor", (void*)SQLCloseCursor },
    /* 12 */ { SQL_API_SQLCOLATTRIBUTE,      "SQLColAttribute", 
                (void*)SQLColAttribute, (void*)SQLColAttributeW },
    /* 13 */ { SQL_API_SQLCOLATTRIBUTES,     "SQLColAttributes", 
                (void*)SQLColAttributes, (void*)SQLColAttributesW },
    /* 14 */ { SQL_API_SQLCOLUMNPRIVILEGES,  "SQLColumnPrivileges", 
                (void*)SQLColumnPrivileges, (void*)SQLColumnPrivilegesW },
    /* 15 */ { SQL_API_SQLCOLUMNS,           "SQLColumns", 
                (void*)SQLColumns, (void*)SQLColumnsW },
    /* 16 */ { SQL_API_SQLCONNECT,           "SQLConnect", 
                (void*)SQLConnect, (void*)SQLConnectW },
    /* 17 */ { SQL_API_SQLCOPYDESC,          "SQLCopyDesc", (void*)SQLCopyDesc },
    /* 18 */ { SQL_API_SQLDATASOURCES,       "SQLDataSources", 
                (void*)SQLDataSources, (void*)SQLDataSourcesW },
    /* 19 */ { SQL_API_SQLDESCRIBECOL,       "SQLDescribeCol", 
                (void*)SQLDescribeCol, (void*)SQLDescribeColW },
    /* 20 */ { SQL_API_SQLDESCRIBEPARAM,     "SQLDescribeParam", (void*)SQLDescribeParam },
    /* 21 */ { SQL_API_SQLDISCONNECT,        "SQLDisconnect", (void*)SQLDisconnect },
    /* 22 */ { SQL_API_SQLDRIVERCONNECT,     "SQLDriverConnect", 
                (void*)SQLDriverConnect, (void*)SQLDriverConnectW },
    /* 23 */ { SQL_API_SQLDRIVERS,           "SQLDrivers", 
                (void*)SQLDrivers, (void*)SQLDriversW },
    /* 24 */ { SQL_API_SQLENDTRAN,           "SQLEndTran", (void*)SQLEndTran },
    /* 25 */ { SQL_API_SQLERROR,             "SQLError", 
                (void*)SQLError, (void*)SQLErrorW },
    /* 26 */ { SQL_API_SQLEXECDIRECT,        "SQLExecDirect", 
                (void*)SQLExecDirect, (void*)SQLExecDirectW },
    /* 27 */ { SQL_API_SQLEXECUTE,           "SQLExecute", (void*)SQLExecute },
    /* 28 */ { SQL_API_SQLEXTENDEDFETCH,     "SQLExtendedFetch", (void*)SQLExtendedFetch },
    /* 29 */ { SQL_API_SQLFETCH,             "SQLFetch", (void*)SQLFetch },
    /* 30 */ { SQL_API_SQLFETCHSCROLL,       "SQLFetchScroll", (void*)SQLFetchScroll },
    /* 31 */ { SQL_API_SQLFOREIGNKEYS,       "SQLForeignKeys", 
                (void*)SQLForeignKeys, (void*)SQLForeignKeysW },
    /* 32 */ { SQL_API_SQLFREEENV,           "SQLFreeEnv", (void*)SQLFreeEnv },
    /* 33 */ { SQL_API_SQLFREEHANDLE,        "SQLFreeHandle", (void*)SQLFreeHandle },
    /* 34 */ { SQL_API_SQLFREESTMT,          "SQLFreeStmt", (void*)SQLFreeStmt },
    /* 35 */ { SQL_API_SQLFREECONNECT,       "SQLFreeConnect", (void*)SQLFreeConnect },
    /* 36 */ { SQL_API_SQLGETCONNECTATTR,    "SQLGetConnectAttr", 
                (void*)SQLGetConnectAttr, (void*)SQLGetConnectAttrW },
    /* 37 */ { SQL_API_SQLGETCONNECTOPTION,  "SQLGetConnectOption", 
                (void*)SQLGetConnectOption, (void*)SQLGetConnectOptionW },
    /* 38 */ { SQL_API_SQLGETCURSORNAME,     "SQLGetCursorName", 
                (void*)SQLGetCursorName, (void*)SQLGetCursorNameW },
    /* 39 */ { SQL_API_SQLGETDATA,           "SQLGetData", (void*)SQLGetData },
    /* 40 */ { SQL_API_SQLGETDESCFIELD,      "SQLGetDescField", 
                (void*)SQLGetDescField, (void*)SQLGetDescFieldW },
    /* 41 */ { SQL_API_SQLGETDESCREC,        "SQLGetDescRec", 
                (void*)SQLGetDescRec, (void*)SQLGetDescRecW },
    /* 42 */ { SQL_API_SQLGETDIAGFIELD,      "SQLGetDiagField", 
                (void*)SQLGetDiagField, (void*)SQLGetDiagFieldW },
    /* 43 */ { SQL_API_SQLGETENVATTR,        "SQLGetEnvAttr", (void*)SQLGetEnvAttr },
    /* 44 */ { SQL_API_SQLGETFUNCTIONS,      "SQLGetFunctions", (void*)SQLGetFunctions },
    /* 45 */ { SQL_API_SQLGETINFO,           "SQLGetInfo", 
                (void*)SQLGetInfo, (void*)SQLGetInfoW },
    /* 46 */ { SQL_API_SQLGETSTMTATTR,       "SQLGetStmtAttr", 
                (void*)SQLGetStmtAttr, (void*)SQLGetStmtAttrW },
    /* 47 */ { SQL_API_SQLGETSTMTOPTION,     "SQLGetStmtOption", (void*)SQLGetStmtOption },
    /* 48 */ { SQL_API_SQLGETTYPEINFO,       "SQLGetTypeInfo", 
                (void*)SQLGetTypeInfo, (void*)SQLGetTypeInfoW },
    /* 49 */ { SQL_API_SQLMORERESULTS,       "SQLMoreResults", (void*)SQLMoreResults },
    /* 50 */ { SQL_API_SQLNATIVESQL,         "SQLNativeSql", 
                (void*)SQLNativeSql, (void*)SQLNativeSqlW },
    /* 51 */ { SQL_API_SQLNUMPARAMS,         "SQLNumParams", (void*)SQLNumParams },
    /* 52 */ { SQL_API_SQLNUMRESULTCOLS,     "SQLNumResultCols", (void*)SQLNumResultCols },
    /* 53 */ { SQL_API_SQLPARAMDATA,         "SQLParamData", (void*)SQLParamData },
    /* 54 */ { SQL_API_SQLPARAMOPTIONS,      "SQLParamOptions", (void*)SQLParamOptions },
    /* 55 */ { SQL_API_SQLPREPARE,           "SQLPrepare", 
                (void*)SQLPrepare, (void*)SQLPrepareW },
    /* 56 */ { SQL_API_SQLPRIMARYKEYS,       "SQLPrimaryKeys", 
                (void*)SQLPrimaryKeys, (void*)SQLPrimaryKeysW },
    /* 57 */ { SQL_API_SQLPROCEDURECOLUMNS,  "SQLProcedureColumns", 
                (void*)SQLProcedureColumns, (void*)SQLProcedureColumnsW },
    /* 58 */ { SQL_API_SQLPROCEDURES,        "SQLProcedures", 
                (void*)SQLProcedures, (void*)SQLProceduresW },
    /* 59 */ { SQL_API_SQLPUTDATA,           "SQLPutData", (void*)SQLPutData },
    /* 60 */ { SQL_API_SQLROWCOUNT,          "SQLRowCount", (void*)SQLRowCount },
    /* 61 */ { SQL_API_SQLSETCONNECTATTR,    "SQLSetConnectAttr", 
                (void*)SQLSetConnectAttr, (void*)SQLSetConnectAttrW },
    /* 62 */ { SQL_API_SQLSETCONNECTOPTION,  "SQLSetConnectOption", 
                (void*)SQLSetConnectOption, (void*)SQLSetConnectOptionW },
    /* 63 */ { SQL_API_SQLSETCURSORNAME,     "SQLSetCursorName", 
                (void*)SQLSetCursorName, (void*)SQLSetCursorNameW },
    /* 64 */ { SQL_API_SQLSETDESCFIELD,      "SQLSetDescField", 
                (void*)SQLSetDescField, (void*)SQLSetDescFieldW },
    /* 65 */ { SQL_API_SQLSETDESCREC,        "SQLSetDescRec", (void*)SQLSetDescRec },
    /* 66 */ { SQL_API_SQLSETENVATTR,        "SQLSetEnvAttr", (void*)SQLSetEnvAttr },
    /* 67 */ { SQL_API_SQLSETPARAM,          "SQLSetParam", (void*)SQLSetParam },
    /* 68 */ { SQL_API_SQLSETPOS,            "SQLSetPos", (void*)SQLSetPos },
    /* 69 */ { SQL_API_SQLSETSCROLLOPTIONS,  "SQLSetScrollOptions", (void*)SQLSetScrollOptions },
    /* 70 */ { SQL_API_SQLSETSTMTATTR,       "SQLSetStmtAttr", 
                (void*)SQLSetStmtAttr, (void*)SQLSetStmtAttrW },
    /* 71 */ { SQL_API_SQLSETSTMTOPTION,     "SQLSetStmtOption", (void*)SQLSetStmtOption },
    /* 72 */ { SQL_API_SQLSPECIALCOLUMNS,    "SQLSpecialColumns", 
                (void*)SQLSpecialColumns, (void*)SQLSpecialColumnsW },
    /* 73 */ { SQL_API_SQLSTATISTICS,        "SQLStatistics", 
                (void*)SQLStatistics, (void*)SQLStatisticsW },
    /* 74 */ { SQL_API_SQLTABLEPRIVILEGES,   "SQLTablePrivileges", 
                (void*)SQLTablePrivileges, (void*)SQLTablePrivilegesW },
    /* 75 */ { SQL_API_SQLTABLES,            "SQLTables", 
                (void*)SQLTables, (void*)SQLTablesW },
    /* 76 */ { SQL_API_SQLTRANSACT,          "SQLTransact", (void*)SQLTransact },
    /* 77 */ { SQL_API_SQLGETDIAGREC,        "SQLGetDiagRec", 
                (void*)SQLGetDiagRec, (void*)SQLGetDiagRecW },
    /* 78 */ { SQL_API_SQLCANCELHANDLE,      "SQLCancelHandle", (void*)SQLCancelHandle },
};

/*
 * connection pooling stuff
 */

CPOOLHEAD *pool_head = NULL;
int pooling_enabled = 0;
int pool_max_size = 0;
int pool_wait_timeout;

/*
 * helper function and macro to make setting any values set before connection 
 * simplier
 */

#define DO_ATTR( connection, value, attr3, attr2 )    \
        do_attr( connection, connection -> value, connection -> value##_set, attr3, \
                attr2 )

static void do_attr( DMHDBC connection, int value, 
        int value_set, int attr3, int attr2  )
{
    if ( value_set )
    {
        if (CHECK_SQLSETCONNECTATTR( connection ))
        {
            SQLSETCONNECTATTR(connection,
                        connection -> driver_dbc,
                        attr3,
                        (SQLPOINTER)(intptr_t) value,
                        sizeof( value ));
        }
        else if (CHECK_SQLSETCONNECTOPTION(connection) && attr2 )
        {
            SQLSETCONNECTOPTION(connection,
                        connection -> driver_dbc,
                        attr2,
                        value );
        }
        else if (CHECK_SQLSETCONNECTATTRW( connection ))     /* they are int values, so this should be safe */
        {
            SQLSETCONNECTATTRW(connection,
                        connection -> driver_dbc,
                        attr3,
                        (SQLPOINTER)(intptr_t) value,
                        sizeof( value ));
        }
        else if (CHECK_SQLSETCONNECTOPTIONW(connection) && attr2 )
        {
            SQLSETCONNECTOPTIONW(connection,
                        connection -> driver_dbc,
                        attr2,
                        value );
        }
    }
}

/*
 * implement reference counting for driver libs
 */

struct lib_count
{
    char                *lib_name;
    int                 count;
    void                *handle;
    struct lib_count    *next;
};

/*
 * I hate statics, but there is little option here, there can be multiple envs
 * so I can't save it in them, I do use a single static instance, this avoid
 * a potential leak if libodbc.so is dynamically loaded
 */

static struct lib_count *lib_list = NULL;
static struct lib_count single_lib_count;
static char single_lib_name[ INI_MAX_PROPERTY_VALUE + 1 ];

static void *odbc_dlopen( char *libname, char **err )
{
    void *hand;
    struct lib_count *list;

    mutex_lib_entry();

    /*
     * have we already got it ?
     */

    list = lib_list;
    while( list )
    {
        if ( strcmp( list -> lib_name, libname ) == 0 )
        {
            break;
        }

        list = list -> next;
    }

    if ( list )
    {
        list -> count ++;
        hand = list -> handle;
    }
    else
    {
        hand = lt_dlopen( libname );

        if ( hand )
        {
	        /*
	        * If only one, then use the static space
	        */
    
	        if ( lib_list == NULL )
	        {
		        list = &single_lib_count;
		        list -> next = lib_list;
		        lib_list = list;
		        list -> count = 1;
		        list -> lib_name = single_lib_name;
		        strcpy( single_lib_name, libname );
		        list -> handle = hand;
	        }
	        else
	        {
		        list = malloc( sizeof( struct lib_count ));
		        list -> next = lib_list;
		        lib_list = list;
		        list -> count = 1;
		        list -> lib_name = strdup( libname );
		        list -> handle = hand;
	        }
        }
        else {
            if ( err ) {
                *err = (char*) lt_dlerror();
            }
        }
    }

    mutex_lib_exit();

    return hand;
}

static void odbc_dlclose( void *handle )
{
    struct lib_count *list, *prev;

    mutex_lib_entry();

    /*
     * look for list entry
     */

    list = lib_list;
    prev = NULL;
    while( list )
    {
        if ( list -> handle == handle )
        {
            break;
        }

        prev = list;
        list = list -> next;
    }

    /*
     * it should always be found, but you never know...
     */

    if ( list )
    {
        list -> count --;

        if ( list -> count < 1 )
        {
		if ( list == &single_lib_count )
		{
            if ( prev ) 
            {
                prev -> next = list -> next;
            }
            else
            {
			    lib_list = list -> next;
            }
			lt_dlclose( list -> handle );
		}
		else
		{
            free( list -> lib_name );
            lt_dlclose( list -> handle );
            if ( prev )
            {
                prev -> next = list -> next;
            }
            else
            {
                lib_list = list -> next;
            }
            free( list );
		}
        }
    }
    else
    {
        lt_dlclose( handle );
    }

    mutex_lib_exit();
}

/*
 * open the library, extract the names, and do setup
 * before the actual connect.
 */

int __connect_part_one( DMHDBC connection, char *driver_lib, char *driver_name, int *warnings )
{
    int i;
    int ret;
    int threading_level;
    char threading_string[ 50 ];
    char mapping_string[ 50 ];
    char disable_gf[ 50 ];
    char fake_string[ 50 ];
    int fake_unicode;
    char *err;
    struct env_lib_struct *env_lib_list, *env_lib_prev;
    char txt[ 531 ];

    /*
     * check to see if we want to alter the default threading level
     * before opening the lib
     */

    /*
     * if the driver comes from odbc.ini not via odbcinst.ini the driver name will be empty
     * so only look for the entry if it's set
     */

    if ( driver_name[ 0 ] != '\0' ) 
	{
    	SQLGetPrivateProfileString( driver_name, "Threading", "99",
					threading_string, sizeof( threading_string ), 
                	"ODBCINST.INI" );
    	threading_level = atoi( threading_string );

        sprintf( txt, "\t\tThreading Level set from Driver Entry in ODBCINST.INI %d from '%s'", threading_level, threading_string );
        dm_log_write_diag( txt );
    }
    else 
	{
	    threading_level = 99;
    }

	/*
	 * look for default in [ODBC] section
	 */

	if ( threading_level == 99 ) 
	{
    	SQLGetPrivateProfileString( "ODBC", "Threading", "0",
				threading_string, sizeof( threading_string ), 
                		"ODBCINST.INI" );

    	threading_level = atoi( threading_string );


        sprintf( txt, "\t\tThreading Level set from [ODBC] Section in ODBCINST.INI %d from '%s'", threading_level, threading_string );
        dm_log_write_diag( txt );
	}

    if ( threading_level >= 0 && threading_level <= 3 )
    {
        dbc_change_thread_support( connection, threading_level );
    }

	connection -> threading_level = threading_level;

    /*
     * do we want to disable the SQLFetch -> SQLExtendedFetch 
     * mapping ?
     */

    SQLGetPrivateProfileString( driver_name, "ExFetchMapping", "1",
				mapping_string, sizeof( mapping_string ), 
                "ODBCINST.INI" );

    connection -> ex_fetch_mapping = atoi( mapping_string );

    if ( connection -> ex_fetch_mapping != 1 ) {
        sprintf( txt, "\t\tExFetchMapping set to %d from '%s'", connection -> ex_fetch_mapping, mapping_string );
        dm_log_write_diag( txt );
    }

    /*
     * Does the driver have support for SQLGetFunctions ?
     */

    SQLGetPrivateProfileString( driver_name, "DisableGetFunctions", "0",
				disable_gf, sizeof( disable_gf ), 
                "ODBCINST.INI" );

    connection -> disable_gf = atoi( disable_gf );

    if ( connection -> disable_gf != 0 ) {
        sprintf( txt, "\t\tDisableGetFunctions set to %d from '%s'", connection -> disable_gf, disable_gf );
        dm_log_write_diag( txt );
    }

    /*
     * do we want to keep hold of the lib handle, DB2 fails if we close
     */

    SQLGetPrivateProfileString( driver_name, "DontDLClose", "1",
				mapping_string, sizeof( mapping_string ), 
                "ODBCINST.INI" );

    connection -> dont_dlclose = atoi( mapping_string ) != 0;

    if ( connection -> dont_dlclose != 1 ) {
        sprintf( txt, "\t\tDisableGetFunctions set to %d from '%s'", connection -> dont_dlclose, mapping_string );
        dm_log_write_diag( txt );
    }

    /*
     * can we pool this one
     */

    SQLGetPrivateProfileString( driver_name, "CPTimeout", "0",
				mapping_string, sizeof( mapping_string ), 
                "ODBCINST.INI" );

    connection -> pooling_timeout = atoi( mapping_string );

    if ( connection -> pooling_timeout != 0 ) {
        sprintf( txt, "\t\tCPTimeout set to %d from '%s'", connection -> pooling_timeout, mapping_string );
        dm_log_write_diag( txt );
    }

    /*
     * have we got a time-to-live value for the pooling
     */

    SQLGetPrivateProfileString( driver_name, "CPTimeToLive", "0",
				mapping_string, sizeof( mapping_string ), 
                "ODBCINST.INI" );

    connection -> ttl = atoi( mapping_string );

    if ( connection -> ttl != 0 ) {
        sprintf( txt, "\t\tCPTimeToLive set to %d from '%s'", connection -> ttl, mapping_string );
        dm_log_write_diag( txt );
    }

    /*
     * Is there a check SQL statement
     */

    SQLGetPrivateProfileString( driver_name, "CPProbe", "",
				connection -> probe_sql, sizeof( connection -> probe_sql ), 
                "ODBCINST.INI" );


    if ( strlen( connection -> probe_sql ) != 0 ) {
        sprintf( txt, "\t\tCPProbe set to '%s'", connection -> probe_sql );
        dm_log_write_diag( txt );
    }

    /*
     * if pooling then leave the dlopen
     */

    if ( connection -> pooling_timeout > 0 )
    {
        connection -> dont_dlclose = 1;
    }

    SQLGetPrivateProfileString( driver_name, "FakeUnicode", "0",
				fake_string, sizeof( fake_string ), 
                "ODBCINST.INI" );

    fake_unicode = atoi( fake_string );

    if ( fake_unicode != 0 ) {
        sprintf( txt, "\t\tFakeUnicode set to %d from '%s'", fake_unicode, fake_string );
        dm_log_write_diag( txt );
    }


#ifdef HAVE_ICONV
#ifdef ENABLE_DRIVER_ICONV
    SQLGetPrivateProfileString( driver_name, "IconvEncoding", DEFAULT_ICONV_ENCODING,
				connection->unicode_string, sizeof( connection->unicode_string ), 
                "ODBCINST.INI" );
#endif
#endif

    /*
     * initialize unicode
     */

    if ( !unicode_setup( connection ))
    {
        char txt[ 256 ];

        sprintf( txt, "Can't initiate unicode conversion" );

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                txt );

        __post_internal_error( &connection -> error,
                ERROR_IM003, txt,
                connection -> environment -> requested_version );

        *warnings = TRUE;
    }

    /*
     * initialize libtool
     */

    mutex_lib_entry();      /* warning, this doesn't protect from other libs in the application */
                            /* in their own threads calling dlinit(); */
    lt_dlinit();
#ifdef MODULEDIR
    lt_dlsetsearchpath(MODULEDIR);
#endif
    mutex_lib_exit();

    /*
     * open the lib
     */

    connection -> driver_env = (DRV_SQLHANDLE)NULL;
    connection -> driver_dbc = (DRV_SQLHANDLE)NULL;
    connection -> functions = NULL;
    connection -> dl_handle = NULL;

    if ( !(connection -> dl_handle = odbc_dlopen( driver_lib, &err )))
    {
        char txt[ 2048 ];

        sprintf( txt, "Can't open lib '%s' : %s", 
                driver_lib, err ? err : "NULL ERROR RETURN" );

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                txt );

        __post_internal_error( &connection -> error,
                ERROR_01000, txt,
                connection -> environment -> requested_version );

        return 0;
    }

    /*
     * try and extract the ini and fini functions, and call ini if it's 
     * found
     */

    connection -> ini_func.func =
            (SQLRETURN (*)()) lt_dlsym( connection -> dl_handle,
                    ODBC_INI_FUNCTION );

    connection -> fini_func.func =
            (SQLRETURN (*)()) lt_dlsym( connection -> dl_handle,
                    ODBC_FINI_FUNCTION );

    if ( connection -> ini_func.func )
    {
        connection -> ini_func.func();
    }

    /*
     * extract all the function entry points
     */
    if ( !(connection -> functions = malloc( sizeof( template_func ))))
    {
        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: IM001" );

        __post_internal_error( &connection -> error,
                ERROR_HY001, NULL,
                connection -> environment -> requested_version );
        return 0;
    }

    memcpy( connection -> functions, template_func,
            sizeof( template_func ));

    for ( i = 0;
            i < sizeof( template_func ) / sizeof( template_func[ 0 ] );
            i ++ )
    {
        char name[ 128 ];

        connection -> functions[ i ].func =
            (SQLRETURN (*)()) lt_dlsym( connection -> dl_handle,
                    connection -> functions[ i ].name );

        if ( connection -> functions[ i ].dm_funcW )
        {
            /*
             * get ANSI version from driver
             */

            if ( fake_unicode )
            {
                sprintf( name, "%sW", connection -> functions[ i ].name );
            }
            else
            {
                sprintf( name, "%sA", connection -> functions[ i ].name );
            }
            connection -> functions[ i ].funcA =
                (SQLRETURN (*)()) lt_dlsym( connection -> dl_handle, name );

            if ( connection -> functions[ i ].funcA &&
                    !connection -> functions[ i ].func )
            {
                connection -> functions[ i ].func =
                    connection -> functions[ i ].funcA;
            }
            else if ( connection -> functions[ i ].func &&
                !connection -> functions[ i ].funcA )
            {
                connection -> functions[ i ].funcA =
                    connection -> functions[ i ].func;
            }

            /*
             * get UNICODE version from driver
             */

            sprintf( name, "%sW", connection -> functions[ i ].name );
            connection -> functions[ i ].funcW =
                (SQLRETURN (*)()) lt_dlsym( connection -> dl_handle, name );
        }
        else
        {
            connection -> functions[ i ].funcA = 
                connection -> functions[ i ].funcW = NULL;
        }

        /*
         * blank out ones that are in the DM to fix a big 
         * with glib 2.0.6
         */

		if ( connection -> functions[ i ].func &&
			(void*)connection -> functions[ i ].func == 
            (void*)connection -> functions[ i ].dm_func )
		{	
			connection -> functions[ i ].func = NULL;
		}

		if ( connection -> functions[ i ].funcW &&
			(void*)connection -> functions[ i ].funcW == 
            (void*)connection -> functions[ i ].dm_funcW )
		{	
			connection -> functions[ i ].funcW = NULL;
		}

        connection -> functions[ i ].can_supply =
            ( connection -> functions[ i ].func != NULL ) || 
              ( connection -> functions[ i ].funcW != NULL );
    }

    /*
     * check if this is the first time this driver has been loaded under this
     * lib, if not then reuse the env, else get the env from the driver
     */

    mutex_lib_entry();

    env_lib_list = connection -> environment -> env_lib_list;
    env_lib_prev = NULL;

    while( env_lib_list )
    {
        if ( strcmp( driver_lib, env_lib_list -> lib_name ) == 0 )
        {
            break;
        }
        env_lib_prev = env_lib_list;
        env_lib_list = env_lib_list -> next;
    }

    connection -> driver_act_ver = 0;
    if ( env_lib_list )
    {
        /*
         * Fix by qcai@starquest.com
         */
        SQLUINTEGER actual_version = 0;
        int ret;

        env_lib_list -> count ++;
        connection -> driver_env = env_lib_list -> env_handle;
        connection -> env_list_ent = env_lib_list;

        /*
         * Fix by qcai@starquest.com, Feb 5, 2003
         *
         * Since the driver was already loaded before, the version number
         * has been properly figured out.  This connection just need to get
         * it from priviously set value.  Without it, the version number is
         * at initial value of 0 which causes this and subsequence connection
         * to return a warning message "Driver does not support the requested
         * version".
         */

        /*
         * Change from Rafie Einstein to check SQLGETENVATTR is valid
         */
        if ((CHECK_SQLGETENVATTR( connection )))
        {
            ret = SQLGETENVATTR( connection,
                 connection -> driver_env,
                 SQL_ATTR_ODBC_VERSION,
                 &actual_version,
                 0,
                 NULL );
        }
        else
        {
            ret = SQL_SUCCESS;
            actual_version = SQL_OV_ODBC2;
        }

        if ( !ret )
        {
            connection -> driver_version = actual_version;
        }
        else
        {
            connection -> driver_version =
            connection -> environment -> requested_version;
        }
        /* end of fix */

        /*
         * get value that has been pushed up by the initial connection to this driver
         */

        connection -> driver_act_ver = env_lib_list -> driver_act_ver;
    }
    else
    {
        env_lib_list = calloc( 1, sizeof( struct env_lib_struct ));

        env_lib_list -> count = 1;
        env_lib_list -> next = connection -> environment -> env_lib_list;
        env_lib_list -> lib_name = strdup( driver_lib );
        connection -> env_list_ent = env_lib_list;

        connection -> environment -> env_lib_list = env_lib_list;

        __set_local_attributes( connection, SQL_HANDLE_ENV );

        /*
         * allocate a env handle
         */

        if ( CHECK_SQLALLOCHANDLE( connection ))
        {
            ret = SQLALLOCHANDLE( connection,
                    SQL_HANDLE_ENV,
                    SQL_NULL_HENV,
                    &connection -> driver_env,
                    connection );
			connection -> driver_act_ver = SQL_OV_ODBC3;
        }
        else if ( CHECK_SQLALLOCENV( connection ))
        {
            ret = SQLALLOCENV( connection,
                    &connection -> driver_env );
			connection -> driver_act_ver = SQL_OV_ODBC2;
        }
        else
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: IM004" );

            __post_internal_error( &connection -> error,
                    ERROR_IM004, NULL,
                    connection -> environment -> requested_version );

            if ( env_lib_list -> count == 1 )
            {
                if ( env_lib_prev )
                {
                    env_lib_prev -> next = env_lib_list -> next;
                }
                else
                {
                    connection -> environment -> env_lib_list = env_lib_list -> next;
                }

                free( env_lib_list -> lib_name );
                free( env_lib_list );
            }
            else
            {
                env_lib_list -> count --;
            }
            
    		mutex_lib_exit();
            return 0;
        }

        /*
         * push up to environment to be reused
         */

        env_lib_list -> driver_act_ver = connection -> driver_act_ver;

        env_lib_list -> env_handle = connection -> driver_env;

        if ( ret )
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: IM004" );

            __post_internal_error( &connection -> error,
                    ERROR_IM004, NULL,
                    connection -> environment -> requested_version );

            if ( env_lib_list -> count == 1 )
            {
                if ( env_lib_prev )
                {
                    env_lib_prev -> next = env_lib_list -> next;
                }
                else
                {
                    connection -> environment -> env_lib_list = env_lib_list -> next;
                }

                free( env_lib_list -> lib_name );
                free( env_lib_list );
            }
            else
            {
                env_lib_list -> count --;
            }

    		mutex_lib_exit();
            return 0;
        }

        /*
         * if it looks like a 3.x driver, try setting the interface type
         * to 3.x
         */
		if ( connection -> driver_act_ver >= SQL_OV_ODBC3 && CHECK_SQLSETENVATTR( connection ))
		{
            ret = SQLSETENVATTR( connection,
                    connection -> driver_env,
                    SQL_ATTR_ODBC_VERSION,
                    (SQLPOINTER)(intptr_t) connection -> environment -> requested_version,
                    0 );

            /*
             * if it don't set then assume a 2.x driver
             */

            if ( ret )
            {
                connection -> driver_version = SQL_OV_ODBC2;
            }
            else
            {
                if ( CHECK_SQLGETENVATTR( connection ))
                {
                    SQLINTEGER actual_version;

                    ret = SQLGETENVATTR( connection,
                        connection -> driver_env,
                        SQL_ATTR_ODBC_VERSION,
                        &actual_version,
                        0,
                        NULL );

                    if ( !ret )
                    {
                        connection -> driver_version = actual_version;
                    }
                    else
                    {
                        connection -> driver_version =
                            connection -> environment -> requested_version;
                    }
                }
                else
                {
                    connection -> driver_version =
                        connection -> environment -> requested_version;
                }
            }
        }
        else
        {
            connection -> driver_version = SQL_OV_ODBC2;
        }

        /*
         * set any env attributes
         */
        __set_attributes( connection, SQL_HANDLE_ENV );
    }

    mutex_lib_exit();

    /*
     * allocate a connection handle
     */

    if ( connection -> driver_version >= SQL_OV_ODBC3 )
    {
        ret = SQL_SUCCESS;

        if ( CHECK_SQLALLOCHANDLE( connection ))
        {
            ret = SQLALLOCHANDLE( connection,
                    SQL_HANDLE_DBC,
                    connection -> driver_env,
                    &connection -> driver_dbc,
                    connection );

            if ( ret )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: IM005" );

                __post_internal_error( &connection -> error,
                        ERROR_IM005, NULL,
                        connection -> environment -> requested_version );
            }
        }
        else if ( CHECK_SQLALLOCCONNECT( connection ))
        {
            ret = SQLALLOCCONNECT( connection,
                    connection -> driver_env,
                    &connection -> driver_dbc );

            if ( ret )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: IM005" );

                __post_internal_error( &connection -> error,
                        ERROR_IM005, NULL,
                        connection -> environment -> requested_version );
            }
        }
        else
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: IM005" );

            __post_internal_error( &connection -> error,
                    ERROR_IM005, NULL,
                    connection -> environment -> requested_version );
            return 0;
        }

        if ( ret )
        {
            SQLCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            /*
             * get the errors from the driver before
             * loseing the connection 
             */

            if ( CHECK_SQLGETDIAGREC( connection ))
            {
                int rec = 1;

                do
                {
                    ret = SQLGETDIAGREC( connection,
                            SQL_HANDLE_ENV,
                            connection -> driver_env,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                            sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLERROR( connection ))
            {
                do
                {
                    ret = SQLERROR( connection,
                            connection -> driver_env,
                            SQL_NULL_HDBC,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                                sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            return 0;
        }
    }
    else
    {
        ret = SQL_SUCCESS;

        if ( CHECK_SQLALLOCCONNECT( connection ))
        {
            ret = SQLALLOCCONNECT( connection,
                    connection -> driver_env,
                    &connection -> driver_dbc );

            if ( ret )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: IM005" );

                __post_internal_error( &connection -> error,
                        ERROR_IM005, NULL,
                        connection -> environment -> requested_version );
            }
        }
        else if ( CHECK_SQLALLOCHANDLE( connection ))
        {
            ret = SQLALLOCHANDLE( connection,
                    SQL_HANDLE_DBC,
                    connection -> driver_env,
                    &connection -> driver_dbc,
                    connection );

            if ( ret )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: IM005" );

                __post_internal_error( &connection -> error,
                        ERROR_IM005, NULL,
                        connection -> environment -> requested_version );
            }
        }
        else
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: IM005" );

            __post_internal_error( &connection -> error,
                    ERROR_IM005, NULL,
                    connection -> environment -> requested_version );
            return 0;
        }

        if ( ret )
        {
            SQLCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            /*
             * get the errors from the driver before
             * loseing the connection 
             */

            if ( CHECK_SQLERROR( connection ))
            {
                do
                {
                    ret = SQLERROR( connection,
                            connection -> driver_env,
                            SQL_NULL_HDBC,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                                sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLGETDIAGREC( connection ))
            {
                int rec = 1;

                do
                {
                    ret = SQLGETDIAGREC( connection,
                            SQL_HANDLE_ENV,
                            connection -> driver_env,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                            sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            return 0;
        }
    }

    /*
     * set any connection atributes
     */

    DO_ATTR( connection, access_mode, SQL_ATTR_ACCESS_MODE, SQL_ACCESS_MODE );
    DO_ATTR( connection, login_timeout, SQL_ATTR_LOGIN_TIMEOUT, SQL_LOGIN_TIMEOUT );
    DO_ATTR( connection, auto_commit, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT );
    DO_ATTR( connection, async_enable, SQL_ATTR_ASYNC_ENABLE, SQL_ASYNC_ENABLE );
    DO_ATTR( connection, auto_ipd, SQL_ATTR_AUTO_IPD, 0 );
    DO_ATTR( connection, connection_timeout, SQL_ATTR_CONNECTION_TIMEOUT, 0 );
    DO_ATTR( connection, metadata_id, SQL_ATTR_METADATA_ID, 0 );
    DO_ATTR( connection, packet_size, SQL_ATTR_PACKET_SIZE, SQL_PACKET_SIZE );
    DO_ATTR( connection, quite_mode, SQL_ATTR_QUIET_MODE, SQL_QUIET_MODE );
    DO_ATTR( connection, txn_isolation, SQL_ATTR_TXN_ISOLATION, SQL_TXN_ISOLATION );

    if ( connection -> save_attr )
    {
        struct save_attr *sa;

        sa = connection -> save_attr;

        while ( sa )
        {
            if ( sa -> str_attr )
            {
                if (CHECK_SQLSETCONNECTATTR( connection ))
                {
                    SQLSETCONNECTATTR(connection,
                            connection -> driver_dbc,
                            sa -> attr_type,
                            sa -> str_attr,
                            sa -> str_len );
                }
                else if (CHECK_SQLSETCONNECTOPTION(connection))
                {
                    SQLSETCONNECTOPTION(connection,
                            connection -> driver_dbc,
                            sa -> attr_type,
                            (SQLULEN) sa -> str_attr );
                }
                else if (CHECK_SQLSETCONNECTATTRW( connection ))
                {
                    SQLSETCONNECTATTRW(connection,
                                connection -> driver_dbc,
                                sa -> attr_type,
                                sa -> str_attr,
                                sa -> str_len );
                }
                else if (CHECK_SQLSETCONNECTOPTIONW(connection))
                {
                    SQLSETCONNECTOPTIONW(connection,
                                connection -> driver_dbc,
                                sa -> attr_type,
                                (SQLULEN) sa -> str_attr );
                }

            }
            else
            {
                if (CHECK_SQLSETCONNECTATTR( connection ))
                {
                    SQLSETCONNECTATTR(connection,
                            connection -> driver_dbc,
                            sa -> attr_type,
                            (SQLPOINTER) sa -> intptr_attr,
                            sa -> str_len );
                }
                else if (CHECK_SQLSETCONNECTOPTION(connection))
                {
                    SQLSETCONNECTOPTION(connection,
                            connection -> driver_dbc,
                            sa -> attr_type,
                            sa -> intptr_attr );
                }
                else if (CHECK_SQLSETCONNECTATTRW( connection ))
                {
                    SQLSETCONNECTATTRW(connection,
                                connection -> driver_dbc,
                                sa -> attr_type,
                                (SQLPOINTER) sa -> intptr_attr,
                                sa -> str_len );
                }
                else if (CHECK_SQLSETCONNECTOPTIONW(connection))
                {
                    SQLSETCONNECTOPTIONW(connection,
                                connection -> driver_dbc,
                                sa -> attr_type,
                                sa -> intptr_attr );
                }
            }
            sa = sa -> next;
        }
    }

    /*
     * set any preset connection attributes
     */

    __set_attributes( connection, SQL_HANDLE_DBC );

    return 1;
}

/*
 * extract the available functions and call SQLSetConnectAttr
 */

int __connect_part_two( DMHDBC connection )
{
    int i, use_cursor;

    /*
     * Call SQLFunctions to get the supported list and
     * mask out those that are exported but not supported
     */

    if ( CHECK_SQLGETFUNCTIONS( connection ) && !connection -> disable_gf )
    {
        SQLRETURN ret;
        SQLUSMALLINT supported_funcs[ SQL_API_ODBC3_ALL_FUNCTIONS_SIZE ];
		SQLUSMALLINT supported_array[ 100 ];

        /*
         * try using fast version, but only if the driver is set to ODBC 3, 
         * some drivers (SAPDB) fail to return the correct values in this situation
         */

        if ( connection -> driver_act_ver >= SQL_OV_ODBC3 )
        {
            ret = SQLGETFUNCTIONS( connection,
                connection -> driver_dbc,
                SQL_API_ODBC3_ALL_FUNCTIONS,
                supported_funcs );
        }
        else
        {
			ret = SQLGETFUNCTIONS( connection,
				connection -> driver_dbc,
				SQL_API_ALL_FUNCTIONS,
				supported_array );
        }

        if ( ret == SQL_SUCCESS )
        {
            for ( i = 0;
                i < sizeof( template_func ) / sizeof( template_func[ 0 ] );
                i ++ )
            {
                if ( connection -> functions[ i ].func )
                {
                    SQLRETURN ret;
                    SQLUSMALLINT supported;

					if ( connection -> driver_act_ver >= SQL_OV_ODBC3 )
					{
                        supported = SQL_FUNC_EXISTS( supported_funcs, connection -> functions[ i ].ordinal );

                    	if ( supported == SQL_FALSE )
                    	{
                        	connection -> functions[ i ].func = NULL;
                        	connection -> functions[ i ].can_supply = 0;
                    	}
					}
					else 
					{
                        if ( connection -> functions[ i ].ordinal >= 100 )
						{
							ret = SQLGETFUNCTIONS( connection,
								connection -> driver_dbc,
								connection -> functions[ i ].ordinal,
								&supported );
						}
						else
						{
							supported = supported_array[ connection -> functions[ i ].ordinal ];
							ret = SQL_SUCCESS;
						}

                    	if ( supported == SQL_FALSE || ret != SQL_SUCCESS )
                    	{
                        	connection -> functions[ i ].func = NULL;
                        	connection -> functions[ i ].can_supply = 0;
                    	}
					}
                }
            }
        }
        else
        {
            for ( i = 0;
                i < sizeof( template_func ) / sizeof( template_func[ 0 ] );
                i ++ )
            {
                if ( connection -> functions[ i ].func )
                {
                    SQLRETURN ret;
                    SQLUSMALLINT supported;

					ret = SQLGETFUNCTIONS( connection,
							connection -> driver_dbc,
							connection -> functions[ i ].ordinal,
							&supported );

                    if ( supported == SQL_FALSE || ret != SQL_SUCCESS )
                    {
                        connection -> functions[ i ].func = NULL;
                        connection -> functions[ i ].can_supply = 0;
                    }
                }
            }
        }
    }

    /*
     * CoLAttributes is the same as ColAttribute
     */

    if ( connection -> functions[ DM_SQLCOLATTRIBUTE ].func &&
        !connection -> functions[ DM_SQLCOLATTRIBUTES ].func )
    {
        connection -> functions[ DM_SQLCOLATTRIBUTES ].can_supply = 1;
    }
    if ( connection -> functions[ DM_SQLCOLATTRIBUTES ].func &&
        !connection -> functions[ DM_SQLCOLATTRIBUTE ].func )
    {
        connection -> functions[ DM_SQLCOLATTRIBUTE ].can_supply = 1;
    }

    /*
     * mark the functions that the driver manager does
     */

    /*
     * SQLDatasources
     */
    connection -> functions[ DM_SQLDATASOURCES ].can_supply = 1;

    /*
     * SQLDrivers
     */
    connection -> functions[ DM_SQLDRIVERS ].can_supply = 1;

    /*
     * SQLAllocHandleStd
     */
    connection -> functions[ DM_SQLALLOCHANDLESTD ].can_supply = 1;

    /*
     * add all the functions that are supported via ODBC 2<->3
     * issues
     */
    if ( !connection -> functions[ DM_SQLALLOCENV ].func &&
            connection -> functions[ DM_SQLALLOCHANDLE ].func )
    {
        connection -> functions[ DM_SQLALLOCENV ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLALLOCCONNECT ].func &&
            connection -> functions[ DM_SQLALLOCHANDLE ].func )
    {
        connection -> functions[ DM_SQLALLOCCONNECT ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLALLOCSTMT ].func &&
            connection -> functions[ DM_SQLALLOCHANDLE ].func )
    {
        connection -> functions[ DM_SQLALLOCSTMT ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLFREEENV ].func &&
            connection -> functions[ DM_SQLFREEHANDLE ].func )
    {
        connection -> functions[ DM_SQLFREEENV ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLFREECONNECT ].func &&
            connection -> functions[ DM_SQLFREEHANDLE ].func )
    {
        connection -> functions[ DM_SQLFREECONNECT ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLGETDIAGREC ].func &&
            connection -> functions[ DM_SQLERROR ].func )
    {
        connection -> functions[ DM_SQLGETDIAGREC ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLGETDIAGFIELD ].func &&
            connection -> functions[ DM_SQLERROR ].func )
    {
        connection -> functions[ DM_SQLGETDIAGFIELD ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLERROR ].func &&
            connection -> functions[ DM_SQLGETDIAGREC ].func )
    {
        connection -> functions[ DM_SQLERROR ].can_supply = 1;
    }

    /*
     * ODBC 3 still needs SQLFreeStmt
     */

    /*
     * this is only partial, as we can't support a descriptor alloc
     */
    if ( !connection -> functions[ DM_SQLALLOCHANDLE ].func &&
            connection -> functions[ DM_SQLALLOCENV ].func &&
            connection -> functions[ DM_SQLALLOCCONNECT ].func &&
            connection -> functions[ DM_SQLALLOCHANDLE ].func )
    {
        connection -> functions[ DM_SQLALLOCHANDLE ].can_supply = 1;
    }
    if ( !connection -> functions[ DM_SQLFREEHANDLE ].func &&
            connection -> functions[ DM_SQLFREEENV ].func &&
            connection -> functions[ DM_SQLFREECONNECT ].func &&
            connection -> functions[ DM_SQLFREEHANDLE ].func )
    {
        connection -> functions[ DM_SQLFREEHANDLE ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLBINDPARAM ].func &&
                        connection -> functions[ DM_SQLBINDPARAMETER ].func )
    {
        connection -> functions[ DM_SQLBINDPARAM ].can_supply = 1;
    }
    else if ( !connection -> functions[ DM_SQLBINDPARAMETER ].func &&
                        connection -> functions[ DM_SQLBINDPARAM ].func )
    {
        connection -> functions[ DM_SQLBINDPARAMETER ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLGETCONNECTOPTION ].func &&
                        connection -> functions[ DM_SQLGETCONNECTATTR ].func )
    {
        connection -> functions[ DM_SQLGETCONNECTOPTION ].can_supply = 1;
    }
    else if ( !connection -> functions[ DM_SQLGETCONNECTATTR ].func &&
                        connection -> functions[ DM_SQLGETCONNECTOPTION ].func )
    {
        connection -> functions[ DM_SQLGETCONNECTATTR ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLGETSTMTOPTION ].func &&
                        connection -> functions[ DM_SQLGETSTMTATTR ].func )
    {
        connection -> functions[ DM_SQLGETSTMTOPTION ].can_supply = 1;
    }
    else if ( !connection -> functions[ DM_SQLGETSTMTATTR ].func &&
                        connection -> functions[ DM_SQLGETSTMTOPTION ].func )
    {
        connection -> functions[ DM_SQLGETSTMTATTR ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLPARAMOPTIONS ].func &&
                        connection -> functions[ DM_SQLSETSTMTATTR ].func )
    {
        connection -> functions[ DM_SQLPARAMOPTIONS ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLSETCONNECTOPTION ].func &&
                        connection -> functions[ DM_SQLSETCONNECTATTR ].func )
    {
        connection -> functions[ DM_SQLSETCONNECTOPTION ].can_supply = 1;
    }
    else if ( !connection -> functions[ DM_SQLSETCONNECTATTR ].func &&
                        connection -> functions[ DM_SQLSETCONNECTOPTION ].func )
    {
        connection -> functions[ DM_SQLSETCONNECTATTR ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLSETPARAM ].func &&
                        connection -> functions[ DM_SQLBINDPARAMETER ].func )
    {
        connection -> functions[ DM_SQLSETPARAM ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLSETSCROLLOPTIONS ].func &&
                        connection -> functions[ DM_SQLSETSTMTATTR ].func )
    {
        connection -> functions[ DM_SQLSETSCROLLOPTIONS ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLSETSTMTOPTION ].func &&
                        connection -> functions[ DM_SQLSETSTMTATTR ].func )
    {
        connection -> functions[ DM_SQLSETSTMTOPTION ].can_supply = 1;
    }
    else if ( !connection -> functions[ DM_SQLSETSTMTATTR ].func &&
                        connection -> functions[ DM_SQLSETSTMTOPTION ].func )
    {
        connection -> functions[ DM_SQLSETSTMTATTR ].can_supply = 1;
    }

    if ( !connection -> functions[ DM_SQLTRANSACT ].func &&
                        connection -> functions[ DM_SQLENDTRAN ].func )
    {
        connection -> functions[ DM_SQLTRANSACT ].can_supply = 1;
    }
    else if ( !connection -> functions[ DM_SQLENDTRAN ].func &&
                        connection -> functions[ DM_SQLTRANSACT ].func )
    {
        connection -> functions[ DM_SQLENDTRAN ].can_supply = 1;
    }

    /*
     * we can always do this
     */

    if ( !connection -> functions[ DM_SQLGETFUNCTIONS ].func )
    {
        connection -> functions[ DM_SQLGETFUNCTIONS ].can_supply = 1;
    }

    /*
     * TO_DO get some driver settings, such as the GETDATA_EXTENSTION
     * it supports
     */

    if ( CHECK_SQLGETINFO( connection ) || CHECK_SQLGETINFOW( connection ))
    {
        char txt[ 20 ];
        SQLRETURN ret;

        if ( connection -> driver_act_ver >= SQL_OV_ODBC3 )
        {
            ret = __SQLGetInfo( connection,
                    SQL_XOPEN_CLI_YEAR,
                    txt, 
                    sizeof( connection -> cli_year ),
                    NULL );

            if ( SQL_SUCCEEDED( ret ))
            {
                strcpy( connection -> cli_year, txt );
            }
        }
    }

    /*
     * TO_DO now we should pass any SQLSetEnvAttr settings
     */

    /*
     * now we have a connection handle, and we can check to see if
     * we need to use the cursor library
     */

    if ( connection -> cursors == SQL_CUR_USE_ODBC )
    {
        use_cursor = 1;
    }
    else if ( connection -> cursors == SQL_CUR_USE_IF_NEEDED )
    {
        /*
         * get scrollable info
         */

        if ( !CHECK_SQLGETINFO( connection ) && !CHECK_SQLGETINFOW( connection ))
        {
            /*
             * bit of a retarded driver, better give up
             */
            use_cursor = 0;
        }
        else
        {
            SQLRETURN ret;
            SQLUINTEGER val;

            /*
             * check if static cursors support scrolling
             */

            if ( connection -> driver_act_ver >=
                    SQL_OV_ODBC3 )
            {
                ret = __SQLGetInfo( connection,
                        SQL_STATIC_CURSOR_ATTRIBUTES1,
                        &val,
                        sizeof( val ),
                        NULL );

                if ( ret != SQL_SUCCESS )
                {
                    use_cursor = 1;
                }
                else
                {
                    /*
                     * do we need it ?
                     */
                    if ( !( val & SQL_CA1_ABSOLUTE )) 
                    {
                        use_cursor = 1;
                    }
                    else
                    {
                        use_cursor = 0;
                    }
                }
            }
            else
            {
                ret = __SQLGetInfo( connection,
                        SQL_FETCH_DIRECTION,
                        &val,
                        sizeof( val ),
                        NULL );

                if ( ret != SQL_SUCCESS )
                {
                    use_cursor = 1;
                }
                else
                {
                    /*
                     * are we needed
                     */

                    if ( !( val & SQL_FD_FETCH_PRIOR )) 
                    {
                        use_cursor = 1;
                    }
                    else 
                    {
                        use_cursor = 0;
                    }
                }
            }
        }
    }
    else
    {
        use_cursor = 0;
    }

    /*
     * if required connect to the cursor lib
     */

    if ( use_cursor )
    {
		char ext[ 32 ]; 
		char name[ ODBC_FILENAME_MAX * 2 + 1 ];
        int (*cl_connect)(void*, struct driver_helper_funcs*);
        char *err;
        struct driver_helper_funcs dh;

		/*
		 * SHLIBEXT can end up unset on some distributions (suze)
		 */

		if ( strlen( SHLIBEXT ) == 0 ) 
		{
			strcpy( ext, ".so" );
		}
		else
		{
            if ( strlen( SHLIBEXT ) + 1 > sizeof( ext )) {
                fprintf( stderr, "internal error, unexpected SHLIBEXT value ('%s') may indicate a problem with configure\n", SHLIBEXT );
                abort();
            }
			strcpy( ext, SHLIBEXT );
		}

#ifdef CURSOR_LIB_VER
            sprintf( name, "%s%s.%s", CURSOR_LIB, ext, CURSOR_LIB_VER );
#else
            sprintf( name, "%s%s", CURSOR_LIB, ext );
#endif

        if ( !(connection -> cl_handle = odbc_dlopen( name, &err )))
        {
            char b1[ ODBC_FILENAME_MAX + 1 ];
            /*
             * try again
             */

#ifdef CURSOR_LIB_VER
#ifdef __VMS
                sprintf( name, "%s:%s%s.%s", odbcinst_system_file_path( b1 ), CURSOR_LIB, ext, CURSOR_LIB_VER );
#else
#ifdef __OS2__
	            /* OS/2 does not use the system_lib_path or version defines to construct a name */
                sprintf( name, "%s.%s", CURSOR_LIB, ext );
#else
                sprintf( name, "%s/%s%s.%s", odbcinst_system_file_path( b1 ), CURSOR_LIB, ext, CURSOR_LIB_VER );
#endif
#endif
#else 
#ifdef __VMS
                sprintf( name, "%s:%s%s", odbcinst_system_file_path( b1 ), CURSOR_LIB, ext );
#else
#ifdef __OS2__
	            /* OS/2 does not use the system_lib_path or version defines to construct a name */
                sprintf( name, "%s%s", CURSOR_LIB, ext );
#else
                sprintf( name, "%s/%s%s", odbcinst_system_file_path( b1 ), CURSOR_LIB, ext );
#endif
#endif
#endif
            if ( !(connection -> cl_handle = odbc_dlopen( name, &err )))
            {
                char txt[ ODBC_FILENAME_MAX * 2 + 45 ];

#ifdef HAVE_SNPRINTF
                snprintf( txt, sizeof( txt ), "Can't open cursor lib '%s' : %s", 
                    name, err ? err : "NULL ERROR RETURN" );
#else
                sprintf( txt, "Can't open cursor lib '%s' : %s", 
                    name, err ? err : "NULL ERROR RETURN" );
#endif

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        txt  );

                __post_internal_error( &connection -> error,
                        ERROR_01000, txt,
                        connection -> environment -> requested_version );

                return 0;
            }
        }

        if ( !( cl_connect = (int(*)(void*, struct driver_helper_funcs* ))lt_dlsym( connection -> cl_handle,
                        "CLConnect" )))
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: 01000 Unable to load Cursor Lib" );

            __post_internal_error( &connection -> error,
                    ERROR_01000, "Unable to load cursor library",
                    connection -> environment -> requested_version );

            odbc_dlclose( connection -> cl_handle );
            connection -> cl_handle = NULL;

            return 0;
        }

        /*
         * setup helper functions
         */

        dh.__post_internal_error_ex = __post_internal_error_ex;
        dh.__post_internal_error = __post_internal_error;
        dh.dm_log_write = dm_log_write;

        if ( cl_connect( connection, &dh ) != SQL_SUCCESS )
        {
            odbc_dlclose( connection -> cl_handle );
            connection -> cl_handle = NULL;
            return 0;
        }
    }
    else
    {
        connection -> cl_handle = NULL;
    }

    return 1;
}

static void release_env( DMHDBC connection )
{
    struct env_lib_struct *env_lib_list, *env_lib_prev;
    int ret;

    if ( connection -> driver_env )
    {
        env_lib_prev = env_lib_list = NULL;

        mutex_lib_entry();

        if ( connection -> env_list_ent && connection -> environment )
        {
            env_lib_list = connection -> environment -> env_lib_list;
            while( env_lib_list )
            {
                if ( env_lib_list == connection -> env_list_ent )
                {
                    break;
                }
                env_lib_prev = env_lib_list;
                env_lib_list = env_lib_list -> next;
            }
        }

        if ( env_lib_list && env_lib_list -> count > 1 )
        {
            env_lib_list -> count --;
        }
        else
        {
            if ( connection -> driver_version >= SQL_OV_ODBC3 )
            {
				ret = SQL_ERROR;
                if ( CHECK_SQLFREEHANDLE( connection ))
                {
                    ret = SQLFREEHANDLE( connection,
                            SQL_HANDLE_ENV,
                            connection -> driver_env );
                }
				else if ( CHECK_SQLFREEENV( connection ))
                {
                    ret = SQLFREEENV( connection,
                            connection -> driver_env );
				}
				if ( !ret )
					connection -> driver_env = (DRV_SQLHANDLE)NULL;
            }
            else
            {
				ret = SQL_ERROR;
                if ( CHECK_SQLFREEENV( connection ))
                {
                    ret = SQLFREEENV( connection,
                            connection -> driver_env );
                }
                else if ( CHECK_SQLFREEHANDLE( connection ))
                {
                    ret = SQLFREEHANDLE( connection,
                            SQL_HANDLE_ENV,
                            connection -> driver_env );
                }

                if ( !ret )
                    connection -> driver_env = (DRV_SQLHANDLE)NULL;
            }

            /*
             * remove the entry
             */

            if ( env_lib_prev && env_lib_list )
            {
                env_lib_prev -> next = env_lib_list -> next;
            }
            else
            {
				if ( env_lib_list )
				{
                	connection -> environment -> env_lib_list = env_lib_list -> next;
				}
            }

	    	if ( env_lib_list )
	    	{
            	free( env_lib_list -> lib_name );
            	free( env_lib_list );
			}
        }

        mutex_lib_exit();
    }
}

/*
 * clean up after the first part of the connect
 */

void __disconnect_part_one( DMHDBC connection )
{
    int ret = SQL_ERROR;

    /*
     * try a version 3 disconnect first on the connection
     */
    if ( connection -> driver_dbc )
    {
        if ( connection -> driver_version >= SQL_OV_ODBC3 )
        {
            if ( CHECK_SQLFREEHANDLE( connection ))
            {
                ret = SQLFREEHANDLE( connection,
                        SQL_HANDLE_DBC,
                        connection -> driver_dbc );
			}
			else if ( CHECK_SQLFREECONNECT( connection ))
			{
				ret = SQLFREECONNECT( connection,
						connection -> driver_dbc );
			}

			if ( !ret )
			{
				connection -> driver_dbc = (DRV_SQLHANDLE)NULL;
            }
        }
		else 
		{
			if ( CHECK_SQLFREECONNECT( connection ))
			{
				ret = SQLFREECONNECT( connection,
						connection -> driver_dbc );
			}
			else if ( CHECK_SQLFREEHANDLE( connection ))
            {
                ret = SQLFREEHANDLE( connection,
                        SQL_HANDLE_DBC,
                        connection -> driver_dbc );
			}

			if ( !ret ) 
			{
				connection -> driver_dbc = (DRV_SQLHANDLE)NULL;
            }
		}
    	connection -> driver_dbc = (DRV_SQLHANDLE)NULL;
    }

    /*
     * now disconnect the environment, if it's the last usage on the connection
     */

    if ( connection -> driver_env )
    {
        release_env( connection );
    }

    connection -> driver_env = (DRV_SQLHANDLE)NULL;

    /*
     * unload the lib
     */
    if ( connection -> cl_handle )
    {
        odbc_dlclose( connection -> cl_handle );
        connection -> cl_handle = NULL;
    }

    if ( connection -> dl_handle )
    {
        if ( !connection -> dont_dlclose )
        {
            /* 
             * call fini function if found
             */

            if ( connection -> fini_func.func )
            {
                connection -> fini_func.func();
            }

            odbc_dlclose( connection -> dl_handle );
        }
        connection -> dl_handle = NULL;
    }

    /*
     * free some memory
     */

    if ( connection -> functions )
    {
        free( connection -> functions );
        connection -> functions = NULL;
    }
}

void __disconnect_part_two( DMHDBC connection )
{
    if ( CHECK_SQLDISCONNECT( connection ))
    {
        SQLDISCONNECT( connection,
                connection -> driver_dbc );
    }
}

/*
 * final clean up
 */

void __disconnect_part_four( DMHDBC connection )
{
    /*
     * now disconnect the environment, if it's the last usage on the connection
     */

    release_env( connection );

    connection -> driver_env = (DRV_SQLHANDLE)NULL;

    /*
     * unload the lib
     */

    if ( connection -> cl_handle )
    {
        odbc_dlclose( connection -> cl_handle );
        connection -> cl_handle = NULL;
    }

    if ( connection -> dl_handle )
    {
        /*
         * this is safe, because the dlopen function will reuse the handle if we 
         * open the same lib again
         */
        if ( !connection -> dont_dlclose )
        {
            if ( connection -> fini_func.func )
            {
                connection -> fini_func.func();
            }

            odbc_dlclose( connection -> dl_handle );
        }
        connection -> dl_handle = NULL;
    }

    /*
     * free some memory
     */

    if ( connection -> functions )
    {
        free( connection -> functions );
        connection -> functions = NULL;
    }
    connection -> state = STATE_C2;

    /*
     * now clean up any statements that are left about
     */

    __clean_stmt_from_dbc( connection );
    __clean_desc_from_dbc( connection );
}

/*
 * normal disconnect
 */

void __disconnect_part_three( DMHDBC connection )
{
    if ( connection -> driver_version >= SQL_OV_ODBC3 )
    {
        if ( CHECK_SQLFREEHANDLE( connection ))
        {
            SQLFREEHANDLE( connection,
                    SQL_HANDLE_DBC,
                    connection -> driver_dbc );
        }
        else if ( CHECK_SQLFREECONNECT( connection ))
        {
            SQLFREECONNECT( connection,
                    connection -> driver_dbc );
        }
    }
    else
    {
        if ( CHECK_SQLFREECONNECT( connection ))
        {
            SQLFREECONNECT( connection,
                    connection -> driver_dbc );
        }
        else if ( CHECK_SQLFREEHANDLE( connection ))
        {
            SQLFREEHANDLE( connection,
                    SQL_HANDLE_DBC,
                    connection -> driver_dbc );
        }
    }

    connection -> driver_dbc = (DRV_SQLHANDLE)NULL;

    __disconnect_part_four( connection );
}

/*
 * interface for SQLGetFunctions
 */

void  __check_for_function( DMHDBC connection,
        SQLUSMALLINT function_id,
        SQLUSMALLINT *supported )
{
    int i;

    if ( !supported ) 
    {
        return;
    }

    if ( function_id == SQL_API_ODBC3_ALL_FUNCTIONS )
    {
        for ( i = 0; i < SQL_API_ODBC3_ALL_FUNCTIONS_SIZE; i ++ )
        {
            supported[ i ] = 0x0000;
        }
        for ( i = 0; i < sizeof( template_func ) / sizeof( template_func[ 0 ] ); i ++ )
        {
        int id = connection -> functions[ i ].ordinal;

            if ( connection -> functions[ i ].can_supply )
                supported[ id >> 4 ] |= ( 1 << ( id & 0x000F ));
        }
    }
    else if ( function_id == SQL_API_ALL_FUNCTIONS )
    {
        for ( i = 0; i < 100; i ++ )
        {
            supported[ i ] = SQL_FALSE;
        }
        for ( i = 0; i < sizeof( template_func ) / sizeof( template_func[ 0 ] ); i ++ )
        {
            if ( connection -> functions[ i ].ordinal < 100 )
            {
                if ( connection -> functions[ i ].can_supply )
                    supported[ connection -> functions[ i ].ordinal ] =
                        SQL_TRUE;
            }
        }
    }
    else
    {
        *supported = SQL_FALSE;
        for ( i = 0; i < sizeof( template_func ) / sizeof( template_func[ 0 ] ); i ++ )
        {
            if ( connection->functions[ i ].ordinal == function_id )
            {
                if ( connection -> functions[ i ].can_supply )
                    *supported = SQL_TRUE;
                break;
            }
        }
    }
}

static int sql_strcmp( SQLCHAR *s1, SQLCHAR *s2, SQLSMALLINT l1, SQLSMALLINT l2 )
{
    if ( l1 != l2 )
    {
        return 1;
    }

    if ( l1 == SQL_NTS )
    {
        return strcmp((char*) s1, (char*)s2 );
    }
    else
    {
        return memcmp( s1, s2, l1 );
    }
}

static void close_pooled_connection( CPOOLENT *ptr )
{
    SQLRETURN ret;
    DMHDBC conn = &ptr -> connection;

    if ( conn -> driver_dbc == NULL ) {
        return;
    }

    /*
     * disconnect from the driver
     */

    if ( !CHECK_SQLDISCONNECT( conn ))
    {
        return;
    }

    ret = SQLDISCONNECT( conn, conn -> driver_dbc );

    if ( SQL_SUCCEEDED( ret ))
    {
        /*
         * complete disconnection from driver
         */

        if ( conn -> driver_version >= SQL_OV_ODBC3 )
        {
            if ( CHECK_SQLFREEHANDLE( conn ))
            {
                SQLFREEHANDLE( conn,
                        SQL_HANDLE_DBC,
                        conn -> driver_dbc );
            }
            else if ( CHECK_SQLFREECONNECT( conn ))
            {
                SQLFREECONNECT( conn, conn -> driver_dbc );
            }
        }
        else
        {
            if ( CHECK_SQLFREECONNECT( conn ))
            {
                SQLFREECONNECT( conn, conn -> driver_dbc );
            }
            else if ( CHECK_SQLFREEHANDLE( conn ))
            {
                SQLFREEHANDLE( conn,
                        SQL_HANDLE_DBC,
                        conn -> driver_dbc );
            }
        }

        conn -> driver_dbc = (DRV_SQLHANDLE)NULL;

        /*
         * Only call freeenv if it's the last connection to the driver
         */

        release_env( conn );

        conn -> driver_env = (DRV_SQLHANDLE)NULL;

        /*
         * unload the lib
         */

        if ( conn -> cl_handle )
        {
            odbc_dlclose( conn -> cl_handle );
            conn -> cl_handle = NULL;
        }

        if ( conn -> dl_handle )
        {
            /*
             * this is safe, because the dlopen function will reuse the handle if we 
             * open the same lib again
             */
            if ( !conn -> dont_dlclose )
            {
                /* 
                 * call fini function if found
                 */

                if ( conn -> fini_func.func )
                {
                    conn -> fini_func.func();
                }

                odbc_dlclose( ptr -> connection.dl_handle );
            }
            conn -> dl_handle = NULL;
        }

        /*
         * free some memory
         */

        if ( conn -> functions )
        {
            free( conn -> functions );
            conn -> functions = NULL;
        }
    }
    else
    {
        /* 
         * All we can do is tidy up
         */

        conn -> driver_dbc = (DRV_SQLHANDLE)NULL;
        conn -> driver_env = (DRV_SQLHANDLE)NULL;

        /*
         * unload the lib
         */

        if ( conn -> cl_handle )
        {
            odbc_dlclose( conn -> cl_handle );
            conn -> cl_handle = NULL;
        }

        if ( conn -> dl_handle )
        {
            /*
             * this is safe, because the dlopen function will reuse the handle if we 
             * open the same lib again
             */
            if ( !conn -> dont_dlclose )
            {
                /* 
                 * call fini function if found
                 */

                if ( conn -> fini_func.func )
                {
                    conn -> fini_func.func();
                }

                odbc_dlclose( conn -> dl_handle );
            }
            conn -> dl_handle = NULL;
        }

        /*
         * free some memory
         */

        if ( conn -> functions )
        {
            free( conn -> functions );
            conn -> functions = NULL;
        }
    }

    /*
     * now clean up any statements that are left about
     */

    __clean_stmt_from_dbc( conn );
    __clean_desc_from_dbc( conn );

#ifdef HAVE_ICONV

    if ( ptr -> connection.iconv_cd_ascii_to_uc != (iconv_t)(-1) )
    {
        iconv_close( ptr -> connection.iconv_cd_ascii_to_uc );
        ptr -> connection.iconv_cd_ascii_to_uc = (iconv_t)(-1);
    }

    if ( ptr -> connection.iconv_cd_uc_to_ascii != (iconv_t)(-1))
    {
        iconv_close( ptr -> connection.iconv_cd_uc_to_ascii );
        ptr -> connection.iconv_cd_uc_to_ascii = (iconv_t)(-1);
    }

#endif

}

/*
 * if a environment gets released from the application, we need to remove any referenvce to that environment 
 * in pooled connections that belong to that environment. Also if needed call the release in the driver itself
 */

void __strip_from_pool( DMHENV env )
{
    CPOOLHEAD *ptrh;

    mutex_pool_entry();

    for( ptrh = pool_head; ptrh; ptrh = ptrh -> next )
    {
        CPOOLENT *ptre;
        for ( ptre = ptrh -> entries; ptre; ptre = ptre -> next )
        {
            if ( ptre -> connection.environment == env )
            {
                /*
                 * disconnect driver side connection, and when the last the driver side env
                 */
                close_pooled_connection( ptre );

                ptre -> connection.environment = NULL;
            }
        }
    }

    mutex_pool_exit();
}

void pool_unreserve( CPOOLHEAD *pooh )
{
    if ( pooh )
    {
        mutex_pool_entry();
        if ( ! -- pooh -> num_entries )
        {
            CPOOLHEAD *head, *prev;
            for ( head = pool_head, prev = NULL ; head ; prev = head, head = head -> next )
            {
                if ( head == pooh )
                {
                    if ( prev )
                    {
                        prev -> next = pooh -> next;
                    }
                    else
                    {
                        pool_head = pooh -> next;
                    }
                    if ( pooh -> _driver_connect_string ) {
                        free( pooh -> _driver_connect_string );
                    }
                    if ( pooh -> _server ) {
                        free( pooh -> _server );
                    }
                    if ( pooh -> _user ) {
                        free( pooh -> _user );
                    }
                    if ( pooh -> _password ) {
                        free( pooh -> _password );
                    }
                    free( pooh );
                    break;
                }
            }
        }
        pool_signal();
        mutex_pool_exit();
    }
}

static void copy_nts( char **dst, SQLCHAR *src, int *out_length, SQLSMALLINT length )
{
    if ( src == NULL ) 
    {
        *dst = malloc( 1 );
        *dst[ 0 ] = '\0';
    }
    else 
    {
        if ( length < 0 )
        {
            *dst = malloc( strlen( src ) + 1 );
            strcpy( *dst, src );
        }
        else
        {
            *dst = calloc( length + 1, 1 );
            memcpy( *dst, src, length );
        }
    }
    if ( out_length)
    {
        *out_length = length;
    }
}

static int pool_match( CPOOLHEAD *pooh,
           SQLCHAR *server_name,
           SQLSMALLINT name_length1,
           SQLCHAR *user_name,
           SQLSMALLINT name_length2,
           SQLCHAR *authentication,
           SQLSMALLINT name_length3,
           SQLCHAR *connect_string,
           SQLSMALLINT connect_string_length )
{
    int match = 1;

    if ( server_name )
    {
        if ( pooh -> server_length == 0 )
        {
            match = 0;
        }
        if ( pooh -> server_length != name_length1 ||
                sql_strcmp( server_name, (SQLCHAR*)pooh -> _server,
                    name_length1, pooh -> server_length ))
        {
            match = 0;
        }
        if ( pooh -> user_length != name_length2 ||
                sql_strcmp( user_name, (SQLCHAR*)pooh -> _user,
                    name_length2, pooh -> user_length ))
        {
            match = 0;
        }
        if ( pooh -> password_length != name_length3 ||
                sql_strcmp( authentication, (SQLCHAR*)pooh -> _password,
                    name_length3, pooh -> password_length ))
        {
            match = 0;
        }
    }
    else
    {
        if ( pooh -> dsn_length == 0 )
        {
            match = 0;
        }
        if ( pooh -> dsn_length != connect_string_length ||
                sql_strcmp( connect_string, (SQLCHAR*)pooh -> _driver_connect_string,
                    connect_string_length, pooh -> dsn_length ))
        {
            match = 0;
        }
    }
    return match;
}

/*
 * use this variable to spot odd behavour with memory and Python, that looks
 * like copies of the process memory are replicated and then wound backwards
 */

/*
static int ecount;

int display_pool( void )
{
    printf( "pool_head: %p %d\n", pool_head, ecount ++ );
    if ( pool_head ) {
        CPOOLHEAD *pptr;
        CPOOLENT *pent;

        pptr = pool_head;

        while( pptr ) {

            printf( "\tpptr: %p\n", pptr );
            printf( "\t\tdsn: %s\n", pptr -> _driver_connect_string );
            printf( "\t\tnum_entries: %d\n", pptr -> num_entries );
            printf( "\t\tentries: %p\n", pptr -> entries );
            printf( "\t\tnext: %p\n", pptr -> next );

            pent = pptr -> entries;
            while( pent ) {
                printf( "\t\t\tpent: %p\n", pent );
                printf( "\t\t\texpiry_time: %d\n", pent -> expiry_time );
                printf( "\t\t\tttl: %d\n", pent -> ttl );
                printf( "\t\t\tin_use: %d\n", pent -> in_use );
                printf( "\t\t\thead: %p\n", pent -> head );
                printf( "\t\t\tcursors: %d\n", pent -> cursors );
                printf( "\t\t\tconnection -> env: %p\n", pent -> connection.environment );
                printf( "\t\t\tconnection -> driver_env: %p\n", pent -> connection.driver_env );
                printf( "\t\t\tconnection -> driver_dbc: %p\n", pent -> connection.driver_dbc );
                printf( "\t\t\tnext: %p\n", pent -> next );
                printf( "\n" );

                pent = pent -> next;
            }

            pptr = pptr -> next;
            printf( "\n" );
        }
    }
}
*/

/*
 * Search for a matching connection from the pool
 * Removes expired connections too
Returns
    0: No connection found, max size not reached (reserves an entry)
    1: Connection found
    2: No connection found, max size reached (pool mutex remains locked)
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
           int retrying )
{
    time_t current_time;
    SQLULEN dead;
    CPOOLHEAD *ptrh, *prevh, *match_head;
    CPOOLENT *ptre, *preve;
    int has_checked = 0;

    if ( !retrying )
    {
        mutex_pool_entry();
    }

    current_time = time( NULL );

    /*
     * look in the list of connections for one that matches
     */

restart:;

    match_head = 0;
    for( ptrh = pool_head, prevh = NULL; ptrh; prevh = ptrh, ptrh = ptrh -> next )
    {
        SQLRETURN ret;
        int conn_match;

        conn_match = pool_match( ptrh,
            server_name,
            name_length1,
            user_name,
            name_length2,
            authentication,
            name_length3,
            connect_string,
            connect_string_length );

        if ( conn_match )
        {
            match_head = ptrh;
        }

        for ( ptre = ptrh -> entries, preve = NULL; ptre; preve = ptre, ptre = ptre -> next )
        {
            if ( ptre -> in_use )
            {
                continue;
            }

            /*
             * has it been previously stripped
             */

            if ( ptre -> connection.environment == NULL ) 
            {
                if ( ptre == ptrh -> entries ) /* head of the list ? */
                {
                    ptrh -> entries = ptre -> next;
                }
                else
                {
                    preve -> next = ptre -> next;
                }
                free( ptre );
                ptrh -> num_entries --;
                pool_signal();

                if ( ! ptrh -> num_entries ) /* free the head too */
                {
                    if ( prevh )
                    {
                        prevh -> next = ptrh -> next;
                    }
                    else
                    {
                        pool_head = ptrh -> next;
                    }
                    if ( ptrh -> _driver_connect_string ) {
                        free( ptrh -> _driver_connect_string );
                    }
                    if ( ptrh -> _server ) {
                        free( ptrh -> _server );
                    }
                    if ( ptrh -> _user ) {
                        free( ptrh -> _user );
                    }
                    if ( ptrh -> _password ) {
                        free( ptrh -> _password );
                    }
                    free( ptrh );
                }
                goto restart;
            }

            /*
             * has it expired ? Do some cleaning up first
             */

            if ( ptre -> expiry_time < current_time && ptre -> in_use == 0 )
            {
                /*
                 * disconnect and remove
                 */
disconnect_and_remove:
                close_pooled_connection( ptre );

                if ( ptre == ptrh -> entries ) /* head of the list ? */
                {
                    ptrh -> entries = ptre -> next;
                }
                else
                {
                    preve -> next = ptre -> next;
                }
                free( ptre );
                ptrh -> num_entries --;
                pool_signal();

                if ( ! ptrh -> num_entries ) /* free the head too */
                {
                    if ( prevh )
                    {
                        prevh -> next = ptrh -> next;
                    }
                    else
                    {
                        pool_head = ptrh -> next;
                    }
                    if ( ptrh -> _driver_connect_string ) {
                        free( ptrh -> _driver_connect_string );
                    }
                    if ( ptrh -> _server ) {
                        free( ptrh -> _server );
                    }
                    if ( ptrh -> _user ) {
                        free( ptrh -> _user );
                    }
                    if ( ptrh -> _password ) {
                        free( ptrh -> _password );
                    }
                    free( ptrh );
                }
                goto restart;
            }

            /*
             * has the time-to-live got to one ?
             */

            if ( ptre -> ttl == 1 )
            {
                goto disconnect_and_remove;
            }
            else if (  ptre -> ttl > 1 )
            {
                ptre -> ttl --;
            }

            /*
             * does it match ?
             */
            if ( !conn_match )
            {
                continue;
            }

            /*
             * is it the same cursor usage ?
             */

            if ( ptre -> cursors != connection -> cursors )
            {
                continue;
            }

            /*
             * we are spanning env's
             */

            if ( ptre -> connection.environment && ptre -> connection.environment != connection -> environment )
            {
                continue;
            }

            /*
             * ok so far, is it still alive ?
             */
            has_checked = 0;

            /*
             * A pointer to memory in which to return the current value of the attribute specified by Attribute. 
             * For integer-type attributes, some drivers may only write the lower 32-bit or 16-bit of a buffer 
             * and leave the higher-order bit unchanged. Therefore, applications should use a buffer of SQLULEN 
             * and initialize the value to 0 before calling this function.
             */
            dead = 0;

            if ( CHECK_SQLGETCONNECTATTR(( &ptre -> connection )))
            {
                ret = SQLGETCONNECTATTR(( &ptre -> connection ),
                        ptre -> connection.driver_dbc,
                        SQL_ATTR_CONNECTION_DEAD,
                        &dead,
                        SQL_IS_INTEGER,
                        0 );
                if ( SQL_SUCCEEDED( ret )) 
                {
                    has_checked = 1;
                    if ( dead == SQL_CD_TRUE )
                    {
                        goto disconnect_and_remove;
                    }
                }
            }
            if ( !has_checked && CHECK_SQLGETCONNECTATTRW(( &ptre -> connection )))
            {
                ret = SQLGETCONNECTATTRW(( &ptre -> connection ),
                        ptre -> connection.driver_dbc,
                        SQL_ATTR_CONNECTION_DEAD,
                        &dead,
                        SQL_IS_INTEGER,
                        0 );
                if ( SQL_SUCCEEDED( ret )) 
                {
                    has_checked = 1;
                    if ( dead == SQL_CD_TRUE )
                    {
                        goto disconnect_and_remove;
                    }
                }
            }
            if ( !has_checked && CHECK_SQLGETCONNECTOPTION(( &ptre -> connection ))) 
            {
                    ret = SQLGETCONNECTOPTION(( &ptre -> connection ),
                        ptre -> connection.driver_dbc,
                        SQL_ATTR_CONNECTION_DEAD,
                        &dead );
                if ( SQL_SUCCEEDED( ret )) 
                {
                    has_checked = 1;
                    if ( dead == SQL_CD_TRUE )
                    {
                        goto disconnect_and_remove;
                    }
                }
            }
            if ( !has_checked && CHECK_SQLGETCONNECTOPTIONW(( &ptre -> connection ))) 
            {
                    ret = SQLGETCONNECTOPTIONW(( &ptre -> connection ),
                        ptre -> connection.driver_dbc,
                        SQL_ATTR_CONNECTION_DEAD,
                        &dead );
                if ( SQL_SUCCEEDED( ret )) 
                {
                    has_checked = 1;
                    if ( dead == SQL_CD_TRUE )
                    {
                        goto disconnect_and_remove;
                    }
                }
            }

            /*
             * Need some other way of checking, This isn't safe to pool...
             * But it needs to be something thats not slower than connecting...
             * I have put this off, so its after the check that the server_name and all
             * the rest is ok to avoid waiting time, as the check could take time
             */

            if ( !has_checked )
            {
                if ( strlen( connection -> probe_sql ) > 0 )
                {
                    /*
                     * Execute the query, check we have all we need
                     */

                    if ( CHECK_SQLEXECDIRECT(( &ptre -> connection )) &&
                            (  CHECK_SQLALLOCHANDLE(( &ptre -> connection )) || CHECK_SQLALLOCSTMT(( &ptre -> connection ))) &&
                            CHECK_SQLNUMRESULTCOLS(( &ptre -> connection )) &&
                            CHECK_SQLFETCH(( &ptre -> connection )) &&
                            CHECK_SQLFREESTMT(( &ptre -> connection )))
                    {
                        DMHSTMT statement;
                        int ret;
                        int check_failed = 0;

                        statement = __alloc_stmt();

                        if ( CHECK_SQLALLOCHANDLE(( &ptre -> connection )))
                        {
                            ret = SQLALLOCHANDLE(( &ptre -> connection ),
                                SQL_HANDLE_STMT,
                                ptre -> connection.driver_dbc,
                                ( &statement -> driver_stmt ),
                                statement );

                        }
                        else
                        {
                            ret = SQLALLOCSTMT(( &ptre -> connection ),
                                    ptre -> connection.driver_dbc,
                                    ( &statement -> driver_stmt ),
                                    statement );
                        }

                        if ( !SQL_SUCCEEDED( ret ))
                        {
                            check_failed = 1;
                        }
                        else
                        {
                            ret = SQLEXECDIRECT(( &ptre -> connection ),
                                    statement -> driver_stmt,
                                    connection -> probe_sql,
                                    SQL_NTS );

                            if ( !SQL_SUCCEEDED( ret ))
                            {
                                check_failed = 1;
                            }
                            else
                            {
                                SQLSMALLINT column_count;

                                /*
                                 * Check if there is a result set
                                 */

                                ret = SQLNUMRESULTCOLS(( &ptre -> connection ),
                                    statement -> driver_stmt,
                                    &column_count );

                                if ( !SQL_SUCCEEDED( ret ))
                                {
                                    check_failed = 1;
                                }
                                else if ( column_count > 0 )
                                {
                                    do
                                    {
                                        ret = SQLFETCH(( &ptre -> connection ),
                                            statement -> driver_stmt );
                                    }
                                    while( SQL_SUCCEEDED( ret ));

                                    if ( ret != SQL_NO_DATA )
                                    {
                                        check_failed = 1;
                                    }

                                    ret = SQLFREESTMT(( &ptre -> connection ),
                                        statement -> driver_stmt,
                                        SQL_CLOSE );

                                    if ( !SQL_SUCCEEDED( ret ))
                                    {
                                        check_failed = 1;
                                    }
                                }
                            }

                            ret = SQLFREESTMT(( &ptre -> connection ),
                                statement -> driver_stmt,
                                SQL_DROP );

                            if ( !SQL_SUCCEEDED( ret ))
                            {
                                check_failed = 1;
                            }
                        }

                        __release_stmt( statement );

                        if ( check_failed )
                        {
                            goto disconnect_and_remove;
                        }
                        else
                        {
                            has_checked = 1;
                        }
                    }
                }
            }

            if ( !has_checked )
            {
                /*
                 * We can't know for sure if the connection is still valid ...
                 */
            }

            /*
             * at this point we have something that should work, lets use it
             */

            ptre -> in_use = 1;
            ptre -> expiry_time = current_time + ptre -> timeout;
            connection -> pooling_timeout = ptre -> timeout;

            /*
             * copy all the info over
             */

            connection -> pooled_connection = ptre;

            connection -> state = ptre -> connection.state;
            connection -> dl_handle = ptre -> connection.dl_handle;
            connection -> functions = ptre -> connection.functions;
            connection -> unicode_driver = ptre -> connection.unicode_driver;
            connection -> driver_env = ptre -> connection.driver_env;
            connection -> driver_dbc = ptre -> connection.driver_dbc;
            connection -> driver_version = ptre -> connection.driver_version;
            connection -> driver_act_ver = ptre -> connection.driver_act_ver;
            connection -> statement_count = 0;

            connection -> access_mode = ptre -> connection.access_mode;
            connection -> access_mode_set = ptre -> connection.access_mode_set;
            connection -> login_timeout = ptre -> connection.login_timeout;
            connection -> login_timeout_set = ptre -> connection.login_timeout_set;
            connection -> auto_commit = ptre -> connection.auto_commit;
            connection -> auto_commit_set = ptre -> connection.auto_commit_set;
            connection -> async_enable = ptre -> connection.async_enable;
            connection -> async_enable_set = ptre -> connection.async_enable_set;
            connection -> auto_ipd = ptre -> connection.auto_ipd;
            connection -> auto_ipd_set = ptre -> connection.auto_ipd_set;
            connection -> connection_timeout = ptre -> connection.connection_timeout;
            connection -> connection_timeout_set = ptre -> connection.connection_timeout_set;
            connection -> metadata_id = ptre -> connection.metadata_id;
            connection -> metadata_id_set = ptre -> connection.metadata_id_set;
            connection -> packet_size = ptre -> connection.packet_size;
            connection -> packet_size_set = ptre -> connection.packet_size_set;
            connection -> quite_mode = ptre -> connection.quite_mode;
            connection -> quite_mode_set = ptre -> connection.quite_mode_set;
            connection -> txn_isolation = ptre -> connection.txn_isolation;
            connection -> txn_isolation_set = ptre -> connection.txn_isolation_set;

            connection -> cursors = ptre -> connection.cursors;
            connection -> cl_handle = ptre -> connection.cl_handle;

            connection -> env_list_ent = ptre -> connection.env_list_ent;
            strcpy( connection -> probe_sql, ptre -> connection.probe_sql );

            connection -> ex_fetch_mapping = ptre -> connection.ex_fetch_mapping;
            connection -> dont_dlclose = ptre -> connection.dont_dlclose;
            connection -> bookmarks_on = ptre -> connection.bookmarks_on;

#ifdef HAVE_ICONV
            connection -> iconv_cd_uc_to_ascii = ptre -> connection.iconv_cd_uc_to_ascii;
            connection -> iconv_cd_ascii_to_uc = ptre -> connection.iconv_cd_ascii_to_uc;
#endif

            /*
             * copy current environment into the pooled connection
             */

            ptre -> connection.environment = connection -> environment;

            strcpy( connection -> dsn, ptre -> connection.dsn );

#if defined( HAVE_LIBPTH ) || defined( HAVE_LIBPTHREAD ) || defined( HAVE_LIBTHREAD )
            dbc_change_thread_support(connection, ptre -> connection.protection_level);
#endif

            mutex_pool_exit();

            return TRUE;
        }
    }

    /* this head is the right one, but is it full ? */
    if ( match_head )
    {
        if ( pool_max_size && match_head -> num_entries >= pool_max_size )
        {
            /* Note we do NOT exit pool mutex here, as wait will exit it automatically */
            return 2;
        }
        match_head -> num_entries ++; /* reserve an entry */
    }
    else
    {
        /* add an empty head with 1 reserved entry */
        CPOOLHEAD *newhead = calloc( sizeof( CPOOLHEAD ), 1 );
        if ( newhead )
        {
            copy_nts( &(newhead -> _server), server_name, &newhead -> server_length, name_length1 );
            copy_nts( &(newhead -> _user), user_name, &newhead -> user_length, name_length2 );
            copy_nts( &(newhead -> _password), authentication, &newhead -> password_length, name_length3 );
            copy_nts( &(newhead -> _driver_connect_string), connect_string, &newhead -> dsn_length, connect_string_length );

            newhead -> num_entries = 1; /* reserve an entry */

            newhead -> next = pool_head;
            pool_head = newhead;
            match_head = newhead;
        }
    }

    if ( pooh )
    {
        *pooh = match_head;
    }

    mutex_pool_exit();
    return FALSE;
}

int add_to_pool ( DMHDBC connection, CPOOLHEAD *pooh )
{
    CPOOLENT *ptr;
    time_t current_time;

    mutex_pool_entry();

    current_time = time( NULL );

    /* Should be new entry */
    ptr = calloc( sizeof( CPOOLENT ), 1 );
    if ( !ptr )
    {
        mutex_pool_exit();
        return FALSE;
    }

    /* Copy info */
    ptr -> in_use = 1;
    ptr -> expiry_time = current_time + connection -> pooling_timeout;
    ptr -> timeout = connection -> pooling_timeout;
    ptr -> ttl = connection -> ttl;
    ptr -> cursors = connection -> cursors;

    ptr -> connection.state = connection -> state;
    ptr -> connection.dl_handle = connection -> dl_handle;
    ptr -> connection.functions = connection -> functions;
    ptr -> connection.driver_env = connection -> driver_env;
    ptr -> connection.driver_dbc = connection -> driver_dbc;
    ptr -> connection.driver_version = connection -> driver_version;
    ptr -> connection.driver_act_ver = connection -> driver_act_ver;

    ptr -> connection.access_mode = connection -> access_mode;
    ptr -> connection.access_mode_set = connection -> access_mode_set;
    ptr -> connection.login_timeout = connection -> login_timeout;
    ptr -> connection.login_timeout_set = connection -> login_timeout_set;
    ptr -> connection.auto_commit = connection -> auto_commit;
    ptr -> connection.auto_commit_set = connection -> auto_commit_set;
    ptr -> connection.async_enable = connection -> async_enable;
    ptr -> connection.async_enable_set = connection -> async_enable_set;
    ptr -> connection.auto_ipd = connection -> auto_ipd;
    ptr -> connection.auto_ipd_set = connection -> auto_ipd_set;
    ptr -> connection.connection_timeout = connection -> connection_timeout;
    ptr -> connection.connection_timeout_set = connection -> connection_timeout_set;
    ptr -> connection.metadata_id = connection -> metadata_id;
    ptr -> connection.metadata_id_set = connection -> metadata_id_set;
    ptr -> connection.packet_size = connection -> packet_size;
    ptr -> connection.packet_size_set = connection -> packet_size_set;
    ptr -> connection.quite_mode = connection -> quite_mode;
    ptr -> connection.quite_mode_set = connection -> quite_mode_set;
    ptr -> connection.txn_isolation = connection -> txn_isolation;
    ptr -> connection.txn_isolation_set = connection -> txn_isolation_set;
    ptr -> connection.unicode_driver = connection ->unicode_driver;

    ptr -> connection.cursors = connection -> cursors;
    ptr -> connection.cl_handle = connection -> cl_handle;

#ifdef HAVE_LIBPTHREAD
    ptr -> connection.mutex = connection -> mutex;
    ptr -> connection.protection_level = connection -> protection_level;
#elif HAVE_LIBTHREAD
    ptr -> connection.mutex = connection -> mutex;
    ptr -> connection.protection_level = connection -> protection_level;
#endif

    ptr -> connection.pooling_timeout = ptr -> timeout;

    ptr -> connection.ex_fetch_mapping = connection -> ex_fetch_mapping;
    ptr -> connection.dont_dlclose = connection -> dont_dlclose;
    ptr -> connection.bookmarks_on = connection -> bookmarks_on;

    ptr -> connection.env_list_ent = connection -> env_list_ent;
    ptr -> connection.environment = connection -> environment;
    strcpy( ptr -> connection.probe_sql, connection -> probe_sql );

#ifdef HAVE_ICONV
    ptr -> connection.iconv_cd_uc_to_ascii = connection -> iconv_cd_uc_to_ascii;
    ptr -> connection.iconv_cd_ascii_to_uc = connection -> iconv_cd_ascii_to_uc;
    connection -> iconv_cd_uc_to_ascii = (iconv_t) -1;
    connection -> iconv_cd_ascii_to_uc = (iconv_t) -1;
#endif

    /*
     * add to the list
     * no need to increment count, since that was reserved in search_for_pool
     */
    ptr -> head = pooh;
    ptr -> next = pooh -> entries;
    pooh -> entries = ptr;

    connection -> pooled_connection = ptr;
    mutex_pool_exit();
    return TRUE;
}

void return_to_pool( DMHDBC connection )
{
    CPOOLENT *ptr;
    time_t current_time;

    mutex_pool_entry();

    ptr = connection -> pooled_connection;
    current_time = time( NULL );

    /*
     * is it a old entry ?
     */

    if ( connection -> pooled_connection )
    {
        ptr -> in_use = 0;
        ptr -> expiry_time = current_time + ptr -> timeout;
#ifdef HAVE_ICONV
	connection -> iconv_cd_uc_to_ascii = (iconv_t) -1;
	connection -> iconv_cd_ascii_to_uc = (iconv_t) -1;
#endif
    }

    /*
     * allow the driver to reset itself if it's a 3.8 driver
     */

    if ( connection -> driver_version == SQL_OV_ODBC3_80 ) 
    {
        if ( CHECK_SQLSETCONNECTATTR( connection ))
        {
            SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_RESET_CONNECTION,
                    (SQLPOINTER)(intptr_t) SQL_RESET_CONNECTION_YES,
                    0 );
        }
        else if ( CHECK_SQLSETCONNECTATTRW( connection ))
        {
            SQLSETCONNECTATTRW( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_RESET_CONNECTION,
                    (SQLPOINTER)(intptr_t) SQL_RESET_CONNECTION_YES,
                    0 );
        }
    }

    if ( connection -> cl_handle ) 
    {
        /*
         * slight hack to warn the cursor lib the connection is going away 
         */

        SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_RESET_CONNECTION,
                    (SQLPOINTER)(intptr_t) 2,
                    0 );
    }

    /*
     * remove all information from the connection
     */

    connection -> state = STATE_C2;
    connection -> driver_env = 0;
    connection -> driver_dbc = 0;
    connection -> dl_handle = 0;
    connection -> cl_handle = 0;
    connection -> functions = 0;
    connection -> pooled_connection = 0;

    pool_signal();

    mutex_pool_exit();
}

void __handle_attr_extensions( DMHDBC connection, char *dsn, char *driver_name )
{
    char txt[ 1024 ];

    if ( dsn && strlen( dsn ))
    {
        SQLGetPrivateProfileString( dsn, "DMEnvAttr", "",
                    txt, sizeof( txt ), 
                    "ODBC.INI" );

        if ( strlen( txt ))
        {
            __parse_attribute_string( &connection -> env_attribute,
                txt, strlen( txt ));
        }

        SQLGetPrivateProfileString( dsn, "DMConnAttr", "",
                    txt, sizeof( txt ), 
                    "ODBC.INI" );

        if ( strlen( txt ))
        {
            __parse_attribute_string( &connection -> dbc_attribute,
                txt, strlen( txt ));
        }

        SQLGetPrivateProfileString( dsn, "DMStmtAttr", "",
                    txt, sizeof( txt ), 
                    "ODBC.INI" );

        if ( strlen( txt ))
        {
            __parse_attribute_string( &connection -> stmt_attribute,
                txt, strlen( txt ));
        }
    }

    if ( driver_name && strlen( driver_name ))
    {
        SQLGetPrivateProfileString( driver_name, "DMEnvAttr", "",
                          txt, sizeof( txt ), 
                          "ODBCINST.INI" );
     
        if ( strlen( txt ))
        {
            __parse_attribute_string( &connection -> env_attribute,
                         txt, strlen( txt ));
        }
    }
}

SQLRETURN SQLConnectA( SQLHDBC connection_handle,
           SQLCHAR *server_name,
           SQLSMALLINT name_length1,
           SQLCHAR *user_name,
           SQLSMALLINT name_length2,
           SQLCHAR *authentication,
           SQLSMALLINT name_length3 )
{
    return SQLConnect( connection_handle,
                        server_name,
                        name_length1,
                        user_name,
                        name_length2,
                        authentication,
                        name_length3 );
}

SQLRETURN SQLConnect( SQLHDBC connection_handle,
           SQLCHAR *server_name,
           SQLSMALLINT name_length1,
           SQLCHAR *user_name,
           SQLSMALLINT name_length2,
           SQLCHAR *authentication,
           SQLSMALLINT name_length3 )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    int len, ret_from_connect;
    char dsn[ SQL_MAX_DSN_LENGTH + 1 ];
    char lib_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    char driver_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ], s2[ 100 + LOG_MESSAGE_LEN ], s3[ 100 + LOG_MESSAGE_LEN ];
    int warnings;
    CPOOLHEAD *pooh = 0;

    /*
     * check connection
     */
    if ( !__validate_dbc( connection ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }

    function_entry( connection );

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, "\n\t\tEntry:\
\n\t\t\tConnection = %p\
\n\t\t\tServer Name = %s\
\n\t\t\tUser Name = %s\
\n\t\t\tAuthentication = %s",
                connection,
                __string_with_length( s1, server_name, name_length1 ),
                __string_with_length( s2, user_name, name_length2 ),
                __string_with_length_pass( s3, authentication, name_length3 ));

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if (( name_length1 < 0 && name_length1 != SQL_NTS ) ||
        ( name_length2 < 0 && name_length2 != SQL_NTS ) ||
        ( name_length3 < 0 && name_length3 != SQL_NTS ))

    {
        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: HY090" );

        __post_internal_error( &connection -> error,
                ERROR_HY090, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    /*
     * check the state of the connection
     */
    if ( connection -> state != STATE_C2 )
    {
        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: 08002" );

        __post_internal_error( &connection -> error,
                ERROR_08002, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    if ( name_length1 && server_name )
    {
        if ( name_length1 == SQL_NTS )
        {
            len = strlen((char*) server_name );

            if ( len > SQL_MAX_DSN_LENGTH )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: HY090" );

                __post_internal_error( &connection -> error,
                        ERROR_HY090, NULL,
                        connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }
        }
        else
        {
            len = name_length1;

            if ( len > SQL_MAX_DSN_LENGTH )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: HY090" );

                __post_internal_error( &connection -> error,
                        ERROR_HY090, NULL,
                        connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }
        }

        memcpy( dsn, server_name, len );
        dsn[ len ] ='\0';
    }
    else if ( name_length1 > SQL_MAX_DSN_LENGTH )
    {
        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: IM010" );

        __post_internal_error( &connection -> error,
                ERROR_IM010, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }
    else
    {
        strcpy( dsn, "DEFAULT" );
    }

    /*
     * can we find a pooled connection to use here ?
     */

    connection -> pooled_connection = NULL;

    if ( pooling_enabled )
    {
        int retpool;
        int retrying = 0;
        time_t wait_begin = time( NULL );

retry:
        retpool = search_for_pool(  connection,
                                    server_name, name_length1,
                                    user_name, name_length2,
                                    authentication, name_length3,
                                    NULL, 0, &pooh, retrying );
        /*
         * found usable existing connection from pool
         */
        if ( retpool == 1 )
        {
            ret_from_connect = SQL_SUCCESS;

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg,
                        "\n\t\tExit:[%s]",
                            __get_return_status( ret_from_connect, s1 ));

                dm_log_write( __FILE__,
                            __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        connection -> msg );
            }

            connection -> state = STATE_C4;

            return function_return_nodrv( SQL_HANDLE_DBC, connection, ret_from_connect );
        }

        /*
         * pool is at capacity
         */
        if ( retpool == 2 )
        {
            /*
             * either no timeout or exceeded the timeout
             */
            if ( ! pool_wait_timeout || time( NULL ) - wait_begin > pool_wait_timeout )
            {
                mutex_pool_exit();
                dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: HYT02" );

                __post_internal_error( &connection -> error,
                    ERROR_HYT02, NULL,
                    connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            /*
             * wait up to 1 second for a signal and try again
             */
            pool_timedwait( connection );
            retrying = 1;
            goto retry;
        }

        /*
         * 1 pool entry has been reserved. Early exits henceforth need to unreserve.
         */
    }

    /*
     * else safe the info for later
     */

    if ( pooling_enabled )
    {
        connection -> dsn_length = 0;

        if ( server_name )
        {
            if ( name_length1 < 0 )
            {
                connection -> _server = strdup((char*)server_name );
            }
            else
            {
                connection -> _server = malloc( name_length1 );
                memcpy( connection -> _server, server_name, name_length1 );
            }
        }
        else
        {
            connection -> _server = malloc( 1 );
            strcpy( connection -> _server, "" );
        }
        connection -> server_length = name_length1;

        if ( user_name )
        {
            if ( name_length2 < 0 )
            {
                connection -> _user = strdup((char*)user_name );
            }
            else
            {
                connection -> _user = malloc( name_length2 );
                memcpy( connection -> _user, user_name, name_length2 );
            }
        }
        else
        {
            connection -> _user = malloc( 1 );
            strcpy( connection -> _user, "" );
        }
        connection -> user_length = name_length2;

        if ( authentication )
        {
            if ( name_length3 < 0 )
            {
                connection -> _password = strdup((char*)authentication );
            }
            else
            {
                connection -> _password = malloc( name_length3 );
                memcpy( connection -> _password, authentication, name_length3 );
            }
        }
        else
        {
            connection -> _password = malloc( 1 );
            strcpy( connection -> _password, "" );
        }
        connection -> password_length = name_length3;
    }

    if ( !*dsn || !__find_lib_name( dsn, lib_name, driver_name ))
    {
        /*
         * if not found look for a default
         */

        if ( !__find_lib_name( "DEFAULT", lib_name, driver_name ))
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: IM002" );

            __post_internal_error( &connection -> error,
                    ERROR_IM002, NULL,
                    connection -> environment -> requested_version );

            pool_unreserve( pooh );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }
    }


    /*
     * do we have any Environment, Connection, or Statement attributes set in the ini ?
     */

    __handle_attr_extensions( connection, dsn, driver_name );

    /*
     * if necessary change the threading level
     */

    warnings = 0;

    if ( !__connect_part_one( connection, lib_name, driver_name, &warnings ))
    {
        __disconnect_part_four( connection );       /* release unicode handles */

        pool_unreserve( pooh );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    if ( !CHECK_SQLCONNECT( connection ) &&
        !CHECK_SQLCONNECTW( connection ))
    {
        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: IM001" );

        __disconnect_part_one( connection );
        __disconnect_part_four( connection );       /* release unicode handles */
        __post_internal_error( &connection -> error,
                ERROR_IM001, NULL,
                connection -> environment -> requested_version );

        pool_unreserve( pooh );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    if ( CHECK_SQLCONNECT( connection ))
    {
        /*
        if ( CHECK_SQLSETCONNECTATTR( connection ))
        {
            int lret;
                
            lret = SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_ANSI_APP,
                    SQL_AA_TRUE,
                    0 );
        }
        */

        ret_from_connect = SQLCONNECT( connection,
                connection -> driver_dbc,
                (SQLCHAR*) dsn, SQL_NTS,
                user_name, name_length2,
                authentication, name_length3 );

        if ( ret_from_connect != SQL_SUCCESS )
        {
            SQLCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            /*
             * get the errors from the driver before
             * loseing the connection 
             */

            if ( CHECK_SQLERROR( connection ))
            {
                do
                {
                    ret = SQLERROR( connection,
                            SQL_NULL_HENV,
                            connection -> driver_dbc,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );

                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                                sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLGETDIAGREC( connection ))
            {
                int rec = 1;

                do
                {
                    ret = SQLGETDIAGREC( connection,
                            SQL_HANDLE_DBC,
                            connection -> driver_dbc,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                            sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
        }

        /* 
         * if it was a error then return now
         */

        if ( !SQL_SUCCEEDED( ret_from_connect ))
        {
            __disconnect_part_one( connection );
            __disconnect_part_four( connection );

            sprintf( connection -> msg,
                    "\n\t\tExit:[%s]",
                        __get_return_status( ret_from_connect, s1 ));

            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    connection -> msg );

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, ret_from_connect, DEFER_R0 );
        }

	    connection -> unicode_driver = 0;
    }
    else
    {
        SQLWCHAR * uc_dsn, *uc_user, *uc_auth;

        uc_dsn = ansi_to_unicode_alloc((SQLCHAR*) dsn, SQL_NTS, connection, NULL );
        uc_user = ansi_to_unicode_alloc( user_name, name_length2, connection, NULL );
        uc_auth = ansi_to_unicode_alloc( authentication, name_length3, connection, NULL );

        if ( CHECK_SQLSETCONNECTATTR( connection ))
        {
            SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_ANSI_APP,
                    SQL_AA_FALSE,
                    0 );
        }

        ret_from_connect = SQLCONNECTW( connection,
                connection -> driver_dbc,
                uc_dsn, SQL_NTS,
                uc_user, name_length2,
                uc_auth, name_length3 );

        if ( uc_dsn )
            free( uc_dsn );
        if ( uc_user )
            free( uc_user );
        if ( uc_auth )
            free( uc_auth );

        if ( ret_from_connect != SQL_SUCCESS )
        {
            SQLWCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLWCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            /*
             * get the errors from the driver before
             * loseing the connection 
             */

            if ( CHECK_SQLERRORW( connection ))
            {
                do
                {
                    ret = SQLERRORW( connection,
                            SQL_NULL_HENV,
                            connection -> driver_dbc,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text )/sizeof(SQLWCHAR),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        SQLCHAR *as1, *as2; 

                        __post_internal_error_ex_w( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        as1 = (SQLCHAR *) unicode_to_ansi_alloc( sqlstate, SQL_NTS, connection, NULL );
                        as2 = (SQLCHAR *) unicode_to_ansi_alloc( message_text, SQL_NTS, connection, NULL );

                        if ( as1 && as2 ) {
                            sprintf( connection -> msg, "\t\tDIAG [%s] %s", as1, as2 );
                        }

                        if ( as1 ) free( as1 );
                        if ( as2 ) free( as2 );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLGETDIAGRECW( connection ))
            {
                int rec = 1;

                do
                {

                    ret = SQLGETDIAGRECW( connection,
                            SQL_HANDLE_DBC,
                            connection -> driver_dbc,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text )/sizeof(SQLWCHAR),
                            &ind );

                    if ( SQL_SUCCEEDED( ret ))
                    {
                        SQLCHAR *as1, *as2; 

                        __post_internal_error_ex_w( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        as1 = (SQLCHAR *) unicode_to_ansi_alloc( sqlstate, SQL_NTS, connection, NULL );
                        as2 = (SQLCHAR *) unicode_to_ansi_alloc( message_text, SQL_NTS, connection, NULL );

                        if (as1 && as2)
                                sprintf( connection -> msg, "\t\tDIAG [%s] %s", as1, as2 );

                        if ( as1 ) free( as1 );
                        if ( as2 ) free( as2 );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
        }

        /* 
         * if it was a error then return now
         */

        if ( !SQL_SUCCEEDED( ret_from_connect ))
        {
            __disconnect_part_one( connection );
            __disconnect_part_four( connection );

            sprintf( connection -> msg,
                    "\n\t\tExit:[%s]",
                        __get_return_status( ret_from_connect, s1 ));

            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    connection -> msg );

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, ret_from_connect, DEFER_R0 );
        }

        connection -> unicode_driver = 1;
    }

    /*
     * we should be connected now
     */
    connection -> state = STATE_C4;
    strcpy( connection -> dsn, dsn );

    /*
     * did we get the type we wanted
     */

    if ( connection -> driver_version !=
            connection -> environment -> requested_version )
    {
        connection -> driver_version =
            connection -> environment -> requested_version;

        __post_internal_error( &connection -> error,
                ERROR_01000, "Driver does not support the requested version",
                connection -> environment -> requested_version );
        ret_from_connect = SQL_SUCCESS_WITH_INFO;
    }

    if ( !__connect_part_two( connection ))
    {
        /*
         * the cursor lib can kill us here, so be careful
         */

        __disconnect_part_two( connection );
        __disconnect_part_one( connection );
        __disconnect_part_four( connection );

        connection -> state = STATE_C3;

        pool_unreserve( pooh );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg,
                "\n\t\tExit:[%s]",
                    __get_return_status( ret_from_connect, s1 ));

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                connection -> msg );
    }

    if ( warnings && ret_from_connect == SQL_SUCCESS )
    {
        ret_from_connect = SQL_SUCCESS_WITH_INFO;
    }

    if ( pooling_enabled && !add_to_pool( connection, pooh ) )
    {
        pool_unreserve( pooh );
    }

    return function_return_nodrv( SQL_HANDLE_DBC, connection, ret_from_connect );
}

/*
 * connection pooling setup, just stubs for the moment
 */

BOOL ODBCSetTryWaitValue ( DWORD dwValue )
{
	return 0;
}

#ifdef __cplusplus
DWORD ODBCGetTryWaitValue ( )
#else
DWORD ODBCGetTryWaitValue ( void )
#endif
{
	return 0;
}
