/**************************************************
 * isql
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under GPL 18.FEB.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlext.h>

#ifdef HAVE_STRTOL

char *szSyntax =
"\n"
"**********************************************\n"
"* unixODBC - isql and iusql                  *\n"
"**********************************************\n"
"* Syntax                                     *\n"
"*                                            *\n"
"*      isql DSN [UID [PWD]] [options]        *\n"
"*                                            *\n"
"*      iusql DSN [UID [PWD]] [options]       *\n"
"*      iusql \"Connection String\" [options]   *\n"
"*                                            *\n"
"* Options                                    *\n"
"*                                            *\n"
"* -b         batch.(no prompting etc)        *\n"
"* -i         unverbose.(no help etc)         *\n"
"* -dx        delimit columns with x          *\n"
"* -x0xXX     delimit columns with XX, where  *\n"
"*            x is in hex, ie 0x09 is tab     *\n"
"* -w         wrap results in an HTML table   *\n"
"* -c         column names on first row.      *\n"
"*            (only used when -d)             *\n"
"* -mn        limit column display width to n *\n"
"* -v         verbose.                        *\n"
"* -lx        set locale to x                 *\n"
"* -q         wrap char fields in dquotes     *\n"
"* -3         Use ODBC 3 calls                *\n"
"* -n         Use new line processing         *\n"
"* -e         Use SQLExecDirect not Prepare   *\n"
"* -k         Use SQLDriverConnect            *\n"
"* -L         Length of col display (def:300) *\n"
"* --version  version                         *\n"
"*                                            *\n"
"* Commands                                   *\n"
"*                                            *\n"
"* help - list tables                         *\n"
"* help table - list columns in table         *\n"
"* help help - list all help options          *\n"
"*                                            *\n"
"* Examples                                   *\n"
"*                                            *\n"
"*      iusql -v WebDB MyID MyPWD -w < My.sql *\n"
"*                                            *\n"
"*      Each line in My.sql must contain      *\n"
"*      exactly 1 SQL command except for the  *\n"
"*      last line which must be blank (unless *\n"
"*      -n option specified).                 *\n"
"*                                            *\n"
"* Datasources, drivers, etc:                 *\n"
"*                                            *\n"
"*      See \"man 1 isql\"                      *\n"
"*                                            *\n"
"* Please visit;                              *\n"
"*                                            *\n"
"*      http://www.unixodbc.org               *\n"
"*      nick@lurcher.org                      *\n"
"*      pharvey@codebydesign.com              *\n"
"**********************************************\n\n";

#else

char *szSyntax =
"\n"
"**********************************************\n"
"* unixODBC - isql and iusql                  *\n"
"**********************************************\n"
"* Syntax                                     *\n"
"*                                            *\n"
"*      isql DSN [UID [PWD]] [options]        *\n"
"*                                            *\n"
"*      iusql DSN [UID [PWD]] [options]       *\n"
"*                                            *\n"
"* Options                                    *\n"
"*                                            *\n"
"* -b         batch.(no prompting etc)        *\n"
"* -i         unverbose.(no help etc)         *\n"
"* -dx        delimit columns with x          *\n"
"* -x0xXX     delimit columns with XX, where  *\n"
"*            x is in hex, ie 0x09 is tab     *\n"
"* -w         wrap results in an HTML table   *\n"
"* -c         column names on first row.      *\n"
"*            (only used when -d)             *\n"
"* -mn        limit column display width to n *\n"
"* -v         verbose.                        *\n"
"* -q         wrap char fields in dquotes     *\n"
"* --version  version                         *\n"
"*                                            *\n"
"* Commands                                   *\n"
"*                                            *\n"
"* help - list tables                         *\n"
"* help table - list columns in table         *\n"
"* help help - list all help options          *\n"
"*                                            *\n"
"* Examples                                   *\n"
"*                                            *\n"
"*      iusql -v WebDB MyID MyPWD -w < My.sql *\n"
"*                                            *\n"
"*      Each line in My.sql must contain      *\n"
"*      exactly 1 SQL command except for the  *\n"
"*      last line which must be blank.        *\n"
"*                                            *\n"
"* Datasources, drivers, etc:                 *\n"
"*                                            *\n"
"*      See \"man 1 isql\"                      *\n"
"*                                            *\n"
"* Please visit;                              *\n"
"*                                            *\n"
"*      http://www.unixodbc.org               *\n"
"*      nick@lurcher.org                      *\n"
"*      pharvey@codebydesign.com              *\n"
"**********************************************\n\n";

#endif

#define MAX_DATA_WIDTH 300

#ifndef max
#define max( a, b ) (((a) > (b)) ? (a) : (b))
#endif

#ifndef min
#define min( a, b ) (((a) < (b)) ? (a) : (b))
#endif
