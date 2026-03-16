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
 * $Id: _logging.c,v 1.5 2009/02/18 17:59:27 lurcher Exp $
 *
 * $Log: _logging.c,v $
 * Revision 1.5  2009/02/18 17:59:27  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.4  2008/05/12 13:07:21  lurcher
 * Push a couple of small changes back into CVS, ready for new release
 *
 * Revision 1.3  2008/02/15 15:47:12  lurcher
 * Add thread protection around ini caching
 *
 * Revision 1.2  2007/11/27 17:52:57  peteralexharvey
 * - changes made during QT4 implementation
 *
 * Revision 1.1.1.1  2001/10/17 16:40:30  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1.1.1  2000/09/04 16:42:53  nick
 * Imported Sources
 *
 * Revision 1.1  1999/07/15 06:23:39  ngorham
 *
 * Added functions to remove the need for _init and _fini
 *
 *
 *********************************************************************/

#include <config.h>
#include <odbcinstext.h>
#include <log.h>

#ifdef HAVE_LIBPTH

#include <pth.h>

static pth_mutex_t mutex_log = PTH_MUTEX_INIT;
static int pth_init_called = 0;

static int local_mutex_entry( void )
{
    if ( !pth_init_called )
    {
        pth_init();
        pth_init_called = 1;
    }
    return pth_mutex_acquire( &mutex_log, 0, NULL );
}

static int local_mutex_exit( void )
{
    return pth_mutex_release( &mutex_log );
}

#elif HAVE_LIBPTHREAD

#include <pthread.h>

static pthread_mutex_t mutex_log = PTHREAD_MUTEX_INITIALIZER;

static int local_mutex_entry( void )
{
    return pthread_mutex_lock( &mutex_log );
}

static int local_mutex_exit( void )
{
    return pthread_mutex_unlock( &mutex_log );
}

#elif HAVE_LIBTHREAD

#include <thread.h>

static mutex_t mutex_log;

static int local_mutex_entry( void )
{
    return mutex_lock( &mutex_log );
}

static int local_mutex_exit( void )
{
    return mutex_unlock( &mutex_log );
}

#else

#define local_mutex_entry()
#define local_mutex_exit()

#endif
/*
 * I don't like these statics but not sure what else we can do...
 *
 * Indeed, access to these statics was in fact not thread safe !
 * So they are now protected by mutex_log...
 */

static HLOG hODBCINSTLog = NULL;
static int log_tried = 0;

int inst_logPushMsg( char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMessage )
{
    int ret = LOG_ERROR;

    local_mutex_entry();

    if ( !log_tried )
    {
        long nMaxMessages = 10; /* \todo ODBC spec says 8 max. We would make it 0 (unlimited) but at the moment logPeekMsg 
                                   would be slow if many messages. Revisit when opt is made to log storage. */

        log_tried = 1;
        if ( logOpen( &hODBCINSTLog, "odbcinst", NULL, nMaxMessages ) != LOG_SUCCESS )
        {
            hODBCINSTLog = NULL;
        }
        else
        {
            logOn( hODBCINSTLog, 1 );
        }
    }
    if ( hODBCINSTLog )
    {
        ret = logPushMsg( hODBCINSTLog,
                pszModule, 
                pszFunctionName, 
                nLine, 
                nSeverity, 
                nCode, 
                pszMessage );
    }

    local_mutex_exit();

    return ret;
}

/*! 
 * \brief   Get a reference to a message in the log.
 *
 *          The caller (SQLInstallerError) could call logPeekMsg directly
 *          but we would have to extern hODBCINSTLog and I have not given
 *          any thought to that at this time.
 * 
 * \param   nMsg
 * \param   phMsg
 * 
 * \return  int
 */
int inst_logPeekMsg( long nMsg, HLOGMSG *phMsg )
{
    int ret = LOG_NO_DATA;

    local_mutex_entry();

    if ( hODBCINSTLog )
        ret = logPeekMsg( hODBCINSTLog, nMsg, phMsg );

    local_mutex_exit();

    return ret;
}

int inst_logClear( void )
{
    int ret = LOG_ERROR;

    local_mutex_entry();

    if ( hODBCINSTLog ) 
        ret = logClear( hODBCINSTLog );

    local_mutex_exit();

    return ret;
}

