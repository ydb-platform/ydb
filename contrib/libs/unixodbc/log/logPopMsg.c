/**********************************************************************
 * logPopMsg
 *
 *
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 *
 **********************************************************************/

#include <config.h>
#include "log.h"

/*! 
 * \brief   Removes the oldest message from the log.
 * 
 *          The log is a FIFO stack and we implement a possible max on the
 *          number of messages we can store. When we hot the max we 'pop'
 *          a message out. The mem used by the message is automatically 
 *          freed with a call to \sa _logFreeMsg.
 *
 * \param   hLog
 * 
 * \return  int
 * \retval  LOG_NO_DATA
 * \retval  LOG_ERROR
 * \retval  LOG_SUCCESS
 *
 * \sa      logPushMsg
 *          logPeekMsg
 */
int logPopMsg( HLOG hLog )
{
    /* we must be logOpen to logPopMsg */
	if ( !hLog ) return LOG_ERROR;

    /* FIFO */
    lstFirst( hLog->hMessages );

    /* do we have a message to delete? */
	if ( lstEOL( hLog->hMessages ) ) return LOG_NO_DATA;

    return lstDelete( hLog->hMessages );
}


