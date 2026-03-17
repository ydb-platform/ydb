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
 * \brief   Returns a specific message.
 * 
 *          This returns a reference to a specific message and does 
 *          NOT delete the message or remove it from the log. This is 
 *          good for 'peeking' at messages in the stack.
 *
 * \param   hLog    Input. Viable log handle.
 * \param   nMsg    Input. This is the index to the desired message. The
 *                  index is 1 based with 1 being the oldest message.
 * \param   phMsg   Output. A reference to the message in the log. This
 *                  message is still maintained/owned by the log. The 
 *                  reference is only valid until some other code modifies
 *                  the log.
 * 
 * \return  int
 * \retval  LOG_NO_DATA No message at nMsg.
 * \retval  LOG_ERROR   
 * \retval  LOG_SUCCESS
 *
 * \sa      logPopMsg  
 */
int logPeekMsg( HLOG hLog, long nMsg, HLOGMSG *phMsg )
{
    /* we must be logOpen to logPeekMsg */
	if ( !hLog ) return LOG_ERROR;

    /* get reference */
    /* \todo This can be terribly slow as we scan for each call. We may
             want to implement this over a vector instead of a list.   */
    *phMsg = (HLOGMSG)lstGoto( hLog->hMessages, nMsg - 1 );

    /* was it found? */
    if ( lstEOL( hLog->hMessages ) )
        return LOG_NO_DATA;

	return LOG_SUCCESS;
}


