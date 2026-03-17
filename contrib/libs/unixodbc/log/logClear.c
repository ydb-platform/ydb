#include <config.h>
#include "log.h"

/*! 
 * \brief   Clear all log messages.
 *
 * \param   hLog
 * 
 * \return  int
 * \retval  LOG_ERROR
 * \retval  LOG_SUCCESS
 *
 * \sa      logOpen
 *          logClose
 */
int logClear( HLOG hLog )
{
    /* we have to be logOpen to logClear messages */
	if ( !hLog ) return LOG_ERROR;

    /* We rely upon a callback being set to handle clearing mem used by each msg.
       This should be set in logOpen but just in case - we check it here.          */
    if ( !hLog->hMessages->pFree ) return LOG_ERROR;

    /* go to last message and delete until no more messages */
    lstLast( hLog->hMessages );
    while ( !lstEOL( hLog->hMessages ) )
    {
        lstDelete( hLog->hMessages );
    }

	return LOG_SUCCESS;
}


