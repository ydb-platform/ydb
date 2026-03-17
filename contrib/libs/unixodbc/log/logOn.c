/**********************************************************************
 * logOn
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
 * \brief   Turn logging on/off.
 *
 *          Logging is turned OFF by default. Turn it on when you want 
 *          logged messages to be stored. Turn it back off when you want to
 *          'pause' logging.
 *
 *          This may be used during debugging sessions to reduce clutter
 *          in the log results.
 *
 * \param   hLog
 * \param   bOn
 * 
 * \return  int
 * \retval  LOG_SUCCESS
 *
 * \sa      logOpen
 *          logClose
 */
int logOn( HLOG hLog, int bOn )
{
    /* log must be logOpen to logOn */
	if ( !hLog ) return LOG_ERROR;
	
    hLog->bOn = bOn;

	return LOG_SUCCESS;
}


