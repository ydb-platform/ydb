/**********************************************************************
 * logClose
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
 * \brief   Closes log.
 * 
 *          This will clear all messages and close the log. All memory used
 *          by the messages is automatically freed by calls to _logFreeMsg.
 *          All remaining mem used by the log is also freed - including the
 *          log handle itself.
 *
 * \param   hLog    A log handle init by \sa logOpen.
 * 
 * \return  int
 * \retval  LOG_SUCCESS
 *
 * \sa      logOpen
 */
int logClose( HLOG hLog )
{
    /* we must be logOpen to logClose */
	if ( !hLog ) return LOG_ERROR;

    /* clear all messages - including the handle                */
    /* _logFreeMsg will automatically be called for each msg    */
    lstClose( hLog->hMessages );

    /* free remaining mem used by log - including the handle    */
	if ( hLog->pszProgramName ) free( hLog->pszProgramName );
	if ( hLog->pszLogFile ) free( hLog->pszLogFile );
	free( hLog );

	return LOG_SUCCESS;
}


