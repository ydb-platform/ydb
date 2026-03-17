/**********************************************************************
 * logOpen
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
 * \brief   Open (init) a log (hLog).
 *
 *          This function must be called before any other in this API. 
 *          _logFreeMsg is applied to the log to ensure that all message
 *          mem is freed.
 * 
 * \param   phLog           Output. Log handle returned here.
 * \param   pszProgramName  Input. Default program name. This is attached to
 *                          each message when written to file. It is only used 
 *                          if a viable pszLogFile given. Can be NULL.
 * \param   pszLogFile      Input. File name of log file. Can be NULL.
 * \param   nMaxMsgs        Input. Max messages to store. When this limit is
 *                          reached - oldest message will be deleted. This
 *                          can be set to 0 to remove any limit.
 * 
 * \return  int
 * \retval  LOG_ERROR
 * \retval  LOG_SUCCESS
 *
 * \sa      logClose
 */
int logOpen( HLOG *phLog, char *pszProgramName, char *pszLogFile, long nMaxMsgs )
{
    /* sanity check */
    if ( !phLog ) return LOG_ERROR;

	/* LOG STRUCT */
	*phLog = malloc( sizeof(LOG) );
        if ( !*phLog )
            return LOG_ERROR;
    (*phLog)->nMaxMsgs			= nMaxMsgs;
    (*phLog)->hMessages			= lstOpen();
    (*phLog)->bOn				= 0;
    (*phLog)->pszLogFile		= NULL;
    (*phLog)->pszProgramName	= NULL;

    /* each msg will be freed when deleted by this (_logFreeMsg) callback */
	lstSetFreeFunc( (*phLog)->hMessages, _logFreeMsg );

	/* PROGRAM NAME */
	if ( pszProgramName )
		(*phLog)->pszProgramName = (char *)strdup( pszProgramName );
	else
		(*phLog)->pszProgramName = (char *)strdup( "UNKNOWN" );

	/* LOG FILE */
	if ( pszLogFile )
		(*phLog)->pszLogFile = (char*)strdup( pszLogFile );

	return LOG_SUCCESS;
}


