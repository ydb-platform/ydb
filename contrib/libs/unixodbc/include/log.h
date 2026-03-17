/**********************************************************************************
 * log.h
 *
 * Include file for liblog.a. Coding? Include this and link against liblog.a.
 *
 * At this time; its a simple list manager but I expect that this will evolve into
 * a list manager which;
 *
 * - allows for messages of different severity and types to be stored
 * - allows for actions (such as saving to file or poping) to occur on selected message
 *   types and severities
 *
 **********************************************************************************/

#ifndef INCLUDED_LOG_H
#define INCLUDED_LOG_H

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#if defined(HAVE_STDARG_H)
# include <stdarg.h>
# define HAVE_STDARGS
#else
# if defined(HAVE_VARARGS_H)
#  include <varargs.h>
#  ifdef HAVE_STDARGS
#   undef HAVE_STDARGS
#  endif
# endif
#endif


#include <lst.h>

/*****************************************************************************
 * FUNCTION RETURN CODES
 *****************************************************************************/
#define     LOG_ERROR               0
#define     LOG_SUCCESS             1
#define		LOG_NO_DATA				2

/*****************************************************************************
 * SEVERITY
 *****************************************************************************/
#define		LOG_INFO				0
#define		LOG_WARNING				1
#define		LOG_CRITICAL			2

/*****************************************************************************
 *
 *****************************************************************************/
#define		LOG_MSG_MAX				1024

/*****************************************************************************
 * HANDLES
 *****************************************************************************/
typedef struct	tLOGMSG
{
	char *  pszModuleName;      /*!< file where message originated                                      */
	char *  pszFunctionName;    /*!< function where message originated.                                 */
	int     nLine;              /*!< File line where message originated.                                */
	int     nSeverity;
	int     nCode;
	char *  pszMessage;

} LOGMSG, *HLOGMSG;


typedef struct	tLOG
{
	HLST		hMessages;								/* list of messages	(we may want to switch to vector)   */

	char		*pszProgramName;						/* liblog will malloc, copy, and free	*/
	char		*pszLogFile;							/* NULL, or filename					*/
	long		nMaxMsgs;								/* OLDEST WILL BE DELETED ONCE MAX		*/
	int			bOn;									/* turn logging on/off (default=off)	*/

} LOG, *HLOG;


/*****************************************************************************
 * API
 *****************************************************************************/

int logOpen( HLOG *phLog, char *pszProgramName, char *pszLogFile, long nMaxMsgs );
int logClose( HLOG hLog );
int logClear( HLOG hLog );
int logPushMsg( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMsg );
int logPushMsgf( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMessageFormat, ... );
int logvPushMsgf( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMessageFormat, va_list args );
int logPeekMsg( HLOG hLog, long nMsg, HLOGMSG *phMsg );
int logPopMsg( HLOG hLog );
int logOn( HLOG hLog, int bOn );

/*****************************************************************************
 * SUPPORTING FUNCS (do not call directly)
 *****************************************************************************/

/******************************
 * _logFreeMsg
 *
 * 1. This is called by lstDelete()
 ******************************/
void _logFreeMsg( void *pMsg );

#endif


