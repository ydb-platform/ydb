/**********************************************************************
 * logPushMsg
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
#include "ini.h"

/* Define this as a fall through, HAVE_STDARG_H is probably already set */

/*#define HAVE_VARARGS_H*/

/* varargs declarations: */

#if defined(HAVE_STDARG_H)
# include <stdarg.h>
    #define VA_LOCAL_DECL   va_list ap
    #define VA_START(f)     va_start(ap, f)
    #define VA_SHIFT(v,t)  ;   /* no-op for ANSI */
    #define VA_END          va_end(ap)
#else
    #if defined(HAVE_VARARGS_H)
        #define VA_LOCAL_DECL   va_list ap
        #define VA_START(f)     va_start(ap)      /* f is ignored! */
        #define VA_SHIFT(v,t) v = va_arg(ap,t)
        #define VA_END        va_end(ap)
    #else
        #error No variable argument support
    #endif
#endif

#ifndef HAVE_VSNPRINTF 
int uodbc_vsnprintf (char *str, size_t count, const char *fmt, va_list args);
#endif


int logPushMsg( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMessage )
{
    HLOGMSG     hMsg;
    FILE        *hFile;

    if ( !hLog ) return LOG_ERROR;
    if ( !hLog->hMessages ) return LOG_ERROR;
    if ( !hLog->bOn ) return LOG_SUCCESS;

    if ( !pszModule ) return LOG_ERROR;
    if ( !pszFunctionName ) return LOG_ERROR;
    if ( !pszMessage ) return LOG_ERROR;


    /* check for, and handle, max msg */
    if ( hLog->nMaxMsgs && hLog->hMessages->nItems >= hLog->nMaxMsgs )
        logPopMsg( hLog );

    hMsg                    = malloc( sizeof(LOGMSG) );
    if (!hMsg) goto error_abort0;

    hMsg->pszModuleName     = (char *)strdup( pszModule );
    if (!hMsg->pszModuleName) goto error_abort1;

    hMsg->pszFunctionName   = (char *)strdup( pszFunctionName );
    if (!hMsg->pszFunctionName) goto error_abort2;

    hMsg->pszMessage        = (char *)strdup( pszMessage );
    if (!hMsg->pszMessage) goto error_abort3;

    hMsg->nLine             = nLine;
    hMsg->nSeverity         = nSeverity;
    hMsg->nCode             = nCode;

    /* append to  list */
    lstAppend( hLog->hMessages, hMsg );

    /* append to file */
    if ( hLog->pszLogFile )
    {
        hFile = uo_fopen( hLog->pszLogFile, "a" );
        if ( !hFile ) return LOG_ERROR;

        uo_fprintf( hFile, "[%s][%s][%s][%d]%s\n", hLog->pszProgramName, pszModule, pszFunctionName, nLine, pszMessage );

        uo_fclose( hFile );
    }

    return LOG_SUCCESS;

    error_abort3:
    free(hMsg->pszFunctionName);
    error_abort2:
    free(hMsg->pszModuleName);
    error_abort1:
    free(hMsg);
    error_abort0:
    return LOG_ERROR;
}

int logvPushMsgf( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMessageFormat, va_list args )
{
    HLOGMSG     hMsg=NULL;
    FILE        *hFile;
    int             mlen=0;

    if ( !hLog ) return LOG_ERROR;
    if ( !hLog->hMessages ) return LOG_ERROR;
    if ( !hLog->bOn ) return LOG_SUCCESS;

    if ( !pszModule ) return LOG_ERROR;
    if ( !pszFunctionName ) return LOG_ERROR;
    if ( !pszMessageFormat ) return LOG_ERROR;

    /* check for, and handle, max msg */
    if ( hLog->nMaxMsgs && hLog->hMessages->nItems == hLog->nMaxMsgs )
        logPopMsg( hLog );

    hMsg                    = malloc( sizeof(LOGMSG) );
    if (!hMsg) goto error_abort0;

    hMsg->pszModuleName     = (char *)strdup( pszModule );
    if (!hMsg->pszModuleName) goto error_abort1;

    hMsg->pszFunctionName   = (char *)strdup( pszFunctionName );
    if (!hMsg->pszFunctionName) goto error_abort2;

#if defined( HAVE_VSNPRINTF )
    mlen=vsnprintf(NULL,0,pszMessageFormat,args);
#else
    mlen=uodbc_vsnprintf(NULL,0,pszMessageFormat,args);
#endif
    mlen++;
    hMsg->pszMessage        = malloc(mlen);
    if (!hMsg->pszMessage) goto error_abort3;

#if defined( HAVE_VSNPRINTF )
    vsnprintf(hMsg->pszMessage,mlen,pszMessageFormat,args);
#else
    uodbc_vsnprintf(hMsg->pszMessage,mlen,pszMessageFormat,args);
#endif

    hMsg->nLine             = nLine;
    hMsg->nSeverity         = nSeverity;
    hMsg->nCode             = nCode;

    /* append to  list */
    lstAppend( hLog->hMessages, hMsg );

    /* append to file */
    if ( hLog->pszLogFile )
    {
        hFile = uo_fopen( hLog->pszLogFile, "a" );
        if ( !hFile ) return LOG_ERROR;

        if (hMsg)
        {
            uo_fprintf( hFile, "[%s][%s][%s][%d]%s\n", hLog->pszProgramName, pszModule, pszFunctionName, nLine, hMsg->pszMessage );
        }
        else
        {
            uo_fprintf( hFile, "[%s][%s][%s][%d]", hLog->pszProgramName, pszModule, pszFunctionName, nLine );
            uo_vfprintf( hFile, pszMessageFormat, args );
            uo_fprintf( hFile, "\n" );
        }

        uo_fclose( hFile );
    }

    return LOG_SUCCESS;

    error_abort3:
    free(hMsg->pszFunctionName);
    error_abort2:
    free(hMsg->pszModuleName);
    error_abort1:
    free(hMsg);
    error_abort0:
    return LOG_ERROR;
}

#ifdef HAVE_STDARGS
int logPushMsgf( HLOG hLog, char *pszModule, char *pszFunctionName, int nLine, int nSeverity, int nCode, char *pszMessageFormat, ... )
#else
int logPushMsgf( va_alist ) va_dcl
#endif
{
    int err;
#ifndef HAVE_STDARGS
    HLOG hLog;
    char *pszModule;
    char *pszFunctionName;
    int nLine;
    int nSeverity;
    int nCode;
    char *pszMessageFormat;
#endif
    VA_LOCAL_DECL;

    VA_START (pszMessageFormat);
    VA_SHIFT (hLog, HLOG);
    VA_SHIFT (pszModule, char *);
    VA_SHIFT (pszFunctionName, char *);
    VA_SHIFT (nLine, int );
    VA_SHIFT (nSeverity, int );
    VA_SHIFT (nCode, int );
    VA_SHIFT (pszMessageFormat, char *);
    err=logvPushMsgf(hLog,pszModule,pszFunctionName,nLine,nSeverity,nCode,pszMessageFormat,ap);
    VA_END;
    return err;

}
