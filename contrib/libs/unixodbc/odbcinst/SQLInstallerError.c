/**************************************************
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#include <config.h>
#include <odbcinstext.h>

/*!
 * \brief   Standard installer error.
 */
typedef struct tODBCINSTErrorMsg
{
	int 	nCode;  /*!< error code */
	char *  szMsg;  /*!< error text */
} ODBCINSTErrorMsg;

/*! 
 * \brief   A lookup for all standard installer (odbcinst) error codes and 
 *          corresponding message text.
 *
 *          An odd thing that we do here is that we assume the values of error codes
 *          (ODBC_ERROR_GENERAL_ERR, ODBC_ERROR_INVALID_BUFF_LEN, etc) are in the same
 *          sequence we have layed out here... ascending order starting at 1. We then 
 *          can index into here using the standard error code and get the standard error
 *          text. This is why we have 0,"Filler" in here.
 */
static ODBCINSTErrorMsg aODBCINSTErrorMsgs[] =
{
	{ 0,                                      	"Filler" },
	{ ODBC_ERROR_GENERAL_ERR,					"General installer error" },
	{ ODBC_ERROR_INVALID_BUFF_LEN,				"Invalid buffer length" },
	{ ODBC_ERROR_INVALID_HWND,					"Invalid window handle" },
	{ ODBC_ERROR_INVALID_STR,					"Invalid string" },
	{ ODBC_ERROR_INVALID_REQUEST_TYPE,			"Invalid type of request" },
	{ ODBC_ERROR_COMPONENT_NOT_FOUND,			"Unable to find component name" },
	{ ODBC_ERROR_INVALID_NAME,					"Invalid driver or translator name" },
	{ ODBC_ERROR_INVALID_KEYWORD_VALUE,			"Invalid keyword-value pairs" },
	{ ODBC_ERROR_INVALID_DSN,					"Invalid DSN" },
	{ ODBC_ERROR_INVALID_INF,					"Invalid INF" },
	{ ODBC_ERROR_REQUEST_FAILED,				"General error request failed" },
	{ ODBC_ERROR_INVALID_PATH,					"Invalid install path" },
	{ ODBC_ERROR_LOAD_LIB_FAILED,				"Could not load the driver or translator setup library" },
	{ ODBC_ERROR_INVALID_PARAM_SEQUENCE,		"Invalid parameter sequence" },
	{ ODBC_ERROR_INVALID_LOG_FILE,				"Invalid log file" },
	{ ODBC_ERROR_USER_CANCELED,					"User canceled operation" },
	{ ODBC_ERROR_USAGE_UPDATE_FAILED,			"Could not increment or decrement the component usage count" },
	{ ODBC_ERROR_CREATE_DSN_FAILED,				"Could not create the requested DSN" },
	{ ODBC_ERROR_WRITING_SYSINFO_FAILED,		"Error writing sysinfo" },
	{ ODBC_ERROR_REMOVE_DSN_FAILED,				"Removing DSN failed" },
	{ ODBC_ERROR_OUT_OF_MEM,					"Out of memory" },
	{ ODBC_ERROR_OUTPUT_STRING_TRUNCATED,		"String right truncated" }
};

/*! 
 * \brief   Returns error information from odbcinst.
 * 
 *          All calls to odbcinst, except SQLInstallerError and SQLPostInstallerError, may
 *          post/log messages. An application checks the return value of a call and then
 *          calls SQLInstallerError, as needed, to get any error information.
 *
 *          All calls to odbcinst, except SQLInstallerError and SQLPostInstallerError, will
 *          start by clearing out existing messages.
 *
 * \param   nError          Input. Messages are enumerated starting with 1 as the oldest. The
 *                          ODBC specification states that there are a max 8 messages stored at
 *                          any time but unixODBC may provide more than that.
 * \param   pnErrorCode     Output. The odbcinst error code as per the ODBC specification.
 * \param   pszErrorMsg     Output. The error text. In general this is the error text from
 *                          the ODBC specification but unixODBC may provide an alternate, more
 *                          meaningfull text.
 * \param   nErrorMsgMax    Input. Max chars which can be returned in pszErrorMsg.
 * \param   pnErrorMsg      Output. strlen of error text available to be returned.
 * 
 * \return  RETCODE
 * \retval  SQL_NO_DATA             No data to be returned for nError.
 * \retval  SQL_ERROR               Something went wrong - most likley bad args in call.
 * \retval  SQL_SUCCESS_WITH_INFO   Text was truncated.
 * \retval  SQL_SUCCESS             Error information was returned.
 *
 * \sa      SQLPostInstallerError
 *          ODBCINSTErrorMsg
 */
RETCODE SQLInstallerError( WORD nError, DWORD *pnErrorCode, LPSTR pszErrorMsg, WORD nErrorMsgMax, WORD *pnErrorMsg )
{
    HLOGMSG hLogMsg     = NULL;
    WORD    nErrorMsg   = 0;
    char *  pszText     = NULL;

    /* these are mandatory so... */
	if ( pnErrorCode == NULL || pszErrorMsg == NULL )
		return SQL_ERROR;

    /* this is optional so... */
    if ( !pnErrorMsg )
        pnErrorMsg = &nErrorMsg;

    /* get our message */
	if ( inst_logPeekMsg( nError, &hLogMsg ) != LOG_SUCCESS )
		return SQL_NO_DATA;

    /* return code */
    *pnErrorCode = hLogMsg->nCode;

    /* any custom message has precedence over the standard messages since its probably more meaningfull */
    if ( *(hLogMsg->pszMessage) )
        pszText = hLogMsg->pszMessage;
    else
        pszText = aODBCINSTErrorMsgs[hLogMsg->nCode].szMsg;

    /* how many chars in error text? */
    *pnErrorMsg = strlen( pszText );

    /* are we going to have to truncate the text due to lack of buffer space? */
    if ( *pnErrorMsg > nErrorMsgMax )
    {
        strncpy( pszErrorMsg, pszText, nErrorMsgMax );
        pszErrorMsg[ nErrorMsgMax ] = '\0';
        return SQL_SUCCESS_WITH_INFO;
    }

    /* success without further complications :) */
    strcpy( pszErrorMsg, pszText );
    return SQL_SUCCESS;
}

/*! 
 * \brief   A wide char version of SQLInstallerError.
 * 
 * \sa      SQLInstallerError
 */
SQLRETURN   INSTAPI SQLInstallerErrorW(WORD iError,
                            DWORD   *pfErrorCode,
                            LPWSTR  lpszErrorMsg,
                            WORD    cbErrorMsgMax,
                            WORD    *pcbErrorMsg)
{
	char *msg;
	SQLRETURN ret;
	WORD len;

	if ( lpszErrorMsg ) 
	{
		if ( cbErrorMsgMax > 0 )
		{
			msg = calloc( cbErrorMsgMax + 1, 1 );
		}
		else
		{
			msg = NULL;
		}
	}
	else
	{
		msg = NULL;
	}

	ret = SQLInstallerError( iError, 
							pfErrorCode,
							msg,
							cbErrorMsgMax,
							&len );

	if ( ret == SQL_SUCCESS )
	{
		if ( pcbErrorMsg )
			*pcbErrorMsg = len;

		if ( msg && lpszErrorMsg )
		{
			_single_copy_to_wide( lpszErrorMsg, msg, len + 1 );
		}
	}
	else if ( ret == SQL_SUCCESS_WITH_INFO )
	{
		if ( pcbErrorMsg )
			*pcbErrorMsg = len;

		if ( msg && lpszErrorMsg )
		{
			_single_copy_to_wide( lpszErrorMsg, msg, cbErrorMsgMax );
		}
	}

    if ( msg ) {
        free( msg );
    }

	return ret;
}
