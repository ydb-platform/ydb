/**********************************************
 * SQLPostInstallerError
 *
 * Drivers can call me to let me know there was a
 * problem. This can be retreived by the app using
 * SQLInstallerError.
 *
 * Does not currently use szErrorMsg due to extreme
 * limitations of logging here. This should be corrected.
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

RETCODE SQLPostInstallerError(	DWORD	nErrorCode,
								LPCSTR	szErrorMsg )
{
	if ( nErrorCode > ODBC_ERROR_OUTPUT_STRING_TRUNCATED )
		return SQL_ERROR;

    inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, nErrorCode, (char *)szErrorMsg );

	return SQL_SUCCESS;
}

SQLRETURN INSTAPI   SQLPostInstallerErrorW(DWORD dwErrorCode,
                            LPCWSTR lpszErrorMsg)
{
	char *msg = lpszErrorMsg ? _single_string_alloc_and_copy( lpszErrorMsg ) : (char*)NULL;
	SQLRETURN ret;

	ret = SQLPostInstallerError( dwErrorCode, msg );

	if ( msg )
		free( msg );

	return ret;
}
