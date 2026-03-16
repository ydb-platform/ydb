/*******************************************************
 * _odbcinst_ConfigModeINI
 *
 * Get first valid INI file name. If we can open it for read then we assume its valid.
 * 1. ODBC_SYSTEM_DSN
 * 		- /etc/odbc.ini
 * 2. ODBC_USER_DSN
 *		- ODBCINI
 * 		- ~/.odbc.ini
 *		- /home/.odbc.ini
 * 3. ODBC_BOTH_DSN
 *		- ODBC_USER_DSN
 *		- ODBC_SYSTEM_DSN
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

BOOL _odbcinst_ConfigModeINI( char *pszFileName )
{
	UWORD nConfigMode;

    __lock_config_mode();
	nConfigMode = __get_config_mode();
    __unlock_config_mode();

	pszFileName[0] = '\0';

	switch ( nConfigMode )
	{
	case ODBC_SYSTEM_DSN:
		if ( !_odbcinst_SystemINI( pszFileName, TRUE ) )
			return FALSE;
		break;
	case ODBC_USER_DSN:
		if ( !_odbcinst_UserINI( pszFileName, TRUE ) )
			return FALSE;
		break;
	case ODBC_BOTH_DSN:
		if ( !_odbcinst_UserINI( pszFileName, TRUE ) )
		{
			if ( !_odbcinst_SystemINI( pszFileName, TRUE ) )
				return FALSE;
		}
		break;
	default:
		return FALSE;
	}

	return TRUE;
}

