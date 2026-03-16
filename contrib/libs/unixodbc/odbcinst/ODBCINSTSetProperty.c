/**************************************************
 * ODBCINSTSetProperty
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

int ODBCINSTSetProperty( HODBCINSTPROPERTY hFirstProperty, char *pszProperty, char *pszValue )
{
	char 				szError[LOG_MSG_MAX+1];
	HODBCINSTPROPERTY	hCurProperty;

	/* SANITY CHECKS */
	if ( hFirstProperty == NULL )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Invalid property list handle" );
		return ODBCINST_ERROR;
	}
	if ( pszProperty == NULL )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Invalid Property Name" );
		return ODBCINST_ERROR;
	}
	if ( pszValue == NULL )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Invalid Value buffer" );
		return ODBCINST_ERROR;
	}

	/* FIND pszProperty */
	for ( hCurProperty = hFirstProperty; hCurProperty != NULL; hCurProperty = hCurProperty->pNext )
	{
		if ( strcasecmp( pszProperty, hCurProperty->szName ) == 0 )
		{
			/* CHANGE IT */
			strncpy( hCurProperty->szValue, pszValue, INI_MAX_PROPERTY_VALUE );
			return ODBCINST_SUCCESS;
		}
	}

	sprintf( szError, "Could not find property (%s)", pszProperty );
	inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_WARNING, ODBC_ERROR_GENERAL_ERR, szError );

	return ODBCINST_ERROR;
}

int ODBCINSTAddProperty( HODBCINSTPROPERTY hFirstProperty, char *pszProperty, char *pszValue )
{
    HODBCINSTPROPERTY hNew;

	hNew						= (HODBCINSTPROPERTY)malloc( sizeof(ODBCINSTPROPERTY) );
	memset(hNew, 0, sizeof(ODBCINSTPROPERTY));
	hNew->nPromptType			= ODBCINST_PROMPTTYPE_HIDDEN;
	hNew->pNext				= NULL;
    hNew->bRefresh				= 0;
    hNew->hDLL					= hFirstProperty->hDLL;
    hNew->pWidget				= NULL;
    hNew->pszHelp				= NULL;
	hNew->aPromptData			= NULL;
	strcpy(hNew->szName, pszProperty );
	strcpy( hNew->szValue, pszValue );

    /*
     * add to end of list
     */

    while ( hFirstProperty -> pNext )
    {
	    hFirstProperty = hFirstProperty -> pNext;
    }

    hNew -> pNext = NULL;
	hFirstProperty -> pNext = hNew;

    return 0;
}
