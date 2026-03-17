/**************************************************
 * ODBCINSTDestructProperties
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

#ifdef UNIXODBC_SOURCE
#include <ltdl.h>
#endif

#include <odbcinstext.h>

int ODBCINSTDestructProperties( HODBCINSTPROPERTY *hFirstProperty )
{
	HODBCINSTPROPERTY	hNextProperty;
	HODBCINSTPROPERTY	hCurProperty;

	/* SANITY CHECKS */
	if ( (*hFirstProperty) == NULL )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Invalid property list handle" );
		return ODBCINST_ERROR;
	}

	/* FREE MEMORY */
	for ( hCurProperty = (*hFirstProperty); hCurProperty != NULL; hCurProperty = hNextProperty )
	{
		hNextProperty = hCurProperty->pNext;

		/* FREE ANY PROMPT DATA (ie pick list options and such) */
		if ( hCurProperty->aPromptData != NULL )
			free( hCurProperty->aPromptData );

		/* 1st PROPERTY HAS HANDLE TO DriverSetup DLL; LETS LET THE O/S KNOW WE ARE DONE WITH IT */
		if ( hCurProperty == (*hFirstProperty) && hCurProperty->hDLL != NULL )
			lt_dlclose( hCurProperty->hDLL );

		/* FREE OTHER STUFF */
        if ( hCurProperty->pszHelp != NULL )
			free( hCurProperty->pszHelp );

		free( hCurProperty );
	}
    (*hFirstProperty) = NULL;

	return ODBCINST_SUCCESS;
}

