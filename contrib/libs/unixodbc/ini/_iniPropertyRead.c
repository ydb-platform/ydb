/**********************************************************************************
 * _iniPropertyRead
 *
 *
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
#include "ini.h"

int _iniPropertyRead( HINI hIni, char *szLine, char *pszPropertyName, char *pszPropertyValue )
{

    /* SANITY CHECKS */
	if ( hIni == NULL )
		return INI_ERROR;
    if ( hIni->hCurObject == NULL )
        return INI_ERROR;

    /* SCAN LINE TO EXTRACT PROPERTY NAME AND VALUE WITH NO TRAILING SPACES */
	strcpy( pszPropertyName, "" );
	strcpy( pszPropertyValue, "" );
	
	iniElement( szLine, '=', '\0', 0, pszPropertyName, INI_MAX_PROPERTY_NAME );
	iniElementToEnd( szLine, '=', '\0', 1, pszPropertyValue, INI_MAX_PROPERTY_VALUE );
	iniAllTrim( pszPropertyName );
	iniAllTrim( pszPropertyValue );

	return INI_SUCCESS;
}


