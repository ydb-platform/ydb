/**********************************************************************************
 * iniPropertySeek
 *
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 * -----------------------------------------------
 *
 * PAH	06.MAR.99	Added this func
 **************************************************/

#include <config.h>
#include "ini.h"

int iniPropertySeekSure( HINI hIni, char *pszObject, char *pszProperty, char *pszValue )
{
	int nReturn;

    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;
	if ( !pszObject )
		return INI_ERROR;
	if ( !pszProperty )
		return INI_ERROR;
	if ( !pszValue )
		return INI_ERROR;

	/* OK */
	if ( (nReturn = iniPropertySeek( hIni, pszObject, pszProperty, "" )) == INI_NO_DATA )
	{
		iniObjectSeekSure( hIni, pszObject );
		return iniPropertyInsert( hIni, pszProperty, pszValue );
	}
	else if ( nReturn == INI_SUCCESS )
		return iniValue( hIni, pszValue );

	return nReturn;
}


