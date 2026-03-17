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
 **************************************************/

#include <config.h>
#include "ini.h"

int iniPropertySeek( HINI hIni, char *pszObject, char *pszProperty, char *pszValue )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

    /* Ok */
	iniObjectFirst( hIni );
	while ( iniObjectEOL( hIni ) != TRUE )
	{
		if	( pszObject[0] == '\0' || strcasecmp( pszObject, hIni->hCurObject->szName ) == 0 )
		{
			/* EITHER THE OBJECT HAS BEEN FOUND OR THE OBJECT DOES NOT MATTER	*/
			/* IN ANYCASE LETS SCAN FOR PROPERTY								*/
			iniPropertyFirst( hIni );
			while ( iniPropertyEOL( hIni ) != TRUE )
			{
				if	( pszProperty[0] == '\0' || strcasecmp( pszProperty, hIni->hCurProperty->szName ) == 0 )
				{
					if ( pszValue[0] == '\0' || strcasecmp( pszValue, hIni->hCurProperty->szValue ) == 0 )
					{
						/* FOUND IT !! */
						return INI_SUCCESS;
					}
				}
				iniPropertyNext( hIni );
			}
			if ( pszObject[0] != '\0' )
			{
				hIni->hCurObject = NULL;
				return INI_NO_DATA;
			}
		}
		iniObjectNext( hIni );
	}

	return INI_NO_DATA;
}


