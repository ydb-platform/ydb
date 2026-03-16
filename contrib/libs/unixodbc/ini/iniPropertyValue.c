/**********************************************************************************
 * .
 *  							totally untested
 *
 * see iniElement instead	
 **********************************************************************************/

#include <config.h>
#include "ini.h"

int iniPropertyValue( char *pszString, char *pszProperty, char *pszValue, char cEqual, char cPropertySep )
{
    char    szBuffer[INI_MAX_LINE+1];
	char	szEqual[2];
	char 	szPropertySep[2];
	char 	*pProperty;
	char	*pValue;
	char	*pValueLastChar;

	szEqual[0]			= cEqual;
	szEqual[1]			= '\0';
    szPropertySep[0]	= cPropertySep;
	szPropertySep[1]	= '\0';

    strcpy( pszValue, "" );
	strncpy( szBuffer, pszString, INI_MAX_LINE );

	/* find pszProperty		*/
	while ( 1 )
	{
		pProperty = (char *)strtok( szBuffer, (const char *)szPropertySep );
		if ( pProperty == NULL )
			break;
		else
		{
			/* extract pszValue 		*/
			if ( strncmp( pProperty, pszProperty, strlen(pszProperty) ) == 0 )
			{
				pValue = (char *)strtok( szBuffer, (const char *)szEqual );
				if ( pValue )
				{
					/* truncate any other data */
					pValueLastChar = (char *)strchr( pValue, szPropertySep[ 0 ] );
					if ( pValueLastChar )
						pValueLastChar[0] = '\0';

					strncpy( pszValue, pValue, INI_MAX_PROPERTY_VALUE );
					iniAllTrim( pszValue );
				}
				break;
			}
		}
	}

    return INI_SUCCESS;
}


