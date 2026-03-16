/****************************************************
 * _odbcinst_GetEntries
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

int _odbcinst_GetEntries(	HINI	hIni,
							LPCSTR	pszSection,
							LPSTR	pRetBuffer,
							int		nRetBuffer,
							int		*pnBufPos
						)
{
	char	szPropertyName[INI_MAX_PROPERTY_NAME+1];
    char    *ptr;

    *pnBufPos = 0;
    *pRetBuffer = '\0';
    ptr = pRetBuffer;

	iniObjectSeek( hIni, (char *)pszSection );

	/* COLLECT ALL ENTRIES FOR THE GIVEN SECTION */

    for( iniPropertyFirst( hIni ); iniPropertyEOL( hIni ) != TRUE; iniPropertyNext( hIni ))
	{
		iniProperty( hIni, szPropertyName );

        if ( *pnBufPos + 1 + strlen( szPropertyName ) >= nRetBuffer )
        {
            break;
        }
        else
		{
            strcpy( ptr, szPropertyName );
            ptr += strlen( ptr ) + 1;
            (*pnBufPos) += strlen( szPropertyName ) + 1;
		}
	}

    /*
     * Add final NULL
     */

    if ( *pnBufPos == 0 )
    {
        ptr ++;
    }

    *ptr = '\0';

	return (*pnBufPos);
}
