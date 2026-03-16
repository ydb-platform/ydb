/****************************************************
 * _odbcinst_GetSections
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

int _odbcinst_GetSections(	HINI	hIni,
							LPSTR	pRetBuffer,
                            int		nRetBuffer,
							int		*pnBufPos 		/* SET TO 0 IF RESULT DATA IS EMPTY */
						)
{
	char	szObjectName[INI_MAX_OBJECT_NAME+1];
    char    *ptr;

    *pnBufPos = 0;
    *pRetBuffer = '\0';
    ptr = pRetBuffer;

	/* JUST COLLECT SECTION NAMES */

    for( iniObjectFirst( hIni ); iniObjectEOL( hIni ) != TRUE; iniObjectNext( hIni ))
	{
		iniObject( hIni, szObjectName );

        if ( strcasecmp( szObjectName, "ODBC Data Sources" ) == 0 )
        {
            continue;
        }
        else if ( *pnBufPos + 1 + strlen( szObjectName ) >= nRetBuffer )
        {
            break;
        }
        else
		{
            strcpy( ptr, szObjectName );
            ptr += strlen( ptr ) + 1;
            (*pnBufPos) += strlen( szObjectName ) + 1;
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
