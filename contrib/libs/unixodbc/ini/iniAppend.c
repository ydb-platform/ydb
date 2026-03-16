/**********************************************************************************
 * iniAppend
 *
 * - Appends Sections which do not exist in hIni. Ignores all else.
 * - Does not try to append 'missing' Entries  to existing Sections (does not try to merge).
 * - hIni will become ReadOnly
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

int iniAppend( HINI hIni, char *pszFileName )
{
    FILE    *hFile;
	char    szLine[INI_MAX_LINE+1];
	char	szObjectName[INI_MAX_OBJECT_NAME+1];
	char	szPropertyName[INI_MAX_PROPERTY_NAME+1];
	char	szPropertyValue[INI_MAX_PROPERTY_VALUE+1];

    /* SANITY CHECK */
    if ( strlen( pszFileName ) > ODBC_FILENAME_MAX )
        return INI_ERROR;

    /* OPEN FILE */
    hFile = uo_fopen( pszFileName, "r" );
	if ( !hFile )
		return INI_ERROR;

	iniObjectLast( hIni );
	iniPropertyLast( hIni );

	/* SCAN UNTIL WE GET TO AN OBJECT NAME OR EOF */
	szLine[0] = '\0';
	if ( _iniScanUntilObject( hIni, hFile, szLine ) == INI_SUCCESS )
	{
		do
		{
			if ( szLine[0] == hIni->cLeftBracket )
			{
				_iniObjectRead( hIni, szLine, szObjectName );
				if ( iniObjectSeek( hIni, szObjectName ) == INI_SUCCESS )
				{
					iniObjectLast( hIni );
					iniPropertyLast( hIni );
					if ( _iniScanUntilNextObject( hIni, hFile, szLine ) != INI_SUCCESS)
						break;
				}
				else
				{
					iniObjectInsert( hIni, szObjectName );
					if ( uo_fgets( szLine, INI_MAX_LINE, hFile ) == NULL )
						break;
				}
			}
			else if ( (strchr( hIni->cComment, szLine[0] ) == NULL ) && isalnum(szLine[0]) )
			{
				_iniPropertyRead( hIni, szLine, szPropertyName, szPropertyValue );
				iniPropertyInsert( hIni, szPropertyName, szPropertyValue );
				if ( uo_fgets( szLine, INI_MAX_LINE, hFile ) == NULL )
					break;
			}
			else
			{
				if ( uo_fgets( szLine, INI_MAX_LINE, hFile ) == NULL )
					break;
			}
		} while( 1 );
	}

	/* WE ARE NOT GOING TO TRY TO BE SMART ENOUGH TO SAVE THIS STUFF */
	hIni->bReadOnly			= 1;

    /* CLEANUP */
    if ( hFile != NULL )
		uo_fclose( hFile );

    return INI_SUCCESS;
}


