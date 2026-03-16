/**********************************************************************************
 * iniObjectInsert
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

int iniObjectInsert( HINI hIni, char *pszObject )
{
	HINIOBJECT	hObject;
	char		szObjectName[INI_MAX_OBJECT_NAME+1];

    /* SANITY CHECK */
    if ( hIni == NULL )
        return INI_ERROR;
    if ( pszObject == NULL )
        return INI_ERROR;

	strncpy( szObjectName, pszObject, INI_MAX_OBJECT_NAME );
	iniAllTrim( szObjectName );

	/* CREATE OBJECT STRUCT */
	hObject = malloc( sizeof(INIOBJECT) );
        if ( !hObject )
            return INI_ERROR;
	hIni->hCurProperty			= NULL;
	hObject->hFirstProperty		= NULL;
	hObject->hLastProperty		= NULL;
	hObject->nProperties		= 0;
	hObject->pNext				= NULL;
	hObject->pPrev				= NULL;
	strncpy( hObject->szName, szObjectName, INI_MAX_OBJECT_NAME );

	/* APPEND TO OBJECT LIST */
	if ( hIni->hFirstObject == NULL )
		hIni->hFirstObject = hObject;
	
	hObject->pPrev				= hIni->hLastObject;
	hIni->hLastObject			= hObject;

	if ( hObject->pPrev != NULL )
        hObject->pPrev->pNext	= hObject;
	
	hIni->hCurObject = hObject;
	hIni->nObjects++;

    return INI_SUCCESS;
}


