/**********************************************************************************
 * iniPropertyInsert
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

int iniPropertyInsert( HINI hIni, char *pszProperty, char *pszValue )
{
	HINIOBJECT		hObject;
	HINIPROPERTY	hProperty;

    /* SANITY CHECKS */
	if ( hIni == NULL )
		return INI_ERROR;
    if ( hIni->hCurObject == NULL )
        return INI_ERROR;
	if ( pszProperty == NULL )
        return INI_ERROR;

	hObject	= hIni->hCurObject;

	/* CREATE PROPERTY STRUCT */
	hProperty = (HINIPROPERTY)malloc( sizeof(INIPROPERTY) );
	strncpy( hProperty->szName, pszProperty, INI_MAX_PROPERTY_NAME );
    if ( pszValue ) {
	    strncpy( hProperty->szValue, pszValue, INI_MAX_PROPERTY_VALUE );
    }
    else {
	    strcpy( hProperty->szValue, "" );
    }
	hProperty->pNext = NULL;
    iniAllTrim( hProperty->szName );
    iniAllTrim( hProperty->szValue );

	/* APPEND TO LIST */
	if ( hObject->hFirstProperty == NULL )
		hObject->hFirstProperty = hProperty;

    hProperty->pPrev		= hObject->hLastProperty;
    hObject->hLastProperty	= hProperty;

	if ( hProperty->pPrev != NULL )
        hProperty->pPrev->pNext	= hProperty;

	hIni->hCurProperty = hProperty;
	hObject->nProperties++;

	return INI_SUCCESS;
}


