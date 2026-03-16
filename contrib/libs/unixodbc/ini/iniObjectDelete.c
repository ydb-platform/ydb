/**********************************************************************************
 * .
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

/******************************
 * iniObjectDelete
 *
 ******************************/
int iniObjectDelete( HINI hIni )
{
	HINIOBJECT		hObject;

    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;
	if ( hIni->hCurObject == NULL )
		return INI_NO_DATA;
	
	hObject	= hIni->hCurObject;

	/* REMOVE ALL SUBORDINATE INFO */
	hIni->hCurProperty = hObject->hFirstProperty;
	while ( iniPropertyDelete( hIni ) == INI_SUCCESS )
	{
	}

	/* REMOVE FROM LIST */
	if ( hIni->hFirstObject == hObject )
		hIni->hFirstObject = hObject->pNext;
	if ( hIni->hLastObject == hObject )
		hIni->hLastObject = hObject->pPrev;

	hIni->hCurObject		= NULL;
	if ( hObject->pNext )
	{
		hObject->pNext->pPrev	= hObject->pPrev;
		hIni->hCurObject		= hObject->pNext;
	}
	if ( hObject->pPrev )
	{
		hObject->pPrev->pNext 	= hObject->pNext;
		hIni->hCurObject		= hObject->pPrev;
	}
	hIni->nObjects--;

	/* FREE MEMORY */
	free( hObject );

	iniPropertyFirst( hIni );

	return INI_SUCCESS;
}


