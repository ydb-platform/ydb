/**********************************************************************************
 * iniGetBookmark
 *
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * PAH = Peter Harvey		- pharvey@codebydesign.com
 * -----------------------------------------------
 *
 * PAH	18.MAR.99	Created.
 **************************************************/

#include <config.h>
#include "ini.h"

int iniGetBookmark( HINI hIni, HINIBOOKMARK hIniBookmark )
{
	if ( hIni == NULL || hIniBookmark == NULL )
		return INI_ERROR;
	
   	hIniBookmark->hIni 			= hIni;
	hIniBookmark->hCurObject	= hIni->hCurObject;
	hIniBookmark->hCurProperty	= hIni->hCurProperty;

    return INI_SUCCESS;
}


