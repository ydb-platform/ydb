/**********************************************************************************
 * iniGotoBookmark
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

int iniGotoBookmark( INIBOOKMARK IniBookmark )
{
	if ( IniBookmark.hIni == NULL )
		return INI_ERROR;
	
	(IniBookmark.hIni)->hCurObject 		= IniBookmark.hCurObject;
	(IniBookmark.hIni)->hCurProperty	= IniBookmark.hCurProperty;

    return INI_SUCCESS;
}


