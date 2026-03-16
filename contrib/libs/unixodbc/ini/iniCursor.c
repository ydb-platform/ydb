/**********************************************************************************
 * iniCursor
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

int iniCursor( HINI hIni, HINI hIniCursor )
{
	if ( hIni == NULL || hIniCursor == NULL )
		return INI_ERROR;
	
	memcpy( hIniCursor, hIni, sizeof(INI) );

    return INI_SUCCESS;
}


