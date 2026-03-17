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

int iniPropertyEOL( HINI hIni )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return TRUE;

	if ( hIni->hCurObject == NULL )
		return TRUE;
	
	if ( hIni->hCurProperty == NULL )
		return TRUE;

	return FALSE;
}


