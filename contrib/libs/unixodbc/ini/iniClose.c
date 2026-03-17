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
 * iniClose
 *
 * 1. free memory previously allocated for HINI
 * 2. DO NOT save any changes (see iniCommit)
 ******************************/
int iniClose( HINI hIni )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	hIni->hCurObject = hIni->hFirstObject;
	while ( iniObjectDelete( hIni ) == INI_SUCCESS )
	{
	}
	
	free( hIni );

	return INI_SUCCESS;
}


