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
 * iniDelete
 *
 ******************************/
int iniDelete( HINI hIni )
{

    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	/* REMOVE ALL SUBORDINATE INFO */
	iniObjectFirst( hIni );
	while ( iniObjectDelete( hIni ) == INI_SUCCESS )
	{
	}

	return INI_SUCCESS;
}


