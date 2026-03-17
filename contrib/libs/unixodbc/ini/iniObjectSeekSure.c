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
 * -----------------------------------------------
 *
 * PAH	06.MAR.99	Added this func
 **************************************************/

#include <config.h>
#include "ini.h"

int iniObjectSeekSure( HINI hIni, char *pszObject )
{
	int nReturn;

    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;
	if ( !pszObject )
        return INI_ERROR;

	if ( (nReturn = iniObjectSeek( hIni, pszObject )) == INI_NO_DATA )
        return iniObjectInsert( hIni, pszObject );

	return nReturn;
}


