/**********************************************************************************
 * iniElementCount
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 05.APR.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/

#include <config.h>
#include "ini.h"

int iniElementCount( char *pszData, char cSeperator, char cTerminator )
{
	int	nToManyElements = 30000;
	int nCurElement		= 0;
	int nChar			= 0;

	for ( ; nCurElement <= nToManyElements; nChar++ )
	{
		/* check for end of data */
        if ( cSeperator != cTerminator && pszData[nChar] == cTerminator )
			break;

		if ( cSeperator == cTerminator && pszData[nChar] == cSeperator && pszData[nChar+1] == cTerminator )
			break;

		/* check for end of element */
		if ( pszData[nChar] == cSeperator )
			nCurElement++;
	}

	return nCurElement;
}

