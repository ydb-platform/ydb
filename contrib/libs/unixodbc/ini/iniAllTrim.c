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

int iniAllTrim( char *pszString )
{
	int nForwardCursor	= 0;
	int nTrailingCursor	= 0;
	int bTrim			= 1;

	/* TRIM LEFT */
	for ( nForwardCursor=0; pszString[nForwardCursor] != '\0'; nForwardCursor++ )
	{
		if ( bTrim && isspace( pszString[nForwardCursor] ) )
		{
			/* DO NOTHING */
		}
		else
		{
			bTrim = 0;
			pszString[nTrailingCursor] = pszString[nForwardCursor];
			nTrailingCursor++;
		}
	}
	pszString[nTrailingCursor] = '\0';

	/* TRIM RIGHT */
	for ( nForwardCursor=strlen(pszString)-1;
		 nForwardCursor >= 0 && isspace( pszString[nForwardCursor] );
		 nForwardCursor-- )
	{
	}
	pszString[nForwardCursor+1] = '\0';

    return INI_SUCCESS;
}


