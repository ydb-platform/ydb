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

int iniToUpper( char *pszString )
{
	int n = 0;

	for ( n = 0; pszString[n] != '\0'; n++ )
		pszString[n] = toupper((unsigned char)pszString[n]);

    return INI_SUCCESS;
}


