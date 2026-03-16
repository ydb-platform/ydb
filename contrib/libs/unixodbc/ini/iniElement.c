/**********************************************************************************
 * iniElement
 *
 * Use when;
 * 1. strtok is scary (also does not handle empty elements well)
 * 2. strstr is not portable
 * 3. performance is less important than simplicity and the above (feel free to improve on this)
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

int iniElement( char *pszData, char cSeperator, char cTerminator, int nElement, char *pszElement, int nMaxElement )
{
	int nCurElement		= 0;
	int nChar			= 0;
	int nCharInElement	= 0;

	memset( pszElement, '\0', nMaxElement );
	for ( ; nCurElement <= nElement && (nCharInElement+1) < nMaxElement; nChar++ )
	{
		/* check for end of data */
		if ( cSeperator != cTerminator && pszData[nChar] == cTerminator )
		{
			break;
		}

		if ( cSeperator == cTerminator && pszData[nChar] == cSeperator && pszData[nChar+1] == cTerminator )
		{
			break;
		}

		/* check for end of element */
		if ( pszData[nChar] == cSeperator )
		{
			nCurElement++;
		}
		else if ( nCurElement == nElement )
		{
			pszElement[nCharInElement] = pszData[nChar];
			nCharInElement++;
		}
	}

	if ( pszElement[0] == '\0' )
	{
		return INI_NO_DATA;
	}

	return INI_SUCCESS;
}

/* Like iniElement(), but rather than a terminator, the input buffer length is given */

int iniElementMax( char *pData, char cSeperator, int nDataLen, int nElement, char *pszElement, int nMaxElement )
{
	int nCurElement		= 0;
	int nChar			= 0;
	int nCharInElement	= 0;

	memset( pszElement, '\0', nMaxElement );
	for ( ; nCurElement <= nElement && (nCharInElement+1) < nMaxElement && nChar < nDataLen ; nChar++ )
	{
		/* check for end of element */
		if ( pData[nChar] == cSeperator )
		{
			nCurElement++;
		}
		else if ( nCurElement == nElement )
		{
			pszElement[nCharInElement] = pData[nChar];
			nCharInElement++;
		}
	}

	if ( pszElement[0] == '\0' )
	{
		return INI_NO_DATA;
	}

	return INI_SUCCESS;
}


int iniElementEOL( char *pszData, char cSeperator, char cTerminator, int nElement, char *pszElement, int nMaxElement )
{
	int nCurElement		= 0;
	int nChar			= 0;
	int nCharInElement	= 0;

	memset( pszElement, '\0', nMaxElement );
	for ( ;(nCharInElement+1) < nMaxElement; nChar++ )
	{
		/* check for end of data */
		if ( cSeperator != cTerminator && pszData[nChar] == cTerminator )
		{
			break;
		}

		if ( cSeperator == cTerminator && pszData[nChar] == cSeperator && pszData[nChar+1] == cTerminator )
		{
			break;
		}

		/* check for end of element */
		if ( pszData[nChar] == cSeperator && nCurElement < nElement )
		{
			nCurElement++;
		}
		else if ( nCurElement >= nElement )
		{
			pszElement[nCharInElement] = pszData[nChar];
			nCharInElement++;
		}
	}

	if ( pszElement[0] == '\0' )
	{
		return INI_NO_DATA;
	}

	return INI_SUCCESS;
}

int iniElementToEnd( char *pszData, char cSeperator, char cTerminator, int nElement, char *pszElement, int nMaxElement )
{
    int nCurElement     = 0;
    int nChar           = 0;
    int nCharInElement  = 0;

    memset( pszElement, '\0', nMaxElement );
    for ( ; nCurElement <= nElement && (nCharInElement+1) < nMaxElement; nChar++ )
    {
        /* check for end of data */
        if ( cSeperator != cTerminator && pszData[nChar] == cTerminator )
            break;

        if ( cSeperator == cTerminator && pszData[nChar] == cSeperator && pszData[nChar+1] == cTerminator )
            break;

        /* check for end of element */
        if ( pszData[nChar] == cSeperator && ( nCurElement < nElement ))
            nCurElement++;
        else if ( nCurElement == nElement )
        {
            pszElement[nCharInElement] = pszData[nChar];
            nCharInElement++;
        }
    }

    if ( pszElement[0] == '\0' )
        return INI_NO_DATA;

    return INI_SUCCESS;
}

