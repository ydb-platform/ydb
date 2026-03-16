/**************************************************
 * SQLGetAvailableDrivers
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
#include <odbcinstext.h>

BOOL SQLGetAvailableDrivers(	LPCSTR     	pszInfFile,
								LPSTR      	pszBuf,
								WORD       	nBufMax,
								WORD 		*pnBufOut)
{
	return SQLGetInstalledDrivers(	pszBuf,	nBufMax, pnBufOut );
}

BOOL INSTAPI SQLGetAvailableDriversW  (LPCWSTR     lpszInfFile,
                                      LPWSTR      lpszBuf,
                                      WORD       cbBufMax,
                                      WORD      * pcbBufOut)
{
	return SQLGetInstalledDriversW(	lpszBuf, cbBufMax, pcbBufOut );
}
