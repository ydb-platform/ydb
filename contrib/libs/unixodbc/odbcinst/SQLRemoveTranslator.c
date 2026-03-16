/**************************************************
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

BOOL SQLRemoveTranslator(		LPCSTR	pszTranslator,
								LPDWORD	pnUsageCount )
{
    inst_logClear();

	return FALSE;
}

BOOL INSTAPI SQLRemoveTranslatorW(LPCWSTR lpszTranslator,
                                 LPDWORD lpdwUsageCount)
{
    inst_logClear();

	return FALSE;
}
