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


BOOL SQLGetTranslator(			HWND	hWnd,
								LPSTR	pszName,
								WORD	nNameMax,
								WORD	*pnNameOut,
								LPSTR	pszPath,
								WORD	nPathMax,
								WORD	*pnPathOut,
								DWORD	*pnOption )
{
    inst_logClear();

	return FALSE;
}


BOOL INSTAPI SQLGetTranslatorW        (HWND       hwnd,
                                      LPWSTR      lpszName,
                                      WORD       cbNameMax,
                                      WORD     *pcbNameOut,
                                      LPWSTR      lpszPath,
                                      WORD       cbPathMax,
                                      WORD     *pcbPathOut,
                                      DWORD    *pvOption)
{
    inst_logClear();

	return FALSE;
}
