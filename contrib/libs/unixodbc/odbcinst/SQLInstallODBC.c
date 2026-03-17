/*************************************************
 * SQLInstallODBC
 *
 * Just provide stub for ODBC 2 installer functions
 *
 **************************************************/

#include <config.h>
#include <odbcinstext.h>

BOOL INSTAPI SQLInstallODBC(HWND       hwndParent,
                                      LPCSTR     lpszInfFile,
									  LPCSTR     lpszSrcPath,
									  LPCSTR     lpszDrivers)
{
	return FALSE;
}

BOOL INSTAPI SQLInstallDriver        (LPCSTR     lpszInfFile,
                                      LPCSTR     lpszDriver,
                                      LPSTR      lpszPath,
                                      WORD       cbPathMax,
                                      WORD 		* pcbPathOut)
{
	return FALSE;
}

BOOL INSTAPI SQLInstallTranslator( LPCSTR lpszInfFile,
								LPCSTR	pszTranslator,
								LPCSTR	pszPathIn,
								LPSTR	pszPathOut,
								WORD	nPathOutMax,
								WORD	*pnPathOut,
								WORD	nRequest,
								LPDWORD	pnUsageCount )

{
    inst_logClear();

	return FALSE;
}

BOOL INSTAPI SQLRemoveDefaultDataSource( void ) 
{
    inst_logClear();

	return SQLConfigDataSource (NULL, ODBC_REMOVE_DEFAULT_DSN, NULL, NULL);
}

BOOL INSTAPI SQLInstallDriverW        (LPCWSTR     lpszInfFile,
                                      LPCWSTR     lpszDriver,
                                      LPWSTR      lpszPath,
                                      WORD       cbPathMax,
                                      WORD      * pcbPathOut)
{
	return FALSE;
}

BOOL INSTAPI SQLInstallODBCW          (HWND       hwndParent,
                                      LPCWSTR     lpszInfFile,
									  LPCWSTR     lpszSrcPath,
									  LPCWSTR     lpszDrivers)
{
	return FALSE;
}

BOOL INSTAPI SQLInstallTranslatorW(LPCWSTR lpszInfFile,
                                  LPCWSTR lpszTranslator,
                                  LPCWSTR lpszPathIn,
                                  LPWSTR  lpszPathOut,
                                  WORD   cbPathOutMax,
                                  WORD  *pcbPathOut,
                                  WORD   fRequest,
                                  LPDWORD   lpdwUsageCount)
{
    inst_logClear();

	return FALSE;
}
