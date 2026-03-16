/**************************************************
 * odbcinstext.h
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#ifndef _ODBCINST_H
#define _ODBCINST_H

#ifdef UNIXODBC_SOURCE

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifndef ODBCVER
#define ODBCVER 0x0380
#endif

#include <ini.h>
#include <log.h>
#include <odbcinst.h>

/********************************************************
 * CONSTANTS WHICH DO NOT EXIST ELSEWHERE
 ********************************************************/
#ifndef TRUE
#define FALSE 0;
#define TRUE 1;
#endif

#else /* not UNIXODBC_SOURCE */

/********************************************************
  * outside the unixODBC source tree only the            *
  * public interface is exposed                          *
  ********************************************************/

#include <odbcinst.h>

/********************************************************
  * these limits are originally defined in ini.h         *
  * but are needed to implement ODBCINSTGetProperties    *
  * for the Driver Setup                                 *
  ********************************************************/

#define     INI_MAX_LINE            1000
#define     INI_MAX_OBJECT_NAME     INI_MAX_LINE
#define     INI_MAX_PROPERTY_NAME   INI_MAX_LINE
#define     INI_MAX_PROPERTY_VALUE  INI_MAX_LINE

#endif  /* UNIXODBC_SOURCE */

/********************************************************
 * PUBLIC API
 ********************************************************/

#ifdef __cplusplus
extern "C"
{
#endif

BOOL INSTAPI SQLConfigDataSource(		HWND	hWnd,
								WORD	nRequest,
								LPCSTR	pszDriver,
								LPCSTR	pszAttributes );

BOOL INSTAPI SQLGetConfigMode(          UWORD	*pnConfigMode );

BOOL INSTAPI SQLGetInstalledDrivers(	LPSTR	pszBuf,
								WORD	nBufMax,
								WORD	*pnBufOut );

BOOL INSTAPI SQLInstallDriverEx(		LPCSTR	pszDriver,
								LPCSTR	pszPathIn,
								LPSTR	pszPathOut,
								WORD	nPathOutMax,
								WORD	*nPathOut,
								WORD	nRequest,
								LPDWORD	pnUsageCount );

BOOL INSTAPI SQLInstallDriverManager(	LPSTR	pszPath,
								WORD	nPathMax,
								WORD	*pnPathOut );

RETCODE INSTAPI SQLInstallerError(		WORD	nError,
								DWORD	*pnErrorCode,
								LPSTR	pszErrorMsg,
								WORD	nErrorMsgMax,
								WORD	*nErrorMsg );

BOOL INSTAPI SQLManageDataSources(		HWND	hWnd );

BOOL INSTAPI SQLReadFileDSN(			LPCSTR	pszFileName,
								LPCSTR	pszAppName,
								LPCSTR	pszKeyName,
								LPSTR	pszString,
								WORD	nString,
								WORD	*pnString );

BOOL INSTAPI SQLRemoveDriver(			LPCSTR	pszDriver,
								BOOL	nRemoveDSN,
								LPDWORD	pnUsageCount );

BOOL INSTAPI SQLRemoveDriverManager(	LPDWORD	pnUsageCount );

BOOL INSTAPI SQLRemoveDSNFromIni(		LPCSTR	pszDSN );

BOOL INSTAPI SQLRemoveTranslator(		LPCSTR	pszTranslator,
								LPDWORD	pnUsageCount );

BOOL INSTAPI SQLSetConfigMode(			UWORD	nConfigMode );

BOOL INSTAPI SQLValidDSN(				LPCSTR	pszDSN );

BOOL INSTAPI SQLWriteDSNToIni(			LPCSTR	pszDSN,
								LPCSTR	pszDriver );

BOOL INSTAPI SQLWriteFileDSN(			LPCSTR	pszFileName,
								LPCSTR	pszAppName,
								LPCSTR	pszKeyName,
								LPCSTR	pszString );

BOOL INSTAPI SQLWritePrivateProfileString(
								LPCSTR	pszSection,
								LPCSTR	pszEntry,
								LPCSTR	pszString,
								LPCSTR	pszFileName );



#ifdef __cplusplus
}
#endif

#ifdef UNIXODBC_SOURCE

/********************************************************
 * PRIVATE API
 ********************************************************/
#if defined(__cplusplus)
         extern  "C" {
#endif

BOOL _odbcinst_UserINI(
	char *pszFileName,
	BOOL bVerify );

BOOL _odbcinst_SystemINI(		
	char *pszFileName,
	BOOL bVerify );

BOOL _odbcinst_FileINI(	char *pszPath );

char * INSTAPI odbcinst_system_file_path( char *buffer );
char * INSTAPI odbcinst_system_file_name( char *buffer );
char * INSTAPI odbcinst_user_file_path( char *buffer );
char * INSTAPI odbcinst_user_file_name( char *buffer );

BOOL _odbcinst_ConfigModeINI( 	
	char *pszFileName );

int _odbcinst_GetSections(	
	HINI	hIni,
	LPSTR	pRetBuffer,
	int		nRetBuffer,
	int		*pnBufPos );

int _odbcinst_GetEntries(	
	HINI	hIni,
	LPCSTR	pszSection,
	LPSTR	pRetBuffer,
	int		nRetBuffer,
	int		*pnBufPos );

int _SQLGetInstalledDrivers(	
	LPCSTR	pszSection,
	LPCSTR	pszEntry,
	LPCSTR	pszDefault,
	LPCSTR	pRetBuffer,
	int     nRetBuffer );

BOOL _SQLWriteInstalledDrivers(
	LPCSTR	pszSection,
	LPCSTR	pszEntry,
	LPCSTR	pszString );

BOOL _SQLDriverConnectPrompt( 
	HWND hwnd, 
	SQLCHAR *dsn, 
	SQLSMALLINT len_dsn );

BOOL _SQLDriverConnectPromptW( 
	HWND hwnd, 
	SQLWCHAR *dsn, 
	SQLSMALLINT len_dsn );

void __set_config_mode( int mode );
int __get_config_mode( void );
void __lock_config_mode( void );
void __unlock_config_mode( void );

int inst_logPushMsg( 
        char *pszModule, 
        char *pszFunctionName, 
        int nLine, 
        int nSeverity, 
        int nCode, 
        char *pszMessage );

int inst_logPeekMsg( long nMsg, HLOGMSG *phMsg );
int inst_logClear();

int __SQLGetPrivateProfileStringNL( LPCSTR  pszSection,
                                LPCSTR  pszEntry,
                                LPCSTR  pszDefault,
                                LPSTR   pRetBuffer,
                                int     nRetBuffer,
                                LPCSTR  pszFileName
                              );

/*
 * we should look at caching this info, the calls can become expensive
 */

#ifndef DISABLE_INI_CACHING

struct ini_cache
{
    char                *fname;
    char                *section;
    char                *entry;
    char                *value;
    char                *default_value;
    int                 buffer_size;
    int                 ret_value;
    int                 config_mode;
    long                timestamp;
    struct ini_cache    *next;
};

#endif

#ifdef __cplusplus
}
#endif

#endif /* UNIXODBC_SOURCE */

/*********************************
 * ODBCINST - PROPERTIES
 *********************************
 *
 * PURPOSE:
 *
 * To provide the caller a mechanism to interact with Data Source properties
 * containing Driver specific options while avoiding embedding GUI code in
 * the ODBC infrastructure.
 *
 * DETAILS:
 *
 *	1.	Application calls libodbcinst.ODBCINSTConstructProperties()
 *		- odbcinst will load the driver and call libMyDrvS.ODBCINSTGetProperties() to build a list of all possible properties
 *	2.	Application calls libodbcinst.ODBCINSTSetProperty()
 *		- use, as required, to init values (ie if configuring existing DataSource)
 *		- use libodbcinst.SetConfigMode() & libodbcinst.SQLGetPrivateProfileString() to read existing Data Source info (do not forget to set the mode back)
 *		- do not forget to set mode back to ODBC_BOTH_DSN using SetConfigMode() when done reading
 *		- no call to Driver Setup
 *	3.	Application calls libodbcinst.ODBCINSTValidateProperty()
 *		- use as required (ie on leave widget event)
 *		- an assesment of the entire property list is also done
 *		- this is passed onto the driver setup DLL
 *	4.	Application should refresh widgets in case aPromptData or szValue has changed
 *		- refresh should occur for each property where bRefresh = 1
 *	5.	Application calls libodbcinst.ODBCINSTValidateProperties()
 *		- call this just before saving new Data Source or updating existing Data Source
 *		- should always call this before saving
 *		- use libodbcinst.SetConfigMode() & libodbcinst.SQLWritePrivateProfileString() to save Data Source info
 *		- do not forget to set mode back to ODBC_BOTH_DSN using SetConfigMode() when done saving
 *		- this is passed onto the driver setup DLL
 *	6.	Application calls ODBCINSTDestructProperties() to free up memory
 *		- unload Driver Setup DLL
 *		- frees memory (Driver Setup allocates most of the memory but we free ALL of it in odbcinst)
 *
 * NOTES
 *
 *	1.	odbcinst implements 5 functions to support this GUI config stuff
 *	2.	Driver Setup DLL implements just 3 functions for its share of the work
 *
 *********************************/

#define ODBCINST_SUCCESS				0
#define ODBCINST_WARNING				1
#define ODBCINST_ERROR					2

#define	ODBCINST_PROMPTTYPE_LABEL		0 /* readonly */
#define	ODBCINST_PROMPTTYPE_TEXTEDIT	1
#define	ODBCINST_PROMPTTYPE_LISTBOX		2
#define	ODBCINST_PROMPTTYPE_COMBOBOX	3
#define	ODBCINST_PROMPTTYPE_FILENAME	4
#define	ODBCINST_PROMPTTYPE_HIDDEN	    5 
#define ODBCINST_PROMPTTYPE_TEXTEDIT_PASSWORD 6

typedef struct	tODBCINSTPROPERTY
{
	struct tODBCINSTPROPERTY *pNext;				/* pointer to next property, NULL if last property										*/

	char	szName[INI_MAX_PROPERTY_NAME+1];		/* property name																		*/
	char	szValue[INI_MAX_PROPERTY_VALUE+1];		/* property value																		*/
	int		nPromptType;							/* PROMPTTYPE_TEXTEDIT, PROMPTTYPE_LISTBOX, PROMPTTYPE_COMBOBOX, PROMPTTYPE_FILENAME	*/
	char	**aPromptData;							/* array of pointers terminated with a NULL value in array. 							*/
	char	*pszHelp;								/* help on this property (driver setups should keep it short)							*/
	void	*pWidget;								/* CALLER CAN STORE A POINTER TO ? HERE													*/
	int		bRefresh;								/* app should refresh widget ie Driver Setup has changed aPromptData or szValue   		*/
	void 	*hDLL;									/* for odbcinst internal use... only first property has valid one 						*/
} ODBCINSTPROPERTY, *HODBCINSTPROPERTY;

/*
 * Plugin name
 */

#define ODBCINSTPLUGIN          "odbcinstQ5"

/*
 * Conversion routines for wide interface
 */

char* _multi_string_alloc_and_copy( LPCWSTR in );
char* _single_string_alloc_and_copy( LPCWSTR in );
void _single_string_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len );
int _multi_string_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len );
int _single_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len );
SQLWCHAR* _multi_string_alloc_and_expand( LPCSTR in );
SQLWCHAR* _single_string_alloc_and_expand( LPCSTR in );
void _single_copy_from_wide( SQLCHAR *out, LPCWSTR in, int len );
int _multi_string_length( LPCSTR in );

/*
 * To support finding UI plugin
 */
char *_getUIPluginName( char *pszName, char *pszUI );
char *_appendUIPluginExtension( char *pszNameAndExtension, char *pszName );
char *_prependUIPluginPath( char *pszPathAndName, char *pszName );

#if defined(__cplusplus)
         extern  "C" {
#endif

/* ONLY IMPLEMENTED IN ODBCINST (not in Driver Setup) */
int INSTAPI ODBCINSTConstructProperties( char *szDriver, HODBCINSTPROPERTY *hFirstProperty );
int INSTAPI ODBCINSTSetProperty( HODBCINSTPROPERTY hFirstProperty, char *pszProperty, char *pszValue );
int INSTAPI ODBCINSTDestructProperties( HODBCINSTPROPERTY *hFirstProperty );
int INSTAPI ODBCINSTAddProperty( HODBCINSTPROPERTY hFirstProperty, char *pszProperty, char *pszValue );

/* IMPLEMENTED IN ODBCINST AND DRIVER SETUP */
int INSTAPI ODBCINSTValidateProperty( HODBCINSTPROPERTY hFirstProperty, char *pszProperty, char *pszMessage );
int INSTAPI ODBCINSTValidateProperties( HODBCINSTPROPERTY hFirstProperty, HODBCINSTPROPERTY hBadProperty, char *pszMessage );

/* ONLY IMPLEMENTED IN DRIVER SETUP (not in ODBCINST) */
int INSTAPI ODBCINSTGetProperties( HODBCINSTPROPERTY hFirstProperty );

#if defined(__cplusplus)
         }
#endif

#endif

