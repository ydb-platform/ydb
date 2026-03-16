/**************************************************
 * ODBCINSTConstructProperties
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

#ifdef UNIXODBC_SOURCE
#include <ltdl.h>
#endif

#include <odbcinstext.h>

/*
static const char *aYesNo[] =
{
	"Yes",
	"No",
	NULL
};
*/

/*! 
 * \brief   Builds a property list for pszDriver.
 *
 *          Adds common DSN properties (Name,Driver,Description) and then asks the
 *          drivers setup to load any additional properties.
 *
 *          This is used to support editing DSN properties without forcing the driver
 *          developer to create a UI for the many different UI implementations. The
 *          driver developer can just implement ODBCINSTGetProperties. This function
 *          can then call ODBCINSTGetProperties to get properties. The code that calls
 *          this function can then display the properties in the UI in use.
 * 
 * \param   pszDriver       Friendly driver name.
 * \param   hFirstProperty  Place to store the properties list. The properties (including
 *                          some of the elements within each HODBCINSTPROPERTY may
 *                          need to be freed using \sa ODBCINSTDestructProperties.
 * 
 * \return  int
 * \retval  ODBCINST_ERROR      Called failed. No memory was allocated at hFirstProperty. The
 *                              likely reasons for this; \li failed to lookup setup library name 
 *                              \li failed to load setup library \li failed to find 
 *                              ODBCINSTGetProperties symbol in setup library
 * \retval  ODBCINST_SUCCESS    Success! Do not forget to call ODBCINSTDestructProperties to
 *                              free memory used by the properties when you are done.
 *
 * \sa      ODBCINSTDestructProperties
 */
int ODBCINSTConstructProperties( char *pszDriver, HODBCINSTPROPERTY *hFirstProperty )
{
	char 				szError[LOG_MSG_MAX+1];
	char 				szDriverSetup[ODBC_FILENAME_MAX+1];
	HINI 				hIni;
	int					(*pODBCINSTGetProperties)( HODBCINSTPROPERTY );
	void 				*hDLL	= NULL;
	HODBCINSTPROPERTY	hLastProperty;
	char				szSectionName[INI_MAX_OBJECT_NAME+1];
    char                szIniName[ ODBC_FILENAME_MAX * 2 + 1 ];
	char				b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];

	/* SANITY CHECKS */
	if ( pszDriver == NULL )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Need a driver name. Make it the friendly name." );
		return ODBCINST_ERROR;
	}
#ifdef VMS
    sprintf( szIniName, "%s:%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ));
#else
    sprintf( szIniName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ));
#endif

	/* GET DRIVER SETUP FILE NAME FOR GIVEN DRIVER */
#ifdef __OS2__
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', FALSE, 1L ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', FALSE ) != INI_SUCCESS )
#endif
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Could not open odbcinst.ini" );
		return ODBCINST_ERROR;
	}

#ifdef PLATFORM64
	/* ASSUME USER FRIENDLY NAME FOR STARTERS */
	if ( iniPropertySeek( hIni, pszDriver, "Setup64", "" ) == INI_SUCCESS )
	{
	}
	else if ( iniPropertySeek( hIni, pszDriver, "Setup", "" ) == INI_SUCCESS )
	{
	}
	else
	{
		/* NOT USER FRIENDLY NAME I GUESS SO ASSUME DRIVER FILE NAME */
		if ( iniPropertySeek( hIni, "", "Driver64", pszDriver ) == INI_SUCCESS )
		{
			iniObject( hIni, szSectionName );
			if ( iniPropertySeek( hIni, szSectionName, "Setup64", "" ) != INI_SUCCESS )
			{
				sprintf( szError, "Could not find Setup property for (%s) in system information", pszDriver );
				inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, szError );
				iniClose( hIni );
				return ODBCINST_ERROR;
			}
		}
		else if ( iniPropertySeek( hIni, "", "Driver", pszDriver ) == INI_SUCCESS )
		{
			iniObject( hIni, szSectionName );
			if ( iniPropertySeek( hIni, szSectionName, "Setup", "" ) != INI_SUCCESS )
			{
				sprintf( szError, "Could not find Setup property for (%s) in system information", pszDriver );
				inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, szError );
				iniClose( hIni );
				return ODBCINST_ERROR;
			}
		}
		else
		{
			sprintf( szError, "Could not find driver (%s) in system information", pszDriver );
			inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, szError );
			iniClose( hIni );
			return ODBCINST_ERROR;
		}
	}
#else
	/* ASSUME USER FRIENDLY NAME FOR STARTERS */
	if ( iniPropertySeek( hIni, pszDriver, "Setup", "" ) != INI_SUCCESS )
	{
		/* NOT USER FRIENDLY NAME I GUESS SO ASSUME DRIVER FILE NAME */
		if ( iniPropertySeek( hIni, "", "Driver", pszDriver ) != INI_SUCCESS )
		{
			sprintf( szError, "Could not find driver (%s) in system information", pszDriver );
			inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, szError );
			iniClose( hIni );
			return ODBCINST_ERROR;
		}
		else
		{
			iniObject( hIni, szSectionName );
			if ( iniPropertySeek( hIni, szSectionName, "Setup", "" ) != INI_SUCCESS )
			{
				sprintf( szError, "Could not find Setup property for (%s) in system information", pszDriver );
				inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, szError );
				iniClose( hIni );
				return ODBCINST_ERROR;
			}
		}
	}
#endif

	iniValue( hIni, szDriverSetup );
	iniClose( hIni );

	if ( szDriverSetup[ 0 ] == '\0' ) 
	{
		sprintf( szError, "Could not find Setup property for (%s) in system information", pszDriver );
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, szError );
		return ODBCINST_ERROR;
	}

    /*
     * initialize libtool
     */

    lt_dlinit();
#ifdef MODULEDIR
    lt_dlsetsearchpath(MODULEDIR);
#endif

	/* TRY GET FUNC FROM DRIVER SETUP */
	if ( !(hDLL = lt_dlopen( szDriverSetup ))  )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Could not load library" );
		return ODBCINST_ERROR;
	}

	pODBCINSTGetProperties = (int(*)(struct tODBCINSTPROPERTY*)) lt_dlsym( hDLL, "ODBCINSTGetProperties" );

/*	PAH - This can be true even when we found the symbol.
    if ( lt_dlerror() != NULL ) 
*/
	if ( pODBCINSTGetProperties == NULL )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Could not find ODBCINSTGetProperties()" );
		lt_dlclose( hDLL );
		return ODBCINST_ERROR;
	}
	
	/* MANDATORY PROPERTIES */
	(*hFirstProperty) 						= (HODBCINSTPROPERTY)malloc( sizeof(ODBCINSTPROPERTY) );
	memset( (*hFirstProperty), 0, sizeof(ODBCINSTPROPERTY) );
	(*hFirstProperty)->nPromptType			= ODBCINST_PROMPTTYPE_TEXTEDIT;
	(*hFirstProperty)->pNext				= NULL;
    (*hFirstProperty)->bRefresh				= 0;
    (*hFirstProperty)->hDLL					= hDLL;
    (*hFirstProperty)->pWidget				= NULL;
    (*hFirstProperty)->pszHelp				= NULL;
	(*hFirstProperty)->aPromptData			= NULL;
	strncpy( (*hFirstProperty)->szName, "Name", INI_MAX_PROPERTY_NAME );
	strcpy( (*hFirstProperty)->szValue, "" );
	hLastProperty = (*hFirstProperty);

	(*hFirstProperty)->pNext 				= (HODBCINSTPROPERTY)malloc( sizeof(ODBCINSTPROPERTY) );
	hLastProperty 							= (*hFirstProperty)->pNext;
	memset( hLastProperty, 0, sizeof(ODBCINSTPROPERTY) );
	hLastProperty->nPromptType				= ODBCINST_PROMPTTYPE_TEXTEDIT;
	hLastProperty->pNext					= NULL;
    hLastProperty->bRefresh					= 0;
    hLastProperty->hDLL						= hDLL;
    hLastProperty->pWidget					= NULL;
    (*hFirstProperty)->pszHelp				= NULL;
	(*hFirstProperty)->aPromptData			= NULL;
	strncpy( hLastProperty->szName, "Description", INI_MAX_PROPERTY_NAME );
	strncpy( hLastProperty->szValue, pszDriver, INI_MAX_PROPERTY_VALUE );

	hLastProperty->pNext 				= (HODBCINSTPROPERTY)malloc( sizeof(ODBCINSTPROPERTY) );
	hLastProperty 							= hLastProperty->pNext;
	memset( hLastProperty, 0, sizeof(ODBCINSTPROPERTY) );
	hLastProperty->nPromptType				= ODBCINST_PROMPTTYPE_LABEL;
	hLastProperty->pNext					= NULL;
    hLastProperty->bRefresh					= 0;
    hLastProperty->hDLL						= hDLL;
    hLastProperty->pWidget					= NULL;
    (*hFirstProperty)->pszHelp				= NULL;
	(*hFirstProperty)->aPromptData			= NULL;
	strncpy( hLastProperty->szName, "Driver", INI_MAX_PROPERTY_NAME );
	strncpy( hLastProperty->szValue, pszDriver, INI_MAX_PROPERTY_VALUE );
/*
	hLastProperty->pNext 				= (HODBCINSTPROPERTY)malloc( sizeof(ODBCINSTPROPERTY) );
	hLastProperty 						= hLastProperty->pNext;
	memset( hLastProperty, 0, sizeof(ODBCINSTPROPERTY) );
	hLastProperty->nPromptType			= ODBCINST_PROMPTTYPE_LISTBOX;
    hLastProperty->aPromptData			= malloc( sizeof(aYesNo) );
	memcpy( hLastProperty->aPromptData, aYesNo, sizeof(aYesNo) );
	strncpy( hLastProperty->szName, "Trace", INI_MAX_PROPERTY_NAME );
    strcpy( hLastProperty->szValue, "No" );

	hLastProperty->pNext 				= (HODBCINSTPROPERTY)malloc( sizeof(ODBCINSTPROPERTY) );
	hLastProperty 						= hLastProperty->pNext;
	memset( hLastProperty, 0, sizeof(ODBCINSTPROPERTY) );
	hLastProperty->nPromptType			= ODBCINST_PROMPTTYPE_FILENAME;
	strncpy( hLastProperty->szName, "TraceFile", INI_MAX_PROPERTY_NAME );
	strncpy( hLastProperty->szValue, "", INI_MAX_PROPERTY_VALUE );
*/

	/* APPEND OTHERS */
	pODBCINSTGetProperties( hLastProperty );

    lt_dlclose( hDLL );

	return ODBCINST_SUCCESS;
}


