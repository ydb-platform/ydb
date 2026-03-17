/**********************************************************************************
 * ini.h
 *
 * Include file for libini.a. Coding? Include this and link against libini.a.
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

#ifndef INCLUDED_INI_H
#define INCLUDED_INI_H

/*********[ CONSTANTS AND TYPES ]**************************************************/
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>

#ifndef TRUE
#define TRUE    1
#endif

#ifndef FALSE
#define FALSE   0
#endif

#define     STDINFILE               ((char*)-1)

#define     INI_NO_DATA             2
#define     INI_SUCCESS             1
#define     INI_ERROR               0

#define     INI_MAX_LINE            1000
#define     INI_MAX_OBJECT_NAME     INI_MAX_LINE
#define     INI_MAX_PROPERTY_NAME   INI_MAX_LINE
#define     INI_MAX_PROPERTY_VALUE  INI_MAX_LINE

#if HAVE_LIMITS_H
#include <limits.h>
#endif

#ifdef PATH_MAX
#define ODBC_FILENAME_MAX         PATH_MAX
#elif MAXPATHLEN
#define ODBC_FILENAME_MAX         MAXPATHLEN
#else
#define ODBC_FILENAME_MAX         FILENAME_MAX
#endif

/********************************************
 * tINIPROPERTY
 *
 * Each property line has Name=Value pair.
 * They are stored in this structure and linked together to provide a list
 * of all properties for a given Object.
 ********************************************/

typedef struct	tINIPROPERTY
{
	struct	tINIPROPERTY	*pNext;
	struct	tINIPROPERTY	*pPrev;

	char	szName[INI_MAX_PROPERTY_NAME+1];
	char	szValue[INI_MAX_PROPERTY_VALUE+1];

} INIPROPERTY, *HINIPROPERTY;

/********************************************
 * tINIOBJECT
 *
 * Each object line has just an object name. This structure
 * stores the object name and its subordinate information.
 * The lines that follow are considered to be properties
 * and are stored in a list of tINIPROPERTY.
 ********************************************/

typedef struct	tINIOBJECT
{
	struct	tINIOBJECT	*pNext;
	struct	tINIOBJECT	*pPrev;

	char	szName[INI_MAX_OBJECT_NAME+1];

    HINIPROPERTY		hFirstProperty;
    HINIPROPERTY		hLastProperty;
	int					nProperties;

} INIOBJECT, *HINIOBJECT;

/********************************************
 * tINI
 *
 * Each INI file contains a list of objects. This
 * structure stores each object in a list of tINIOBJECT.
 ********************************************/

typedef struct     tINI
{
#ifdef __OS2__
    int     iniFileType;			/*  ini file type 0 = text, 1 = binary (OS/2 only) */
#endif
    char    szFileName[ODBC_FILENAME_MAX+1];        /* FULL INI FILE NAME                                                           */
    char    cComment[ 5 ];                          /* COMMENT CHAR MUST BE IN FIRST COLUMN                                         */
    char    cLeftBracket;                           /* BRACKETS DELIMIT THE OBJECT NAME, THE LEFT BRACKET MUST BE IN COLUMN ONE     */
    char    cRightBracket;
    char    cEqual;                                 /* SEPERATES THE PROPERTY NAME FROM ITS VALUE                                   */
	int		bChanged;								/* IF true, THEN THE WHOLE FILE IS OVERWRITTEN UPON iniCommit					*/
	int		bReadOnly;								/* TRUE IF AT LEAST ONE CALL HAS BEEN MADE TO iniAppend() 						*/

	HINIOBJECT		hFirstObject;
	HINIOBJECT		hLastObject;
	HINIOBJECT		hCurObject;
	int				nObjects;

	HINIPROPERTY	hCurProperty;

} INI, *HINI;

/********************************************
 * tINIBOOKMARK
 *
 * Used to store the current Object and Property pointers so
 * that the caller can quickly return to some point in the
 * INI data.
 ********************************************/

typedef struct tINIBOOKMARK
{
	HINI			hIni;
	HINIOBJECT		hCurObject;
    HINIPROPERTY	hCurProperty;

} INIBOOKMARK, *HINIBOOKMARK;

#if defined(__cplusplus)
         extern  "C" {
#endif

/*********[ PRIMARY INTERFACE ]*****************************************************/

/******************************
 * iniOpen
 *
 * 1. make sure file exists
 * 2. allocate memory for HINI
 * 3. initialize HINI
 * 4. load entire file into structured memory
 * 5. set TRUE if you want to create file if non-existing
 ******************************/
#ifdef __OS2__
int iniOpen( HINI *hIni, char *pszFileName, char *cComment, char cLeftBracket, char cRightBracket, char cEqual, int bCreate, int bFileType  );
#else
int iniOpen( HINI *hIni, char *pszFileName, char *cComment, char cLeftBracket, char cRightBracket, char cEqual, int bCreate  );
#endif
/******************************
 * iniAppend
 *
 * 1. append Sections in pszFileName that do not already exist in hIni
 * 2. Makes hIni ReadOnly!
 ******************************/
int iniAppend( HINI hIni, char *pszFileName );

/******************************
 * iniDelete
 *
 * 1. simple removes all objects (and their properties) from the list using iniObjectDelete() in a loop
 ******************************/
int iniDelete( HINI hIni );

/******************************
 * iniClose
 *
 * 1. free memory previously allocated for HINI
 * 2. DO NOT SAVE ANY CHANGES (see iniCommit)
 ******************************/
int iniClose( HINI hIni );

/******************************
 * iniCommit
 *
 * 1. replaces file contents with memory contents (overwrites the file)
 ******************************/
int iniCommit( HINI hIni );

/******************************
 * iniObjectFirst
 *
 ******************************/
int iniObjectFirst( HINI hIni );

/******************************
 * iniObjectLast
 *
 ******************************/
int iniObjectLast( HINI hIni );

/******************************
 * iniObjectNext
 *
 * 1. iniObjects() if no current object name else
 * 2. find and store next object name
 ******************************/
int iniObjectNext( HINI hIni );

/******************************
 * iniObjectSeek
 *
 * 1. find and store object name
 ******************************/
int iniObjectSeek( HINI hIni, char *pszObject );

/******************************
 * iniObjectSeekSure
 *
 * 1. find and store object name
 * 2. ensure that it exists
 ******************************/
int iniObjectSeekSure( HINI hIni, char *pszObject );

/******************************
 * iniObjectEOL
 *
 ******************************/
int iniObjectEOL( HINI hIni );

/******************************
 * iniObject
 *
 * 1. returns the current object name
 ******************************/
int iniObject( HINI hIni, char *pszObject );

/******************************
 * iniObjectDelete
 *
 * 1. deletes current Object
 ******************************/
int iniObjectDelete( HINI hIni );

/******************************
 * iniObjectUpdate
 *
 * 1. update current Object
 ******************************/
int iniObjectUpdate( HINI hIni, char *pszObject );

/******************************
 * iniPropertyObject
 *
 * 1. inserts a new Object
 * 2. becomes current
 ******************************/
int iniObjectInsert( HINI hIni, char *pszObject );

/******************************
 * iniPropertyFirst
 *
 ******************************/
int iniPropertyFirst( HINI hIni );

/******************************
 * iniPropertyLast
 *
 ******************************/
int iniPropertyLast( HINI hIni );

/******************************
 * iniPropertyNext
 *
 * 1. iniProperties() if no current property name else
 * 2. find and store next property name
 ******************************/
int iniPropertyNext( HINI hIni );

/******************************
 * iniPropertySeek
 *
 * 1. set current Object & Property positions where matching parameters
 * 2. any parms which are empty strings (ie pszObject[0]) are ignored
 * 3. it is kinda pointless to pass empty strings for all parms... you will get 1st Property in 1st Object
 ******************************/
int iniPropertySeek( HINI hIni, char *pszObject, char *pszProperty, char *pszValue );

/******************************
 * iniPropertySeekSure
 *
 * 1. same as iniPropertySeek but
 * 2. will ensure that both Object and Property exist
 ******************************/
int iniPropertySeekSure( HINI hIni, char *pszObject, char *pszProperty, char *pszValue );

/******************************
 * iniPropertyEOL
 *
 ******************************/
int iniPropertyEOL( HINI hIni );

/******************************
 * iniProperty
 *
 * 1. returns the current property name
 ******************************/
int iniProperty( HINI hIni, char *pszProperty );

/******************************
 * iniPropertyDelete
 *
 * 1. deletes current Property
 ******************************/
int iniPropertyDelete( HINI hIni );

/******************************
 * iniPropertyUpdate
 *
 * 1. update current Property
 ******************************/
int iniPropertyUpdate( HINI hIni, char *pszProperty, char *pszValue );

/******************************
 * iniPropertyInsert
 *
 * 1. inserts a new Property for current Object
 * 2. becomes current
 ******************************/
int iniPropertyInsert( HINI hIni, char *pszProperty, char *pszValue );

/******************************
 * iniValue
 *
 * 1. returns the value for the current object/property
 ******************************/
int iniValue( HINI hIni, char *pszValue );

/******************************
 * iniGetBookmark
 *
 * 1. Store the current data positions (Object and Property)
 *    into hIniBookmark.
 * 2. Does not allocate memory for hIniBookmark so pass a
 *    pointer to a valid bookmark.
 ******************************/
int iniGetBookmark( HINI hIni, HINIBOOKMARK hIniBookmark );

/******************************
 * iniGotoBookmark
 *
 * 1. Sets the current Object and Property positions to
 *    those stored in IniBookmark.
 * 2. Does not account for the bookmark becoming
 *    invalid ie from the Object or Property being deleted.
 ******************************/
int iniGotoBookmark( INIBOOKMARK IniBookmark );

/******************************
 * iniCursor
 *
 * 1. Returns a copy of the hIni with a new
 *    set of position cursors (current Object and Property).
 * 2. Not safe to use when in the possibility of
 *    deleting data in another cursor on same data.
 * 3. Use when reading data only.
 * 4. Does not allocate memory so hIniCursor should be valid.
 * 5. All calls, other than those for movement, are
 *    global and will affect any other view of the data.
 ******************************/
int iniCursor( HINI hIni, HINI hIniCursor );

/*************************************************************************************/
/*********[ SUPPORT FUNCS ]***********************************************************/
/*************************************************************************************/

/******************************
 * iniElement
 *
 ******************************/
int iniElement( char *pszData, char cSeperator, char cTerminator, int nElement, char *pszElement, int nMaxElement );
int iniElementMax( char *pData, char cSeperator, int nDataLen, int nElement, char *pszElement, int nMaxElement );
int iniElementToEnd( char *pszData, char cSeperator, char cTerminator, int nElement, char *pszElement, int nMaxElement );
int iniElementEOL( char *pszData, char cSeperator, char cTerminator, int nElement, char *pszElement, int nMaxElement );

/******************************
 * iniElementCount
 *
 ******************************/
int iniElementCount( char *pszData, char cSeperator, char cTerminator );

/******************************
 * iniPropertyValue
 *
 * 1. returns the property value for pszProperty in pszValue
 * 2. pszString example;
 *		"PropertyName1=Value1;PropertyName2=Value2;..."
 * 3. cEqual is usually '='
 * 4. cPropertySep is usually ';'
 *
 * This function can be called without calling any other functions in this lib.
 ******************************/
int iniPropertyValue( char *pszString, char *pszProperty, char *pszValue, char cEqual, char cPropertySep );

/******************************
 * iniAllTrim
 *
 * 1. trims blanks, tabs and newlines from start and end of pszString
 *
 * This function can be called without calling any other functions in this lib.
 ******************************/
int iniAllTrim( char *pszString );

/******************************
 * iniToUpper
 *
 * 1. Converts all chars in pszString to upper case.
 *
 * This function can be called without calling any other functions in this lib.
 ******************************/
int iniToUpper( char *pszString );


/******************************
 * _iniObjectRead
 *
 ******************************/
int _iniObjectRead( HINI hIni, char *szLine, char *pszObjectName );

/******************************
 * _iniPropertyRead
 *
 ******************************/
int _iniPropertyRead( HINI hIni, char *szLine, char *pszPropertyName, char *pszPropertyValue );

/******************************
 * _iniDump
 *
 ******************************/
int _iniDump( HINI hIni, FILE *hStream );

/******************************
 * _iniScanUntilObject
 *
 ******************************/
int _iniScanUntilObject( HINI hIni, FILE *hFile, char *pszLine );
int _iniScanUntilNextObject( HINI hIni, FILE *hFile, char *pszLine );

/*
 * Some changes to avoid a 255 file handle limit, thanks MQJoe.
 * Make it conditional as it does have some performance impact esp with LARGE ini files (like what I have :-)
 */

#if defined( HAVE_VSNPRINTF ) && defined( USE_LL_FIO )

FILE *uo_fopen( const char *filename, const char *mode );
int uo_fclose( FILE *stream );
char *uo_fgets( char *szbuffer, int n, FILE *stream );
int uo_fprintf( FILE *stream, const char *fmt, ...);
int uo_vfprintf( FILE *stream, const char *fmt, va_list ap);

#else

#define uo_fopen   fopen
#define uo_fclose  fclose
#define uo_fgets   fgets
#define uo_fprintf fprintf
#define uo_vfprintf vfprintf

#endif

#if defined(__cplusplus)
         }
#endif

#endif
