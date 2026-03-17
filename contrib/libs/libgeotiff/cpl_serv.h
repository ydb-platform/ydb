/******************************************************************************
 * Copyright (c) 1998, Frank Warmerdam
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************
 *
 * cpl_serv.h
 *
 * This include file derived and simplified from the GDAL Common Portability
 * Library.
 */

#ifndef CPL_SERV_H_INCLUDED
#define CPL_SERV_H_INCLUDED

/* ==================================================================== */
/*	Standard include files.						*/
/* ==================================================================== */

#include "geo_config.h"
#include <stdio.h>

#include <math.h>

#include <string.h>
#if defined(GEOTIFF_HAVE_STRINGS_H) && !defined(_WIN32)
#  include <strings.h>
#endif
#include <stdlib.h>

/**********************************************************************
 * Do we want to build as a DLL on windows?
 **********************************************************************/
#if !defined(GTIF_DLL)
#  if defined(_WIN32) && defined(BUILD_AS_DLL)
#    define GTIF_DLL     __declspec(dllexport)
#  else
#    define GTIF_DLL
#  endif
#endif

/* ==================================================================== */
/*      Other standard services.                                        */
/* ==================================================================== */
#ifdef __cplusplus
#  define CPL_C_START		extern "C" {
#  define CPL_C_END		}
#else
#  define CPL_C_START
#  define CPL_C_END
#endif

#ifndef NULL
#  define NULL	0
#endif

#ifndef FALSE
#  define FALSE	0
#endif

#ifndef TRUE
#  define TRUE	1
#endif

#ifndef MAX
#  define MIN(a,b)      (((a)<(b)) ? (a) : (b))
#  define MAX(a,b)      (((a)>(b)) ? (a) : (b))
#endif

#ifndef NULL
#define NULL 0
#endif

#ifndef ABS
#  define ABS(x)        (((x)<0) ? (-1*(x)) : (x))
#endif

#ifndef EQUAL
#if defined(_WIN32) && !defined(__CYGWIN__)
#   if     (_MSC_FULL_VER >= 15000000)
#       define EQUALN(a,b,n)           (_strnicmp(a,b,n)==0)
#       define EQUAL(a,b)              (_stricmp(a,b)==0)
#   else
#       define EQUALN(a,b,n)           (strnicmp(a,b,n)==0)
#       define EQUAL(a,b)              (stricmp(a,b)==0)
#   endif
#else
#  define EQUALN(a,b,n)           (strncasecmp(a,b,n)==0)
#  define EQUAL(a,b)              (strcasecmp(a,b)==0)
#endif
#endif

/* ==================================================================== */
/*      VSI Services (just map directly onto Standard C services.       */
/* ==================================================================== */

#define VSIFOpen	fopen
#define VSIFClose	fclose
#define VSIFEof		feof
#define VSIFPrintf	fprintf
#define VSIFPuts	fputs
#define VSIFPutc	fputc
#define VSIFGets	fgets
#define VSIRewind	rewind
#define VSIFSeek        fseek
#define VSIFTell        ftell
#define VSIFRead        fread

#ifndef notdef
#define VSICalloc(x,y)	_GTIFcalloc((x)*(y))
#define VSIMalloc	_GTIFcalloc
#define VSIFree	        _GTIFFree
#define VSIRealloc      _GTIFrealloc
#else
#define VSICalloc(x,y)	(((char *) _GTIFcalloc((x)*(y)+4)) + 4)
#define VSIMalloc(x)	(((char *) _GTIFcalloc((x)+4)) + 4)
#define VSIFree(x)      _GTIFFree(((char *) (x)) - 4)
#define VSIRealloc(p,n) (((char *) _GTIFrealloc(((char *)(p))-4,(n)+4)) + 4)
#endif


#if !defined(GTIFAtof)
#  define GTIFAtof atof
#endif


/* -------------------------------------------------------------------- */
/*      Safe malloc() API.  Thin cover over VSI functions with fatal    */
/*      error reporting if memory allocation fails.                     */
/* -------------------------------------------------------------------- */
CPL_C_START

#define CPLMalloc  gtCPLMalloc
#define CPLCalloc  gtCPLCalloc
#define CPLRealloc gtCPLRealloc
#define CPLStrdup  gtCPLStrdup

void GTIF_DLL *CPLMalloc( int );
void GTIF_DLL *CPLCalloc( int, int );
void GTIF_DLL *CPLRealloc( void *, int );
char GTIF_DLL *CPLStrdup( const char * );

#define CPLFree(x)	{ if( x != NULL ) VSIFree(x); }

/* -------------------------------------------------------------------- */
/*      Locale insensitive string to float conversion.                  */
/* -------------------------------------------------------------------- */
/*double GTIFAtof(const char *nptr); */
double GTIFStrtod(const char *nptr, char **endptr);

/* -------------------------------------------------------------------- */
/*      Read a line from a text file, and strip of CR/LF.               */
/* -------------------------------------------------------------------- */

#define CPLReadLine gtCPLReadLine

const char GTIF_DLL *CPLReadLine( FILE * );

/*=====================================================================
                   Error handling functions (cpl_error.c)
 =====================================================================*/

typedef enum
{
    CE_None = 0,
    CE_Log = 1,
    CE_Warning = 2,
    CE_Failure = 3,
    CE_Fatal = 4
} CPLErr;

#define CPLError      gtCPLError
#define CPLErrorReset gtCPLErrorReset
#define CPLGetLastErrorNo gtCPLGetLastErrorNo
#define CPLGetLastErrorMsg gtCPLGetLastErrorMsg
#define CPLSetErrorHandler gtCPLSetErrorHandler
#define _CPLAssert    gt_CPLAssert

void GTIF_DLL CPLError(CPLErr eErrClass, int err_no, const char *fmt, ...);
void GTIF_DLL CPLErrorReset();
int  GTIF_DLL CPLGetLastErrorNo();
const char GTIF_DLL * CPLGetLastErrorMsg();
void GTIF_DLL CPLSetErrorHandler(void(*pfnErrorHandler)(CPLErr,int,
                                                       const char *));
void GTIF_DLL _CPLAssert( const char *, const char *, int );

#ifdef DEBUG
#  define CPLAssert(expr)  ((expr) ? (void)(0) : _CPLAssert(#expr,__FILE__,__LINE__))
#else
#  define CPLAssert(expr)
#endif

CPL_C_END

/* ==================================================================== */
/*      Well known error codes.                                         */
/* ==================================================================== */

#define CPLE_AppDefined			1
#define CPLE_OutOfMemory		2
#define CPLE_FileIO			3
#define CPLE_OpenFailed			4
#define CPLE_IllegalArg			5
#define CPLE_NotSupported		6
#define CPLE_AssertionFailed		7
#define CPLE_NoWriteAccess		8

/*=====================================================================
                   Stringlist functions (strlist.c)
 =====================================================================*/
CPL_C_START

#define CSLAddString gtCSLAddString
#define CSLCount     gtCSLCount
#define CSLGetField  gtCSLGetField
#define CSLDestroy   gtCSLDestroy
#define CSLDuplicate gtCSLDuplicate
#define CSLTokenizeString gtCSLTokenizeString
#define CSLTokenizeStringComplex gtCSLTokenizeStringComplex

char GTIF_DLL   **CSLAddString(char **papszStrList, const char *pszNewString);
int  GTIF_DLL   CSLCount(char **papszStrList);
const char GTIF_DLL *CSLGetField( char **, int );
void GTIF_DLL   CSLDestroy(char **papszStrList);
char GTIF_DLL   **CSLDuplicate(char **papszStrList);

char GTIF_DLL   **CSLTokenizeString(const char *pszString );
char GTIF_DLL   **CSLTokenizeStringComplex(const char *pszString,
                                   const char *pszDelimiter,
                                   int bHonourStrings, int bAllowEmptyTokens );

/*
 * The following functions were used up to libgeotiff 1.4.X series, but
 * are now no-operation, since there is no longer any CSV use in libgeotiff.
 */
#define SetCSVFilenameHook gtSetCSVFilenameHook
void GTIF_DLL SetCSVFilenameHook( const char *(*CSVFileOverride)(const char *) );


CPL_C_END

#endif /* ndef CPL_SERV_H_INCLUDED */
