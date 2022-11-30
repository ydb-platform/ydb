/*
 *  osLayer.h
 *  Release $Name: MATRIXSSL_1_8_7_OPEN $
 *
 *  Layered header for OS specific functions
 *  Contributors adding new OS support must implement all functions
 *  externed below.
 */
/*
 *  Copyright (c) PeerSec Networks, 2002-2009. All Rights Reserved.
 *  The latest version of this code is available at http://www.matrixssl.org
 *
 *  This software is open source; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This General Public License does NOT permit incorporating this software
 *  into proprietary programs.  If you are unable to comply with the GPL, a
 *  commercial license for this software may be purchased from PeerSec Networks
 *  at http://www.peersec.com
 *
 *  This program is distributed in WITHOUT ANY WARRANTY; without even the
 *  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *  http://www.gnu.org/copyleft/gpl.html
 */
/******************************************************************************/

#ifndef _h_OS_LAYER
#define _h_OS_LAYER
#define _h_EXPORT_SYMBOLS

#include <stdio.h>
#include <stdlib.h>
#include <util/system/platform.h>

#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************/

#include "../../matrixCommon.h"
#include "psMalloc.h"

/*
    Functions defined at OS level
*/
extern int32    sslOpenOsdep(void);
extern int32    sslCloseOsdep(void);
extern int32    sslGetEntropy(unsigned char *bytes, int32 size);

/*
    Defines to make library multithreading safe
*/
typedef void * sslMutex_t;
extern int32    sslCreateMutex(sslMutex_t *mutex);
extern int32    sslLockMutex(sslMutex_t *mutex);
extern int32    sslUnlockMutex(sslMutex_t *mutex);
extern void sslDestroyMutex(sslMutex_t *mutex);


typedef ui64 sslTime_t;
extern int32    sslInitMsecs(sslTime_t *t);
extern int32    sslCompareTime(sslTime_t a, sslTime_t b);
extern int32    sslDiffSecs(sslTime_t then, sslTime_t now);
extern long     sslDiffMsecs(sslTime_t then, sslTime_t now);


/******************************************************************************/
/*
    Debugging functionality.

    If MSSLDEBUG is defined matrixStrDebugMsg and matrixIntDebugMsg messages are
    output to stdout, sslAsserts go to stderror and call psBreak.

    In non-MSSLDEBUG builds matrixStrDebugMsg and matrixIntDebugMsg are
    compiled out.  sslAsserts still go to stderr, but psBreak is not called.

*/

#ifdef MSSLDEBUG
extern void psBreak(void);
extern void matrixStrDebugMsg(char *message, char *arg);
extern void matrixIntDebugMsg(char *message, int32 arg);
extern void matrixPtrDebugMsg(char *message, void *arg);
#define sslAssert(C)  if (C) ; else \
    {fprintf(stderr, "%s:%d sslAssert(%s)\n",__FILE__, __LINE__, #C); psBreak(); }
#else
#define matrixStrDebugMsg(x, y)
#define matrixIntDebugMsg(x, y)
#define matrixPtrDebugMsg(x, y)
#define sslAssert(C)  if (C) ; else \
    {fprintf(stderr, "%s:%d sslAssert(%s)\n",__FILE__, __LINE__, #C); }
#endif /* MSSLDEBUG */

#ifdef __cplusplus
}
#endif

#endif /* _h_OS_LAYER */

/******************************************************************************/
