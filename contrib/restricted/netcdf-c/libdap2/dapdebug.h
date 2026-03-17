/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef DEBUG_H
#define DEBUG_H

/* Warning: setting CATCHERROR has significant performance impact */
#undef CATCHERROR

#if 0
#define DAPDEBUG 2
#define OCDEBUG 1
#endif

#include "ocdebug.h"

#ifdef DAPDEBUG
#  define DEBUG
#  if DAPDEBUG > 0
#    define DEBUG1
#  endif
#  if DAPDEBUG > 1
#    define DEBUG2
#  endif
#  if DAPDEBUG > 2
#    define DEBUG3
#  endif
#endif

/* Dump info about constraint processing */
#undef DCEVERBOSE

#undef PARSEDEBUG

#include <stdarg.h>
#include <assert.h>

#ifdef DAPDEBUG
#undef CATCHERROR
#define CATCHERROR
#endif

#define PANIC(msg) assert(dappanic(msg));
#define PANIC1(msg,arg) assert(dappanic(msg,arg));
#define PANIC2(msg,arg1,arg2) assert(dappanic(msg,arg1,arg2));

#define ASSERT(expr) if(!(expr)) {PANIC(#expr);} else {}

extern int ncdap3debug;

extern int dappanic(const char* fmt, ...);

#define MEMCHECK(var,throw) {if((var)==NULL) return (throw);}

#ifdef CATCHERROR
/* Place breakpoint on dapbreakpoint to catch errors close to where they occur*/
/* Warning: do not evaluate more than once */
#define THROW(e) dapthrow(e,__LINE__,__FILE__)
#define THROWCHK(e) (void)dapthrow(e,__LINE__,__FILE__)

extern int dapbreakpoint(int err);
extern int dapthrow(int err, int lineno, const char* filename);
#else
#define THROW(e) (e)
#define THROWCHK(e)
#endif

#ifdef DEBUG
#define SHOWFETCH (1)
#else
#define SHOWFETCH FLAGSET(nccomm->controls,NCF_SHOWFETCH)
#endif

#define LOG0(level,msg) nclog(level,msg)
#define LOG1(level,msg,a1) nclog(level,msg,a1)
#define LOG2(level,msg,a1,a2) nclog(level,msg,a1,a2)

#endif /*DEBUG_H*/
