/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Header$
 *********************************************************************/

#ifndef NCLOG_H
#define NCLOG_H

#include <stdarg.h>
#include <stdio.h>
#include "ncexternl.h"

#undef NCCATCH

#define NCENVLOGGING "NCLOGGING"
#define NCENVTRACING "NCTRACING"

/* Log level: linear order */
/* Suggested tag values */
#define NCLOGOFF   (0)	/* Stop Logging */
#define NCLOGERR   (1)	/* Errors */
#define NCLOGWARN  (2)	/* Warnings */
#define NCLOGNOTE  (3)	/* General info */
#define NCLOGDEBUG (4)	/* Everything */

/* Support ptr valued arguments that are used to store results */
#define PTRVAL(t,p,d) ((t)((p) == NULL ? (t)(d) : (t)*(p)))

#if defined(_CPLUSPLUS_) || defined(__CPLUSPLUS__)
extern "C" {
#endif

EXTERNL void ncloginit(void);
EXTERNL int ncsetloglevel(int level);
EXTERNL int nclogopen(FILE* stream);

/* The tag value is an arbitrary integer */
EXTERNL void nclog(int tag, const char* fmt, ...);
EXTERNL void ncvlog(int tag, const char* fmt, va_list ap);
EXTERNL void nclogtext(int tag, const char* text);
EXTERNL void nclogtextn(int tag, const char* text, size_t count);

/* Trace support */
EXTERNL int nctracelevel(int level);
EXTERNL void nctrace(int level, const char* fcn, const char* fmt, ...);
EXTERNL void nctracemore(int level, const char* fmt, ...);
EXTERNL void ncvtrace(int level, const char* fcn, const char* fmt, va_list ap);
EXTERNL int ncuntrace(const char* fcn, int err,const char* fmt,...);
EXTERNL int ncthrow(int err,const char* file,int line);
EXTERNL int ncbreakpoint(int err);

/* Debug support */
#if defined(NCCATCH)
/* Warning: do not evaluate e more than once */
#define NCTHROW(e) ncthrow(e,__FILE__,__LINE__)
#else
#define NCTHROW(e) (e)
#endif

#ifdef HAVE_EXECINFO_H
EXTERNL void ncbacktrace(void);
#else
#define ncbacktrace()
#endif

#if defined(_CPLUSPLUS_) || defined(__CPLUSPLUS__)
}
#endif

#endif /*NCLOG_H*/
