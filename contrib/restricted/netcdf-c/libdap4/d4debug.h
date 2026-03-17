/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef D4DEBUG_H
#define D4DEBUG_H

#include <assert.h>
#include <stdarg.h>

#undef D4CATCH /* Warning: significant performance impact */

#undef D4DEBUG /* general debug */
#undef D4DEBUGPARSER
#undef D4DEBUGMETA
#undef D4DUMPDAP
#undef D4DUMPRAW
#undef D4DEBUGDATA
#undef D4DUMPDMR
#undef D4DUMPCSUM

#ifdef D4DEBUG
#define D4DEBUGPARSER
#define D4DEBUGMETA
#define D4DEBUGDATA
#define D4DUMPCSUM
#define D4DUMPDMR
#define D4DUMPDAP
#endif

#define PANIC(msg) assert(d4panic(msg));
#define PANIC1(msg,arg) assert(d4panic(msg,arg));
#define PANIC2(msg,arg1,arg2) assert(d4panic(msg,arg1,arg2));

#define ASSERT(expr) if(!(expr)) {PANIC(#expr);} else {}

extern int ncd4debug;

extern int d4panic(const char* fmt, ...);

#define MEMCHECK(var) {if((var)==NULL) return (NC_ENOMEM);}

#ifdef D4CATCH
/* Place breakpoint on dapbreakpoint to catch errors close to where they occur*/
/* WARNING: do not evaluate e more than once */
#define THROW(e) d4throw(e)
#define THROWCHK(e) (void)d4throw(e)
extern int d4breakpoint(int err);
extern int d4throw(int err);
#else
#define THROW(e) (e)
#define THROWCHK(e)
#endif

#ifdef D4DEBUG
#define SHOWFETCH (1)
#else
#define SHOWFETCH FLAGSET(nccomm->controls,NCF_SHOWFETCH)
#endif

#define LOG0(level,msg) nclog(level,msg)
#define LOG1(level,msg,a1) nclog(level,msg,a1)
#define LOG2(level,msg,a1,a2) nclog(level,msg,a1,a2)

extern const char* NCD4_sortname(NCD4sort sort);
extern const char* NCD4_subsortname(nc_type subsort);

extern int NCD4_debugcopy(NCD4INFO* info);

#endif /*D4DEBUG_H*/
