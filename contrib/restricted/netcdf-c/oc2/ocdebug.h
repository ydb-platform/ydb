/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef OCOCDBG_H
#define OCOCDBG_H

#ifndef OCDEBUG
#undef OCDEBUG
#endif

#include "config.h"

#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif

#include <curl/curl.h>

#include "oc.h"
#include "ocinternal.h"


/* OCCATCHERROR is used to detect errors as close
   to their point of origin as possible. When
   enabled, one can set a breakpoint in ocbreakpoint()
   to catch the failure. Turing it on incurs a significant
   performance penalty, so it is off by default.*/

#undef OCCATCHERROR

#define OCPANIC(msg) assert(ocpanic(msg))
#define OCPANIC1(msg,arg) assert(ocpanic(msg,arg))
#define OCPANIC2(msg,arg1,arg2) assert(ocpanic(msg,arg1,arg2))

/* Make it possible to catch assertion failures by breakpointing ocpanic*/
#define OCASSERT(expr) if(!(expr)) {OCPANIC((#expr));} else {}

/* Need some syntactic trickery to make these macros work*/
#ifdef OCDEBUG
#define OCDBG(msg) {oclog(OCLOGDBG,msg);}
#define OCDBG1(msg,arg) {oclog(OCLOGDBG,msg,arg);}
#define OCDBG2(msg,arg1,arg2) {oclog(OCLOGDBG,msg,arg1,arg2);}
#define OCDBGTEXT(text) {oclogtext(OCLOGNOTE,text);} else {}
#define OCDBGCODE(code) {code;}

#else
#define OCDBG(msg)
#define OCDBG1(msg,arg)
#define OCDBG2(msg,arg1,arg2)
#define OCDBGTEXT(text)
#define OCDBGCODE(code)
#endif


/*
OCPROGRESS attempts to provide some info
about how IO is getting along.
*/
#undef OCPROGRESS

/*extern char* dent2(int n);*/
/*/extern char* dent(int n);*/
extern int ocpanic(const char* fmt, ...);

extern int xdrerror(void);

/*
Provide wrapped versions of calloc and malloc.
The wrapped version panics if memory
is exhausted.  It also guarantees that the
memory has been zero'd.
*/

extern void* occalloc(size_t size, size_t nelems);
extern void* ocmalloc(size_t size);
extern void  ocfree(void*);

#define MEMCHECK(var,throw) {if((var)==NULL) return (throw);}
#define MEMFAIL(var) MEMCHECK(var,OCCATCH(OC_ENOMEM))
#define MEMGOTO(var,stat,label) {if((var)==NULL) {stat=OC_ENOMEM;goto label;}}

#ifdef OCCATCHERROR
extern OCerror ocbreakpoint(OCerror err);
extern OCerror occatch(OCerror err);
extern CURLcode ocreportcurlerror(struct OCstate* state, CURLcode cstat);
/* Place breakpoint on ocbreakpoint to catch errors close to where they occur*/
/* Warning: do not evaluate e more than once */
#define OCCATCH(e) occatch(e)
#define OCCATCHCHK(e) (void)occatch(e)
#define OCGOTO(label) {ocbreakpoint(-1); goto label;}
#define OCCURLERR(s,e) ocreportcurlerror(s,e)
#define CURLERR(e) ocreportcurlerror(NULL,e)
#else
#define OCCATCH(e) (e)
#define OCCATCHCHK(e)
#define OCGOTO(label) goto label
#define OCCURLERR(s,e) (e)
#define CURLERR(e) (e)
#endif
#define OCTHROW(e) OCCATCH(e)
#define OCTHROWCHK(e) OCCATCHCHK(e)

#endif /*OCOCDBG_H*/
