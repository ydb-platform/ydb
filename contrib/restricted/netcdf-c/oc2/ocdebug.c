/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#include "config.h"
#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif
#include "ocinternal.h"
#include "ocdebug.h"

#ifdef OCCATCHERROR
/* Place breakpoint here to catch errors close to where they occur*/
OCerror
ocbreakpoint(OCerror err) {return err;}

OCerror
occatch(OCerror err)
{
    if(err == 0) return err;
    return ocbreakpoint(err);
}
#endif

int
xxdrerror(void)
{
    nclog(NCLOGERR,"xdr failure");
    return OCCATCH(OC_EDATADDS);
}


void*
occalloc(size_t size, size_t nelems)
{
    return ocmalloc(size*nelems);
}

void*
ocmalloc(size_t size)
{
    void* memory = calloc(size,1); /* use calloc to zero memory*/
    if(memory == NULL) nclog(NCLOGERR,"ocmalloc: out of memory");
    return memory;
}

void
ocfree(void* mem)
{
    if(mem != NULL) free(mem);
}

int
ocpanic(const char* fmt, ...)
{
    va_list args;
    if(fmt != NULL) {
      va_start(args, fmt);
      vfprintf(stderr, fmt, args);
      fprintf(stderr, "\n" );
      va_end( args );
    } else {
      fprintf(stderr, "panic" );
    }
    fprintf(stderr, "\n" );
    fflush(stderr);
    return 0;
}

CURLcode
ocreportcurlerror(OCstate* state, CURLcode cstat)
{
    if(cstat != CURLE_OK) {
        fprintf(stderr,"CURL Error: %s",curl_easy_strerror(cstat));
	if(state != NULL)
            fprintf(stderr," ; %s",state->error.curlerrorbuf);
        fprintf(stderr,"\n");
    }
    fflush(stderr);
    return cstat;
}
