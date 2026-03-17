/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   Derived from poco library from Boost Software: see nccpoco/LICENSE file.
 *********************************************************************/

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_DLFCN_H
#include <dlfcn.h>
#endif
#ifdef USE_MUTEX
#include <pthread.h>
#endif

#include "netcdf.h"
#include "ncpoco.h"
#include "ncpathmgr.h"

#undef DEBUG

/* Note: cygwin is missing RTLD_LOCAL, set it to 0 */
#if !defined(RTLD_LOCAL)
#define RTLD_LOCAL 0
#endif

#if !defined(RTLD_GLOBAL)
#define RTLD_GLOBAL 0
#endif

#if !defined(RTLD_LAZY)
#define RTLD_LAZY 1
#endif

#if !defined(RTLD_NOW)
#define RTLD_NOW 2
#endif

#ifdef USE_MUTEX
static pthread_mutex_t mutex;
#endif

static void
ncperr(const char* fcn, NCPSharedLib* lib)
{
    const char* msg = dlerror();
    memset(lib->err.msg,0,sizeof(lib->err.msg));
    if(msg != NULL) {
	strlcat(lib->err.msg,fcn,sizeof(lib->err.msg));
	strlcat(lib->err.msg,": ",sizeof(lib->err.msg));
	strlcat(lib->err.msg,msg,sizeof(lib->err.msg));
#ifdef DEBUG
	fprintf(stderr,">>> %s\n",lib->err.msg);
#endif
    }
}

int
ncp_unix_initialize(void)
{
    int ret = 1;
#ifdef USE_MUTEX
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL);
    if(pthread_mutex_init(&mutex, &attr)) {
	fprintf(stderr,"cp_unix: Cannot create mutext\n");
	ret = 1;
    }
    pthread_mutexattr_destroy(&attr);
#endif
    return ret;
}

int
ncp_unix_finalize(void)
{
#ifdef USE_MUTEX
    pthread_mutex_destroy(&mutex);
#endif
    return 1;
}

/**************************************************/

#ifdef USE_MUTEX
static void lock(void) {pthread_mutex_lock(&mutex);}
#else
#define lock()
#endif

#ifdef USE_MUTEX
static void unlock(void) {pthread_mutex_unlock(&mutex);}
#else
#define unlock()
#endif

/**************************************************/

static int
init(NCPSharedLib* lib)
{
    int ret = NC_NOERR;
    return ret;
}

static int
reclaim(NCPSharedLib* lib)
{
    int ret = NC_NOERR;
    return ret;
}

static int
load(NCPSharedLib* lib , const char* path0, int flags)
{
    int ret = NC_NOERR;
    int realflags = RTLD_LAZY; /* versus RTLD_NOW which does not appear to work */
    char* path = NULL;

    if((path = NCpathcvt(path0))==NULL) {ret = NC_ENOMEM; goto done;}

    lock();
    if(lib->state.handle != NULL)
	{ret = NC_EEXIST; goto ldone;}
    lib->path = nulldup(path);
    lib->flags = flags;
    if(flags & NCP_LOCAL)
	realflags |= RTLD_LOCAL;
    else
	realflags |= RTLD_GLOBAL;
    lib->state.flags = realflags;
    lib->state.handle = dlopen(lib->path, lib->state.flags);
    if(lib->state.handle == NULL) {
	ncperr("dlopen",lib);
	ret = NC_ENOTFOUND;
	goto ldone;
    }
ldone:
    unlock();
done:
    nullfree(path);
    return ret;
}

static int
unload(NCPSharedLib* lib)
{
    int ret = NC_NOERR;
    lock();
    if(lib->state.handle != NULL) {
	dlclose(lib->state.handle);
	lib->state.handle = NULL;
    }
    unlock();
    return ret;
}

static int
isLoaded(NCPSharedLib* lib)
{
    return lib->state.handle != NULL;
}

static void*
getsymbol(NCPSharedLib* lib, const char* name)
{
    void* result = NULL;
    lock();
    if(lib->state.handle != NULL) {
	result = dlsym(lib->state.handle, name);
	if(result == NULL) {
	    ncperr("dlsym",lib);
	}
    }
    unlock();
    return result;
}

static const char*
getpath(NCPSharedLib* lib)
{
    return lib->path;
}

struct NCPAPI ncp_unix_api = {
	init,
	reclaim,
	load,
	unload,
	isLoaded,
	getsymbol,
	getpath,
};
