/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   Derived from poco library from Boost Software: see nccpoco/LICENSE file.
 *********************************************************************/

#ifdef _WIN32

#include "config.h"
#include <windows.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <windows.h>
#include <errhandlingapi.h>

#include "netcdf.h"
#include "ncpoco.h"
#include "ncpathmgr.h"

#ifdef USE_MUTEX
static CRITICAL_SECTION mutex;
#endif

static const char* driveletters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/* Forward */
static int isAbsolutePath(const char* path);

/**************************************************/

int
ncp_win32_initialize()
{
    int ret = 1;
#ifdef USE_MUTEX
    InitializeCriticalSectionAndSpinCount(&mutex, 4000);
#endif
    (void)SetErrorMode(SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX);
    return ret;
}

int
ncp_win32_finalize()
{
#ifdef USE_MUTEX
    DeleteCriticalSection(&mutex);
#endif
    return 1;
}

/**************************************************/


#ifdef USE_MUTEX
static void lock(void) {EnterCriticalSection(&mutex);}
#else
#define lock()
#endif

#ifdef USE_MUTEX
static void unlock(void) {LeaveCriticalSection(&mutex);}
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
    DWORD realflags = 0;
    char* path = NULL;

    path = NCpathcvt(path0);
    if(path == NULL) {ret = NC_EINVAL; goto done;}

    lock();
    if(lib->state.handle != NULL)
	{ret = NC_EEXIST; goto ldone;}
    lib->path = nulldup(path);
    lib->flags = flags;
    if(isAbsolutePath(path)) realflags |= LOAD_WITH_ALTERED_SEARCH_PATH;
    lib->state.flags = realflags;    
    lib->state.handle = LoadLibraryExA(path, 0, realflags);
    if(lib->state.handle == NULL) {
	int errcode = GetLastError();
	char* msg = NULL;
        FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                          NULL, errcode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (char*)&msg, 0, NULL);
	memset(lib->err.msg,0,sizeof(lib->err.msg));
	if(msg)
	    strncpy(lib->err.msg,msg,sizeof(lib->err.msg));
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
	FreeLibrary((HMODULE)lib->state.handle);
	lib->state.handle = NULL;
	goto done;
    }
done:
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
	result = (void*)GetProcAddress((HMODULE)lib->state.handle,name);
    }
    unlock();
    return result;
}

static const char*
getpath(NCPSharedLib* lib)
{
    return lib->path;
}

static int
isAbsolutePath(const char* path)
{
    if(path == NULL || path[0] == '\0')
	return 0;
    if(strchr(driveletters,path[0]) != NULL
       && path[1] == ':'
       && (path[2] == '/' || path[2] == '\\'))
        return 1;
    if(path[0] == '/' || path[2] == '\\')
	return 1;
    return 0;
}

struct NCPAPI ncp_win32_api = {
	init,
	reclaim,
	load,
	unload,
	isLoaded,
	getsymbol,
	getpath,
};

#endif /*_WIN32*/
