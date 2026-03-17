/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   Derived from poco library from Boost Software: see nccpoco/SourceLicense file.
 *********************************************************************/

#include "config.h"
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "netcdf.h"
#include "ncpoco.h"

/**************************************************/
#ifdef _WIN32
extern struct NCPAPI ncp_win32_api;
#else
extern struct NCPAPI ncp_unix_api;
#endif

/**************************************************/

/* Creates a SharedLib object. */
EXTERNL int
ncpsharedlibnew(NCPSharedLib** libp)
{
    int ret = NC_NOERR;
    NCPSharedLib* lib;
    lib = (NCPSharedLib*)calloc(1,sizeof(NCPSharedLib));
    if(lib == 0)
	{ret = NC_ENOMEM; goto done;}
    /* fill in the api */
#ifdef _WIN32
    lib->api = ncp_win32_api;
#else
    lib->api = ncp_unix_api;
#endif
    ret = lib->api.init(lib);
    if(ret == NC_NOERR && libp)
	*libp = lib;
	
done:
    return ret;
}

/* free this shared library */
EXTERNL int
ncpsharedlibfree(NCPSharedLib* lib)
{
    int ret = NC_NOERR;
    if(lib == NULL) return NC_NOERR;
    ret = lib->api.unload(lib);
    ret = lib->api.reclaim(lib);
    /* reclaim common stuff */
    nullfree(lib->path);
    free(lib);
    return ret;
}

/*
Loads a shared library from the given path using specified flags.
Returns error if a library has already been loaded or cannot be loaded.
*/
EXTERNL int
ncpload(NCPSharedLib* lib, const char* path, int flags)
{
    if(lib == NULL || path == NULL) return NC_EINVAL;
    ncpclearerrmsg(lib);
    return lib->api.load(lib,path,flags);
}

EXTERNL int
ncpunload(NCPSharedLib* lib) /* Unloads a shared library. */
{
    if(lib == NULL) return NC_EINVAL;
    ncpclearerrmsg(lib);
    return lib->api.unload(lib);
}

EXTERNL int
ncpisloaded(NCPSharedLib* lib) /* Returns 1 iff a library has been loaded. */
{
    if(lib == NULL) return 0;
    return lib->api.isloaded(lib);
}

/* Returns the address of the symbol with the given name.
   For functions, this is the entry point of the function.
   Return error if the symbol does not exist
*/
EXTERNL void*
ncpgetsymbol(NCPSharedLib* lib,const char* name)
{
    if(lib == NULL) return NULL;
    ncpclearerrmsg(lib);
    return lib->api.getsymbol(lib,name);
}

/* Returns the path of the library, as specified in
   a call to load()
*/
EXTERNL const char*
ncpgetpath(NCPSharedLib* lib)
{
    if(lib == NULL) return NULL;
    return lib->api.getpath(lib);
}

/* Returns the last err msg. */
EXTERNL const char*
ncpgeterrmsg(NCPSharedLib* lib)
{
    if(lib == NULL) return NULL;
    return (lib->err.msg[0] == '\0' ? NULL : lib->err.msg);
}

/* Clear the last err msg. */
EXTERNL void
ncpclearerrmsg(NCPSharedLib* lib)
{
    if(lib == NULL) return;
    memset(lib->err.msg,0,sizeof(lib->err.msg));
}
