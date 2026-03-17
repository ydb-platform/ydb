/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   Derived from poco library from Boost Software: see nccpoco/SourceLicence file.
 *********************************************************************/

#ifndef NCPOCO_H
#define NCPOCO_H

/* Use NCP or nccp or ncpoco as our "namespace */

/* Forward */
typedef struct NCPSharedLib NCPSharedLib;
typedef enum NCP_Flags NCP_Flags;

enum NCP_Flags {
    NCP_GLOBAL = 1, /* (default) use RTLD_GLOBAL; ignored on platforms that do not use dlopen(). */
    NCP_LOCAL  = 2  /* use RTLD_LOCAL; RTTI will not work with this flag. */
};

struct NCPSharedLib {
    char* path;
    int flags;
    struct NCPstate {
        void* handle;
        int flags;
    } state;
    struct {
	char msg[4096];
    } err;
    struct NCPAPI {
	int (*init)(NCPSharedLib*);
	int (*reclaim)(NCPSharedLib*); /* reclaim resources; do not free lib */
	int (*load)(NCPSharedLib*,const char*,int);
	int (*unload)(NCPSharedLib*);
	int (*isloaded)(NCPSharedLib*);
	void* (*getsymbol)(NCPSharedLib*,const char*);
	const char* (*getpath)(NCPSharedLib*);
    } api;
};


/**************************************************/
/* SharedLib API */

EXTERNL int ncpsharedlibnew(NCPSharedLib**); /* Creates a NCPSharedLib object. */

EXTERNL int ncpsharedlibfree(NCPSharedLib*); /* free this shared library */

/*
Loads a shared library from the given path using specified flags.
Returns error + message if a library has already been loaded or cannot be loaded.
*/
EXTERNL int ncpload(NCPSharedLib*,const char* path, int flags);

EXTERNL int ncpunload(NCPSharedLib*); /* Unloads a shared library. */

EXTERNL int ncpisloaded(NCPSharedLib*); /* Returns 1 iff a library has been loaded.*/

/* Returns the address of the symbol with the given name.
   For functions, this is the entry point of the function.
   Return NULL if the symbol does not exist
*/
EXTERNL void* ncpgetsymbol(NCPSharedLib*,const char* name);

/* Returns the path of the library, as specified in
   a call to load(NCPSharedLib*)
*/
EXTERNL const char* ncpgetpath(NCPSharedLib*);

/* Return last err msg */
EXTERNL const char* ncpgeterrmsg(NCPSharedLib* lib);

/* Clear the last err msg. */
EXTERNL void ncpclearerrmsg(NCPSharedLib* lib);

EXTERNL const char* intstr(int err1);

#endif /*NCPOCO_H*/
