/*
 * Copyright 2018, University Corporation for Atmospheric Research
 * See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

/**************************************************/
/* Global state plugin path implementation */

/**
 * @file
 * Functions for working with plugins. 
 */

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _MSC_VER
#include <io.h>
#endif

#include "netcdf.h"
#include "netcdf_filter.h"
#include "ncdispatch.h"
#include "nc4internal.h"
#include "nclog.h"
#include "ncbytes.h"
#include "ncplugins.h"
#include "netcdf_aux.h"

/*
Unified plugin related code
*/
/**************************************************/
/* Plugin-path API */ 

/* list of environment variables to check for plugin roots */
#define PLUGIN_ENV "HDF5_PLUGIN_PATH"

/* Control path verification */
#define PLUGINPATHVERIFY "NC_PLUGIN_PATH_VERIFY"

/*Forward*/
static int buildinitialpluginpath(NCPluginList* dirs);

static int NC_plugin_path_initialized = 0;
static int NC_plugin_path_verify = 1;

/**
 * This function is called as part of nc_initialize.
 * Its purpose is to initialize the plugin paths state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

EXTERNL int
nc_plugin_path_initialize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NULL;
    NCPluginList dirs = {0,NULL};
#ifdef USE_HDF5
    int hdf5found = 0; /* 1 => we got a legit plugin path set from HDF5 */
#endif

    if(!NC_initialized) nc_initialize();
    if(NC_plugin_path_initialized != 0) goto done;
    NC_plugin_path_initialized = 1;

    if(getenv(PLUGINPATHVERIFY) != NULL) NC_plugin_path_verify = 1;

    gs = NC_getglobalstate();

   /**
    * When the netcdf-c library initializes itself (at runtime), it chooses an
    * initial global plugin path using the following rules, which are those used
    * by the HDF5 library, except as modified for plugin install (which HDF5 does not support).
    */

    /* Initialize the implementations */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if((stat = NCZ_plugin_path_initialize())) goto done;    
#endif
#ifdef USE_HDF5
    if((stat = NC4_hdf5_plugin_path_initialize())) goto done;
#endif

    /* Compute the initial global plugin path */
    assert(dirs.ndirs == 0 && dirs.dirs == NULL);
    if((stat = buildinitialpluginpath(&dirs))) goto done; /* Construct a default */

    /* Sync to the actual implementations */
#ifdef USE_HDF5
    if(!hdf5found)
	{if((stat = NC4_hdf5_plugin_path_set(&dirs))) goto done;}
#endif
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if((stat = NCZ_plugin_path_set(&dirs))) goto done;
#endif
    /* Set the global plugin dirs sequence */
    assert(gs->pluginpaths == NULL);
    gs->pluginpaths = nclistnew();
    if(dirs.ndirs > 0) {
	size_t i;
	char** dst;
        nclistsetlength(gs->pluginpaths,dirs.ndirs);
	dst = (char**)nclistcontents(gs->pluginpaths);
	assert(dst != NULL);
	for(i=0;i<dirs.ndirs;i++)
	    dst[i] = strdup(dirs.dirs[i]);
    }
done:
    ncaux_plugin_path_clear(&dirs);
    return NCTHROW(stat);
}

/**
 * This function is called as part of nc_finalize()
 * Its purpose is to clean-up plugin path state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

int
nc_plugin_path_finalize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(NC_plugin_path_initialized == 0) goto done;
    NC_plugin_path_initialized = 0;

    NC_plugin_path_verify = 0;

    /* Finalize the actual implementation */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if((stat = NCZ_plugin_path_finalize())) goto done;    
#endif
#ifdef USE_HDF5
    if((stat = NC4_hdf5_plugin_path_finalize())) goto done;
#endif

    nclistfreeall(gs->pluginpaths); gs->pluginpaths = NULL;
done:
    return NCTHROW(stat);
}

/**
 * Return the length of the current sequence of directories
 * in the internal global plugin path list.
 * @param ndirsp length is returned here
 * @return NC_NOERR | NC_EXXX
 *
 * @author Dennis Heimbigner
 */

int
nc_plugin_path_ndirs(size_t* ndirsp)
{
    int stat = NC_NOERR;
    size_t ndirs = 0;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(gs->pluginpaths == NULL) gs->pluginpaths = nclistnew(); /* suspenders and belt */
    ndirs = nclistlength(gs->pluginpaths);

    /* Verify that the implementation plugin paths are consistent in length*/
    if(NC_plugin_path_verify) {
#ifdef NETCDF_ENABLE_HDF5
	{
	    size_t ndirs5 = 0;
	    if((stat=NC4_hdf5_plugin_path_ndirs(&ndirs5))) goto done;
	    assert(ndirs5 == ndirs);
	}
#endif /*NETCDF_ENABLE_HDF5*/
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
	{
	    size_t ndirsz = 0;
	    if((stat=NCZ_plugin_path_ndirs(&ndirsz))) goto done;
	    assert(ndirsz == ndirs);
        }
#endif /*NETCDF_ENABLE_NCZARR_FILTERS*/
    }
    if(ndirsp) *ndirsp = ndirs;
done:
    return NCTHROW(stat);
}

/**
 * Return the current sequence of directories in the internal global
 * plugin path list. Since this function does not modify the plugin path,
 * it can be called at any time.
 * @param dirs pointer to an NCPluginList object
 * @return NC_NOERR | NC_EXXX
 * @author Dennis Heimbigner
 *
 * WARNING: if dirs->dirs is NULL, then space for the directory
 * vector will be allocated. If not NULL, then the specified space will
 * be overwritten with the vector.
 *
 * @author: Dennis Heimbigner
*/

int
nc_plugin_path_get(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();
    size_t i;

    if(gs->pluginpaths == NULL) gs->pluginpaths = nclistnew(); /* suspenders and belt */
    if(dirs == NULL) goto done;
    dirs->ndirs = nclistlength(gs->pluginpaths);
    if(dirs->ndirs > 0 && dirs->dirs == NULL) {
	if((dirs->dirs = (char**)calloc(dirs->ndirs,sizeof(char*)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }
    for(i=0;i<dirs->ndirs;i++) {
	const char* dir = nclistget(gs->pluginpaths,i);
	dirs->dirs[i] = nulldup(dir);
    }

    /* Verify that the implementation plugin paths are consistent */
    if(NC_plugin_path_verify) {
#ifdef NETCDF_ENABLE_HDF5
	{
	    size_t i;
	    NCPluginList l5 = {0,NULL};
	    if((stat=NC4_hdf5_plugin_path_get(&l5))) goto done;
	    assert(l5.ndirs == nclistlength(gs->pluginpaths));
	    for(i=0;i<l5.ndirs;i++) {
		assert(strcmp(dirs->dirs[i],l5.dirs[i])==0);
		nullfree(l5.dirs[i]);
	    }
	    nullfree(l5.dirs);
	}
#endif /*NETCDF_ENABLE_HDF5*/
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
	{
	    size_t i;
	    NCPluginList lz = {0,NULL};
	    if((stat=NCZ_plugin_path_get(&lz))) goto done;
	    assert(lz.ndirs == nclistlength(gs->pluginpaths));
	    for(i=0;i<lz.ndirs;i++) {
		assert(strcmp(dirs->dirs[i],lz.dirs[i])==0);
		nullfree(lz.dirs[i]);
	    }
	    nullfree(lz.dirs);
        }
#endif /*NETCDF_ENABLE_NCZARR_FILTERS*/
    }
done:
    return NCTHROW(stat);
}

/**
 * Empty the current internal path sequence
 * and replace with the sequence of directories argument.
 * Using a dirs->ndirs argument of 0 will clear the set of plugin dirs.
 *
 * @param dirs to overwrite the current internal dir list
 * @return NC_NOERR | NC_EXXX
 *
 * @author Dennis Heimbigner
*/
int
nc_plugin_path_set(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(dirs == NULL) {stat = NC_EINVAL; goto done;}

    /* Clear the current dir list */
    nclistfreeall(gs->pluginpaths);
    gs->pluginpaths = nclistnew();

    if(dirs->ndirs > 0) {
	size_t i;
        assert(gs->pluginpaths != NULL);
	for(i=0;i<dirs->ndirs;i++) {
	    nclistpush(gs->pluginpaths,nulldup(dirs->dirs[i]));
	}
    }

    /* Sync the global plugin path set to the individual implementations */
#ifdef NETCDF_ENABLE_HDF5
    if((stat = NC4_hdf5_plugin_path_set(dirs))) goto done;
#endif
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if((stat = NCZ_plugin_path_set(dirs))) goto done;
#endif

done:
    return NCTHROW(stat);
}

/**
 * When the netcdf-c library initializes itself (at runtime), it chooses an
 * initial global plugin path using the following rules, which are those used
 * by the HDF5 library, except as modified for plugin install (which HDF5 does not support).
 *
 * Note: In the following, PLATFORMPATH is:
 * -- /usr/local/lhdf5/lib/plugin if platform is *nix*
 * -- %ALLUSERSPROFILE%/hdf5/lib/plugin if platform is Windows or Mingw
 * and in the following, PLATFORMSEP is:
 * -- ":" if platform is *nix*
 * -- ";" if platform is Windows or Mingw
 * and
 *     NETCDF_PLUGIN_SEARCH_PATH is the value constructed at build-time (see configure.ac)
 *
 * Table showing the computation of the initial global plugin path
 * =================================================
 * | HDF5_PLUGIN_PATH | Initial global plugin path |
 * =================================================
 * | undefined        | NETCDF_PLUGIN_SEARCH_PATH  |
 * -------------------------------------------------
 * | <path1;...pathn> | <path1;...pathn>           |
 * -------------------------------------------------
 */
static int
buildinitialpluginpath(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    const char* hdf5path = NULL;
    /* Find the plugin directory root(s) */
    hdf5path = getenv(PLUGIN_ENV); /* HDF5_PLUGIN_PATH */
    if(hdf5path == NULL) {
	/* Use NETCDF_PLUGIN_SEARCH_PATH */
	hdf5path = NETCDF_PLUGIN_SEARCH_PATH;
    }
    if((stat = ncaux_plugin_path_parse(hdf5path,'\0',dirs))) goto done;

done:
    return stat;
}
