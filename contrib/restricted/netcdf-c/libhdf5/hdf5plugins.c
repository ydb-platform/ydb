/* Copyright 2003-2018, University Corporation for Atmospheric
 * Research. See the COPYRIGHT file for copying and redistribution
 * conditions.
 */
/**
 * @file @internal netcdf-4 functions for the plugin list.
 *
 * @author Dennis Heimbigner
 */

#include "config.h"
#include <stddef.h>
#include <stdlib.h>
#include "netcdf.h"
#include "ncbytes.h"
#include "hdf5internal.h"
#include "hdf5debug.h"
#include "ncplugins.h"

#undef TPLUGINS

/**************************************************/
/**
 * @file
 * @internal
 * Internal netcdf hdf5 plugin path functions.
 *
 * @author Dennis Heimbigner
 */
/**************************************************/

/**
 * Return the length of the current sequence of directories
 * in the internal global plugin path list.
 * @param ndirsp length is returned here
 * @return NC_NOERR | NC_EXXX
 *
 * @author Dennis Heimbigner
 */
int
NC4_hdf5_plugin_path_ndirs(size_t* ndirsp)
{
    int stat = NC_NOERR;
    size_t ndirs = 0;
    unsigned undirs = 0;  
    herr_t hstat = 0;

    /* Get the length of the HDF5 plugin path set */
    if((hstat = H5PLsize(&undirs))<0) goto done;
    ndirs = (size_t)undirs;
    if(ndirsp) *ndirsp = ndirs;
done:
    return THROW(stat);
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
NC4_hdf5_plugin_path_get(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    unsigned i;
    herr_t hstat = 0;
    unsigned undirs = 0;  
    ssize_t dirlen;
    char* dirbuf = NULL;

    if(dirs == NULL) {stat = NC_EINVAL; goto done;}

    /* Get the length of the HDF5 plugin path set */
    if((hstat = H5PLsize(&undirs))<0) goto done;
    dirs->ndirs = (size_t)undirs;

    /* Copy out the paths from the HDF5 library */
    /* Watch out for nul term handling WRT dir string length */
    if(dirs->dirs == NULL) {
	if((dirs->dirs=(char**)calloc(dirs->ndirs,sizeof(char*)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }
    for(i=0;i<undirs;i++) {
	if((dirlen = H5PLget(i, NULL, 0))<0) {stat = NC_EHDFERR; goto done;}
	nullfree(dirbuf); dirbuf = NULL; /* suspenders and belt */
	if((dirbuf = (char*)malloc((size_t)dirlen+1))==NULL) {stat = NC_ENOMEM; goto done;} /* dirlen does not include nul term */
	if((dirlen = H5PLget(i, dirbuf, ((size_t)dirlen)+1))<0) {stat = NC_EHDFERR; goto done;}
	dirs->dirs[i] = dirbuf; dirbuf = NULL;	    
    }
    
done:
    if(hstat < 0 && stat != NC_NOERR) stat = NC_EHDFERR;
    return THROW(stat);
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
NC4_hdf5_plugin_path_set(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    size_t i;
    herr_t hstat = 0;
    unsigned undirs = 0;  

    /* validate */
    if(dirs == NULL || (dirs->ndirs > 0 && dirs->dirs == NULL))
	{stat = NC_EINVAL; goto done;}

    /* Clear the current path list */
    if((hstat = H5PLsize(&undirs))<0) goto done;
    if(undirs > 0) {
	for(i=0;i<undirs;i++) {
	    /* Always remove the first element to avoid index confusion */
	    if((hstat = H5PLremove(0))<0) {stat = NC_EINVAL; goto done;}
	}
    }

    /* Insert the new path list */
    for(i=0;i<dirs->ndirs;i++) {
	/* Always append */
	if((hstat = H5PLappend(dirs->dirs[i]))<0)
	    {stat = NC_EINVAL; goto done;}
    }

done:
    if(hstat < 0 && stat != NC_NOERR) stat = NC_EHDFERR;
    return stat;
}

int
NC4_hdf5_plugin_path_initialize(void)
{
    return NC_NOERR;
}

int
NC4_hdf5_plugin_path_finalize(void)
{
    return NC_NOERR;
}

/**************************************************/
/* Debug printer for HDF5 plugin paths */

static NCbytes* ncppbuf;
const char*
NC4_hdf5_plugin_path_tostring(void)
{
    int stat = NC_NOERR;
    herr_t hstat = 0;
    unsigned npaths = 0;
    char* dir = NULL;

    if(ncppbuf == NULL) ncppbuf = ncbytesnew();
    ncbytesclear(ncppbuf);

    if((hstat = H5PLsize(&npaths))<0) {stat = NC_EINVAL; goto done;}
    if(npaths > 0) {
	ssize_t dirlen = 0;
	unsigned i;
	for(i=0;i<npaths;i++) {
	    dirlen = H5PLget(i,NULL,0);
	    if(dirlen < 0) {stat = NC_EINVAL; goto done;}
	    if((dir = (char*)malloc(1+(size_t)dirlen))==NULL)
		{stat = NC_ENOMEM; goto done;}
	    /* returned dirlen does not include the nul terminator, but the length argument must include it */
	    dirlen = H5PLget(i,dir,(size_t)(dirlen+1));
	    dir[dirlen] = '\0';
	    if(i > 0) ncbytescat(ncppbuf,";");
	    ncbytescat(ncppbuf,dir);
	    nullfree(dir); dir = NULL;
	}
    }

done:
    nullfree(dir);
    if(stat != NC_NOERR) ncbytesclear(ncppbuf);
    ncbytesnull(ncppbuf);
    return ncbytescontents(ncppbuf);
}

/**************************************************/
#ifdef TPLUGINS

static void
printplugin1(struct NC_HDF5_Plugin* nfs)
{
    int i;
    if(nfs == NULL) {
	fprintf(stderr,"{null}");
	return;
    }
    fprintf(stderr,"{%u,(%u)",nfs->pluginid,(int)nfs->nparams);
    for(i=0;i<nfs->nparams;i++) {
      fprintf(stderr," %s",nfs->params[i]);
    }
    fprintf(stderr,"}");
}

static void
printplugin(struct NC_HDF5_Plugin* nfs, const char* tag, int line)
{
    fprintf(stderr,"%s: line=%d: ",tag,line);
    printplugin1(nfs);
    fprintf(stderr,"\n");
}

static void
printpluginlist(NC_VAR_INFO_T* var, const char* tag, int line)
{
    int i;
    const char* name;
    if(var == NULL) name = "null";
    else if(var->hdr.name == NULL) name = "?";
    else name = var->hdr.name;
    fprintf(stderr,"%s: line=%d: var=%s plugins=",tag,line,name);
    if(var != NULL) {
	NClist* plugins = (NClist*)var->plugins;
        for(i=0;i<nclistlength(plugins);i++) {
	    struct NC_HDF5_Plugin* nfs = (struct NC_HDF5_Plugin*)nclistget(plugins,i);
	    fprintf(stderr,"[%d]",i);
	    printplugin1(nfs);
	}
    }
    fprintf(stderr,"\n");
}
#endif /*TPLUGINS*/
