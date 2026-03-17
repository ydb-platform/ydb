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
#include "zincludes.h"
#include "ncpathmgr.h"
#include "ncpoco.h"
#include "netcdf_filter.h"
#include "netcdf_filter_build.h"
#include "zfilter.h"
#include "ncplugins.h"
#include "zplugins.h"
#ifdef _WIN32
#include <windows.h>
#endif

/**************************************************/
/* Forward */
static int NCZ_load_plugin(const char* path, NCZ_Plugin** plugp);
static int NCZ_unload_plugin(NCZ_Plugin* plugin);
static int NCZ_load_plugin_dir(const char* path);
static int NCZ_plugin_save(size_t filterid, NCZ_Plugin* p);
static int getentries(const char* path, NClist* contents);
static int loadcodecdefaults(const char* path, const NCZ_codec_t** cp, NCPSharedLib* lib, int* lib_usedp);

#if defined(NAMEOPT) || defined(_WIN32)
static int pluginnamecheck(const char* name);
#endif

/**************************************************/
/**
 * @file
 * @internal
 * Internal netcdf zarr plugin path functions.
 *
 * @author Dennis Heimbigner
 */

/**
 * This function is called as part of nc_plugin_path_initialize.
 * Its purpose is to initialize the plugin state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/

int
NCZ_plugin_path_initialize(void)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    gs->zarr.pluginpaths = nclistnew();
    gs->zarr.default_libs = nclistnew();
    gs->zarr.codec_defaults = nclistnew();
    gs->zarr.loaded_plugins = (struct NCZ_Plugin**)calloc(H5Z_FILTER_MAX+1,sizeof(struct NCZ_Plugin*));
    if(gs->zarr.loaded_plugins == NULL) {stat = NC_ENOMEM; goto done;}

done:
    return stat;
}

/**
 * This function is called as part of nc_plugin_path_finalize.
 * Its purpose is to clean-up the plugin state.
 *
 * @return NC_NOERR
 *
 * @author Dennis Heimbigner
*/
int
NCZ_plugin_path_finalize(void)
{
    int stat = NC_NOERR;
    size_t i;
    struct NCglobalstate* gs = NC_getglobalstate();

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Reclaim all loaded filters */
    for(i=1;i<=gs->zarr.loaded_plugins_max;i++) {
	if(gs->zarr.loaded_plugins[i]) {
            NCZ_unload_plugin(gs->zarr.loaded_plugins[i]);
	    gs->zarr.loaded_plugins[i] = NULL;
	}
    }
    /* Reclaim the codec defaults */
    if(nclistlength(gs->zarr.codec_defaults) > 0) {
        for(i=0;i<nclistlength(gs->zarr.codec_defaults);i++) {
	    struct CodecAPI* ca = (struct CodecAPI*)nclistget(gs->zarr.codec_defaults,i);
    	    nullfree(ca);
	}
    }
    /* Reclaim the defaults library contents; Must occur as last act */
    if(nclistlength(gs->zarr.default_libs) > 0) {
        for(i=0;i<nclistlength(gs->zarr.default_libs);i++) {
	    NCPSharedLib* l = (NCPSharedLib*)nclistget(gs->zarr.default_libs,i);
    	    if(l != NULL) (void)ncpsharedlibfree(l);
	}
    }
#endif
    gs->zarr.loaded_plugins_max = 0;
    nullfree(gs->zarr.loaded_plugins); gs->zarr.loaded_plugins = NULL;
    nclistfree(gs->zarr.default_libs); gs->zarr.default_libs = NULL;
    nclistfree(gs->zarr.codec_defaults); gs->zarr.codec_defaults = NULL;
    nclistfreeall(gs->zarr.pluginpaths); gs->zarr.pluginpaths = NULL;
    return THROW(stat);
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
NCZ_plugin_path_ndirs(size_t* ndirsp)
{
    int stat = NC_NOERR;
    size_t ndirs = 0;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(gs->zarr.pluginpaths == NULL) gs->zarr.pluginpaths = nclistnew(); /* suspenders and belt */

    ndirs = nclistlength(gs->zarr.pluginpaths);
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
NCZ_plugin_path_get(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(dirs == NULL) {stat = NC_EINVAL; goto done;}

    if(gs->zarr.pluginpaths == NULL) gs->zarr.pluginpaths = nclistnew(); /* suspenders and belt */

    dirs->ndirs = nclistlength(gs->zarr.pluginpaths);
    if(dirs->dirs == NULL && dirs->ndirs > 0) {
	if((dirs->dirs = (char**)calloc(dirs->ndirs,sizeof(char*)))==NULL)
	    {stat = NC_ENOMEM; goto done;}
    }
    if(dirs->ndirs > 0) {
	size_t i;
	for(i=0;i<dirs->ndirs;i++) {
	    const char* dir = (const char*)nclistget(gs->zarr.pluginpaths,i);
	    dirs->dirs[i] = nulldup(dir);
	}
    }
done:
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
NCZ_plugin_path_set(NCPluginList* dirs)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    if(dirs == NULL) {stat = NC_EINVAL; goto done;}
    if(dirs->ndirs > 0 && dirs->dirs == NULL) {stat = NC_EINVAL; goto done;}

    /* Clear the current dir list */
    nclistfreeall(gs->zarr.pluginpaths);
    gs->zarr.pluginpaths = nclistnew();

    if(dirs->ndirs > 0) {
	size_t i;
	for(i=0;i<dirs->ndirs;i++) {
	    nclistpush(gs->zarr.pluginpaths,nulldup(dirs->dirs[i]));
	}
    }
done:
    return THROW(stat);
}

/**************************************************/
/* Filter<->Plugin interface */

int
NCZ_load_all_plugins(void)
{
    int ret = NC_NOERR;
    size_t i,j;
    struct NCglobalstate* gs = NC_getglobalstate();
    #ifdef _WIN64
        struct _stat64 buf;
    #else
        struct stat buf;
    #endif
    NClist* dirs = nclistnew();
    char* defaultpluginpath = NULL;

   ZTRACE(6,"");

   for(i=0;i<nclistlength(gs->pluginpaths);i++) {
	const char* dir = (const char*)nclistget(gs->pluginpaths,i);
        /* Make sure the root is actually a directory */
        errno = 0;
        ret = NCstat(dir, &buf);
#if 1
        ZTRACEMORE(6,"stat: ret=%d, errno=%d st_mode=%d",ret,errno,buf.st_mode);
#endif
        if(ret < 0) {errno = 0; ret = NC_NOERR; continue;} /* ignore unreadable directories */
	if(! S_ISDIR(buf.st_mode))
            ret = NC_EINVAL;
        if(ret) goto done;

        /* Try to load plugins from this directory */
        if((ret = NCZ_load_plugin_dir(dir))) goto done;
    }
    if(nclistlength(gs->zarr.codec_defaults)) { /* Try to provide default for any HDF5 filters without matching Codec. */
        /* Search the defaults */
	for(j=0;j<nclistlength(gs->zarr.codec_defaults);j++) {
            struct CodecAPI* dfalt = (struct CodecAPI*)nclistget(gs->zarr.codec_defaults,j);
	    if(dfalt->codec != NULL) {
	        const NCZ_codec_t* codec = dfalt->codec;
	        size_t hdf5id = codec->hdf5id;
		NCZ_Plugin* p = NULL;
		if(hdf5id <= 0 || hdf5id > gs->zarr.loaded_plugins_max) {ret = NC_EFILTER; goto done;}
	        p = gs->zarr.loaded_plugins[hdf5id]; /* get candidate */
	        if(p != NULL && p->hdf5.filter != NULL
                   && p->codec.codec == NULL) {
		    p->codec.codec = codec;
		    p->codec.codeclib = dfalt->codeclib;
		    p->codec.defaulted = 1;
		}
	    }
	}
    }

    /* Mark all plugins for which we do not have both HDF5 and codec */
    {
        size_t i;
	NCZ_Plugin* p;
	for(i=1;i<gs->zarr.loaded_plugins_max;i++) {
	    if((p = gs->zarr.loaded_plugins[i]) != NULL) {
		if(p->hdf5.filter == NULL || p->codec.codec == NULL) {
		    /* mark this entry as incomplete */
		    p->incomplete = 1;
		}
	    }
	}
    }
    /* Initialize all remaining plugins */
    {
        size_t i;
	NCZ_Plugin* p;
	for(i=1;i<gs->zarr.loaded_plugins_max;i++) {
	    if((p = gs->zarr.loaded_plugins[i]) != NULL) {
		if(p->incomplete) continue;
		if(p->hdf5.filter != NULL && p->codec.codec != NULL) {
		    if(p->codec.codec && p->codec.codec->NCZ_codec_initialize)
			p->codec.codec->NCZ_codec_initialize();
		}
	    }
	}
    }
    
done:
    nullfree(defaultpluginpath);
    nclistfreeall(dirs);
    errno = 0;
    return ZUNTRACE(ret);
}

/* Load all the filters within a specified directory */
static int
NCZ_load_plugin_dir(const char* path)
{
    size_t i;
    int stat = NC_NOERR;
    size_t pathlen;
    NClist* contents = nclistnew();
    char* file = NULL;
    struct NCglobalstate* gs = NC_getglobalstate();

    ZTRACE(7,"path=%s",path);


    if(path == NULL) {stat = NC_EINVAL; goto done;}
    pathlen = strlen(path);
    if(pathlen == 0) {stat = NC_EINVAL; goto done;}

    if((stat = getentries(path,contents))) goto done;
    for(i=0;i<nclistlength(contents);i++) {
        const char* name = (const char*)nclistget(contents,i);
	size_t nmlen = strlen(name);
	size_t flen = pathlen+1+nmlen+1;
	size_t id;
	NCZ_Plugin* plugin = NULL;

	assert(nmlen > 0);
	nullfree(file); file = NULL;
	if((file = (char*)malloc(flen))==NULL) {stat = NC_ENOMEM; goto done;}
	file[0] = '\0';
	strlcat(file,path,flen);
	strlcat(file,"/",flen);
	strlcat(file,name,flen);
	/* See if can load the file */
	stat = NCZ_load_plugin(file,&plugin);
	switch (stat) {
	case NC_NOERR: break;
	case NC_ENOFILTER: case NC_ENOTFOUND:
	    stat = NC_NOERR;
	    break; /* will cause it to be ignored */
	default: goto done;
	}
	if(plugin != NULL) {
	    id = (size_t)plugin->hdf5.filter->id;
	    if(gs->zarr.loaded_plugins[id] == NULL) {
	        gs->zarr.loaded_plugins[id] = plugin;
		if(id > gs->zarr.loaded_plugins_max) gs->zarr.loaded_plugins_max = id;
	    } else {
	        NCZ_unload_plugin(plugin); /* its a duplicate */
	    }
	} else
	    stat = NC_NOERR; /*ignore failure */
    }	

done:
    nullfree(file);
    nclistfreeall(contents);
    return ZUNTRACE(stat);
}

int
NCZ_load_plugin(const char* path, struct NCZ_Plugin** plugp)
{
    int stat = NC_NOERR;
    NCZ_Plugin* plugin = NULL;
    const H5Z_class2_t* h5class = NULL;
    H5PL_type_t h5type = 0;
    const NCZ_codec_t** cp = NULL;
    const NCZ_codec_t* codec = NULL;
    NCPSharedLib* lib = NULL;
    int flags = NCP_GLOBAL;
    size_t h5id = 0;
    
    assert(path != NULL && strlen(path) > 0 && plugp != NULL);

    ZTRACE(8,"path=%s",path);

    if(plugp) *plugp = NULL;

#if defined NAMEOPT || defined _WIN32
    /*triage because visual studio does a popup if the file will not load*/
    if(!pluginnamecheck(path)) {stat = NC_ENOFILTER; goto done;}
#endif

    /* load the shared library */
    if((stat = ncpsharedlibnew(&lib))) goto done;
    if((stat = ncpload(lib,path,flags))) goto done;


    /* See what we have */
    {
	const H5PL_get_plugin_type_proto gpt =  (H5PL_get_plugin_type_proto)ncpgetsymbol(lib,"H5PLget_plugin_type");
	const H5PL_get_plugin_info_proto gpi =  (H5PL_get_plugin_info_proto)ncpgetsymbol(lib,"H5PLget_plugin_info");
	const NCZ_get_codec_info_proto  npi =  (NCZ_get_codec_info_proto)ncpgetsymbol(lib,"NCZ_get_codec_info");
	const NCZ_codec_info_defaults_proto  cpd =  (NCZ_codec_info_defaults_proto)ncpgetsymbol(lib,"NCZ_codec_info_defaults");

        if(gpt == NULL && gpi == NULL && npi == NULL && cpd == NULL)
	    {stat = THROW(NC_ENOFILTER); goto done;}

	/* We can have cpd  or we can have (gpt && gpi && npi) but not both sets */
	if(cpd != NULL) {
	    cp = (const NCZ_codec_t**)cpd();
        } else {/* cpd => !gpt && !gpi && !npi */
            if(gpt != NULL && gpi != NULL) { /* get HDF5 info */
                h5type = gpt();
                h5class = gpi();        
                /* Verify */
                if(h5type != H5PL_TYPE_FILTER) {stat = NC_EPLUGIN; goto done;}
                if(h5class->version != H5Z_CLASS_T_VERS) {stat = NC_EFILTER; goto done;}
            }
            if(npi != NULL) {/* get Codec info */
		codec = npi();
                /* Verify */
                if(codec->version != NCZ_CODEC_CLASS_VER) {stat = NC_EPLUGIN; goto done;}
                if(codec->sort != NCZ_CODEC_HDF5) {stat = NC_EPLUGIN; goto done;}
	    }
        }
    }

    /* Handle defaults separately */
    if(cp != NULL) {
	int used = 0;
        if((stat = loadcodecdefaults(path,cp,lib,&used))) goto done;
	if(used) lib = NULL;
	goto done;
    }

    if(h5class != NULL && codec != NULL) {
	/* Verify consistency of the HDF5 and the Codec */
	if(((size_t)h5class->id) != codec->hdf5id) goto done; /* ignore */
    } 

    /* There are several cases to consider:
    1. This library has both HDF5 API and Codec API => merge
    2. This library has HDF5 API only and Codec API was already found in another library => merge
    3. This library has Codec API only and HDF5 API was already found in another library => merge    
    */

    /* Get any previous plugin entry for this id; may be NULL */
    if(h5class != NULL) {
	h5id = (size_t)h5class->id;
	if((stat = NCZ_plugin_loaded(h5id,&plugin))) goto done;
    } else if(codec != NULL) {
	h5id = (size_t)codec->hdf5id;
	if((stat = NCZ_plugin_loaded(h5id,&plugin))) goto done;
    }

    if(plugin == NULL) {
	/* create new entry */
	if((plugin = (NCZ_Plugin*)calloc(1,sizeof(NCZ_Plugin)))==NULL) {stat = NC_ENOMEM; goto done;}
    }
    
    /* Fill in the plugin */
    if(h5class != NULL && plugin->hdf5.filter == NULL) {
	plugin->hdf5.filter = h5class;
	plugin->hdf5.hdf5lib = lib;
	lib = NULL;
    }
    if(codec != NULL && plugin->codec.codec == NULL) {
	plugin->codec.codec = codec;
	plugin->codec.codeclib = lib;
	lib = NULL;
    }
    /* Cleanup */
    if(plugin->hdf5.hdf5lib == plugin->codec.codeclib) /* Works for NULL case also */
	plugin->codec.codeclib = NULL;
    if((stat=NCZ_plugin_save(h5id,plugin))) goto done;
    plugin = NULL;

done:
    if(lib)
       (void)ncpsharedlibfree(lib);
    if(plugin) NCZ_unload_plugin(plugin);
    return ZUNTRACEX(stat,"plug=%p",*plugp);
}

int
NCZ_unload_plugin(NCZ_Plugin* plugin)
{
    struct NCglobalstate* gs = NC_getglobalstate();

    ZTRACE(9,"plugin=%p",plugin);

    if(plugin) {
	if(plugin->codec.codec && plugin->codec.codec->NCZ_codec_finalize)
		plugin->codec.codec->NCZ_codec_finalize();
        if(plugin->hdf5.filter != NULL) gs->zarr.loaded_plugins[plugin->hdf5.filter->id] = NULL;
	if(plugin->hdf5.hdf5lib != NULL) (void)ncpsharedlibfree(plugin->hdf5.hdf5lib);
	if(!plugin->codec.defaulted && plugin->codec.codeclib != NULL) (void)ncpsharedlibfree(plugin->codec.codeclib);
memset(plugin,0,sizeof(NCZ_Plugin));
	free(plugin);
    }
    return ZUNTRACE(NC_NOERR);
}

int
NCZ_plugin_loaded(size_t filterid, NCZ_Plugin** pp)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    struct NCZ_Plugin* plug = NULL;
    ZTRACE(6,"filterid=%d",filterid);
    if(filterid <= 0 || filterid >= H5Z_FILTER_MAX)
	{stat = NC_EINVAL; goto done;}
    if(filterid <= gs->zarr.loaded_plugins_max) 
        plug = gs->zarr.loaded_plugins[filterid];
    if(pp) *pp = plug;
done:
    return ZUNTRACEX(stat,"plugin=%p",*pp);
}

int
NCZ_plugin_loaded_byname(const char* name, NCZ_Plugin** pp)
{
    int stat = NC_NOERR;
    size_t i;
    struct NCZ_Plugin* plug = NULL;
    struct NCglobalstate* gs = NC_getglobalstate();

    ZTRACE(6,"pluginname=%s",name);
    if(name == NULL) {stat = NC_EINVAL; goto done;}
    for(i=1;i<=gs->zarr.loaded_plugins_max;i++) {
        if (!gs->zarr.loaded_plugins[i]) continue;
        if(!gs->zarr.loaded_plugins[i] || !gs->zarr.loaded_plugins[i]->codec.codec) continue; /* no plugin or no codec */
        if(strcmp(name, gs->zarr.loaded_plugins[i]->codec.codec->codecid) == 0)
	    {plug = gs->zarr.loaded_plugins[i]; break;}
    }
    if(pp) *pp = plug;
done:
    return ZUNTRACEX(stat,"plugin=%p",*pp);
}

static int
NCZ_plugin_save(size_t filterid, NCZ_Plugin* p)
{
    int stat = NC_NOERR;
    struct NCglobalstate* gs = NC_getglobalstate();

    ZTRACE(6,"filterid=%d p=%p",filterid,p);
    if(filterid <= 0 || filterid >= H5Z_FILTER_MAX)
	{stat = NC_EINVAL; goto done;}
    if(filterid > gs->zarr.loaded_plugins_max) gs->zarr.loaded_plugins_max = filterid;
    gs->zarr.loaded_plugins[filterid] = p;
done:
    return ZUNTRACE(stat);
}

static int
loadcodecdefaults(const char* path, const NCZ_codec_t** cp, NCPSharedLib* lib, int* lib_usedp)
{
    int stat = NC_NOERR;
    int lib_used = 0;
    struct NCglobalstate* gs = NC_getglobalstate();

    nclistpush(gs->zarr.default_libs,lib);
    for(;*cp;cp++) {
        struct CodecAPI* c0;
        c0 = (struct CodecAPI*)calloc(1,sizeof(struct CodecAPI));
	if(c0 == NULL) {stat = NC_ENOMEM; goto done;}
        c0->codec = *cp;
	c0->codeclib = lib;
	lib_used = 1; /* remember */
	nclistpush(gs->zarr.codec_defaults,c0); c0 = NULL;
    }
done:
    if(lib_usedp) *lib_usedp = lib_used;
    return stat;
}

#if defined(NAMEOPT) || defined(_WIN32)
static int
pluginnamecheck(const char* name)
{
   size_t count,len;
   long i;
   const char* p;
   if(name == NULL) return 0;
   /* get basename */
   p = strrchr(name,'/');
   if(p != NULL) name = (p+1);
   len = strlen(name);
   if(len == 0) return 0;
   i = (long)(len-1);
   count = 1;
   p = name+i;
   for(;i>=0;i--,count++,p--) {
	char c = *p;
	if(c == '/') break;
	if(c == '.') {
	    if(count >= 3 && memcmp(p,".so",3)==0) return 1;
    	    if(count >= 4 && memcmp(p,".dll",4)==0) return 1;
       	    if(count >= 6 && memcmp(p,".dylib",6)==0) return 1;
	}
   }
   return 0;
}
#endif

/**************************************************/
/*
Get entries in a path that is assumed to be a directory.
*/

#ifdef _WIN32

static int
getentries(const char* path, NClist* contents)
{
    /* Iterate over the entries in the directory */
    int ret = NC_NOERR;
    errno = 0;
    WIN32_FIND_DATA FindFileData;
    HANDLE dir = NULL;
    char* ffpath = NULL;
    char* lpath = NULL;
    size_t len;
    char* d = NULL;

    ZTRACE(6,"path=%s",path);

    /* We need to process the path to make it work with FindFirstFile */
    len = strlen(path);
    /* Need to terminate path with '/''*' */
    ffpath = (char*)malloc(len+2+1);
    memcpy(ffpath,path,len);
    if(path[len-1] != '/') {
	ffpath[len] = '/';	
	len++;
    }
    ffpath[len] = '*'; len++;
    ffpath[len] = '\0';

    /* localize it */
    if((ret = nczm_localize(ffpath,&lpath,LOCALIZE))) goto done;
    dir = FindFirstFile(lpath, &FindFileData);
    if(dir == INVALID_HANDLE_VALUE) {
	/* Distinguish not-a-directory from no-matching-file */
        switch (GetLastError()) {
	case ERROR_FILE_NOT_FOUND: /* No matching files */ /* fall thru */
	    ret = NC_NOERR;
	    goto done;
	case ERROR_DIRECTORY: /* not a directory */
	default:
            ret = NC_EEMPTY;
	    goto done;
	}
    }
    do {
	char* p = NULL;
	const char* name = NULL;
        name = FindFileData.cFileName;
	if(strcmp(name,".")==0 || strcmp(name,"..")==0)
	    continue;
	nclistpush(contents,strdup(name));
    } while(FindNextFile(dir, &FindFileData));

done:
    if(dir) FindClose(dir);
    nullfree(lpath);
    nullfree(ffpath);
    nullfree(d);
    errno = 0;
    return ZUNTRACEX(ret,"|contents|=%d",(int)nclistlength(contents));
}

#else /* !_WIN32 */

int
getentries(const char* path, NClist* contents)
{
    int ret = NC_NOERR;
    errno = 0;
    DIR* dir = NULL;

    ZTRACE(6,"path=%s",path);

    dir = NCopendir(path);
    if(dir == NULL)
        {ret = (errno); goto done;}
    for(;;) {
	const char* name = NULL;
	struct dirent* de = NULL;
	errno = 0;
        de = readdir(dir);
        if(de == NULL)
	    {ret = (errno); goto done;}
	if(strcmp(de->d_name,".")==0 || strcmp(de->d_name,"..")==0)
	    continue;
	name = de->d_name;
	nclistpush(contents,strdup(name));
    }
done:
    if(dir) NCclosedir(dir);
    errno = 0;
    return ZUNTRACEX(ret,"|contents|=%d",(int)nclistlength(contents));
}
#endif /*_WIN32*/

/**************************************************/


#if defined(DEBUGF) || defined(DEBUGL)

const char*
printplugin(const NCZ_Plugin* plugin)
{
    static char plbuf[4096];
    char plbuf2[2000];
    char plbuf1[2000];

    if(plugin == NULL) return "plugin=NULL";
    plbuf2[0] = '\0'; plbuf1[0] = '\0';
    if(plugin->hdf5.filter)
        snprintf(plbuf1,sizeof(plbuf1),"hdf5={id=%u name=%s}",plugin->hdf5.filter->id,plugin->hdf5.filter->name);
    if(plugin->codec.codec)
        snprintf(plbuf2,sizeof(plbuf2),"codec={codecid=%s hdf5id=%u}",plugin->codec.codec->codecid,plugin->codec.codec->hdf5id);
    snprintf(plbuf,4096,"plugin={%s %s}",plbuf1,plbuf2);
    return plbuf;
}

static char*
printparams(size_t nparams, const unsigned* params)
{
    static char ppbuf[4096];
    if(nparams == 0)
        snprintf(ppbuf,4096,"{0,%p}",params);
    else 
        snprintf(ppbuf,4096,"{%u %s}",(unsigned)nparams,nczprint_paramvector(nparams,params));
    return ppbuf;
}

static char*
printnczparams(const NCZ_Params p)
{
    return printparams(p.nparams,p.params);
}

static const char*
printcodec(const NCZ_Codec c)
{
    static char pcbuf[4096];
    snprintf(pcbuf,sizeof(pcbuf),"{id=%s codec=%s}",
		c.id,NULLIFY(c.codec));
    return pcbuf;
}

static const char*
printhdf5(const NCZ_HDF5 h)
{
    static char phbuf[4096];
    snprintf(phbuf,sizeof(phbuf),"{id=%u visible=%s working=%s}",
    		h.id, printnczparams(h.visible), printnczparams(h.working));
    return phbuf;
}
#endif /* defined(DEBUGF) || defined(DEBUGL) */

/* Suppress selected unused static functions */
static void static_unused(void)
{
    void* p = NULL;
    (void)p;
    p = static_unused;
#if defined(DEBUGF) || defined(DEBUGL)
(void)printplugin(NULL);
(void)printparams(0, NULL);
(void)printnczparams(const NCZ_Params p);
(void)printcodec(const NCZ_Codec c);
(void)printhdf5(const NCZ_HDF5 h);
#endif
}

