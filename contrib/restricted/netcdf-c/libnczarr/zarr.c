/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include <stddef.h>

/**************************************************/
/* Forwards */

static int applycontrols(NCZ_FILE_INFO_T* zinfo);

/***************************************************/
/* API */

/**
@internal Create the topmost dataset object and setup up
          NCZ_FILE_INFO_T state.
@param zinfo - [in] the internal state
@return NC_NOERR
@author Dennis Heimbigner
*/

int
ncz_create_dataset(NC_FILE_INFO_T* file, NC_GRP_INFO_T* root, NClist* controls)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCZ_GRP_INFO_T* zgrp = NULL;
    NCURI* uri = NULL;
    NC* nc = NULL;
    NCjson* json = NULL;
    char* key = NULL;

    ZTRACE(3,"file=%s root=%s controls=%s",file->hdr.name,root->hdr.name,(controls?nczprint_envv(controls):"null"));

    nc = (NC*)file->controller;

    /* Add struct to hold NCZ-specific file metadata. */
    if (!(zinfo = calloc(1, sizeof(NCZ_FILE_INFO_T))))
        {stat = NC_ENOMEM; goto done;}
    file->format_file_info = zinfo;
    zinfo->common.file = file;

    /* Add struct to hold NCZ-specific group info. */
    if (!(zgrp = calloc(1, sizeof(NCZ_GRP_INFO_T))))
        {stat = NC_ENOMEM; goto done;}
    root->format_grp_info = zgrp;
    zgrp->common.file = file;

    /* Fill in NCZ_FILE_INFO_T */
    zinfo->creating = 1;
    zinfo->common.file = file;
    zinfo->native_endianness = (NC_isLittleEndian() ? NC_ENDIAN_LITTLE : NC_ENDIAN_BIG);
    if((zinfo->controllist=nclistclone(controls,1)) == NULL)
	{stat = NC_ENOMEM; goto done;}

    /* fill in some of the zinfo and zroot fields */
    zinfo->zarr.zarr_version = atoi(ZARRVERSION);
    sscanf(NCZARRVERSION,"%lu.%lu.%lu",
	   &zinfo->zarr.nczarr_version.major,
	   &zinfo->zarr.nczarr_version.minor,
	   &zinfo->zarr.nczarr_version.release);

    zinfo->default_maxstrlen = NCZ_MAXSTR_DEFAULT;

    /* Apply client controls */
    if((stat = applycontrols(zinfo))) goto done;

    /* Load auth info from rc file */
    if((stat = ncuriparse(nc->path,&uri))) goto done;
    if(uri) {
	if((stat = NC_authsetup(&zinfo->auth, uri)))
	    goto done;
    }

    /* initialize map handle*/
    if((stat = nczmap_create(zinfo->controls.mapimpl,nc->path,nc->mode,zinfo->controls.flags,NULL,&zinfo->map)))
	goto done;

    if((stat = NCZMD_set_metadata_handler(zinfo))){
        goto done;
    }

done:
    ncurifree(uri);
    NCJreclaim(json);
    nullfree(key);
    return ZUNTRACE(stat);
}

/**
@internal Open the topmost dataset object.
@param file - [in] the file struct
@param controls - [in] the fragment list in envv form from uri
@return NC_NOERR
@author Dennis Heimbigner
*/

int
ncz_open_dataset(NC_FILE_INFO_T* file, NClist* controls)
{
    int stat = NC_NOERR;
    NC* nc = NULL;
    NC_GRP_INFO_T* root = NULL;
    NCURI* uri = NULL;
    void* content = NULL;
    NCjson* json = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;
    int mode;
    NClist* modeargs = NULL;
    char* nczarr_version = NULL;
    char* zarr_format = NULL;

    ZTRACE(3,"file=%s controls=%s",file->hdr.name,(controls?nczprint_envv(controls):"null"));

    /* Extract info reachable via file */
    nc = (NC*)file->controller;
    mode = nc->mode;

    root = file->root_grp;
    assert(root != NULL && root->hdr.sort == NCGRP);

    /* Add struct to hold NCZ-specific file metadata. */
    if (!(file->format_file_info = calloc(1, sizeof(NCZ_FILE_INFO_T))))
        {stat = NC_ENOMEM; goto done;}
    zinfo = file->format_file_info;

    /* Fill in NCZ_FILE_INFO_T */
    zinfo->creating = 0;
    zinfo->common.file = file;
    zinfo->native_endianness = (NC_isLittleEndian() ? NC_ENDIAN_LITTLE : NC_ENDIAN_BIG);
    if((zinfo->controllist=nclistclone(controls,1)) == NULL)
	{stat = NC_ENOMEM; goto done;}
    zinfo->default_maxstrlen = NCZ_MAXSTR_DEFAULT;

    /* Add struct to hold NCZ-specific group info. */
    if (!(root->format_grp_info = calloc(1, sizeof(NCZ_GRP_INFO_T))))
        {stat = NC_ENOMEM; goto done;}
    ((NCZ_GRP_INFO_T*)root->format_grp_info)->common.file = file;

    /* Apply client controls */
    if((stat = applycontrols(zinfo))) goto done;

    /* initialize map handle*/
    if((stat = nczmap_open(zinfo->controls.mapimpl,nc->path,mode,zinfo->controls.flags,NULL,&zinfo->map)))
	goto done;

    if((stat = NCZMD_set_metadata_handler(zinfo))) {
        goto done;
    }

    /* Ok, try to read superblock */
    if((stat = ncz_read_superblock(file,&nczarr_version,&zarr_format))) goto done;

    if(nczarr_version == NULL) /* default */
        nczarr_version = strdup(NCZARRVERSION);
    if(zarr_format == NULL) /* default */
       zarr_format = strdup(ZARRVERSION);
    /* Extract the information from it */
    if(sscanf(zarr_format,"%d",&zinfo->zarr.zarr_version)!=1)
	{stat = NC_ENCZARR; goto done;}		
    if(sscanf(nczarr_version,"%lu.%lu.%lu",
		    &zinfo->zarr.nczarr_version.major,
		    &zinfo->zarr.nczarr_version.minor,
		    &zinfo->zarr.nczarr_version.release) == 0)
	{stat = NC_ENCZARR; goto done;}

    /* Load auth info from rc file */
    if((stat = ncuriparse(nc->path,&uri))) goto done;
    if(uri) {
	if((stat = NC_authsetup(&zinfo->auth, uri)))
	    goto done;
    }

done:
    nullfree(zarr_format);
    nullfree(nczarr_version);
    ncurifree(uri);
    nclistfreeall(modeargs);
    if(json) NCJreclaim(json);
    nullfree(content);
    return ZUNTRACE(stat);
}

/**
 * @internal Determine whether file is netCDF-4.
 *
 * For libzarr, this is always true.
 *
 * @param h5 Pointer to HDF5 file info struct.
 *
 * @returns NC_NOERR No error.
 * @author Dennis Heimbigner.
 */
int
NCZ_isnetcdf4(struct NC_FILE_INFO* h5)
{
    int isnc4 = 1;
    NC_UNUSED(h5);
    return isnc4;
}

/**
 * @internal Determine version info
 *
 * For libzarr, this is not well defined
 *
 * @param majorp Pointer to major version number
 * @param minorp Pointer to minor version number
 * @param releasep Pointer to release version number
 *
 * @returns NC_NOERR No error.
 * @author Dennis Heimbigner.
 */
int
NCZ_get_libversion(unsigned long* majorp, unsigned long* minorp,unsigned long* releasep)
{
    unsigned long m0,m1,m2;
    sscanf(NCZARRVERSION,"%lu.%lu.%lu",&m0,&m1,&m2);
    if(majorp) *majorp = m0;
    if(minorp) *minorp = m1;
    if(releasep) *releasep = m2;
    return NC_NOERR;
}

/**
 * @internal Determine "superblock" number.
 *
 * For libzarr, use the value of the major part of the nczarr version.
 *
 * @param superblocp Pointer to place to return superblock.
 * use the nczarr format version major as the superblock number.
 *
 * @returns NC_NOERR No error.
 * @author Dennis Heimbigner.
 */
int
NCZ_get_superblock(NC_FILE_INFO_T* file, int* superblockp)
{
    NCZ_FILE_INFO_T* zinfo = file->format_file_info;
    if(superblockp) *superblockp = zinfo->zarr.nczarr_version.major;
    return NC_NOERR;
}

/**************************************************/
/* Utilities */

static const char*
controllookup(NClist* controls, const char* key)
{
    size_t i;
    for(i=0;i<nclistlength(controls);i+=2) {
        const char* p = (char*)nclistget(controls,i);
	if(strcasecmp(key,p)==0) {
	    return (const char*)nclistget(controls,i+1);
	}
    }
    return NULL;
}


static int
applycontrols(NCZ_FILE_INFO_T* zinfo)
{
    size_t i;
    int stat = NC_NOERR;
    const char* value = NULL;
    NClist* modelist = nclistnew();
    size64_t noflags = 0; /* track non-default negative flags */

    if((value = controllookup(zinfo->controllist,"mode")) != NULL) {
	if((stat = NCZ_comma_parse(value,modelist))) goto done;
    }
    /* Process the modelist first */
    zinfo->controls.mapimpl = NCZM_DEFAULT;
    zinfo->controls.flags |= FLAG_XARRAYDIMS; /* Always support XArray convention where possible */
    zinfo->controls.flags |= (NCZARR_CONSOLIDATED_DEFAULT ? FLAG_CONSOLIDATED : 0);
    for(i=0;i<nclistlength(modelist);i++) {
        const char* p = nclistget(modelist,i);
	if(strcasecmp(p,PUREZARRCONTROL)==0)
	    zinfo->controls.flags |= (FLAG_PUREZARR);
	else if(strcasecmp(p,XARRAYCONTROL)==0)
	    zinfo->controls.flags |= FLAG_PUREZARR;
	else if(strcasecmp(p,NOXARRAYCONTROL)==0)
	    noflags |= FLAG_XARRAYDIMS;
	else if(strcasecmp(p,"zip")==0) zinfo->controls.mapimpl = NCZM_ZIP;
	else if(strcasecmp(p,"file")==0) zinfo->controls.mapimpl = NCZM_FILE;
	else if(strcasecmp(p,"s3")==0) zinfo->controls.mapimpl = NCZM_S3;
	else if(strcasecmp(p,"consolidated") == 0)
	        zinfo->controls.flags |= FLAG_CONSOLIDATED;
    }
    /* Apply negative controls by turning off negative flags */
    /* This is necessary to avoid order dependence of mode flags when both positive and negative flags are defined */
    zinfo->controls.flags &= (~noflags);

    /* Process other controls */
    if((value = controllookup(zinfo->controllist,"log")) != NULL) {
	zinfo->controls.flags |= FLAG_LOGGING;
        ncsetloglevel(NCLOGNOTE);
    }
    if((value = controllookup(zinfo->controllist,"show")) != NULL) {
	if(strcasecmp(value,"fetch")==0)
	    zinfo->controls.flags |= FLAG_SHOWFETCH;
    }
done:
    nclistfreeall(modelist);
    return stat;
}
