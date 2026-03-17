/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include "zfilter.h"
#include <stddef.h>
#include "ncutil.h"

#ifndef nulldup
#define nulldup(x) ((x)?strdup(x):(x))
#endif

#undef FILLONCLOSE

/*mnemonics*/
#define DICTOPEN '{'
#define DICTCLOSE '}'

/* Forward */
static int ncz_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp);
static int ncz_sync_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose);

static int download_jatts(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson** jattsp, const NCjson** jtypesp);
static int zconvert(const NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst);
static int computeattrinfo(const char* name, const NCjson* jtypes, nc_type typehint, int purezarr, NCjson* values,
		nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap);
static int parse_group_content(const NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps);
static int parse_group_content_pure(NCZ_FILE_INFO_T*  zinfo, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps);
static int define_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp);
static int define_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* diminfo);
static int define_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames);
static int define_var1(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* varname);
static int define_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames);
static int searchvars(NCZ_FILE_INFO_T*, NC_GRP_INFO_T*, NClist*);
static int searchsubgrps(NCZ_FILE_INFO_T*, NC_GRP_INFO_T*, NClist*);
static int locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp);
static int createdim(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp);
static int parsedimrefs(NC_FILE_INFO_T*, NClist* dimnames,  size64_t* shape, NC_DIM_INFO_T** dims, int create);
static int decodeints(const NCjson* jshape, size64_t* shapes);
static int computeattrdata(nc_type typehint, nc_type* typeidp, const NCjson* values, size_t* typelenp, size_t* lenp, void** datap);
static int computedimrefs(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int purezarr, int xarray, int ndims, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims);
static int json_convention_read(const NCjson* jdict, NCjson** jtextp);
static int ncz_validate(NC_FILE_INFO_T* file);
static int insert_attr(NCjson* jatts, NCjson* jtypes, const char* aname, NCjson* javalue, const char* atype);
static int insert_nczarr_attr(NCjson* jatts, NCjson* jtypes);
static int upload_attrs(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jatts);
static int getnczarrkey(NC_OBJ* container, const char* name, const NCjson** jncxxxp);
static int downloadzarrobj(NC_FILE_INFO_T*, struct ZARROBJ* zobj, const char* fullpath, const char* objname);
static int dictgetalt(const NCjson* jdict, const char* name, const char* alt, const NCjson** jvaluep);

/**************************************************/
/**************************************************/
/* Synchronize functions to make map and memory
be consistent. There are two sets of functions,
1) _sync_ - push memory to map (optionally create target)
2) _read_ - pull map data into memory
These functions are generally non-recursive. It is assumed
that the recursion occurs in the caller's code.
*/

/**
 * @internal Synchronize file metadata from memory to map.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_sync_file(NC_FILE_INFO_T* file, int isclose)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    NC_UNUSED(isclose);

    LOG((3, "%s: file: %s", __func__, file->controller->path));
    ZTRACE(3,"file=%s isclose=%d",file->controller->path,isclose);

    /* Write out root group recursively */
    if((stat = ncz_sync_grp(file, file->root_grp, isclose)))
        goto done;

    stat = NCZMD_consolidate((NCZ_FILE_INFO_T*)file->format_file_info);
done:
    NCJreclaim(json);
    return ZUNTRACE(stat);
}

/**
 * @internal Synchronize dimension data from memory to map.
 *
 * @param grp Pointer to grp struct containing the dims.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_collect_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NCjson** jdimsp)
{
    int stat=NC_NOERR;
    size_t i;
    NCjson* jdims = NULL;
    NCjson* jdimsize = NULL;
    NCjson* jdimargs = NULL;

    LOG((3, "%s: ", __func__));
    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);

    NCJnew(NCJ_DICT,&jdims);
    for(i=0; i<ncindexsize(grp->dim); i++) {
	NC_DIM_INFO_T* dim = (NC_DIM_INFO_T*)ncindexith(grp->dim,i);
	char slen[128];

        snprintf(slen,sizeof(slen),"%llu",(unsigned long long)dim->len);
	NCJnewstring(NCJ_INT,slen,&jdimsize);

	/* If dim is not unlimited, then write in the old format to provide
           maximum back compatibility.
        */
	if(dim->unlimited) {
	    NCJnew(NCJ_DICT,&jdimargs);
	    if((stat = NCJaddstring(jdimargs,NCJ_STRING,"size"))<0) {stat = NC_EINVAL; goto done;}
	    if((stat = NCJappend(jdimargs,jdimsize))<0) {stat = NC_EINVAL; goto done;}
	    jdimsize = NULL;
  	    if((stat = NCJaddstring(jdimargs,NCJ_STRING,"unlimited"))<0) {stat = NC_EINVAL; goto done;}
	    if((stat = NCJaddstring(jdimargs,NCJ_INT,"1"))<0) {stat = NC_EINVAL; goto done;}
	} else { /* !dim->unlimited */
	    jdimargs = jdimsize;
	    jdimsize = NULL;
	}
	if((stat = NCJaddstring(jdims,NCJ_STRING,dim->hdr.name))<0) {stat = NC_EINVAL; goto done;}
	if((stat = NCJappend(jdims,jdimargs))<0) {stat = NC_EINVAL; goto done;}
    }
    if(jdimsp) {*jdimsp = jdims; jdims = NULL;}
done:
    NCJreclaim(jdims);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Recursively synchronize group from memory to map.
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_sync_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, int isclose)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zinfo = NULL;
    char version[1024];
    int purezarr = 0;
    NCZMAP* map = NULL;
    char* key = NULL;
    NCjson* json = NULL;
    NCjson* jgroup = NULL;
    NCjson* jdims = NULL;
    NCjson* jvars = NULL;
    NCjson* jsubgrps = NULL;
    NCjson* jnczgrp = NULL;
    NCjson* jsuper = NULL;
    NCjson* jtmp = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;

    LOG((3, "%s: dims: %s", __func__, key));
    ZTRACE(3,"file=%s grp=%s isclose=%d",file->controller->path,grp->hdr.name,isclose);

    zinfo = file->format_file_info;
    map = zinfo->map;

    purezarr = (zinfo->controls.flags & FLAG_PUREZARR)?1:0;

    /* build Z2GROUP contents */
    NCJnew(NCJ_DICT,&jgroup);
    snprintf(version,sizeof(version),"%d",zinfo->zarr.zarr_version);
    if((stat = NCJaddstring(jgroup,NCJ_STRING,"zarr_format"))<0) {stat = NC_EINVAL; goto done;}
    if((stat = NCJaddstring(jgroup,NCJ_INT,version))<0) {stat = NC_EINVAL; goto done;}

    /* Write to map */
    NCZ_grpkey(grp,&key);
    if((stat=NCZMD_update_json_group(zinfo,key,(const NCjson*)jgroup))) goto done;
    nullfree(key); key = NULL;

    if(!purezarr) {
        if(grp->parent == NULL) { /* Root group */
	    /* create superblock */
            snprintf(version,sizeof(version),"%lu.%lu.%lu",
		 zinfo->zarr.nczarr_version.major,
		 zinfo->zarr.nczarr_version.minor,
		 zinfo->zarr.nczarr_version.release);
	    NCJnew(NCJ_DICT,&jsuper);
	    if((stat = NCJinsertstring(jsuper,"version",version))<0) {stat = NC_EINVAL; goto done;}
	}
        /* Create dimensions dict */
        if((stat = ncz_collect_dims(file,grp,&jdims))) goto done;

        /* Create vars list */
        NCJnew(NCJ_ARRAY,&jvars);
        for(i=0; i<ncindexsize(grp->vars); i++) {
	    NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	    if((stat = NCJaddstring(jvars,NCJ_STRING,var->hdr.name))<0) {stat = NC_EINVAL; goto done;}
        }

        /* Create subgroups list */
        NCJnew(NCJ_ARRAY,&jsubgrps);
        for(i=0; i<ncindexsize(grp->children); i++) {
	    NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	    if((stat = NCJaddstring(jsubgrps,NCJ_STRING,g->hdr.name))<0) {stat = NC_EINVAL; goto done;}
        }
        /* Create the "_nczarr_group" dict */
        NCJnew(NCJ_DICT,&jnczgrp);
        /* Insert the various dicts and arrays */
        if((stat = NCJinsert(jnczgrp,"dimensions",jdims))<0) {stat = NC_EINVAL; goto done;}
        jdims = NULL; /* avoid memory problems */
        if((stat = NCJinsert(jnczgrp,"arrays",jvars))<0) {stat = NC_EINVAL; goto done;}
        jvars = NULL; /* avoid memory problems */
        if((stat = NCJinsert(jnczgrp,"groups",jsubgrps))<0) {stat = NC_EINVAL; goto done;}
        jsubgrps = NULL; /* avoid memory problems */
    }

    /* Build the .zattrs object */
    assert(grp->att);
    NCJnew(NCJ_DICT,&jatts);
    NCJnew(NCJ_DICT,&jtypes);
    if((stat = ncz_sync_atts(file, (NC_OBJ*)grp, grp->att, jatts, jtypes, isclose))) goto done;

    if(!purezarr && jnczgrp != NULL) {
        /* Insert _nczarr_group */
        if((stat=insert_attr(jatts,jtypes,NCZ_V2_GROUP,jnczgrp,"|J0"))) goto done;
	jnczgrp = NULL;
    }

    if(!purezarr && jsuper != NULL) {
        /* Insert superblock */
        if((stat=insert_attr(jatts,jtypes,NCZ_V2_SUPERBLOCK,jsuper,"|J0"))) goto done;
	jsuper = NULL;
    }

    /* As a last mod to jatts, insert the jtypes as an attribute */
    if(!purezarr && jtypes != NULL) {
	if((stat = insert_nczarr_attr(jatts,jtypes))) goto done;
	jtypes = NULL;
    }

    /* Write out the .zattrs */
    nullfree(key);
    NCZ_grpkey(grp,&key);
    if((stat = NCZMD_update_json_attrs(zinfo, key, (const NCjson *)jatts))) goto done;

    /* Now synchronize all the variables */
    for(i=0; i<ncindexsize(grp->vars); i++) {
	NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars,i);
	if((stat = ncz_sync_var(file,var,isclose))) goto done;
    }

    /* Now recurse to synchronize all the subgrps */
    for(i=0; i<ncindexsize(grp->children); i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = ncz_sync_grp(file,g,isclose))) goto done;
    }

done:
    NCJreclaim(jtmp);
    NCJreclaim(jsuper);
    NCJreclaim(json);
    NCJreclaim(jgroup);
    NCJreclaim(jdims);
    NCJreclaim(jvars);
    NCJreclaim(jsubgrps);
    NCJreclaim(jnczgrp);
    NCJreclaim(jtypes);
    NCJreclaim(jatts);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize variable meta data from memory to map.
 *
 * @param file Pointer to file struct
 * @param var Pointer to var struct
 * @param isclose If this called as part of nc_close() as opposed to nc_enddef().
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_sync_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose)
{
    size_t i;
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    char number[1024];
    NCZMAP* map = NULL;
    char* key = NULL;
    char* dimpath = NULL;
    NClist* dimrefs = NULL;
    NCjson* jvar = NULL;
    NCjson* jncvar = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jtmp = NULL;
    NCjson* jfill = NULL;
    NCjson* jatts = NULL;
    NCjson* jtypes = NULL;
    char* dtypename = NULL;
    int purezarr = 0;
    size64_t shape[NC_MAX_VAR_DIMS];
    NCZ_VAR_INFO_T* zvar = var->format_var_info;
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    NClist* filterchain = NULL;
    NCjson* jfilter = NULL;
#endif

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    zinfo = file->format_file_info;
    map = zinfo->map;

    purezarr = (zinfo->controls.flags & FLAG_PUREZARR)?1:0;

    /* Make sure that everything is established */
    /* ensure the fill value */
    if((stat = NCZ_ensure_fill_value(var))) goto done; /* ensure var->fill_value is set */
    assert(var->no_fill || var->fill_value != NULL);
    /* ensure the chunk cache */
    if((stat = NCZ_adjust_var_cache(var))) goto done;
    /* rebuild the fill chunk */
    if((stat = NCZ_ensure_fill_chunk(zvar->cache))) goto done;
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Build the filter working parameters for any filters */
    if((stat = NCZ_filter_setup(var))) goto done;
#endif

    /* Construct var path */
    if((stat = NCZ_varkey(var,&key)))
	goto done;

    /* Create the zarray json object */
    NCJnew(NCJ_DICT,&jvar);

    /* zarr_format key */
    snprintf(number,sizeof(number),"%d",zinfo->zarr.zarr_version);
    if((stat = NCJaddstring(jvar,NCJ_STRING,"zarr_format"))<0) {stat = NC_EINVAL; goto done;}
    if((stat = NCJaddstring(jvar,NCJ_INT,number))<0) {stat = NC_EINVAL; goto done;}

    /* Collect the shape vector */
    for(i=0;i<var->ndims;i++) {
	NC_DIM_INFO_T* dim = var->dim[i];
	shape[i] = dim->len;
    }
    /* but might be scalar */
    if(var->ndims == 0)
        shape[0] = 1;

    /* shape key */
    /* Integer list defining the length of each dimension of the array.*/
    /* Create the list */
    NCJnew(NCJ_ARRAY,&jtmp);
    if(zvar->scalar) {
	NCJaddstring(jtmp,NCJ_INT,"1");
    } else for(i=0;i<var->ndims;i++) {
	snprintf(number,sizeof(number),"%llu",shape[i]);
	NCJaddstring(jtmp,NCJ_INT,number);
    }
    if((stat = NCJinsert(jvar,"shape",jtmp))<0) {stat = NC_EINVAL; goto done;}
    jtmp = NULL;

    /* dtype key */
    /* A string or list defining a valid data type for the array. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"dtype"))<0) {stat = NC_EINVAL; goto done;}
    {	/* Add the type name */
	int endianness = var->type_info->endianness;
	int atomictype = var->type_info->hdr.id;
	assert(atomictype > 0 && atomictype <= NC_MAX_ATOMIC_TYPE);
	if((stat = ncz_nctype2dtype(atomictype,endianness,purezarr,NCZ_get_maxstrlen((NC_OBJ*)var),&dtypename))) goto done;
	if((stat = NCJaddstring(jvar,NCJ_STRING,dtypename))<0) {stat = NC_EINVAL; goto done;}
	nullfree(dtypename); dtypename = NULL;
    }

    /* chunks key */
    /* The zarr format does not support the concept
       of contiguous (or compact), so it will never appear in the read case.
    */
    /* list of chunk sizes */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"chunks"))<0) {stat = NC_EINVAL; goto done;}
    /* Create the list */
    NCJnew(NCJ_ARRAY,&jtmp);
    if(zvar->scalar) {
	NCJaddstring(jtmp,NCJ_INT,"1"); /* one chunk of size 1 */
    } else for(i=0;i<var->ndims;i++) {
	size64_t len = var->chunksizes[i];
	snprintf(number,sizeof(number),"%lld",len);
	NCJaddstring(jtmp,NCJ_INT,number);
    }
    if((stat = NCJappend(jvar,jtmp))<0) {stat = NC_EINVAL; goto done;}
    jtmp = NULL;

    /* fill_value key */
    if(var->no_fill) {
	NCJnew(NCJ_NULL,&jfill);
    } else {/*!var->no_fill*/
	int atomictype = var->type_info->hdr.id;
        if(var->fill_value == NULL) {
	     if((stat = NCZ_ensure_fill_value(var))) goto done;
	}
        /* Convert var->fill_value to a string */
	if((stat = NCZ_stringconvert(atomictype,1,var->fill_value,&jfill))) goto done;
	assert(jfill->sort != NCJ_ARRAY);
    }
    if((stat = NCJinsert(jvar,"fill_value",jfill))<0) {stat = NC_EINVAL; goto done;}
    jfill = NULL;

    /* order key */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"order"))<0) {stat = NC_EINVAL; goto done;}
    /* "C" means row-major order, i.e., the last dimension varies fastest;
       "F" means column-major order, i.e., the first dimension varies fastest.*/
    /* Default to C for now */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"C"))<0) {stat = NC_EINVAL; goto done;}

    /* Compressor and Filters */
    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"compressor"))<0) {stat = NC_EINVAL; goto done;}
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    filterchain = (NClist*)var->filters;
    if(nclistlength(filterchain) > 0) {
	struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,nclistlength(filterchain)-1);
        /* encode up the compressor */
        if((stat = NCZ_filter_jsonize(file,var,filter,&jtmp))) goto done;
    } else
#endif
    { /* no filters at all */
        /* Default to null */
        NCJnew(NCJ_NULL,&jtmp);
    }
    if(jtmp && (stat = NCJappend(jvar,jtmp))<0) {stat = NC_EINVAL; goto done;}
    jtmp = NULL;

    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* A list of JSON objects providing codec configurations, or ``null``
       if no filters are to be applied. */
    if((stat = NCJaddstring(jvar,NCJ_STRING,"filters"))<0) {stat = NC_EINVAL; goto done;}
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if(nclistlength(filterchain) > 1) {
	size_t k;
	/* jtmp holds the array of filters */
	NCJnew(NCJ_ARRAY,&jtmp);
	for(k=0;k<nclistlength(filterchain)-1;k++) {
 	    struct NCZ_Filter* filter = (struct NCZ_Filter*)nclistget(filterchain,k);
	    /* encode up the filter as a string */
	    if((stat = NCZ_filter_jsonize(file,var,filter,&jfilter))) goto done;
	    if((stat = NCJappend(jtmp,jfilter))<0) {stat = NC_EINVAL; goto done;}
	}
    } else
#endif
    /* no filters at all */
    NCJnew(NCJ_NULL,&jtmp);
    if((stat = NCJappend(jvar,jtmp))<0) {stat = NC_EINVAL; goto done;}
    jtmp = NULL;

    /* dimension_separator key */
    /* Single char defining the separator in chunk keys */
    if(zvar->dimension_separator != DFALT_DIM_SEPARATOR) {
	char sep[2];
	sep[0] = zvar->dimension_separator;/* make separator a string*/
	sep[1] = '\0';
        NCJnewstring(NCJ_STRING,sep,&jtmp);
        if((stat = NCJinsert(jvar,"dimension_separator",jtmp))<0) {stat = NC_EINVAL; goto done;}
        jtmp = NULL;
    }

    nullfree(key);
    key = NULL;
    NCZ_varkey(var, &key);
    if((stat=NCZMD_update_json_array(zinfo,key,(const NCjson*)jvar))) {
        goto done;
    }

    /* Capture dimref names as FQNs */
    if(var->ndims > 0) {
        if((dimrefs = nclistnew())==NULL) {stat = NC_ENOMEM; goto done;}
	for(i=0;i<var->ndims;i++) {
	    NC_DIM_INFO_T* dim = var->dim[i];
	    if((stat = NCZ_dimkey(dim,&dimpath))) goto done;
	    nclistpush(dimrefs,dimpath);
	    dimpath = NULL;
	}
    }

    /* Build the NCZ_V2_ARRAY object */
    {
	/* Create the dimrefs json object */
	NCJnew(NCJ_ARRAY,&jdimrefs);
	for(i=0;i<nclistlength(dimrefs);i++) {
	    const char* dim = nclistget(dimrefs,i);
	    NCJaddstring(jdimrefs,NCJ_STRING,dim);
	}
	NCJnew(NCJ_DICT,&jncvar);

	/* Insert dimension_referencess  */
	if((stat = NCJinsert(jncvar,"dimension_references",jdimrefs))<0) {stat = NC_EINVAL; goto done;}
	jdimrefs = NULL; /* Avoid memory problems */

	/* Add the _Storage flag */
	/* Record if this is a scalar */
	if(var->ndims == 0) {
	    NCJnewstring(NCJ_INT,"1",&jtmp);
	    if((stat = NCJinsert(jncvar,"scalar",jtmp))<0) {stat = NC_EINVAL; goto done;}
	    jtmp = NULL;
	}
	/* everything looks like it is chunked */
	NCJnewstring(NCJ_STRING,"chunked",&jtmp);
	if((stat = NCJinsert(jncvar,"storage",jtmp))<0) {stat = NC_EINVAL; goto done;}
	jtmp = NULL;
    }

    /* Build .zattrs object */
    assert(var->att);
    NCJnew(NCJ_DICT,&jatts);
    NCJnew(NCJ_DICT,&jtypes);
    if((stat = ncz_sync_atts(file,(NC_OBJ*)var, var->att, jatts, jtypes, isclose))) goto done;

    if(!purezarr && jncvar != NULL) {
        /* Insert _nczarr_array */
        if((stat=insert_attr(jatts,jtypes,NCZ_V2_ARRAY,jncvar,"|J0"))) goto done;
	jncvar = NULL;
    }

    /* As a last mod to jatts, optionally insert the jtypes as an attribute and add _nczarr_attr as attribute*/
    if(!purezarr && jtypes != NULL) {
	if((stat = insert_nczarr_attr(jatts,jtypes))) goto done;
	jtypes = NULL;
    }

    /* Write out the .zattrs */
    nullfree(key);
    key = NULL;
    NCZ_varkey(var, &key);
    if((stat = NCZMD_update_json_attrs(zinfo, key, jatts))) goto done;

    var->created = 1;

done:
    nclistfreeall(dimrefs);
    nullfree(key);
    nullfree(dtypename);
    nullfree(dimpath);
    NCJreclaim(jvar);
    NCJreclaim(jncvar);
    NCJreclaim(jtmp);
    NCJreclaim(jfill);
    NCJreclaim(jatts);
    NCJreclaim(jtypes);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize variable meta data and data from memory to map.
 *
 * @param file Pointer to file struct
 * @param var Pointer to var struct
 * @param isclose If this called as part of nc_close() as opposed to nc_enddef().
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
ncz_sync_var(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int isclose)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar = var->format_var_info;

    ZTRACE(3,"file=%s var=%s isclose=%d",file->controller->path,var->hdr.name,isclose);

    if(isclose) {
	if((stat = ncz_sync_var_meta(file,var,isclose))) goto done;
    }

    /* flush only chunks that have been written */
    if(zvar->cache) {
        if((stat = NCZ_flush_chunk_cache(zvar->cache)))
	    goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}


/*
Flush all chunks to disk. Create any that are missing
and fill as needed.
*/
int
ncz_write_var(NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)var->format_var_info;

    ZTRACE(3,"var=%s",var->hdr.name);

    /* Flush the cache */
    if(zvar->cache) {
        if((stat = NCZ_flush_chunk_cache(zvar->cache))) goto done;
    }

#ifdef FILLONCLOSE
    /* If fill is enabled, then create missing chunks */
    if(!var->no_fill) {
        int i;
    NCZOdometer* chunkodom =  NULL;
    NC_FILE_INFO_T* file = var->container->nc4_info;
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZMAP* map = zfile->map;
    size64_t start[NC_MAX_VAR_DIMS];
    size64_t stop[NC_MAX_VAR_DIMS];
    size64_t stride[NC_MAX_VAR_DIMS];
    char* key = NULL;

    if(var->ndims == 0) { /* scalar */
	start[i] = 0;
	stop[i] = 1;
        stride[i] = 1;
    } else {
        for(i=0;i<var->ndims;i++) {
	    size64_t nchunks = ceildiv(var->dim[i]->len,var->chunksizes[i]);
	    start[i] = 0;
	    stop[i] = nchunks;
	    stride[i] = 1;
        }
    }

    {
	if(zvar->scalar) {
	    if((chunkodom = nczodom_new(1,start,stop,stride,stop))==NULL)
	} else {
	    /* Iterate over all the chunks to create missing ones */
	    if((chunkodom = nczodom_new(var->ndims,start,stop,stride,stop))==NULL)
	        {stat = NC_ENOMEM; goto done;}
	}
	for(;nczodom_more(chunkodom);nczodom_next(chunkodom)) {
	    size64_t* indices = nczodom_indices(chunkodom);
	    /* Convert to key */
	    if((stat = NCZ_buildchunkpath(zvar->cache,indices,&key))) goto done;
	    switch (stat = nczmap_exists(map,key)) {
	    case NC_NOERR: goto next; /* already exists */
	    case NC_EEMPTY: break; /* does not exist, create it with fill */
	    default: goto done; /* some other error */
	    }
            /* If we reach here, then chunk does not exist, create it with fill */
	    assert(zvar->cache->fillchunk != NULL);
	    if((stat=nczmap_write(map,key,0,zvar->cache->chunksize,zvar->cache->fillchunk))) goto done;
next:
	    nullfree(key);
	    key = NULL;
	}
    }
    nczodom_free(chunkodom);
    nullfree(key);
    }
#endif /*FILLONCLOSE*/

done:
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Synchronize attribute data from memory to map.
 *
 * @param file
 * @param container Pointer to grp|var struct containing the attributes
 * @param attlist
 * @param jattsp
 * @param jtypesp
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_sync_atts(NC_FILE_INFO_T* file, NC_OBJ* container, NCindex* attlist, NCjson* jatts, NCjson* jtypes, int isclose)
{
    int stat = NC_NOERR;
    size_t i;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCjson* jdimrefs = NULL;
    NCjson* jdict = NULL;
    NCjson* jint = NULL;
    NCjson* jdata = NULL;
    char* key = NULL;
    char* content = NULL;
    char* dimpath = NULL;
    int isxarray = 0;
    int inrootgroup = 0;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    char* tname = NULL;
    int purezarr = 0;
    int endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);

    NC_UNUSED(isclose);
    
    LOG((3, "%s", __func__));
    ZTRACE(3,"file=%s container=%s |attlist|=%u",file->controller->path,container->name,(unsigned)ncindexsize(attlist));
    
    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
	if(var->container && var->container->parent == NULL)
	    inrootgroup = 1;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
    }
    
    zinfo = file->format_file_info;
    purezarr = (zinfo->controls.flags & FLAG_PUREZARR)?1:0;
    if(zinfo->controls.flags & FLAG_XARRAYDIMS) isxarray = 1;

    if(ncindexsize(attlist) > 0) {
        /* Walk all the attributes convert to json and collect the dtype */
        for(i=0;i<ncindexsize(attlist);i++) {
	    NC_ATT_INFO_T* a = (NC_ATT_INFO_T*)ncindexith(attlist,i);
	    size_t typesize = 0;
	    if(a->nc_typeid > NC_MAX_ATOMIC_TYPE)
	        {stat = (THROW(NC_ENCZARR)); goto done;}
	    if(a->nc_typeid == NC_STRING)
	        typesize = (size_t)NCZ_get_maxstrlen(container);
	    else
	        {if((stat = NC4_inq_atomic_type(a->nc_typeid,NULL,&typesize))) goto done;}
	    /* Convert to storable json */
	    if((stat = NCZ_stringconvert(a->nc_typeid,a->len,a->data,&jdata))) goto done;

	    /* Collect the corresponding dtype */
            if((stat = ncz_nctype2dtype(a->nc_typeid,endianness,purezarr,typesize,&tname))) goto done;

	    /* Insert the attribute; consumes jdata */
	    if((stat = insert_attr(jatts,jtypes,a->hdr.name, jdata, tname))) goto done;

	    /* cleanup */
            nullfree(tname); tname = NULL;
	    jdata = NULL;

        }
    }
    /* Construct container path */
    if(container->sort == NCGRP)
	stat = NCZ_grpkey(grp,&key);
    else
	stat = NCZ_varkey(var,&key);
    if(stat)
	goto done;

    if(container->sort == NCVAR) { 
        if(isxarray) {
	    /* Optionally insert XARRAY-related attributes:
		- XARRAYSCALAR
		- _ARRAY_ATTRIBUTE
	    */
	    NCJnew(NCJ_ARRAY,&jdimrefs);
	    /* Fake the scalar case */
	    if(var->ndims == 0)
	        NCJaddstring(jdimrefs,NCJ_STRING,XARRAYSCALAR);
	    /* Walk the dimensions and capture the names */
	    for(i=0;i<var->ndims;i++) {
		char* dimname;
	        NC_DIM_INFO_T* dim = var->dim[i];
		dimname = strdup(dim->hdr.name);
		if(dimname == NULL) {stat = NC_ENOMEM; goto done;}
	        NCJaddstring(jdimrefs,NCJ_STRING,dimname);
   	        nullfree(dimname); dimname = NULL;
	    }
	    /* Add the _ARRAY_DIMENSIONS attribute */
	    if((stat = NCJinsert(jatts,NC_XARRAY_DIMS,jdimrefs))<0) {stat = NC_EINVAL; goto done;}
	    jdimrefs = NULL;
        }
    }
    /* Add Quantize Attribute */
    if(container->sort == NCVAR && var && var->quantize_mode > 0) {    
	char mode[64];
	snprintf(mode,sizeof(mode),"%d",var->nsd);
        NCJnewstring(NCJ_INT,mode,&jint);
	/* Insert the quantize attribute */
	switch (var->quantize_mode) {
	case NC_QUANTIZE_BITGROOM:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITGROOM_ATT_NAME,jint))<0) {stat = NC_EINVAL; goto done;}	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_GRANULARBR:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_GRANULARBR_ATT_NAME,jint))<0) {stat = NC_EINVAL; goto done;}	
	    jint = NULL;
	    break;
	case NC_QUANTIZE_BITROUND:
	    if((stat = NCJinsert(jatts,NC_QUANTIZE_BITROUND_ATT_NAME,jint))<0) {stat = NC_EINVAL; goto done;}	
	    jint = NULL;
	    break;
	default: break;
	}
    }

done:
    nullfree(key);
    nullfree(content);
    nullfree(dimpath);
    nullfree(tname);
    NCJreclaim(jdimrefs);
    NCJreclaim(jdict);
    NCJreclaim(jint);
    NCJreclaim(jdata);
    return ZUNTRACE(THROW(stat));
}


/**************************************************/

/**
@internal Extract attributes from a group or var and return
the corresponding NCjson dict.
@param map - [in] the map object for storage
@param container - [in] the containing object
@param jattsp    - [out] the json for .zattrs || NULL if not found
@param jtypesp   - [out] the json attribute type dict || NULL
@param jnczgrp   - [out] the json for _nczarr_group || NULL
@param jnczarray - [out] the json for _nczarr_array || NULL
@return NC_NOERR
@return NC_EXXX
@author Dennis Heimbigner
*/
static int
download_jatts(NC_FILE_INFO_T* file, NC_OBJ* container, const NCjson** jattsp, const NCjson** jtypesp)
{
    int stat = NC_NOERR;
    const NCjson* jatts = NULL;
    const NCjson* jtypes = NULL;
    const NCjson* jnczattr = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_GRP_INFO_T* zgrp = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    int purezarr = 0;
    int zarrkey = 0;

    ZTRACE(3,"container=%s ",container->name);

    purezarr = (zinfo->controls.flags & FLAG_PUREZARR)?1:0;
    zarrkey = (zinfo->controls.flags & FLAG_NCZARR_KEY)?1:0;

    if(container->sort == NCGRP) {
	grp = (NC_GRP_INFO_T*)container;
	zgrp = (NCZ_GRP_INFO_T*)grp->format_grp_info;	
	jatts = zgrp->zgroup.atts;
    } else {
	var = (NC_VAR_INFO_T*)container;
	zvar = (NCZ_VAR_INFO_T*)var->format_var_info;
	jatts = zvar->zarray.atts;
    }
    assert(purezarr || zarrkey || jatts != NULL);

    if(jatts != NULL) {
	/* Get _nczarr_attr from .zattrs */
        if((stat = NCJdictget(jatts,NCZ_V2_ATTR,&jnczattr))<0) {stat = NC_EINVAL; goto done;}
	if(jnczattr != NULL) {
	    /* jnczattr attribute should be a dict */
	    if(NCJsort(jnczattr) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	    /* Extract "types"; may not exist if only hidden attributes are defined */
	    if((stat = NCJdictget(jnczattr,"types",&jtypes))<0) {stat = NC_EINVAL; goto done;}
	    if(jtypes != NULL) {
	        if(NCJsort(jtypes) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	    }
	}
    }
    if(jattsp) {*jattsp = jatts; jatts = NULL;}
    if(jtypes) {*jtypesp = jtypes; jtypes = NULL;}

done:
    return ZUNTRACE(THROW(stat));
}

/* Convert a JSON singleton or array of strings to a single string */
static int
zcharify(const NCjson* src, NCbytes* buf)
{
    int stat = NC_NOERR;
    size_t i;
    struct NCJconst jstr;

    memset(&jstr,0,sizeof(jstr));

    if(NCJsort(src) != NCJ_ARRAY) { /* singleton */
        if((stat = NCJcvt(src, NCJ_STRING, &jstr))<0) {stat = NC_EINVAL; goto done;}
        ncbytescat(buf,jstr.sval);
    } else for(i=0;i<NCJarraylength(src);i++) {
	NCjson* value = NCJith(src,i);
	if((stat = NCJcvt(value, NCJ_STRING, &jstr))<0) {stat = NC_EINVAL; goto done;}
	ncbytescat(buf,jstr.sval);
        nullfree(jstr.sval);jstr.sval = NULL;
    }
done:
    nullfree(jstr.sval);
    return stat;
}

/* Convert a json value to actual data values of an attribute. */
static int
zconvert(const NCjson* src, nc_type typeid, size_t typelen, int* countp, NCbytes* dst)
{
    int stat = NC_NOERR;
    int i;
    int count = 0;
    
    ZTRACE(3,"src=%s typeid=%d typelen=%u",NCJtotext(src,0),typeid,typelen);
	    
    /* 3 cases:
       (1) singleton atomic value
       (2) array of atomic values
       (3) other JSON expression
    */
    switch (NCJsort(src)) {
    case NCJ_INT: case NCJ_DOUBLE: case NCJ_BOOLEAN: /* case 1 */
	count = 1;
	if((stat = NCZ_convert1(src, typeid, dst)))
	    goto done;
	break;

    case NCJ_ARRAY:
        if(typeid == NC_CHAR) {
	    if((stat = zcharify(src,dst))) goto done;
	    count = ncbyteslength(dst);
        } else {
	    count = NCJarraylength(src);
	    for(i=0;i<count;i++) {
	        NCjson* value = NCJith(src,i);
                if((stat = NCZ_convert1(value, typeid, dst))) goto done;
	    }
	}
	break;
    case NCJ_STRING:
	if(typeid == NC_CHAR) {
	    if((stat = zcharify(src,dst))) goto done;
	    count = ncbyteslength(dst);
	    /* Special case for "" */
	    if(count == 0) {
	        ncbytesappend(dst,'\0');
	        count = 1;
	    }
	} else {
	    if((stat = NCZ_convert1(src, typeid, dst))) goto done;
	    count = 1;
	}
	break;
    default: stat = (THROW(NC_ENCZARR)); goto done;
    }
    if(countp) *countp = count;

done:
    return ZUNTRACE(THROW(stat));
}

/*
Extract type and data for an attribute
*/
static int
computeattrinfo(const char* name, const NCjson* jtypes, nc_type typehint, int purezarr, NCjson* values,
		nc_type* typeidp, size_t* typelenp, size_t* lenp, void** datap)
{
    int stat = NC_NOERR;
    size_t i;
    size_t len, typelen;
    void* data = NULL;
    nc_type typeid;

    ZTRACE(3,"name=%s typehint=%d purezarr=%d values=|%s|",name,typehint,purezarr,NCJtotext(values,0));

    /* Get type info for the given att */
    typeid = NC_NAT;
    for(i=0;i<NCJdictlength(jtypes);i++) {
	NCjson* akey = NCJdictkey(jtypes,i);
	if(strcmp(NCJstring(akey),name)==0) {
	    const NCjson* avalue = NULL;
	    NCJdictget(jtypes,NCJstring(akey),&avalue);
	    if((stat = ncz_dtype2nctype(NCJstring(avalue),typehint,purezarr,&typeid,NULL,NULL))) goto done;
//		if((stat = ncz_nctypedecode(atype,&typeid))) goto done;
	    break;
	}
    }
    if(typeid > NC_MAX_ATOMIC_TYPE)
	{stat = NC_EINTERNAL; goto done;}
    /* Use the hint if given one */
    if(typeid == NC_NAT)
        typeid = typehint;

    if((stat = computeattrdata(typehint, &typeid, values, &typelen, &len, &data))) goto done;

    if(typeidp) *typeidp = typeid;
    if(lenp) *lenp = len;
    if(typelenp) *typelenp = typelen;
    if(datap) {*datap = data; data = NULL;}

done:
    nullfree(data);
    return ZUNTRACEX(THROW(stat),"typeid=%d typelen=%d len=%u",*typeidp,*typelenp,*lenp);
}

/*
Extract data for an attribute
*/
static int
computeattrdata(nc_type typehint, nc_type* typeidp, const NCjson* values, size_t* typelenp, size_t* countp, void** datap)
{
    int stat = NC_NOERR;
    NCbytes* buf = ncbytesnew();
    size_t typelen;
    nc_type typeid = NC_NAT;
    NCjson* jtext = NULL;
    int reclaimvalues = 0;
    int isjson = 0; /* 1 => attribute value is neither scalar nor array of scalars */
    int count = 0; /* no. of attribute values */

    ZTRACE(3,"typehint=%d typeid=%d values=|%s|",typehint,*typeidp,NCJtotext(values,0));

    /* Get assumed type */
    if(typeidp) typeid = *typeidp;
    if(typeid == NC_NAT && !isjson) {
        if((stat = NCZ_inferattrtype(values,typehint, &typeid))) goto done;
    }

    /* See if this is a simple vector (or scalar) of atomic types */
    isjson = NCZ_iscomplexjson(values,typeid);

    if(isjson) {
	/* Apply the JSON attribute convention and convert to JSON string */
	typeid = NC_CHAR;
	if((stat = json_convention_read(values,&jtext))) goto done;
	values = jtext; jtext = NULL;
	reclaimvalues = 1;
    } 

    if((stat = NC4_inq_atomic_type(typeid, NULL, &typelen)))
        goto done;

    /* Convert the JSON attribute values to the actual netcdf attribute bytes */
    if((stat = zconvert(values,typeid,typelen,&count,buf))) goto done;

    if(typelenp) *typelenp = typelen;
    if(typeidp) *typeidp = typeid; /* return possibly inferred type */
    if(countp) *countp = (size_t)count;
    if(datap) *datap = ncbytesextract(buf);

done:
    ncbytesfree(buf);
    if(reclaimvalues) NCJreclaim((NCjson*)values); /* we created it */
    return ZUNTRACEX(THROW(stat),"typelen=%d count=%u",(typelenp?*typelenp:0),(countp?*countp:-1));
}

/**
 * @internal Read file data from map to memory.
 *
 * @param file Pointer to file info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
ncz_read_file(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    LOG((3, "%s: file: %s", __func__, file->controller->path));
    ZTRACE(3,"file=%s",file->controller->path);
    
    /* _nczarr should already have been read in ncz_open_dataset */

    /* Now load the groups starting with root */
    if((stat = define_grp(file,file->root_grp)))
	goto done;

done:
    NCJreclaim(json);
    return ZUNTRACE(stat);
}

/**
 * @internal Read group data from map to memory
 *
 * @param file Pointer to file struct
 * @param grp Pointer to grp struct
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
define_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NCZ_GRP_INFO_T* zgrp = NULL;
    char* key = NULL;
    NCjson* json = NULL;
    const NCjson* jgroup = NULL;
    const NCjson* jattrs = NULL;
    const NCjson* jnczgrp = NULL;
    NClist* dimdefs = nclistnew();
    NClist* varnames = nclistnew();
    NClist* subgrps = nclistnew();
    int purezarr = 0;

    LOG((3, "%s: dims: %s", __func__, key));
    ZTRACE(3,"file=%s grp=%s",file->controller->path,grp->hdr.name);
    
    zinfo = file->format_file_info;
    zgrp = grp->format_grp_info;

    purezarr = (zinfo->controls.flags & FLAG_PUREZARR)?1:0;

    /* Construct grp path */
    if((stat = NCZ_grpkey(grp,&key))) goto done;

    /* Download .zgroup and .zattrs */
    nullfree(zgrp->zgroup.prefix);
    zgrp->zgroup.prefix = strdup(key);
    NCJreclaim(zgrp->zgroup.obj);
    NCJreclaim(zgrp->zgroup.atts);
    if ((stat = NCZMD_fetch_json_group(zinfo, key, &zgrp->zgroup.obj)) \
    || (stat = NCZMD_fetch_json_attrs(zinfo, key, &zgrp->zgroup.atts))) {
        goto done;
    }
    jgroup = zgrp->zgroup.obj;
    jattrs = zgrp->zgroup.atts;

    if(purezarr) {
	if((stat = parse_group_content_pure(zinfo,grp,varnames,subgrps)))
	    goto done;
        purezarr = 1;
    } else { /*!purezarr*/
	if(jgroup == NULL) { /* does not exist, use search */
            if((stat = parse_group_content_pure(zinfo,grp,varnames,subgrps))) goto done;
	    purezarr = 1;
	}
	if(jattrs == NULL)  { /* does not exist, use search */
            if((stat = parse_group_content_pure(zinfo,grp,varnames,subgrps))) goto done;
	    purezarr = 1;
	} else { /* Extract the NCZ_V2_GROUP attribute*/
            if((stat = getnczarrkey((NC_OBJ*)grp,NCZ_V2_GROUP,&jnczgrp))) goto done;
	}
	nullfree(key); key = NULL;
	if(jnczgrp) {
            /* Pull out lists about group content */
	    if((stat = parse_group_content(jnczgrp,dimdefs,varnames,subgrps)))
	        goto done;
	}
    }

    if(!purezarr) {
	/* Define dimensions */
	if((stat = define_dims(file,grp,dimdefs))) goto done;
    }

    /* Define vars taking xarray into account */
    if((stat = define_vars(file,grp,varnames))) goto done;

    /* Define sub-groups */
    if((stat = define_subgrps(file,grp,subgrps))) goto done;

done:
    NCJreclaim(json);
    nclistfreeall(dimdefs);
    nclistfreeall(varnames);
    nclistfreeall(subgrps);
    nullfree(key);
    return ZUNTRACE(stat);
}


/**
@internal Read attributes from a group or var and create a list
of annotated NC_ATT_INFO_T* objects. This will process
_NCProperties attribute specially.
@param zfile - [in] the containing file (annotation)
@param container - [in] the containing object
@return NC_NOERR
@author Dennis Heimbigner
*/
int
ncz_read_atts(NC_FILE_INFO_T* file, NC_OBJ* container)
{
    int stat = NC_NOERR;
    size_t i;
    char* fullpath = NULL;
    char* key = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NCZ_GRP_INFO_T* zgrp = NULL;
    NC_ATT_INFO_T* att = NULL;
    NCindex* attlist = NULL;
    nc_type typeid;
    size_t len, typelen;
    void* data = NULL;
    NC_ATT_INFO_T* fillvalueatt = NULL;
    nc_type typehint = NC_NAT;
    int purezarr,zarrkeys;
    const NCjson* jattrs = NULL;
    const NCjson* jtypes = NULL;
    struct ZARROBJ* zobj = NULL;

    ZTRACE(3,"file=%s container=%s",file->controller->path,container->name);

    zinfo = file->format_file_info;
    purezarr = (zinfo->controls.flags & FLAG_PUREZARR)?1:0;
    zarrkeys = (zinfo->controls.flags & FLAG_NCZARR_KEY)?1:0;
 
    if(container->sort == NCGRP) {	
	grp = ((NC_GRP_INFO_T*)container);
	attlist =  grp->att;
        zgrp = (NCZ_GRP_INFO_T*)(grp->format_grp_info);
	zobj = &zgrp->zgroup;
    } else {
	var = ((NC_VAR_INFO_T*)container);
	attlist =  var->att;
        zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
	zobj = &zvar->zarray;
    }
    assert(purezarr || zarrkeys || zobj->obj != NULL);

    if((stat = download_jatts(file, container, &jattrs, &jtypes))) goto done;

    if(jattrs != NULL) {
	/* Iterate over the attributes to create the in-memory attributes */
	/* Watch for special cases: _FillValue and  _ARRAY_DIMENSIONS (xarray), etc. */
	for(i=0;i<NCJdictlength(jattrs);i++) {
	    NCjson* key = NCJdictkey(jattrs,i);
	    NCjson* value = NCJdictvalue(jattrs,i);
	    const NC_reservedatt* ra = NULL;
	    int isfillvalue = 0;
    	    int isdfaltmaxstrlen = 0;
       	    int ismaxstrlen = 0;
	    const char* aname = NCJstring(key);
	    /* See if this is a notable attribute */
	    if(var != NULL && strcmp(aname,NC_ATT_FILLVALUE)==0) isfillvalue = 1;
	    if(grp != NULL && grp->parent == NULL && strcmp(aname,NC_NCZARR_DEFAULT_MAXSTRLEN_ATTR)==0)
	        isdfaltmaxstrlen = 1;
	    if(var != NULL && strcmp(aname,NC_NCZARR_MAXSTRLEN_ATTR)==0)
	        ismaxstrlen = 1;

	    /* See if this is reserved attribute */
	    ra = NC_findreserved(aname);
	    if(ra != NULL) {
		/* case 1: name = _NCProperties, grp=root, varid==NC_GLOBAL */
		if(strcmp(aname,NCPROPS)==0 && grp != NULL && file->root_grp == grp) {
		    /* Setup provenance */
		    if(NCJsort(value) != NCJ_STRING)
			{stat = (THROW(NC_ENCZARR)); goto done;} /*malformed*/
		    if((stat = NCZ_read_provenance(file,aname,NCJstring(value))))
			goto done;
		}
		/* case 2: name = _ARRAY_DIMENSIONS, sort==NCVAR, flags & HIDDENATTRFLAG */
		if(strcmp(aname,NC_XARRAY_DIMS)==0 && var != NULL && (ra->flags & HIDDENATTRFLAG)) {
  	            /* store for later */
		    size_t i;
		    assert(NCJsort(value) == NCJ_ARRAY);
		    if((zvar->xarray = nclistnew())==NULL)
		        {stat = NC_ENOMEM; goto done;}
		    for(i=0;i<NCJarraylength(value);i++) {
			const NCjson* k = NCJith(value,i);
			assert(k != NULL && NCJsort(k) == NCJ_STRING);
			nclistpush(zvar->xarray,strdup(NCJstring(k)));
		    }
		}
		/* case other: if attribute is hidden */
		if(ra->flags & HIDDENATTRFLAG) continue; /* ignore it */
	    }
	    typehint = NC_NAT;
	    if(isfillvalue)
	        typehint = var->type_info->hdr.id ; /* if unknown use the var's type for _FillValue */
	    /* Create the attribute */
	    /* Collect the attribute's type and value  */
	    if((stat = computeattrinfo(aname,jtypes,typehint,purezarr,value,
				   &typeid,&typelen,&len,&data)))
		goto done;
	    if((stat = ncz_makeattr(container,attlist,aname,typeid,len,data,&att)))
		goto done;
	    /* No longer need this copy of the data */
   	    if((stat = NC_reclaim_data_all(file->controller,att->nc_typeid,data,len))) goto done;	    	    
	    data = NULL;
	    if(isfillvalue)
	        fillvalueatt = att;
	    if(ismaxstrlen && att->nc_typeid == NC_INT)
	        zvar->maxstrlen = ((int*)att->data)[0];
	    if(isdfaltmaxstrlen && att->nc_typeid == NC_INT)
	        zinfo->default_maxstrlen = ((int*)att->data)[0];
	}
    }
    /* If we have not read a _FillValue, then go ahead and create it */
    if(fillvalueatt == NULL && container->sort == NCVAR) {
	if((stat = ncz_create_fillvalue((NC_VAR_INFO_T*)container)))
	    goto done;
    }

    /* Remember that we have read the atts for this var or group. */
    if(container->sort == NCVAR)
	((NC_VAR_INFO_T*)container)->atts_read = 1;
    else
	((NC_GRP_INFO_T*)container)->atts_read = 1;

done:
    if(data != NULL)
        stat = NC_reclaim_data(file->controller,att->nc_typeid,data,len);
    nullfree(fullpath);
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/**
 * @internal Materialize dimensions into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param diminfo List of (name,length,isunlimited) triples
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
define_dims(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* diminfo)
{
    size_t i;
    int stat = NC_NOERR;

    ZTRACE(3,"file=%s grp=%s |diminfo|=%u",file->controller->path,grp->hdr.name,nclistlength(diminfo));

    /* Reify each dim in turn */
    for(i = 0; i < nclistlength(diminfo); i+=3) {
	NC_DIM_INFO_T* dim = NULL;
	size64_t len = 0;
	long long isunlim = 0;
	const char* name = nclistget(diminfo,i);
	const char* slen = nclistget(diminfo,i+1);
	const char* sisunlimited = nclistget(diminfo,i+2);

	/* Create the NC_DIM_INFO_T object */
	sscanf(slen,"%lld",&len); /* Get length */
	if(sisunlimited != NULL)
	    sscanf(sisunlimited,"%lld",&isunlim); /* Get unlimited flag */
	else
	    isunlim = 0;
	if((stat = nc4_dim_list_add(grp, name, (size_t)len, -1, &dim)))
	    goto done;
	dim->unlimited = (isunlim ? 1 : 0);
	if((dim->format_dim_info = calloc(1,sizeof(NCZ_DIM_INFO_T))) == NULL)
	    {stat = NC_ENOMEM; goto done;}
	((NCZ_DIM_INFO_T*)dim->format_dim_info)->common.file = file;
    }

done:
    return ZUNTRACE(THROW(stat));
}


/**
 * @internal Materialize single var into memory;
 * Take xarray and purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param varname name of variable in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
define_var1(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* varname)
{
    int stat = NC_NOERR;
    size_t j;
    NCZ_FILE_INFO_T* zinfo = NULL;
    int purezarr = 0;
    int xarray = 0;
    /* per-variable info */
    NC_VAR_INFO_T* var = NULL;
    NCZ_VAR_INFO_T* zvar = NULL;
    const NCjson* jvar = NULL;
    const NCjson* jatts = NULL; /* corresponding to jvar */
    const NCjson* jncvar = NULL;
    const NCjson* jdimrefs = NULL;
    const NCjson* jvalue = NULL;
    char* key = NULL;
    size64_t* shapes = NULL;
    NClist* dimnames = NULL;
    int varsized = 0;
    int suppress = 0; /* Abort processing of this variable */
    nc_type vtype = NC_NAT;
    int vtypelen = 0;
    size_t rank = 0;
    size_t zarr_rank = 0; /* Need to watch out for scalars */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    const NCjson* jfilter = NULL;
    int chainindex = 0;
#endif

    ZTRACE(3,"file=%s grp=%s varname=%s",file->controller->path,grp->hdr.name,varname);

    zinfo = file->format_file_info;

    if(zinfo->controls.flags & FLAG_PUREZARR) purezarr = 1;
    if(zinfo->controls.flags & FLAG_XARRAYDIMS) {xarray = 1;}

    dimnames = nclistnew();

    if((stat = nc4_var_list_add2(grp, varname, &var)))
	goto done;

    /* And its annotation */
    if((zvar = calloc(1,sizeof(NCZ_VAR_INFO_T)))==NULL)
	{stat = NC_ENOMEM; goto done;}
    var->format_var_info = zvar;
    zvar->common.file = file;

    /* pretend it was created */
    var->created = 1;

    /* Indicate we do not have quantizer yet */
    var->quantize_mode = -1;

    /* Construct var path */
    if((stat = NCZ_varkey(var,&key)))
	goto done;

    if ((stat = NCZMD_fetch_json_array(zinfo, key, &zvar->zarray.obj)) \
    || (stat = NCZMD_fetch_json_attrs(zinfo, key, &zvar->zarray.atts))) {
        goto done;
    }
    jvar = zvar->zarray.obj;
    jatts = zvar->zarray.atts;
    assert(jvar == NULL || NCJsort(jvar) == NCJ_DICT);
    assert(jatts == NULL || NCJsort(jatts) == NCJ_DICT);

    /* Verify the format */
    {
	int version;
	if((stat = NCJdictget(jvar,"zarr_format",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	sscanf(NCJstring(jvalue),"%d",&version);
	if(version != zinfo->zarr.zarr_version)
	    {stat = (THROW(NC_ENCZARR)); goto done;}
    }

    /* Set the type and endianness of the variable */
    {
	int endianness;
	if((stat = NCJdictget(jvar,"dtype",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	/* Convert dtype to nc_type + endianness */
	if((stat = ncz_dtype2nctype(NCJstring(jvalue),NC_NAT,purezarr,&vtype,&endianness,&vtypelen)))
	    goto done;
	if(vtype > NC_NAT && vtype <= NC_MAX_ATOMIC_TYPE) {
	    /* Locate the NC_TYPE_INFO_T object */
	    if((stat = ncz_gettype(file,grp,vtype,&var->type_info)))
		goto done;
	} else {stat = NC_EBADTYPE; goto done;}
#if 0 /* leave native in place */
	if(endianness == NC_ENDIAN_NATIVE)
	    endianness = zinfo->native_endianness;
	if(endianness == NC_ENDIAN_NATIVE)
	    endianness = (NC_isLittleEndian()?NC_ENDIAN_LITTLE:NC_ENDIAN_BIG);
	if(endianness == NC_ENDIAN_LITTLE || endianness == NC_ENDIAN_BIG) {
	    var->endianness = endianness;
	} else {stat = NC_EBADTYPE; goto done;}
#else
	var->endianness = endianness;
#endif
	var->type_info->endianness = var->endianness; /* Propagate */
	if(vtype == NC_STRING) {
	    zvar->maxstrlen = vtypelen;
	    vtypelen = sizeof(char*); /* in-memory len */
	    if(zvar->maxstrlen <= 0) zvar->maxstrlen = NCZ_get_maxstrlen((NC_OBJ*)var);
	}
    }

    if(!purezarr) {
	if(jatts == NULL) {stat = NC_ENCZARR; goto done;}
	/* Extract the _NCZARR_ARRAY values */
	/* Do this first so we know about storage esp. scalar */
	/* Extract the NCZ_V2_ARRAY dict */
	if((stat = getnczarrkey((NC_OBJ*)var,NCZ_V2_ARRAY,&jncvar))) goto done;
	if(jncvar == NULL) {stat = NC_ENCZARR; goto done;}
	assert((NCJsort(jncvar) == NCJ_DICT));
	/* Extract scalar flag */
	if((stat = NCJdictget(jncvar,"scalar",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL) {
	    var->storage = NC_CHUNKED;
	    zvar->scalar = 1;
	}
	/* Extract storage flag */
	if((stat = NCJdictget(jncvar,"storage",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL)
	    var->storage = NC_CHUNKED;
	/* Extract dimrefs list	 */
	if((stat = dictgetalt(jncvar,"dimension_references","dimrefs",&jdimrefs))) goto done;
	if(jdimrefs != NULL) { /* Extract the dimref names */
	    assert((NCJsort(jdimrefs) == NCJ_ARRAY));
	    if(zvar->scalar) {
		assert(NCJarraylength(jdimrefs) == 0);	       
	    } else {
		rank = NCJarraylength(jdimrefs);
		for(j=0;j<rank;j++) {
		    const NCjson* dimpath = NCJith(jdimrefs,j);
		    assert(NCJsort(dimpath) == NCJ_STRING);
		    nclistpush(dimnames,strdup(NCJstring(dimpath)));
		}
	    }
	    jdimrefs = NULL; /* avoid double free */
	} /* else  simulate it from the shape of the variable */
    }

    /* Capture dimension_separator (must precede chunk cache creation) */
    {
	NCglobalstate* ngs = NC_getglobalstate();
	assert(ngs != NULL);
	zvar->dimension_separator = 0;
	if((stat = NCJdictget(jvar,"dimension_separator",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL) {
	    /* Verify its value */
	    if(NCJsort(jvalue) == NCJ_STRING && NCJstring(jvalue) != NULL && strlen(NCJstring(jvalue)) == 1)
	       zvar->dimension_separator = NCJstring(jvalue)[0];
	}
	/* If value is invalid, then use global default */
	if(!islegaldimsep(zvar->dimension_separator))
	    zvar->dimension_separator = ngs->zarr.dimension_separator; /* use global value */
	assert(islegaldimsep(zvar->dimension_separator)); /* we are hosed */
    }

    /* fill_value; must precede calls to adjust cache */
    {
	if((stat = NCJdictget(jvar,"fill_value",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue == NULL || NCJsort(jvalue) == NCJ_NULL)
	    var->no_fill = 1;
	else {
	    size_t fvlen;
	    nc_type atypeid = vtype;
	    var->no_fill = 0;
	    if((stat = computeattrdata(var->type_info->hdr.id, &atypeid, jvalue, NULL, &fvlen, &var->fill_value)))
		goto done;
	    assert(atypeid == vtype);
	    /* Note that we do not create the _FillValue
	       attribute here to avoid having to read all
	       the attributes and thus foiling lazy read.*/
	}
    }

    /* shape */
    {
	if((stat = NCJdictget(jvar,"shape",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(NCJsort(jvalue) != NCJ_ARRAY) {stat = (THROW(NC_ENCZARR)); goto done;}
	
	/* Process the rank */
	zarr_rank = NCJarraylength(jvalue);
	if(zarr_rank == 0) {
	    /* suppress variable */
	    ZLOG(NCLOGWARN,"Empty shape for variable %s suppressed",var->hdr.name);
	    suppress = 1;
	    goto suppressvar;
	}

	if(zvar->scalar) {
	    rank = 0;
	    zarr_rank = 1; /* Zarr does not support scalars */
	} else 
	    rank = (zarr_rank = NCJarraylength(jvalue));

	if(zarr_rank > 0) {
	    /* Save the rank of the variable */
	    if((stat = nc4_var_set_ndims(var, rank))) goto done;
	    /* extract the shapes */
	    if((shapes = (size64_t*)malloc(sizeof(size64_t)*(size_t)zarr_rank)) == NULL)
		{stat = (THROW(NC_ENOMEM)); goto done;}
	    if((stat = decodeints(jvalue, shapes))) goto done;
	}
    }

    /* chunks */
    {
	size64_t chunks[NC_MAX_VAR_DIMS];
	if((stat = NCJdictget(jvar,"chunks",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL && NCJsort(jvalue) != NCJ_ARRAY)
	    {stat = (THROW(NC_ENCZARR)); goto done;}
	/* Verify the rank */
	if(zvar->scalar || zarr_rank == 0) {
	    if(var->ndims != 0)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	    zvar->chunkproduct = 1;
	    zvar->chunksize = zvar->chunkproduct * var->type_info->size;
	    /* Create the cache */
	    if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
		goto done;
	} else {/* !zvar->scalar */
	    if(zarr_rank == 0) {stat = NC_ENCZARR; goto done;}
	    var->storage = NC_CHUNKED;
	    if(var->ndims != rank)
		{stat = (THROW(NC_ENCZARR)); goto done;}
	    if((var->chunksizes = malloc(sizeof(size_t)*(size_t)zarr_rank)) == NULL)
		{stat = NC_ENOMEM; goto done;}
	    if((stat = decodeints(jvalue, chunks))) goto done;
	    /* validate the chunk sizes */
	    zvar->chunkproduct = 1;
	    for(j=0;j<rank;j++) {
		if(chunks[j] == 0)
		    {stat = (THROW(NC_ENCZARR)); goto done;}
		var->chunksizes[j] = (size_t)chunks[j];
		zvar->chunkproduct *= chunks[j];
	    }
	    zvar->chunksize = zvar->chunkproduct * var->type_info->size;
	    /* Create the cache */
	    if((stat = NCZ_create_chunk_cache(var,var->type_info->size*zvar->chunkproduct,zvar->dimension_separator,&zvar->cache)))
		goto done;
	}
	if((stat = NCZ_adjust_var_cache(var))) goto done;
    }
    /* Capture row vs column major; currently, column major not used*/
    {
	if((stat = NCJdictget(jvar,"order",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(strcmp(NCJstring(jvalue),"C") > 0)
	    ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 1;
	else ((NCZ_VAR_INFO_T*)var->format_var_info)->order = 0;
    }
    /* filters key */
    /* From V2 Spec: A list of JSON objects providing codec configurations,
       or null if no filters are to be applied. Each codec configuration
       object MUST contain a "id" key identifying the codec to be used. */
    /* Do filters key before compressor key so final filter chain is in correct order */
    {
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
	if(var->filters == NULL) var->filters = (void*)nclistnew();
	if(zvar->incompletefilters == NULL) zvar->incompletefilters = (void*)nclistnew();
	chainindex = 0; /* track location of filter in the chain */
	if((stat = NCZ_filter_initialize())) goto done;
	if((stat = NCJdictget(jvar,"filters",&jvalue))<0) {stat = NC_EINVAL; goto done;}
	if(jvalue != NULL && NCJsort(jvalue) != NCJ_NULL) {
	    int k;
	    if(NCJsort(jvalue) != NCJ_ARRAY) {stat = NC_EFILTER; goto done;}
	    for(k=0;;k++) {
		jfilter = NULL;
		jfilter = NCJith(jvalue,k);
		if(jfilter == NULL) break; /* done */
		if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
		if((stat = NCZ_filter_build(file,var,jfilter,chainindex++))) goto done;
	    }
	}
#endif
    }

    /* compressor key */
    /* From V2 Spec: A JSON object identifying the primary compression codec and providing
       configuration parameters, or ``null`` if no compressor is to be used. */
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    { 
	if(var->filters == NULL) var->filters = (void*)nclistnew();
	if((stat = NCZ_filter_initialize())) goto done;
	if((stat = NCJdictget(jvar,"compressor",&jfilter))<0) {stat = NC_EINVAL; goto done;}
	if(jfilter != NULL && NCJsort(jfilter) != NCJ_NULL) {
	    if(NCJsort(jfilter) != NCJ_DICT) {stat = NC_EFILTER; goto done;}
	    if((stat = NCZ_filter_build(file,var,jfilter,chainindex++))) goto done;
	}
    }
    /* Suppress variable if there are filters and var is not fixed-size */
    if(varsized && nclistlength((NClist*)var->filters) > 0)
	suppress = 1;
#endif
    if(zarr_rank > 0) {
	if((stat = computedimrefs(file, var, purezarr, xarray, rank, dimnames, shapes, var->dim)))
	    goto done;
	if(!zvar->scalar) {
	    /* Extract the dimids */
	    for(j=0;j<rank;j++)
		var->dimids[j] = var->dim[j]->hdr.id;
	}
    }

#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    if(!suppress) {
	/* At this point, we can finalize the filters */
	if((stat = NCZ_filter_setup(var))) goto done;
    }
#endif

suppressvar:
    if(suppress) {
	/* Reclaim NCZarr variable specific info */
	(void)NCZ_zclose_var1(var);
	/* Remove from list of variables and reclaim the top level var object */
	(void)nc4_var_list_del(grp, var);
	var = NULL;
    }

done:
    nclistfreeall(dimnames); dimnames = NULL;
    nullfree(shapes); shapes = NULL;
    nullfree(key); key = NULL;
    return ZUNTRACE(stat);
}

/**
 * @internal Materialize vars into memory;
 * Take xarray and purezarr into account.
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param varnames List of names of variables in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
define_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* varnames)
{
    int stat = NC_NOERR;
    size_t i;

    ZTRACE(3,"file=%s grp=%s |varnames|=%u",file->controller->path,grp->hdr.name,nclistlength(varnames));

    /* Load each var in turn */
    for(i = 0; i < nclistlength(varnames); i++) {
	const char* varname = (const char*)nclistget(varnames,i);
        if((stat = define_var1(file,grp,varname))) goto done;
	varname = nclistget(varnames,i);
    }

done:
    return ZUNTRACE(stat);
}

/**
 * @internal Materialize subgroups into memory
 *
 * @param file Pointer to file info struct.
 * @param grp Pointer to grp info struct.
 * @param subgrpnames List of names of subgroups in this group
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
define_subgrps(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, NClist* subgrpnames)
{
    size_t i;
    int stat = NC_NOERR;

    ZTRACE(3,"file=%s grp=%s |subgrpnames|=%u",file->controller->path,grp->hdr.name,nclistlength(subgrpnames));

    /* Load each subgroup name in turn */
    for(i = 0; i < nclistlength(subgrpnames); i++) {
	NC_GRP_INFO_T* g = NULL;
	const char* gname = nclistget(subgrpnames,i);
	char norm_name[NC_MAX_NAME];
	/* Check and normalize the name. */
	if((stat = nc4_check_name(gname, norm_name)))
	    goto done;
	if((stat = nc4_grp_list_add(file, grp, norm_name, &g)))
	    goto done;
	if(!(g->format_grp_info = calloc(1, sizeof(NCZ_GRP_INFO_T))))
	    {stat = NC_ENOMEM; goto done;}
	((NCZ_GRP_INFO_T*)g->format_grp_info)->common.file = file;
    }

    /* Recurse to fill in subgroups */
    for(i=0;i<ncindexsize(grp->children);i++) {
	NC_GRP_INFO_T* g = (NC_GRP_INFO_T*)ncindexith(grp->children,i);
	if((stat = define_grp(file,g)))
	    goto done;
    }

done:
    return ZUNTRACE(THROW(stat));
}

int
ncz_read_superblock(NC_FILE_INFO_T* file, char** nczarrvp, char** zarrfp)
{
    int stat = NC_NOERR;
    const NCjson* jnczgroup = NULL;
    const NCjson* jnczattr = NULL;
    const NCjson* jzgroup = NULL;
    const NCjson* jsuper = NULL;
    const NCjson* jtmp = NULL;
    char* nczarr_version = NULL;
    char* zarr_format = NULL;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NC_GRP_INFO_T* root = NULL;
    NCZ_GRP_INFO_T* zroot = NULL;
    char* key = NULL;

    ZTRACE(3,"file=%s",file->controller->path);

    root = file->root_grp;
    assert(root != NULL);

    zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;    
    zroot = (NCZ_GRP_INFO_T*)root->format_grp_info;    

    /* Construct grp key */
    if((stat = NCZ_grpkey(root,&key))) goto done;

    if((stat = NCZMD_fetch_json_group(zinfo, key, &zroot->zgroup.obj)) \
        || NCZMD_fetch_json_attrs(zinfo, key, &zroot->zgroup.atts)) {
            goto done;
    }
    jzgroup = zroot->zgroup.obj;    

    /* Look for superblock; first in .zattrs and then in .zgroup */
    if((stat = getnczarrkey((NC_OBJ*)root,NCZ_V2_SUPERBLOCK,&jsuper))) goto done;

    /* Set the format flags */

    /* Set where _nczarr_xxx are stored */
    if(jsuper != NULL && zroot->zgroup.nczv1) {
	zinfo->controls.flags |= FLAG_NCZARR_KEY;
	/* Also means file is read only */
	file->no_write = 1;
    }
    
    if(jsuper == NULL) {
	/* See if this is looks like a NCZarr/Zarr dataset at all
           by looking for anything here of the form ".z*" */
        if ((zinfo->metadata.flags & ZARR_CONSOLIDATED) == 0 && (stat = ncz_validate(file))) goto done;
	/* ok, assume pure zarr with no groups */
	zinfo->controls.flags |= FLAG_PUREZARR;	
	if(zarr_format == NULL) zarr_format = strdup("2");
    }

    /* Look for _nczarr_group */
    if((stat = getnczarrkey((NC_OBJ*)root,NCZ_V2_GROUP,&jnczgroup))) goto done;

    /* Look for _nczarr_attr*/
    if((stat = getnczarrkey((NC_OBJ*)root,NCZ_V2_ATTR,&jnczattr))) goto done;

    if(jsuper != NULL) {
	if(jsuper->sort != NCJ_DICT) {stat = NC_ENCZARR; goto done;}
	if((stat = dictgetalt(jsuper,"nczarr_version","version",&jtmp))<0) {stat = NC_EINVAL; goto done;}
	nczarr_version = nulldup(NCJstring(jtmp));
    }

    if(jzgroup != NULL) {
        if(jzgroup->sort != NCJ_DICT) {stat = NC_ENCZARR; goto done;}
        /* In any case, extract the zarr format */
        if((stat = NCJdictget(jzgroup,"zarr_format",&jtmp))<0) {stat = NC_EINVAL; goto done;}
	if(zarr_format == NULL)
	    zarr_format = nulldup(NCJstring(jtmp));
	else if(strcmp(zarr_format,NCJstring(jtmp))!=0)
	    {stat = NC_ENCZARR; goto done;}
    }

    if(nczarrvp) {*nczarrvp = nczarr_version; nczarr_version = NULL;}
    if(zarrfp) {*zarrfp = zarr_format; zarr_format = NULL;}
done:
    nullfree(key);
    nullfree(zarr_format);
    nullfree(nczarr_version);
    return ZUNTRACE(THROW(stat));
}

/**************************************************/
/* Utilities */

static int
parse_group_content(const NCjson* jcontent, NClist* dimdefs, NClist* varnames, NClist* subgrps)
{
    int stat = NC_NOERR;
    size_t i;
    const NCjson* jvalue = NULL;

    ZTRACE(3,"jcontent=|%s| |dimdefs|=%u |varnames|=%u |subgrps|=%u",NCJtotext(jcontent,0),(unsigned)nclistlength(dimdefs),(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    if((stat=dictgetalt(jcontent,"dimensions","dims",&jvalue))) goto done;
    if(jvalue != NULL) {
	if(NCJsort(jvalue) != NCJ_DICT) {stat = (THROW(NC_ENCZARR)); goto done;}
	/* Extract the dimensions defined in this group */
	for(i=0;i<NCJdictlength(jvalue);i++) {
	    const NCjson* jname = NCJdictkey(jvalue,i);
	    const NCjson* jleninfo = NCJdictvalue(jvalue,i);
    	    const NCjson* jtmp = NULL;
       	    const char* slen = "0";
       	    const char* sunlim = "0";
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    /* check the length */
            if(NCJsort(jleninfo) == NCJ_DICT) {
		if((stat = NCJdictget(jleninfo,"size",&jtmp))<0) {stat = NC_EINVAL; goto done;}
		if(jtmp== NULL)
		    {stat = NC_EBADNAME; goto done;}
		slen = NCJstring(jtmp);
		/* See if unlimited */
		if((stat = NCJdictget(jleninfo,"unlimited",&jtmp))<0) {stat = NC_EINVAL; goto done;}
	        if(jtmp == NULL) sunlim = "0"; else sunlim = NCJstring(jtmp);
            } else if(jleninfo != NULL && NCJsort(jleninfo) == NCJ_INT) {
		slen = NCJstring(jleninfo);		
	    } else
		{stat = NC_ENCZARR; goto done;}
	    nclistpush(dimdefs,strdup(norm_name));
	    nclistpush(dimdefs,strdup(slen));
    	    nclistpush(dimdefs,strdup(sunlim));
	}
    }

    if((stat=dictgetalt(jcontent,"arrays","vars",&jvalue))) goto done;
    if(jvalue != NULL) {
	/* Extract the variable names in this group */
	for(i=0;i<NCJarraylength(jvalue);i++) {
	    NCjson* jname = NCJith(jvalue,i);
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    nclistpush(varnames,strdup(norm_name));
	}
    }

    if((stat = NCJdictget(jcontent,"groups",&jvalue))<0) {stat = NC_EINVAL; goto done;}
    if(jvalue != NULL) {
	/* Extract the subgroup names in this group */
	for(i=0;i<NCJarraylength(jvalue);i++) {
	    NCjson* jname = NCJith(jvalue,i);
	    char norm_name[NC_MAX_NAME + 1];
	    /* Verify name legality */
	    if((stat = nc4_check_name(NCJstring(jname), norm_name)))
		{stat = NC_EBADNAME; goto done;}
	    nclistpush(subgrps,strdup(norm_name));
	}
    }

done:
    return ZUNTRACE(THROW(stat));
}

static int
parse_group_content_pure(NCZ_FILE_INFO_T*  zinfo, NC_GRP_INFO_T* grp, NClist* varnames, NClist* subgrps)
{
    int stat = NC_NOERR;
    char *key = NULL;

    ZTRACE(3,"zinfo=%s grp=%s |varnames|=%u |subgrps|=%u",zinfo->common.file->controller->path,grp->hdr.name,(unsigned)nclistlength(varnames),(unsigned)nclistlength(subgrps));

    if ((stat = NCZ_grpkey(grp,&key))){
        goto done;
    }
    nclistclear(varnames);
    nclistclear(subgrps);
    if((stat = NCZMD_list_nodes(zinfo, key, subgrps, varnames))) goto done;

done:
    nullfree(key);
    return ZUNTRACE(THROW(stat));
}

/* Convert a list of integer strings to 64 bit dimension sizes (shapes) */
static int
decodeints(const NCjson* jshape, size64_t* shapes)
{
    int stat = NC_NOERR;
    size_t i;

    for(i=0;i<NCJarraylength(jshape);i++) {
	struct ZCVT zcvt;
	nc_type typeid = NC_NAT;
	NCjson* jv = NCJith(jshape,i);
	if((stat = NCZ_json2cvt(jv,&zcvt,&typeid))) goto done;
	switch (typeid) {
	case NC_INT64:
	if(zcvt.int64v < 0) {stat = (THROW(NC_ENCZARR)); goto done;}
	    shapes[i] = (size64_t)zcvt.int64v;
	    break;
	case NC_UINT64:
	    shapes[i] = (size64_t)zcvt.uint64v;
	    break;
	default: {stat = (THROW(NC_ENCZARR)); goto done;}
	}
    }

done:
    return THROW(stat);
}

/* This code is a subset of NCZ_def_dim */
static int
createdim(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, const char* name, size64_t dimlen, NC_DIM_INFO_T** dimp)
{
    int stat = NC_NOERR;
    NC_DIM_INFO_T* thed = NULL;
    if(grp == NULL) grp = file->root_grp;

    if((stat = nc4_dim_list_add(grp, name, (size_t)dimlen, -1, &thed)))
        goto done;
    assert(thed != NULL);
    /* Create struct for NCZ-specific dim info. */
    if (!(thed->format_dim_info = calloc(1, sizeof(NCZ_DIM_INFO_T))))
	{stat = NC_ENOMEM; goto done;}
    ((NCZ_DIM_INFO_T*)thed->format_dim_info)->common.file = file;
    *dimp = thed; thed = NULL;
done:
    return stat;
}


/*
Given a list of segments, find corresponding group.
*/
static int
locategroup(NC_FILE_INFO_T* file, size_t nsegs, NClist* segments, NC_GRP_INFO_T** grpp)
{
    size_t i, j;
    int found, stat = NC_NOERR;
    NC_GRP_INFO_T* grp = NULL;

    grp = file->root_grp;
    for(i=0;i<nsegs;i++) {
	const char* segment = nclistget(segments,i);
	char norm_name[NC_MAX_NAME];
	found = 0;
	if((stat = nc4_check_name(segment,norm_name))) goto done;
	for(j=0;j<ncindexsize(grp->children);j++) {
	    NC_GRP_INFO_T* subgrp = (NC_GRP_INFO_T*)ncindexith(grp->children,j);
	    if(strcmp(subgrp->hdr.name,norm_name)==0) {
		grp = subgrp;
		found = 1;
		break;
	    }
	}
	if(!found) {stat = NC_ENOGRP; goto done;}
    }
    /* grp should be group of interest */
    if(grpp) *grpp = grp;

done:
    return THROW(stat);
}

static int
parsedimrefs(NC_FILE_INFO_T* file, NClist* dimnames, size64_t* shape, NC_DIM_INFO_T** dims, int create)
{
    size_t i;
    int stat = NC_NOERR;
    NClist* segments = NULL;

    for(i=0;i<nclistlength(dimnames);i++) {
	NC_GRP_INFO_T* g = NULL;
	NC_DIM_INFO_T* d = NULL;
	size_t j;
	const char* dimpath = nclistget(dimnames,i);
	const char* dimname = NULL;

	/* Locate the corresponding NC_DIM_INFO_T* object */
	nclistfreeall(segments);
	segments = nclistnew();
	if((stat = ncz_splitkey(dimpath,segments)))
	    goto done;
	if((stat=locategroup(file,nclistlength(segments)-1,segments,&g)))
	    goto done;
	/* Lookup the dimension */
	dimname = nclistget(segments,nclistlength(segments)-1);
	d = NULL;
	dims[i] = NULL;
	for(j=0;j<ncindexsize(g->dim);j++) {
	    d = (NC_DIM_INFO_T*)ncindexith(g->dim,j);
	    if(strcmp(d->hdr.name,dimname)==0) {
		dims[i] = d;
		break;
	    }
	}
	if(dims[i] == NULL && create) {
	    /* If not found and create then create it */
	    if((stat = createdim(file, g, dimname, shape[i], &dims[i])))
	        goto done;
	} else {
	    /* Verify consistency */
	    if(dims[i]->len != shape[i])
	        {stat = NC_EDIMSIZE; goto done;}
	}
	assert(dims[i] != NULL);
    }
done:
    nclistfreeall(segments);
    return THROW(stat);
}

/**
 * @internal Get the metadata for a variable.
 *
 * @param var Pointer to var info struct.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_ENOMEM Out of memory.
 * @return ::NC_EHDFERR HDF5 returned error.
 * @return ::NC_EVARMETA Error with var metadata.
 * @author Ed Hartnett
 */
int
ncz_get_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var)
{
    int retval = NC_NOERR;

    assert(file && var && var->format_var_info);
    LOG((3, "%s: var %s", __func__, var->hdr.name));
    ZTRACE(3,"file=%s var=%s",file->controller->path,var->hdr.name);
    
    /* Have we already read the var metadata? */
    if (var->meta_read)
	goto done;

#ifdef LOOK
    /* Get the current chunk cache settings. */
    if ((access_pid = H5Dget_access_plist(hdf5_var->hdf_datasetid)) < 0)
	BAIL(NC_EVARMETA);

    /* Learn about current chunk cache settings. */
    if ((H5Pget_chunk_cache(access_pid, &(var->chunk_cache_nelems),
			    &(var->chunk_cache_size), &rdcc_w0)) < 0)
	BAIL(NC_EHDFERR);
    var->chunk_cache_preemption = rdcc_w0;

    /* Get the dataset creation properties. */
    if ((propid = H5Dget_create_plist(hdf5_var->hdf_datasetid)) < 0)
	BAIL(NC_EHDFERR);

    /* Get var chunking info. */
    if ((retval = get_chunking_info(propid, var)))
	BAIL(retval);

    /* Get filter info for a var. */
    if ((retval = get_filter_info(propid, var)))
	BAIL(retval);

    /* Get fill value, if defined. */
    if ((retval = get_fill_info(propid, var)))
	BAIL(retval);

    /* Is this a deflated variable with a chunksize greater than the
     * current cache size? */
    if ((retval = nc4_adjust_var_cache(var)))
	BAIL(retval);

    /* Is there an attribute which means quantization was used? */
    if ((retval = get_quantize_info(var)))
	BAIL(retval);

    if (var->coords_read && !var->dimscale)
	if ((retval = get_attached_info(var, hdf5_var, var->ndims, hdf5_var->hdf_datasetid)))
	    goto done;;
#endif

    /* Remember that we have read the metadata for this var. */
    var->meta_read = NC_TRUE;
done:
    return ZUNTRACE(retval);
}

/* Compute the set of dim refs for this variable, taking purezarr and xarray into account */
static int
computedimrefs(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, int purezarr, int xarray, int ndims, NClist* dimnames, size64_t* shapes, NC_DIM_INFO_T** dims)
{
    int stat = NC_NOERR;
    size_t i;
    int createdims = 0; /* 1 => we need to create the dims in current group
                                if they do not already exist */
    NCZ_FILE_INFO_T* zfile = (NCZ_FILE_INFO_T*)file->format_file_info;
    NCZ_VAR_INFO_T* zvar = (NCZ_VAR_INFO_T*)(var->format_var_info);
    NCjson* jatts = NULL;
    NC_GRP_INFO_T* grp = var->container; /* Immediate parent group for var */
    char* grpfqn = NULL; /* FQN for grp */

    ZTRACE(3,"file=%s var=%s purezarr=%d xarray=%d ndims=%d shape=%s",
    	file->controller->path,var->hdr.name,purezarr,xarray,(int)ndims,nczprint_vector(ndims,shapes));
    assert(zfile && zvar);

    if(purezarr && xarray) {/* Read in the attributes to get xarray dimdef attribute; Note that it might not exist */
	/* Note that if xarray && !purezarr, then xarray will be superseded by the nczarr dimensions key */
        char zdimname[4096];
	if(zvar->xarray == NULL) {
	    assert(nclistlength(dimnames) == 0);
	    if((stat = ncz_read_atts(file,(NC_OBJ*)var))) goto done;
	}
	if(zvar->xarray != NULL) {
	    size_t len;
	    /* get the FQN of the current group */
	    if((stat = NCZ_grpkey(grp, &grpfqn))) goto done;
	    assert(grpfqn != NULL);
	    len = strlen(grpfqn);
	    assert(len > 0);
	    /* supress any trailing '/' */
	    if(grpfqn[len - 1] == '/') grpfqn[len -1] = '\0';
	    /* convert xarray to FQN dimnames */
	    for(i=0;i<nclistlength(zvar->xarray);i++) {
	        snprintf(zdimname,sizeof(zdimname),"%s/%s",grpfqn,(const char*)nclistget(zvar->xarray,i));
	        nclistpush(dimnames,strdup(zdimname));
	    }
	}
	createdims = 1; /* may need to create them */
    }

    /* If pure zarr and we have no dimref names, then fake it */
    /* Note that to avoid creating duplicates of anonymous dims, we create dim in root group */
    if(purezarr && nclistlength(dimnames) == 0) {
	int i;
	createdims = 1;
        for(i=0;i<ndims;i++) {
	    /* Compute the set of absolute paths to dimrefs */
            char zdimname[4096];
	    snprintf(zdimname,sizeof(zdimname),"/%s_%llu",NCDIMANON,shapes[i]);
	    nclistpush(dimnames,strdup(zdimname));
	}
    }

    /* Now, use dimnames to get the dims; create if necessary */
    if((stat = parsedimrefs(file,dimnames,shapes,dims,createdims)))
        goto done;

done:
    nullfree(grpfqn);
    NCJreclaim(jatts);
    return ZUNTRACE(THROW(stat));
}

/**
Implement the JSON convention:
Stringify it as the value and make the attribute be of type "char".
*/

static int
json_convention_read(const NCjson* json, NCjson** jtextp)
{
    int stat = NC_NOERR;
    NCjson* jtext = NULL;
    char* text = NULL;

    if(json == NULL) {stat = NC_EINVAL; goto done;}
    if(NCJunparse(json,0,&text)) {stat = NC_EINVAL; goto done;}
    NCJnewstring(NCJ_STRING,text,&jtext);
    *jtextp = jtext; jtext = NULL;
done:
    NCJreclaim(jtext);
    nullfree(text);
    return stat;
}

#if 0
/**
Implement the JSON convention:
Parse it as JSON and use that as its value in .zattrs.
*/
static int
json_convention_write(size_t len, const void* data, NCjson** jsonp, int* isjsonp)
{
    int stat = NC_NOERR;
    NCjson* jexpr = NULL;
    int isjson = 0;

    assert(jsonp != NULL);
    if(NCJparsen(len,(char*)data,0,&jexpr)) {
	/* Ok, just treat as sequence of chars */
	NCJnewstringn(NCJ_STRING, len, data, &jexpr);
    }
    isjson = 1;
    *jsonp = jexpr; jexpr = NULL;
    if(isjsonp) *isjsonp = isjson;
done:
    NCJreclaim(jexpr);
    return stat;
}
#endif

#if 0
/* Convert an attribute "types list to an envv style list */
static int
jtypes2atypes(NCjson* jtypes, NClist* atypes)
{
    int stat = NC_NOERR;
    size_t i;
    for(i=0;i<NCJdictlength(jtypes);i++) {
	const NCjson* key = NCJdictkey(jtypes,i);
	const NCjson* value = NCJdictvalue(jtypes,i);
	if(NCJsort(key) != NCJ_STRING) {stat = (THROW(NC_ENCZARR)); goto done;}
	if(NCJsort(value) != NCJ_STRING) {stat = (THROW(NC_ENCZARR)); goto done;}
	nclistpush(atypes,strdup(NCJstring(key)));
	nclistpush(atypes,strdup(NCJstring(value)));
    }
done:
    return stat;
}
#endif

/* See if there is reason to believe the specified path is a legitimate (NC)Zarr file
 * Do a breadth first walk of the tree starting at file path.
 * @param file to validate
 * @return NC_NOERR if it looks ok
 * @return NC_ENOTNC if it does not look ok
 */
static int
ncz_validate(NC_FILE_INFO_T* file)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = (NCZ_FILE_INFO_T*)file->format_file_info;
    int validate = 0;
    NCbytes* prefix = ncbytesnew();
    NClist* queue = nclistnew();
    NClist* nextlevel = nclistnew();
    NCZMAP* map = zinfo->map;
    char* path = NULL;
    char* segment = NULL;
    size_t seglen;
	    
    ZTRACE(3,"file=%s",file->controller->path);

    path = strdup("/");
    nclistpush(queue,path);
    path = NULL;
    do {
        nullfree(path); path = NULL;
	/* This should be full path key */
	path = nclistremove(queue,0); /* remove from front of queue */
	/* get list of next level segments (partial keys) */
	assert(nclistlength(nextlevel)==0);
        if((stat=nczmap_search(map,path,nextlevel))) {validate = 0; goto done;}
        /* For each s in next level, test, convert to full path, and push onto queue */
	while(nclistlength(nextlevel) > 0) {
            segment = nclistremove(nextlevel,0);
            seglen = nulllen(segment);
	    if((seglen >= 2 && memcmp(segment,".z",2)==0) || (seglen >= 4 && memcmp(segment,".ncz",4)==0)) {
		validate = 1;
	        goto done;
	     }
	     /* Convert to full path */
	     ncbytesclear(prefix);
	     ncbytescat(prefix,path);
	     if(strlen(path) > 1) ncbytescat(prefix,"/");
	     ncbytescat(prefix,segment);
	     /* push onto queue */
	     nclistpush(queue,ncbytesextract(prefix));
 	     nullfree(segment); segment = NULL;
	 }
    } while(nclistlength(queue) > 0);
done:
    if(!validate) stat = NC_ENOTNC;
    nullfree(path);
    nullfree(segment);
    nclistfreeall(queue);
    nclistfreeall(nextlevel);
    ncbytesfree(prefix);
    return ZUNTRACE(THROW(stat));
}

/**
Insert an attribute into a list of attribute, including typing
Takes control of javalue.
@param jatts
@param jtypes
@param aname
@param javalue
*/
static int
insert_attr(NCjson* jatts, NCjson* jtypes, const char* aname, NCjson* javalue, const char* atype)
{
    int stat = NC_NOERR;
    if(jatts != NULL) {
	if(jtypes != NULL) {
            NCJinsertstring(jtypes,aname,atype);
	}
        NCJinsert(jatts,aname,javalue);
    }
    return THROW(stat);
}

/**
Insert _nczarr_attr into .zattrs
Take control of jtypes
@param jatts
@param jtypes
*/
static int
insert_nczarr_attr(NCjson* jatts, NCjson* jtypes)
{
    NCjson* jdict = NULL;
    if(jatts != NULL && jtypes != NULL) {
	NCJinsertstring(jtypes,NCZ_V2_ATTR,"|J0"); /* type for _nczarr_attr */
        NCJnew(NCJ_DICT,&jdict);
        NCJinsert(jdict,"types",jtypes);
        NCJinsert(jatts,NCZ_V2_ATTR,jdict);
        jdict = NULL;
    }
    return NC_NOERR;
}

/**
Upload a .zattrs object
Optionally take control of jatts and jtypes
@param file
@param container
@param jattsp
@param jtypesp
*/
static int
upload_attrs(NC_FILE_INFO_T* file, NC_OBJ* container, NCjson* jatts)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
    NC_VAR_INFO_T* var = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NCZMAP* map = NULL;
    char* fullpath = NULL;
    char* key = NULL;

    ZTRACE(3,"file=%s grp=%s",file->controller->path,container->name);

    if(jatts == NULL) goto done;    

    zinfo = file->format_file_info;
    map = zinfo->map;

    if(container->sort == NCVAR) {
        var = (NC_VAR_INFO_T*)container;
    } else if(container->sort == NCGRP) {
        grp = (NC_GRP_INFO_T*)container;
    }

    /* Construct container path */
    if(container->sort == NCGRP)
	stat = NCZ_grpkey(grp,&fullpath);
    else
	stat = NCZ_varkey(var,&fullpath);
    if(stat) goto done;

    /* write .zattrs*/
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat=NCZ_uploadjson(map,key,jatts))) goto done;
    nullfree(key); key = NULL;

done:
    nullfree(fullpath);
    return ZUNTRACE(THROW(stat));
}

#if 0
/**
@internal Get contents of a meta object; fail it it does not exist
@param zmap - [in] map
@param key - [in] key of the object
@param jsonp - [out] return parsed json || NULL if not exists
@return NC_NOERR
@return NC_EXXX
@author Dennis Heimbigner
*/
static int
readarray(NCZMAP* zmap, const char* key, NCjson** jsonp)
{
    int stat = NC_NOERR;
    NCjson* json = NULL;

    if((stat = NCZ_downloadjson(zmap,key,&json))) goto done;
    if(json != NULL && NCJsort(json) != NCJ_ARRAY) {stat = NC_ENCZARR; goto done;}
    if(jsonp) {*jsonp = json; json = NULL;}
done:
    NCJreclaim(json);
    return stat;
}
#endif

/* Get one of two key values from a dict */
static int
dictgetalt(const NCjson* jdict, const char* name, const char* alt, const NCjson** jvaluep)
{
    int stat = NC_NOERR;
    const NCjson* jvalue = NULL;
    if((stat = NCJdictget(jdict,name,&jvalue))<0) {stat = NC_EINVAL; goto done;} /* try this first */
    if(jvalue == NULL) {
        if((stat = NCJdictget(jdict,alt,&jvalue))<0) {stat = NC_EINVAL; goto done;} /* try this alternative*/
    }
    if(jvaluep) *jvaluep = jvalue;
done:
    return THROW(stat);
}

/* Get _nczarr_xxx from either .zXXX or .zattrs */
static int
getnczarrkey(NC_OBJ* container, const char* name, const NCjson** jncxxxp)
{
    int stat = NC_NOERR;
    const NCjson* jxxx = NULL;
    NC_GRP_INFO_T* grp = NULL;
    NC_VAR_INFO_T* var = NULL;
    struct ZARROBJ* zobj = NULL;

    /* Decode container */
    if(container->sort == NCGRP) {
	grp = (NC_GRP_INFO_T*)container;
	zobj = &((NCZ_GRP_INFO_T*)grp->format_grp_info)->zgroup;
    } else {
	var = (NC_VAR_INFO_T*)container;
	zobj = &((NCZ_VAR_INFO_T*)var->format_var_info)->zarray;
    }

    /* Try .zattrs first */
    if(zobj->atts != NULL) {
	jxxx = NULL;
        if((stat = NCJdictget(zobj->atts,name,&jxxx))<0) {stat = NC_EINVAL; goto done;}
    }
    if(jxxx == NULL) {
        /* Try .zxxx second */
	if(zobj->obj != NULL) {
            if((stat = NCJdictget(zobj->obj,name,&jxxx))<0) {stat = NC_EINVAL; goto done;}
	}
	if(jxxx != NULL)
  	    zobj->nczv1 = 1; /* Mark as old style with _nczarr_xxx in obj not attributes */
    }
    if(jncxxxp) *jncxxxp = jxxx;
done:
    return THROW(stat);
}

static int
downloadzarrobj(NC_FILE_INFO_T* file, struct ZARROBJ* zobj, const char* fullpath, const char* objname)
{
    int stat = NC_NOERR;
    char* key = NULL;
    NCZMAP* map = ((NCZ_FILE_INFO_T*)file->format_file_info)->map;

    /* Download .zXXX and .zattrs */
    nullfree(zobj->prefix);
    zobj->prefix = strdup(fullpath);
    NCJreclaim(zobj->obj); zobj->obj = NULL;
    NCJreclaim(zobj->atts); zobj->obj = NULL;
    if((stat = nczm_concat(fullpath,objname,&key))) goto done;
    if((stat=NCZ_downloadjson(map,key,&zobj->obj))) goto done;
    nullfree(key); key = NULL;
    if((stat = nczm_concat(fullpath,Z2ATTRS,&key))) goto done;
    if((stat=NCZ_downloadjson(map,key,&zobj->atts))) goto done;
done:
    nullfree(key);
    return THROW(stat);
}
