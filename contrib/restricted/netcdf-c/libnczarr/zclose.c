/*********************************************************************
 *   Copyright 1993, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "zincludes.h"
#include "zfilter.h"

/* Forward */
static int zclose_group(NC_GRP_INFO_T*);
static int zclose_gatts(NC_GRP_INFO_T*);
static int zclose_vars(NC_GRP_INFO_T*);
static int zclose_dims(NC_GRP_INFO_T*);
static int zclose_types(NC_GRP_INFO_T*);
static int zclose_type(NC_TYPE_INFO_T* type);
static int zwrite_vars(NC_GRP_INFO_T *grp);

/**************************************************/

/**
 * @internal This function will recurse through an open ZARR file and
 * release resources. All ZARR annotations reclaimed
 *
 * @param file Pointer to ZARR file info struct.
 * @param abort True if this is an abort.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_ENCZARR could not close the file.
 * @author Dennis Heimbigner
 */
int
ncz_close_file(NC_FILE_INFO_T* file, int abort)
{
    int stat = NC_NOERR;
    NCZ_FILE_INFO_T* zinfo = NULL;
 
    ZTRACE(2,"file=%s abort=%d",file->hdr.name,abort);

    if(!abort) {
        /* Flush | create all chunks for all vars */
        if((stat=zwrite_vars(file->root_grp))) goto done;
    }

    /* Internal close to reclaim zarr annotations */
    if((stat = zclose_group(file->root_grp)))
	goto done;

    zinfo = file->format_file_info;

    if((stat = nczmap_close(zinfo->map,(abort && zinfo->creating)?1:0)))
	goto done;
    nclistfreeall(zinfo->controllist);
    NC_authfree(zinfo->auth);
    NCZMD_free_metadata_handler(&(zinfo->metadata));
    nullfree(zinfo);

done:
    return ZUNTRACE(stat);
}

/**************************************************/
/**
 * @internal Recursively free zarr annotations for a group (and everything
 * it contains).
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zclose_group(NC_GRP_INFO_T *grp)
{
    int stat = NC_NOERR;
    NCZ_GRP_INFO_T* zgrp;
    size_t i;

    assert(grp && grp->format_grp_info != NULL);
    LOG((3, "%s: grp->name %s", __func__, grp->hdr.name));

    /* Recursively call this function for each child, if any, stopping
     * if there is an error. */
    for(i=0; i<ncindexsize(grp->children); i++) {
        if ((stat = zclose_group((NC_GRP_INFO_T*)ncindexith(grp->children,i))))
            goto done;
    }

    /* Close resources associated with global attributes. */
    if ((stat = zclose_gatts(grp)))
        goto done;

    /* Close resources associated with vars. */
    if ((stat = zclose_vars(grp)))
        goto done;

    /* Close resources associated with dims. */
    if ((stat = zclose_dims(grp)))
        goto done;

    /* Close resources associated with types. */
    if ((stat = zclose_types(grp)))
        goto done;

    /* Close the zgroup. */
    zgrp = grp->format_grp_info;
    LOG((4, "%s: closing group %s", __func__, grp->hdr.name));
    nullfree(zgrp->zgroup.prefix);
    NCJreclaim(zgrp->zgroup.obj);
    NCJreclaim(zgrp->zgroup.atts);
    nullfree(zgrp);
    grp->format_grp_info = NULL; /* avoid memory errors */

done:
    return stat;
}

/**
 * @internal Close resources for global atts in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zclose_gatts(NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NC_ATT_INFO_T *att;
    size_t a;
    for(a = 0; a < ncindexsize(grp->att); a++) {
        NCZ_ATT_INFO_T* zatt = NULL;
        att = (NC_ATT_INFO_T* )ncindexith(grp->att, a);
        assert(att && att->format_att_info != NULL);
        zatt = att->format_att_info;
	nullfree(zatt);
        att->format_att_info = NULL; /* avoid memory errors */
    }
    return stat;
}

/**
 * @internal Close resources for single var
 *
 * @param var var to reclaim
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NCZ_zclose_var1(NC_VAR_INFO_T* var)
{
    int stat = NC_NOERR;
    NCZ_VAR_INFO_T* zvar;
    NC_ATT_INFO_T* att;
    size_t a;

    assert(var && var->format_var_info);
    for(a = 0; a < ncindexsize(var->att); a++) {
	NCZ_ATT_INFO_T* zatt;
	att = (NC_ATT_INFO_T*)ncindexith(var->att, a);
	assert(att && att->format_att_info);
	zatt = att->format_att_info;
	nullfree(zatt);
	att->format_att_info = NULL; /* avoid memory errors */
    }
#ifdef NETCDF_ENABLE_NCZARR_FILTERS
    /* Reclaim filters */
    if(var->filters != NULL) {
	(void)NCZ_filter_freelists(var);
    }
    var->filters = NULL;
#endif
    /* Reclaim the type */
    if(var->type_info) (void)zclose_type(var->type_info);
    /* reclaim dispatch info */
    zvar = var->format_var_info;;
    if(zvar->cache) NCZ_free_chunk_cache(zvar->cache);
    /* reclaim xarray */
    if(zvar->xarray) nclistfreeall(zvar->xarray);
    nullfree(zvar->zarray.prefix);
    NCJreclaim(zvar->zarray.obj);
    NCJreclaim(zvar->zarray.atts);
    nullfree(zvar);
    var->format_var_info = NULL; /* avoid memory errors */
    return stat;
}

/**
 * @internal Close nczarr resources for vars in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zclose_vars(NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NC_VAR_INFO_T* var;
    size_t i;

    for(i = 0; i < ncindexsize(grp->vars); i++) {
        var = (NC_VAR_INFO_T*)ncindexith(grp->vars, i);
        assert(var && var->format_var_info);
	if((stat = NCZ_zclose_var1(var))) goto done;
    }
done:
    return stat;
}

/**
 * @internal Close resources for dims in a group.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zclose_dims(NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    NC_DIM_INFO_T* dim;
    size_t i;

    for(i = 0; i < ncindexsize(grp->dim); i++) {
        NCZ_DIM_INFO_T* zdim;
        dim = (NC_DIM_INFO_T*)ncindexith(grp->dim, i);
        assert(dim && dim->format_dim_info);
        zdim = dim->format_dim_info;
	nullfree(zdim);
	dim->format_dim_info = NULL; /* avoid memory errors */
    }

    return stat;
}

/**
 * @internal Close resources for a single type.  Set values to
 * 0 after closing types. Because of type reference counters, these
 * closes can be called multiple times.
 *
 * @param type Pointer to type struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zclose_type(NC_TYPE_INFO_T* type)
{
    int stat = NC_NOERR;

    assert(type && type->format_type_info != NULL);
    nullfree(type->format_type_info);
    return stat;
}

/**
 * @internal Close resources for types in a group.  Set values to
 * 0 after closing types. Because of type reference counters, these
 * closes can be called multiple times.
 * Warning: note that atomic types are not covered here; this
 * is only for user-defined types.
 *
 * @param grp Pointer to group info struct.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zclose_types(NC_GRP_INFO_T* grp)
{
    int stat = NC_NOERR;
    size_t i;
    NC_TYPE_INFO_T* type;

    for(i = 0; i < ncindexsize(grp->type); i++)
    {
        type = (NC_TYPE_INFO_T*)ncindexith(grp->type, i);
	if((stat = zclose_type(type))) goto done;
    }
done:
    return stat;
}

/**
 * @internal Recursively flush/create all data for all vars.
 *
 * @param grp Pointer to group info struct whose vars need to be written
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
static int
zwrite_vars(NC_GRP_INFO_T *grp)
{
    int stat = NC_NOERR;
    size_t i;

    assert(grp && grp->format_grp_info != NULL);
    LOG((3, "%s: grp->name %s", __func__, grp->hdr.name));

    /* Write all vars for this group breadth first */
    for(i = 0; i < ncindexsize(grp->vars); i++) {
        NC_VAR_INFO_T* var = (NC_VAR_INFO_T*)ncindexith(grp->vars, i);
	if((stat = ncz_write_var(var))) goto done;
    }

    /* Recursively call this function for each child group, if any, stopping
     * if there is an error. */
    for(i=0; i<ncindexsize(grp->children); i++) {
        if ((stat = zwrite_vars((NC_GRP_INFO_T*)ncindexith(grp->children,i))))
            goto done;
    }

done:
    return stat;
}


