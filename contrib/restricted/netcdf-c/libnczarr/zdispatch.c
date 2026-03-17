/* Copyright 2005-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @file
 * @internal This header file contains prototypes and initialization
 * for the ZARR dispatch layer.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "zincludes.h"
#include "zplugins.h"

/* Forward */
static int NCZ_var_par_access(int ncid, int varid, int par_access);
static int NCZ_show_metadata(int ncid);

static const NC_Dispatch NCZ_dispatcher = {

    NC_FORMATX_NCZARR,
    NC_DISPATCH_VERSION,

    NCZ_create,
    NCZ_open,

    NCZ_redef,
    NCZ__enddef,
    NCZ_sync,
    NCZ_abort,
    NCZ_close,
    NCZ_set_fill,
    NCZ_inq_format,
    NCZ_inq_format_extended,

    NCZ_inq,
    NCZ_inq_type,

    NCZ_def_dim,
    NCZ_inq_dimid,
    NCZ_inq_dim,
    NC4_inq_unlimdim,
    NCZ_rename_dim,

    NCZ_inq_att,
    NCZ_inq_attid,
    NCZ_inq_attname,
    NCZ_rename_att,
    NCZ_del_att,
    NCZ_get_att,
    NCZ_put_att,

    NCZ_def_var,
    NCZ_inq_varid,
    NCZ_rename_var,
    NCZ_get_vara,
    NCZ_put_vara,
    NCZ_get_vars,
    NCZ_put_vars,
    NCDEFAULT_get_varm,
    NCDEFAULT_put_varm,

    NCZ_inq_var_all,

    NCZ_var_par_access,
    NCZ_def_var_fill,

    NCZ_show_metadata,
    NC4_inq_unlimdims,

    NCZ_inq_ncid,
    NCZ_inq_grps,
    NCZ_inq_grpname,
    NCZ_inq_grpname_full,
    NCZ_inq_grp_parent,
    NCZ_inq_grp_full_ncid,
    NCZ_inq_varids,
    NCZ_inq_dimids,
    NCZ_inq_typeids,
    NCZ_inq_type_equal,
    NCZ_def_grp,
    NCZ_rename_grp,
    NCZ_inq_user_type,
    NCZ_inq_typeid,

    NC_NOTNC4_def_compound,
    NC_NOTNC4_insert_compound,
    NC_NOTNC4_insert_array_compound,
    NC_NOTNC4_inq_compound_field,
    NC_NOTNC4_inq_compound_fieldindex,
    NC_NOTNC4_def_vlen,
    NC_NOTNC4_put_vlen_element,
    NC_NOTNC4_get_vlen_element,
    NC_NOTNC4_def_enum,
    NC_NOTNC4_insert_enum,
    NC_NOTNC4_inq_enum_member,
    NC_NOTNC4_inq_enum_ident,
    NC_NOTNC4_def_opaque,
    NCZ_def_var_deflate,
    NCZ_def_var_fletcher32,
    NCZ_def_var_chunking,
    NCZ_def_var_endian,
    NCZ_def_var_filter,
    NCZ_set_var_chunk_cache,
    NC4_get_var_chunk_cache,
    NCZ_inq_var_filter_ids,
    NCZ_inq_var_filter_info,
    NCZ_def_var_quantize,
    NCZ_inq_var_quantize,
    NCZ_inq_filter_avail,
};

const NC_Dispatch* NCZ_dispatch_table = NULL; /* moved here from ddispatch.c */

/**
 * @internal Initialize the ZARR dispatch layer.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner, Ed Hartnett
 */
int ncz_initialized = 0; /**< True if initialization has happened. */

int
NCZ_initialize(void)
{
    int stat;
    NCZ_dispatch_table = &NCZ_dispatcher;
    if (!ncz_initialized)
        NCZ_initialize_internal();
    stat = NCZ_provenance_init();
    if(stat) ncz_initialized = 1;
    return stat;
}

/**
 * @internal Finalize the ZARR dispatch layer.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NCZ_finalize(void)
{
    NCZ_finalize_internal();
    NCZ_provenance_finalize();
    return NC_NOERR;
}

static int
NCZ_var_par_access(int ncid, int varid, int par_access)
{
    return NC_NOERR; /* no-op */
}

static int
NCZ_show_metadata(int ncid)
{
    return NC_NOERR;
}

#ifndef NETCDF_ENABLE_NCZARR_FILTERS
int 
NCZ_def_var_filter(int ncid, int varid, unsigned int id , size_t n , const unsigned int *params)
{
    NC_UNUSED(ncid);
    NC_UNUSED(varid);
    NC_UNUSED(id);
    NC_UNUSED(n);
    NC_UNUSED(params);
    return REPORT(NC_NOERR,"def_var_filter");
}

int 
NCZ_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids)
{
    NC_UNUSED(ncid);
    NC_UNUSED(varid);
    NC_UNUSED(filterids);
    if(nfilters) *nfilters = 0;
    return REPORT(NC_NOERR,"inq_var_filter_ids");
}

int
NCZ_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params)
{
    NC_UNUSED(ncid);
    NC_UNUSED(varid);
    NC_UNUSED(id);
    NC_UNUSED(nparams);
    NC_UNUSED(params);
    return REPORT(NC_ENOFILTER,"inq_var_filter_info");
}

int
NCZ_inq_filter_avail(int ncid, unsigned id)
{
    NC_UNUSED(ncid);
    NC_UNUSED(id);
    return REPORT(NC_ENOFILTER,"inq_filter_avail");
}

#endif /*NETCDF_ENABLE_NCZARR_FILTERS*/

/**************************************************/
/* Following functions call into libsrc4 */

int
NCZ_inq_type(int ncid, nc_type xtype, char *name, size_t *size)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_type(ncid,xtype,name,size)");
    stat = NC4_inq_type(ncid,xtype,name,size);
    return ZUNTRACE(stat);
}

int
NCZ_inq_dimid(int ncid, const char *name, int *idp)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_dimid(ncid,name,idp)");
    stat = NC4_inq_dimid(ncid,name,idp);
    return ZUNTRACE(stat);
}

int
NCZ_inq_unlimdim(int ncid,  int *unlimdimidp)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_unlimdim(ncid,unlimdimidp)");
    stat = NC4_inq_unlimdim(ncid,unlimdimidp);
    return ZUNTRACE(stat);
}

int
NCZ_inq_varid(int ncid, const char* name, int *varidp)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_varid(ncid,name,varidp)");
    stat = NC4_inq_varid(ncid,name,varidp);
    return ZUNTRACE(stat);
}

int
NCZ_inq_ncid(int ncid, const char* name, int* grpidp)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_ncid(ncid,name,grpidp)");
    stat = NC4_inq_ncid(ncid,name,grpidp);
    return ZUNTRACE(stat);
}

int
NCZ_inq_grps(int ncid, int* n, int* ncids)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_grps(ncid,n,ncids)");
    stat = NC4_inq_grps(ncid,n,ncids);
    return ZUNTRACE(stat);
}

int
NCZ_inq_grpname(int ncid, char* name)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_grpname(ncid,name)");
    stat = NC4_inq_grpname(ncid,name);
    return ZUNTRACE(stat);
}

int
NCZ_inq_grpname_full(int ncid, size_t* lenp, char* fullname)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_grpname_full(ncid,lenp,fullname)");
    stat = NC4_inq_grpname_full(ncid,lenp,fullname);
    return ZUNTRACE(stat);
}

int
NCZ_inq_grp_parent(int ncid, int* parentidp)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_grp_parent(ncid,parentidp)");
    stat = NC4_inq_grp_parent(ncid,parentidp);
    return ZUNTRACE(stat);
}

int
NCZ_inq_grp_full_ncid(int ncid, const char* fullname, int* grpidp)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_grp_full_ncid(ncid,fullname,grpidp)");
    stat = NC4_inq_grp_full_ncid(ncid,fullname,grpidp);
    return ZUNTRACE(stat);
}

int
NCZ_inq_varids(int ncid, int* nvars, int* varids)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_varids(ncid,nvars,varids)");
    stat = NC4_inq_varids(ncid,nvars,varids);
    return ZUNTRACE(stat);
}

int
NCZ_inq_dimids(int ncid, int* ndims, int* dimids, int inclparents)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_dimids(ncid,ndims,dimids,inclparents)");
    stat = NC4_inq_dimids(ncid,ndims,dimids,inclparents);
    return ZUNTRACE(stat);
}

int
NCZ_inq_user_type(int ncid, nc_type xtype, char* name, size_t* size, nc_type* basetid, size_t* nfields, int* classp)
{
    int stat = NC_NOERR;
    ZTRACE(0,"NC4_inq_user_type(ncid,xtype,name,size,basetid,nfields,classp)");
    stat = NC4_inq_user_type(ncid,xtype,name,size,basetid,nfields,classp);
    return ZUNTRACE(stat);
}
