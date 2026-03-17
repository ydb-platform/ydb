/* Copyright 2005-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal This header file contains prototypes and initialization
 * for the HDF5 dispatch layer.
 *
 * @author Ed Hartnett, Dennis Heimbigner
 */

#include "config.h"
#include "hdf5internal.h"
#include "hdf5dispatch.h"

#ifdef NETCDF_ENABLE_BYTERANGE
#include "H5FDhttp.h"
#endif

static const NC_Dispatch HDF5_dispatcher = {

    NC_FORMATX_NC4,
    NC_DISPATCH_VERSION,

    NC4_create,
    NC4_open,

    NC4_redef,
    NC4__enddef,
    NC4_sync,
    NC4_abort,
    NC4_close,
    NC4_set_fill,
    NC4_inq_format,
    NC4_inq_format_extended,

    NC4_inq,
    NC4_inq_type,

    HDF5_def_dim,
    NC4_inq_dimid,
    HDF5_inq_dim,
    NC4_inq_unlimdim,
    HDF5_rename_dim,

    NC4_HDF5_inq_att,
    NC4_HDF5_inq_attid,
    NC4_HDF5_inq_attname,
    NC4_HDF5_rename_att,
    NC4_HDF5_del_att,
    NC4_HDF5_get_att,
    NC4_HDF5_put_att,

    NC4_def_var,
    NC4_inq_varid,
    NC4_rename_var,
    NC4_get_vara,
    NC4_put_vara,
    NC4_get_vars,
    NC4_put_vars,
    NCDEFAULT_get_varm,
    NCDEFAULT_put_varm,

    NC4_HDF5_inq_var_all,

    NC4_var_par_access,
    NC4_def_var_fill,

    NC4_show_metadata,
    NC4_inq_unlimdims,

    NC4_inq_ncid,
    NC4_inq_grps,
    NC4_inq_grpname,
    NC4_inq_grpname_full,
    NC4_inq_grp_parent,
    NC4_inq_grp_full_ncid,
    NC4_inq_varids,
    NC4_inq_dimids,
    NC4_inq_typeids,
    NC4_inq_type_equal,
    NC4_def_grp,
    NC4_rename_grp,
    NC4_inq_user_type,
    NC4_inq_typeid,

    NC4_def_compound,
    NC4_insert_compound,
    NC4_insert_array_compound,
    NC4_inq_compound_field,
    NC4_inq_compound_fieldindex,
    NC4_def_vlen,
    NC4_put_vlen_element,
    NC4_get_vlen_element,
    NC4_def_enum,
    NC4_insert_enum,
    NC4_inq_enum_member,
    NC4_inq_enum_ident,
    NC4_def_opaque,
    NC4_def_var_deflate,
    NC4_def_var_fletcher32,
    NC4_def_var_chunking,
    NC4_def_var_endian,
    NC4_hdf5_def_var_filter,
    NC4_HDF5_set_var_chunk_cache,
    NC4_get_var_chunk_cache,

    NC4_hdf5_inq_var_filter_ids,
    NC4_hdf5_inq_var_filter_info,

    NC4_def_var_quantize,
    NC4_inq_var_quantize,
    
    NC4_hdf5_inq_filter_avail,
};

const NC_Dispatch* HDF5_dispatch_table = NULL; /* moved here from ddispatch.c */

/**
 * @internal Initialize the HDF5 dispatch layer.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
NC_HDF5_initialize(void)
{
    HDF5_dispatch_table = &HDF5_dispatcher;
    if (!nc4_hdf5_initialized)
        nc4_hdf5_initialize();

#ifdef NETCDF_ENABLE_BYTERANGE
    (void)H5FD_http_init();
#endif
    return NC4_provenance_init();
}

/**
 * @internal Finalize the HDF5 dispatch layer.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NC_HDF5_finalize(void)
{
#ifdef NETCDF_ENABLE_BYTERANGE
    (void)H5FD_http_finalize();
#endif
    (void)nc4_hdf5_finalize();
    return NC_NOERR;
}
