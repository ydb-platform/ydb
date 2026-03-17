/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal Includes prototypes for libsrc4 dispatch functions.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#ifndef _NC4DISPATCH_H
#define _NC4DISPATCH_H

#include "config.h"
#include <stddef.h> /* size_t, ptrdiff_t */
#include "ncdispatch.h"

#define NCLOGLEVELENV "NETCDF_LOG_LEVEL"

#if defined(__cplusplus)
extern "C" {
#endif

    EXTERNL int
    NC4_create(const char *path, int cmode,
               size_t initialsz, int basepe, size_t *chunksizehintp,
               void* parameters, const NC_Dispatch*, int);

    EXTERNL int
    NC4_open(const char *path, int mode,
             int basepe, size_t *chunksizehintp,
             void* parameters, const NC_Dispatch*, int);

    EXTERNL int
    NC4_redef(int ncid);

    EXTERNL int
    NC4__enddef(int ncid, size_t h_minfree, size_t v_align,
                size_t v_minfree, size_t r_align);

    EXTERNL int
    NC4_sync(int ncid);

    EXTERNL int
    NC4_abort(int ncid);

    EXTERNL int
    NC4_close(int ncid,void*);

    EXTERNL int
    NC4_set_fill(int ncid, int fillmode, int *old_modep);

    EXTERNL int
    NC4_inq_format(int ncid, int *formatp);

    EXTERNL int
    NC4_inq_format_extended(int ncid, int *formatp, int *modep);

    EXTERNL int
    NC4_inq(int ncid, int *ndimsp, int *nvarsp, int *nattsp, int *unlimdimidp);

    EXTERNL int
    NC4_inq_type(int, nc_type, char *, size_t *);

/* Begin _dim */

    EXTERNL int
    NC4_inq_dimid(int ncid, const char *name, int *idp);

    EXTERNL int
    NC4_inq_unlimdim(int ncid, int *unlimdimidp);

/* End _dim */
/* Begin _att */

    EXTERNL int
    NC4_inq_att(int ncid, int varid, const char *name,
                nc_type *xtypep, size_t *lenp);

    EXTERNL int
    NC4_inq_attid(int ncid, int varid, const char *name, int *idp);

    EXTERNL int
    NC4_inq_attname(int ncid, int varid, int attnum, char *name);

/* End _att */
/* Begin {put,get}_att */

    EXTERNL int
    NC4_get_att(int ncid, int varid, const char *name, void *value, nc_type);

/* End {put,get}_att */
/* Begin _var */

    EXTERNL int
    NC4_def_var(int ncid, const char *name,
                nc_type xtype, int ndims, const int *dimidsp, int *varidp);

    EXTERNL int
    NC4_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
                    int *ndimsp, int *dimidsp, int *nattsp,
                    int *shufflep, int *deflatep, int *deflate_levelp,
                    int *fletcher32p, int *contiguousp, size_t *chunksizesp,
                    int *no_fill, void *fill_valuep, int *endiannessp,
                    unsigned int* idp, size_t* nparamsp, unsigned int* params);

    EXTERNL int
    NC4_inq_varid(int ncid, const char *name, int *varidp);

    EXTERNL int
    NC4_rename_var(int ncid, int varid, const char *name);

    EXTERNL int
    NC4_put_vara(int ncid, int varid,
                 const size_t *start, const size_t *count,
                 const void *value, nc_type);

    EXTERNL int
    NC4_get_vara(int ncid, int varid,
                 const size_t *start, const size_t *count,
                 void *value, nc_type);

    extern int
    NC4_put_vars(int ncid, int varid,
                 const size_t *start, const size_t *count, const ptrdiff_t* stride,
                 const void *value, nc_type);

    extern int
    NC4_get_vars(int ncid, int varid,
                 const size_t *start, const size_t *count, const ptrdiff_t* stride,
                 void *value, nc_type);

/* End _var */

/* netCDF4 API only */
    EXTERNL int
    NC4_var_par_access(int, int, int);

    EXTERNL int
    NC4_inq_ncid(int, const char *, int *);

    EXTERNL int
    NC4_inq_grps(int, int *, int *);

    EXTERNL int
    NC4_inq_grpname(int, char *);

    EXTERNL int
    NC4_inq_grpname_full(int, size_t *, char *);

    EXTERNL int
    NC4_inq_grp_parent(int, int *);

    EXTERNL int
    NC4_inq_grp_full_ncid(int, const char *, int *);

    EXTERNL int
    NC4_inq_varids(int, int * nvars, int *);

    EXTERNL int
    NC4_inq_dimids(int, int * ndims, int *, int);

    EXTERNL int
    NC4_inq_typeids(int, int * ntypes, int *);

    EXTERNL int
    NC4_inq_type_equal(int, nc_type, int, nc_type, int *);

    EXTERNL int
    NC4_def_grp(int, const char *, int *);

    EXTERNL int
    NC4_rename_grp(int, const char *);

    EXTERNL int
    NC4_inq_user_type(int, nc_type, char *, size_t *, nc_type *,
                      size_t *, int *);

    EXTERNL int
    NC4_def_compound(int, size_t, const char *, nc_type *);

    EXTERNL int
    NC4_insert_compound(int, nc_type, const char *, size_t, nc_type);

    EXTERNL int
    NC4_insert_array_compound(int, nc_type, const char *, size_t,
                              nc_type, int, const int *);

    EXTERNL int
    NC4_inq_typeid(int, const char *, nc_type *);

    EXTERNL int
    NC4_inq_compound_field(int, nc_type, int, char *, size_t *,
                           nc_type *, int *, int *);

    EXTERNL int
    NC4_inq_compound_fieldindex(int, nc_type, const char *, int *);

    EXTERNL int
    NC4_def_vlen(int, const char *, nc_type base_typeid, nc_type *);

    EXTERNL int
    NC4_put_vlen_element(int, int, void *, size_t, const void *);

    EXTERNL int
    NC4_get_vlen_element(int, int, const void *, size_t *, void *);

    EXTERNL int
    NC4_def_enum(int, nc_type, const char *, nc_type *);

    EXTERNL int
    NC4_insert_enum(int, nc_type, const char *, const void *);

    EXTERNL int
    NC4_inq_enum_member(int, nc_type, int, char *, void *);

    EXTERNL int
    NC4_inq_enum_ident(int, nc_type, long long, char *);

    EXTERNL int
    NC4_def_opaque(int, size_t, const char *, nc_type *);

    EXTERNL int
    NC4_def_var_deflate(int, int, int, int, int);

    EXTERNL int
    NC4_def_var_fletcher32(int, int, int);

    EXTERNL int
    NC4_def_var_chunking(int, int, int, const size_t *);

    EXTERNL int
    NC4_def_var_fill(int, int, int, const void *);

    EXTERNL int
    NC4_def_var_endian(int, int, int);

    EXTERNL int
    NC4_def_var_filter(int, int, unsigned int, size_t, const unsigned int*);

    EXTERNL int
    NC4_get_var_chunk_cache(int, int, size_t *, size_t *, float *);

    EXTERNL int
    NC4_inq_unlimdims(int, int *, int *);

    EXTERNL int
    NC4_show_metadata(int);

    EXTERNL int
    NC4_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids);
    EXTERNL int
    NC4_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params);

    EXTERNL int
    NC4_def_var_quantize(int ncid, int varid, int quantize_mode, int nsd);

    EXTERNL int
    NC4_inq_var_quantize(int ncid, int varid, int *quantize_modep, int *nsdp);
    
#if defined(__cplusplus)
}
#endif

#endif /*_NC4DISPATCH_H */
