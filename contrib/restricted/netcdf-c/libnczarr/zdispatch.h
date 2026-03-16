/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal Includes prototypes for libzarr dispatch functions.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#ifndef ZDISPATCH_H
#define ZDISPATCH_H

#include "zincludes.h"

#if defined(__cplusplus)
extern "C" {
#endif

EXTERNL int NCZ_create(const char *path, int cmode, size_t initialsz, int basepe, size_t *chunksizehintp, void* parameters, const NC_Dispatch*, int);

EXTERNL int NCZ_open(const char *path, int mode, int basepe, size_t *chunksizehintp, void* parameters, const NC_Dispatch*, int);

EXTERNL int NCZ_redef(int ncid);

EXTERNL int NCZ__enddef(int ncid, size_t h_minfree, size_t v_align, size_t v_minfree, size_t r_align);

EXTERNL int NCZ_sync(int ncid);

EXTERNL int NCZ_abort(int ncid);

EXTERNL int NCZ_close(int ncid,void*);

EXTERNL int NCZ_set_fill(int ncid, int fillmode, int *old_modep);

EXTERNL int NCZ_set_base_pe(int ncid, int pe);

EXTERNL int NCZ_inq_base_pe(int ncid, int *pe);

EXTERNL int NCZ_inq_format(int ncid, int *formatp);

EXTERNL int NCZ_inq_format_extended(int ncid, int *formatp, int *modep);

EXTERNL int NCZ_inq(int ncid, int *ndimsp, int *nvarsp, int *nattsp, int *unlimdimidp);

EXTERNL int NCZ_inq_type(int, nc_type, char *, size_t *);

/* Begin _dim */

EXTERNL int NCZ_def_dim(int ncid, const char *name, size_t len, int *idp);

EXTERNL int NCZ_inq_dimid(int ncid, const char *name, int *idp);

EXTERNL int NCZ_inq_dim(int ncid, int dimid, char *name, size_t *lenp);

EXTERNL int NCZ_inq_unlimdim(int ncid, int *unlimdimidp);

EXTERNL int NCZ_rename_dim(int ncid, int dimid, const char *name);

/* End _dim */
/* Begin _att */

EXTERNL int NCZ_inq_att(int ncid, int varid, const char *name, nc_type *xtypep, size_t *lenp);

EXTERNL int NCZ_inq_attid(int ncid, int varid, const char *name, int *idp);

EXTERNL int NCZ_inq_attname(int ncid, int varid, int attnum, char *name);

EXTERNL int NCZ_rename_att(int ncid, int varid, const char *name, const char *newname);

EXTERNL int NCZ_del_att(int ncid, int varid, const char *name);


/* End _att */
/* Begin {put,get}_att */

EXTERNL int NCZ_get_att(int ncid, int varid, const char *name, void *value, nc_type);

EXTERNL int NCZ_put_att(int ncid, int varid, const char *name, nc_type file_type, size_t len, const void *data, nc_type mem_type);

/* End {put,get}_att */
/* Begin _var */

EXTERNL int NCZ_def_var(int ncid, const char *name, nc_type xtype, int ndims, const int *dimidsp, int *varidp);

EXTERNL int NCZ_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep, int *ndimsp, int *dimidsp, int *nattsp, int *shufflep, int *deflatep, int *deflate_levelp, int *fletcher32p, int *contiguousp, size_t *chunksizesp, int *no_fill, void *fill_valuep, int *endiannessp, unsigned int* idp, size_t* nparamsp, unsigned int* params
);

EXTERNL int NCZ_inq_varid(int ncid, const char *name, int *varidp);

EXTERNL int NCZ_rename_var(int ncid, int varid, const char *name);

EXTERNL int NCZ_put_vara(int ncid, int varid, const size_t *start, const size_t *count, const void *value, nc_type);

EXTERNL int NCZ_get_vara(int ncid, int varid, const size_t *start, const size_t *count, void *value, nc_type);

extern int
NCZ_put_vars(int ncid, int varid, const size_t *start, const size_t *count, const ptrdiff_t* stride, const void *value, nc_type);

extern int
NCZ_get_vars(int ncid, int varid, const size_t *start, const size_t *count, const ptrdiff_t* stride, void *value, nc_type);

/* End _var */

/* netCDF4 API only */

EXTERNL int NCZ_inq_ncid(int, const char *, int *);

EXTERNL int NCZ_inq_grps(int, int *, int *);

EXTERNL int NCZ_inq_grpname(int, char *);

EXTERNL int NCZ_inq_grpname_full(int, size_t *, char *);

EXTERNL int NCZ_inq_grp_parent(int, int *);

EXTERNL int NCZ_inq_grp_full_ncid(int, const char *, int *);

EXTERNL int NCZ_inq_varids(int, int * nvars, int *);

EXTERNL int NCZ_inq_dimids(int, int * ndims, int *, int);

EXTERNL int NCZ_inq_typeids(int, int * ntypes, int *);

EXTERNL int NCZ_inq_type_equal(int, nc_type, int, nc_type, int *);

EXTERNL int NCZ_def_grp(int, const char *, int *);

EXTERNL int NCZ_rename_grp(int, const char *);

EXTERNL int NCZ_inq_user_type(int, nc_type, char *, size_t *, nc_type *, size_t *, int *);

EXTERNL int NCZ_def_compound(int, size_t, const char *, nc_type *);

EXTERNL int NCZ_insert_compound(int, nc_type, const char *, size_t, nc_type);

EXTERNL int NCZ_insert_array_compound(int, nc_type, const char *, size_t, nc_type, int, const int *);

EXTERNL int NCZ_inq_typeid(int, const char *, nc_type *);

EXTERNL int NCZ_inq_compound_field(int, nc_type, int, char *, size_t *, nc_type *, int *, int *);

EXTERNL int NCZ_inq_compound_fieldindex(int, nc_type, const char *, int *);

EXTERNL int NCZ_def_vlen(int, const char *, nc_type base_typeid, nc_type *);

EXTERNL int NCZ_put_vlen_element(int, int, void *, size_t, const void *);

EXTERNL int NCZ_get_vlen_element(int, int, const void *, size_t *, void *);

EXTERNL int NCZ_def_enum(int, nc_type, const char *, nc_type *);

EXTERNL int NCZ_insert_enum(int, nc_type, const char *, const void *);

EXTERNL int NCZ_inq_enum_member(int, nc_type, int, char *, void *);

EXTERNL int NCZ_inq_enum_ident(int, nc_type, long long, char *);

EXTERNL int NCZ_def_opaque(int, size_t, const char *, nc_type *);

EXTERNL int NCZ_def_var_deflate(int, int, int, int, int);

EXTERNL int NCZ_def_var_fletcher32(int, int, int);

EXTERNL int NCZ_def_var_chunking(int, int, int, const size_t *);

EXTERNL int NCZ_def_var_fill(int, int, int, const void *);

EXTERNL int NCZ_def_var_endian(int, int, int);

EXTERNL int NCZ_inq_unlimdims(int, int *, int *);

EXTERNL int NCZ_def_var_filter(int ncid, int varid, unsigned int filterid, size_t nparams, const unsigned int *params);
EXTERNL int NCZ_inq_var_filter_ids(int ncid, int varid, size_t* nfiltersp, unsigned int *filterids);
EXTERNL int NCZ_inq_var_filter_info(int ncid, int varid, unsigned int filterid, size_t* nparamsp, unsigned int *params);
EXTERNL int NCZ_inq_filter_avail(int ncid, unsigned id);

EXTERNL int NCZ_def_var_quantize(int ncid, int varid, int quantize_mode, int nsd);
EXTERNL int NCZ_inq_var_quantize(int ncid, int varid, int *quantize_modep, int *nsdp);

/**************************************************/
/* Following functions wrap libsrc4 */
EXTERNL int NCZ_inq_type(int ncid, nc_type xtype, char *name, size_t *size);
EXTERNL int NCZ_inq_dimid(int ncid, const char *name, int *idp);
EXTERNL int NCZ_inq_unlimdim(int ncid,  int *unlimdimidp);
EXTERNL int NCZ_inq_attname(int ncid, int varid, int attnum, char *name);
EXTERNL int NCZ_inq_varid(int ncid, const char* name, int *varidp);
EXTERNL int NCZ_inq_ncid(int ncid, const char* name, int* grpidp);
EXTERNL int NCZ_inq_grps(int ncid, int* n, int* ncids);
EXTERNL int NCZ_inq_grpname(int ncid, char* name);
EXTERNL int NCZ_inq_grpname_full(int ncid, size_t* lenp, char* fullname);
EXTERNL int NCZ_inq_grp_parent(int ncid, int* parentidp);
EXTERNL int NCZ_inq_grp_full_ncid(int ncid, const char* fullname, int* grpidp);
EXTERNL int NCZ_inq_varids(int ncid, int* nvars, int* varids);
EXTERNL int NCZ_inq_dimids(int ncid, int* ndims, int* dimids, int inclparents);
EXTERNL int NCZ_inq_typeids(int ncid, int* ntypes, int* typeids);
EXTERNL int NCZ_inq_user_type(int ncid, nc_type xtype, char* name, size_t* size, nc_type* basetid, size_t* nfields, int* classp);

/**************************************************/

#if defined(__cplusplus)
}
#endif

#endif /* ZDISPATCH_H */
