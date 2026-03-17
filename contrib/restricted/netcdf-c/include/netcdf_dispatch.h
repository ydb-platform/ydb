/* Copyright 2019 University Corporation for Atmospheric
   Research/Unidata. */

/*
 * In order to use any of the netcdf_XXX.h files, it is necessary
 * to include netcdf.h followed by any netcdf_XXX.h files.
 * Various things (like EXTERNL) are defined in netcdf.h
 * to make them available for use by the netcdf_XXX.h files.
 */

/**
 * @file
 * This header contains the definition of the dispatch table. This
 * table contains a pointer to every netcdf function. When a file is
 * opened or created, the dispatch code in libdispatch decides which
 * dispatch table to use, and all subsequent netCDF calls for that
 * file will use the selected dispatch table. There are dispatch
 * tables for HDF5, HDF4, pnetcdf, etc.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#ifndef NETCDF_DISPATCH_H
#define NETCDF_DISPATCH_H

/* This is the version of the dispatch table. It should be changed
 * when new functions are added to the dispatch table. */
#ifndef NC_DISPATCH_VERSION
#define NC_DISPATCH_VERSION 5
#endif /*NC_DISPATCH_VERSION*/

/* This is the dispatch table, with a pointer to each netCDF
 * function. */
struct NC_Dispatch
{
    int model; /* one of the NC_FORMATX #'s */
    int dispatch_version;

    int (*create)(const char *path, int cmode, size_t initialsz,
                  int basepe, size_t *chunksizehintp, void *parameters,
                  const struct NC_Dispatch *table, int ncid);
    int (*open)(const char *path, int mode, int basepe, size_t *chunksizehintp,
                void *parameters, const struct NC_Dispatch *table, int ncid);

    int (*redef)(int);
    int (*_enddef)(int,size_t,size_t,size_t,size_t);
    int (*sync)(int);
    int (*abort)(int);
    int (*close)(int, void *);
    int (*set_fill)(int, int, int *);
    int (*inq_format)(int, int*);
    int (*inq_format_extended)(int, int *, int *);

    int (*inq)(int, int *, int *, int *, int *);
    int (*inq_type)(int, nc_type, char *, size_t *);

    int (*def_dim)(int, const char *, size_t, int *);
    int (*inq_dimid)(int, const char *, int *);
    int (*inq_dim)(int, int, char *, size_t *);
    int (*inq_unlimdim)(int ncid,  int *unlimdimidp);
    int (*rename_dim)(int, int, const char *);

    int (*inq_att)(int, int, const char *, nc_type *, size_t *);
    int (*inq_attid)(int, int, const char *, int*);
    int (*inq_attname)(int, int, int, char *);
    int (*rename_att)(int, int, const char *, const char *);
    int (*del_att)(int, int, const char *);
    int (*get_att)(int, int, const char *, void *, nc_type);
    int (*put_att)(int, int, const char *, nc_type, size_t, const void *,
                   nc_type);

    int (*def_var)(int, const char *, nc_type, int, const int *, int *);
    int (*inq_varid)(int, const char *, int *);
    int (*rename_var)(int, int, const char *);

    int (*get_vara)(int, int, const size_t *, const size_t *, void *, nc_type);
    int (*put_vara)(int, int, const size_t *, const size_t *,
                    const void *, nc_type);

    int (*get_vars)(int, int, const size_t *, const size_t *, const ptrdiff_t *,
                    void *, nc_type);
    int (*put_vars)(int, int, const size_t *, const size_t *, const ptrdiff_t *,
                    const void *, nc_type);

    int (*get_varm)(int, int, const size_t *, const size_t *, const ptrdiff_t *,
                    const ptrdiff_t *, void *, nc_type);
    int (*put_varm)(int, int, const size_t *, const size_t *, const ptrdiff_t *,
                    const ptrdiff_t *, const void *, nc_type);

    int (*inq_var_all)(int ncid, int varid, char *name, nc_type *xtypep,
                       int *ndimsp, int *dimidsp, int *nattsp,
                       int *shufflep, int *deflatep, int *deflate_levelp,
                       int *fletcher32p, int *contiguousp, size_t *chunksizesp,
                       int *no_fill, void *fill_valuep, int *endiannessp,
                       unsigned int *idp, size_t *nparamsp,
                       unsigned int *params);

    int (*var_par_access)(int, int, int);
    int (*def_var_fill)(int, int, int, const void *);

/* Note the following are specific to netcdf4, but must still be
   implemented by all dispatch tables. They may still be invoked by
   netcdf client code even when the file is a classic file; they
   will just return an error or be ignored.
*/

    int (*show_metadata)(int);
    int (*inq_unlimdims)(int, int*, int*);
    int (*inq_ncid)(int, const char*, int*);
    int (*inq_grps)(int, int*, int*);
    int (*inq_grpname)(int, char*);
    int (*inq_grpname_full)(int, size_t*, char*);
    int (*inq_grp_parent)(int, int*);
    int (*inq_grp_full_ncid)(int, const char*, int*);
    int (*inq_varids)(int, int* nvars, int*);
    int (*inq_dimids)(int, int* ndims, int*, int);
    int (*inq_typeids)(int, int* ntypes, int*);
    int (*inq_type_equal)(int, nc_type, int, nc_type, int*);
    int (*def_grp)(int, const char*, int*);
    int (*rename_grp)(int, const char*);
    int (*inq_user_type)(int, nc_type, char*, size_t*, nc_type*, size_t*, int*);
    int (*inq_typeid)(int, const char*, nc_type*);

    int (*def_compound)(int, size_t, const char *, nc_type *);
    int (*insert_compound)(int, nc_type, const char *, size_t, nc_type);
    int (*insert_array_compound)(int, nc_type, const char *, size_t, nc_type,
                                 int, const int *);
    int (*inq_compound_field)(int, nc_type, int, char *, size_t *, nc_type *,
                              int *, int *);
    int (*inq_compound_fieldindex)(int, nc_type, const char *, int *);
    int (*def_vlen)(int, const char *, nc_type base_typeid, nc_type *);
    int (*put_vlen_element)(int, int, void *, size_t, const void *);
    int (*get_vlen_element)(int, int, const void *, size_t *, void *);
    int (*def_enum)(int, nc_type, const char *, nc_type *);
    int (*insert_enum)(int, nc_type, const char *, const void *);
    int (*inq_enum_member)(int, nc_type, int, char *, void *);
    int (*inq_enum_ident)(int, nc_type, long long, char *);
    int (*def_opaque)(int, size_t, const char *, nc_type*);
    int (*def_var_deflate)(int, int, int, int, int);
    int (*def_var_fletcher32)(int, int, int);
    int (*def_var_chunking)(int, int, int, const size_t *);
    int (*def_var_endian)(int, int, int);
    int (*def_var_filter)(int, int, unsigned int, size_t, const unsigned int *);
    int (*set_var_chunk_cache)(int, int, size_t, size_t, float);
    int (*get_var_chunk_cache)(int ncid, int varid, size_t *sizep,
                               size_t *nelemsp, float *preemptionp);
    /* Version 3 Replace filteractions with more specific functions */
    int (*inq_var_filter_ids)(int ncid, int varid, size_t* nfilters, unsigned int* filterids);
    int (*inq_var_filter_info)(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params);
    /* Version 4 Add quantization. */
    int (*def_var_quantize)(int ncid, int varid, int quantize_mode, int nsd);
    int (*inq_var_quantize)(int ncid, int varid, int *quantize_modep, int *nsdp);
    /* Version 5 adds filter availability */
    int (*inq_filter_avail)(int ncid, unsigned id);
};

#if defined(__cplusplus)
extern "C" {
#endif

    /* Read-only dispatch layers can use these functions to return
     * NC_EPERM to all attempts to modify a file. */
    EXTERNL int NC_RO_create(const char *path, int cmode, size_t initialsz, int basepe,
                             size_t *chunksizehintp, void* parameters,
                             const NC_Dispatch*, int);
    EXTERNL int NC_RO_redef(int ncid);
    EXTERNL int NC_RO__enddef(int ncid, size_t h_minfree, size_t v_align, size_t v_minfree,
                              size_t r_align);
    EXTERNL int NC_RO_sync(int ncid);
    EXTERNL int NC_RO_def_var_fill(int, int, int, const void *);
    EXTERNL int NC_RO_rename_att(int ncid, int varid, const char *name,
                                 const char *newname);
    EXTERNL int NC_RO_del_att(int ncid, int varid, const char*);
    EXTERNL int NC_RO_put_att(int ncid, int varid, const char *name, nc_type datatype,
                              size_t len, const void *value, nc_type);
    EXTERNL int NC_RO_def_var(int ncid, const char *name,
                              nc_type xtype, int ndims, const int *dimidsp, int *varidp);
    EXTERNL int NC_RO_rename_var(int ncid, int varid, const char *name);
    EXTERNL int NC_RO_put_vara(int ncid, int varid,
                               const size_t *start, const size_t *count,
                               const void *value, nc_type);
    EXTERNL int NC_RO_def_dim(int ncid, const char *name, size_t len, int *idp);
    EXTERNL int NC_RO_rename_dim(int ncid, int dimid, const char *name);
    EXTERNL int NC_RO_set_fill(int ncid, int fillmode, int *old_modep);

    /* These functions are for dispatch layers that don't implement
     * the enhanced model. They return NC_ENOTNC4. */
    EXTERNL int NC_NOTNC4_def_var_filter(int, int, unsigned int, size_t,
                                         const unsigned int*);
    EXTERNL int NC_NOTNC4_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids);
    EXTERNL int NC_NOTNC4_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params);
    EXTERNL int NC_NOOP_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids);
    EXTERNL int NC_NOOP_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params);
    EXTERNL int NC_NOOP_inq_filter_avail(int ncid, unsigned id);

    EXTERNL int NC_NOTNC4_def_grp(int, const char *, int *);
    EXTERNL int NC_NOTNC4_rename_grp(int, const char *);
    EXTERNL int NC_NOTNC4_def_compound(int, size_t, const char *, nc_type *);
    EXTERNL int NC_NOTNC4_insert_compound(int, nc_type, const char *, size_t, nc_type);
    EXTERNL int NC_NOTNC4_insert_array_compound(int, nc_type, const char *, size_t,
                                                nc_type, int, const int *);
    EXTERNL int NC_NOTNC4_inq_typeid(int, const char *, nc_type *);
    EXTERNL int NC_NOTNC4_inq_compound_field(int, nc_type, int, char *, size_t *,
                                             nc_type *, int *, int *);
    EXTERNL int NC_NOTNC4_inq_compound_fieldindex(int, nc_type, const char *, int *);
    EXTERNL int NC_NOTNC4_def_vlen(int, const char *, nc_type base_typeid, nc_type *);
    EXTERNL int NC_NOTNC4_put_vlen_element(int, int, void *, size_t, const void *);
    EXTERNL int NC_NOTNC4_get_vlen_element(int, int, const void *, size_t *, void *);
    EXTERNL int NC_NOTNC4_def_enum(int, nc_type, const char *, nc_type *);
    EXTERNL int NC_NOTNC4_insert_enum(int, nc_type, const char *, const void *);
    EXTERNL int NC_NOTNC4_inq_enum_member(int, nc_type, int, char *, void *);
    EXTERNL int NC_NOTNC4_inq_enum_ident(int, nc_type, long long, char *);
    EXTERNL int NC_NOTNC4_def_opaque(int, size_t, const char *, nc_type *);
    EXTERNL int NC_NOTNC4_def_var_deflate(int, int, int, int, int);
    EXTERNL int NC_NOTNC4_def_var_fletcher32(int, int, int);
    EXTERNL int NC_NOTNC4_def_var_chunking(int, int, int, const size_t *);
    EXTERNL int NC_NOTNC4_def_var_endian(int, int, int);
    EXTERNL int NC_NOTNC4_set_var_chunk_cache(int, int, size_t, size_t, float);
    EXTERNL int NC_NOTNC4_get_var_chunk_cache(int, int, size_t *, size_t *, float *);
    EXTERNL int NC_NOTNC4_var_par_access(int, int, int);
    EXTERNL int NC_NOTNC4_inq_ncid(int, const char *, int *);
    EXTERNL int NC_NOTNC4_inq_grps(int, int *, int *);
    EXTERNL int NC_NOTNC4_inq_grpname(int, char *);
    EXTERNL int NC_NOTNC4_inq_grpname_full(int, size_t *, char *);
    EXTERNL int NC_NOTNC4_inq_grp_parent(int, int *);
    EXTERNL int NC_NOTNC4_inq_grp_full_ncid(int, const char *, int *);
    EXTERNL int NC_NOTNC4_inq_varids(int, int *, int *);
    EXTERNL int NC_NOTNC4_inq_dimids(int, int *, int *, int);
    EXTERNL int NC_NOTNC4_inq_typeids(int, int *, int *);
    EXTERNL int NC_NOTNC4_inq_user_type(int, nc_type, char *, size_t *,
                                        nc_type *, size_t *, int *);
    EXTERNL int NC_NOTNC4_def_var_quantize(int, int,  int, int);
    EXTERNL int NC_NOTNC4_inq_var_quantize(int, int,  int *, int *);
    
    /* These functions are for dispatch layers that don't implement
     * the enhanced model, but want to succeed anyway.
     * They return NC_NOERR plus properly set the out parameters.
     */

    /* These functions are for dispatch layers that don't want to
     * implement the deprecated varm functions. They return
     * NC_ENOTNC3. */
    EXTERNL int NC_NOTNC3_get_varm(int ncid, int varid, const size_t *start,
				   const size_t *edges, const ptrdiff_t *stride,
				   const ptrdiff_t *imapp, void *value0, nc_type memtype);
    EXTERNL int NC_NOTNC3_put_varm(int ncid, int varid, const size_t * start,
				   const size_t *edges, const ptrdiff_t *stride,
				   const ptrdiff_t *imapp, const void *value0,
				   nc_type memtype);

#if defined(__cplusplus)
}
#endif

#endif /* NETCDF_DISPATCH_H */
