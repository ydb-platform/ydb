/* Copyright 2018, UCAR/Unidata See netcdf/COPYRIGHT file for copying
 * and redistribution conditions.*/
/**
 * @file
 * @internal This file contains functions that 
 * can be used in dispatch tables to handle
 * such things as unimplemented functions.
 * As a rule, these functions will return NC_ENOTNC4.
 * In some cases, the function may succeed and set
 * appropriate output parameter values. These are indicated
 * by the NC_NOOP prefix to the function name.
 *
 * @author Ed Hartnett, Dennis Heimbigner
 */

#include "netcdf.h"
#include "netcdf_filter.h"
#include "netcdf_aux.h"
#include "ncdispatch.h"
#include "nc4internal.h"

/**
 * @internal Not implemented in some dispatch tables
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param quantize_mode Ignored.
 * @param nsd Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_var_quantize(int ncid, int varid,  int quantize_mode, int nsd)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented in some dispatch tables
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param quantize_modep Ignored.
 * @param nsdp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_var_quantize(int ncid, int varid,  int *quantize_modep,
			   int *nsdp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented in some dispatch tables
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param id Ignored.
 * @param nparams Ignored.
 * @param params Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_var_filter(int ncid, int varid, unsigned int  id, size_t nparams, const unsigned int* params)
{
    return NC_ENOTNC4;
}

int
NC_NOTNC4_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids)
{
    return NC_ENOTNC4;
}

int
NC_NOTNC4_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params)
{
    return NC_ENOTNC4;
}

int
NC_NOOP_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids)
{
    if(nfilters) *nfilters = 0;
    return NC_NOERR;
}

int
NC_NOOP_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params)
{
    NC_UNUSED(ncid);
    return NC_ENOFILTER;
}

int
NC_NOOP_inq_filter_avail(int ncid, unsigned id)
{
    NC_UNUSED(ncid);
    NC_UNUSED(id);
    return NC_ENOFILTER;
}

/**
 * @internal Not allowed for classic model.
 *
 * @param parent_ncid Ignored.
 * @param name Ignored.
 * @param new_ncid Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_grp(int parent_ncid, const char *name, int *new_ncid)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param grpid Ignored.
 * @param name Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_rename_grp(int grpid, const char *name)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param size Ignored.
 * @param name Ignored.
 * @param typeidp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_compound(int ncid, size_t size, const char *name, nc_type *typeidp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param name Ignored.
 * @param offset Ignored.
 * @param field Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_insert_compound(int ncid, nc_type typeid1, const char *name, size_t offset,
                          nc_type field_typeid)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param name Ignored.
 * @param offset Ignored.
 * @param field Ignored.
 * @param ndims Ignored.
 * @param dim Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
extern int
NC_NOTNC4_insert_array_compound(int ncid, int typeid1, const char *name,
                                size_t offset, nc_type field_typeid,
                                int ndims, const int *dim_sizesp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param fieldid Ignored.
 * @param name Ignored.
 * @param offsetp Ignored.
 * @param field Ignored.
 * @param ndimsp Ignored.
 * @param dim Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_compound_field(int ncid, nc_type typeid1, int fieldid, char *name,
                             size_t *offsetp, nc_type *field_typeidp, int *ndimsp,
                             int *dim_sizesp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param name Ignored.
 * @param fieldidp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_compound_fieldindex(int ncid, nc_type typeid1, const char *name, int *fieldidp)
{
    return NC_ENOTNC4;
}

/* Opaque type. */

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param datum Ignored.
 * @param name Ignored.
 * @param typeidp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_opaque(int ncid, size_t datum_size, const char *name,
                     nc_type *typeidp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param name Ignored.
 * @param base_typeid Ignored.
 * @param typeidp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_vlen(int ncid, const char *name, nc_type base_typeid,
                   nc_type *typeidp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param base_typeid Ignored.
 * @param name Ignored.
 * @param typeidp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_enum(int ncid, nc_type base_typeid, const char *name,
                   nc_type *typeidp)
{
    return NC_ENOTNC4;
}


/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param xtype Ignored.
 * @param value Ignored.
 * @param identifier Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_enum_ident(int ncid, nc_type xtype, long long value, char *identifier)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param idx Ignored.
 * @param identifier Ignored.
 * @param value Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_enum_member(int ncid, nc_type typeid1, int idx, char *identifier,
                          void *value)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param identifier Ignored.
 * @param value Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_insert_enum(int ncid, nc_type typeid1, const char *identifier,
                      const void *value)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param vlen_element Ignored.
 * @param len Ignored.
 * @param data Ignored.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_put_vlen_element(int ncid, int typeid1, void *vlen_element,
                           size_t len, const void *data)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param vlen_element Ignored.
 * @param len Ignored.
 * @param data Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_get_vlen_element(int ncid, int typeid1, const void *vlen_element,
                           size_t *len, void *data)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param size Ignored.
 * @param nelems Ignored.
 * @param preemption Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems,
                              float preemption)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param sizep Ignored.
 * @param nelemsp Ignored.
 * @param preemptionp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_get_var_chunk_cache(int ncid, int varid, size_t *sizep,
                              size_t *nelemsp, float *preemptionp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param shuffle Ignored.
 * @param deflate Ignored.
 * @param deflate_level Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC_NOTNC4_def_var_deflate(int ncid, int varid, int shuffle, int deflate,
                          int deflate_level)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param fletcher32 Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC_NOTNC4_def_var_fletcher32(int ncid, int varid, int fletcher32)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param contiguous Ignored.
 * @param chunksizesp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC_NOTNC4_def_var_chunking(int ncid, int varid, int contiguous, const size_t *chunksizesp)
{
    return NC_EPERM;
}


/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param endianness Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_def_var_endian(int ncid, int varid, int endianness)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param varid Ignored.
 * @param par_access Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC_NOTNC4_var_par_access(int ncid, int varid, int par_access)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param name Ignored.
 * @param grp_ncid Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_ncid(int ncid, const char *name, int *grp_ncid)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param numgrps Ignored.
 * @param ncids Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_grps(int ncid, int *numgrps, int *ncids)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param name Ignored.

 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_grpname(int ncid, char *name)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param lenp Ignored.
 * @param full_name Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_grpname_full(int ncid, size_t *lenp, char *full_name)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param parent_ncid Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_grp_parent(int ncid, int *parent_ncid)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param full_name Ignored.
 * @param grp_ncid Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_grp_full_ncid(int ncid, const char *full_name, int *grp_ncid)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param nvars Ignored.
 * @param varids Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_varids(int ncid, int *nvars, int *varids)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param ndims Ignored.
 * @param dimids Ignored.
 * @param include_parents Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_dimids(int ncid, int *ndims, int *dimids, int include_parents)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param ntypes Ignored.
 * @param typeids Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_typeids(int ncid, int *ntypes, int *typeids)
{
    return NC_ENOTNC4;
}

/**
 * @internal Not implemented for a dispatch table.
 *
 * @param ncid Ignored.
 * @param typeid1 Ignored.
 * @param name Ignored.
 * @param size Ignored.
 * @param base_nc_typep Ignored.
 * @param nfieldsp Ignored.
 * @param classp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_user_type(int ncid, nc_type typeid1, char *name, size_t *size,
                        nc_type *base_nc_typep, size_t *nfieldsp, int *classp)
{
    return NC_ENOTNC4;
}

/**
 * @internal Get the id of a type from the name.
 *
 * @param ncid Ignored.
 * @param name Ignored.
 * @param typeidp Ignored.
 *
 * @return ::NC_ENOTNC4 Not implemented for a dispatch table.
 * @author Ed Hartnett
 */
int
NC_NOTNC4_inq_typeid(int ncid, const char *name, nc_type *typeidp)
{
    /* Note that this should actually work for atomic types */
    return NC_ENOTNC4;
}
