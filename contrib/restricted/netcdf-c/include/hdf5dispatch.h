/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal Includes prototypes for libhdf5 dispatch functions.
 *
 * @author Ed Hartnett
 */

#ifndef _HDF5DISPATCH_H
#define _HDF5DISPATCH_H

#include "config.h"
#include <stddef.h> /* size_t, ptrdiff_t */
#include "ncdispatch.h"

#if defined(__cplusplus)
extern "C" {
#endif

    EXTERNL int
    NC4_HDF5_inq_att(int ncid, int varid, const char *name,
                     nc_type *xtypep, size_t *lenp);

    EXTERNL int
    NC4_HDF5_inq_attid(int ncid, int varid, const char *name, int *idp);

    EXTERNL int
    NC4_HDF5_inq_attname(int ncid, int varid, int attnum, char *name);

    EXTERNL int
    NC4_HDF5_rename_att(int ncid, int varid, const char *name, const char *newname);

    EXTERNL int
    NC4_HDF5_del_att(int ncid, int varid, const char*);

    EXTERNL int
    NC4_HDF5_put_att(int ncid, int varid, const char *name, nc_type datatype,
                     size_t len, const void *value, nc_type);

    EXTERNL int
    NC4_HDF5_get_att(int ncid, int varid, const char *name, void *value, nc_type);

    EXTERNL int
    NC4_HDF5_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
                         int *ndimsp, int *dimidsp, int *nattsp,
                         int *shufflep, int *deflatep, int *deflate_levelp,
                         int *fletcher32p, int *contiguousp, size_t *chunksizesp,
                         int *no_fill, void *fill_valuep, int *endiannessp,
                         unsigned int *idp, size_t *nparamsp, unsigned int *params);

    EXTERNL int
    NC4_HDF5_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems,
                                 float preemption);

    EXTERNL int
    HDF5_def_dim(int ncid, const char *name, size_t len, int *idp);

    EXTERNL int
    HDF5_inq_dim(int ncid, int dimid, char *name, size_t *lenp);

    EXTERNL int
    HDF5_rename_dim(int ncid, int dimid, const char *name);

#if defined(__cplusplus)
}
#endif

#endif /*_HDF5DISPATCH_H */
