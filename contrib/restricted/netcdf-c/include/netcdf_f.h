/*
 * Copyright 2018-2018 University Corporation for Atmospheric Research/Unidata
 *
 * This header contains prototypes for functions only called by fortran 77.
 */
#ifndef _NETCDF_F_
#define _NETCDF_F_

#include <netcdf.h>

#if defined(__cplusplus)
extern "C" {
#endif

EXTERNL int
nc_inq_var_chunking_ints(int ncid, int varid, int *contiguousp, int *chunksizesp);

EXTERNL int
nc_def_var_chunking_ints(int ncid, int varid, int contiguous, int *chunksizesp);

EXTERNL int
nc_open_par_fortran(const char *path, int mode, int comm,
		    int info, int *ncidp);

EXTERNL int
nc_create_par_fortran(const char *path, int cmode, int comm,
		      int info, int *ncidp);

EXTERNL int
nc_set_chunk_cache_ints(int size, int nelems, int preemption);

EXTERNL int
nc_get_chunk_cache_ints(int *sizep, int *nelemsp, int *preemptionp);

EXTERNL int
nc_set_var_chunk_cache_ints(int ncid, int varid, int size, int nelems,
			    int preemption);
EXTERNL int
nc_get_var_chunk_cache_ints(int ncid, int varid, int *sizep,
			    int *nelemsp, int *preemptionp);

/* Prototypes for some extra functions in fort-lib.c. */
EXTERNL int
nc_inq_varids_f(int ncid, int *nvars, int *fvarids);

EXTERNL int
nc_inq_dimids_f(int ncid, int *ndims, int *fdimids, int parent);

EXTERNL int
nc_insert_array_compound_f(int ncid, int typeid1, char *name,
			   size_t offset, nc_type field_typeid,
			   int ndims, int *dim_sizesp);

EXTERNL int
nc_inq_compound_field_f(int ncid, nc_type xtype, int fieldid, char *name,
			size_t *offsetp, nc_type *field_typeidp, int *ndimsp,
			int *dim_sizesp);

#if defined(__cplusplus)
}
#endif

#endif
