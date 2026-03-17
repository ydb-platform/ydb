/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef _NCD4DISPATCH_H
#define _NCD4DISPATCH_H

#include <stddef.h> /* size_t, ptrdiff_t */
#include "netcdf.h"
#include "ncdispatch.h"

#if defined(__cplusplus)
extern "C" {
#endif

extern int
NCD4_open(const char *path, int mode,
         int basepe, size_t *chunksizehintp,
         void *mpidata, const struct NC_Dispatch *dispatch, int ncid);

extern int
NCD4_close(int ncid,void*);

extern int
NCD4_abort(int ncid);

extern int
NCD4_inq_dim(int ncid, int dimid, char* name, size_t* lenp);

extern int
NCD4_get_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges,
            void *value,
	    nc_type memtype);

extern int
NCD4_get_vars(int ncid, int varid,
	    const size_t *start, const size_t *edges, const ptrdiff_t* strides,
            void *value,
	    nc_type memtype);

extern int NCD4_initialize(void);

#if defined(__cplusplus)
}
#endif

#endif /*_NCD4DISPATCH_H*/
