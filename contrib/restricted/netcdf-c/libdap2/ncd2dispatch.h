/*
* Copyright 1993-2018 University Corporation for Atmospheric Research/Unidata
*
* Portions of this software were developed by the Unidata Program at the
* University Corporation for Atmospheric Research.
*
* Access and use of this software shall impose the following obligations
* and understandings on the user. The user is granted the right, without
* any fee or cost, to use, copy, modify, alter, enhance and distribute
* this software, and any derivative works thereof, and its supporting
* documentation for any purpose whatsoever, provided that this entire
* notice appears in all copies of the software, derivative works and
* supporting documentation.  Further, UCAR requests that the user credit
* UCAR/Unidata in any publications that result from the use of this
* software or in any product that includes this software. The names UCAR
* and/or Unidata, however, may not be used in any advertising or publicity
* to endorse or promote any products or commercial entity unless specific
* written permission is obtained from UCAR/Unidata. The user also
* understands that UCAR/Unidata is not obligated to provide the user with
* any support, consulting, training or assistance of any kind with regard
* to the use, operation and performance of this software nor to provide
* the user with any updates, revisions, new versions or "bug fixes."
*
* THIS SOFTWARE IS PROVIDED BY UCAR/UNIDATA "AS IS" AND ANY EXPRESS OR
* IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL UCAR/UNIDATA BE LIABLE FOR ANY SPECIAL,
* INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING
* FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
* NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION
* WITH THE ACCESS, USE OR PERFORMANCE OF THIS SOFTWARE.
*/

#ifndef _NCD2DISPATCH_H
#define _NCD2DISPATCH_H

#include <stddef.h> /* size_t, ptrdiff_t */
#include "netcdf.h"
#include "ncdispatch.h"
#include "config.h"

#if defined(__cplusplus)
extern "C" {
#endif

extern int
NCD2_open(const char *path, int mode,
       int basepe, size_t *chunksizehintp,
       void* mpidata, const struct NC_Dispatch* dispatch, int ncid);

extern int
NCD2_close(int ncid,void*);

extern int
NCD2_inq_format_extended(int ncid, int* formatp, int* modep);

extern int
NCD2_set_fill(int ncid, int fillmode, int *old_modep);

extern int
NCD2_inq_format(int ncid, int *formatp);

extern int
NCD2_inq_format_extended(int ncid, int *formatp, int *modep);

extern int
NCD2_inq(int ncid, int *ndimsp, int *nvarsp, int *nattsp, int *unlimdimidp);

extern int
NCD2_inq_type(int, nc_type, char *, size_t *);

/* Begin _dim */

extern int
NCD2_def_dim(int ncid, const char *name, size_t len, int *idp);

extern int
NCD2_inq_dimid(int ncid, const char *name, int *idp);

extern int
NCD2_inq_dim(int ncid, int dimid, char *name, size_t *lenp);

extern int
NCD2_inq_unlimdim(int ncid, int *unlimdimidp);

extern int
NCD2_rename_dim(int ncid, int dimid, const char *name);

/* End _dim */
/* Begin _att */

extern int
NCD2_inq_att(int ncid, int varid, const char *name,
	    nc_type *xtypep, size_t *lenp);

extern int
NCD2_inq_attid(int ncid, int varid, const char *name, int *idp);

extern int
NCD2_inq_attname(int ncid, int varid, int attnum, char *name);

extern int
NCD2_rename_att(int ncid, int varid, const char *name, const char *newname);

extern int
NCD2_del_att(int ncid, int varid, const char*);

/* End _att */
/* Begin {put,get}_att */

extern int
NCD2_get_att(int ncid, int varid, const char *name, void *value, nc_type);

extern int
NCD2_put_att(int ncid, int varid, const char *name, nc_type datatype,
	   size_t len, const void *value, nc_type);

/* End {put,get}_att */
/* Begin _var */

extern int
NCD2_def_var(int ncid, const char *name,
	 nc_type xtype, int ndims, const int *dimidsp, int *varidp);

extern int
NCD2_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
             int *ndimsp, int *dimidsp, int *nattsp,
             int *shufflep, int *deflatep, int *deflate_levelp,
             int *fletcher32p, int *contiguousp, size_t *chunksizesp,
             int *no_fill, void *fill_valuep, int *endiannessp,
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
             );

extern int
NC3_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep,
             int *ndimsp, int *dimidsp, int *nattsp,
             int *shufflep, int *deflatep, int *deflate_levelp,
             int *fletcher32p, int *contiguousp, size_t *chunksizesp,
             int *no_fill, void *fill_valuep, int *endiannessp,
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
             );

extern int
NCD2_inq_varid(int ncid, const char *name, int *varidp);

extern int
NCD2_rename_var(int ncid, int varid, const char *name);

/* End _var */

extern int
NCD2_var_par_access(int, int, int);

extern int
NCD2_def_var_fill(int, int, int, const void *);

/* netCDF4 API only. but still need to be defined */
extern int
NCD2_inq_ncid(int, const char *, int *);

extern int
NCD2_inq_grps(int, int *, int *);

extern int
NCD2_inq_grpname(int, char *);

extern int
NCD2_inq_grpname_full(int, size_t *, char *);

extern int
NCD2_inq_grp_parent(int, int *);

extern int
NCD2_inq_grp_full_ncid(int, const char *, int *);

extern int
NCD2_inq_varids(int, int * nvars, int *);

extern int
NCD2_inq_dimids(int, int * ndims, int *, int);

extern int
NCD2_inq_typeids(int, int * ntypes, int *);

extern int
NCD2_inq_type_equal(int, nc_type, int, nc_type, int *);

extern int
NCD2_rename_grp(int, const char *);

extern int
NCD2_inq_user_type(int, nc_type, char *, size_t *, nc_type *,
                   size_t *, int *);

extern int
NCD2_insert_compound(int, nc_type, const char *, size_t, nc_type);

extern int
NCD2_insert_array_compound(int, nc_type, const char *, size_t,
                           nc_type, int, const int *);

extern int
NCD2_inq_typeid(int, const char *, nc_type *);

extern int
NCD2_inq_compound_field(int, nc_type, int, char *, size_t *,
                        nc_type *, int *, int *);

extern int
NCD2_inq_compound_fieldindex(int, nc_type, const char *, int *);

extern int
NCD2_def_vlen(int, const char *, nc_type base_typeid, nc_type *);

extern int
NCD2_put_vlen_element(int, int, void *, size_t, const void *);

extern int
NCD2_get_vlen_element(int, int, const void *, size_t *, void *);

extern int
NCD2_insert_enum(int, nc_type, const char *, const void *);

extern int
NCD2_inq_enum_member(int, nc_type, int, char *, void *);

extern int
NCD2_inq_enum_ident(int, nc_type, long long, char *);

extern int
NCD2_def_var_deflate(int, int, int, int, int);

extern int
NCD2_def_var_fletcher32(int, int, int);

extern int
NCD2_def_var_chunking(int, int, int, const size_t *);

extern int
NCD2_def_var_endian(int, int, int);

extern int
NCD2_def_var_filter(int, int, unsigned int, size_t, const unsigned int*);

extern int
NCD2_inq_unlimdims(int, int *, int *);

extern int
NCD2_show_metadata(int);

extern int
NCD2_def_compound(int, size_t, const char *, nc_type *);

extern int
NCD2_def_enum(int, nc_type, const char *, nc_type *);

extern int
NCD2_def_grp(int, const char *, int *);

extern int
NCD2_def_opaque(int, size_t, const char *, nc_type *);

extern int
NCD2_set_var_chunk_cache(int, int, size_t, size_t, float);


extern int
NCD2_get_var_chunk_cache(int, int, size_t *, size_t *, float *);

#if defined(__cplusplus)
}
#endif

#endif /*_NCD2DISPATCH_H*/
