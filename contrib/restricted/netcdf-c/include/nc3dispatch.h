/*
 * Copyright 2018-1996 University Corporation for Atmospheric Research/Unidata
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
/* "$Id: nc3dispatch.h,v 2.8 2010/05/26 18:11:08 dmh Exp $" */

#ifndef _NC3DISPATCH_H
#define _NC3DISPATCH_H

#include <stddef.h> /* size_t, ptrdiff_t */
#include "netcdf.h"
#include "ncdispatch.h"



#if defined(__cplusplus)
extern "C" {
#endif

/*
 * The Interface
 */

/* WARNING: this signature differs from external nc_create API*/
    extern int
    NC3_create(const char *path, int cmode,
               size_t initialsz, int basepe, size_t *chunksizehintp,
               void* mpidata, const struct NC_Dispatch*, int ncid);

/* WARNING: this signature differs from external nc_open API*/
    extern int
    NC3_open(const char *path, int mode,
             int basepe, size_t *chunksizehintp,
             void* mpidata, const NC_Dispatch*, int ncid);

    extern int
    NC3_new_nc(NC**);

    extern int
    NC3_redef(int ncid);

    extern int
    NC3__enddef(int ncid, size_t h_minfree, size_t v_align,
                size_t v_minfree, size_t r_align);

    extern int
    NC3_sync(int ncid);

    extern int
    NC3_abort(int ncid);

    extern int
    NC3_close(int ncid,void*);

    extern int
    NC3_set_fill(int ncid, int fillmode, int *old_modep);

    extern int
    NC3_inq_format(int ncid, int *formatp);

    extern int
    NC3_inq_format_extended(int ncid, int *formatp, int *modep);

    extern int
    NC3_inq(int ncid, int *ndimsp, int *nvarsp, int *nattsp, int *unlimdimidp);

    extern int
    NC3_inq_type(int, nc_type, char *, size_t *);

/* Begin _dim */

    extern int
    NC3_def_dim(int ncid, const char *name, size_t len, int *idp);

    extern int
    NC3_inq_dimid(int ncid, const char *name, int *idp);

    extern int
    NC3_inq_dim(int ncid, int dimid, char *name, size_t *lenp);

    extern int
    NC3_inq_unlimdim(int ncid, int *unlimdimidp);

    extern int
    NC3_rename_dim(int ncid, int dimid, const char *name);

/* End _dim */
/* Begin _att */

    extern int
    NC3_inq_att(int ncid, int varid, const char *name,
                nc_type *xtypep, size_t *lenp);

    extern int
    NC3_inq_attid(int ncid, int varid, const char *name, int *idp);

    extern int
    NC3_inq_attname(int ncid, int varid, int attnum, char *name);

    extern int
    NC3_rename_att(int ncid, int varid, const char *name, const char *newname);

    extern int
    NC3_del_att(int ncid, int varid, const char*);

/* End _att */
/* Begin {put,get}_att */

    extern int
    NC3_get_att(int ncid, int varid, const char *name, void *value, nc_type);

    extern int
    NC3_put_att(int ncid, int varid, const char *name, nc_type datatype,
                size_t len, const void *value, nc_type);

/* End {put,get}_att */
/* Begin _var */

    extern int
    NC3_def_var(int ncid, const char *name,
                nc_type xtype, int ndims, const int *dimidsp, int *varidp);

    extern int
    NC3_inq_var(int ncid, int varid, char *name,
                nc_type *xtypep, int *ndimsp, int *dimidsp, int *nattsp,
                int *no_fill, void *fill_valuep);

    extern int
    NC3_inq_varid(int ncid, const char *name, int *varidp);

    extern int
    NC3_rename_var(int ncid, int varid, const char *name);

    extern int
    NC3_def_var_fill(int,int,int,const void*);

    extern int
    NC3_put_vara(int ncid, int varid,
                 const size_t *start, const size_t *count,
                 const void *value, nc_type);

    extern int
    NC3_get_vara(int ncid, int varid,
                 const size_t *start, const size_t *count,
                 void *value, nc_type);

/* End _var */

    extern int NC3_initialize(void);
    extern int NC3_finalize(void);

#if defined(__cplusplus)
}
#endif

#endif /*_NC3DISPATCH_H*/
