/*********************************************************************
   Copyright 2018, UCAR/Unidata See netcdf/COPYRIGHT file for
   copying and redistribution conditions.

   $Id: nc3dispatch.c,v 2.8 2010/05/26 11:11:26 ed Exp $
 *********************************************************************/

#include "config.h"
#include <stdlib.h>
#include <string.h>

#include "netcdf.h"
#include "nc3internal.h"
#include "nc3dispatch.h"

#ifndef NC_CONTIGUOUS
#define NC_CONTIGUOUS 1
#endif

#ifndef NC_ENOTNC4
#define NC_ENOTNC4 (-111)
#endif

#ifndef NC_ENOGRP
#define NC_ENOGRP (-125)
#endif

#ifndef NC_STRING
#define NC_STRING (12)
#endif


static int NC3_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep, 
               int *ndimsp, int *dimidsp, int *nattsp, 
               int *shufflep, int *deflatep, int *deflate_levelp,
               int *fletcher32p, int *contiguousp, size_t *chunksizesp, 
               int *no_fill, void *fill_valuep, int *endiannessp, 
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
               );

static int NC3_var_par_access(int,int,int);

static int NC3_show_metadata(int);
static int NC3_inq_unlimdims(int,int*,int*);
static int NC3_inq_ncid(int,const char*,int*);
static int NC3_inq_grps(int,int*,int*);
static int NC3_inq_grpname(int,char*);
static int NC3_inq_grpname_full(int,size_t*,char*);
static int NC3_inq_grp_parent(int,int*);
static int NC3_inq_grp_full_ncid(int,const char*,int*);
static int NC3_inq_varids(int,int* nvars,int*);
static int NC3_inq_dimids(int,int* ndims,int*,int);
static int NC3_inq_typeids(int,int* ntypes,int*);
static int NC3_inq_type_equal(int,nc_type,int,nc_type,int*);
static int NC3_def_grp(int,const char*,int*);
static int NC3_rename_grp(int,const char*);
static int NC3_inq_user_type(int,nc_type,char*,size_t*,nc_type*,size_t*,int*);
static int NC3_inq_typeid(int,const char*,nc_type*);
static int NC3_def_compound(int,size_t,const char*,nc_type*);
static int NC3_insert_compound(int,nc_type,const char*,size_t,nc_type);
static int NC3_insert_array_compound(int,nc_type,const char*,size_t,nc_type,int,const int*);
static int NC3_inq_compound_field(int,nc_type,int,char*,size_t*,nc_type*,int*,int*);
static int NC3_inq_compound_fieldindex(int,nc_type,const char*,int*);
static int NC3_def_vlen(int,const char*,nc_type base_typeid,nc_type*);
static int NC3_put_vlen_element(int,int,void*,size_t,const void*);
static int NC3_get_vlen_element(int,int,const void*,size_t*,void*);
static int NC3_def_enum(int,nc_type,const char*,nc_type*);
static int NC3_insert_enum(int,nc_type,const char*,const void*);
static int NC3_inq_enum_member(int,nc_type,int,char*,void*);
static int NC3_inq_enum_ident(int,nc_type,long long,char*);
static int NC3_def_opaque(int,size_t,const char*,nc_type*);
static int NC3_def_var_deflate(int,int,int,int,int);
static int NC3_def_var_fletcher32(int,int,int);
static int NC3_def_var_chunking(int,int,int,const size_t*);
static int NC3_def_var_endian(int,int,int);
static int NC3_def_var_filter(int, int, unsigned int, size_t, const unsigned int*);

static int NC3_set_var_chunk_cache(int,int,size_t,size_t,float);
static int NC3_get_var_chunk_cache(int,int,size_t*,size_t*,float*);

static const NC_Dispatch NC3_dispatcher = {

NC_FORMATX_NC3,
NC_DISPATCH_VERSION,
NC3_create,
NC3_open,

NC3_redef,
NC3__enddef,
NC3_sync,
NC3_abort,
NC3_close,
NC3_set_fill,
NC3_inq_format,
NC3_inq_format_extended,

NC3_inq,
NC3_inq_type,

NC3_def_dim,
NC3_inq_dimid,
NC3_inq_dim,
NC3_inq_unlimdim,
NC3_rename_dim,

NC3_inq_att,
NC3_inq_attid,
NC3_inq_attname,
NC3_rename_att,
NC3_del_att,
NC3_get_att,
NC3_put_att,

NC3_def_var,
NC3_inq_varid,
NC3_rename_var,
NC3_get_vara,
NC3_put_vara,
NCDEFAULT_get_vars,
NCDEFAULT_put_vars,
NCDEFAULT_get_varm,
NCDEFAULT_put_varm,

NC3_inq_var_all,

NC3_var_par_access,
NC3_def_var_fill,

NC3_show_metadata,
NC3_inq_unlimdims,
NC3_inq_ncid,
NC3_inq_grps,
NC3_inq_grpname,
NC3_inq_grpname_full,
NC3_inq_grp_parent,
NC3_inq_grp_full_ncid,
NC3_inq_varids,
NC3_inq_dimids,
NC3_inq_typeids,
NC3_inq_type_equal,
NC3_def_grp,
NC3_rename_grp,
NC3_inq_user_type,
NC3_inq_typeid,

NC3_def_compound,
NC3_insert_compound,
NC3_insert_array_compound,
NC3_inq_compound_field,
NC3_inq_compound_fieldindex,
NC3_def_vlen,
NC3_put_vlen_element,
NC3_get_vlen_element,
NC3_def_enum,
NC3_insert_enum,
NC3_inq_enum_member,
NC3_inq_enum_ident,
NC3_def_opaque,
NC3_def_var_deflate,
NC3_def_var_fletcher32,
NC3_def_var_chunking,
NC3_def_var_endian,
NC3_def_var_filter,
NC3_set_var_chunk_cache,
NC3_get_var_chunk_cache,

NC_NOOP_inq_var_filter_ids,
NC_NOOP_inq_var_filter_info,

NC_NOTNC4_def_var_quantize,
NC_NOTNC4_inq_var_quantize,

NC_NOOP_inq_filter_avail,
};

const NC_Dispatch* NC3_dispatch_table = NULL; /*!< NC3 Dispatch table, moved here from ddispatch.c */

int
NC3_initialize(void)
{
    NC3_dispatch_table = &NC3_dispatcher;
    return NC_NOERR;
}

int
NC3_finalize(void)
{
    return NC_NOERR;
}

static int
NC3_inq_var_all(int ncid, int varid, char *name, nc_type *xtypep, 
               int *ndimsp, int *dimidsp, int *nattsp, 
               int *shufflep, int *deflatep, int *deflate_levelp,
               int *fletcher32p, int *contiguousp, size_t *chunksizesp, 
               int *no_fill, void *fill_valuep, int *endiannessp, 
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
	       )
{
    int stat = NC3_inq_var(ncid,varid,name,xtypep,ndimsp,dimidsp,nattsp,no_fill,fill_valuep);
    if(stat) return stat;
    if(shufflep) *shufflep = 0;
    if(deflatep) *deflatep = 0;
    if(fletcher32p) *fletcher32p = 0;
    if(contiguousp) *contiguousp = NC_CONTIGUOUS;
    if(endiannessp) return NC_ENOTNC4;
    if(idp) return NC_ENOTNC4;
    if(nparamsp) return NC_ENOTNC4;
    if(params) return NC_ENOTNC4;
    return NC_NOERR;
}

static int
NC3_var_par_access(int ncid, int varid, int par_access)
{
    return NC_NOERR; /* no-op for netcdf classic */
}

static int
NC3_inq_unlimdims(int ncid, int *ndimsp, int *unlimdimidsp)
{
    int retval;
    int unlimid;

    if ((retval = NC3_inq_unlimdim(ncid, &unlimid)))
        return retval;
    if (unlimid != -1) {
        if(ndimsp) *ndimsp = 1;
        if (unlimdimidsp)
            unlimdimidsp[0] = unlimid;
    } else
        if(ndimsp) *ndimsp = 0;
    return NC_NOERR;
}

static int
NC3_def_grp(int parent_ncid, const char *name, int *new_ncid)
{
    return NC_ENOTNC4;
}

static int
NC3_rename_grp(int grpid, const char *name)
{
    return NC_ENOTNC4;
}

static int
NC3_inq_ncid(int ncid, const char *name, int *grp_ncid)
{
  if(grp_ncid) *grp_ncid = ncid;
    return NC_NOERR;
}

static int
NC3_inq_grps(int ncid, int *numgrps, int *ncids)
{
  if (numgrps)
       *numgrps = 0;
    return NC_NOERR;
}

static int
NC3_inq_grpname(int ncid, char *name)
{
    if (name)
        strcpy(name, "/");
    return NC_NOERR;
}

static int
NC3_inq_grpname_full(int ncid, size_t *lenp, char *full_name)
{
    if (full_name)
        strcpy(full_name, "/");
    if(lenp) *lenp = 1;
    return NC_NOERR;
}

static int
NC3_inq_grp_parent(int ncid, int *parent_ncid)
{
    return NC_ENOGRP;
}

static int
NC3_inq_grp_full_ncid(int ncid, const char *full_name, int *grp_ncid)
{
    return NC_ENOGRP;
}

static int
NC3_inq_varids(int ncid, int *nvarsp, int *varids)
{
    int retval,v,nvars;
    /* This is a netcdf-3 file, there is only one group, the root
        group, and its vars have ids 0 thru nvars - 1. */
    if ((retval = NC3_inq(ncid, NULL, &nvars, NULL, NULL)))
        return retval;
    if(nvarsp) *nvarsp = nvars;
    if (varids)
        for (v = 0; v < nvars; v++)
            varids[v] = v;
    return NC_NOERR;
}

static int
NC3_inq_dimids(int ncid, int *ndimsp, int *dimids, int include_parents)
{
    int retval,d,ndims;
    /* If this is a netcdf-3 file, then the dimids are going to be 0
       thru ndims-1, so just provide them. */
    if ((retval = NC3_inq(ncid, &ndims,  NULL, NULL, NULL)))
        return retval;
    if(ndimsp) *ndimsp = ndims;
    if (dimids)
        for (d = 0; d < ndims; d++)
            dimids[d] = d;
    return NC_NOERR;
}

static int
NC3_show_metadata(int ncid)
{
    return NC_NOERR;
}

static int
NC3_inq_type_equal(int ncid1, nc_type typeid1, int ncid2, nc_type typeid2, int* equalp)
{
    /* Check input. */
    if(equalp == NULL) return NC_NOERR;
    
    if (typeid1 <= NC_NAT || typeid2 <= NC_NAT)
       return NC_EINVAL;
    
    *equalp = 0; /* assume */
    
    /* If one is atomic, and the other user-defined, the types are not equal */
    if ((typeid1 <= NC_STRING && typeid2 > NC_STRING) ||
        (typeid2 <= NC_STRING && typeid1 > NC_STRING)) {
        if (equalp) *equalp = 0;
        return NC_NOERR;
    }
    
    /* If both are atomic types, the answer is easy. */
    if (typeid1 <= ATOMICTYPEMAX3) {
        if (equalp) {
            if (typeid1 == typeid2)
                *equalp = 1;
            else
                *equalp = 0;
        }
        return NC_NOERR;
    }
    return NC_NOERR;
}

static int
NC3_inq_typeid(int ncid, const char *name, nc_type *typeidp)
{
    int i;
    for (i = 0; i <= ATOMICTYPEMAX3; i++)
        if (!strcmp(name, NC_atomictypename(i))) {
            if (typeidp) *typeidp = i;
                return NC_NOERR;
        }
    return NC_ENOTNC4;
}

static int
NC3_inq_typeids(int ncid, int *ntypes, int *typeids)
{
    if(ntypes) *ntypes = 0;
    return NC_NOERR;
}

static int
NC3_inq_user_type(int ncid, nc_type typeid, char *name, size_t *size,
		 nc_type *base_nc_typep, size_t *nfieldsp, int *classp)
{
    return NC_ENOTNC4;
}

static int
NC3_def_compound(int ncid, size_t size, const char *name, nc_type *typeidp)
{
    return NC_ENOTNC4;
}

static int
NC3_insert_compound(int ncid, nc_type typeid, const char *name, size_t offset,
                    nc_type field_typeid)
{
    return NC_ENOTNC4;
}

static int
NC3_insert_array_compound(int ncid, nc_type typeid, const char *name,
			 size_t offset, nc_type field_typeid,
			 int ndims, const int *dim_sizes)
{
    return NC_ENOTNC4;
}


static int
NC3_inq_compound_field(int ncid, nc_type typeid, int fieldid, char *name,
		      size_t *offsetp, nc_type *field_typeidp, int *ndimsp, 
		      int *dim_sizesp)
{
    return NC_ENOTNC4;
}

static int
NC3_inq_compound_fieldindex(int ncid, nc_type typeid, const char *name, int *fieldidp)
{
    return NC_ENOTNC4;
}

static int
NC3_def_opaque(int ncid, size_t datum_size, const char *name, nc_type* xtypep)
{
    return NC_ENOTNC4;
}

static int
NC3_def_vlen(int ncid, const char *name, nc_type base_typeid, nc_type* xtypep)
{
    return NC_ENOTNC4;
}

static int
NC3_def_enum(int ncid, nc_type base_typeid, const char *name,
	    nc_type *typeidp)
{
    return NC_ENOTNC4;
}

static int
NC3_inq_enum_ident(int ncid, nc_type xtype, long long value, char *identifier)
{
    return NC_ENOTNC4;
}

static int
NC3_inq_enum_member(int ncid, nc_type typeid, int idx, char *identifier,
		   void *value)
{
    return NC_ENOTNC4;
}

static int
NC3_insert_enum(int ncid, nc_type typeid, const char *identifier,
	       const void *value)
{
    return NC_ENOTNC4;
}

static int
NC3_put_vlen_element(int ncid, int typeid, void *vlen_element,
		    size_t len, const void *data)
{
    return NC_ENOTNC4;
}

static int
NC3_get_vlen_element(int ncid, int typeid, const void *vlen_element,
		    size_t *len, void *data)
{
    return NC_ENOTNC4;
}

static int
NC3_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems, float preemption)
{
    return NC_ENOTNC4;
}

static int
NC3_get_var_chunk_cache(int ncid, int varid, size_t *sizep, size_t *nelemsp, float *preemptionp)
{
    return NC_ENOTNC4;
}

static int
NC3_def_var_deflate(int ncid, int varid, int shuffle, int deflate,
		   int deflate_level)
{
    return NC_ENOTNC4;
}

static int
NC3_def_var_fletcher32(int ncid, int varid, int fletcher32)
{
    return NC_ENOTNC4;
}

static int
NC3_def_var_chunking(int ncid, int varid, int contiguous, const size_t *chunksizesp)
{
    return NC_ENOTNC4;
}

static int
NC3_def_var_endian(int ncid, int varid, int endianness)
{
    return NC_ENOTNC4;
}

static int
NC3_def_var_filter(int ncid, int varid, unsigned int id, size_t nparams, const unsigned int* params)
{
    return NC_ENOTNC4;
}

