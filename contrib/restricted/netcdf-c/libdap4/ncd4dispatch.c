/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include <stddef.h>
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#if defined(_WIN32) || defined(_WIN64)
#include <crtdbg.h>
#include <direct.h>
#endif
#include "ncd4dispatch.h"
#include "nc4internal.h"
#include "d4includes.h"
#include "d4curlfunctions.h"

#ifdef HAVE_GETRLIMIT
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/time.h>
#  endif
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#endif

#if 0
/* Define the set of protocols known to be constrainable */
static char* constrainableprotocols[] = {"http", "https",NULL};
#endif

static int ncd4initialized = 0;

static const NC_Dispatch NCD4_dispatch_base;

const NC_Dispatch* NCD4_dispatch_table = NULL; /* moved here from ddispatch.c */

/**************************************************/
/* Define a structure defining reserved attributes */

/** @internal List of reserved attributes. */
static const NC_reservedatt NCD4_reserved[] = {
    {D4CHECKSUMATTR, READONLYFLAG|NAMEONLYFLAG},      /*_DAP4_Checksum_CRC32*/
    {D4LEATTR, READONLYFLAG|NAMEONLYFLAG},            /*_DAP4_Little_Endian*/
    /* Also need to include the provenance attributes */
    {NCPROPS, READONLYFLAG|NAMEONLYFLAG},		/*_NCProperties*/
    {NULL, 0}
};

/* Forward */
static int globalinit(void);
static int ncd4_get_att_reserved(NC* ncp, int ncid, int varid, const char* name, void* value, nc_type t, const NC_reservedatt* rsvp);
static int ncd4_inq_att_reserved(NC* ncp, int ncid, int varid, const char* name, nc_type* xtypep, size_t* lenp, const NC_reservedatt* rsvp);


/**************************************************/
int
NCD4_initialize(void)
{
    NCD4_dispatch_table = &NCD4_dispatch_base;
    ncd4initialized = 1;
    ncloginit();
#ifdef D4DEBUG
    /* force logging to go to stderr */
    if(nclogopen(NULL))
        ncsetlogging(1); /* turn it on */
#endif
    /* Init global state */
    globalinit();

    return THROW(NC_NOERR);
}

int
NCD4_finalize(void)
{
    return THROW(NC_NOERR);
}

static int
NCD4_redef(int ncid)
{
    return (NC_EPERM);
}

static int
NCD4__enddef(int ncid, size_t h_minfree, size_t v_align, size_t v_minfree, size_t r_align)
{
    return (NC_NOERR); /* let it go */
}

static int
NCD4_sync(int ncid)
{
    return (NC_NOERR); /* let it go */
}

static int
NCD4_create(const char *path, int cmode,
           size_t initialsz, int basepe, size_t *chunksizehintp,
           void* mpidata, const NC_Dispatch *dispatch, int ncid)
{
   return THROW(NC_EPERM);
}

static int
NCD4_put_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges,
            const void *value,
	    nc_type memtype)
{
    return THROW(NC_EPERM);
}


static int
NCD4_put_vars(int ncid, int varid,
	    const size_t *start, const size_t *edges, const ptrdiff_t* stride,
            const void *value0, nc_type memtype)
{
    return THROW(NC_EPERM);
}

/*
Force dap4 access to be read-only
*/
static int
NCD4_set_fill(int ncid, int fillmode, int* old_modep)
{
    return (NC_EPERM);
}

static int
NCD4_def_dim(int ncid, const char* name, size_t len, int* idp)
{
    return (NC_EPERM);
}

static int
NCD4_put_att(int ncid, int varid, const char* name, nc_type datatype,
	   size_t len, const void* value, nc_type t)
{
    return (NC_EPERM);
}

static int
NCD4_def_var(int ncid, const char *name,
  	     nc_type xtype, int ndims, const int *dimidsp, int *varidp)
{
    return (NC_EPERM);
}

static int
NCD4_def_grp(int ncid, const char* p2, int* p3)
{
    return (NC_EPERM);
}

static int
NCD4_rename_grp(int ncid, const char* p)
{
    return (NC_EPERM);
}

static int
NCD4_def_compound(int ncid, size_t p2, const char* p3, nc_type* t)
{
    return (NC_EPERM);
}

static int
NCD4_insert_compound(int ncid, nc_type t1, const char* p3, size_t p4, nc_type t2)
{
    return (NC_EPERM);
}

static int
NCD4_insert_array_compound(int ncid, nc_type t1, const char* p3, size_t p4,
			  nc_type t2, int p6, const int* p7)
{
    return (NC_EPERM);
}


static int
NCD4_def_vlen(int ncid, const char* p2, nc_type base_typeid, nc_type* t)
{
    return (NC_EPERM);
}

static int
NCD4_put_vlen_element(int ncid, int p2, void* p3, size_t p4, const void* p5)
{
    return (NC_EPERM);
}

static int
NCD4_def_enum(int ncid, nc_type t1, const char* p3, nc_type* t)
{
    return (NC_EPERM);
}

static int
NCD4_insert_enum(int ncid, nc_type t1, const char* p3, const void* p4)
{
    return (NC_EPERM);
}

static int
NCD4_def_opaque(int ncid, size_t p2, const char* p3, nc_type* t)
{
    return (NC_EPERM);
}

static int
NCD4_def_var_deflate(int ncid, int p2, int p3, int p4, int p5)
{
    return (NC_EPERM);
}

static int
NCD4_def_var_fletcher32(int ncid, int p2, int p3)
{
    return (NC_EPERM);
}

static int
NCD4_def_var_chunking(int ncid, int p2, int p3, const size_t* p4)
{
    return (NC_EPERM);
}

static int
NCD4_def_var_fill(int ncid, int p2, int p3, const void* p4)
{
    return (NC_EPERM);
}

static int
NCD4_def_var_endian(int ncid, int p2, int p3)
{
    return (NC_EPERM);
}

static int
NCD4_def_var_filter(int ncid, int varid, unsigned int id, size_t n, const unsigned int* params)
{
    return (NC_EPERM);
}

static int
NCD4_set_var_chunk_cache(int ncid, int p2, size_t p3, size_t p4, float p5)
{
    return (NC_EPERM);
}

/**************************************************/
/*
Following functions basically return the netcdf-4 value WRT to the nc4id.
However, it is necessary to modify the grpid(ncid) to point to the substrate.
*/

static int
NCD4_inq_format(int ncid, int* formatp)
{
    NC* ncp;
    int ret = NC_NOERR;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_format(substrateid, formatp);
    return (ret);
}

static int
NCD4_inq(int ncid, int* ndimsp, int* nvarsp, int* nattsp, int* unlimdimidp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq(substrateid, ndimsp, nvarsp, nattsp, unlimdimidp);
    return (ret);
}

static int
NCD4_inq_type(int ncid, nc_type p2, char* p3, size_t* p4)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_type(substrateid, p2, p3, p4);
    return (ret);
}

static int
NCD4_inq_dimid(int ncid, const char* name, int* idp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_dimid(substrateid, name, idp);
    return (ret);
}

static int
NCD4_inq_unlimdim(int ncid, int* unlimdimidp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_unlimdim(substrateid, unlimdimidp);
    return (ret);
}

static int
NCD4_rename_dim(int ncid, int dimid, const char* name)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_rename_dim(substrateid, dimid, name);
    return (ret);
}

static int
NCD4_inq_att(int ncid, int varid, const char* name,
	    nc_type* xtypep, size_t* lenp)
{
    NC* ncp;
    int ret;
    int substrateid;
    const NC_reservedatt* rsvp = NULL;
    
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);

    /* Is this a reserved attribute name? */
    if(name && (rsvp = NCD4_lookupreserved(name)))
	return ncd4_inq_att_reserved(ncp,ncid,varid,name,xtypep,lenp, rsvp);
    ret = nc_inq_att(substrateid, varid, name, xtypep, lenp);
    return (ret);
}

const struct NC_reservedatt*
NCD4_lookupreserved(const char* name)
{
    const NC_reservedatt* p = NCD4_reserved;
    for(;p->name;p++) {
	if(strcmp(name,p->name)==0)
	    return p;
    }
    return NULL;
}

static int
NCD4_inq_attid(int ncid, int varid, const char *name, int *idp)
{
    NC* ncp;
    int ret;
    int substrateid;
    const NC_reservedatt* rsvp = NULL;

    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    /* Is this a reserved attribute name? */
    if(name && (rsvp = NCD4_lookupreserved(name)))
	return NC_EATTMETA;
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_attid(substrateid, varid, name, idp);
    return (ret);
}

static int
NCD4_inq_attname(int ncid, int varid, int attnum, char* name)
{
    NC* ncp;
    int ret;
    int substrateid;
    const NC_reservedatt* rsvp = NULL;
    
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_attname(substrateid, varid, attnum, name);
    /* Is this a reserved attribute name? */
    if(name && (rsvp = NCD4_lookupreserved(name)))
	return NC_EATTMETA;
    return (ret);
}

static int
NCD4_rename_att(int ncid, int varid, const char* name, const char* newname)
{
    NC* ncp;
    int ret;
    int substrateid;
    const NC_reservedatt* rsvp = NULL;
    
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    /* Is this a reserved attribute name? */
    if(name && (rsvp = NCD4_lookupreserved(name)))
	return NC_EATTMETA;
    substrateid = makenc4id(ncp,ncid);
    ret = nc_rename_att(substrateid, varid, name, newname);
    return (ret);
}

static int
NCD4_del_att(int ncid, int varid, const char* name)
{
    NC* ncp;
    int ret;
    int substrateid;
    const NC_reservedatt* rsvp = NULL;
    
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    /* Is this a reserved attribute name? */
    if(name && (rsvp = NCD4_lookupreserved(name)))
	return NC_EATTMETA;
    substrateid = makenc4id(ncp,ncid);
    ret = nc_del_att(substrateid, varid, name);
    return (ret);
}

static int
NCD4_get_att(int ncid, int varid, const char* name, void* value, nc_type t)
{
    NC* ncp;
    int ret;
    int substrateid;
    const NC_reservedatt* rsvp = NULL;
    
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    /* Is this a reserved attribute name? */
    if(name && (rsvp = NCD4_lookupreserved(name)))
	return ncd4_get_att_reserved(ncp,ncid,varid,name,value,t,rsvp);
    substrateid = makenc4id(ncp,ncid);
    ret = NCDISPATCH_get_att(substrateid, varid, name, value, t);
    return (ret);
}

static int
NCD4_inq_var_all(int ncid, int varid, char *name, nc_type* xtypep,
               int* ndimsp, int* dimidsp, int* nattsp,
               int* shufflep, int* deflatep, int* deflate_levelp,
               int* fletcher32p, int* contiguousp, size_t* chunksizesp,
               int* no_fill, void* fill_valuep, int* endiannessp,
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
               )
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = NCDISPATCH_inq_var_all(substrateid, varid, name, xtypep,
               ndimsp, dimidsp, nattsp,
               shufflep, deflatep, deflate_levelp,
               fletcher32p, contiguousp, chunksizesp,
               no_fill, fill_valuep, endiannessp,
               idp, nparamsp, params);
    return (ret);
}

static int
NCD4_inq_varid(int ncid, const char *name, int *varidp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_varid(substrateid,name,varidp);
    return (ret);
}

static int
NCD4_rename_var(int ncid, int varid, const char* name)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_rename_var(substrateid, varid, name);
    return (ret);
}

static int
NCD4_var_par_access(int ncid, int p2, int p3)
{
    return (NC_ENOPAR);
}


static int
NCD4_inq_ncid(int ncid, const char* name, int* grp_ncid)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_ncid(substrateid, name, grp_ncid);
    return (ret);
}

static int
NCD4_show_metadata(int ncid)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_show_metadata(substrateid);
    return (ret);
}

static int
NCD4_inq_grps(int ncid, int* ngrpsp, int* grpids)
{
    NC* ncp;
    int ret;
    int substrateid;
    int ngrps;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    /* We always need to know |grpids| */
    ret = nc_inq_grps(substrateid, &ngrps, grpids);
    if(ret == NC_NOERR) {
	if(ngrpsp != NULL)
	    *ngrpsp = ngrps; /* return if caller want it */
	if(grpids != NULL) {
	    int i;
	    /* We need to convert the substrate group ids to dap4 group ids */
	    for(i=0;i<ngrps;i++)
		grpids[i] = makedap4id(ncp,grpids[i]);
	}
    }
    return (ret);
}

static int
NCD4_inq_grpname(int ncid, char* p)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_grpname(substrateid, p);
    return (ret);
}


static int
NCD4_inq_unlimdims(int ncid, int* p2, int* p3)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_unlimdims(substrateid, p2, p3);
    return (ret);
}

static int
NCD4_inq_grpname_full(int ncid, size_t* p2, char* p3)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_grpname_full(substrateid, p2, p3);
    return (ret);
}

static int
NCD4_inq_grp_parent(int ncid, int* p)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_grp_parent(substrateid, p);
    if(p != NULL)
	*p = makedap4id(ncp,*p);
    return (ret);
}

static int
NCD4_inq_grp_full_ncid(int ncid, const char* fullname, int* grpidp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_grp_full_ncid(substrateid, fullname, grpidp);
    if(grpidp != NULL)
	*grpidp = makedap4id(ncp,*grpidp);
    return (ret);
}

static int
NCD4_inq_varids(int ncid, int* nvars, int* p)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_varids(substrateid, nvars, p);
    return (ret);
}

static int
NCD4_inq_dimids(int ncid, int* ndims, int* p3, int p4)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_dimids(substrateid, ndims, p3, p4);
    return (ret);
}

static int
NCD4_inq_typeids(int ncid, int*  ntypes, int* p)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_typeids(substrateid, ntypes, p);
    return (ret);
}

static int
NCD4_inq_type_equal(int ncid, nc_type t1, int p3, nc_type t2, int* p5)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_type_equal(substrateid, t1, p3, t2, p5);
    return (ret);
}

static int
NCD4_inq_user_type(int ncid, nc_type t, char* p3, size_t* p4, nc_type* p5,
                   size_t* p6, int* p7)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_user_type(substrateid, t, p3, p4, p5, p6, p7);
    return (ret);
}

static int
NCD4_inq_typeid(int ncid, const char* name, nc_type* t)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_typeid(substrateid, name, t);
    return (ret);
}

static int
NCD4_inq_compound_field(int ncid, nc_type xtype, int fieldid, char *name,
		      size_t *offsetp, nc_type* field_typeidp, int *ndimsp,
		      int *dim_sizesp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_compound_field(substrateid, xtype, fieldid, name, offsetp, field_typeidp, ndimsp, dim_sizesp);
    return (ret);
}

static int
NCD4_inq_compound_fieldindex(int ncid, nc_type xtype, const char *name,
			   int *fieldidp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_compound_fieldindex(substrateid, xtype, name, fieldidp);
    return (ret);
}

static int
NCD4_get_vlen_element(int ncid, int p2, const void* p3, size_t* p4, void* p5)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_get_vlen_element(substrateid, p2, p3, p4, p5);
    return (ret);
}

static int
NCD4_inq_enum_member(int ncid, nc_type t1, int p3, char* p4, void* p5)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_enum_member(substrateid, t1, p3, p4, p5);
    return (ret);
}

static int
NCD4_inq_enum_ident(int ncid, nc_type t1, long long p3, char* p4)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_enum_ident(substrateid, t1, p3, p4);
    return (ret);
}

static int
NCD4_get_var_chunk_cache(int ncid, int p2, size_t* p3, size_t* p4, float* p5)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_get_var_chunk_cache(substrateid, p2, p3, p4, p5);
    return (ret);
}

static int
NCD4_inq_var_quantize(int ncid, int varid, int *quantize_modep, int *nsdp)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_var_quantize(substrateid, varid, quantize_modep, nsdp);
    return (ret);
}

static int
NCD4_inq_var_filter_ids(int ncid, int varid, size_t* nfilters, unsigned int* filterids)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_var_filter_ids(substrateid, varid, nfilters, filterids);
    return (ret);
}

static int
NCD4_inq_var_filter_info(int ncid, int varid, unsigned int id, size_t* nparams, unsigned int* params)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_var_filter_info(substrateid, varid, id, nparams, params);
    return (ret);
}

static int
NCD4_inq_filter_avail(int ncid, unsigned id)
{
    NC* ncp;
    int ret;
    int substrateid;
    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR) return (ret);
    substrateid = makenc4id(ncp,ncid);
    ret = nc_inq_filter_avail(substrateid, id);
    return (ret);
}

/**************************************************/
/*
Following functions are overridden to handle 
dap4 implementation specific issues.
*/

/* Force specific format */
static int
NCD4_inq_format_extended(int ncid, int* formatp, int* modep)
{
    NC* nc;
    int ncstatus = NC_check_id(ncid, (NC**)&nc);
    if(ncstatus != NC_NOERR) return (ncstatus);
    if(modep) *modep = nc->mode;
    if(formatp) *formatp = NC_FORMATX_DAP4;
    return THROW(NC_NOERR);
}

/*
Override nc_inq_dim to handle the fact
that unlimited dimensions will not have
a proper size because the substrate has
never (not yet) been written.
*/
int
NCD4_inq_dim(int ncid, int dimid, char* name, size_t* lenp)
{
    int ret = NC_NOERR;
    NC* ncp;
    NCD4INFO* info;
    NCD4meta* meta;
    size_t i;
    NCD4node* dim = NULL;

    if((ret = NC_check_id(ncid, (NC**)&ncp)) != NC_NOERR)
	goto done;
    info = (NCD4INFO*)ncp->dispatchdata;
    meta = info->dmrmetadata;

    /* Locate the dimension specified by dimid */
    for(i=0;i<nclistlength(meta->allnodes);i++) {
	NCD4node* n = (NCD4node*)nclistget(meta->allnodes,i);
	if(n->sort == NCD4_DIM && n->meta.id == dimid) {
	    dim = n;
	    break;
	}
    }
    if(dim == NULL)
	{ret = NC_EBADDIM; goto done;}
    if(name)
	strncpy(name,dim->name,NC_MAX_NAME);
    if(lenp)
	*lenp = (size_t)dim->dim.size;
done:
    return (ret);
}

static int
ncd4_get_att_reserved(NC* ncp, int ncid, int varid, const char* name, void* value, nc_type t, const NC_reservedatt* rsvp)
{
    int ret = NC_NOERR;
    NCD4node* var = NULL;

    if((ret=NCD4_findvar(ncp,ncid,varid,&var,NULL))) goto done;

    if(strcmp(rsvp->name,D4CHECKSUMATTR)==0) {
	unsigned int* ip = (unsigned int*)value;
	if(varid == NC_GLOBAL)
            {ret = NC_EBADID; goto done;}
	if(t != NC_UINT) {ret = NC_EBADTYPE; goto done;}
	if(var->data.checksumattr == 0)
	    {ret = NC_ENOTATT; goto done;} 
	*ip = (var->data.remotechecksum);
    } else if(strcmp(rsvp->name,D4LEATTR)==0) {
	int* ip = (int*)value;
	if(varid != NC_GLOBAL)
            {ret = NC_EBADID; goto done;}
	if(t != NC_INT) {ret = NC_EBADTYPE; goto done;}
	*ip = (var->data.response->remotelittleendian?1:0);
    }
done:
    return THROW(ret);
}

static int
ncd4_inq_att_reserved(NC* ncp, int ncid, int varid, const char* name, nc_type* xtypep, size_t* lenp, const NC_reservedatt* rsvp)
{
    int ret = NC_NOERR;
    NCD4node* var = NULL;

    if(strcmp(rsvp->name,D4CHECKSUMATTR)==0) {
	if(varid == NC_GLOBAL)
            {ret = NC_EBADID; goto done;}
        if((ret=NCD4_findvar(ncp,ncid,varid,&var,NULL))) goto done;
	if(var->data.checksumattr == 0)
	    {ret = NC_ENOTATT; goto done;} 
	if(xtypep) *xtypep = NC_UINT;
	if(lenp) *lenp = 1;
    } else if(strcmp(rsvp->name,D4LEATTR)==0) {
	if(varid != NC_GLOBAL)
            {ret = NC_EBADID; goto done;}
	if(xtypep) *xtypep = NC_INT;
	if(lenp) *lenp = 1;
    }
done:
    return THROW(ret);
}

/**************************************************/

static int
globalinit(void)
{
    int stat = NC_NOERR;
    return THROW(stat);
}

/**************************************************/

static const NC_Dispatch NCD4_dispatch_base = {

NC_FORMATX_DAP4,
NC_DISPATCH_VERSION,

NCD4_create,
NCD4_open,

NCD4_redef,
NCD4__enddef,
NCD4_sync,
NCD4_abort,
NCD4_close,
NCD4_set_fill,
NCD4_inq_format,
NCD4_inq_format_extended, /*inq_format_extended*/

NCD4_inq,
NCD4_inq_type,

NCD4_def_dim,
NCD4_inq_dimid,
NCD4_inq_dim,
NCD4_inq_unlimdim,
NCD4_rename_dim,

NCD4_inq_att,
NCD4_inq_attid,
NCD4_inq_attname,
NCD4_rename_att,
NCD4_del_att,
NCD4_get_att,
NCD4_put_att,

NCD4_def_var,
NCD4_inq_varid,
NCD4_rename_var,
NCD4_get_vara,
NCD4_put_vara,
NCD4_get_vars,
NCD4_put_vars,
NCDEFAULT_get_varm,
NCDEFAULT_put_varm,

NCD4_inq_var_all,

NCD4_var_par_access,
NCD4_def_var_fill,

NCD4_show_metadata,
NCD4_inq_unlimdims,
NCD4_inq_ncid,
NCD4_inq_grps,
NCD4_inq_grpname,
NCD4_inq_grpname_full,
NCD4_inq_grp_parent,
NCD4_inq_grp_full_ncid,
NCD4_inq_varids,
NCD4_inq_dimids,
NCD4_inq_typeids,
NCD4_inq_type_equal,
NCD4_def_grp,
NCD4_rename_grp,
NCD4_inq_user_type,
NCD4_inq_typeid,

NCD4_def_compound,
NCD4_insert_compound,
NCD4_insert_array_compound,
NCD4_inq_compound_field,
NCD4_inq_compound_fieldindex,
NCD4_def_vlen,
NCD4_put_vlen_element,
NCD4_get_vlen_element,
NCD4_def_enum,
NCD4_insert_enum,
NCD4_inq_enum_member,
NCD4_inq_enum_ident,
NCD4_def_opaque,
NCD4_def_var_deflate,
NCD4_def_var_fletcher32,
NCD4_def_var_chunking,
NCD4_def_var_endian,
NCD4_def_var_filter,
NCD4_set_var_chunk_cache,
NCD4_get_var_chunk_cache,

NCD4_inq_var_filter_ids,
NCD4_inq_var_filter_info,

NC_NOTNC4_def_var_quantize,
NCD4_inq_var_quantize,

NCD4_inq_filter_avail,
};
