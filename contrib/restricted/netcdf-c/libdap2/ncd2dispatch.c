/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "dapincludes.h"
#include "ncd2dispatch.h"
#include "ncrc.h"
#include "ncoffsets.h"
#include "netcdf_dispatch.h"
#include <stddef.h>
#ifdef DEBUG2
#include "dapdump.h"
#endif

#ifdef _MSC_VER
#include <crtdbg.h>
#endif

#ifdef HAVE_GETRLIMIT
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/time.h>
#  endif
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#endif

/* Define the set of protocols known to be constrainable */
static const char* constrainableprotocols[] = {"http", "https",NULL};

static int ncd2initialized = 0;

/* Forward */
static NCerror buildncstructures(NCDAPCOMMON*);
static NCerror builddims(NCDAPCOMMON*);
static char* getdefinename(CDFnode* node);
static NCerror buildvars(NCDAPCOMMON*);
static NCerror buildglobalattrs(NCDAPCOMMON*, CDFnode* root);
static NCerror buildattribute(NCDAPCOMMON*, CDFnode*, NCattribute*);
static void computedimindexanon(CDFnode* dim, CDFnode* var);
static void replacedims(NClist* dims);
static int equivalentdim(CDFnode* basedim, CDFnode* dupdim);
static NCerror addstringdims(NCDAPCOMMON*);
static NCerror defrecorddim(NCDAPCOMMON*);
static NCerror defseqdims(NCDAPCOMMON*);
static NCerror showprojection(NCDAPCOMMON*, CDFnode* var);
static NCerror getseqdimsize(NCDAPCOMMON*, CDFnode* seq, size_t* sizep);
static NCerror makeseqdim(NCDAPCOMMON*, CDFnode* seq, size_t count, CDFnode** sqdimp);
static NCerror countsequence(NCDAPCOMMON*, CDFnode* xseq, size_t* sizep);
static NCerror freeNCDAPCOMMON(NCDAPCOMMON*);
static NCerror fetchpatternmetadata(NCDAPCOMMON*);
static size_t fieldindex(CDFnode* parent, CDFnode* child);
static NCerror computeseqcountconstraints(NCDAPCOMMON*, CDFnode*, NCbytes*);
static void computeseqcountconstraintsr(NCDAPCOMMON*, CDFnode*, CDFnode**);
static void estimatevarsizes(NCDAPCOMMON*);
static NCerror fetchconstrainedmetadata(NCDAPCOMMON*);
static NCerror suppressunusablevars(NCDAPCOMMON*);
static NCerror fixzerodims(NCDAPCOMMON*);
static void applyclientparamcontrols(NCDAPCOMMON*);
static NCerror applyclientparams(NCDAPCOMMON*);

/**************************************************/

static int
NCD2_create(const char *path, int cmode,
           size_t initialsz, int basepe, size_t *chunksizehintp,
           void* mpidata, const struct NC_Dispatch*,int);

static int NCD2_redef(int ncid);
static int NCD2__enddef(int ncid, size_t h_minfree, size_t v_align, size_t v_minfree, size_t r_align);
static int NCD2_sync(int ncid);
static int NCD2_abort(int ncid);

static int NCD2_put_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges0,
            const void *value0,
	    nc_type memtype);

static int NCD2_get_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges,
            void *value,
	    nc_type memtype);

static int NCD2_put_vars(int ncid, int varid,
	    const size_t *start, const size_t *edges, const ptrdiff_t* stride,
            const void *value0, nc_type memtype);

static int NCD2_get_vars(int ncid, int varid,
	    const size_t *start, const size_t *edges, const ptrdiff_t* stride,
            void *value, nc_type memtype);

static const NC_Dispatch NCD2_dispatch_base = {

NC_FORMATX_DAP2,
NC_DISPATCH_VERSION,

NCD2_create,
NCD2_open,

NCD2_redef,
NCD2__enddef,
NCD2_sync,
NCD2_abort,
NCD2_close,
NCD2_set_fill,
NCD2_inq_format,
NCD2_inq_format_extended, /*inq_format_extended*/

NCD2_inq,
NCD2_inq_type,

NCD2_def_dim,
NCD2_inq_dimid,
NCD2_inq_dim,
NCD2_inq_unlimdim,
NCD2_rename_dim,

NCD2_inq_att,
NCD2_inq_attid,
NCD2_inq_attname,
NCD2_rename_att,
NCD2_del_att,
NCD2_get_att,
NCD2_put_att,

NCD2_def_var,
NCD2_inq_varid,
NCD2_rename_var,
NCD2_get_vara,
NCD2_put_vara,
NCD2_get_vars,
NCD2_put_vars,
NCDEFAULT_get_varm,
NCDEFAULT_put_varm,

NCD2_inq_var_all,

NCD2_var_par_access,
NCD2_def_var_fill,

NCD2_show_metadata,
NCD2_inq_unlimdims,
NCD2_inq_ncid,
NCD2_inq_grps,
NCD2_inq_grpname,
NCD2_inq_grpname_full,
NCD2_inq_grp_parent,
NCD2_inq_grp_full_ncid,
NCD2_inq_varids,
NCD2_inq_dimids,
NCD2_inq_typeids,
NCD2_inq_type_equal,
NCD2_def_grp,
NCD2_rename_grp,
NCD2_inq_user_type,
NCD2_inq_typeid,

NCD2_def_compound,
NCD2_insert_compound,
NCD2_insert_array_compound,
NCD2_inq_compound_field,
NCD2_inq_compound_fieldindex,
NCD2_def_vlen,
NCD2_put_vlen_element,
NCD2_get_vlen_element,
NCD2_def_enum,
NCD2_insert_enum,
NCD2_inq_enum_member,
NCD2_inq_enum_ident,
NCD2_def_opaque,
NCD2_def_var_deflate,
NCD2_def_var_fletcher32,
NCD2_def_var_chunking,
NCD2_def_var_endian,
NCD2_def_var_filter,
NCD2_set_var_chunk_cache,
NCD2_get_var_chunk_cache,

NC_NOOP_inq_var_filter_ids,
NC_NOOP_inq_var_filter_info,

NC_NOTNC4_def_var_quantize,
NC_NOTNC4_inq_var_quantize,

NC_NOOP_inq_filter_avail,
};

const NC_Dispatch* NCD2_dispatch_table = NULL; /* moved here from ddispatch.c */

int
NCD2_initialize(void)
{
    NCD2_dispatch_table = &NCD2_dispatch_base;
    ncd2initialized = 1;
#ifdef DEBUG
    /* force logging to go to stderr */
    ncsetlogging(1); /* turn it on */
    nclogopen(NULL))
#endif
    return NC_NOERR;
}

int
NCD2_finalize(void)
{
    return NC_NOERR;
}

static int
NCD2_redef(int ncid)
{
    return (NC_EPERM);
}

static int
NCD2__enddef(int ncid, size_t h_minfree, size_t v_align, size_t v_minfree, size_t r_align)
{
    return (NC_EPERM);
}

static int
NCD2_sync(int ncid)
{
    return (NC_EINVAL);
}

static int
NCD2_abort(int ncid)
{
    return NCD2_close(ncid,NULL);
}

static int
NCD2_create(const char *path, int cmode,
           size_t initialsz, int basepe, size_t *chunksizehintp,
           void* mpidata, const NC_Dispatch* dispatch, int ncid)
{
   return NC_EPERM;
}

static int
NCD2_put_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges,
            const void *value,
	    nc_type memtype)
{
    return NC_EPERM;
}

static int
NCD2_get_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges,
            void *value,
	    nc_type memtype)
{
    int stat = nc3d_getvarx(ncid, varid, start, edges, NC_stride_one, value,memtype);
    return stat;
}

static int
NCD2_put_vars(int ncid, int varid,
	    const size_t *start, const size_t *edges, const ptrdiff_t* stride,
            const void *value0, nc_type memtype)
{
    return NC_EPERM;
}

static int
NCD2_get_vars(int ncid, int varid,
	    const size_t *start, const size_t *edges, const ptrdiff_t* stride,
            void *value, nc_type memtype)
{
    int stat = nc3d_getvarx(ncid, varid, start, edges, stride, value, memtype);
    return stat;
}

/* See ncd2dispatch.c for other version */
int
NCD2_open(const char* path, int mode, int basepe, size_t *chunksizehintp,
          void* mpidata, const NC_Dispatch* dispatch, int ncid)
{
    NCerror ncstat = NC_NOERR;
    OCerror ocstat = OC_NOERR;
    NCDAPCOMMON* dapcomm = NULL;
    NC* drno;
    const char* value;
    int nc3id = -1;

    /* Find pointer to NC struct for this file. */
    ncstat = NC_check_id(ncid,&drno);
    if(ncstat != NC_NOERR) {goto done;}

    if(path == NULL)
	{ncstat = NC_EDAPURL; goto done;}
    if(dispatch == NULL)
	PANIC("NCD3_open: no dispatch table");

    /* Setup our NC and NCDAPCOMMON state*/

    dapcomm = (NCDAPCOMMON*)calloc(1,sizeof(NCDAPCOMMON));
    if(dapcomm == NULL)
	{ncstat = NC_ENOMEM; goto done;}

    NCD2_DATA_SET(drno,dapcomm);
    drno->int_ncid = nc__pseudofd(); /* create a unique id */
    dapcomm->controller = (NC*)drno;

    dapcomm->cdf.separator = ".";
    dapcomm->cdf.smallsizelimit = DFALTSMALLLIMIT;
    dapcomm->cdf.cache = createnccache();

#ifdef HAVE_GETRLIMIT
    { struct rlimit rl;
      if(getrlimit(RLIMIT_NOFILE, &rl) >= 0) {
	dapcomm->cdf.cache->cachecount = (size_t)(rl.rlim_cur / 2);
      }
    }
#endif

#ifdef OCCOMPILEBYDEFAULT
    { int rullen = 0;
    /* set the compile flag by default */
    rullen = strlen(path)+strlen("[compile]");;
    rullen++; /* strlcat nul */
    dapcomm->oc.rawurltext = (char*)emalloc(rullen+1);
    strncpy(dapcomm->oc.rawurltext,"[compile]",rullen);
    strlcat(dapcomm->oc.rawurltext, path, rullen);
    }
#else
    dapcomm->oc.rawurltext = strdup(path);
#endif

    if(ncuriparse(dapcomm->oc.rawurltext,&dapcomm->oc.url))
	{ncstat = NC_EURL; goto done;}

    if(!constrainable(dapcomm->oc.url))
	SETFLAG(dapcomm->controls,NCF_UNCONSTRAINABLE);

#ifdef COLUMBIA_HACK
    {
	const char* p;
	/* Does this url look like it is from columbia? */
	if(dapcomm->oc.url->host != NULL) {
	    for(p=dapcomm->oc.url->host;*p;p++) {
	        if(strncmp(p,COLUMBIA_HACK,strlen(COLUMBIA_HACK))==0)
		    SETFLAG(dapcomm->controls,NCF_COLUMBIA);
	    }
	}
    }
#endif

    /* fail if we are unconstrainable but have constraints */
    if(FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE)) {
	if(dapcomm->oc.url != NULL && dapcomm->oc.url->query != NULL) {
	    nclog(NCLOGWARN,"Attempt to constrain an unconstrainable data source: %s",
		   dapcomm->oc.url->query);
	    ncstat = THROW(NC_EDAPCONSTRAINT);
	    goto done;
	}
    }

    /* Use libsrc code (netcdf-3) for storing metadata */
    {
	char tmpname[32];

        /* Create fake file name: exact name must be unique,
           but is otherwise irrelevant because we are using NC_DISKLESS
        */
        snprintf(tmpname,sizeof(tmpname),"tmp_%d",drno->int_ncid);

        /* Now, use the file to create the hidden, in-memory netcdf file.
	   We want this hidden file to always be NC_CLASSIC, so we need to
           force default format temporarily in case user changed it.
	   Since diskless is enabled, create file in-memory.
	*/
	{
	    int new = 0; /* format netcdf-3 */
	    int old = 0;
	    int ncflags = NC_CLOBBER|NC_CLASSIC_MODEL;
	    ncflags |= NC_DISKLESS;
	    nc_set_default_format(new,&old); /* save and change */
            ncstat = nc_create(tmpname,ncflags,&nc3id);
	    nc_set_default_format(old,&new); /* restore */
	    dapcomm->substrate.realfile = ((ncflags & NC_DISKLESS) != 0);
	    dapcomm->substrate.filename = strdup(tmpname);
	    if(dapcomm->substrate.filename == NULL) ncstat = NC_ENOMEM;
	    dapcomm->substrate.nc3id = nc3id;
	}
        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
	/* Avoid fill */
	nc_set_fill(nc3id,NC_NOFILL,NULL);

    }
    dapcomm->oc.dapconstraint = (DCEconstraint*)dcecreate(CES_CONSTRAINT);
    dapcomm->oc.dapconstraint->projections = nclistnew();
    dapcomm->oc.dapconstraint->selections = nclistnew();

     /* Parse constraints to make sure they are syntactically correct */
     ncstat = dapparsedapconstraints(dapcomm,dapcomm->oc.url->query,dapcomm->oc.dapconstraint);
     if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
#ifdef DEBUG2
fprintf(stderr,"ce=%s\n",dumpconstraint(dapcomm->oc.dapconstraint));
#endif
    /* Construct a url for oc minus any constraint and params*/
    dapcomm->oc.urltext = ncuribuild(dapcomm->oc.url,NULL,NULL,NCURIBASE);

    /* Pass to OC */
    ocstat = oc_open(dapcomm->oc.urltext,&dapcomm->oc.conn);
    if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}

    if(getenv("CURLOPT_VERBOSE") != NULL)
        (void)oc_trace_curl(dapcomm->oc.conn);

    nullfree(dapcomm->oc.urltext); /* clean up */
    dapcomm->oc.urltext = NULL;

    /* process control client parameters */
    applyclientparamcontrols(dapcomm);

    /* Turn on logging; only do this after oc_open*/
    if((value = dapparamvalue(dapcomm,"log")) != NULL) {
        ncsetloglevel(NCLOGNOTE);
    }

    /* fetch and build the unconstrained DDS for use as
       pattern */
    ncstat = fetchpatternmetadata(dapcomm);
    if(ncstat != NC_NOERR) goto done;

    /* Operations on the pattern tree */

    /* Accumulate useful nodes sets  */
    ncstat = computecdfnodesets(dapcomm,dapcomm->cdf.fullddsroot->tree);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Define the dimsettrans list */
    ncstat = definedimsettrans(dapcomm,dapcomm->cdf.fullddsroot->tree);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Mark the nodes of the pattern that are eligible for prefetch */
    ncstat = markprefetch(dapcomm);

    /* fetch and build the constrained DDS */
    ncstat = fetchconstrainedmetadata(dapcomm);
    if(ncstat != NC_NOERR) goto done;

#ifdef DEBUG2
fprintf(stderr,"constrained dds: %s\n",dumptree(dapcomm->cdf.ddsroot));
#endif

    /* Operations on the constrained tree */

    /* Accumulate useful nodes sets  */
    ncstat = computecdfnodesets(dapcomm,dapcomm->cdf.ddsroot->tree);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Fix grids */
    ncstat = fixgrids(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Locate and mark usable sequences */
    ncstat = sequencecheck(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* suppress variables not in usable sequences */
    ncstat = suppressunusablevars(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* apply client parameters */
    ncstat = applyclientparams(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Add (as needed) string dimensions*/
    ncstat = addstringdims(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    if(nclistlength(dapcomm->cdf.ddsroot->tree->seqnodes) > 0) {
	/* Build the sequence related dimensions */
        ncstat = defseqdims(dapcomm);
        if(ncstat) {THROWCHK(ncstat); goto done;}
    }

    /* Define the dimsetplus and dimsetall lists */
    ncstat = definedimsets(dapcomm,dapcomm->cdf.ddsroot->tree);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Re-compute the dimension names*/
    ncstat = computecdfdimnames(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Deal with zero size dimensions */
    ncstat = fixzerodims(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Attempt to use the DODS_EXTRA info to turn
       one of the dimensions into unlimited.
       Assume computecdfdimnames34 has already been called.
    */
    ncstat = defrecorddim(dapcomm);
    if(ncstat) {THROWCHK(ncstat); goto done;}
    if(dapcomm->cdf.recorddimname != NULL
       && nclistlength(dapcomm->cdf.ddsroot->tree->seqnodes) > 0) {
	/*nclog(NCLOGWARN,"unlimited dimension specified, but sequences exist in DDS");*/
	PANIC("unlimited dimension specified, but sequences exist in DDS");
    }

    /* Re-compute the var names*/
    ncstat = computecdfvarnames(dapcomm,
				 dapcomm->cdf.ddsroot,
				 dapcomm->cdf.ddsroot->tree->varnodes);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* Transfer data from the unconstrained DDS data to the unconstrained DDS */
    ncstat = dimimprint(dapcomm);
    if(ncstat) goto done;

    /* Process the constraints to map to the constrained CDF tree */
    /* (must follow fixgrids3) */
    ncstat = dapmapconstraints(dapcomm->oc.dapconstraint,dapcomm->cdf.ddsroot);
    if(ncstat != NC_NOERR) goto done;

    /* Canonicalize the constraint */
    ncstat = dapfixprojections(dapcomm->oc.dapconstraint->projections);
    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}

    /* Fill in segment information */
    ncstat = dapqualifyconstraints(dapcomm->oc.dapconstraint);
    if(ncstat != NC_NOERR) goto done;

    /* Accumulate set of variables in the constraint's projections */
    ncstat = dapcomputeprojectedvars(dapcomm,dapcomm->oc.dapconstraint);
    if(ncstat) {THROWCHK(ncstat); goto done;}

    /* using the modified constraint, rebuild the constraint string */
    if(FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE)) {
	/* ignore all constraints */
	dapcomm->oc.urltext = ncuribuild(dapcomm->oc.url,NULL,NULL,NCURIBASE);
    } else {
	char* constraintstring = dcebuildconstraintstring(dapcomm->oc.dapconstraint);
        ncurisetquery(dapcomm->oc.url,constraintstring);
	nullfree(constraintstring);
        dapcomm->oc.urltext = ncuribuild(dapcomm->oc.url,NULL,NULL,NCURISVC);
    }

#ifdef DEBUG
fprintf(stderr,"ncdap3: final constraint: %s\n",dapcomm->oc.url->query);
#endif

    /* Estimate the variable sizes */
    estimatevarsizes(dapcomm);

    /* Build the meta data */
    ncstat = buildncstructures(dapcomm);
    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}

    /* Explicitly do not call enddef because it will complain
       about variables that are too large.
    */
#if 0
    ncstat = nc_endef(nc3id,NC_NOFILL,NULL);
    if(ncstat != NC_NOERR && ncstat != NC_EVARSIZE)
        {THROWCHK(ncstat); goto done;}
#endif

    { /* (for now) break abstractions*/
	    NC* ncsub;
	    NC3_INFO* nc3i;
	    CDFnode* unlimited = dapcomm->cdf.recorddim;
            /* get the dispatch data for the substrate */
            ncstat = NC_check_id(nc3id,&ncsub);
	    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
	    nc3i = (NC3_INFO*)ncsub->dispatchdata;
	    /* This must be checked after all dds and data processing
               so we can figure out the value of numrecs.
	    */
            if(unlimited != NULL) { /* Set the effective size of UNLIMITED */
                NC_set_numrecs(nc3i,unlimited->dim.declsize);
	    }
            /* Pretend the substrate is read-only */
	    NC_set_readonly(nc3i);
    }

    /* Do any necessary data prefetch */
    if(FLAGSET(dapcomm->controls,NCF_PREFETCH)
       && FLAGSET(dapcomm->controls,NCF_PREFETCH_EAGER)) {
        ncstat = prefetchdata(dapcomm);
        if(ncstat != NC_NOERR) {
            del_from_NCList((NC*)drno); /* undefine here */
	    {THROWCHK(ncstat); goto done;}
	}
    }

    return ncstat;

done:
    if(drno != NULL) NCD2_close(drno->ext_ncid,NULL);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return THROW(ncstat);
}

int
NCD2_close(int ncid, void* ignore)
{
    NC* drno;
    NCDAPCOMMON* dapcomm;
    int ncstatus = NC_NOERR;

    ncstatus = NC_check_id(ncid, (NC**)&drno);
    if(ncstatus != NC_NOERR) return THROW(ncstatus);
    dapcomm = (NCDAPCOMMON*)drno->dispatchdata;

    /* We call abort rather than close to avoid
       trying to write anything or try to pad file length
     */
    ncstatus = nc_abort(getnc3id(drno));

    /* clean NC* */
    freeNCDAPCOMMON(dapcomm);

    return THROW(ncstatus);
}

/**************************************************/

static NCerror
buildncstructures(NCDAPCOMMON* dapcomm)
{
    NCerror ncstat = NC_NOERR;
    CDFnode* dds = dapcomm->cdf.ddsroot;

    ncstat = buildglobalattrs(dapcomm,dds);
    if(ncstat != NC_NOERR) goto done;

    ncstat = builddims(dapcomm);
    if(ncstat != NC_NOERR) goto done;

    ncstat = buildvars(dapcomm);
    if(ncstat != NC_NOERR) goto done;

done:
    return THROW(ncstat);
}

static NCerror
builddims(NCDAPCOMMON* dapcomm)
{
    NCerror ncstat = NC_NOERR;
    int dimid;
    NClist* dimset = NULL;
    NC* ncsub;
    char* definename;

    /* collect all dimensions from variables */
    dimset = dapcomm->cdf.ddsroot->tree->dimnodes;

    /* Sort by fullname just for the fun of it */
    for(;;) {
	const size_t last = nclistlength(dimset);
	if (last == 0) break;
	int swap = 0;
	for(size_t i=0;i<last-1;i++) {
	    CDFnode* dim1 = (CDFnode*)nclistget(dimset,i);
	    CDFnode* dim2 = (CDFnode*)nclistget(dimset,i+1);
	    if(strcmp(dim1->ncfullname,dim2->ncfullname) > 0) {
		nclistset(dimset,i,(void*)dim2);
		nclistset(dimset,i+1,(void*)dim1);
		swap = 1;
		break;
	    }
	}
	if(!swap) break;
    }

    /* Define unlimited only if needed */
    if(dapcomm->cdf.recorddim != NULL) {
	CDFnode* unlimited = dapcomm->cdf.recorddim;
	definename = getdefinename(unlimited);
        ncstat = nc_def_dim(dapcomm->substrate.nc3id,
			definename,
			NC_UNLIMITED,
			&unlimited->ncid);
	nullfree(definename);
        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}

        /* get the id for the substrate */
        ncstat = NC_check_id(dapcomm->substrate.nc3id,&ncsub);
        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
#if 0
	nc3sub = (NC3_INFO*)&ncsub->dispatchdata;
        /* Set the effective size of UNLIMITED;
           note that this cannot easily be done through the normal API.*/
        NC_set_numrecs(nc3sub,unlimited->dim.declsize);
#endif

    }

    for(size_t i=0;i<nclistlength(dimset);i++) {
	CDFnode* dim = (CDFnode*)nclistget(dimset,i);
        if(dim->dim.basedim != NULL) continue; /* handle below */
	if(DIMFLAG(dim,CDFDIMRECORD)) continue; /* defined above */
#ifdef DEBUG1
fprintf(stderr,"define: dim: %s=%ld\n",dim->ncfullname,(long)dim->dim.declsize);
#endif
	definename = getdefinename(dim);
        ncstat = nc_def_dim(dapcomm->substrate.nc3id,definename,dim->dim.declsize,&dimid);
        if(ncstat != NC_NOERR) {
          THROWCHK(ncstat); nullfree(definename); goto done;
	}
	nullfree(definename);
        dim->ncid = dimid;
    }

    /* Make all duplicate dims have same dimid as basedim*/
    /* (see computecdfdimnames)*/
    for(size_t i=0;i<nclistlength(dimset);i++) {
	CDFnode* dim = (CDFnode*)nclistget(dimset,i);
        if(dim->dim.basedim != NULL) {
	    dim->ncid = dim->dim.basedim->ncid;
	}
    }
done:
    nclistfree(dimset);
    return THROW(ncstat);
}

/* Simultaneously build any associated attributes*/
/* and any necessary pseudo-dimensions for string types*/
static NCerror
buildvars(NCDAPCOMMON* dapcomm)
{
    size_t i,j;
    NCerror ncstat = NC_NOERR;
    int varid;
    NClist* varnodes = dapcomm->cdf.ddsroot->tree->varnodes;
    char* definename;

    ASSERT((varnodes != NULL));
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* var = (CDFnode*)nclistget(varnodes,i);
        int dimids[NC_MAX_VAR_DIMS];
	size_t ncrank;
        NClist* vardims = NULL;

	if(var->invisible) continue;
	if(var->array.basevar != NULL) continue;

#ifdef DEBUG1
fprintf(stderr,"buildvars.candidate=|%s|\n",var->ncfullname);
#endif

	vardims = var->array.dimsetall;
	ncrank = nclistlength(vardims);
	if(ncrank > 0) {
            for(j=0;j<ncrank;j++) {
                CDFnode* dim = (CDFnode*)nclistget(vardims,j);
                dimids[j] = dim->ncid;
 	    }
        }
	definename = getdefinename(var);

#ifdef DEBUG1
fprintf(stderr,"define: var: %s/%s",
		definename,var->ocname);
if(ncrank > 0) {
int k;
for(k=0;k<ncrank;k++) {
CDFnode* dim = (CDFnode*)nclistget(vardims,k);
fprintf(stderr,"[%ld]",dim->dim.declsize);
 }
 }
fprintf(stderr,"\n");
#endif
        ncstat = nc_def_var(dapcomm->substrate.nc3id,
		        definename,
                        var->externaltype,
                        (int)ncrank,
                        (ncrank==0?NULL:dimids),
                        &varid);
	nullfree(definename);
        if(ncstat != NC_NOERR) {
	    THROWCHK(ncstat);
	    goto done;
	}
        var->ncid = varid;
	if(var->attributes != NULL) {
	    NCattribute* unsignedatt = NULL;
	    int unsignedval = 0;
	    /* See if variable has _Unsigned attribute */ 
	    for(j=0;j<nclistlength(var->attributes);j++) {
		NCattribute* att = (NCattribute*)nclistget(var->attributes,j);
		if(strcmp(att->name,"_Unsigned") == 0) {
		    char* value = nclistget(att->values,0);
		    unsignedatt = att;
		    if(value != NULL) {
			if(strcasecmp(value,"false")==0
			   || strcmp(value,"0")==0)
			    unsignedval = 0;
			else
			    unsignedval = 1;
		    }
		    break;
		}
	    }
	    for(j=0;j<nclistlength(var->attributes);j++) {
		NCattribute* att = (NCattribute*)nclistget(var->attributes,j);
		char* val = NULL;
		/* Check for _FillValue/Variable mismatch */
		if(strcmp(att->name,NC_FillValue)==0) {
		    /* Special case var is byte, fillvalue is int16 and 
			unsignedattr == 0;
			This exception is needed because DAP2 byte type
			is equivalent to netcdf ubyte type. So passing
                        a signed byte thru DAP2 requires some type and
                        signedness hacking that we have to undo.
		    */
		    if(var->etype == NC_UBYTE
			&& att->etype == NC_SHORT
			&& unsignedatt != NULL && unsignedval == 0) {
			/* Forcibly change the attribute type and signedness */
			att->etype = NC_BYTE;
			val = nclistremove(unsignedatt->values,0);
			if(val) free(val);
			nclistpush(unsignedatt->values,strdup("false"));
		    } else if(att->etype != var->etype) {/* other mismatches */
			/* See if mismatch is allowed */
			if(FLAGSET(dapcomm->controls,NCF_FILLMISMATCH)) {
			    /* Forcibly change the attribute type to match */
			    att->etype = var->etype;
			} else {
	  		    /* Log a message */
	                    nclog(NCLOGWARN,"_FillValue/Variable type mismatch: variable=%s",var->ncbasename);
			    ncstat = NC_EBADTYPE; /* fail */
			    goto done;
			}
		    }
		}
		ncstat = buildattribute(dapcomm,var,att);
        	if(ncstat != NC_NOERR) goto done;
	    }
	}
	/* Tag the variable with its DAP path */
	if(dapparamcheck(dapcomm,"show","projection"))
	    showprojection(dapcomm,var);
    }
done:
    return THROW(ncstat);
}

static NCerror
buildglobalattrs(NCDAPCOMMON* dapcomm, CDFnode* root)
{
    size_t i;
    NCerror ncstat = NC_NOERR;
    const char* txt;
    char *nltxt, *p;
    NCbytes* buf = NULL;
    NClist* cdfnodes;

    if(root->attributes != NULL) {
        for(i=0;i<nclistlength(root->attributes);i++) {
   	    NCattribute* att = (NCattribute*)nclistget(root->attributes,i);
	    ncstat = buildattribute(dapcomm,NULL,att);
            if(ncstat != NC_NOERR) goto done;
	}
    }

    /* Add global attribute identifying the sequence dimensions */
    if(dapparamcheck(dapcomm,"show","seqdims")) {
        buf = ncbytesnew();
        cdfnodes = dapcomm->cdf.ddsroot->tree->nodes;
        for(i=0;i<nclistlength(cdfnodes);i++) {
	    CDFnode* dim = (CDFnode*)nclistget(cdfnodes,i);
	    if(dim->nctype != NC_Dimension) continue;
	    if(DIMFLAG(dim,CDFDIMSEQ)) {
	        char* cname = cdflegalname(dim->ocname);
	        if(ncbyteslength(buf) > 0) ncbytescat(buf,", ");
	        ncbytescat(buf,cname);
	        nullfree(cname);
	    }
	}
        if(ncbyteslength(buf) > 0) {
            ncstat = nc_put_att_text(dapcomm->substrate.nc3id,NC_GLOBAL,"_sequence_dimensions",
	           ncbyteslength(buf),ncbytescontents(buf));
	}
    }

    /* Define some additional system global attributes
       depending on show= clientparams*/
    /* Ignore failures*/

    if(dapparamcheck(dapcomm,"show","translate")) {
        /* Add a global attribute to show the translation */
        ncstat = nc_put_att_text(dapcomm->substrate.nc3id,NC_GLOBAL,"_translate",
	           strlen("netcdf-3"),"netcdf-3");
    }
    if(dapparamcheck(dapcomm,"show","url")) {
	if(dapcomm->oc.rawurltext != NULL)
            ncstat = nc_put_att_text(dapcomm->substrate.nc3id,NC_GLOBAL,"_url",
				       strlen(dapcomm->oc.rawurltext),dapcomm->oc.rawurltext);
    }
    if(dapparamcheck(dapcomm,"show","dds")) {
	txt = NULL;
	if(dapcomm->cdf.ddsroot != NULL)
  	    txt = oc_tree_text(dapcomm->oc.conn,dapcomm->cdf.ddsroot->ocnode);
	if(txt != NULL) {
	    /* replace newlines with spaces*/
	    nltxt = nulldup(txt);
	    for(p=nltxt;*p;p++) {if(*p == '\n' || *p == '\r' || *p == '\t') {*p = ' ';}};
            ncstat = nc_put_att_text(dapcomm->substrate.nc3id,NC_GLOBAL,"_dds",strlen(nltxt),nltxt);
	    nullfree(nltxt);
	}
    }
    if(dapparamcheck(dapcomm,"show","das")) {
	txt = NULL;
	if(dapcomm->oc.ocdasroot != NULL)
	    txt = oc_tree_text(dapcomm->oc.conn,dapcomm->oc.ocdasroot);
	if(txt != NULL) {
	    nltxt = nulldup(txt);
	    for(p=nltxt;*p;p++) {if(*p == '\n' || *p == '\r' || *p == '\t') {*p = ' ';}};
            ncstat = nc_put_att_text(dapcomm->substrate.nc3id,NC_GLOBAL,"_das",strlen(nltxt),nltxt);
	    nullfree(nltxt);
	}
    }

done:
    ncbytesfree(buf);
    return THROW(ncstat);
}

static NCerror
buildattribute(NCDAPCOMMON* dapcomm, CDFnode* var, NCattribute* att)
{
    size_t i;
    NCerror ncstat = NC_NOERR;
    size_t nvalues = nclistlength(att->values);
    int varid = (var == NULL ? NC_GLOBAL : var->ncid);
    void* mem = NULL;

    /* If the type of the attribute is string, then we need*/
    /* to convert to a single character string by concatenation.
	modified: 10/23/09 to insert newlines.
	modified: 10/28/09 to interpret escapes
    */
    if(att->etype == NC_STRING || att->etype == NC_URL) {
	char* newstring = NULL;
	size_t newlen = 0;
	for(i=0;i<nvalues;i++) {
	    char* s = (char*)nclistget(att->values,i);
	    newlen += (1+strlen(s));
	}
	newlen++; /* for strlcat nul */
        newstring = (char*)malloc(newlen+1);
        MEMCHECK(newstring,NC_ENOMEM);
	newstring[0] = '\0';
	for(i=0;i<nvalues;i++) {
	    char* s = (char*)nclistget(att->values,i);
	    if(i > 0) strlcat(newstring,"\n",newlen);
	    strlcat(newstring,s,newlen);
	}
        dapexpandescapes(newstring);
	if(newstring[0]=='\0')
	    ncstat = nc_put_att_text(dapcomm->substrate.nc3id,varid,att->name,1,newstring);
	else
	    ncstat = nc_put_att_text(dapcomm->substrate.nc3id,varid,att->name,strlen(newstring),newstring);
	free(newstring);
        if(ncstat) goto done;
    } else {
	nc_type atype;
	size_t typesize;
	atype = nctypeconvert(dapcomm,att->etype);
	typesize = nctypesizeof(atype);
	if (nvalues > 0) {
	    mem = malloc(typesize * nvalues);
	}
        ncstat = dapcvtattrval(atype,mem,att->values,att);
        if(ncstat == NC_ERANGE)
	    nclog(NCLOGERR,"Attribute value out of range: %s:%s",
		(var==NULL?"":var->ncbasename),att->name);
        if(ncstat) goto done;
        ncstat = nc_put_att(dapcomm->substrate.nc3id,varid,att->name,atype,nvalues,mem);
        if(ncstat) goto done;
    }
done:
    if(mem) free(mem);
    return THROW(ncstat);
}

static char*
getdefinename(CDFnode* node)
{
    char* spath = NULL;
    NClist* path = NULL;

    switch (node->nctype) {
    case NC_Atomic:
	/* The define name is same as the fullname with elided nodes */
	path = nclistnew();
        collectnodepath(node,path,!WITHDATASET);
        spath = makepathstring(path,".",PATHNC|PATHELIDE);
        nclistfree(path);
	break;

    case NC_Dimension:
	/* Return just the node's ncname */
	spath = nulldup(node->ncbasename);
	break;

    default:
	PANIC("unexpected nctype");
    }
    return spath;
}

int
NCDAP2_ping(const char* url)
{
    OCerror ocstat = OC_NOERR;
    ocstat = oc_ping(url);
    return ocerrtoncerr(ocstat);
}

int
NCD2_inq_format_extended(int ncid, int* formatp, int* modep)
{
    NC* nc;
    int ncstatus = NC_check_id(ncid, (NC**)&nc);
    if(ncstatus != NC_NOERR) return THROW(ncstatus);
    if(modep) *modep = nc->mode;
    if(formatp) *formatp = NC_FORMATX_DAP2;
    return NC_NOERR;
}

/**************************************************/
/* Support functions */

/*
   Provide short and/or unified names for dimensions.
   This must mimic lib-ncdap, which is difficult.
*/
NCerror
computecdfdimnames(NCDAPCOMMON* nccomm)
{
    size_t i,j;
    char tmp[NC_MAX_NAME*2];
    NClist* conflicts = nclistnew();
    NClist* varnodes = nccomm->cdf.ddsroot->tree->varnodes;
    NClist* alldims;
    NClist* basedims;

    /* Collect all dimension nodes from dimsetall lists */

    alldims = getalldims(nccomm,0);

    /* Assign an index to all anonymous dimensions
       vis-a-vis its containing variable
    */
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* var = (CDFnode*)nclistget(varnodes,i);
        for(j=0;j<nclistlength(var->array.dimsetall);j++) {
	    CDFnode* dim = (CDFnode*)nclistget(var->array.dimsetall,j);
	    if(dim->ocname != NULL) continue; /* not anonymous */
 	    computedimindexanon(dim,var);
	}
    }

    /* Unify dimensions by defining one dimension as the "base"
       dimension, and make all "equivalent" dimensions point to the
       base dimension.
	1. Equivalent means: same size and both have identical non-null names.
	2. Dims with same name but different sizes will be handled separately
    */
    for(i=0;i<nclistlength(alldims);i++) {
	CDFnode* dupdim = NULL;
	CDFnode* basedim = (CDFnode*)nclistget(alldims,i);
	if(basedim == NULL) continue;
	if(basedim->dim.basedim != NULL) continue; /* already processed*/
	for(j=i+1;j<nclistlength(alldims);j++) { /* Sigh, n**2 */
	    dupdim = (CDFnode*)nclistget(alldims,j);
	    if(basedim == dupdim) continue;
	    if(dupdim == NULL) continue;
	    if(dupdim->dim.basedim != NULL) continue; /* already processed */
	    if(!equivalentdim(basedim,dupdim))
		continue;
            dupdim->dim.basedim = basedim; /* equate */
#ifdef DEBUG1
fprintf(stderr,"assign: %s/%s -> %s/%s\n",
basedim->dim.array->ocname,basedim->ocname,
dupdim->dim.array->ocname,dupdim->ocname
);
#endif
	}
    }

    /* Next case: same name and different sizes*/
    /* => rename second dim */

    for(i=0;i<nclistlength(alldims);i++) {
	CDFnode* basedim = (CDFnode*)nclistget(alldims,i);
	if(basedim->dim.basedim != NULL) continue;
	/* Collect all conflicting dimensions */
	nclistclear(conflicts);
        for(j=i+1;j<nclistlength(alldims);j++) {
	    CDFnode* dim = (CDFnode*)nclistget(alldims,j);
	    if(dim->dim.basedim != NULL) continue;
	    if(dim->ocname == NULL && basedim->ocname == NULL) continue;
	    if(dim->ocname == NULL || basedim->ocname == NULL) continue;
	    if(strcmp(dim->ocname,basedim->ocname)!=0) continue;
	    if(dim->dim.declsize == basedim->dim.declsize) continue;
#ifdef DEBUG2
fprintf(stderr,"conflict: %s[%lu] %s[%lu]\n",
			basedim->ncfullname,(unsigned long)basedim->dim.declsize,
			dim->ncfullname,(unsigned long)dim->dim.declsize);
#endif
	    nclistpush(conflicts,(void*)dim);
	}
	/* Give  all the conflicting dimensions an index */
	for(j=0;j<nclistlength(conflicts);j++) {
	    CDFnode* dim = (CDFnode*)nclistget(conflicts,j);
	    dim->dim.index1 = (int)j+1;
	}
    }
    nclistfree(conflicts);

    /* Replace all non-base dimensions with their base dimension */
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(varnodes,i);
	replacedims(node->array.dimsetall);
	replacedims(node->array.dimsetplus);
	replacedims(node->array.dimset0);
    }

    /* Collect list of all basedims */
    basedims = nclistnew();
    for(i=0;i<nclistlength(alldims);i++) {
	CDFnode* dim = (CDFnode*)nclistget(alldims,i);
	if(dim->dim.basedim == NULL) {
	    if(!nclistcontains(basedims,(void*)dim)) {
		nclistpush(basedims,(void*)dim);
	    }
	}
    }

    nccomm->cdf.ddsroot->tree->dimnodes = basedims;

    /* cleanup */
    nclistfree(alldims);

    /* Assign ncbasenames and ncfullnames to base dimensions */
    for(i=0;i<nclistlength(basedims);i++) {
	CDFnode* dim = (CDFnode*)nclistget(basedims,i);
	CDFnode* var = dim->dim.array;
	if(dim->dim.basedim != NULL) PANIC1("nonbase basedim: %s\n",dim->ocname);
	/* stringdim names are already assigned */
	if(dim->ocname == NULL) { /* anonymous: use the index to compute the name */
            snprintf(tmp,sizeof(tmp),"%s_%d",
                            var->ncfullname,dim->dim.index1-1);
            nullfree(dim->ncbasename);
            dim->ncbasename = cdflegalname(tmp);
            nullfree(dim->ncfullname);
            dim->ncfullname = nulldup(dim->ncbasename);
    	} else { /* !anonymous; use index1 if defined */
   	    char* legalname = cdflegalname(dim->ocname);
	    nullfree(dim->ncbasename);
	    if(dim->dim.index1 > 0) {/* need to fix conflicting names (see above) */
	        char sindex[64];
		size_t baselen;
		snprintf(sindex,sizeof(sindex),"_%d",dim->dim.index1);
		baselen = strlen(sindex)+strlen(legalname);
		baselen++; /* for strlcat nul */
		dim->ncbasename = (char*)malloc(baselen+1);
		if(dim->ncbasename == NULL) {nullfree(legalname); return NC_ENOMEM;}
		strncpy(dim->ncbasename,legalname,baselen);
		strlcat(dim->ncbasename,sindex,baselen);
		nullfree(legalname);
	    } else {/* standard case */
	        dim->ncbasename = legalname;
	    }
    	    nullfree(dim->ncfullname);
	    dim->ncfullname = nulldup(dim->ncbasename);
	}
     }

    /* Verify unique and defined names for dimensions*/
    for(i=0;i<nclistlength(basedims);i++) {
	CDFnode* dim1 = (CDFnode*)nclistget(basedims,i);
        CDFnode* dim2 = NULL;
	if(dim1->dim.basedim != NULL) PANIC1("nonbase basedim: %s\n",dim1->ncbasename);
	if(dim1->ncbasename == NULL || dim1->ncfullname == NULL)
	    PANIC1("missing dim names: %s",dim1->ocname);
	/* search backward so we can delete duplicates */
	for(j=nclistlength(basedims)-1;j>i;j--) {
      if(!dim1->ncfullname) continue;
      dim2 = (CDFnode*)nclistget(basedims,j);
      if(strcmp(dim1->ncfullname,dim2->ncfullname)==0) {
		/* complain and suppress one of them */
		fprintf(stderr,"duplicate dim names: %s[%lu] %s[%lu]\n",
                dim1->ncfullname,(unsigned long)dim1->dim.declsize,
                dim2->ncfullname,(unsigned long)dim2->dim.declsize);
		nclistremove(basedims,j);
      }
	}
    }

#ifdef DEBUG
for(i=0;i<nclistlength(basedims);i++) {
CDFnode* dim = (CDFnode*)nclistget(basedims,i);
fprintf(stderr,"basedim: %s=%ld\n",dim->ncfullname,(long)dim->dim.declsize);
 }
#endif

    return NC_NOERR;
}

int
constrainable(NCURI* durl)
{
   const char** protocol = constrainableprotocols;
   for(;*protocol;protocol++) {
	if(strcmp(durl->protocol,*protocol)==0)
	    return 1;
   }
   return 0;
}

/* Lookup a parameter key; case insensitive */
static const char*
paramlookup(NCDAPCOMMON* state, const char* key)
{
    const char* value = NULL;
    if(state == NULL || key == NULL || state->oc.url == NULL) return NULL;
    value = ncurifragmentlookup(state->oc.url,key);
    return value;
}

/* Note: this routine only applies some common
   client parameters, other routines may apply
   specific ones.
*/

static NCerror
applyclientparams(NCDAPCOMMON* nccomm)
{
    size_t i;
    unsigned int len;
    unsigned int dfaltstrlen = DEFAULTSTRINGLENGTH;
    unsigned int dfaltseqlim = DEFAULTSEQLIMIT;
    const char* value;
    char tmpname[NC_MAX_NAME+32];
    char* pathstr = NULL;
    OClink conn = nccomm->oc.conn;
    unsigned long limit;

    ASSERT(nccomm->oc.url != NULL);

    nccomm->cdf.cache->cachelimit = DFALTCACHELIMIT;
    value = paramlookup(nccomm,"cachelimit");
    limit = getlimitnumber(value);
    if(limit > 0) nccomm->cdf.cache->cachelimit = limit;

    nccomm->cdf.fetchlimit = DFALTFETCHLIMIT;
    value = paramlookup(nccomm,"fetchlimit");
    limit = getlimitnumber(value);
    if(limit > 0) nccomm->cdf.fetchlimit = limit;

    nccomm->cdf.smallsizelimit = DFALTSMALLLIMIT;
    value = paramlookup(nccomm,"smallsizelimit");
    limit = getlimitnumber(value);
    if(limit > 0) nccomm->cdf.smallsizelimit = limit;

    nccomm->cdf.cache->cachecount = DFALTCACHECOUNT;
#ifdef HAVE_GETRLIMIT
    { struct rlimit rl;
      if(getrlimit(RLIMIT_NOFILE, &rl) >= 0) {
	nccomm->cdf.cache->cachecount = (size_t)(rl.rlim_cur / 2);
      }
    }
#endif
    value = paramlookup(nccomm,"cachecount");
    limit = getlimitnumber(value);
    if(limit > 0) nccomm->cdf.cache->cachecount = limit;
    /* Ignore limit if not caching */
    if(!FLAGSET(nccomm->controls,NCF_CACHE))
        nccomm->cdf.cache->cachecount = 0;

    if(paramlookup(nccomm,"nolimit") != NULL)
	dfaltseqlim = 0;
    value = paramlookup(nccomm,"limit");
    if(value != NULL && strlen(value) != 0) {
        if(sscanf(value,"%u",&len) && len > 0) dfaltseqlim = len;
    }
    nccomm->cdf.defaultsequencelimit = dfaltseqlim;

    /* allow embedded _ */
    value = paramlookup(nccomm,"stringlength");
    if(value == NULL) 
        value = paramlookup(nccomm,"maxstrlen");
    if(value != NULL && strlen(value) != 0) {
        if(sscanf(value,"%u",&len) && len > 0) dfaltstrlen = len;
    } 
    nccomm->cdf.defaultstringlength = dfaltstrlen;

    /* String dimension limits apply to variables */
    for(i=0;i<nclistlength(nccomm->cdf.ddsroot->tree->varnodes);i++) {
	CDFnode* var = (CDFnode*)nclistget(nccomm->cdf.ddsroot->tree->varnodes,i);
	/* Define the client param stringlength/maxstrlen for this variable*/
	/* create the variable path name */
	var->maxstringlength = 0; /* => use global dfalt */
	strncpy(tmpname,"stringlength_",sizeof(tmpname));
	pathstr = makeocpathstring(conn,var->ocnode,".");
	strlcat(tmpname,pathstr,sizeof(tmpname));
	value = paramlookup(nccomm,tmpname);
	if(value == NULL) {
	    strncpy(tmpname,"maxstrlen_",sizeof(tmpname));
	    strlcat(tmpname,pathstr,sizeof(tmpname));
	    value = paramlookup(nccomm,tmpname);
        }
	nullfree(pathstr);
        if(value != NULL && strlen(value) != 0) {
            if(sscanf(value,"%d",&len) && len > 0) var->maxstringlength = len;
	}
    }
    /* Sequence limits apply to sequences */
    for(i=0;i<nclistlength(nccomm->cdf.ddsroot->tree->nodes);i++) {
	CDFnode* var = (CDFnode*)nclistget(nccomm->cdf.ddsroot->tree->nodes,i);
	if(var->nctype != NC_Sequence) continue;
	var->sequencelimit = dfaltseqlim;
	strncpy(tmpname,"nolimit_",sizeof(tmpname));
	pathstr = makeocpathstring(conn,var->ocnode,".");
	strlcat(tmpname,pathstr,sizeof(tmpname));
	if(paramlookup(nccomm,tmpname) != NULL)
	    var->sequencelimit = 0;
	strncpy(tmpname,"limit_",sizeof(tmpname));
	strlcat(tmpname,pathstr,sizeof(tmpname));
	value = paramlookup(nccomm,tmpname);
        if(value != NULL && strlen(value) != 0) {
            if(sscanf(value,"%d",&len) && len > 0)
		var->sequencelimit = len;
	}
	nullfree(pathstr);
    }

    /* test for the appropriate fetch flags */
    value = paramlookup(nccomm,"fetch");
    if(value != NULL && strlen(value) > 0) {
	if(value[0] == 'd' || value[0] == 'D') {
            SETFLAG(nccomm->controls,NCF_ONDISK);
	}
    }

    /* test for the force-whole-var flag */
    value = paramlookup(nccomm,"wholevar");
    if(value != NULL) {
        SETFLAG(nccomm->controls,NCF_WHOLEVAR);
    }

    return NC_NOERR;
}

/**
 *  Given an anonymous dimension, compute the
 *  effective 0-based index wrt to the specified var.
 *  The result should mimic the libnc-dap indices.
 */

static void
computedimindexanon(CDFnode* dim, CDFnode* var)
{
    size_t i;
    NClist* dimset = var->array.dimsetall;
    for(i=0;i<nclistlength(dimset);i++) {
	CDFnode* candidate = (CDFnode*)nclistget(dimset,i);
        if(dim == candidate) {
           dim->dim.index1 = (int)i+1;
	   return;
	}
    }
}

/* Replace dims in a list with their corresponding basedim */
static void
replacedims(NClist* dims)
{
    size_t i;
    for(i=0;i<nclistlength(dims);i++) {
        CDFnode* dim = (CDFnode*)nclistget(dims,i);
	CDFnode* basedim = dim->dim.basedim;
	if(basedim == NULL) continue;
	nclistset(dims,i,(void*)basedim);
    }
}

/**
 Two dimensions are equivalent if
 1. they have the same size
 2. neither are anonymous
 3. they ave the same names.
 */
static int
equivalentdim(CDFnode* basedim, CDFnode* dupdim)
{
    if(dupdim->dim.declsize != basedim->dim.declsize) return 0;
    if(basedim->ocname == NULL && dupdim->ocname == NULL) return 0;
    if(basedim->ocname == NULL || dupdim->ocname == NULL) return 0;
    if(strcmp(dupdim->ocname,basedim->ocname) != 0) return 0;
    return 1;
}

static void
getalldimsa(NClist* dimset, NClist* alldims)
{
    size_t i;
    for(i=0;i<nclistlength(dimset);i++) {
	CDFnode* dim = (CDFnode*)nclistget(dimset,i);
	if(!nclistcontains(alldims,(void*)dim)) {
#ifdef DEBUG3
fprintf(stderr,"getalldims: %s[%lu]\n",
			dim->ncfullname,(unsigned long)dim->dim.declsize);
#endif
	    nclistpush(alldims,(void*)dim);
	}
    }
}

/* Accumulate a set of all the known dimensions
   vis-a-vis defined variables
*/
NClist*
getalldims(NCDAPCOMMON* nccomm, int visibleonly)
{
    size_t i;
    NClist* alldims = nclistnew();
    NClist* varnodes = nccomm->cdf.ddsroot->tree->varnodes;

    /* get bag of all dimensions */
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(varnodes,i);
	if(!visibleonly || !node->invisible) {
	    getalldimsa(node->array.dimsetall,alldims);
	}
    }
    return alldims;
}


static NCerror
addstringdims(NCDAPCOMMON* dapcomm)
{
    /* for all variables of string type, we will need another dimension
       to represent the string; Accumulate the needed sizes and create
       the dimensions with a specific name: either as specified
       in DODS{...} attribute set or defaulting to the variable name.
       All such dimensions are global.
    */
    size_t i;
    NClist* varnodes = dapcomm->cdf.ddsroot->tree->varnodes;
    CDFnode* globalsdim = NULL;
    char dimname[4096];
    size_t dimsize;

    /* Start by creating the global string dimension */
    snprintf(dimname,sizeof(dimname),"maxStrlen%lu",
	    (unsigned long)dapcomm->cdf.defaultstringlength);
    globalsdim = makecdfnode(dapcomm, dimname, OC_Dimension, NULL,
                                 dapcomm->cdf.ddsroot);
    nclistpush(dapcomm->cdf.ddsroot->tree->nodes,(void*)globalsdim);
    DIMFLAGSET(globalsdim,CDFDIMSTRING);
    globalsdim->dim.declsize = dapcomm->cdf.defaultstringlength;
    globalsdim->dim.declsize0 = globalsdim->dim.declsize;
    globalsdim->dim.array = dapcomm->cdf.ddsroot;
    globalsdim->ncbasename = cdflegalname(dimname);
    globalsdim->ncfullname = nulldup(globalsdim->ncbasename);
    dapcomm->cdf.globalstringdim = globalsdim;

    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* var = (CDFnode*)nclistget(varnodes,i);
	CDFnode* sdim = NULL;

	/* Does this node need a string dim? */
	if(var->etype != NC_STRING && var->etype != NC_URL) continue;

	dimsize = 0;
	if(var->dodsspecial.maxstrlen > 0)
	    dimsize = var->dodsspecial.maxstrlen;
	else
	    dimsize = var->maxstringlength;

	/* check is a variable-specific string length was specified */
	if(dimsize == 0)
	    sdim = dapcomm->cdf.globalstringdim; /* use default */
	else {
	    /* create a pseudo dimension for the charification of the string*/
	    if(var->dodsspecial.dimname != NULL) {
	        strncpy(dimname,var->dodsspecial.dimname,sizeof(dimname));
	        dimname[sizeof(dimname)-1] = '\0';
	    } else
	        snprintf(dimname,sizeof(dimname),"maxStrlen%lu",
			 (unsigned long)dimsize);
	    sdim = makecdfnode(dapcomm, dimname, OC_Dimension, NULL,
                                 dapcomm->cdf.ddsroot);
	    if(sdim == NULL) return THROW(NC_ENOMEM);
	    nclistpush(dapcomm->cdf.ddsroot->tree->nodes,(void*)sdim);
	    DIMFLAGSET(sdim,CDFDIMSTRING);
	    sdim->dim.declsize = dimsize;
	    sdim->dim.declsize0 = dimsize;
	    sdim->dim.array = var;
	    sdim->ncbasename = cdflegalname(sdim->ocname);
	    sdim->ncfullname = nulldup(sdim->ncbasename);
	}
	/* tag the variable with its string dimension*/
	var->array.stringdim = sdim;
    }
    return NC_NOERR;
}

static NCerror
defrecorddim(NCDAPCOMMON* dapcomm)
{
    size_t i;
    NCerror ncstat = NC_NOERR;
    NClist* basedims;

    if(dapcomm->cdf.recorddimname == NULL) return NC_NOERR; /* ignore */
    /* Locate the base dimension matching the record dim */
    basedims = dapcomm->cdf.ddsroot->tree->dimnodes;
    for(i=0;i<nclistlength(basedims);i++) {
        CDFnode* dim = (CDFnode*)nclistget(basedims,i);
	if(strcmp(dim->ocname,dapcomm->cdf.recorddimname) != 0) continue;
        DIMFLAGSET(dim,CDFDIMRECORD);
	dapcomm->cdf.recorddim = dim;
	break;
    }

    return ncstat;
}

static NCerror
defseqdims(NCDAPCOMMON* dapcomm)
{
    unsigned int i = 0;
    NCerror ncstat = NC_NOERR;
    int seqdims = 1; /* default is to compute seq dims counts */

    /* Does the user want to compute actual sequence sizes? */
    if(dapparamvalue(dapcomm,"noseqdims")) seqdims = 0;

    /*
	Compute and define pseudo dimensions for sequences
	meeting the following qualifications:
	1. all parents (transitively) of the sequence must
           be either a dataset or a scalar structure.
	2. it must be possible to find a usable sequence constraint.
	All other sequences will be ignored.
    */

    for(i=0;i<nclistlength(dapcomm->cdf.ddsroot->tree->seqnodes);i++) {
        CDFnode* seq = (CDFnode*)nclistget(dapcomm->cdf.ddsroot->tree->seqnodes,i);
	size_t seqsize = 0;
	CDFnode* sqdim = NULL;
	CDFnode* container;
	/* Does this sequence match the requirements for use ? */
	seq->usesequence = 1; /* assume */
	for(container=seq->container;container != NULL;container=container->container) {
	    if(container->nctype == NC_Dataset) break;
	    if(container->nctype != NC_Structure
	       || nclistlength(container->array.dimset0) > 0)
		{seq->usesequence = 0; break;}/* no good */
	}
	/* Does the user want us to compute the actual sequence dim size? */
	if(seq->usesequence && seqdims) {
	    ncstat = getseqdimsize(dapcomm,seq,&seqsize);
	    if(ncstat != NC_NOERR) {
                /* Cannot read sequence; mark as unusable */
	        seq->usesequence = 0;
	    }
	} else { /* !seqdims default to size = 1 */
	    seqsize = 1;
	}
	if(seq->usesequence) {
	    /* Note: we are making the dimension in the dds root tree */
            ncstat = makeseqdim(dapcomm,seq,seqsize,&sqdim);
            if(ncstat) goto fail;
            seq->array.seqdim = sqdim;
	} else
            seq->array.seqdim = NULL;
    }

fail:
    return ncstat;
}

static NCerror
showprojection(NCDAPCOMMON* dapcomm, CDFnode* var)
{
    size_t i,rank;
    NCerror ncstat = NC_NOERR;
    NCbytes* projection = ncbytesnew();
    NClist* path = nclistnew();
    NC* drno = dapcomm->controller;

    /* Collect the set of DDS node name forming the xpath */
    collectnodepath(var,path,WITHOUTDATASET);
    for(i=0;i<nclistlength(path);i++) {
        CDFnode* node = (CDFnode*)nclistget(path,i);
	if(i > 0) ncbytescat(projection,".");
	ncbytescat(projection,node->ocname);
    }
    nclistfree(path);
    /* Now, add the dimension info */
    rank = nclistlength(var->array.dimset0);
    for(i=0;i<rank;i++) {
	CDFnode* dim = (CDFnode*)nclistget(var->array.dimset0,i);
	char tmp[32];
	ncbytescat(projection,"[");
	snprintf(tmp,sizeof(tmp),"%lu",(unsigned long)dim->dim.declsize);
	ncbytescat(projection,tmp);
	ncbytescat(projection,"]");
    }
    /* Define the attribute */
    ncstat = nc_put_att_text(getncid(drno),var->ncid,
                               "_projection",
		               ncbyteslength(projection),
			       ncbytescontents(projection));
    ncbytesfree(projection);
    return ncstat;
}

static NCerror
getseqdimsize(NCDAPCOMMON* dapcomm, CDFnode* seq, size_t* sizep)
{
    NCerror ncstat = NC_NOERR;
    OCerror ocstat = OC_NOERR;
    OClink conn = dapcomm->oc.conn;
    OCddsnode ocroot;
    CDFnode* dxdroot;
    CDFnode* xseq;
    NCbytes* seqcountconstraints = ncbytesnew();
    size_t seqsize = 0;

    /* Read the minimal amount of data in order to get the count */
    /* If the url is unconstrainable, then get the whole thing */
    computeseqcountconstraints(dapcomm,seq,seqcountconstraints);
#ifdef DEBUG
fprintf(stderr,"seqcountconstraints: %s\n",ncbytescontents(seqcountconstraints));
#endif

    /* Fetch the minimal data */
    if(FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE))
        ncstat = dap_fetch(dapcomm,conn,NULL,OCDATADDS,&ocroot);
    else
        ncstat = dap_fetch(dapcomm,conn,ncbytescontents(seqcountconstraints),OCDATADDS,&ocroot);
    if(ncstat) goto fail;

    ncstat = buildcdftree(dapcomm,ocroot,OCDATA,&dxdroot);
    if(ncstat) goto fail;
    /* attach DATADDS to DDS */
    ncstat = attach(dxdroot,seq);
    if(ncstat) goto fail;

    /* WARNING: we are now switching to datadds tree */
    xseq = seq->attachment;
    ncstat = countsequence(dapcomm,xseq,&seqsize);
    if(ncstat != NC_NOERR) goto fail;

#ifdef DEBUG
fprintf(stderr,"sequencesize: %s = %lu\n",seq->ocname,(unsigned long)seqsize);
#endif

    /* throw away the fetch'd trees */
    unattach(dapcomm->cdf.ddsroot);
    freecdfroot(dxdroot);
#if 1
/*Note sure what this is doing?*/
    if(ncstat != NC_NOERR) {
        /* Cannot get DATADDDS*/
	char* code;
	char* msg;
	long httperr;
	oc_svcerrordata(dapcomm->oc.conn,&code,&msg,&httperr);
	if(code != NULL) {
	    nclog(NCLOGERR,"oc_fetch_datadds failed: %s %s %l",
			code,msg,httperr);
	}
	ocstat = OC_NOERR;
    }
#endif

    if(sizep) *sizep = seqsize;

fail:
    ncbytesfree(seqcountconstraints);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return ncstat;
}

static NCerror
makeseqdim(NCDAPCOMMON* dapcomm, CDFnode* seq, size_t count, CDFnode** sqdimp)
{
    CDFnode* sqdim;
    CDFnode* root = seq->root;
    CDFtree* tree = root->tree;

    /* build the dimension with given size; keep the dimension anonymous */
    sqdim = makecdfnode(dapcomm,seq->ocname,OC_Dimension,NULL,root);
    if(sqdim == NULL) return THROW(NC_ENOMEM);
    nclistpush(tree->nodes,(void*)sqdim);
    /* Assign a name to the sequence node */
    sqdim->ncbasename = cdflegalname(seq->ocname);
    sqdim->ncfullname = nulldup(sqdim->ncbasename);
    DIMFLAGSET(sqdim,CDFDIMSEQ);
    sqdim->dim.declsize = count;
    sqdim->dim.declsize0 = count;
    sqdim->dim.array = seq;
    if(sqdimp) *sqdimp = sqdim;
    return NC_NOERR;
}

static NCerror
countsequence(NCDAPCOMMON* dapcomm, CDFnode* xseq, size_t* sizep)
{
    unsigned int i;
    NClist* path = nclistnew();
    OCerror ocstat = OC_NOERR;
    NCerror ncstat = NC_NOERR;
    OClink conn = dapcomm->oc.conn;
    size_t recordcount;
    CDFnode* xroot;
    OCdatanode data = NULL;

    ASSERT((xseq->nctype == NC_Sequence));

    /* collect the path to the sequence node */
    collectnodepath(xseq,path,WITHDATASET);

    /* Get tree root */
    ASSERT(xseq->root == (CDFnode*)nclistget(path,0));
    xroot = xseq->root;
    ocstat = oc_data_getroot(conn,xroot->tree->ocroot,&data);
    if(ocstat) goto done;

    /* Basically we use the path to walk the data instances to reach
       the sequence instance
    */
    for(i=0;i<nclistlength(path);i++) {
        CDFnode* current = (CDFnode*)nclistget(path,i);
	OCdatanode nextdata = NULL;
	CDFnode* next = NULL;

	/* invariant: current = ith node in path; data = corresponding
           datanode
        */

	/* get next node in next and next instance in nextdata */
	if(current->nctype == NC_Structure
	   || current->nctype == NC_Dataset) {
	    if(nclistlength(current->array.dimset0) > 0) {
		/* Cannot handle this case */
		ncstat = THROW(NC_EDDS);
		goto done;
	    }
	    /* get next node in path; structure/dataset => exists */
	    next = (CDFnode*)nclistget(path,i+1);
	    size_t index = fieldindex(current,next);
            /* Move to appropriate field */
	    ocstat = oc_data_ithfield(conn,data,index,&nextdata);
	    if(ocstat) goto done;
	    oc_data_free(conn,data);
	    data = nextdata; /* set up for next loop iteration */
	} else if(current->nctype ==  NC_Sequence) {
	    /* Check for nested Sequences */
	    if(current != xseq) {
		/* Cannot handle this case */
		ncstat = THROW(NC_EDDS);
		goto done;
	    }
	    /* Get the record count */
	    ocstat = oc_data_recordcount(conn,data,&recordcount);
    	    if(sizep) *sizep = recordcount;
	    oc_data_free(conn,data); /* reclaim */
	    break; /* leave the loop */
	} else {
	    PANIC("unexpected mode");
	    return NC_EINVAL;
        }
    }

done:
    nclistfree(path);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return THROW(ncstat);
}

static NCerror
freeNCDAPCOMMON(NCDAPCOMMON* dapcomm)
{
    if(dapcomm == NULL) return NC_NOERR;
    freenccache(dapcomm,dapcomm->cdf.cache);
    nclistfree(dapcomm->cdf.projectedvars);
    nullfree(dapcomm->cdf.recorddimname);

    /* free the trees */
    freecdfroot(dapcomm->cdf.ddsroot);
    dapcomm->cdf.ddsroot = NULL;
    freecdfroot(dapcomm->cdf.fullddsroot);
    dapcomm->cdf.fullddsroot = NULL;
    if(dapcomm->oc.ocdasroot != NULL)
        oc_root_free(dapcomm->oc.conn,dapcomm->oc.ocdasroot);
    dapcomm->oc.ocdasroot = NULL;
    oc_close(dapcomm->oc.conn); /* also reclaims remaining OC trees */
    ncurifree(dapcomm->oc.url);
    nullfree(dapcomm->oc.urltext);
    nullfree(dapcomm->oc.rawurltext);

    dcefree((DCEnode*)dapcomm->oc.dapconstraint);
    dapcomm->oc.dapconstraint = NULL;

    /* Note that the ncio layer will figure out that the tmp file needs to be deleted,
       so we do not have to do it.
    */
    nullfree(dapcomm->substrate.filename); /* always reclaim */

    free(dapcomm);

    return NC_NOERR;
}

static size_t
fieldindex(CDFnode* parent, CDFnode* child)
{
    unsigned int i;
    for(i=0;i<nclistlength(parent->subnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(parent->subnodes,i);
	if(node == child) return i;
    }
    return -1;
}

/*
This is more complex than one might think. We want to find
a path to a variable inside the given node so that we can
ask for a single instance of that variable to minimize the
amount of data we retrieve. However, we want to avoid passing
through any nested sequence. This is possible because of the way
that sequencecheck() works.
TODO: some servers will not accept an unconstrained fetch, so
make sure we always have a constraint.
*/

static NCerror
computeseqcountconstraints(NCDAPCOMMON* dapcomm, CDFnode* seq, NCbytes* seqcountconstraints)
{
    size_t i,j;
    NClist* path = NULL;
    CDFnode* var = NULL;

    ASSERT(seq->nctype == NC_Sequence);
    computeseqcountconstraintsr(dapcomm,seq,&var);

    ASSERT((var != NULL));

    /* Compute var path */
    path = nclistnew();
    collectnodepath(var,path,WITHOUTDATASET);

    /* construct the projection path using minimal index values */
    for(i=0;i<nclistlength(path);i++) {
	CDFnode* node = (CDFnode*)nclistget(path,i);
	if(i > 0) ncbytescat(seqcountconstraints,".");
	ncbytescat(seqcountconstraints,node->ocname);
	if(node == seq) {
	    /* Use the limit */
	    if(node->sequencelimit > 0) {
		char tmp[64];
		snprintf(tmp,sizeof(tmp),"[0:%lu]",
		         (unsigned long)(node->sequencelimit - 1));
		ncbytescat(seqcountconstraints,tmp);
	    }
	} else if(nclistlength(node->array.dimset0) > 0) {
	    size_t ndims = nclistlength(node->array.dimset0);
	    for(j=0;j<ndims;j++) {
		CDFnode* dim = (CDFnode*)nclistget(node->array.dimset0,j);
		if(DIMFLAG(dim,CDFDIMSTRING)) {
		    ASSERT((j == (ndims - 1)));
		    break;
		}
		ncbytescat(seqcountconstraints,"[0]");
	    }
	}
    }
    /* Finally, add in any selection from the original URL */
    if(dap_getselection(dapcomm->oc.url) != NULL)
        ncbytescat(seqcountconstraints,dap_getselection(dapcomm->oc.url));
    nclistfree(path);
    return NC_NOERR;
}


/* Given an existing candidate, see if we prefer newchoice */
static CDFnode*
prefer(CDFnode* candidate, CDFnode* newchoice)
{
    nc_type newtyp;
    nc_type cantyp;
    int newisstring;
    int canisstring;
    int newisscalar;
    int canisscalar;

    /* always choose !null over null */
    if(newchoice == NULL)
	return candidate;
    if(candidate == NULL)
	return newchoice;

    newtyp = newchoice->etype;
    cantyp = candidate->etype;
    newisstring = (newtyp == NC_STRING || newtyp == NC_URL);
    canisstring = (cantyp == NC_STRING || cantyp == NC_URL);
    newisscalar = (nclistlength(newchoice->array.dimset0) == 0);
    canisscalar = (nclistlength(candidate->array.dimset0) == 0);

    ASSERT(candidate->nctype == NC_Atomic && newchoice->nctype == NC_Atomic);

    /* choose non-string over string */
    if(canisstring && !newisstring)
	return newchoice;
    if(!canisstring && newisstring)
	return candidate;

    /* choose scalar over array */
    if(canisscalar && !newisscalar)
	return candidate;
    if(!canisscalar && newisscalar)
	return candidate;

    /* otherwise choose existing candidate */
    return candidate;
}

/* computeseqcountconstraints recursive helper function */
static void
computeseqcountconstraintsr(NCDAPCOMMON* dapcomm, CDFnode* node, CDFnode** candidatep)
{
    CDFnode* candidate;
    CDFnode* compound;
    unsigned int i;

    candidate = NULL;
    compound = NULL;
    if(node == NULL)
      return;
    for(i=0;i<nclistlength(node->subnodes);i++) {
        CDFnode* subnode = (CDFnode*)nclistget(node->subnodes,i);
        if(subnode->nctype == NC_Structure || subnode->nctype == NC_Grid)
	    compound = subnode; /* save for later recursion */
	else if(subnode->nctype == NC_Atomic)
	    candidate = prefer(candidate,subnode);
    }
    if(candidate == NULL && compound == NULL) {
	PANIC("cannot find candidate for seqcountconstraints for a sequence");
    } else if(candidate != NULL && candidatep != NULL) {
	*candidatep = candidate;
    } else { /* compound != NULL by construction */
	/* recurse on a nested grids or structures */
        computeseqcountconstraintsr(dapcomm,compound,candidatep);
    }
}


static unsigned long
cdftotalsize(NClist* dimensions)
{
    unsigned int i;
    unsigned long total = 1;
    if(dimensions != NULL) {
	for(i=0;i<nclistlength(dimensions);i++) {
	    CDFnode* dim = (CDFnode*)nclistget(dimensions,i);
	    total *= dim->dim.declsize;
	}
    }
    return total;
}

/* Estimate variables sizes and then resort the variable list
   by that size
*/
static void
estimatevarsizes(NCDAPCOMMON* dapcomm)
{
    size_t ivar;
    size_t rank;
    size_t totalsize = 0;

    for(ivar=0;ivar<nclistlength(dapcomm->cdf.ddsroot->tree->varnodes);ivar++) {
        CDFnode* var = (CDFnode*)nclistget(dapcomm->cdf.ddsroot->tree->varnodes,ivar);
	NClist* ncdims = var->array.dimset0;
	rank = nclistlength(ncdims);
	if(rank == 0) { /* use instance size of the type */
	    var->estimatedsize = nctypesizeof(var->etype);
#ifdef DEBUG1
fprintf(stderr,"scalar %s.estimatedsize = %lu\n",
	makecdfpathstring(var,"."),var->estimatedsize);
#endif
	} else {
	    unsigned long size = cdftotalsize(ncdims);
	    size *= nctypesizeof(var->etype);
#ifdef DEBUG1
fprintf(stderr,"array %s(%u).estimatedsize = %lu\n",
	makecdfpathstring(var,"."),rank,size);
#endif
	    var->estimatedsize = size;
	}
	totalsize += var->estimatedsize;
    }
#ifdef DEBUG1
fprintf(stderr,"total estimatedsize = %lu\n",totalsize);
#endif
    dapcomm->cdf.totalestimatedsize = totalsize;
}

static NCerror
fetchpatternmetadata(NCDAPCOMMON* dapcomm)
{
    NCerror ncstat = NC_NOERR;
    OCerror ocstat = OC_NOERR;
    OCddsnode ocroot = NULL;
    CDFnode* ddsroot = NULL;
    char* ce = NULL;

    /* Temporary hack: we need to get the selection string
       from the url
    */
    /* Get (almost) unconstrained DDS; In order to handle functions
       correctly, those selections must always be included
    */
    if(FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE))
	ce = NULL;
    else
        ce = nulldup(dap_getselection(dapcomm->oc.url));

    /* Get selection constrained DDS */
    ncstat = dap_fetch(dapcomm,dapcomm->oc.conn,ce,OCDDS,&ocroot);
    if(ncstat != NC_NOERR) {
	/* Special Hack. If the protocol is file, then see if
           we can get the dds from the .dods file
        */
	if(strcmp(dapcomm->oc.url->protocol,"file") != 0) {
	    THROWCHK(ocstat); goto done;
	}
	/* Fetch the data dds */
        ncstat = dap_fetch(dapcomm,dapcomm->oc.conn,ce,OCDATADDS,&ocroot);
        if(ncstat != NC_NOERR) {
	    THROWCHK(ncstat); goto done;
	}
	/* Note what we did */
	nclog(NCLOGWARN,"Cannot locate .dds file, using .dods file");
    }

    /* Get selection constrained DAS */
    ncstat = dap_fetch(dapcomm,dapcomm->oc.conn,ce,OCDAS,&dapcomm->oc.ocdasroot);
    if(ncstat != NC_NOERR) {
	/* Ignore but complain */
	nclog(NCLOGWARN,"Could not read DAS; ignored");
        dapcomm->oc.ocdasroot = NULL;
	ncstat = NC_NOERR;
    }

    /* Construct the netcdf cdf tree corresponding to the dds tree*/
    ncstat = buildcdftree(dapcomm,ocroot,OCDDS,&ddsroot);
    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
    dapcomm->cdf.fullddsroot = ddsroot;
    ddsroot = NULL; /* avoid double reclaim */

    /* Combine DDS and DAS */
    if(dapcomm->oc.ocdasroot != NULL) {
	ncstat = dapmerge(dapcomm,dapcomm->cdf.fullddsroot,
                           dapcomm->oc.ocdasroot);
        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
    }

#ifdef DEBUG2
fprintf(stderr,"full pattern:\n%s",dumptree(dapcomm->cdf.fullddsroot));
#endif

done:
    nullfree(ce);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return ncstat;
}

static NCerror
fetchconstrainedmetadata(NCDAPCOMMON* dapcomm)
{
    NCerror ncstat = NC_NOERR;
    OCerror ocstat = OC_NOERR;
    OCddsnode ocroot;
    CDFnode* ddsroot; /* constrained */
    char* ce = NULL;

    if(FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE))
	ce = NULL;
    else
        ce = dcebuildconstraintstring(dapcomm->oc.dapconstraint);
    {
        ncstat = dap_fetch(dapcomm,dapcomm->oc.conn,ce,OCDDS,&ocroot);
        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}

        /* Construct our parallel dds tree; including attributes*/
        ncstat = buildcdftree(dapcomm,ocroot,OCDDS,&ddsroot);
        if(ncstat) goto fail;
	ocroot = NULL; /* avoid duplicate reclaim */

	dapcomm->cdf.ddsroot = ddsroot;
	ddsroot = NULL; /* to avoid double reclamation */

        if(!FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE)) {
            /* fix DAP server problem by adding back any inserting needed structure nodes */
            ncstat = restruct(dapcomm, dapcomm->cdf.ddsroot,dapcomm->cdf.fullddsroot,dapcomm->oc.dapconstraint->projections);
            if(ncstat) goto fail;
	}

#ifdef DEBUG2
fprintf(stderr,"constrained:\n%s",dumptree(dapcomm->cdf.ddsroot));
#endif

        /* Combine DDS and DAS */
	if(dapcomm->oc.ocdasroot != NULL) {
            ncstat = dapmerge(dapcomm,dapcomm->cdf.ddsroot,
                               dapcomm->oc.ocdasroot);
            if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}
	}
        /* map the constrained DDS to the unconstrained DDS */
        ncstat = mapnodes(dapcomm->cdf.ddsroot,dapcomm->cdf.fullddsroot);
        if(ncstat) goto fail;

    }

fail:
    nullfree(ce);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return ncstat;
}

/* Suppress variables not in usable sequences*/
static NCerror
suppressunusablevars(NCDAPCOMMON* dapcomm)
{
    size_t i;
    int found = 1;
    NClist* path = nclistnew();

    while(found) {
	found = 0;
	/* Walk backwards to aid removal semantics */
	for(i = nclistlength(dapcomm->cdf.ddsroot->tree->varnodes); i-->0;) {
	    CDFnode* var = (CDFnode*)nclistget(dapcomm->cdf.ddsroot->tree->varnodes,i);
	    /* See if this var is under an unusable sequence */
	    nclistclear(path);
	    collectnodepath(var,path,WITHOUTDATASET);
	    for(size_t j=0;j<nclistlength(path);j++) {
		CDFnode* node = (CDFnode*)nclistget(path,j);
		if(node->nctype == NC_Sequence
		   && !node->usesequence) {
#ifdef DEBUG
fprintf(stderr,"suppressing var in unusable sequence: %s.%s\n",node->ncfullname,var->ncbasename);
#endif
		    found = 1;
		    break;
		}
	    }
	    if(found) break;
	}
        if(found) nclistremove(dapcomm->cdf.ddsroot->tree->varnodes,i);
    }
    nclistfree(path);
    return NC_NOERR;
}


/*
For variables which have a zero size dimension,
make them invisible.
*/
static NCerror
fixzerodims(NCDAPCOMMON* dapcomm)
{
    size_t i,j;
    for(i=0;i<nclistlength(dapcomm->cdf.ddsroot->tree->varnodes);i++) {
	CDFnode* var = (CDFnode*)nclistget(dapcomm->cdf.ddsroot->tree->varnodes,i);
        NClist* ncdims = var->array.dimsetplus;
	if(nclistlength(ncdims) == 0) continue;
        for(j=0;j<nclistlength(ncdims);j++) {
	    CDFnode* dim = (CDFnode*)nclistget(ncdims,j);
	    if(dim->dim.declsize == 0) {
	 	/* make node invisible */
		var->invisible = 1;
		var->zerodim = 1;
	    }
	}
    }
    return NC_NOERR;
}

static void
applyclientparamcontrols(NCDAPCOMMON* dapcomm)
{
    const char* value = NULL;

    /* clear the flags */
    CLRFLAG(dapcomm->controls,NCF_CACHE);
    CLRFLAG(dapcomm->controls,NCF_SHOWFETCH);
    CLRFLAG(dapcomm->controls,NCF_NC3);
    CLRFLAG(dapcomm->controls,NCF_NCDAP);
    CLRFLAG(dapcomm->controls,NCF_PREFETCH);
    CLRFLAG(dapcomm->controls,NCF_PREFETCH_EAGER);
    CLRFLAG(dapcomm->controls,NCF_ENCODE_PATH);
    CLRFLAG(dapcomm->controls,NCF_ENCODE_QUERY);

    /* Turn on any default on flags */
    SETFLAG(dapcomm->controls,DFALT_ON_FLAGS);
    SETFLAG(dapcomm->controls,(NCF_NC3|NCF_NCDAP));

    /* enable/disable caching */
    if(dapparamcheck(dapcomm,"cache",NULL))
	SETFLAG(dapcomm->controls,NCF_CACHE);
    else if(dapparamcheck(dapcomm,"nocache",NULL))
	CLRFLAG(dapcomm->controls,NCF_CACHE);

    /* enable/disable cache prefetch and lazy vs eager*/
    if(dapparamcheck(dapcomm,"prefetch","eager")) {
        SETFLAG(dapcomm->controls,NCF_PREFETCH);
        SETFLAG(dapcomm->controls,NCF_PREFETCH_EAGER);
    } else if(dapparamcheck(dapcomm,"prefetch","lazy")
              || dapparamcheck(dapcomm,"prefetch",NULL)) {
        SETFLAG(dapcomm->controls,NCF_PREFETCH);
        CLRFLAG(dapcomm->controls,NCF_PREFETCH_EAGER);
    } else if(dapparamcheck(dapcomm,"noprefetch",NULL))
        CLRFLAG(dapcomm->controls,NCF_PREFETCH);

    if(FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE))
	SETFLAG(dapcomm->controls,NCF_CACHE);

    if(dapparamcheck(dapcomm,"show","fetch"))
	SETFLAG(dapcomm->controls,NCF_SHOWFETCH);

    /* enable/disable _FillValue/Variable Mismatch */
    if(dapparamcheck(dapcomm,"fillmismatch",NULL))
	SETFLAG(dapcomm->controls,NCF_FILLMISMATCH);
    else if(dapparamcheck(dapcomm,"nofillmismatch",NULL))
	CLRFLAG(dapcomm->controls,NCF_FILLMISMATCH);

    if((value=dapparamvalue(dapcomm,"encode")) != NULL) {
	size_t i;
	NClist* encode = nclistnew();
	if(dapparamparselist(value,',',encode)) 
            nclog(NCLOGERR,"Malformed encode parameter: %s",value);
	else {
	    /* First, turn off all the encode flags */
            CLRFLAG(dapcomm->controls,NCF_ENCODE_PATH|NCF_ENCODE_QUERY);
	    for(i=0;i<nclistlength(encode);i++) {
	        const char* s = nclistget(encode,i);
	        if(strcmp(s,"path")==0)
	            SETFLAG(dapcomm->controls,NCF_ENCODE_PATH);
	        else if(strcmp(s,"query")==0)
	            SETFLAG(dapcomm->controls,NCF_ENCODE_QUERY);
	        else if(strcmp(s,"all")==0)
	            SETFLAG(dapcomm->controls,NCF_ENCODE_PATH|NCF_ENCODE_QUERY);
	        else if(strcmp(s,"none")==0)
	            CLRFLAG(dapcomm->controls,NCF_ENCODE_PATH|NCF_ENCODE_QUERY);
	    }
            nclistfreeall(encode);
	}
    } else { /* Set defaults */
	SETFLAG(dapcomm->controls,NCF_ENCODE_QUERY);
    }
    nclog(NCLOGNOTE,"Caching=%d",FLAGSET(dapcomm->controls,NCF_CACHE));
}

/*
Force dap2 access to be read-only
*/
int
NCD2_set_fill(int ncid, int fillmode, int* old_modep)
{
    return THROW(NC_EPERM);
}

int
NCD2_def_dim(int ncid, const char* name, size_t len, int* idp)
{
    return THROW(NC_EPERM);
}

int
NCD2_put_att(int ncid, int varid, const char* name, nc_type datatype,
	   size_t len, const void* value, nc_type t)
{
    return THROW(NC_EPERM);
}

int
NCD2_def_var(int ncid, const char *name,
  	     nc_type xtype, int ndims, const int *dimidsp, int *varidp)
{
    return THROW(NC_EPERM);
}

/*
Following functions basically return the netcdf-3 value WRT to the nc3id.
*/

int
NCD2_inq_format(int ncid, int* formatp)
{
    NC* drno;
    int ret = NC_NOERR;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_format(getnc3id(drno), formatp);
    return THROW(ret);
}

int
NCD2_inq(int ncid, int* ndimsp, int* nvarsp, int* nattsp, int* unlimdimidp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq(getnc3id(drno), ndimsp, nvarsp, nattsp, unlimdimidp);
    return THROW(ret);
}

int
NCD2_inq_type(int ncid, nc_type p2, char* p3, size_t* p4)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_type(getnc3id(drno), p2, p3, p4);
    return THROW(ret);
}

int
NCD2_inq_dimid(int ncid, const char* name, int* idp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_dimid(getnc3id(drno), name, idp);
    return THROW(ret);
}

int
NCD2_inq_dim(int ncid, int dimid, char* name, size_t* lenp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_dim(getnc3id(drno), dimid, name, lenp);
    return THROW(ret);
}

int
NCD2_inq_unlimdim(int ncid, int* unlimdimidp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_unlimdim(getnc3id(drno), unlimdimidp);
    return THROW(ret);
}

int
NCD2_rename_dim(int ncid, int dimid, const char* name)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_rename_dim(getnc3id(drno), dimid, name);
    return THROW(ret);
}

int
NCD2_inq_att(int ncid, int varid, const char* name,
	    nc_type* xtypep, size_t* lenp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_att(getnc3id(drno), varid, name, xtypep, lenp);
    return THROW(ret);
}

int
NCD2_inq_attid(int ncid, int varid, const char *name, int *idp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_attid(getnc3id(drno), varid, name, idp);
    return THROW(ret);
}

int
NCD2_inq_attname(int ncid, int varid, int attnum, char* name)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_attname(getnc3id(drno), varid, attnum, name);
    return THROW(ret);
}

int
NCD2_rename_att(int ncid, int varid, const char* name, const char* newname)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_rename_att(getnc3id(drno), varid, name, newname);
    return THROW(ret);
}

int
NCD2_del_att(int ncid, int varid, const char* p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_del_att(getnc3id(drno), varid, p3);
    return THROW(ret);
}

int
NCD2_get_att(int ncid, int varid, const char* name, void* value, nc_type t)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = NCDISPATCH_get_att(getnc3id(drno), varid, name, value, t);
    return THROW(ret);
}

int
NCD2_inq_var_all(int ncid, int varid, char *name, nc_type* xtypep,
               int* ndimsp, int* dimidsp, int* nattsp,
               int* shufflep, int* deflatep, int* deflate_levelp,
               int* fletcher32p, int* contiguousp, size_t* chunksizesp,
               int* no_fill, void* fill_valuep, int* endiannessp,
	       unsigned int* idp, size_t* nparamsp, unsigned int* params
               )
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = NCDISPATCH_inq_var_all(getnc3id(drno), varid, name, xtypep,
               ndimsp, dimidsp, nattsp,
               shufflep, deflatep, deflate_levelp,
               fletcher32p, contiguousp, chunksizesp,
               no_fill, fill_valuep, endiannessp,
	       idp,nparamsp,params
	       );
    return THROW(ret);
}

int
NCD2_inq_varid(int ncid, const char *name, int *varidp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_varid(getnc3id(drno),name,varidp);
    return THROW(ret);
}

int
NCD2_rename_var(int ncid, int varid, const char* name)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_rename_var(getnc3id(drno), varid, name);
    return THROW(ret);
}

int
NCD2_var_par_access(int ncid, int p2, int p3)
{
    return THROW(NC_ENOPAR);
}

int
NCD2_def_var_fill(int ncid, int p2, int p3, const void* p4)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_var_fill(getnc3id(drno), p2, p3, p4);
    return THROW(ret);
}

int
NCD2_inq_ncid(int ncid, const char* name, int* grp_ncid)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_ncid(getnc3id(drno), name, grp_ncid);
    return THROW(ret);
}

int
NCD2_show_metadata(int ncid)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_show_metadata(getnc3id(drno));
    return THROW(ret);
}

int
NCD2_inq_grps(int ncid, int* p2, int* p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_grps(getnc3id(drno), p2, p3);
    return THROW(ret);
}

int
NCD2_inq_grpname(int ncid, char* p)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_grpname(getnc3id(drno), p);
    return THROW(ret);
}


int
NCD2_inq_unlimdims(int ncid, int* p2, int* p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_unlimdims(getnc3id(drno), p2, p3);
    return THROW(ret);
}

int
NCD2_inq_grpname_full(int ncid, size_t* p2, char* p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_grpname_full(getnc3id(drno), p2, p3);
    return THROW(ret);
}

int
NCD2_inq_grp_parent(int ncid, int* p)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_grp_parent(getnc3id(drno), p);
    return THROW(ret);
}

int
NCD2_inq_grp_full_ncid(int ncid, const char* p2, int* p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_grp_full_ncid(getnc3id(drno), p2, p3);
    return THROW(ret);
}

int
NCD2_inq_varids(int ncid, int* nvars, int* p)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_varids(getnc3id(drno), nvars, p);
    return THROW(ret);
}

int
NCD2_inq_dimids(int ncid, int* ndims, int* p3, int p4)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_dimids(getnc3id(drno), ndims, p3, p4);
    return THROW(ret);
}

int
NCD2_inq_typeids(int ncid, int*  ntypes, int* p)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_typeids(getnc3id(drno), ntypes, p);
    return THROW(ret);
}

int
NCD2_inq_type_equal(int ncid, nc_type t1, int p3, nc_type t2, int* p5)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_type_equal(getnc3id(drno), t1, p3, t2, p5);
    return THROW(ret);
}

int
NCD2_inq_user_type(int ncid, nc_type t, char* p3, size_t* p4, nc_type* p5,
                   size_t* p6, int* p7)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_user_type(getnc3id(drno), t, p3, p4, p5, p6, p7);
    return THROW(ret);
}

int
NCD2_inq_typeid(int ncid, const char* name, nc_type* t)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_typeid(getnc3id(drno), name, t);
    return THROW(ret);
}

int
NCD2_def_grp(int ncid, const char* p2, int* p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_grp(getnc3id(drno), p2, p3);
    return THROW(ret);
}

int
NCD2_rename_grp(int ncid, const char* p)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_rename_grp(getnc3id(drno), p);
    return THROW(ret);
}

int
NCD2_def_compound(int ncid, size_t p2, const char* p3, nc_type* t)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_compound(getnc3id(drno), p2, p3, t);
    return THROW(ret);
}

int
NCD2_insert_compound(int ncid, nc_type t1, const char* p3, size_t p4, nc_type t2)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_insert_compound(getnc3id(drno), t1, p3, p4, t2);
    return THROW(ret);
}

int
NCD2_insert_array_compound(int ncid, nc_type t1, const char* p3, size_t p4,
			  nc_type t2, int p6, const int* p7)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_insert_array_compound(getnc3id(drno), t1, p3, p4,  t2, p6, p7);
    return THROW(ret);
}

int
NCD2_inq_compound_field(int ncid, nc_type xtype, int fieldid, char *name,
		      size_t *offsetp, nc_type* field_typeidp, int *ndimsp,
		      int *dim_sizesp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_compound_field(getnc3id(drno), xtype, fieldid, name, offsetp, field_typeidp, ndimsp, dim_sizesp);
    return THROW(ret);
}

int
NCD2_inq_compound_fieldindex(int ncid, nc_type xtype, const char *name,
			   int *fieldidp)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_compound_fieldindex(getnc3id(drno), xtype, name, fieldidp);
    return THROW(ret);
}

int
NCD2_def_vlen(int ncid, const char* p2, nc_type base_typeid, nc_type* t)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_vlen(getnc3id(drno), p2, base_typeid, t);
    return THROW(ret);
}

int
NCD2_put_vlen_element(int ncid, int p2, void* p3, size_t p4, const void* p5)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_put_vlen_element(getnc3id(drno), p2, p3, p4, p5);
    return THROW(ret);
}

int
NCD2_get_vlen_element(int ncid, int p2, const void* p3, size_t* p4, void* p5)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_get_vlen_element(getnc3id(drno), p2, p3, p4, p5);
    return THROW(ret);
}

int
NCD2_def_enum(int ncid, nc_type t1, const char* p3, nc_type* t)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_enum(getnc3id(drno), t1, p3, t);
    return THROW(ret);
}

int
NCD2_insert_enum(int ncid, nc_type t1, const char* p3, const void* p4)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_insert_enum(getnc3id(drno), t1, p3, p4);
    return THROW(ret);
}

int
NCD2_inq_enum_member(int ncid, nc_type t1, int p3, char* p4, void* p5)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_enum_member(getnc3id(drno), t1, p3, p4, p5);
    return THROW(ret);
}

int
NCD2_inq_enum_ident(int ncid, nc_type t1, long long p3, char* p4)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_inq_enum_ident(getnc3id(drno), t1, p3, p4);
    return THROW(ret);
}

int
NCD2_def_opaque(int ncid, size_t p2, const char* p3, nc_type* t)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_opaque(getnc3id(drno), p2, p3, t);
    return THROW(ret);
}

int
NCD2_def_var_deflate(int ncid, int p2, int p3, int p4, int p5)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_var_deflate(getnc3id(drno), p2, p3, p4, p5);
    return THROW(ret);
}

int
NCD2_def_var_fletcher32(int ncid, int p2, int p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_var_fletcher32(getnc3id(drno), p2, p3);
    return THROW(ret);
}

int
NCD2_def_var_chunking(int ncid, int p2, int p3, const size_t* p4)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_var_chunking(getnc3id(drno), p2, p3, p4);
    return THROW(ret);
}

int
NCD2_def_var_endian(int ncid, int p2, int p3)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_var_endian(getnc3id(drno), p2, p3);
    return THROW(ret);
}

int
NCD2_def_var_filter(int ncid, int varid, unsigned int id, size_t n, const unsigned int* params)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_def_var_filter(getnc3id(drno), varid, id, n, params);
    return THROW(ret);
}

int
NCD2_set_var_chunk_cache(int ncid, int p2, size_t p3, size_t p4, float p5)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_set_var_chunk_cache(getnc3id(drno), p2, p3, p4, p5);
    return THROW(ret);
}

int
NCD2_get_var_chunk_cache(int ncid, int p2, size_t* p3, size_t* p4, float* p5)
{
    NC* drno;
    int ret;
    if((ret = NC_check_id(ncid, (NC**)&drno)) != NC_NOERR) return THROW(ret);
    ret = nc_get_var_chunk_cache(getnc3id(drno), p2, p3, p4, p5);
    return THROW(ret);
}

/* Get substrate NC* object.
   This function breaks the abstraction, but is necessary for
   code that accesses the underlying netcdf-4 metadata objects.
   Used in: NC_reclaim_data[_all]
            NC_copy_data[_all]
*/

NC*
NCD2_get_substrate(NC* nc)
{
    NC* subnc = NULL;
    /* Iff nc->dispatch is the DAP2 dispatcher, then do a level of indirection */
    if(USED2INFO(nc)) {
        NCDAPCOMMON* d2 = (NCDAPCOMMON*)nc->dispatchdata;
        /* Find pointer to NC struct for this file. */
        (void)NC_check_id(d2->substrate.nc3id,&subnc);
    } else subnc = nc;
    return subnc;
}

