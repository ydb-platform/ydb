/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "config.h"
#include "ncdispatch.h"
#include "ncd4dispatch.h"
#include "nc4internal.h"
#include "d4includes.h"
#include "d4odom.h"
#include <stddef.h>

/* Forward */
static int getvarx(int gid, int varid, NCD4INFO**, NCD4node** varp, nc_type* xtypep, size_t*, nc_type* nc4typep, size_t*);
static int mapvars(NCD4meta* dapmeta, NCD4meta* dmrmeta, int inferredchecksumming);

int
NCD4_get_vara(int ncid, int varid,
	    const size_t *start, const size_t *edges,
            void *value,
	    nc_type memtype)
{
    int ret;
    /* TODO: optimize since we know stride is 1 */
    ret = NCD4_get_vars(ncid,varid,start,edges,NC_stride_one,value,memtype);
    return THROW(ret);
}

int
NCD4_get_vars(int gid, int varid,
	    const size_t *start, const size_t *edges, const ptrdiff_t* stride,
            void *memoryin, nc_type xtype)
{
    size_t i;
    int ret;
    NCD4INFO* info;
    NCD4meta* meta;
    NCD4node* ncvar;
    NCD4node* nctype;
    D4odometer* odom = NULL;
    nc_type nc4type;
    size_t nc4size, xsize, dapsize;
    void* instance = NULL; /* Staging area in case we have to convert */
    NClist* blobs = NULL;
    size_t rank;
    size_t dimsizes[NC_MAX_VAR_DIMS];
    d4size_t dimproduct;
    size_t dstpos;
    NCD4offset* offset = NULL;
    
    /* Get netcdf var metadata and data */
    if((ret=getvarx(gid, varid, &info, &ncvar, &xtype, &xsize, &nc4type, &nc4size)))
	{goto done;}

    meta = info->dmrmetadata;
    nctype = ncvar->basetype;
    rank = nclistlength(ncvar->dims);
    blobs = nclistnew();

    /* Get the type's dapsize */
    dapsize = nctype->meta.dapsize;

    instance = malloc(nc4size);
    if(instance == NULL)
	{ret = THROW(NC_ENOMEM); goto done;}	

    dimproduct = NCD4_dimproduct(ncvar);
    /* build size vector */
    for(i=0;i<rank;i++)  {
	NCD4node* dim = nclistget(ncvar->dims,i);
	dimsizes[i] = dim->dim.size;
    }
	
    /* Extract and desired subset of data */
    if(rank > 0)
        odom = d4odom_new(rank,start,edges,stride,dimsizes);
    else
        odom = d4scalarodom_new();
    dstpos = 0; /* We always write into dst starting at position 0*/
    for(;d4odom_more(odom);dstpos++) {
	void* xpos;
	void* dst;
	d4size_t pos;

        pos = d4odom_next(odom);
	if(pos >= dimproduct) {
	    ret = THROW(NC_EINVALCOORDS);
	    goto done;
	}
        xpos = ((char*)memoryin)+(xsize * dstpos); /* ultimate destination */
	/* We need to compute the offset in the dap4 data of this instance;
	   for fixed size types, this is easy, otherwise we have to walk
	   the variable size type
	*/
	/* Allocate the offset object */
        if(offset) free(offset); /* Reclaim last loop */
	offset = NULL;
	offset = BUILDOFFSET(NULL,0);
        BLOB2OFFSET(offset,ncvar->data.dap4data);
	/* Move offset to the pos'th element of the array */
	if(nctype->meta.isfixedsize) {
	    INCR(offset,(dapsize*pos));
	} else {
	    /* We have to walk to the pos'th location in the data */
	    if((ret=NCD4_moveto(meta,ncvar,pos,offset)))
	        {goto done;}		    
	}
	dst = instance;
	if((ret=NCD4_movetoinstance(meta,nctype,offset,&dst,blobs)))
	    {goto done;}
	if(xtype == nc4type) {
	    /* We can just copy out the data */
	    memcpy(xpos,instance,nc4size);
	} else { /* Need to convert */
	    if((ret=NCD4_convert(nc4type,xtype,xpos,instance,1)))
	        {goto done;}
	}
    }

done:
    if(offset) free(offset); /* Reclaim last loop */
    /* cleanup */
    if(odom != NULL)
	d4odom_free(odom);
    if(instance != NULL)
	free(instance);
    if(ret != NC_NOERR) { /* reclaim all malloc'd data if there is an error*/
	for(i=0;i<nclistlength(blobs);i++) {
	    nullfree(nclistget(blobs,i));
	}
    }
    if(blobs) nclistfree(blobs);
    return (ret);
}

static int
getvarx(int gid, int varid, NCD4INFO** infop, NCD4node** varp,
	nc_type* xtypep, size_t* xsizep, nc_type* nc4typep, size_t* nc4sizep)
{
    int ret = NC_NOERR;
    NC* ncp = NULL;
    NCD4INFO* info = NULL;
    NCD4meta* dmrmeta = NULL;
    NCD4node* group = NULL;
    NCD4node* var = NULL;
    NCD4node* type = NULL;
    nc_type xtype, actualtype;
    size_t instancesize, xsize;
    NCURI* ceuri = NULL; /* Constrained uri */
    NCD4meta* dapmeta = NULL;
    NCD4response* dapresp = NULL;

    if((ret = NC_check_id(gid, (NC**)&ncp)) != NC_NOERR)
	goto done;

    info = getdap(ncp);
    dmrmeta = info->dmrmetadata;

    if((ret = NCD4_findvar(ncp,gid,varid,&var,&group))) goto done;
    type = var->basetype;
    actualtype = type->meta.id;
    instancesize = type->meta.memsize;

    /* Figure out the type conversion, if any */
    xtype = *xtypep;
    if(xtype == NC_NAT)
	xtype = actualtype;
    if(xtype != actualtype && xtype > NC_MAX_ATOMIC_TYPE)
	return THROW(NC_EBADTYPE);
    if((xtype == NC_CHAR || xtype == NC_STRING)
	&& (actualtype != NC_CHAR && actualtype != NC_STRING))
	return THROW(NC_ECHAR);
    if(xtype <= NC_MAX_ATOMIC_TYPE)
        xsize = NCD4_typesize(xtype);
    else
	xsize = instancesize;

    /* If we already have valid data, then just return */
    if(var->data.valid) goto validated;

    /* Ok, we need to read from the server */

    /* Add the variable to the URI, unless the URI is already constrained or is unconstrainable */
    ceuri = ncuriclone(info->dmruri);
    /* append the request for a specific variable */
    if(ncuriquerylookup(ceuri,DAP4CE) == NULL && !FLAGSET(info->controls.flags,NCF_UNCONSTRAINABLE)) {
	ncurisetquerykey(ceuri,strdup("dap4.ce"),NCD4_makeFQN(var));
    }

    /* Read and process the data */

    /* Setup the meta-data for the DAP */
    if((ret=NCD4_newMeta(info,&dapmeta))) goto done;
    if((ret=NCD4_newResponse(info,&dapresp))) goto done;
    dapresp->mode = NCD4_DAP;
    nclistpush(info->responses,dapresp);
    if((ret=NCD4_readDAP(info, ceuri, dapresp))) goto done;

    /* Extract DMR and dechunk the data part */
    if((ret=NCD4_dechunk(dapresp))) goto done;

    /* Process the dmr part */
    if((ret=NCD4_parse(dapmeta,dapresp,1))) goto done;

    /* See if we are checksumming */
    if((ret=NCD4_inferChecksums(dapmeta,dapresp))) goto done;

    /* connect variables and corresponding dap data */
    if((ret = NCD4_parcelvars(dapmeta,dapresp))) goto done;

    /* Process checksums and byte-order swapping */
    if((ret = NCD4_processdata(dapmeta,dapresp))) goto done;

    /* Transfer and process the data */
    if((ret = mapvars(dapmeta,dmrmeta,dapresp->inferredchecksumming))) goto done;

validated:
    /* Return relevant info */
    if(infop) *infop = info;
    if(xtypep) *xtypep = xtype;
    if(xsizep) *xsizep = xsize;
    if(nc4typep) *nc4typep = actualtype;
    if(nc4sizep) *nc4sizep = instancesize;
    if(varp) *varp = var;
done:
    if(dapmeta) NCD4_reclaimMeta(dapmeta);
    if(dapresp != NULL && dapresp->error.message != NULL)
    	NCD4_reporterror(dapresp,ceuri);    /* Make sure the user sees this */
    ncurifree(ceuri);
    return THROW(ret);    
}

#if 0
static NCD4node*
findbyname(const char* name, NClist* nodes)
{
    int i;
    for(i=0;i<nclistlength(nodes);i++) {
        NCD4node* node = (NCD4node*)nclistget(nodes,i);
        if(strcmp(name,node->name)==0)
	    return node;
    }
    return NULL;
}
#endif

static int
matchvar(NCD4meta* dmrmeta, NCD4node* dapvar, NCD4node** dmrvarp)
{
    size_t i;
    int ret = NC_NOERR;
    NCD4node* x = NULL;
    NClist* dappath = nclistnew();
    NClist* dmrpath = nclistnew(); /* compute path for this dmr var */
    int found = 0;
    NCD4node* match = NULL;
    
    /* Capture the dap path starting at root and ending at the dapvar (assumed to be topvar) */
    for(x=dapvar;x != NULL;x=x->container) nclistinsert(dappath,0,x);
    /* Iterate over all variable nodes to find matching one */
    for(i=0;i<nclistlength(dmrmeta->allnodes);i++) {
	NCD4node* node = (NCD4node*)nclistget(dmrmeta->allnodes,i);
	if(ISVAR(node->sort) && strcmp(node->name,dapvar->name)==0) { /* possible candidate */
	    size_t j;
    	    found = 0;
	    nclistclear(dmrpath);
	    for(x=node;x != NULL;x=x->container) nclistinsert(dmrpath,0,x);
	    if(nclistlength(dmrpath) == nclistlength(dappath)) { /* same length paths */
		/* compare paths: name and sort */
	        for(found=1,j=0;j<nclistlength(dmrpath);j++) {
		    NCD4node* pdmr = (NCD4node*)nclistget(dmrpath,j);
    		    NCD4node* pdap = (NCD4node*)nclistget(dappath,j);
	     	    if(pdmr->sort != pdap->sort || strcmp(pdmr->name,pdap->name) != 0)
		        {found = 0; break;}
		}
		if(found) {match = node; break;}
	    }
	}
    }
    if(!found) {ret = NC_EINVAL; goto done;}
    if(dmrvarp) *dmrvarp = match;

done:
    nclistfree(dappath);
    nclistfree(dmrpath);
    return THROW(ret);
}

/*
Map each toplevel dap var to the corresponding
toplevel dmr var and transfer necessary info;
*/

static int
mapvars(NCD4meta* dapmeta, NCD4meta* dmrmeta, int inferredchecksumming)
{
    size_t i;
    int ret = NC_NOERR;
    NCD4node* daproot = dapmeta->root;
    NClist* daptop = NULL; /* top variables in dap tree */

    /* Get top level variables from the dap node tree */
    daptop = nclistnew();
    NCD4_getToplevelVars(dapmeta,daproot,daptop);

    /* Match up the dap top variables with the dmr top variables */
    for(i=0;i<nclistlength(daptop);i++) {
	NCD4node* dapvar = (NCD4node*)nclistget(daptop,i);
	NCD4node* dmrvar = NULL;
	if((ret=matchvar(dmrmeta,dapvar,&dmrvar))) goto done;
        /* Transfer info from dap var to dmr var */
	dmrvar->data = dapvar->data;
        memset(&dapvar->data,0,sizeof(NCD4vardata));
	dmrvar->data.valid = 1;
    }

done:
    nclistfree(daptop);
    return THROW(ret);
}
