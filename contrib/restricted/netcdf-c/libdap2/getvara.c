/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribuution conditions.
 *********************************************************************/


#include "dapincludes.h"
#include "dapodom.h"
#include "dapdump.h"
#include "ncd2dispatch.h"
#include "ocx.h"
#include <stddef.h>

#define NEWVARM

/* Define a tracker for memory to support*/
/* the concatenation*/
struct NCMEMORY {
    void* memory;
    char* next; /* where to store the next chunk of data*/
};

/* Forward:*/
static NCerror moveto(NCDAPCOMMON*, Getvara*, CDFnode* dataroot, void* memory);
static NCerror movetor(NCDAPCOMMON*, OCdatanode currentcontent,
		   NClist* path, size_t depth,
		   Getvara*, size_t dimindex,
		   struct NCMEMORY*, NClist* segments);
static NCerror movetofield(NCDAPCOMMON*, OCdatanode,
			   NClist*, size_t depth,
		   	   Getvara*, size_t dimindex,
		   	   struct NCMEMORY*, NClist* segments);

static int findfield(CDFnode* node, CDFnode* subnode);
static NCerror removepseudodims(DCEprojection* proj);

static int extract(NCDAPCOMMON*, Getvara*, CDFnode*, DCEsegment*, size_t dimindex, OClink, OCdatanode, struct NCMEMORY*);
static int extractstring(NCDAPCOMMON*, Getvara*, CDFnode*, DCEsegment*, size_t dimindex, OClink, OCdatanode, struct NCMEMORY*);
static void freegetvara(Getvara* vara);
static NCerror makegetvar(NCDAPCOMMON*, CDFnode*, void*, nc_type, Getvara**);
static NCerror attachsubset(CDFnode* target, CDFnode* pattern);

/**************************************************/
/**
1. We build the projection to be sent to the server aka
the fetch constraint.  We want the server do do as much work
as possible, so we send it a url with a fetch constraint
that is the merge of the url constraint with the vara
constraint.

The url constraint, if any, is the one that was provided
in the url specified in nc_open().

The vara constraint is the one formed from the arguments
(start, count, stride) provided to the call to nc_get_vara().

There are some exceptions to the formation of the fetch constraint.
In all cases, the fetch constraint will use any URL selections,
but will use different fetch projections.
a. URL is unconstrainable (e.g. file://...):
	   fetchprojection = null => fetch whole dataset
b. The target variable (as specified in nc_get_vara())
   is already in the cache and is whole variable, but note
   that it might be part of the prefetch mixed in with other prefetched
   variables.
	   fetchprojection = N.A. since variable is in the cache
c. Vara is requesting part of a variable but NCF_WHOLEVAR flag is set.
	   fetchprojection = unsliced vara variable => fetch whole variable
d. Vara is requesting part of a variable and NCF_WHOLEVAR flag is not set.
	   fetchprojection = sliced vara variable => fetch part variable

2. At this point, all or part of the target variable is available in the cache.

3. We build a projection to walk (guide) the use of the oc
   data procedures to extract the required data from the cache.
   For cases a,b,c:
       walkprojection = merge(urlprojection,varaprojection)
   For case d:
       walkprojection =  varaprojection without slicing.
       This means we need only extract the complete contents of the cache.
       Notice that this will not necessarily be a direct memory to
       memory copy because the dap encoding still needs to be
       interpreted. For this case, we derive a walk projection
       from the vara projection that will properly access the cached data.
       This walk projection shifts the merged projection so all slices
       start at 0 and have a stride of 1.
*/

NCerror
nc3d_getvarx(int ncid, int varid,
	    const size_t *startp,
	    const size_t *countp,
	    const ptrdiff_t* stridep,
	    void *data,
	    nc_type dsttype0)
{
    NCerror ncstat = NC_NOERR;
    OCerror ocstat = OC_NOERR;
    size_t i;
    NC* drno;
    NC* substrate;
    NCDAPCOMMON* dapcomm;
    CDFnode* cdfvar = NULL; /* cdf node mapping to var*/
    NClist* varnodes;
    nc_type dsttype;
    Getvara* varainfo = NULL;
    CDFnode* xtarget = NULL; /* target in DATADDS */
    CDFnode* target = NULL; /* target in constrained DDS */
    DCEprojection* varaprojection = NULL;
    NCcachenode* cachenode = NULL;
    size_t localcount[NC_MAX_VAR_DIMS];
    NClist* ncdimsall;
    size_t ncrank;
    NClist* vars = NULL;
    DCEconstraint* fetchconstraint = NULL;
    DCEprojection* fetchprojection = NULL;
    DCEprojection* walkprojection = NULL;
    int state;
#define FETCHWHOLE 1 /* fetch whole data set */
#define FETCHVAR   2 /* fetch whole variable */
#define FETCHPART  4 /* fetch constrained variable */
#define CACHED     8 /* whole variable is already in the cache */

    ncstat = NC_check_id(ncid, (NC**)&drno);
    if(ncstat != NC_NOERR) goto fail;
    dapcomm = (NCDAPCOMMON*)drno->dispatchdata;

    ncstat = NC_check_id(getnc3id(drno), (NC**)&substrate);
    if(ncstat != NC_NOERR) goto fail;

    /* Locate var node via varid */
    varnodes = dapcomm->cdf.ddsroot->tree->varnodes;
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(varnodes,i);
	if(node->array.basevar == NULL
           && node->nctype == NC_Atomic
           && node->ncid == varid) {
	    cdfvar = node;
	    break;
	}
    }

    ASSERT((cdfvar != NULL));

    /* If the variable is prefetchable, then now
       is the time to do a lazy prefetch */
   if(FLAGSET(dapcomm->controls,NCF_PREFETCH)
      && !FLAGSET(dapcomm->controls,NCF_PREFETCH_EAGER)) {
        if(dapcomm->cdf.cache != NULL && dapcomm->cdf.cache->prefetch == NULL) {
            ncstat = prefetchdata(dapcomm);
            if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}
	}
    }

    /* Get the dimension info */
    ncdimsall = cdfvar->array.dimsetall;
    ncrank = nclistlength(ncdimsall);

#ifdef DEBUG
 {
int i;
fprintf(stderr,"getvarx: %s",cdfvar->ncfullname);
for(i=0;i<ncrank;i++)
  fprintf(stderr,"(%ld:%ld:%ld)",
	(long)startp[i],
	(long)countp[i],
	(long)stridep[i]
	);
fprintf(stderr,"\n");
 }
#endif

    /* Fill in missing arguments */
    if(startp == NULL)
	startp = NC_coord_zero;

    if(countp == NULL) {
        /* Accumulate the dimension sizes */
        for(i=0;i<ncrank;i++) {
	    CDFnode* dim = (CDFnode*)nclistget(ncdimsall,i);
	    localcount[i] = dim->dim.declsize;
	}
	countp = localcount;
    }

    if(stridep == NULL)
	stridep = NC_stride_one;

    /* Validate the dimension sizes */
    for(i=0;i<ncrank;i++) {
      CDFnode* dim = (CDFnode*)nclistget(ncdimsall,i);
      /* countp and startp are unsigned, so will never be < 0 */
      if(stridep[i] < 1) {
	ncstat = NC_EINVALCOORDS;
	goto fail;
      }
      if(startp[i] >= dim->dim.declsize
	 || startp[i]+((size_t)stridep[i]*(countp[i]-1)) >= dim->dim.declsize) {
	ncstat = NC_EINVALCOORDS;
	goto fail;
      }
    }

#ifdef DEBUG
 {
NClist* dims = cdfvar->array.dimsetall;
fprintf(stderr,"getvarx: %s",cdfvar->ncfullname);
if(nclistlength(dims) > 0) {int i;
for(i=0;i<nclistlength(dims);i++)
fprintf(stderr,"(%lu:%lu:%lu)",(unsigned long)startp[i],(unsigned long)countp[i],(unsigned long)stridep[i]);
fprintf(stderr," -> ");
for(i=0;i<nclistlength(dims);i++)
if(stridep[i]==1)
fprintf(stderr,"[%lu:%lu]",(unsigned long)startp[i],(unsigned long)((startp[i]+countp[i])-1));
else {
unsigned long iend = (stridep[i] * countp[i]);
iend = (iend + startp[i]);
iend = (iend - 1);
fprintf(stderr,"[%lu:%lu:%lu]",
(unsigned long)startp[i],(unsigned long)stridep[i],iend);
 }
 }
fprintf(stderr,"\n");
 }
#endif

    dsttype = (dsttype0);

    /* Default to using the inquiry type for this var*/
    if(dsttype == NC_NAT) dsttype = cdfvar->externaltype;

    /* Validate any implied type conversion*/
    if(cdfvar->etype != dsttype && dsttype == NC_CHAR) {
        /* The only disallowed conversion is to/from char and non-byte
           numeric types*/
	switch (cdfvar->etype) {
	case NC_STRING: case NC_URL:
	case NC_CHAR: case NC_BYTE: case NC_UBYTE:
	    break;
	default:
	    return THROW(NC_ECHAR);
	}
    }

    ncstat = makegetvar(dapcomm,cdfvar,data,dsttype,&varainfo);
    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}

    /* Compile the start/stop/stride info into a projection */
    ncstat = dapbuildvaraprojection(varainfo->target,
		                  startp,countp,stridep,
                                  &varaprojection);
    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}

    fetchprojection = NULL;
    walkprojection = NULL;

    /* Create walkprojection as the merge of the url projections
       and the vara projection; may change in FETCHPART case below*/
    ncstat = daprestrictprojection(dapcomm->oc.dapconstraint->projections,
				   varaprojection,&walkprojection);
    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}

#ifdef DEBUG
fprintf(stderr,"getvarx: walkprojection: |%s|\n",dumpprojection(walkprojection));
#endif

    /* define the var list of interest */
    vars = nclistnew();
    nclistpush(vars,(void*)varainfo->target);

    state = 0;
    if(iscached(dapcomm,cdfvar,&cachenode)) { /* ignores non-whole variable cache entries */
	state = CACHED;
	ASSERT((cachenode != NULL));
#ifdef DEBUG
fprintf(stderr,"var is in cache\n");
#endif
        /* If it is cached, then it is a whole variable but may still
           need to apply constraints during the walk */
	ASSERT(cachenode->wholevariable); /* by construction */
    } else if(FLAGSET(dapcomm->controls,NCF_UNCONSTRAINABLE)) {
	state = FETCHWHOLE;
    } else {/* load using constraints */
        if(FLAGSET(dapcomm->controls,NCF_WHOLEVAR))
	    state = FETCHVAR;
	else
	    state = FETCHPART;
    }
    ASSERT(state != 0);

    switch (state) {

    case FETCHWHOLE: {
        /* buildcachenode3 will create a new cachenode and
           will also fetch the whole corresponding datadds.
	*/
        /* Build the complete constraint to use in the fetch */
        fetchconstraint = (DCEconstraint*)dcecreate(CES_CONSTRAINT);
        /* Use no projections or selections */
        fetchconstraint->projections = nclistnew();
        fetchconstraint->selections = nclistnew();
#ifdef DEBUG
fprintf(stderr,"getvarx: FETCHWHOLE: fetchconstraint: %s\n",dumpconstraint(fetchconstraint));
#endif
        ncstat = buildcachenode(dapcomm,fetchconstraint,vars,&cachenode,0);
	fetchconstraint = NULL; /*buildcachenode34 takes control of fetchconstraint.*/
	if(ncstat != NC_NOERR) {THROWCHK(ncstat); nullfree(varainfo);
                                                varainfo=NULL;
                                                goto fail;}
    } break;

    case CACHED: {
    } break;

    case FETCHVAR: { /* Fetch a complete single variable */
        /* Create fetch projection as the merge of the url projections
           and the vara projection */
        ncstat = daprestrictprojection(dapcomm->oc.dapconstraint->projections,
				       varaprojection,&fetchprojection);
	/* elide any sequence and string dimensions (dap servers do not allow such). */
	ncstat = removepseudodims(fetchprojection);
        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}

	/* Convert to a whole variable projection */
	dcemakewholeprojection(fetchprojection);

#ifdef DEBUG
    fprintf(stderr,"getvarx: FETCHVAR: fetchprojection: |%s|\n",dumpprojection(fetchprojection));
#endif

        /* Build the complete constraint to use in the fetch */
        fetchconstraint = (DCEconstraint*)dcecreate(CES_CONSTRAINT);
        /* merged constraint just uses the url constraint selection */
        fetchconstraint->selections = dceclonelist(dapcomm->oc.dapconstraint->selections);
	/* and the created fetch projection */
        fetchconstraint->projections = nclistnew();
        nclistpush(fetchconstraint->projections,(void*)fetchprojection);
#ifdef DEBUG
fprintf(stderr,"getvarx: FETCHVAR: fetchconstraint: %s\n",dumpconstraint(fetchconstraint));
#endif
        /* buildcachenode3 will create a new cachenode and
           will also fetch the corresponding datadds.
        */
 ncstat = buildcachenode(dapcomm,fetchconstraint,vars,&cachenode,0);
        fetchconstraint = NULL; /*buildcachenode34 takes control of fetchconstraint.*/
	if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}
    } break;

    case FETCHPART: {
        /* Create fetch projection as the merge of the url projections
           and the vara projection */
        ncstat = daprestrictprojection(dapcomm->oc.dapconstraint->projections,
				       varaprojection,&fetchprojection);
	/* elide any sequence and string dimensions (dap servers do not allow such). */
	ncstat = removepseudodims(fetchprojection);
        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}

	/* Shift the varaprojection for simple walk */
	dcefree((DCEnode*)walkprojection) ; /* reclaim any existing walkprojection */
	walkprojection = (DCEprojection*)dceclone((DCEnode*)varaprojection);
        dapshiftprojection(walkprojection);

#ifdef DEBUG
        fprintf(stderr,"getvarx: FETCHPART: fetchprojection: |%s|\n",dumpprojection(fetchprojection));
#endif

        /* Build the complete constraint to use in the fetch */
        fetchconstraint = (DCEconstraint*)dcecreate(CES_CONSTRAINT);
        /* merged constraint just uses the url constraint selection */
        fetchconstraint->selections = dceclonelist(dapcomm->oc.dapconstraint->selections);
	/* and the created fetch projection */
        fetchconstraint->projections = nclistnew();
        nclistpush(fetchconstraint->projections,(void*)fetchprojection);
#ifdef DEBUG
        fprintf(stderr,"getvarx: FETCHPART: fetchconstraint: %s\n",dumpconstraint(fetchconstraint));
#endif
        /* buildcachenode3 will create a new cachenode and
           will also fetch the corresponding datadds.
        */
        ncstat = buildcachenode(dapcomm,fetchconstraint,vars,&cachenode,0);

	fetchconstraint = NULL; /*buildcachenode34 takes control of fetchconstraint.*/
	if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}
    } break;

    default: PANIC1("unknown fetch state: %d\n",state);
    }

    ASSERT(cachenode != NULL);

#ifdef DEBUG
fprintf(stderr,"cache.datadds=%s\n",dumptree(cachenode->datadds));
#endif

    /* attach DATADDS to (constrained) DDS */
    unattach(dapcomm->cdf.ddsroot);
    ncstat = attachsubset(cachenode->datadds,dapcomm->cdf.ddsroot);
    if(ncstat) goto fail;

    /* Fix up varainfo to use the cache */
    varainfo->cache = cachenode;
    cachenode = NULL;
    varainfo->varaprojection = walkprojection;
    walkprojection = NULL;

    /* Get the var correlate from the datadds */
    target = varainfo->target;
    xtarget = target->attachment;
    if(xtarget == NULL)
	{THROWCHK(ncstat=NC_ENODATA); goto fail;}

    /* Switch to datadds tree space*/
    varainfo->target = xtarget;
    ncstat = moveto(dapcomm,varainfo,varainfo->cache->datadds,data);
    if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto fail;}

fail:
    if(vars != NULL) nclistfree(vars);
    if(varaprojection != NULL) dcefree((DCEnode*)varaprojection);
    if(fetchconstraint != NULL) dcefree((DCEnode*)fetchconstraint);
    if(varainfo != NULL) freegetvara(varainfo);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return THROW(ncstat);
}

/* Remove any pseudodimensions (sequence and string)*/
static NCerror
removepseudodims(DCEprojection* proj)
{
    size_t i;
#ifdef DEBUG1
fprintf(stderr,"removesequencedims.before: %s\n",dumpprojection(proj));
#endif
    for(i=0;i<nclistlength(proj->var->segments);i++) {
	DCEsegment* seg = (DCEsegment*)nclistget(proj->var->segments,i);
	CDFnode* cdfnode = (CDFnode*)seg->annotation;
	if(cdfnode->array.seqdim != NULL)
	    seg->rank = 0;
	else if(cdfnode->array.stringdim != NULL)
	    seg->rank--;
    }
#ifdef DEBUG1
fprintf(stderr,"removepseudodims.after: %s\n",dumpprojection(proj));
#endif
    return NC_NOERR;
}

static NCerror
moveto(NCDAPCOMMON* nccomm, Getvara* xgetvar, CDFnode* xrootnode, void* memory)
{
    OCerror ocstat = OC_NOERR;
    NCerror ncstat = NC_NOERR;
    OClink conn = nccomm->oc.conn;
    OCdatanode xrootcontent;
    OCddsnode ocroot;
    NClist* path = nclistnew();
    struct NCMEMORY memstate;

    memstate.next = (memstate.memory = memory);

    /* Get the root content*/
    ocroot = xrootnode->tree->ocroot;
    ocstat = oc_data_getroot(conn,ocroot,&xrootcontent);
    if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}

    /* Remember: xgetvar->target is in DATADDS tree */
    collectnodepath(xgetvar->target,path,WITHDATASET);
    ncstat = movetor(nccomm,xrootcontent,
                     path,0,xgetvar,0,&memstate,
                     xgetvar->varaprojection->var->segments);
done:
    nclistfree(path);
    oc_data_free(conn,xrootcontent);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return THROW(ncstat);
}

static NCerror
movetor(NCDAPCOMMON* nccomm,
	OCdatanode currentcontent,
	NClist* path,
        size_t depth, /* depth is position in segment list*/
	Getvara* xgetvar,
        size_t dimindex, /* dimindex is position in xgetvar->slices*/
	struct NCMEMORY* memory,
	NClist* segments)
{
    OCerror ocstat = OC_NOERR;
    NCerror ncstat = NC_NOERR;
    OClink conn = nccomm->oc.conn;
    CDFnode* xnode = (CDFnode*)nclistget(path,depth);
    OCdatanode reccontent = NULL;
    OCdatanode dimcontent = NULL;
    Dapodometer* odom = NULL;
    int hasstringdim = 0;
    DCEsegment* segment;
    OCDT mode;

    /* Note that we use depth-1 because the path contains the DATASET
       but the segment list does not */
    segment = (DCEsegment*)nclistget(segments,depth-1); /*may be NULL*/
    if(xnode->etype == NC_STRING || xnode->etype == NC_URL) hasstringdim = 1;

    /* Get the mode */
    ocstat = oc_data_mode(conn,currentcontent,&mode);
    if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}

#ifdef DEBUG2
fprintf(stderr,"moveto: nctype=%d depth=%d dimindex=%d",
        xnode->nctype, (int)depth, (int)dimindex);
fprintf(stderr," segment=%s hasstringdim=%d\n",
		dcetostring((DCEnode*)segment),hasstringdim);
#endif

    switch (xnode->nctype) {

#if 0
#define OCDT_FIELD     ((OCDT)(1)) /* field of a container */
#define OCDT_ELEMENT   ((OCDT)(2)) /* element of a structure array */
#define OCDT_RECORD    ((OCDT)(4)) /* record of a sequence */
#define OCDT_ARRAY     ((OCDT)(8)) /* is structure array */
#define OCDT_SEQUENCE  ((OCDT)(16)) /* is sequence */
#define OCDT_ATOMIC    ((OCDT)(32)) /* is atomic leaf */
#endif

    default:
	goto done;

    case NC_Grid:
    case NC_Dataset:
    case NC_Structure:
	/* Separate out the case where structure is dimensioned */
	if(oc_data_indexable(conn,currentcontent)) {
	    /* => dimensioned structure */
            /* The current segment should reflect the
	       proper parts of the nc_get_vara argument
	    */
	    /* Create odometer for this structure's part of the projection */
            odom = dapodom_fromsegment(segment,0,segment->rank);
            while(dapodom_more(odom)) {
                /* Compute which instance to move to*/
                ocstat = oc_data_ithelement(conn,currentcontent,
                                            odom->index,&dimcontent);
                if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
                ASSERT(oc_data_indexed(conn,dimcontent));
                ncstat = movetor(nccomm,dimcontent,
                                 path,depth,/*keep same depth*/
                                 xgetvar,dimindex+segment->rank,
                                 memory,segments);
                dapodom_next(odom);
            }
            dapodom_free(odom);
	    odom = NULL;
	} else {/* scalar instance */
	    ncstat = movetofield(nccomm,currentcontent,path,depth,xgetvar,dimindex,memory,segments);
	    if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	} break;

    case NC_Sequence:
	if(fIsSet(mode,OCDT_SEQUENCE)) {
            ASSERT((xnode->attachment != NULL));
            ASSERT((segment != NULL));
	    ASSERT((segment->rank == 1));
	    /* Build an odometer for walking the sequence,
               however, watch out
               for the case when the user set a limit and that limit
               is not actually reached in this request.
            */
            /* By construction, this sequence represents the first
               (and only) dimension of this segment */
            odom = dapodom_fromsegment(segment,0,1);
	    while(dapodom_more(odom)) {
		size_t recordindex = dapodom_count(odom);
                ocstat = oc_data_ithrecord(conn,currentcontent,
					   recordindex,&reccontent);
                if(ocstat != OC_NOERR) {
		    if(ocstat == OC_EINDEX)
			ocstat = OC_EINVALCOORDS;
		    THROWCHK(ocstat); goto done;
		}
                ncstat = movetor(nccomm,reccontent,
                                     path,depth,
                                     xgetvar,dimindex+1,
                                     memory,segments);
                if(ncstat != OC_NOERR) {THROWCHK(ncstat); goto done;}
		dapodom_next(odom);
            }
	} else if(fIsSet(mode,OCDT_RECORD)) {
	    /* Treat like structure */
	    /* currentcontent points to the record instance */
	    ncstat = movetofield(nccomm,currentcontent,path,depth,xgetvar,dimindex,memory,segments);
	    if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	}
	break;

    case NC_Atomic:

        if(hasstringdim)
            ncstat = extractstring(nccomm, xgetvar, xnode, segment, dimindex, conn, currentcontent, memory);
        else
            ncstat = extract(nccomm, xgetvar, xnode, segment, dimindex, conn, currentcontent, memory);
        break;

    }

done:
    oc_data_free(conn,dimcontent);
    oc_data_free(conn,reccontent);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    if(odom) dapodom_free(odom);
    return THROW(ncstat);
}

static NCerror
movetofield(NCDAPCOMMON* nccomm,
	OCdatanode currentcontent,
	NClist* path,
        size_t depth, /* depth is position in segment list*/
	Getvara* xgetvar,
        size_t dimindex, /* dimindex is position in xgetvar->slices*/
	struct NCMEMORY* memory,
	NClist* segments)
{
    OCerror ocstat = OC_NOERR;
    NCerror ncstat = NC_NOERR;
    size_t fieldindex,gridindex;
    OClink conn = nccomm->oc.conn;
    CDFnode* xnode = (CDFnode*)nclistget(path,depth);
    OCdatanode fieldcontent = NULL;
    CDFnode* xnext;
    size_t newdepth;
    int ffield;

    /* currentcontent points to the grid/dataset/structure/record instance */
    xnext = (CDFnode*)nclistget(path,depth+1);
    ASSERT((xnext != NULL));

     /* If findfield is less than 0,
         and passes through this stanza,
         an undefined value will be passed to
         oc_data_ithfield.  See coverity
         issue 712596. */
    ffield = findfield(xnode, xnext);
    if(ffield < 0) {
      ncstat = NC_EBADFIELD;
      goto done;
    } else {
      fieldindex = findfield(xnode,xnext);
    }

    /* If the next node is a nc_virtual node, then
       we need to effectively
       ignore it and use the appropriate subnode.
       If the next node is a re-struct'd node, then
       use it as is.
    */
    if(xnext->nc_virtual) {
        CDFnode* xgrid = xnext;
	xnext = (CDFnode*)nclistget(path,depth+2); /* real node */
        gridindex = fieldindex;
	fieldindex = findfield(xgrid,xnext);
	fieldindex += gridindex;
	newdepth = depth+2;
    } else {
        newdepth = depth+1;
    }
    /* Move to appropriate field */
    ocstat = oc_data_ithfield(conn,currentcontent,fieldindex,&fieldcontent);
    if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
    ncstat = movetor(nccomm,fieldcontent,
                     path,newdepth,xgetvar,dimindex,memory,
		     segments);

done:
    oc_data_free(conn,fieldcontent);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return THROW(ncstat);
}

#if 0

/* Determine the index in the odometer at which
   the odometer will be walking the whole subslice
   This will allow us to optimize.
*/
static int
wholeslicepoint(Dapodometer* odom)
{
    unsigned int i;
    int point;
    for(point=-1,i=0;i<odom->rank;i++) {
        ASSERT((odom->size[i] != 0));
        if(odom->start[i] != 0 || odom->stride[i] != 1
           || odom->count[i] != odom->size[i])
	    point = i;
    }
    if(point == -1)
	point = 0; /* wholevariable */
    else if(point == (odom->rank - 1))
	point = -1; /* no whole point */
    else
	point += 1; /* intermediate point */
    return point;
}
#endif

static int
findfield(CDFnode* node, CDFnode* field)
{
    size_t i;
    for(i=0;i<nclistlength(node->subnodes);i++) {
        CDFnode* test = (CDFnode*) nclistget(node->subnodes,i);
        if(test == field) return i;
    }
    return -1;
}

static int
conversionrequired(nc_type t1, nc_type t2)
{
    if(t1 == t2)
	return 0;
    if(nctypesizeof(t1) != nctypesizeof(t2))
	return 1;
    /* Avoid too many cases by making t1 < t2 */
    if(t1 > t2) {int tmp = t1; t1 = t2; t2 = tmp;}
#undef CASE
#define CASE(t1,t2) ((t1)<<5 | (t2))
    switch (CASE(t1,t2)) {
    case CASE(NC_BYTE,NC_UBYTE):
    case CASE(NC_BYTE,NC_CHAR):
    case CASE(NC_CHAR,NC_UBYTE):
    case CASE(NC_SHORT,NC_USHORT):
    case CASE(NC_INT,NC_UINT):
    case CASE(NC_INT64,NC_UINT64):
	return 0;
    default: break;
    }
    return 1;
}

/* We are at a primitive variable or scalar that has no string dimensions.
   Extract the data. (This is way too complicated)
*/
static int
extract(
	NCDAPCOMMON* nccomm,
	Getvara* xgetvar,
	CDFnode* xnode,
        DCEsegment* segment,
	size_t dimindex,/*notused*/
        OClink conn,
        OCdatanode currentcontent,
	struct NCMEMORY* memory
       )
{
    OCerror ocstat = OC_NOERR;
    NCerror ncstat = NC_NOERR;
    size_t count,rank0;
    Dapodometer* odom = NULL;
    size_t externtypesize;
    size_t interntypesize;
    int requireconversion;
    char value[16];

    ASSERT((segment != NULL));

    requireconversion = conversionrequired(xgetvar->dsttype,xnode->etype);

    ASSERT(xgetvar->cache != NULL);
    externtypesize = nctypesizeof(xgetvar->dsttype);
    interntypesize = nctypesizeof(xnode->etype);

    rank0 = nclistlength(xnode->array.dimset0);

#ifdef DEBUG2
fprintf(stderr,"moveto: primitive: segment=%s",
                dcetostring((DCEnode*)segment));
fprintf(stderr," iswholevariable=%d",xgetvar->cache->wholevariable);
fprintf(stderr,"\n");
#endif

    if(rank0 == 0) {/* scalar */
	char* mem = (requireconversion?value:memory->next);
	ASSERT(externtypesize <= sizeof(value));
	/* Read the whole scalar directly into memory  */
	ocstat = oc_data_readscalar(conn,currentcontent,externtypesize,mem);
	if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	if(requireconversion) {
	    /* convert the value to external type */
            ncstat = dapconvert(xnode->etype,xgetvar->dsttype,memory->next,value,1);
            if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
        }
        memory->next += (externtypesize);
    } else if(xgetvar->cache->wholevariable) {/* && rank0 > 0 */
	/* There are multiple cases, assuming no conversion required.
           1) client is asking for whole variable
              => start=0, count=totalsize, stride=1
	      => read whole thing at one shot
           2) client is asking for non-strided subset
              and edges are maximal
              => start=x, count=y, stride=1
	      => read whole subset at one shot
           3) client is asking for strided subset or edges are not maximal
              => start=x, count=y, stride=s
	      => we have to use odometer on leading prefix.
           If conversion required, then read one-by-one
	*/
	size_t safeindex = dcesafeindex(segment,0,rank0);
	assert(safeindex <= rank0);

	if(!requireconversion && safeindex == 0) { /* can read whole thing */
            size_t internlen;
	    count = dcesegmentsize(segment,0,rank0); /* how many to read */
	    internlen = interntypesize*count;
            /* Read the whole variable directly into memory.*/
            ocstat = oc_data_readn(conn,currentcontent,NC_coord_zero,count,internlen,memory->next);
	    /* bump memory pointer */
	    memory->next += internlen;
            if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	} else if(!requireconversion && safeindex > 0 && safeindex < rank0) {
	    size_t internlen;
	    /* We need to build an odometer for the unsafe prefix of the slices */
	    odom = dapodom_fromsegment(segment,0,safeindex);
	    count = dcesegmentsize(segment,safeindex,rank0); /* read in count chunks */
	    internlen = interntypesize*count;
	    while(dapodom_more(odom)) {
	        ocstat = oc_data_readn(conn,currentcontent,odom->index,count,internlen,memory->next);
	        if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	        memory->next += internlen;
	        dapodom_next(odom);
	    }
            dapodom_free(odom);
        } else {
	     /* Cover following cases, all of which require reading
                values one-by-one:
		1. requireconversion
		2. !requireconversion but safeindex == rank0 =>no safe indices
		Note that in case 2, we will do a no-op conversion.
	     */
            odom = dapodom_fromsegment(segment,0,rank0);
	    while(dapodom_more(odom)) {
	        char value[16]; /* Big enough to hold any numeric value */
	        ocstat = oc_data_readn(conn,currentcontent,odom->index,1,interntypesize,value);
	        if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	        ncstat = dapconvert(xnode->etype,xgetvar->dsttype,memory->next,value,1);
	        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
	        memory->next += (externtypesize);
	        dapodom_next(odom);
	    }
            dapodom_free(odom);
        }
    } else { /* !xgetvar->cache->wholevariable && rank0 > 0 */
	/* This is the case where the constraint was applied by the server,
           so we just read it in, possibly with conversion
	*/
	if(requireconversion) {
	    /* read one-by-one */
            odom = dapodom_fromsegment(segment,0,rank0);
	    while(dapodom_more(odom)) {
	        char value[16]; /* Big enough to hold any numeric value */
	        ocstat = oc_data_readn(conn,currentcontent,odom->index,1,interntypesize,value);
	        if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	        ncstat = dapconvert(xnode->etype,xgetvar->dsttype,memory->next,value,1);
	        if(ncstat != NC_NOERR) {THROWCHK(ncstat); goto done;}
	        memory->next += (externtypesize);
	        dapodom_next(odom);
	    }
            dapodom_free(odom);
	} else {/* Read straight to memory */
            size_t internlen;
	    count = dcesegmentsize(segment,0,rank0); /* how many to read */
	    internlen = interntypesize*count;
            ocstat = oc_data_readn(conn,currentcontent,NC_coord_zero,count,internlen,memory->next);
            if(ocstat != OC_NOERR) {THROWCHK(ocstat); goto done;}
	}
    }
done:
    return THROW(ncstat);
}

static NCerror
slicestring(OClink conn, char* stringmem, DCEslice* slice, struct NCMEMORY* memory)
{
    size_t stringlen;
    NCerror ncstat = NC_NOERR;
    char* lastchar;
    size_t charcount; /* number of characters inserted into memory */

    /* libnc-dap chooses to convert string escapes to the corresponding
       character; so we do likewise.
    */
    dapexpandescapes(stringmem);
    stringlen = strlen(stringmem);

#ifdef DEBUG2
fprintf(stderr,"moveto: slicestring: string/%lu=%s\n",stringlen,stringmem);
fprintf(stderr,"slicestring: %lu string=|%s|\n",stringlen,stringmem);
fprintf(stderr,"slicestring: slice=[%lu:%lu:%lu/%lu]\n",
slice->first,slice->stride,slice->last,slice->declsize);
#endif

    /* Stride across string; if we go past end of string, then pad*/
    charcount = 0;
    for(size_t i=slice->first;i<slice->length;i+=slice->stride) {
        if(i < stringlen)
            *memory->next = stringmem[i];
        else /* i >= stringlen*/
            *memory->next = NC_FILL_CHAR;
	memory->next++;
	charcount++;
    }
    lastchar = (memory->next);
    if(charcount > 0) {
        lastchar--;
    }

    return THROW(ncstat);
}

/*
Extract data for a netcdf variable that has a string dimension.
*/
static int
extractstring(
	NCDAPCOMMON* nccomm,
	Getvara* xgetvar,
	CDFnode* xnode,
        DCEsegment* segment,
	size_t dimindex, /*notused*/
        OClink conn,
        OCdatanode currentcontent,
	struct NCMEMORY* memory
       )
{
    NCerror ncstat = NC_NOERR;
    OCerror ocstat = OC_NOERR;
    size_t i;
    size_t rank0;
    NClist* strings = NULL;
    Dapodometer* odom = NULL;

    ASSERT(xnode->etype == NC_STRING || xnode->etype == NC_URL);

    /* Compute rank minus string dimension */
    rank0 = nclistlength(xnode->array.dimset0);

    /* keep whole extracted strings stored in an NClist */
    strings = nclistnew();

    if(rank0 == 0) {/*=> scalar*/
	char* value = NULL;
	ocstat = oc_data_readscalar(conn,currentcontent,sizeof(value),&value);
	if(ocstat != OC_NOERR) goto done;
	nclistpush(strings,(void*)value);
    } else {
        /* Use the odometer to walk to the appropriate fields*/
        odom = dapodom_fromsegment(segment,0,rank0);
        while(dapodom_more(odom)) {
	    char* value = NULL;
	    ocstat = oc_data_readn(conn,currentcontent,odom->index,1,sizeof(value),&value);
	    if(ocstat != OC_NOERR)
		goto done;
	    nclistpush(strings,(void*)value);
            dapodom_next(odom);
	}
        dapodom_free(odom);
	odom = NULL;
    }
    /* Get each string in turn, slice it by applying the string dimm
       and store in user supplied memory
    */
    for(i=0;i<nclistlength(strings);i++) {
	char* s = (char*)nclistget(strings,i);
	slicestring(conn,s,&segment->slices[rank0],memory);
	free(s);
    }
done:
    if(strings != NULL) nclistfree(strings);
    if(ocstat != OC_NOERR) ncstat = ocerrtoncerr(ocstat);
    return THROW(ncstat);
}

static NCerror
makegetvar(NCDAPCOMMON* nccomm, CDFnode* var, void* data, nc_type dsttype, Getvara** getvarp)
{
    NCerror ncstat = NC_NOERR;

    if(getvarp)
    {
       Getvara* getvar;

       getvar = (Getvara*)calloc(1,sizeof(Getvara));
       MEMCHECK(getvar,NC_ENOMEM);

       getvar->target = var;
       getvar->memory = data;
       getvar->dsttype = dsttype;

       *getvarp = getvar;
    }
    return ncstat;
}

/*
Given DDS node, locate the node
in a DATADDS that matches the DDS node.
Return NULL if no node found
*/

void
unattach(CDFnode* root)
{
    unsigned int i;
    CDFtree* xtree = root->tree;
    for(i=0;i<nclistlength(xtree->nodes);i++) {
	CDFnode* xnode = (CDFnode*)nclistget(xtree->nodes,i);
	/* break bi-directional link */
        xnode->attachment = NULL;
    }
}

static void
setattach(CDFnode* target, CDFnode* pattern)
{
    target->attachment = pattern;
    pattern->attachment = target;
    /* Transfer important information */
    target->externaltype = pattern->externaltype;
    target->maxstringlength = pattern->maxstringlength;
    target->sequencelimit = pattern->sequencelimit;
    target->ncid = pattern->ncid;
    /* also transfer libncdap4 info */
    target->typeid = pattern->typeid;
    target->typesize = pattern->typesize;
}

static NCerror
attachdims(CDFnode* xnode, CDFnode* pattern)
{
    unsigned int i;
    for(i=0;i<nclistlength(xnode->array.dimsetall);i++) {
	CDFnode* xdim = (CDFnode*)nclistget(xnode->array.dimsetall,i);
	CDFnode* tdim = (CDFnode*)nclistget(pattern->array.dimsetall,i);
	setattach(xdim,tdim);
#ifdef DEBUG2
fprintf(stderr,"attachdim: %s->%s\n",xdim->ocname,tdim->ocname);
#endif
    }
    return NC_NOERR;
}

/*
Match a DATADDS node to a DDS node.
It is assumed that both trees have been re-struct'ed if necessary.
*/

static NCerror
attachr(CDFnode* xnode, NClist* patternpath, size_t depth)
{
    size_t i,plen,lastnode,gridable;
    NCerror ncstat = NC_NOERR;
    CDFnode* patternpathnode;
    CDFnode* patternpathnext;

    plen = nclistlength(patternpath);
    if(depth >= plen) {THROWCHK(ncstat=NC_EINVAL); goto done;}

    lastnode = (depth == (plen-1));
    patternpathnode = (CDFnode*)nclistget(patternpath,depth);
    ASSERT((simplenodematch(xnode,patternpathnode)));
    setattach(xnode,patternpathnode);
#ifdef DEBUG2
fprintf(stderr,"attachnode: %s->%s\n",xnode->ocname,patternpathnode->ocname);
#endif

    if(lastnode) goto done; /* We have the match and are done */

    if(nclistlength(xnode->array.dimsetall) > 0) {
	attachdims(xnode,patternpathnode);
    }

    ASSERT((!lastnode));
    patternpathnext = (CDFnode*)nclistget(patternpath,depth+1);

    gridable = (patternpathnext->nctype == NC_Grid && depth+2 < plen);

    /* Try to find an xnode subnode that matches patternpathnext */
    for(i=0;i<nclistlength(xnode->subnodes);i++) {
        CDFnode* xsubnode = (CDFnode*)nclistget(xnode->subnodes,i);
        if(simplenodematch(xsubnode,patternpathnext)) {
	    ncstat = attachr(xsubnode,patternpath,depth+1);
	    if(ncstat) goto done;
        } else if(gridable && xsubnode->nctype == NC_Atomic) {
            /* grids may or may not appear in the datadds;
	       try to match the xnode subnodes against the parts of the grid
	    */
   	    CDFnode* patternpathnext2 = (CDFnode*)nclistget(patternpath,depth+2);
	    if(simplenodematch(xsubnode,patternpathnext2)) {
	        ncstat = attachr(xsubnode,patternpath,depth+2);
                if(ncstat) goto done;
	    }
	}
    }
done:
    return THROW(ncstat);
}

NCerror
attach(CDFnode* xroot, CDFnode* pattern)
{
    NCerror ncstat = NC_NOERR;
    NClist* patternpath = nclistnew();
    CDFnode* ddsroot = pattern->root;

    if(xroot->attachment) unattach(xroot);
    if(ddsroot != NULL && ddsroot->attachment) unattach(ddsroot);
    if(!simplenodematch(xroot,ddsroot))
	{THROWCHK(ncstat=NC_EINVAL); goto done;}
    collectnodepath(pattern,patternpath,WITHDATASET);
    ncstat = attachr(xroot,patternpath,0);
done:
    nclistfree(patternpath);
    return ncstat;
}

static NCerror
attachsubsetr(CDFnode* target, CDFnode* pattern)
{
    unsigned int i;
    NCerror ncstat = NC_NOERR;
    size_t fieldindex;

#ifdef DEBUG2
fprintf(stderr,"attachsubsetr: attach: target=%s pattern=%s\n",
	target->ocname,pattern->ocname);
#endif

    ASSERT((nodematch(target,pattern)));
    setattach(target,pattern);

    /* Try to match target subnodes against pattern subnodes */

    fieldindex = 0;
    for(fieldindex=0,i=0;i<nclistlength(pattern->subnodes) && fieldindex<nclistlength(target->subnodes);i++) {
        CDFnode* patternsubnode = (CDFnode*)nclistget(pattern->subnodes,i);
        CDFnode* targetsubnode = (CDFnode*)nclistget(target->subnodes,fieldindex);
        if(nodematch(targetsubnode,patternsubnode)) {
#ifdef DEBUG2
fprintf(stderr,"attachsubsetr: match: %s :: %s\n",targetsubnode->ocname,patternsubnode->ocname);
#endif
            ncstat = attachsubsetr(targetsubnode,patternsubnode);
   	    if(ncstat) goto done;
	    fieldindex++;
	}
    }
done:
    return THROW(ncstat);
}


/*
Match nodes in pattern tree to nodes in target tree;
pattern tree is typically a structural superset of target tree.
WARNING: Dimensions are not attached
*/

static NCerror
attachsubset(CDFnode* target, CDFnode* pattern)
{
    NCerror ncstat = NC_NOERR;

    if(pattern == NULL) {THROWCHK(ncstat=NC_NOERR); goto done;}
    if(!nodematch(target,pattern)) {THROWCHK(ncstat=NC_EINVAL); goto done;}
#ifdef DEBUG2
fprintf(stderr,"attachsubset: target=%s\n",dumptree(target));
fprintf(stderr,"attachsubset: pattern=%s\n",dumptree(pattern));
#endif
    ncstat = attachsubsetr(target,pattern);
done:
    return ncstat;
}

static void
freegetvara(Getvara* vara)
{
    if(vara == NULL) return;
    dcefree((DCEnode*)vara->varaprojection);
    nullfree(vara);
}

#ifdef EXTERN_UNUSED
int
nc3d_getvarmx(int ncid, int varid,
	    const size_t *start,
	    const size_t *edges,
	    const ptrdiff_t* stride,
 	    const ptrdiff_t* map,
	    void* data,
	    nc_type dsttype0)
{
    NCerror ncstat = NC_NOERR;
    int i;
    NC* drno;
    NC* substrate;
    NC3_INFO* substrate3;
    NCDAPCOMMON* dapcomm;
    NC_var* var;
    CDFnode* cdfvar; /* cdf node mapping to var*/
    NClist* varnodes;
    nc_type dsttype;
    size_t externsize;
    size_t dimsizes[NC_MAX_VAR_DIMS];
    Dapodometer* odom = NULL;
    unsigned int ncrank;
    NClist* ncdims = NULL;
    size_t nelems;
#ifdef NEWVARM
    char* localcopy; /* of whole variable */
#endif

    ncstat = NC_check_id(ncid, (NC**)&drno);
    if(ncstat != NC_NOERR) goto done;
    dapcomm = (NCDAPCOMMON*)drno->dispatchdata;

    ncstat = NC_check_id(drno->substrate, &substrate);
    if(ncstat != NC_NOERR) goto done;
    substrate3 = (NC3_INFO*)drno->dispatchdata;

    var = NC_lookupvar(substrate3,varid);
    if(var == NULL) {ncstat = NC_ENOTVAR; goto done;}

    /* Locate var node via varid */
    varnodes = dapcomm->cdf.ddsroot->tree->varnodes;
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(varnodes,i);
	if(node->array.basevar == NULL
           && node->nctype == NC_Atomic
           && node->ncid == varid) {
	    cdfvar = node;
	    break;
	}
    }

    ASSERT((cdfvar != NULL));
    ASSERT((strcmp(cdfvar->ncfullname,var->name->cp)==0));

    if(nclistlength(cdfvar->array.dimsetplus) == 0) {
       /* The variable is a scalar; consequently, there is only one
          thing to get and only one place to put it. */
	/* recurse with additional parameters */
        return THROW(nc3d_getvarx(ncid,varid,
		 NULL,NULL,NULL,
		 data,dsttype0));
    }

    dsttype = (dsttype0);

    /* Default to using the inquiry type for this var*/
    if(dsttype == NC_NAT) dsttype = cdfvar->externaltype;

    /* Validate any implied type conversion*/
    if(cdfvar->etype != dsttype && dsttype == NC_CHAR) {
	/* The only disallowed conversion is to/from char and non-byte
           numeric types*/
	switch (cdfvar->etype) {
	case NC_STRING: case NC_URL:
	case NC_CHAR: case NC_BYTE: case NC_UBYTE:
 	    break;
	default:
	    return THROW(NC_ECHAR);
	}
    }

    externsize = nctypesizeof(dsttype);

    /* Accumulate the dimension sizes and the total # of elements */
    ncdims = cdfvar->array.dimsetall;
    ncrank = nclistlength(ncdims);

    nelems = 1; /* also Compute the number of elements being retrieved */
    for(i=0;i<ncrank;i++) {
	CDFnode* dim = (CDFnode*)nclistget(ncdims,i);
	dimsizes[i] = dim->dim.declsize;
	nelems *= edges[i];
    }

    /* Originally, this code repeatedly extracted single values
       using get_var1. In an attempt to improve performance,
       I have converted to reading the whole variable at once
       and walking it locally.
    */

#ifdef NEWVARM
    localcopy = (char*)malloc(nelems*externsize);

    /* We need to use the varieties of get_vars in order to
       properly do conversion to the external type
    */

    switch (dsttype) {

    case NC_CHAR:
	ncstat = nc_get_vars_text(ncid,varid,start, edges, stride,
				  (char*)localcopy);
	break;
    case NC_BYTE:
	ncstat = nc_get_vars_schar(ncid,varid,start, edges, stride,
				   (signed char*)localcopy);
	break;
    case NC_SHORT:
	ncstat = nc_get_vars_short(ncid,varid, start, edges, stride,
			  	   (short*)localcopy);
	break;
    case NC_INT:
	ncstat = nc_get_vars_int(ncid,varid,start, edges, stride,
				 (int*)localcopy);
	break;
    case NC_FLOAT:
	ncstat = nc_get_vars_float(ncid,varid,start, edges, stride,
				   (float*)localcopy);
	break;
    case NC_DOUBLE:
	ncstat = nc_get_vars_double(ncid,varid,	start, edges, stride,
		 		    (double*)localcopy);
	break;
    default: break;
    }


    odom = dapodom_new(ncrank,start,edges,stride,NULL);

    /* Walk the local copy */
    for(i=0;i<nelems;i++) {
	size_t voffset = dapodom_varmcount(odom,map,dimsizes);
	void* dataoffset = (void*)(((char*)data) + (externsize*voffset));
	char* localpos = (localcopy + externsize*i);
	/* extract the indexset'th value from local copy */
	memcpy(dataoffset,(void*)localpos,externsize);
/*
fprintf(stderr,"new: %lu -> %lu  %f\n",
	(unsigned long)(i),
        (unsigned long)voffset,
	*(float*)localpos);
*/
	dapodom_next(odom);
    }
#else
    odom = dapodom_new(ncrank,start,edges,stride,NULL);
    while(dapodom_more(odom)) {
	size_t* indexset = odom->index;
	size_t voffset = dapodom_varmcount(odom,map,dimsizes);
	char internalmem[128];
	char externalmem[128];
	void* dataoffset = (void*)(((char*)data) + (externsize*voffset));

	/* get the indexset'th value using variable's internal type */
	ncstat = nc_get_var1(ncid,varid,indexset,(void*)&internalmem);
        if(ncstat != NC_NOERR) goto done;
	/* Convert to external type */
	ncstat = dapconvert(cdfvar->etype,dsttype,externalmem,internalmem);
        if(ncstat != NC_NOERR) goto done;
	memcpy(dataoffset,(void*)externalmem,externsize);
/*
fprintf(stderr,"old: %lu -> %lu  %f\n",
	(unsigned long)dapodom_count(odom),
        (unsigned long)voffset,
	*(float*)externalmem);
*/
	dapodom_next(odom);
    }
#endif

done:
    return ncstat;
}
#endif /*EXTERN_UNUSED*/
