/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT filey for copying and redistribution conditions.
 *********************************************************************/

#include "dapincludes.h"
#include "dceparselex.h"
#include "dceconstraints.h"
#include "dapdump.h"
#include <stddef.h>

static void completesegments(NClist* fullpath, NClist* segments);
static NCerror qualifyprojectionnames(DCEprojection* proj);
static NCerror qualifyprojectionsizes(DCEprojection* proj);
static NCerror matchpartialname(NClist* nodes, NClist* segments, CDFnode** nodep);
static int matchsuffix(NClist* matchpath, NClist* segments);
static int iscontainer(CDFnode* node);
static DCEprojection* projectify(CDFnode* field, DCEprojection* container);
static int slicematch(NClist* seglist1, NClist* seglist2);

/* Parse incoming url constraints, if any,
   to check for syntactic correctness */
NCerror
dapparsedapconstraints(NCDAPCOMMON* dapcomm, char* constraints,
		    DCEconstraint* dceconstraint)
{
    NCerror ncstat = NC_NOERR;
    char* errmsg = NULL;

    ASSERT(dceconstraint != NULL);
    nclistclear(dceconstraint->projections);
    nclistclear(dceconstraint->selections);

    ncstat = dapceparse(constraints,dceconstraint,&errmsg);
    if(ncstat) {
      nclog(NCLOGWARN,"DCE constraint parse failure: %s",errmsg);
      nclistclear(dceconstraint->projections);
      nclistclear(dceconstraint->selections);
    }
    /* errmsg is freed whether ncstat or not. */
    nullfree(errmsg);
    return ncstat;
}

/* Map constraint paths to CDFnode paths in specified tree and fill
   in the declsizes.

   Two things to watch out for:
   1. suffix paths are legal (i.e. incomplete paths)
   2. nc_virtual nodes (via restruct)

*/

NCerror
dapmapconstraints(DCEconstraint* constraint,
		CDFnode* root)
{
    size_t i;
    NCerror ncstat = NC_NOERR;
    NClist* nodes = root->tree->nodes;
    NClist* dceprojections = constraint->projections;

    /* Convert the projection paths to leaves in the dds tree */
    for(i=0;i<nclistlength(dceprojections);i++) {
	CDFnode* cdfmatch = NULL;
	DCEprojection* proj = (DCEprojection*)nclistget(dceprojections,i);
	if(proj->discrim != CES_VAR) continue; /* ignore functions */
	ncstat = matchpartialname(nodes,proj->var->segments,&cdfmatch);
	if(ncstat) goto done;
	/* Cross links */
	assert(cdfmatch != NULL);
	proj->var->annotation = (void*)cdfmatch;
    }

done:
    return THROW(ncstat);
}


/* Fill in:
    1. projection segments
    2. projection segment slices declsize
    3. selection path
*/
NCerror
dapqualifyconstraints(DCEconstraint* constraint)
{
    NCerror ncstat = NC_NOERR;
    size_t i;
#ifdef DEBUG
fprintf(stderr,"ncqualifyconstraints.before: %s\n",
		dumpconstraint(constraint));
#endif
    if(constraint != NULL) {
        for(i=0;i<nclistlength(constraint->projections);i++) {
            DCEprojection* p = (DCEprojection*)nclistget(constraint->projections,i);
            ncstat = qualifyprojectionnames(p);
            ncstat = qualifyprojectionsizes(p);
        }
    }
#ifdef DEBUG
fprintf(stderr,"ncqualifyconstraints.after: %s\n",
		dumpconstraint(constraint));
#endif
    return ncstat;
}

/* convert all names in projections in paths to be fully qualified
   by adding prefix segment objects.
*/
static NCerror
qualifyprojectionnames(DCEprojection* proj)
{
    NCerror ncstat = NC_NOERR;
    NClist* fullpath = nclistnew();

    if (proj->discrim == CES_VAR) {
    ASSERT((proj->discrim == CES_VAR
            && proj->var->annotation != NULL
            && ((CDFnode*)proj->var->annotation)->ocnode != NULL));
    collectnodepath((CDFnode*)proj->var->annotation,fullpath,!WITHDATASET);
#ifdef DEBUG
fprintf(stderr,"qualify: %s -> ",
	dumpprojection(proj));
#endif
    /* Now add path nodes to create full path */
    completesegments(fullpath,proj->var->segments);

#ifdef DEBUG
fprintf(stderr,"%s\n",
	dumpprojection(proj));
#endif
    }
    nclistfree(fullpath);
    return ncstat;
}

/* Make sure that the slice declsizes are all defined for this projection */
static NCerror
qualifyprojectionsizes(DCEprojection* proj)
{
    size_t i,j;
    if (proj->discrim == CES_VAR) {
    ASSERT(proj->discrim == CES_VAR);
#ifdef DEBUG
fprintf(stderr,"qualifyprojectionsizes.before: %s\n",
		dumpprojection(proj));
#endif
    for(i=0;i<nclistlength(proj->var->segments);i++) {
        DCEsegment* seg = (DCEsegment*)nclistget(proj->var->segments,i);
	NClist* dimset = NULL;
	CDFnode* cdfnode = (CDFnode*)seg->annotation;
	ASSERT(cdfnode != NULL);
        dimset = cdfnode->array.dimsetplus;
        seg->rank = nclistlength(dimset);
	/* For this, we do not want any string dimensions */
	if(cdfnode->array.stringdim != NULL) seg->rank--;
        for(j=0;j<seg->rank;j++) {
	    CDFnode* dim = (CDFnode*)nclistget(dimset,j);
	    if(dim->dim.basedim != NULL) dim = dim->dim.basedim;
            ASSERT(dim != null);
	    if(seg->slicesdefined)
	        seg->slices[j].declsize = dim->dim.declsize;
	    else
	        dcemakewholeslice(seg->slices+j,dim->dim.declsize);
	}
        seg->slicesdefined = 1;
        seg->slicesdeclized = 1;
    }
#ifdef DEBUG
fprintf(stderr,"qualifyprojectionsizes.after: %s\n",
		dumpprojection(proj));
#endif
    }
    return NC_NOERR;
}

static void
completesegments(NClist* fullpath, NClist* segments)
{
    size_t i,delta;
    /* add path nodes to segments to create full path */
    delta = (nclistlength(fullpath) - nclistlength(segments));
    for(i=0;i<delta;i++) {
        DCEsegment* seg = (DCEsegment*)dcecreate(CES_SEGMENT);
        CDFnode* node = (CDFnode*)nclistget(fullpath,i);
        seg->name = nulldup(node->ocname);
        seg->annotation = (void*)node;
	seg->rank = nclistlength(node->array.dimset0);
        nclistinsert(segments,i,(void*)seg);
    }
    /* Now modify the segments to point to the appropriate node
       and fill in the slices.
    */
    for(i=delta;i<nclistlength(segments);i++) {
        DCEsegment* seg = (DCEsegment*)nclistget(segments,i);
        CDFnode* node = (CDFnode*)nclistget(fullpath,i);
	seg->annotation = (void*)node;
    }
}

/*
We are given a set of segments (in path)
representing a partial path for a CDFnode variable.
Our goal is to locate all matching
variables for which the path of that
variable has a suffix matching
the given partial path.
If one node matches exactly, then use that one;
otherwise there had better be exactly one
match else ambiguous.
Additional constraints (4/12/2010):
1. if a segment is dimensioned, then use that info
   to distinguish e.g a grid node from a possible
   grid array within it of the same name.
   Treat sequences as of rank 1.
2. if there are two matches, and one is the grid
   and the other is the grid array within that grid,
   then choose the grid array.
3. If there are multiple matches choose the one with the
   shortest path
4. otherwise complain about ambiguity
*/

/**
 * Given a path as segments,
 * try to locate the CDFnode
 * instance (from a given set)
 * that corresponds to the path.
 * The key difficulty is that the
 * path may only be a suffix of the
 * complete path.
 */

static NCerror
matchpartialname(NClist* nodes, NClist* segments, CDFnode** nodep)
{
    size_t i,nsegs;
    NCerror ncstat = NC_NOERR;
    DCEsegment* lastseg = NULL;
    NClist* namematches = nclistnew();
    NClist* matches = nclistnew();
    NClist* matchpath = nclistnew();

    /* Locate all nodes with the same name
       as the last element in the segment path
    */
    nsegs = nclistlength(segments);
    lastseg = (DCEsegment*)nclistget(segments,nsegs-1);
    for(i=0;i<nclistlength(nodes);i++) {
        CDFnode* node = (CDFnode*)nclistget(nodes,i);
	if(node->ocname == null)
	    continue;
	/* Path names come from oc space */
        if(strcmp(node->ocname,lastseg->name) != 0)
	    continue;
	/* Only look at selected kinds of nodes */
	if(node->nctype != NC_Sequence
               && node->nctype != NC_Structure
               && node->nctype != NC_Grid
               && node->nctype != NC_Atomic
          )
	    continue;
	nclistpush(namematches,(void*)node);
    }
    if(nclistlength(namematches)==0) {
        nclog(NCLOGERR,"No match for projection name: %s",lastseg->name);
        ncstat = NC_EDDS;
	goto done;
    }

    /* Now, collect and compare paths of the matching nodes */
    for(i=0;i<nclistlength(namematches);i++) {
        CDFnode* matchnode = (CDFnode*)nclistget(namematches,i);
	nclistclear(matchpath);
	collectnodepath(matchnode,matchpath,0);
	/* Do a suffix match */
        if(matchsuffix(matchpath,segments)) {
	    nclistpush(matches,(void*)matchnode);
#ifdef DEBUG
fprintf(stderr,"matchpartialname: pathmatch: %s :: %s\n",
matchnode->ncfullname,dumpsegments(segments));
#endif
	}
    }
    /* |matches|==0 => no match; |matches|>1 => ambiguity */
    switch (nclistlength(matches)) {
    case 0:
        nclog(NCLOGERR,"No match for projection name: %s",lastseg->name);
        ncstat = NC_EDDS;
	break;
    case 1:
        if(nodep)
	    *nodep = (CDFnode*)nclistget(matches,0);
	break;
    default: {
	CDFnode* minnode = NULL;
	size_t minpath = 0;
	int nmin = 0; /* to catch multiple ones with same short path */
	/* ok, see if one of the matches has a path that is shorter
           then all the others */
	for(i=0;i<nclistlength(matches);i++) {
	    CDFnode* candidate = (CDFnode*)nclistget(matches,i);
	    nclistclear(matchpath);
	    collectnodepath(candidate,matchpath,0);
	    if(minpath == 0) {
		minpath = nclistlength(matchpath);
		minnode = candidate;
	    } else if(nclistlength(matchpath) == minpath) {
	        nmin++;
	    } else if(nclistlength(matchpath) < minpath) {
		minpath = nclistlength(matchpath);
		minnode = candidate;
		nmin = 1;
	    }
	} /*for*/
	if(minnode == NULL || nmin > 1) {
	    nclog(NCLOGERR,"Ambiguous match for projection name: %s",
			lastseg->name);
            ncstat = NC_EDDS;
	} else if(nodep)
	    *nodep = minnode;
	} break;
    }
#ifdef DEBUG
fprintf(stderr,"matchpartialname: choice: %s %s for %s\n",
(nclistlength(matches) > 1?"":"forced"),
(*nodep)->ncfullname,dumpsegments(segments));
#endif

done:
    nclistfree(namematches);
    nclistfree(matches);
    nclistfree(matchpath);
    return THROW(ncstat);
}

static int
matchsuffix(NClist* matchpath, NClist* segments)
{
    size_t i;
    int pathstart;
    int nsegs = (int)nclistlength(segments);
    int pathlen = (int)nclistlength(matchpath);
    int segmatch;

    /* try to match the segment list as a suffix of the path list */

    /* Find the maximal point in the path s.t. |suffix of path|
       == |segments|
    */
    pathstart = (pathlen - nsegs);
    if(pathstart < 0)
	return 0; /* pathlen <nsegs => no match possible */

    /* Walk the suffix of the path and the segments and
       matching as we go
    */
    for(i=0;i<nsegs;i++) {
	CDFnode* node = (CDFnode*)nclistget(matchpath, (size_t)pathstart+i);
	DCEsegment* seg = (DCEsegment*)nclistget(segments,i);
	size_t rank = seg->rank;
	segmatch = 1; /* until proven otherwise */
	/* Do the names match (in oc name space) */
	if(strcmp(seg->name,node->ocname) != 0) {
	    segmatch = 0;
	} else if (rank != 0) {
	    /* Do the ranks match (watch out for sequences) */
	    if(node->nctype == NC_Sequence)
		rank--; /* remove sequence pseudo-rank */
	    if(rank > 0
		&& rank != nclistlength(node->array.dimset0))
		segmatch = 0; /* rank mismatch */
	}
	if(!segmatch)
	    return 0;
   }
   return 1; /* all segs matched */
}

/* Given the arguments to vara
   construct a corresponding projection
   with any pseudo dimensions removed
*/
NCerror
dapbuildvaraprojection(CDFnode* var,
		     const size_t* startp, const size_t* countp, const ptrdiff_t* stridep,
		     DCEprojection** projectionp)
{
    size_t i,j;
    NCerror ncstat = NC_NOERR;
    DCEprojection* projection = NULL;
    NClist* path = nclistnew();
    NClist* segments = NULL;
    size_t dimindex;

    /* Build a skeleton projection that has 1 segment for
       every cdfnode from root to the variable of interest.
       Each segment has the slices from its corresponding node
       in the path, including pseudo-dims
    */
    ncstat = dapvar2projection(var,&projection);

#ifdef DEBUG
fprintf(stderr,"buildvaraprojection: skeleton: %s\n",dumpprojection(projection));
#endif

    /* Now, modify the projection to reflect the corresponding
       start/count/stride from the nc_get_vara arguments.
    */
    segments = projection->var->segments;
    dimindex = 0;
    for(i=0;i<nclistlength(segments);i++) {
	DCEsegment* segment = (DCEsegment*)nclistget(segments,i);
        for(j=0;j<segment->rank;j++) {
	    size_t count = 0;
	    DCEslice* slice = &segment->slices[j];
	    /* make each slice represent the corresponding
               start/count/stride */
	    slice->first = startp[dimindex+j];
	    slice->stride = stridep[dimindex+j];
	    count = countp[dimindex+j];
	    slice->count = count;
	    slice->length = count * slice->stride;
	    slice->last = (slice->first + slice->length) - 1;
	    if(slice->last >= slice->declsize) {
		slice->last = slice->declsize - 1;
		/* reverse compute the new length */
		slice->length = (slice->last - slice->first) + 1;
	    }
	}
	dimindex += segment->rank;
    }
#ifdef DEBUG
fprintf(stderr,"buildvaraprojection.final: %s\n",dumpprojection(projection));
#endif

#ifdef DEBUG
fprintf(stderr,"buildvaraprojection3: final: projection=%s\n",
        dumpprojection(projection));
#endif

    if(projectionp) *projectionp = projection;

    nclistfree(path);
    if(ncstat) dcefree((DCEnode*)projection);
    return ncstat;
}

int
dapiswholeslice(DCEslice* slice, CDFnode* dim)
{
    if(slice->first != 0 || slice->stride != 1) return 0;
    if(dim != NULL) {
	if(slice->length != dim->dim.declsize) return 0;
    } else if(dim == NULL) {
	size_t count = slice->count;
	if(slice->declsize == 0
	   || count != slice->declsize) return 0;
    }
    return 1;
}

int
dapiswholesegment(DCEsegment* seg)
{
    size_t i;
    int whole;
    NClist* dimset = NULL;
    size_t rank;

    if(seg->rank == 0) return 1;
    if(!seg->slicesdefined) return 0;
    if(seg->annotation == NULL) return 0;
    dimset = ((CDFnode*)seg->annotation)->array.dimset0;
    rank = nclistlength(dimset);
    whole = 1; /* assume so */
    for(i=0;i<rank;i++) {
	CDFnode* dim = (CDFnode*)nclistget(dimset,i);
	if(!dapiswholeslice(&seg->slices[i],dim)) {whole = 0; break;}
    }
    return whole;
}

int
dapiswholeprojection(DCEprojection* proj)
{
    size_t i;
    int whole;

    ASSERT((proj->discrim == CES_VAR));

    whole = 1; /* assume so */
    for(i=0;i<nclistlength(proj->var->segments);i++) {
        DCEsegment* segment = (DCEsegment*)nclistget(proj->var->segments,i);
	if(!dapiswholesegment(segment)) {whole = 0; break;}
    }
    return whole;
}

int
dapiswholeconstraint(DCEconstraint* con)
{
    size_t i;
    if(con == NULL) return 1;
    if(con->projections != NULL) {
	for(i=0;i<nclistlength(con->projections);i++) {
	 if(!dapiswholeprojection((DCEprojection*)nclistget(con->projections,i)))
	    return 0;
	}
    }
    if(con->selections != NULL)
	return 0;
    return 1;
}


/*
Given a set of projections, we need to produce
an expanded, correct, and equivalent set of projections.
The term "correct" means we must fix the following cases:
1. Multiple occurrences of the same leaf variable
   with differing projection slices. Fix is to complain.
2. Occurrences of container and one or more of its fields.
   Fix is to suppress the container.
The term "expanded" means
1. Expand all occurrences of only a container by
   replacing it with all of its fields.
*/

NCerror
dapfixprojections(NClist* list)
{
    size_t i,j,k;
    NCerror ncstat = NC_NOERR;
    NClist* tmp = nclistnew(); /* misc. uses */

#ifdef DEBUG
fprintf(stderr,"fixprojection: list = %s\n",dumpprojections(list));
#endif

    if(nclistlength(list) == 0) goto done;

    /* Step 1: remove duplicates and complain about slice mismatches */
    for(i=0;i<nclistlength(list);i++) {
	DCEprojection* p1 = (DCEprojection*)nclistget(list,i);
	if(p1 == NULL) continue;
        if(p1->discrim != CES_VAR) continue; /* don't try to unify functions */
        for(j=i;j<nclistlength(list);j++) {
	    DCEprojection* p2 = (DCEprojection*)nclistget(list,j);
	    if(p2 == NULL) continue;
	    if(p1 == p2) continue;
	    if(p2->discrim != CES_VAR) continue;
	    if(p1->var->annotation != p2->var->annotation) continue;
	    /* check for slice mismatches */
	    if(!slicematch(p1->var->segments,p2->var->segments)) {
		/* complain */
		nclog(NCLOGWARN,"Malformed projection: same variable with different slicing");
	    }
	    /* remove p32 */
	    nclistset(list,j,(void*)NULL);
	    dcefree((DCEnode*)p2);
	}
    }

    /* Step 2: remove containers when a field is also present */
    for(i=0;i<nclistlength(list);i++) {
	DCEprojection* p1 = (DCEprojection*)nclistget(list,i);
	if(p1 == NULL) continue;
        if(p1->discrim != CES_VAR) continue; /* don't try to unify functions */
	if(!iscontainer((CDFnode*)p1->var->annotation))
	    continue;
        for(j=i;j<nclistlength(list);j++) {
	    DCEprojection* p2 = (DCEprojection*)nclistget(list,j);
	    if(p2 == NULL) continue;
	    if(p2->discrim != CES_VAR) continue;
	    nclistclear(tmp);
	    collectnodepath((CDFnode*)p2->var->annotation,tmp,WITHDATASET);
	    for(k=0;k<nclistlength(tmp);k++) {
		void* candidate = (void*)nclistget(tmp,k);
	        if(candidate == p1->var->annotation) {
		    nclistset(list,i,(void*)NULL);
	            dcefree((DCEnode*)p1);
		    goto next;
		}
	    }
	}
next:   continue;
    }

    /* Step 3: expand all containers recursively down to the leaf nodes */
    for(;;) {
	nclistclear(tmp);
        for(i=0;i<nclistlength(list);i++) {
            DCEprojection* target = (DCEprojection*)nclistget(list,i);
            CDFnode* leaf;
            if(target == NULL) continue;
            if(target->discrim != CES_VAR)
                continue; /* don't try to unify functions */
            leaf = (CDFnode*)target->var->annotation;
            ASSERT(leaf != NULL);
            if(iscontainer(leaf)) {/* capture container */
		if(!nclistcontains(tmp,(void*)target))
                    nclistpush(tmp,(void*)target);
                nclistset(list,i,(void*)NULL);
            }
        }
	if(nclistlength(tmp) == 0) break; /*done*/
        /* Now explode the containers */
        for(i=0;i<nclistlength(tmp);i++) {
            DCEprojection* container = (DCEprojection*)nclistget(tmp,i);
	    CDFnode* leaf = (CDFnode*)container->var->annotation;
            for(j=0;i<nclistlength(leaf->subnodes);j++) {
                CDFnode* field = (CDFnode*)nclistget(leaf->subnodes,j);
		/* Convert field node to a proper constraint */
		DCEprojection* proj = projectify(field,container);
		nclistpush(list,(void*)proj);
	    }
            /* reclaim the container */
	    dcefree((DCEnode*)container);
	}
    } /*for(;;)*/

    /* remove all NULL elements */
    for(size_t n = nclistlength(list); n-->0;) {
        DCEprojection* target = (DCEprojection*)nclistget(list,n);
        if(target == NULL)
            nclistremove(list,n);
    }
done:
#ifdef DEBUG
fprintf(stderr,"fixprojection: exploded = %s\n",dumpprojections(list));
#endif
    nclistfree(tmp);
    return ncstat;
}

static int
iscontainer(CDFnode* node)
{
    return (node->nctype == NC_Dataset
               || node->nctype == NC_Sequence
               || node->nctype == NC_Structure
               || node->nctype == NC_Grid);
}

static DCEprojection*
projectify(CDFnode* field, DCEprojection* container)
{
    DCEprojection* proj  = (DCEprojection*)dcecreate(CES_PROJECT);
    DCEvar* var  = (DCEvar*)dcecreate(CES_VAR);
    DCEsegment* seg  = (DCEsegment*)dcecreate(CES_SEGMENT);
    proj->discrim = CES_VAR;
    proj->var = var;
    var->annotation = (void*)field;
    /* Dup the segment list */
    var->segments = dceclonelist(container->var->segments);
    seg->rank = 0;
    nclistpush(var->segments,(void*)seg);
    return proj;
}

static int
slicematch(NClist* seglist1, NClist* seglist2)
{
    size_t i,j;
    if((seglist1 == NULL || seglist2 == NULL) && seglist1 != seglist2)
	return 0;
    if(nclistlength(seglist1) != nclistlength(seglist2))
	return 0;
    for(i=0;i<nclistlength(seglist1);i++) {
	DCEsegment* seg1 = (DCEsegment*)nclistget(seglist1,i);
	DCEsegment* seg2 = (DCEsegment*)nclistget(seglist2,i);
	if(seg1->rank != seg2->rank)
	    return 0;
	for(j=0;j<seg1->rank;j++) {
	    DCEslice* slice1 = &seg1->slices[j];
	    DCEslice* slice2 = &seg2->slices[j];
	    size_t count1 = slice1->count;
	    size_t count2 = slice2->count;
	    if(slice1->first != slice2->first
	       || count1 != count2
	       || slice1->stride != slice2->stride)
		return 0;
	}
    }
    return 1;
}

/* Convert a CDFnode var to a projection; include
   pseudodimensions; always whole variable.
*/
int
dapvar2projection(CDFnode* var, DCEprojection** projectionp)
{
    size_t i,j;
    int ncstat = NC_NOERR;
    NClist* path = nclistnew();
    NClist* segments;
    DCEprojection* projection = NULL;

    /* Collect the nodes needed to construct the projection segments */
    collectnodepath(var,path,!WITHDATASET);

    segments = nclistnew();
    nclistsetalloc(segments,nclistlength(path));
    for(i=0;i<nclistlength(path);i++) {
	DCEsegment* segment = (DCEsegment*)dcecreate(CES_SEGMENT);
	CDFnode* n = (CDFnode*)nclistget(path,i);
	size_t localrank;
        NClist* dimset;

	segment->annotation = (void*)n;
        segment->name = nulldup(n->ocname);
        /* We need to assign whole slices to each segment */
	localrank = nclistlength(n->array.dimsetplus);
        segment->rank = localrank;
	dimset = n->array.dimsetplus;
        for(j=0;j<localrank;j++) {
	    DCEslice* slice;
	    CDFnode* dim;
	    slice = &segment->slices[j];
	    dim = (CDFnode*)nclistget(dimset,j);
	    ASSERT(dim->dim.declsize0 > 0);
	    dcemakewholeslice(slice,dim->dim.declsize0);
	}
	segment->slicesdefined = 1;
	segment->slicesdeclized = 1;
	nclistpush(segments,(void*)segment);
    }

    projection = (DCEprojection*)dcecreate(CES_PROJECT);
    projection->discrim = CES_VAR;
    projection->var = (DCEvar*)dcecreate(CES_VAR);
    projection->var->annotation = (void*)var;
    projection->var->segments = segments;

#ifdef DEBUG1
fprintf(stderr,"dapvar2projection: projection=%s\n",
        dumpprojection(projection));
#endif

    nclistfree(path);
    if(ncstat) dcefree((DCEnode*)projection);
    else if(projectionp) *projectionp = projection;
    return ncstat;
}

/*
Given a set of projections and a projection
representing a variable (from, say vara or prefetch)
construct a single projection for fetching that variable
with the proper constraints.
*/
int
daprestrictprojection(NClist* projections, DCEprojection* var, DCEprojection** resultp)
{
    int ncstat = NC_NOERR;
    size_t i;
    DCEprojection* result = NULL;
#ifdef DEBUG1
fprintf(stderr,"restrictprojection.before: constraints=|%s| vara=|%s|\n",
		dumpprojections(projections),
		dumpprojection(var));
#endif

    ASSERT(var != NULL);

    /* the projection list will contain at most 1 match for the var by construction */
    for(result=null,i=0;i<nclistlength(projections);i++) {
	DCEprojection* p1 = (DCEprojection*)nclistget(projections,i);
	if(p1 == NULL || p1->discrim != CES_VAR) continue;
	if(p1->var->annotation == var->var->annotation) {
	    result = p1;
	    break;
	}
    }
    if(result == NULL) {
	result = (DCEprojection*)dceclone((DCEnode*)var); /* use only the var projection */
 	goto done;
    }
    result = (DCEprojection*)dceclone((DCEnode*)result); /* so we can modify */

#ifdef DEBUG1
fprintf(stderr,"restrictprojection.choice: base=|%s| add=|%s|\n",
	dumpprojection(result),dumpprojection(var));
#endif
    /* We need to merge the projection from the projection list
       with the var projection
    */
    ncstat = dcemergeprojections(result,var); /* result will be modified */

done:
    if(resultp) *resultp = result;
#ifdef DEBUG
fprintf(stderr,"restrictprojection.after=|%s|\n",
		dumpprojection(result));
#endif
    return ncstat;
}

/* Shift the slice so it runs from 0..count by step 1 */
static void
dapshiftslice(DCEslice* slice)
{
    size_t first = slice->first;
    size_t stride = slice->stride;
    if(first == 0 && stride == 1) return; /* no need to do anything */
    slice->first = 0;
    slice->stride = 1;
    slice->length = slice->count;
    slice->last = slice->length - 1;
}

int
dapshiftprojection(DCEprojection* projection)
{
    int ncstat = NC_NOERR;
    size_t i,j;
    NClist* segments;

#ifdef DEBUG1
fprintf(stderr,"dapshiftprojection.before: %s\n",dumpprojection(projection));
#endif

    ASSERT(projection->discrim == CES_VAR);
    segments = projection->var->segments;
    for(i=0;i<nclistlength(segments);i++) {
	DCEsegment* seg = (DCEsegment*)nclistget(segments,i);
        for(j=0;j<seg->rank;j++) {
	    DCEslice* slice = seg->slices+j;
	    dapshiftslice(slice);
	}
    }

#ifdef DEBUG1
fprintf(stderr,"dapshiftprojection.after: %s\n",dumpprojection(projection));
#endif

    return ncstat;
}

/* Compute the set of variables referenced in the projections
   of the input constraint.
*/
NCerror
dapcomputeprojectedvars(NCDAPCOMMON* dapcomm, DCEconstraint* constraint)
{
    NCerror ncstat = NC_NOERR;
    NClist* vars = NULL;
    size_t i;

    vars = nclistnew();

    if(dapcomm->cdf.projectedvars != NULL)
	nclistfree(dapcomm->cdf.projectedvars);
    dapcomm->cdf.projectedvars = vars;

    if(constraint == NULL || constraint->projections == NULL)
	goto done;

    for(i=0;i<nclistlength(constraint->projections);i++) {
	CDFnode* node;
	DCEprojection* proj = (DCEprojection*)nclistget(constraint->projections,i);
	if(proj->discrim == CES_FCN) continue; /* ignore these */
	node = (CDFnode*)proj->var->annotation;
	if(!nclistcontains(vars,(void*)node)) {
	    nclistpush(vars,(void*)node);
	}
    }

done:
    return ncstat;
}
