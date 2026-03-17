/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "dapincludes.h"
#include "daputil.h"
#include "dapdump.h"
#include <stddef.h>

#ifdef DAPDEBUG
extern char* ocfqn(OCddsnode);
#endif

/* Forward*/
static NCerror sequencecheckr(CDFnode* node, NClist* vars, CDFnode* topseq);
static NCerror restructr(NCDAPCOMMON*, CDFnode*, CDFnode*, NClist*);
static NCerror repairgrids(NCDAPCOMMON*, NClist*);
static NCerror structwrap(NCDAPCOMMON*, CDFnode*, CDFnode*, size_t, CDFnode*);
static size_t findin(CDFnode* parent, CDFnode* child);
static CDFnode* makenewstruct(NCDAPCOMMON*, CDFnode*, CDFnode*);
static NCerror mapnodesr(CDFnode*, CDFnode*, int depth);
static NCerror mapfcn(CDFnode* dstnode, CDFnode* srcnode);
static NCerror definedimsetplus(NCDAPCOMMON* nccomm, CDFnode* node);
static NCerror definedimsetall(NCDAPCOMMON* nccomm, CDFnode* node);
static NCerror definetransdimset(NCDAPCOMMON* nccomm, CDFnode* node);
static NCerror definedimsettransR(NCDAPCOMMON* nccomm, CDFnode* node);
static NCerror definedimsetsR(NCDAPCOMMON* nccomm, CDFnode* node);
static NCerror buildcdftreer(NCDAPCOMMON*, OCddsnode, CDFnode*, CDFtree*, CDFnode**);
static void free1cdfnode(CDFnode* node);
static NCerror fixnodes(NCDAPCOMMON*, NClist* cdfnodes);
static void defdimensions(OCddsnode ocnode, CDFnode* cdfnode, NCDAPCOMMON* nccomm, CDFtree* tree);

/* Accumulate useful node sets  */
NCerror
computecdfnodesets(NCDAPCOMMON* nccomm, CDFtree* tree)
{
    unsigned int i;
    NClist* varnodes;
    NClist* allnodes;

    allnodes = tree->nodes;
    varnodes = nclistnew();

    if(tree->seqnodes == NULL) tree->seqnodes = nclistnew();
    if(tree->gridnodes == NULL) tree->gridnodes = nclistnew();
    nclistclear(tree->seqnodes);
    nclistclear(tree->gridnodes);

    computevarnodes(nccomm,allnodes,varnodes);
    nclistfree(tree->varnodes);
    tree->varnodes = varnodes;
    varnodes = NULL;

    /* Now compute other sets of interest */
    for(i=0;i<nclistlength(allnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(allnodes,i);
	switch (node->nctype) {
	case NC_Sequence:
	    nclistpush(tree->seqnodes,(void*)node);
	    break;
	case NC_Grid:
	    nclistpush(tree->gridnodes,(void*)node);
	    break;
	default: break;
	}
    }
    return NC_NOERR;
}

NCerror
computevarnodes(NCDAPCOMMON* nccomm, NClist* allnodes, NClist* varnodes)
{
    size_t i, len;
    NClist* allvarnodes = nclistnew();
    for(i=0;i<nclistlength(allnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(allnodes,i);
#if 0
	/* If this node has a bad name, repair it */
	if(dap_badname(node->ocname)) {
	    char* newname = dap_repairname(node->ocname);
	    nullfree(node->ocname);
	    node->ocname = newname;
	}
#endif
	if(node->nctype == NC_Atomic)
	    nclistpush(allvarnodes,(void*)node);
    }
    /* Further process the variable nodes to get the final set */
    /* Use toplevel vars first */
    len = nclistlength(allvarnodes);
    for(i=0;i<len;i++) {
	CDFnode* node = (CDFnode*)nclistget(allvarnodes,i);
	if(node == NULL) continue;
        if(daptoplevel(node)) {
	    nclistpush(varnodes,(void*)node);
	    nclistset(allvarnodes,i,(void*)NULL);
	}
    }
    /*... then grid arrays and maps.
      but exclude the coordinate variables if we are trying to
      exactly mimic nc-dap
    */
    for(i=0;i<len;i++) {
	CDFnode* node = (CDFnode*)nclistget(allvarnodes,i);
	if(node == NULL) continue;
	if(dapgridarray(node)) {
	    nclistpush(varnodes,(void*)node);
	    nclistset(allvarnodes,i,(void*)NULL);
        } else if(dapgridmap(node)) {
	    if(!FLAGSET(nccomm->controls,NCF_NCDAP))
		nclistpush(varnodes,(void*)node);
	    nclistset(allvarnodes,i,(void*)NULL);
	}
    }
    /*... then all others */
    for(i=0;i<len;i++) {
	CDFnode* node = (CDFnode*)nclistget(allvarnodes,i);
	if(node == NULL) continue;
        nclistpush(varnodes,(void*)node);
    }
    nclistfree(allvarnodes);
#ifdef DEBUG2
for(i=0;i<nclistlength(varnodes);i++) {
CDFnode* node = (CDFnode*)nclistget(varnodes,i);
if(node == NULL) continue;
fprintf(stderr,"computevarnodes: var: %s\n",makecdfpathstring(node,"."));
}
#endif
    return NC_NOERR;
}



NCerror
fixgrid(NCDAPCOMMON* nccomm, CDFnode* grid)
{
    unsigned int i;
    CDFnode* array;

    size_t glen = nclistlength(grid->subnodes);
    array = (CDFnode*)nclistget(grid->subnodes,0);
    if(nccomm->controls.flags & (NCF_NC3)) {
        /* Rename grid Array: variable, but leave its oc base name alone */
        nullfree(array->ncbasename);
        array->ncbasename = nulldup(grid->ncbasename);
        if(!array->ncbasename) return NC_ENOMEM;
    }
    /* validate and modify the grid structure */
    if((glen-1) != nclistlength(array->array.dimset0)) goto invalid;
    for(i=1;i<glen;i++) {
	CDFnode* arraydim = (CDFnode*)nclistget(array->array.dimset0,i-1);
	CDFnode* map = (CDFnode*)nclistget(grid->subnodes,i);
	CDFnode* mapdim;
	/* map must have 1 dimension */
	if(nclistlength(map->array.dimset0) != 1) goto invalid;
	/* and the map name must match the ith array dimension */
	if(arraydim->ocname != NULL && map->ocname != NULL
	   && strcmp(arraydim->ocname,map->ocname) != 0)
	    goto invalid;
	/* and the map name must match its dim name (if any) */
	mapdim = (CDFnode*)nclistget(map->array.dimset0,0);
	if(mapdim->ocname != NULL && map->ocname != NULL
	   && strcmp(mapdim->ocname,map->ocname) != 0)
	    goto invalid;
	/* Add appropriate names for the anonymous dimensions */
	/* Do the map name first, so the array dim may inherit */
	if(mapdim->ocname == NULL) {
	    nullfree(mapdim->ncbasename);
	    mapdim->ocname = nulldup(map->ocname);
	    if(!mapdim->ocname) return NC_ENOMEM;
	    mapdim->ncbasename = cdflegalname(mapdim->ocname);
	    if(!mapdim->ncbasename) return NC_ENOMEM;
	}
	if(arraydim->ocname == NULL) {
	    nullfree(arraydim->ncbasename);
	    arraydim->ocname = nulldup(map->ocname);
	    if(!arraydim->ocname) return NC_ENOMEM;
	    arraydim->ncbasename = cdflegalname(arraydim->ocname);
	    if(!arraydim->ncbasename) return NC_ENOMEM;
	}
        if(FLAGSET(nccomm->controls,(NCF_NCDAP|NCF_NC3))) {
	    char tmp[3*NC_MAX_NAME];
            /* Add the grid name to the basename of the map */
	    snprintf(tmp,sizeof(tmp),"%s%s%s",map->container->ncbasename,
					  nccomm->cdf.separator,
					  map->ncbasename);
	    nullfree(map->ncbasename);
            map->ncbasename = nulldup(tmp);
	    if(!map->ncbasename) return NC_ENOMEM;
	}
    }
    return NC_NOERR;
invalid:
    return NC_EINVAL; /* mal-formed grid */
}

NCerror
fixgrids(NCDAPCOMMON* nccomm)
{
    unsigned int i;
    NClist* gridnodes = nccomm->cdf.ddsroot->tree->gridnodes;

    for(i=0;i<nclistlength(gridnodes);i++) {
        CDFnode* grid = (CDFnode*)nclistget(gridnodes,i);
        (void)fixgrid(nccomm,grid);
	/* Ignore mal-formed grids */
    }
    return NC_NOERR;
}

/*
Figure out the names for variables.
*/
NCerror
computecdfvarnames(NCDAPCOMMON* nccomm, CDFnode* root, NClist* varnodes)
{
    unsigned int i,j,d;

    /* clear all elided marks; except for dataset and grids */
    for(i=0;i<nclistlength(root->tree->nodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(root->tree->nodes,i);
	node->elided = 0;
	if(node->nctype == NC_Grid || node->nctype == NC_Dataset)
	    node->elided = 1;
    }

    /* ensure all variables have an initial full name defined */
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* var = (CDFnode*)nclistget(varnodes,i);
	nullfree(var->ncfullname);
	var->ncfullname = makecdfpathstring(var,nccomm->cdf.separator);
#ifdef DEBUG2
fprintf(stderr,"var names: %s %s %s\n",
	var->ocname,var->ncbasename,var->ncfullname);
#endif
    }

    /*  unify all variables with same fullname and dimensions
	basevar fields says: "for duplicate grid variables";
        when does this happen?
    */
    if(FLAGSET(nccomm->controls,NCF_NC3)) {
        for(i=0;i<nclistlength(varnodes);i++) {
	    int match;
	    CDFnode* var = (CDFnode*)nclistget(varnodes,i);
	    for(j=0;j<i;j++) {
	        CDFnode* testnode = (CDFnode*)nclistget(varnodes,j);
		match = 1;
	        if(testnode->array.basevar != NULL)
		    continue; /* already processed */
	        if(strcmp(var->ncfullname,testnode->ncfullname) != 0)
		    match = 0;
		else if(nclistlength(testnode->array.dimsetall)
			!= nclistlength(var->array.dimsetall))
		    match = 0;
	        else for(d=0;d<nclistlength(testnode->array.dimsetall);d++) {
		    CDFnode* vdim = (CDFnode*)nclistget(var->array.dimsetall,d);
		    CDFnode* tdim = (CDFnode*)nclistget(testnode->array.dimsetall,d);
	            if(vdim->dim.declsize != tdim->dim.declsize) {
		        match = 0;
			break;
		    }
		}
		if(match) {
		    testnode->array.basevar = var;
fprintf(stderr,"basevar invoked: %s\n",var->ncfullname);
		}
	    }
	}
    }

    /* Finally, verify unique names */
    for(i=0;i<nclistlength(varnodes);i++) {
	CDFnode* var1 = (CDFnode*)nclistget(varnodes,i);
	if(var1->array.basevar != NULL) continue;
	for(j=0;j<i;j++) {
	    CDFnode* var2 = (CDFnode*)nclistget(varnodes,j);
	    if(var2->array.basevar != NULL) continue;
	    if(strcmp(var1->ncfullname,var2->ncfullname)==0) {
		PANIC1("duplicate var names: %s",var1->ncfullname);
	    }
	}
    }
    return NC_NOERR;
}


/* locate and connect usable sequences and vars.
A sequence is usable iff:
1. it has a path from one of its subnodes to a leaf and that
   path does not contain a sequence.
2. No parent container has dimensions.
*/

NCerror
sequencecheck(NCDAPCOMMON* nccomm)
{
    (void)sequencecheckr(nccomm->cdf.ddsroot,
                          nccomm->cdf.ddsroot->tree->varnodes,NULL);
    return NC_NOERR;
}


static NCerror
sequencecheckr(CDFnode* node, NClist* vars, CDFnode* topseq)
{
    unsigned int i;
    NCerror err = NC_NOERR;
    int ok = 0;
    if(topseq == NULL && nclistlength(node->array.dimset0) > 0) {
	err = NC_EINVAL; /* This container has dimensions, so no sequence within it
                            can be usable */
    } else if(node->nctype == NC_Sequence) {
	/* Recursively walk the path for each subnode of this sequence node
           looking for a path without any sequence */
	for(i=0;i<nclistlength(node->subnodes);i++) {
	    CDFnode* sub = (CDFnode*)nclistget(node->subnodes,i);
	    err = sequencecheckr(sub,vars,node);
	    if(err == NC_NOERR) ok = 1; /* there is at least 1 usable var below */
	}
	if(topseq == NULL && ok == 1) {
	    /* this sequence is usable because it has scalar container
               (by construction) and has a path to a leaf without an intermediate
               sequence. */
	    err = NC_NOERR;
	    node->usesequence = 1;
	} else {
	    /* this sequence is unusable because it has no path
               to a leaf without an intermediate sequence. */
	    node->usesequence = 0;
	    err = NC_EINVAL;
	}
    } else if(nclistcontains(vars,(void*)node)) {
	/* If we reach a leaf, then topseq is usable, so save it */
	node->array.sequence = topseq;
    } else { /* Some kind of non-sequence container node with no dimensions */
	/* recursively compute usability */
	for(i=0;i<nclistlength(node->subnodes);i++) {
	    CDFnode* sub = (CDFnode*)nclistget(node->subnodes,i);
	    err = sequencecheckr(sub,vars,topseq);
	    if(err == NC_NOERR) ok = 1;
	}
	err = (ok?NC_NOERR:NC_EINVAL);
    }
    return err;
}

/*
Originally, if one did a constraint on a Grid such that only
one array or map in the grid was returned, that element was
returned as a top level variable.  This is incorrect because
it loses the Grid scope information.

Eventually, this behavior was changed so that such partial
grids are converted to structures where the structure name
is the grid name. This preserves the proper scoping.
However, it is still the case that some servers do the old
behavior.

The rules that most old-style servers appear to adhere to are these.
1. Asking for just a grid array or a single grid map
   returns just the array not wrapped in a structure.
2. Asking for a subset of the fields (array plus map) of a grid
   returns those fields wrapped in a structure.
3. However, there is an odd situation: asking for a grid array
   plus any subset of maps that includes the last map in the grid
   returns a malformed grid. This is clearly a bug.

For case 1, we insert a structure node so that case 1 is consistent
with case 2. Case 3 should cause an error with a malformed grid.

[Note: for some reason, this code has been difficult to get right;
I have rewritten 6 times and it probably is still not right.]
[2/25/2013 Sigh! Previous fixes have introduced another bug,
so now we fix the fix.]

Input is
(1) the root of the dds that needs to be re-gridded
(2) the full datadds tree that defines where the grids are.
(3) the projections that were used to produce (1) from (2).

*/

NCerror
restruct(NCDAPCOMMON* ncc, CDFnode* ddsroot, CDFnode* patternroot, NClist* projections)
{
    NCerror ncstat = NC_NOERR;
    NClist* repairs = nclistnew();

    /* The current restruct assumes that the ddsroot tree
       has missing grids compared to the pattern.
       It is also assumed that order of the nodes
       in the ddsroot is the same as in the pattern.
    */
    if(ddsroot->tree->restructed) {
      nclistfree(repairs);
      return NC_NOERR;
    }

#ifdef DEBUG
fprintf(stderr,"restruct: ddsroot=%s\n",dumptree(ddsroot));
fprintf(stderr,"restruct: patternroot=%s\n",dumptree(patternroot));
#endif

    /* Match roots */
    if(!simplenodematch(ddsroot,patternroot))
	ncstat = NC_EDATADDS;
    else if(!restructr(ncc,ddsroot,patternroot,repairs))
	ncstat = NC_EDATADDS;
    else if(nclistlength(repairs) > 0) {
	/* Do the repairs */
	ncstat = repairgrids(ncc, repairs);
    }

    if(repairs)
      nclistfree(repairs);

    return THROW(ncstat);
}

/*
Locate nodes in the tree rooted at node
that correspond to a single grid field in the pattern
when the pattern is a grid.
Wrap that grid field in a synthesized structure.

The key thing to look for is the case where
we have an atomic variable that appear where
we expected a grid.

*/

static int
restructr(NCDAPCOMMON* ncc, CDFnode* dxdparent, CDFnode* patternparent, NClist* repairlist)
{
    size_t index, i, j, match;

#ifdef DEBUG
fprintf(stderr,"restruct: matched: %s -> %s\n",
ocfqn(dxdparent->ocnode),ocfqn(patternparent->ocnode));
#endif

    /* walk each node child and locate its match
       in the pattern's children; recurse on matches,
       non-matches may be nodes needing wrapping.
    */

    for(index=0;index<nclistlength(dxdparent->subnodes);index++) {
        CDFnode* dxdsubnode = (CDFnode*)nclistget(dxdparent->subnodes,index);
	CDFnode* matchnode = NULL;

	/* Look for a matching pattern node with same ocname */
        for(i=0;i<nclistlength(patternparent->subnodes);i++) {
            CDFnode* patternsubnode = (CDFnode*)nclistget(patternparent->subnodes,i);
	    if(strcmp(dxdsubnode->ocname,patternsubnode->ocname) == 0) {
		matchnode = patternsubnode;
		break;
	    }
	}
#ifdef DEBUG
fprintf(stderr,"restruct: candidate: %s -> %s\n",
ocfqn(dxdsubnode->ocnode),ocfqn(matchnode->ocnode));
#endif
	if(simplenodematch(dxdsubnode,matchnode)) {
	    /* this subnode of the node matches the corresponding
               node of the pattern, so it is ok =>
               recurse looking for nested mis-matches
            */
	    if(!restructr(ncc,dxdsubnode,matchnode,repairlist))
		return 0;
	} else {
            /* If we do not have a direct match, then we need to look
               at all the grids to see if this node matches a field
               in one of the grids
            */
            for(match=0,i=0;!match && i<nclistlength(patternparent->subnodes);i++) {
                CDFnode* subtemp = (CDFnode*)nclistget(patternparent->subnodes,i);
                if(subtemp->nctype == NC_Grid) {
		    /* look inside */
                    for(j=0;j<nclistlength(patternparent->subnodes);j++) {
                        CDFnode* gridfield = (CDFnode*)nclistget(subtemp->subnodes,j);
                        if(simplenodematch(dxdsubnode,gridfield)) {
                            /* We need to do this repair */
                            nclistpush(repairlist,(void*)dxdsubnode);
                            nclistpush(repairlist,(void*)gridfield);
                            match = 1;
                            break;
                        }
                    }
                }
            }
	    if(!match) return 0; /* we failed */
	}
    }
    return 1; /* we matched everything at this level */
}

/* Wrap the node wrt the pattern grid or pattern struct */

static NCerror
repairgrids(NCDAPCOMMON* ncc, NClist* repairlist)
{
    NCerror ncstat = NC_NOERR;
    size_t i;
    assert(nclistlength(repairlist) % 2 == 0);
    for(i=0;i<nclistlength(repairlist);i+=2) {
	CDFnode* node = (CDFnode*)nclistget(repairlist,i);
	CDFnode* pattern = (CDFnode*)nclistget(repairlist,i+1);
	size_t index = findin(node->container,node);
	ncstat = structwrap(ncc, node,node->container,index,
                             pattern->container);
#ifdef DEBUG
fprintf(stderr,"repairgrids: %s -> %s\n",
ocfqn(node->ocnode),ocfqn(pattern->ocnode));
#endif

    }
    return ncstat;
}

static NCerror
structwrap(NCDAPCOMMON* ncc, CDFnode* node, CDFnode* parent, size_t parentindex,
                           CDFnode* patterngrid)
{
    CDFnode* newstruct;

    ASSERT((patterngrid->nctype == NC_Grid));
    newstruct = makenewstruct(ncc, node,patterngrid);
    if(newstruct == NULL) {return THROW(NC_ENOMEM);}

    /* replace the node with the new structure
       in the parent's list of children*/
    nclistset(parent->subnodes,parentindex,(void*)newstruct);

    /* Update the list of all nodes in the tree */
    nclistpush(node->root->tree->nodes,(void*)newstruct);
    return NC_NOERR;
}

static size_t
findin(CDFnode* parent, CDFnode* child)
{
    NClist* subnodes = parent->subnodes;
    for(size_t i=0;i<nclistlength(subnodes);i++) {
	if(nclistget(subnodes,i) == child)
	    return i;
    }
    return -1;
}

/* Create a structure to surround projected grid array or map;
   this occurs because some servers (that means you ferret and you thredds!)
   do not adhere to the DAP2 protocol spec.
*/

static CDFnode*
makenewstruct(NCDAPCOMMON* ncc, CDFnode* node, CDFnode* patternnode)
{
    CDFnode* newstruct = makecdfnode(ncc,patternnode->ocname,OC_Structure,
                                      patternnode->ocnode, node->container);
    if(newstruct == NULL) return NULL;
    newstruct->nc_virtual = 1;
    newstruct->ncbasename = nulldup(patternnode->ncbasename);
    newstruct->subnodes = nclistnew();
    newstruct->pattern = patternnode;
    node->container = newstruct;
    nclistpush(newstruct->subnodes,(void*)node);
    return newstruct;
}

/**
Make the constrained dds nodes (root)
point to the corresponding unconstrained
dds nodes (fullroot).
 */

NCerror
mapnodes(CDFnode* root, CDFnode* fullroot)
{
    NCerror ncstat = NC_NOERR;
    ASSERT(root != NULL && fullroot != NULL);
    if(!simplenodematch(root,fullroot))
	{THROWCHK(ncstat=NC_EINVAL); goto done;}
    /* clear out old associations*/
    unmap(root);
    ncstat = mapnodesr(root,fullroot,0);
done:
    return ncstat;
}

static NCerror
mapnodesr(CDFnode* connode, CDFnode* fullnode, int depth)
{
    unsigned int i,j;
    NCerror ncstat = NC_NOERR;

    ASSERT((simplenodematch(connode,fullnode)));

#ifdef DEBUG
  {
char* path1 = makecdfpathstring(fullnode,".");
char* path2 = makecdfpathstring(connode,".");
fprintf(stderr,"mapnode: %s->%s\n",path1,path2);
nullfree(path1); nullfree(path2);
  }
#endif

    /* Map node */
    mapfcn(connode,fullnode);

#if 0
  {
    int i;
    for(i=0;i<nclistlength(fullnode->subnodes);i++) {
	CDFnode* n = (CDFnode*)nclistget(fullnode->subnodes,i);
	fprintf(stderr,"fullnode.subnode[%d]: (%d) %s\n",i,n->nctype,n->ocname);
    }
    for(i=0;i<nclistlength(connode->subnodes);i++) {
	CDFnode* n = (CDFnode*)nclistget(connode->subnodes,i);
	fprintf(stderr,"connode.subnode[%d]: (%d) %s\n",i,n->nctype,n->ocname);
    }
  }
#endif

    /* Try to match connode subnodes against fullnode subnodes */
    ASSERT(nclistlength(connode->subnodes) <= nclistlength(fullnode->subnodes));

    for(i=0;i<nclistlength(connode->subnodes);i++) {
        CDFnode* consubnode = (CDFnode*)nclistget(connode->subnodes,i);
	/* Search full subnodes for a matching subnode from con */
        for(j=0;j<nclistlength(fullnode->subnodes);j++) {
            CDFnode* fullsubnode = (CDFnode*)nclistget(fullnode->subnodes,j);
            if(simplenodematch(fullsubnode,consubnode)) {
                ncstat = mapnodesr(consubnode,fullsubnode,depth+1);
   	        if(ncstat) goto done;
	    }
	}
    }
done:
    return THROW(ncstat);
}


/* The specific actions of a map are defined
   by this function.
*/
static NCerror
mapfcn(CDFnode* dstnode, CDFnode* srcnode)
{
    /* Mark node as having been mapped */
    dstnode->basenode = srcnode;
    return NC_NOERR;
}

void
unmap(CDFnode* root)
{
    unsigned int i;
    CDFtree* tree = root->tree;
    for(i=0;i<nclistlength(tree->nodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(tree->nodes,i);
	node->basenode = NULL;
    }
}

/*
Move dimension data from basenodes to nodes
*/

NCerror
dimimprint(NCDAPCOMMON* nccomm)
{
    NCerror ncstat = NC_NOERR;
    NClist* allnodes;
    size_t i,j;
    CDFnode* basenode;

    allnodes = nccomm->cdf.ddsroot->tree->nodes;
    for(i=0;i<nclistlength(allnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(allnodes,i);
	size_t noderank, baserank;
        /* Do dimension imprinting */
	basenode = node->basenode;
	if(basenode == NULL) continue;
	noderank = nclistlength(node->array.dimset0);
	baserank = nclistlength(basenode->array.dimset0);
	if(noderank == 0) continue;
        ASSERT(noderank == baserank);
#ifdef DEBUG
fprintf(stderr,"dimimprint %s/%zu -> %s/%zu\n",
	makecdfpathstring(basenode,"."),
	noderank,
	makecdfpathstring(node,"."),
	baserank);
#endif
        for(j=0;j<noderank;j++) {
	    CDFnode* dim = (CDFnode*)nclistget(node->array.dimset0,j);
	    CDFnode* basedim = (CDFnode*)nclistget(basenode->array.dimset0,j);
	    dim->dim.declsize0 = basedim->dim.declsize;
#ifdef DEBUG
fprintf(stderr,"dimimprint: %d: %lu -> %lu\n",i,basedim->dim.declsize,dim->dim.declsize0);
#endif
        }
    }
    return ncstat;
}

static CDFnode*
clonedim(NCDAPCOMMON* nccomm, CDFnode* dim, CDFnode* var)
{
    CDFnode* clone;
    clone = makecdfnode(nccomm,dim->ocname,OC_Dimension,
			  NULL,dim->container);
    /* Record its existence */
    nclistpush(dim->container->root->tree->nodes,(void*)clone);
    clone->dim = dim->dim; /* copy most everything */
    clone->dim.dimflags |= CDFDIMCLONE;
    clone->dim.array = var;
    return clone;
}

static NClist*
clonedimset(NCDAPCOMMON* nccomm, NClist* dimset, CDFnode* var)
{
    NClist* result = NULL;
    size_t i;

    for(i=0;i<nclistlength(dimset);i++) {
        CDFnode *dim = NULL;
        if(result == NULL)
           result = nclistnew();

        dim = (CDFnode*)nclistget(dimset,i);
        nclistpush(result,(void*)clonedim(nccomm,dim,var));
    }
    return result;
}

/* Define the dimsetplus list for a node = dimset0+pseudo dims */
static NCerror
definedimsetplus(NCDAPCOMMON* nccomm/*notused*/, CDFnode* node)
{
    int ncstat = NC_NOERR;
    NClist* dimset = NULL;
    CDFnode* clone = NULL;

    if(node->array.dimset0 != NULL)
        /* copy the dimset0 into dimset */
        dimset = nclistclone(node->array.dimset0, 0);
    /* Insert the sequence or string dims */
    if(node->array.stringdim != NULL) {
        if(dimset == NULL) dimset = nclistnew();
	clone = node->array.stringdim;
        nclistpush(dimset,(void*)clone);
    }
    if(node->array.seqdim != NULL) {
        if(dimset == NULL) dimset = nclistnew();
	clone = node->array.seqdim;
        nclistpush(dimset,(void*)clone);
    }
    node->array.dimsetplus = dimset;
    return ncstat;
}

/* Define the dimsetall list for a node =  */
static NCerror
definedimsetall(NCDAPCOMMON* nccomm/*notused*/, CDFnode* node)
{
    size_t i;
    int ncstat = NC_NOERR;
    NClist* dimsetall = NULL;

    if(node->container != NULL) {
	/* We need to clone the parent dimensions because we will be assigning
           indices vis-a-vis this variable */
        dimsetall = clonedimset(nccomm,node->container->array.dimsetall,node);
    }
    /* append dimsetplus; */
    for(i=0;i<nclistlength(node->array.dimsetplus);i++) {
		CDFnode* clone = NULL;
        if(dimsetall == NULL) dimsetall = nclistnew();
		clone = (CDFnode*)nclistget(node->array.dimsetplus,i);
		nclistpush(dimsetall,(void*)clone);
    }
    node->array.dimsetall = dimsetall;
#ifdef DEBUG1
fprintf(stderr,"dimsetall: |%s|=%d\n",node->ocname,(int)nclistlength(dimsetall));
#endif
    return ncstat;
}

/* Define the dimsettrans list for a single node */
static NCerror
definetransdimset(NCDAPCOMMON* nccomm/*notused*/, CDFnode* node)
{
    size_t i;
    int ncstat = NC_NOERR;
    NClist* dimsettrans = NULL;

#ifdef DEBUG1
fprintf(stderr,"dimsettrans3: node=%s/%d\n",node->ocname,nclistlength(node->array.dimset0));
#endif
    if(node->container != NULL) {
	/* We need to clone the parent dimensions because we will be assigning
           indices vis-a-vis this variable */
        dimsettrans = clonedimset(nccomm,node->container->array.dimsettrans,node);
    }
    /* concat parent dimset0 and dimset;*/
    if(dimsettrans == NULL)
	dimsettrans = nclistnew();
    for(i=0;i<nclistlength(node->array.dimset0);i++) {
	CDFnode* clone = NULL;
	clone = (CDFnode*)nclistget(node->array.dimset0,i);
	nclistpush(dimsettrans,(void*)clone);
    }
    node->array.dimsettrans = dimsettrans;
    dimsettrans = NULL;
#ifdef DEBUG1
fprintf(stderr,"dimsettrans: |%s|=%d\n",node->ocname,(int)nclistlength(dimsettrans));
#endif
    return ncstat;
}

/*
Recursively define the transitive closure of dimensions
(dimsettrans) based on the original dimension set (dimset0):
*/

NCerror
definedimsettrans(NCDAPCOMMON* nccomm, CDFtree* tree)
{
    /* recursively walk the tree */
    definedimsettransR(nccomm, tree->root);
    return NC_NOERR;
}

/*
Recursive helper for definedimsettrans3
*/
static NCerror
definedimsettransR(NCDAPCOMMON* nccomm, CDFnode* node)
{
    size_t i;
    int ncstat = NC_NOERR;

    definetransdimset(nccomm,node);
    /* recurse */
    for(i=0;i<nclistlength(node->subnodes);i++) {
	CDFnode* subnode = (CDFnode*)nclistget(node->subnodes,i);
	if(subnode->nctype == NC_Dimension) continue; /*ignore*/
	ASSERT((subnode->array.dimsettrans == NULL));
	ASSERT((subnode->array.dimsetplus == NULL));
	ASSERT((subnode->array.dimsetall == NULL));
	ncstat = definedimsettransR(nccomm,subnode);
	if(ncstat != NC_NOERR)
	    break;
    }
    return ncstat;
}


/*
Recursively define two dimension sets for each structural node
based on the original dimension set (dimset0):
1. dimsetplus = dimset0+pseudo-dimensions (string,sequence).
2. dimsetall = parent-dimsetall + dimsetplus
*/

NCerror
definedimsets(NCDAPCOMMON* nccomm, CDFtree* tree)
{
    /* recursively walk the tree */
    definedimsetsR(nccomm, tree->root);
    return NC_NOERR;
}

/*
Recursive helper
*/
static NCerror
definedimsetsR(NCDAPCOMMON* nccomm, CDFnode* node)
{
    size_t i;
    int ncstat = NC_NOERR;

    definedimsetplus(nccomm,node);
    definedimsetall(nccomm,node);
    /* recurse */
    for(i=0;i<nclistlength(node->subnodes);i++) {
	CDFnode* subnode = (CDFnode*)nclistget(node->subnodes,i);
	if(subnode->nctype == NC_Dimension) continue; /*ignore*/
	ASSERT((subnode->array.dimsettrans == NULL));
	ASSERT((subnode->array.dimsetplus == NULL));
	ASSERT((subnode->array.dimsetall == NULL));
	ncstat = definedimsetsR(nccomm,subnode);
	if(ncstat != NC_NOERR)
	    break;
    }
    return ncstat;
}

CDFnode*
makecdfnode(NCDAPCOMMON* nccomm, char* ocname, OCtype octype,
             /*optional*/ OCddsnode ocnode, CDFnode* container)
{
    CDFnode* node;
    assert(nccomm != NULL);
    node = (CDFnode*)calloc(1,sizeof(CDFnode));
    if(node == NULL) return (CDFnode*)NULL;

    node->ocname = NULL;
    if(ocname) {
        size_t len = strlen(ocname);
        if(len >= NC_MAX_NAME) len = NC_MAX_NAME-1;
        node->ocname = (char*)malloc(len+1);
	if(node->ocname == NULL) { nullfree(node); return NULL;}
	memcpy(node->ocname,ocname,len);
	node->ocname[len] = '\0';
    }
    node->nctype = octypetonc(octype);
    node->ocnode = ocnode;
    node->subnodes = nclistnew();
    node->container = container;
    if(ocnode != NULL) {
	oc_dds_atomictype(nccomm->oc.conn,ocnode,&octype);
        node->etype = octypetonc(octype);
    }
    if(container != NULL)
	node->root = container->root;
    else if(node->nctype == NC_Dataset)
	node->root = node;
    return node;
}

/* Given an OCnode tree, mimic it as a CDFnode tree;
   Add DAS attributes if DAS is available. Accumulate set
   of all nodes in preorder.
*/
NCerror
buildcdftree(NCDAPCOMMON* nccomm, OCddsnode ocroot, OCdxd occlass, CDFnode** cdfrootp)
{
    CDFnode* root = NULL;
    CDFtree* tree = (CDFtree*)calloc(1,sizeof(CDFtree));
    NCerror err = NC_NOERR;
    if(!tree)
      return OC_ENOMEM;

    tree->ocroot = ocroot;
    tree->nodes = nclistnew();
    tree->occlass = occlass;
    tree->owner = nccomm;

    err = buildcdftreer(nccomm,ocroot,NULL,tree,&root);
    if(!err) {
	if(occlass != OCDAS)
	    fixnodes(nccomm,tree->nodes);
	if(cdfrootp) *cdfrootp = root;
    }
    return err;
}

static NCerror
buildcdftreer(NCDAPCOMMON* nccomm, OCddsnode ocnode, CDFnode* container,
                CDFtree* tree, CDFnode** cdfnodep)
{
    size_t i,ocrank,ocnsubnodes;
    OCtype octype;
    OCtype ocatomtype;
    char* ocname = NULL;
    NCerror ncerr = NC_NOERR;
    CDFnode* cdfnode = NULL;

    oc_dds_class(nccomm->oc.conn,ocnode,&octype);
    if(octype == OC_Atomic)
	oc_dds_atomictype(nccomm->oc.conn,ocnode,&ocatomtype);
    else
	ocatomtype = OC_NAT;
    oc_dds_name(nccomm->oc.conn,ocnode,&ocname);
    oc_dds_rank(nccomm->oc.conn,ocnode,&ocrank);
    oc_dds_nsubnodes(nccomm->oc.conn,ocnode,&ocnsubnodes);

#ifdef DEBUG1
    if(ocatomtype == OC_NAT)
	fprintf(stderr,"buildcdftree: connect: %s %s\n",oc_typetostring(octype),ocname);
    else
	fprintf(stderr,"buildcdftree: connect: %s %s\n",oc_typetostring(ocatomtype),ocname);
#endif

    switch (octype) {
    case OC_Dataset:
	cdfnode = makecdfnode(nccomm,ocname,octype,ocnode,container);
	nclistpush(tree->nodes,(void*)cdfnode);
	tree->root = cdfnode;
	cdfnode->tree = tree;
	break;

    case OC_Grid:
    case OC_Structure:
    case OC_Sequence:
	cdfnode = makecdfnode(nccomm,ocname,octype,ocnode,container);
	nclistpush(tree->nodes,(void*)cdfnode);
#if 0
	if(tree->root == NULL) {
	    tree->root = cdfnode;
	    cdfnode->tree = tree;
	}
#endif
	break;

    case OC_Atomic:
	cdfnode = makecdfnode(nccomm,ocname,octype,ocnode,container);
	nclistpush(tree->nodes,(void*)cdfnode);
#if 0
	if(tree->root == NULL) {
	    tree->root = cdfnode;
	    cdfnode->tree = tree;
	}
#endif
	break;

    case OC_Dimension:
    default: PANIC1("buildcdftree: unexpected OC node type: %d",(int)octype);

    }
    /* Avoid a rare but perhaps possible null-dereference
       of cdfnode. Not sure what error to throw, so using
       NC_EDAP: generic DAP error. */
    if(!cdfnode) {
      return NC_EDAP;
    }

#if 0
    /* cross link */
    assert(tree->root != NULL);
    cdfnode->root = tree->root;
#endif

    if(ocrank > 0) defdimensions(ocnode,cdfnode,nccomm,tree);
    for(i=0;i<ocnsubnodes;i++) {
	OCddsnode ocsubnode;
	CDFnode* subnode;
	oc_dds_ithfield(nccomm->oc.conn,ocnode,i,&ocsubnode);
	ncerr = buildcdftreer(nccomm,ocsubnode,cdfnode,tree,&subnode);
	if(ncerr) {
	  if(ocname) free(ocname);
	  return ncerr;
	}
	nclistpush(cdfnode->subnodes,(void*)subnode);
    }
    nullfree(ocname);
    if(cdfnodep) *cdfnodep = cdfnode;
    return ncerr;
}

void
freecdfroot(CDFnode* root)
{
    size_t i;
    CDFtree* tree;
    NCDAPCOMMON* nccomm;
    if(root == NULL) return;
    tree = root->tree;
    ASSERT((tree != NULL));
    /* Explicitly FREE the ocroot */
    nccomm = tree->owner;
    oc_root_free(nccomm->oc.conn,tree->ocroot);
    tree->ocroot = NULL;
    for(i=0;i<nclistlength(tree->nodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(tree->nodes,i);
	free1cdfnode(node);
    }
    nclistfree(tree->nodes);
    nclistfree(tree->varnodes);
    nclistfree(tree->seqnodes);
    nclistfree(tree->gridnodes);
    nullfree(tree);
}

/* Free up a single node, but not any
   nodes it points to.
*/
static void
free1cdfnode(CDFnode* node)
{
    unsigned int j,k;
    if(node == NULL) return;
    nullfree(node->ocname);
    nullfree(node->ncbasename);
    nullfree(node->ncfullname);
    nullfree(node->dodsspecial.dimname);
    if(node->attributes != NULL) {
	for(j=0;j<nclistlength(node->attributes);j++) {
	    NCattribute* att = (NCattribute*)nclistget(node->attributes,j);
	    nullfree(att->name);
	    for(k=0;k<nclistlength(att->values);k++)
		nullfree((char*)nclistget(att->values,k));
	    nclistfree(att->values);
	    nullfree(att);
	}
    }
    nclistfree(node->subnodes);
    nclistfree(node->attributes);
    nclistfree(node->array.dimsetplus);
    nclistfree(node->array.dimsetall);
    nclistfree(node->array.dimset0);
    nclistfree(node->array.dimsettrans);

    /* Clean up the ncdap4 fields also */
    nullfree(node->typename);
    nullfree(node->vlenname);
    nullfree(node);
}



/* Return true if node and node1 appear to refer to the same thing;
   takes grid->structure changes into account.
*/
int
nodematch(CDFnode* node1, CDFnode* node2)
{
    return simplenodematch(node1,node2);
}

/*
Try to figure out if two nodes
are the "related" =>
    same name && same nc_type and same arity
but: Allow Grid == Structure
*/

int
simplenodematch(CDFnode* node1, CDFnode* node2)
{
    /* Test all the obvious stuff */
    if(node1 == NULL || node2 == NULL)
	return 0;

    /* Add hack to address the screwed up Columbia server
       which returns different Dataset {...} names
       depending on the constraint.
    */

    if(FLAGSET(node1->root->tree->owner->controls,NCF_COLUMBIA)
       && node1->nctype == NC_Dataset) return 1;

    if(strcmp(node1->ocname,node2->ocname)!=0) /* same names */
	return 0;
    if(nclistlength(node1->array.dimset0)
	!= nclistlength(node2->array.dimset0)) /* same arity */
	return 0;

    if(node1->nctype != node2->nctype) {
	/* test for struct-grid match */
	int structgrid = ((node1->nctype == NC_Grid && node2->nctype == NC_Structure)
                          || (node1->nctype == NC_Structure && node2->nctype == NC_Grid) ? 1 : 0);
	if(!structgrid)
	    return 0;
    }

    if(node1->nctype == NC_Atomic && node1->etype != node2->etype)
	return 0;

    return 1;
}

/* Ensure every node has an initial base name defined and fullname */
/* Exceptions: anonymous dimensions. */
static NCerror
fix1node(NCDAPCOMMON* nccomm, CDFnode* node)
{
    if(node->nctype == NC_Dimension && node->ocname == NULL) return NC_NOERR;
    ASSERT((node->ocname != NULL));
    nullfree(node->ncbasename);
    node->ncbasename = cdflegalname(node->ocname);
    if(node->ncbasename == NULL) return NC_ENOMEM;
    nullfree(node->ncfullname);
    node->ncfullname = makecdfpathstring(node,nccomm->cdf.separator);
    if(node->ncfullname == NULL) return NC_ENOMEM;
    if(node->nctype == NC_Atomic)
        node->externaltype = nctypeconvert(nccomm,node->etype);
    return NC_NOERR;
}

static NCerror
fixnodes(NCDAPCOMMON* nccomm, NClist* cdfnodes)
{
    size_t i;
    for(i=0;i<nclistlength(cdfnodes);i++) {
	CDFnode* node = (CDFnode*)nclistget(cdfnodes,i);
	NCerror err = fix1node(nccomm,node);
	if(err) return err;
    }
    return NC_NOERR;
}

static void
defdimensions(OCddsnode ocnode, CDFnode* cdfnode, NCDAPCOMMON* nccomm, CDFtree* tree)
{
    size_t i,ocrank;

    oc_dds_rank(nccomm->oc.conn,ocnode,&ocrank);
    assert(ocrank > 0);
    for(i=0;i<ocrank;i++) {
	CDFnode* cdfdim;
	OCddsnode ocdim;
	char* ocname;
	size_t declsize;

	oc_dds_ithdimension(nccomm->oc.conn,ocnode,i,&ocdim);
	oc_dimension_properties(nccomm->oc.conn,ocdim,&declsize,&ocname);

	cdfdim = makecdfnode(nccomm,ocname,OC_Dimension,
                              ocdim,cdfnode->container);
	nullfree(ocname);
	nclistpush(tree->nodes,(void*)cdfdim);
	/* Initially, constrained and unconstrained are same */
	cdfdim->dim.declsize = declsize;
	cdfdim->dim.array = cdfnode;
	if(cdfnode->array.dimset0 == NULL)
	    cdfnode->array.dimset0 = nclistnew();
	nclistpush(cdfnode->array.dimset0,(void*)cdfdim);
    }
}
