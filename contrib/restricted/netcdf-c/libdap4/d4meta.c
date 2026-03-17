/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "d4includes.h"
#include <stdarg.h>
#include <stddef.h>
#include "nc4internal.h"
#include "ncoffsets.h"

/**
 * Build the netcdf-4 metadata from the NCD4node nodes.
 */

/***************************************************/
/* Forwards */

static char* backslashEscape(const char* s);
static char* getFieldFQN(NCD4node* field, const char* tail);
static int build(NCD4meta* builder, NCD4node* root);
static int buildAtomicVar(NCD4meta* builder, NCD4node* var);
static int buildAttributes(NCD4meta* builder, NCD4node* varorgroup);
static int buildCompound(NCD4meta* builder, NCD4node* cmpdtype, NCD4node* group, char* name);
static int buildDimension(NCD4meta* builder, NCD4node* dim);
static int buildEnumeration(NCD4meta* builder, NCD4node* en);
static int buildGroups(NCD4meta*, NCD4node* parent);
static int buildMaps(NCD4meta* builder, NCD4node* var);
static int buildMetaData(NCD4meta* builder, NCD4node* var);
static int buildOpaque(NCD4meta* builder, NCD4node* op);
static int buildSequence(NCD4meta* builder, NCD4node* seq);
static int buildStructure(NCD4meta* builder, NCD4node* structvar);
static int buildStructureType(NCD4meta* builder, NCD4node* structtype);
static int buildVariable(NCD4meta* builder, NCD4node* var);
static int buildVlenType(NCD4meta* builder, NCD4node* seqtype);
static int compileAttrValues(NCD4meta* builder, NCD4node* attr, void** memoryp, NClist* blobs);
static void computeOffsets(NCD4meta* builder, NCD4node* cmpd);
static int convertString(union ATOMICS* converter, NCD4node* type, const char* s);
static void* copyAtomic(union ATOMICS* converter, nc_type type, size_t len, void* dst, NClist* blobs);
static int decodeEconst(NCD4meta* builder, NCD4node* enumtype, const char* nameorval, union ATOMICS* converter);
static int downConvert(union ATOMICS* converter, NCD4node* type);
static void freeStringMemory(char** mem, size_t count);
static size_t getDimrefs(NCD4node* var, int* dimids);
static size_t getDimsizes(NCD4node* var, int* dimsizes);
static d4size_t getpadding(d4size_t offset, size_t alignment);
static int markdapsize(NCD4meta* meta);
static int markfixedsize(NCD4meta* meta);
static void savegroupbyid(NCD4meta*,NCD4node* group);
static void savevarbyid(NCD4node* group, NCD4node* var);
static int isfilltypecompatible(NCD4node* parenttype, NCD4node* attrtype);
#ifndef FIXEDOPAQUE
static int buildBytestringType(NCD4meta* builder);
#endif

/***************************************************/
/* API */

int
NCD4_metabuild(NCD4meta* metadata, int ncid)
{
    int ret = NC_NOERR;
    size_t i;

    metadata->ncid = ncid;
    metadata->root->meta.id = ncid;

    /* Fix up the atomic types */
    for(i=0;i<nclistlength(metadata->atomictypes);i++) {
	NCD4node* n = (NCD4node*)nclistget(metadata->atomictypes,i);
	if(n->sort != NCD4_TYPE) continue;
	if(n->subsort > NC_MAX_ATOMIC_TYPE) continue;
	n->meta.id = n->subsort;
        n->meta.isfixedsize = (n->subsort == NC_STRING ? 0 : 1);
	if(n->subsort <= NC_STRING)
	    n->meta.dapsize = NCD4_typesize(n->subsort);
	n->container = metadata->root;
    }

    /* Topo sort the set of all nodes */
    NCD4_toposort(metadata);
    markfixedsize(metadata);
    markdapsize(metadata);
    /* Process the metadata state */
    if((ret = build(metadata,metadata->root))) goto done;
    /* Done with the metadata*/
    if((ret=nc_enddef(metadata->ncid)))
	goto done;

done:
    return THROW(ret);
}

void
reclaimNode(NCD4node* node)
{
    if(node == NULL) return;
    nullfree(node->name); node->name = NULL;
    nclistfree(node->groups); node->groups = NULL;
    nclistfree(node->vars); node->vars = NULL;
    nclistfree(node->types); node->types = NULL;
    nclistfree(node->dims); node->dims = NULL;
    nclistfree(node->attributes); node->attributes = NULL;
    nclistfreeall(node->mapnames); node->mapnames = NULL;
    nclistfree(node->maps); node->maps = NULL;
    nclistfreeall(node->xmlattributes); node->xmlattributes = NULL;
    nclistfreeall(node->attr.values); node->attr.values = NULL;
    nclistfree(node->en.econsts); node->en.econsts = NULL;
    nclistfree(node->group.elements); node->group.elements = NULL;
    nullfree(node->group.dapversion); node->group.dapversion = NULL;
    nullfree(node->group.dmrversion); node->group.dmrversion = NULL;
    nullfree(node->group.datasetname); node->group.datasetname = NULL;
    nclistfree(node->group.varbyid); node->group.varbyid = NULL;
    nullfree(node->nc4.orig.name); node->nc4.orig.name = NULL;
    nullfree(node);
}

/**************************************************/

/* Recursively walk the tree to create the metadata */
static int
build(NCD4meta* builder, NCD4node* root)
{
    size_t i;
    int ret = NC_NOERR;
    size_t len = nclistlength(builder->allnodes);

    /* Tag the root group */
    savegroupbyid(builder,root);

    /* Compute the sizes for all type objects. Will of necessity
       compute the offsets for compound types as well
    */
    for(i=0;i<len;i++) {/* Walk in postfix order */
	NCD4node* x = (NCD4node*)nclistget(builder->allnodes,i);
	if(x->sort != NCD4_TYPE) continue;
	switch (x->subsort) {
	case NC_OPAQUE:
	case NC_ENUM:
	case NC_SEQ:
	default: /* Atomic */
	    x->meta.memsize = NCD4_computeTypeSize(builder,x);
	    x->meta.alignment = x->meta.memsize; /* Same for these cases */
	    break;
	case NC_STRUCT:
	    /* We need to compute the field offsets in order to compute the struct size */
	    computeOffsets(builder,x);
	    break;
        }
    }

    /* Start by defining  group tree separately so we can maintain
       order */
    if((ret=buildGroups(builder,root))) goto done;

    /* Now, walks selected other nodes to define the netcdf-4 substrate metadata */

    /* Walk and define the dimensions */
    for(i=0;i<len;i++) {/* Walk in postfix order */
	NCD4node* x = (NCD4node*)nclistget(builder->allnodes,i);
	if(x->sort != NCD4_DIM) continue;
        if((ret=buildDimension(builder,x)))
	    goto done;
    }

    /* Walk and define the enums */
    for(i=0;i<len;i++) {/* Walk in postfix order */
	NCD4node* x = (NCD4node*)nclistget(builder->allnodes,i);
	if(x->sort != NCD4_TYPE) continue;
	if(x->subsort != NC_ENUM) continue;
        if((ret=buildEnumeration(builder,x)))
	    goto done;
    }

    /* Walk and define the opaques */
#ifndef FIXEDOPAQUE
    /* If _bytestring was required by parser, then create it */
    if(builder->_bytestring != NULL && (ret = buildBytestringType(builder)))
	goto done;
#endif
    /* Create other opaque types */
    for(i=0;i<len;i++) {/* Walk in postfix order */
	NCD4node* x = (NCD4node*)nclistget(builder->allnodes,i);
	if(x->sort != NCD4_TYPE) continue;
	if(x->subsort != NC_OPAQUE) continue;
	if(x->opaque.size > 0 && (ret=buildOpaque(builder,x)))
	    goto done;
    }

    /* Walk and define the compounds and sequences */
    for(i=0;i<len;i++) {/* Walk in postfix order */
	NCD4node* x = (NCD4node*)nclistget(builder->allnodes,i);
	if(x->sort != NCD4_TYPE) continue;
	switch(x->subsort) {
	case NC_STRUCT:
	    if((ret=buildStructureType(builder,x)))
		goto done;
	    break;
	case NC_SEQ:
	    if((ret=buildVlenType(builder,x)))
		goto done;
	    break;
	default: /* ignore */ break;
	}
    }

    /* Compute the type size for all type */
    for(i=0;i<len;i++) {/* Walk in postfix order */
	NCD4node* x = (NCD4node*)nclistget(builder->allnodes,i);
	if(x->sort != NCD4_TYPE) continue;;
        NCD4_computeTypeSize(builder,x);
    }

    /* Finally, define the top-level variables */
    for(i=0;i<len;i++) {
	NCD4node* x = (NCD4node*)nclistget(builder->allnodes,i);
	if(ISVAR(x->sort) && ISTOPLEVEL(x)) {
	    if((ret=buildVariable(builder,x))) goto done;
	}
    }

done:
    return THROW(ret);
}

static int
buildGroups(NCD4meta* builder, NCD4node* parent)
{
    size_t i;
    int ret = NC_NOERR;
#ifdef D4DEBUG
    fprintf(stderr,"build group: %s\n",parent->name);
#endif
    /* Define any group level attributes */
    if((ret = buildAttributes(builder,parent))) goto done;

    for(i=0;i<nclistlength(parent->groups);i++) {
	NCD4node* g = (NCD4node*)nclistget(parent->groups,i);
        if(g->group.isdataset) {
	    g->meta.id = builder->ncid;
        } else {
	    NCCHECK((nc_def_grp(parent->meta.id,g->name,&g->meta.id)));
	    savegroupbyid(builder,g);
        }
	if((ret=buildGroups(builder,g))) goto done; /* recurse */
    }
done:
    return THROW(ret);
}

static int
buildDimension(NCD4meta* builder, NCD4node* dim)
{
    int ret = NC_NOERR;
    NCD4node* group = NCD4_groupFor(dim);
    if(dim->dim.isunlimited) {
	NCCHECK((nc_def_dim(group->meta.id,dim->name,NC_UNLIMITED,&dim->meta.id)));
    } else {
	NCCHECK((nc_def_dim(group->meta.id,dim->name,dim->dim.size,&dim->meta.id)));
    }
done:
    return THROW(ret);
}

static int
buildEnumeration(NCD4meta* builder, NCD4node* en)
{
    size_t i;
    int ret = NC_NOERR;
    NCD4node* group = NCD4_groupFor(en);
    NCCHECK((nc_def_enum(group->meta.id,en->basetype->meta.id,en->name,&en->meta.id)));
    for(i=0;i<nclistlength(en->en.econsts);i++) {
	NCD4node* ec = (NCD4node*)nclistget(en->en.econsts,i);
	NCCHECK((nc_insert_enum(group->meta.id, en->meta.id, ec->name, ec->en.ecvalue.i8)));
    }
done:
    return THROW(ret);
}

static int
buildOpaque(NCD4meta* builder, NCD4node* op)
{
    int ret = NC_NOERR;
    NCD4node* group = NCD4_groupFor(op);
    char* name  = op->name;

    assert(op->opaque.size > 0);
    /* Two cases, with and without UCARTAGORIGTYPE */
    if(op->nc4.orig.name != NULL) {
	name = op->nc4.orig.name;
	group = op->nc4.orig.group;
    }
    NCCHECK((nc_def_opaque(group->meta.id,op->opaque.size,name,&op->meta.id)));
done:
    return THROW(ret);
}

#ifndef FIXEDOPAQUE
static int
buildBytestringType(NCD4meta* builder)
{
    int ret = NC_NOERR;
    NCD4node* bstring = builder->_bytestring;

    assert(bstring != NULL); /* Will fail if we need bytestring and it was not created in d4parse*/

    /* Define once */
    if(bstring->meta.id > 0) goto done;
    /* create in root as ubyte(*) vlen named "_bytestring" */
    NCCHECK((nc_def_vlen(builder->root->meta.id,bstring->name,NC_UBYTE,&bstring->meta.id)));

done:
    return THROW(ret);
}
#endif

static int
buildVariable(NCD4meta* builder, NCD4node* var)
{
    int ret = NC_NOERR;

    switch (var->subsort) {
    default:
	if((ret = buildAtomicVar(builder,var))) goto done;
	break;
    case NC_STRUCT:
	if((ret = buildStructure(builder,var))) goto done;
	break;
    case NC_SEQ:
	if((ret = buildSequence(builder,var))) goto done;
	break;
    }
done:
    return THROW(ret);
}

static int
buildMetaData(NCD4meta* builder, NCD4node* var)
{
    int ret = NC_NOERR;
    if((ret = buildAttributes(builder,var))) goto done;
    if((ret = buildMaps(builder,var))) goto done;
done:
    return THROW(ret);
}

static int
buildMaps(NCD4meta* builder, NCD4node* var)
{
    size_t i;
    int ret = NC_NOERR;
    size_t count = nclistlength(var->maps);
    char** memory = NULL;
    char** p;
    NCD4node* group;

    if(count == 0) goto done;

    /* Add an attribute to the parent variable
       listing fqn's of all specified variables in map order*/
    memory = (char**)d4alloc(count*sizeof(char*));
    if(memory == NULL) {ret=NC_ENOMEM; goto done;}
    p = memory;
    for(i=0;i<count;i++) {
        NCD4node* mapref = (NCD4node*)nclistget(var->maps,i);
	char* fqn = NCD4_makeFQN(mapref);
        *p++ = fqn;
    }
    /* Make map info visible in the netcdf-4 file */
    group = NCD4_groupFor(var);
    NCCHECK((nc_put_att(group->meta.id,var->meta.id,NC4TAGMAPS,NC_STRING,count,memory)));
done:
    if(memory != NULL)
	freeStringMemory(memory,count);
    return THROW(ret);
}

static int
buildAttributes(NCD4meta* builder, NCD4node* varorgroup)
{
    size_t i;
    int ret = NC_NOERR;
    NClist* blobs = NULL;

    for(i=0;i<nclistlength(varorgroup->attributes);i++) {
	NCD4node* attr = nclistget(varorgroup->attributes,i);
	void* memory = NULL;
	size_t count = nclistlength(attr->attr.values);
	NCD4node* group;
        int varid;

	/* Suppress all UCARTAG attributes */
	if(strncmp(attr->name,UCARTAG,strlen(UCARTAG)) == 0)
	    continue;

	/* Suppress all reserved attributes */
	if(NCD4_lookupreserved(attr->name) != NULL)
	    continue;

	if(ISGROUP(varorgroup->sort))
	    varid = NC_GLOBAL;
	else
	    varid = varorgroup->meta.id;
	blobs = nclistnew();
        if((ret=compileAttrValues(builder,attr,&memory,blobs))) {
	    nullfree(memory);
            FAIL(ret,"Malformed attribute value(s) for: %s",attr->name);
        }
        if(memory != NULL) {
  	    group = NCD4_groupFor(varorgroup);
            NCCHECK((nc_put_att(group->meta.id,varid,attr->name,attr->basetype->meta.id,count,memory)));
	}
        nclistfreeall(blobs); blobs = NULL;
        nullfree(memory);
    }
done:
    nclistfreeall(blobs);
    return THROW(ret);
}

static int
buildStructureType(NCD4meta* builder, NCD4node* structtype)
{
    int tid,ret = NC_NOERR;
    NCD4node* group = NULL;
    char* name = NULL;

    group = NCD4_groupFor(structtype); /* default */

    /* Figure out the type name and containing group */
    if(structtype->nc4.orig.name != NULL) {
	name = strdup(structtype->nc4.orig.name);
	group = structtype->nc4.orig.group;
    } else {
        name = getFieldFQN(structtype,"_t");
    }

    /* Step 2: See if already defined */
    if(nc_inq_typeid(group->meta.id,name,&tid) == NC_NOERR) {/* Already exists */
	FAIL(NC_ENAMEINUSE,"Inferred type name conflict: %s",name);
    }

    /* Since netcdf does not support forward references,
       we presume all field types are defined */
    if((ret=buildCompound(builder,structtype,group,name))) goto done;

done:
    nullfree(name);
    return THROW(ret);
}

static int
buildVlenType(NCD4meta* builder, NCD4node* vlentype)
{
    int ret = NC_NOERR;
    NCD4node* group;
    NCD4node* basetype;
    nc_type tid = NC_NAT;
    char* name = NULL;

    group = NCD4_groupFor(vlentype);

    /* Figure out the type name and containing group */
    if(vlentype->nc4.orig.name != NULL) {
	name = strdup(vlentype->nc4.orig.name);
	group = vlentype->nc4.orig.group;
    } else {
        name = getFieldFQN(vlentype,NULL);
    }

    /* See if already defined */
    if(nc_inq_typeid(group->meta.id,name,&tid) == NC_NOERR) {/* Already exists */
	FAIL(NC_ENAMEINUSE,"Inferred type name conflict: %s",name);
    }

    /* Get the baseline type */
    basetype = vlentype->basetype;
    /* build the vlen type */
    NCCHECK(nc_def_vlen(group->meta.id, name, basetype->meta.id, &vlentype->meta.id));

done:
    nullfree(name);
    return THROW(ret);
}

static int
buildCompound(NCD4meta* builder, NCD4node* cmpdtype, NCD4node* group, char* name)
{
    size_t i;
    int ret = NC_NOERR;

    /* Step 1: compute field offsets */
    computeOffsets(builder,cmpdtype);

    /* Step 2: define this node's compound type */
    NCCHECK((nc_def_compound(group->meta.id,(size_t)cmpdtype->meta.memsize,name,&cmpdtype->meta.id)));

    /* Step 3: add the fields to type */
    for(i=0;i<nclistlength(cmpdtype->vars);i++) {
	int dimsizes[NC_MAX_VAR_DIMS];
        NCD4node* field = (NCD4node*)nclistget(cmpdtype->vars,i);
	size_t rank = nclistlength(field->dims);
        if(rank == 0) { /* scalar */
            NCCHECK((nc_insert_compound(group->meta.id, cmpdtype->meta.id,
					field->name, field->meta.offset,
					field->basetype->meta.id)));
        } else { /* array  */
  	    int idimsizes[NC_MAX_VAR_DIMS];
	    int j;
	    getDimsizes(field,dimsizes);
	    /* nc_insert_array_compound: dimsizes arg is not size_t */
	    for(j=0;j<rank;j++) idimsizes[j] = (int)dimsizes[j];
            NCCHECK((nc_insert_array_compound(group->meta.id, cmpdtype->meta.id,
					      field->name, field->meta.offset,
					      field->basetype->meta.id,
					      (int)rank, idimsizes)));
	}
    }

done:
    return THROW(ret);
}

static int
buildAtomicVar(NCD4meta* builder, NCD4node* var)
{
    int ret = NC_NOERR;
    size_t rank;
    int dimids[NC_MAX_VAR_DIMS];
    NCD4node* group;

    group = NCD4_groupFor(var);

#ifdef D4DEBUG
    fprintf(stderr,"build var: %s.%s\n",group->name,var->name); fflush(stderr);
#endif

    rank = getDimrefs(var,dimids);
    NCCHECK((nc_def_var(group->meta.id,var->name,var->basetype->meta.id,(int)rank,dimids,&var->meta.id)));
    /* Tag the var */
    savevarbyid(group,var);

    /* Build attributes and map attributes */
    if((ret = buildMetaData(builder,var))) goto done;
done:
    return THROW(ret);
}

static int
buildStructure(NCD4meta* builder, NCD4node* structvar)
{
    int ret = NC_NOERR;
    NCD4node* group;
    int dimids[NC_MAX_VAR_DIMS];

    /* Step 1: define the variable */
    size_t rank = nclistlength(structvar->dims);
    getDimrefs(structvar,dimids);
    group = NCD4_groupFor(structvar);
    NCCHECK((nc_def_var(group->meta.id,structvar->name,structvar->basetype->meta.id,(int)rank,dimids,&structvar->meta.id)));
    /* Tag the var */
    savevarbyid(group,structvar);

    /* Build attributes and map attributes WRT the variable */
    if((ret = buildMetaData(builder,structvar))) goto done;

done:
    return THROW(ret);
}

static int
buildSequence(NCD4meta* builder, NCD4node* seq)
{

    int ret = NC_NOERR;
    NCD4node* group;
    int dimids[NC_MAX_VAR_DIMS];

    size_t rank = nclistlength(seq->dims);
    getDimrefs(seq,dimids);
    group = NCD4_groupFor(seq);
    NCCHECK((nc_def_var(group->meta.id,seq->name,seq->basetype->meta.id,(int)rank,dimids,&seq->meta.id)));
    savevarbyid(group,seq);

    /* Build attributes and map attributes WRT the variable */
    if((ret = buildMetaData(builder,seq))) goto done;

done:
    return THROW(ret);
}

/***************************************************/
/* Utilities */

/* Insert a group into the groupbyid for a group */
static void
savegroupbyid(NCD4meta* meta, NCD4node* group)
{
    if(meta->groupbyid == NULL)
        meta->groupbyid = nclistnew();
    nclistsetalloc(meta->groupbyid,GROUPIDPART(group->meta.id));
    nclistinsert(meta->groupbyid,GROUPIDPART(group->meta.id),group);
}

/* Insert a var into the varbyid for a group */
static void
savevarbyid(NCD4node* group, NCD4node* var)
{
    if(group->group.varbyid == NULL)
        group->group.varbyid = nclistnew();
    nclistsetlength(group->group.varbyid, (size_t)var->meta.id+1);
    nclistinsert(group->group.varbyid, (size_t)var->meta.id,var);
}

/* Collect FQN path from var node up to and including
   the root group and create an name from it
*/
char*
NCD4_getVarFQN(NCD4node* var, const char* tail)
{
    size_t i;
    NCD4node* x = NULL;
    NClist* path = NULL;
    NCbytes* fqn =  NULL;
    char* result;

    path = nclistnew();
    for(x=var->container;ISGROUP(x->sort);x=x->container) {
	nclistinsert(path,0,x);
    }
    fqn = ncbytesnew();
    for(i=0;i<nclistlength(path);i++) {
	NCD4node* grp = (NCD4node*)nclistget(path,i);
	char* escaped = backslashEscape(grp->name);
	if(escaped == NULL) return NULL;
	if(i > 0) ncbytesappend(fqn,'/');
	ncbytescat(fqn,escaped);
	free(escaped);
    }
    nclistfree(path);
    if(tail != NULL)
        ncbytescat(fqn,tail);
    result = ncbytesextract(fqn);
    ncbytesfree(fqn);
    return result;
}

/* Collect FQN path from node up to (but not including)
   the first enclosing group and create an name from it
*/
static char*
getFieldFQN(NCD4node* field, const char* tail)
{
    size_t i;
    NCD4node* x = NULL;
    NClist* path = NULL;
    NCbytes* fqn =  NULL;
    char* result;

    path = nclistnew();
    for(x=field;!ISGROUP(x->sort);x=x->container) {
	nclistinsert(path,0,x);
    }
    fqn = ncbytesnew();
    for(i=0;i<nclistlength(path);i++) {
	NCD4node* elem = (NCD4node*)nclistget(path,i);
	char* escaped = backslashEscape(elem->name);
	if(escaped == NULL) return NULL;
	if(i > 0) ncbytesappend(fqn,'.');
	ncbytescat(fqn,escaped);
	free(escaped);
    }
    nclistfree(path);
    if(tail != NULL)
        ncbytescat(fqn,tail);
    result = ncbytesextract(fqn);
    ncbytesfree(fqn);
    return result;
}

static size_t
getDimrefs(NCD4node* var, int* dimids)
{
    size_t i;
    size_t rank = nclistlength(var->dims);
    for(i=0;i<rank;i++) {
	NCD4node* dim = (NCD4node*)nclistget(var->dims,i);
	dimids[i] = dim->meta.id;
    }
    return rank;
}

static size_t
getDimsizes(NCD4node* var, int* dimsizes)
{
    size_t i;
    size_t rank = nclistlength(var->dims);
    for(i=0;i<rank;i++) {
	NCD4node* dim = (NCD4node*)nclistget(var->dims,i);
	dimsizes[i] = (int)dim->dim.size;
    }
    return rank;
}

/**************************************************/
/* Utilities */

static void
freeStringMemory(char** mem, size_t count)
{
    if(mem == NULL) return;
    for(size_t i=0;i<count;i++) {
	char* p = mem[i];
        if(p) free(p);
    }
    free(mem);
}

/**
Convert a list of attribute value strings
into a memory chunk capable of being passed
to nc_put_att().
*/
static int
compileAttrValues(NCD4meta* builder, NCD4node* attr, void** memoryp, NClist* blobs)
{
    size_t i;
    int ret = NC_NOERR;
    unsigned char* memory = NULL;
    unsigned char* p;
    size_t size;
    NCD4node* truebase = NULL;
    union ATOMICS converter;
    int isenum = 0;
    NCD4node* container = attr->container;
    NCD4node* basetype = attr->basetype;
    NClist* values = attr->attr.values;
    size_t count = nclistlength(values);

    memset((void*)&converter,0,sizeof(converter));

    /* Deal with _FillValue */
    if(container->sort == NCD4_VAR && strcmp(attr->name,NC_FillValue)==0) {
	/* Verify or fix or ignore or fail on type mismatch */
	if(container->basetype != basetype) {/* _FillValue/Variable type mismatch */
	    int compatible = isfilltypecompatible(container->basetype, basetype);
	    /* Test type compatibility */
	    if(compatible && FLAGSET(builder->controller->controls.flags,NCF_FILLMISMATCH)) {
		/* Force type match */
		basetype = (attr->basetype = container->basetype);
	    } else if(FLAGSET(builder->controller->controls.flags,NCF_FILLMISMATCH_FAIL)) {/* Fail */
	        FAIL(NC_EBADTYPE,"_FillValue/Variable type mismatch: %s:%s",container->name,attr->name);
	    } else
	        goto done; /* ignore */
	}
    }  
    isenum = (basetype->subsort == NC_ENUM);
    truebase = (isenum ? basetype->basetype : basetype);
    if(!ISTYPE(truebase->sort) || (truebase->meta.id > NC_MAX_ATOMIC_TYPE))
        FAIL(NC_EBADTYPE,"Illegal attribute type: %s",basetype->name);
    size = NCD4_typesize(truebase->meta.id);
    if((memory = (unsigned char*)d4alloc(count*size))==NULL)
        return THROW(NC_ENOMEM);
    p = memory;
    for(i=0;i<count;i++) {
        char* s = (char*)nclistget(values,i);
        if(isenum) {
            if((ret=decodeEconst(builder,basetype,s,&converter)))
                FAIL(ret,"Illegal enum const: ",s);
        } else {
            if((ret = convertString(&converter,basetype,s)))
            FAIL(NC_EBADTYPE,"Illegal attribute type: ",basetype->name);
        }
        ret = downConvert(&converter,truebase);
        p = copyAtomic(&converter,truebase->meta.id,NCD4_typesize(truebase->meta.id),p,blobs);
    }
    if(memoryp) *memoryp = memory;
done:
    return THROW(ret);
}

static void*
copyAtomic(union ATOMICS* converter, nc_type type, size_t len, void* dst, NClist* blobs)
{
    switch (type) {
    case NC_CHAR: case NC_BYTE: case NC_UBYTE:
        memcpy(dst,&converter->u8[0],len); break;
    case NC_SHORT: case NC_USHORT:
        memcpy(dst,&converter->u16[0],len); break;
    case NC_INT: case NC_UINT:
        memcpy(dst,&converter->u32[0],len); break;
    case NC_INT64: case NC_UINT64:
        memcpy(dst,&converter->u64[0],len); break;
    case NC_FLOAT:
        memcpy(dst,&converter->f32[0],len); break;
    case NC_DOUBLE:
        memcpy(dst,&converter->f64[0],len); break;
    case NC_STRING:
        memcpy(dst,&converter->s[0],len);
	nclistpush(blobs,converter->s[0]);
        converter->s[0] = NULL; /* avoid duplicate free */
	break;
    }/*switch*/
    return (((char*)dst)+len);
}

static int
convertString(union ATOMICS* converter, NCD4node* type, const char* s)
{
    switch (type->subsort) {
    case NC_BYTE:
    case NC_SHORT:
    case NC_INT:
    case NC_INT64:
	if(sscanf(s,"%lld",&converter->i64[0]) != 1) return THROW(NC_ERANGE);
	break;
    case NC_UBYTE:
    case NC_USHORT:
    case NC_UINT:
    case NC_UINT64:
	if(sscanf(s,"%llu",&converter->u64[0]) != 1) return THROW(NC_ERANGE);
	break;
    case NC_FLOAT:
    case NC_DOUBLE:
	if(sscanf(s,"%lf",&converter->f64[0]) != 1) return THROW(NC_ERANGE);
	break;
    case NC_CHAR:
#ifdef WORDS_BIGENDIAN
	converter->i8[7] = s[0];
#else
	converter->i8[0] = s[0];
#endif
	break;
    case NC_STRING:
	converter->s[0]= strdup(s);
	break;
    }/*switch*/
    return downConvert(converter,type);
}

static int
downConvert(union ATOMICS* converter, NCD4node* type)
{
    d4size_t u64 = converter->u64[0];
    long long i64 = converter->i64[0];
    double f64 = converter->f64[0];
    char* s = converter->s[0];
    switch (type->subsort) {
    case NC_CHAR:
    case NC_BYTE:
	converter->i8[0] = (char)i64;
	break;
    case NC_UBYTE:
	converter->u8[0] = (unsigned char)u64;
	break;
    case NC_SHORT:
	converter->i16[0] = (short)i64;
	break;
    case NC_USHORT:
	converter->u16[0] = (unsigned short)u64;
	break;
    case NC_INT:
	converter->i32[0] = (int)i64;
	break;
    case NC_UINT:
	converter->u32[0] = (unsigned int)u64;
	break;
    case NC_INT64:
	converter->i64[0] = i64;
	break;
    case NC_UINT64:
	converter->u64[0]= u64;
	break;
    case NC_FLOAT:
	converter->f32[0] = (float)f64;
	break;
    case NC_DOUBLE:
	converter->f64[0] = f64;
	break;
    case NC_STRING:
	converter->s[0]= s;
	break;
    }/*switch*/
    return THROW(NC_NOERR);
}

/*
Given an enum type, and a string representing an econst,
convert to integer.
Note: this will work if the econst string is a number or a econst name
*/
static int
decodeEconst(NCD4meta* builder, NCD4node* enumtype, const char* nameorval, union ATOMICS* converter)
{
    size_t i;
    int ret = NC_NOERR;
    union ATOMICS number = {0};
    NCD4node* match = NULL;

    /* First, see if the value is an econst name */
    for(i=0;i<nclistlength(enumtype->en.econsts);i++) {
        NCD4node* ec = (NCD4node*)nclistget(enumtype->en.econsts,i);
        if(strcmp(ec->name,nameorval)==0) {match = ec; break;}
    }
    /* If no match, try to invert as a number to see if there is a matching econst */
    if(!match && enumtype->en.econsts) {
        /* get the incoming value as number */
        if((ret=convertString(&number,enumtype->basetype,nameorval)))
            goto done;
        for(i=0;i<nclistlength(enumtype->en.econsts);i++) {
            NCD4node* ec = (NCD4node*)nclistget(enumtype->en.econsts,i);
            if(ec->en.ecvalue.u64[0] == number.u64[0]) {match = ec; break;}
        }
    }
    if(match == NULL)
        FAIL(NC_EINVAL,"No enum const matching value: %s",nameorval);
    if(converter) *converter = match->en.ecvalue;
done:
    return THROW(ret);
}

static char*
backslashEscape(const char* s)
{
    const char* p;
    char* q;
    size_t len;
    char* escaped = NULL;

    len = strlen(s);
    escaped = (char*)d4alloc(1+(2*len)); /* max is everychar is escaped */
    if(escaped == NULL) return NULL;
    for(p=s,q=escaped;*p;p++) {
        char c = *p;
        switch (c) {
        case '\\':
        case '/':
        case '.':
        case '@':
            *q++ = '\\'; *q++ = '\\';
            break;
        default: *q++ = c; break;
        }
    }
    *q = '\0';
    return escaped;
}

/* Tag each compound type as fixed size or not
   Assumes:
	- atomic types defined and marked
	- topo sorted
*/

static int
markfixedsize(NCD4meta* meta)
{
    size_t i,j;
    for(i=0;i<nclistlength(meta->allnodes);i++) {
	int fixed = 1;
	NCD4node* n = (NCD4node*)nclistget(meta->allnodes,i);
	if(n->sort != NCD4_TYPE) continue;
	switch (n->subsort) {
	case NC_STRUCT:
            for(j=0;j<nclistlength(n->vars);j++) {
                NCD4node* field = (NCD4node*)nclistget(n->vars,j);
	        if(!field->basetype->meta.isfixedsize) {
		    fixed = 0;
		    break;
	        }
	    }
	    n->meta.isfixedsize = fixed;
	    break;
	case NC_ENUM:
	    n->meta.isfixedsize = 1;
	    break;
	default: /* leave as is */
	    break;
	}
    }
    return NC_NOERR;
}

/* Compute compound type field offsets and compound type total size */
static void
computeOffsets(NCD4meta* builder, NCD4node* cmpd)
{
    size_t i;
    d4size_t offset = 0;
    d4size_t largestalign = 1;
    d4size_t size = 0;

    for(i=0;i<nclistlength(cmpd->vars);i++) {
	NCD4node* field = (NCD4node*)nclistget(cmpd->vars,i);
	NCD4node* ftype = field->basetype;
	size_t alignment;
	if(ftype->subsort == NC_STRUCT) {
	    /* Recurse */
	    computeOffsets(builder, ftype);
	    assert(ftype->meta.memsize > 0);
	    size=ftype->meta.memsize;
	    alignment = ftype->meta.alignment;
	} else {/* Size and alignment will already have been set */
	    assert(ftype->meta.memsize > 0);
	    alignment = ftype->meta.alignment;
            size=ftype->meta.memsize;
	}
#if 0
	} else if(ftype->subsort == NC_SEQ) { /* VLEN */
	    alignment = nctypealignment(NC_VLEN);
	    assert(ftype->meta.memsize > 0); size=ftype->meta.memsize;
	    /*size = NCD4_computeTypeSize(builder,ftype);*/
	} else if(ftype->subsort == NC_OPAQUE) {
	    /* Either fixed or a vlen */
	    assert(ftype->meta.memsize > 0); size=ftype->meta.memsize;
	    if(ftype->opaque.size == 0) {/* treat like vlen */
	        alignment = nctypealignment(NC_VLEN);
	        /*size = NCD4_computeTypeSize(builder,ftype);*/
	    } else { /* fixed size */
	        alignment = nctypealignment(NC_OPAQUE);
	        /*size = NCD4_computeTypeSize(builder,ftype);*/
	    }
	} else if(ftype->subsort == NC_ENUM) {
	    NCD4node* truetype = ftype->basetype;
	    alignment = nctypealignment(truetype->meta.id);
	    assert(ftype->meta.memsize > 0); size=ftype->meta.memsize;
	    /*size = NCD4_computeTypeSize(builder,truetype);*/
	} else { /* Basically a primitive */
	    alignment = nctypealignment(ftype->meta.id);
	    assert(ftype->meta.memsize > 0); size=ftype->meta.memsize;
	    /*size = NCD4_computeTypeSize(builder,ftype);*/
	}
#endif
        if(alignment > largestalign)
	    largestalign = alignment;
	/* Add possible padding wrt to previous field */
	offset += getpadding(offset,alignment);
	field->meta.offset = offset;
	assert(ftype->meta.memsize > 0);
	size = ftype->meta.memsize;
#if 0
	field->meta.memsize = size;
#endif
	/* Now ultiply by the field dimproduct*/
	if(nclistlength(field->dims) > 0) {
            d4size_t count = NCD4_dimproduct(field);
	    size = (size * count);
	}
	offset += size;
    }
    /* Compute compound-level info */
    /* A struct alignment is the same as largestalign */
    cmpd->meta.alignment = largestalign;
    offset += (offset % largestalign); /* round up compound size */
    cmpd->meta.memsize = offset;
}

/*
Compute the in-memory size of an instance of a type.
Note that nc_inq_type is used, so that C struct field
alignment is taken into account for compound types.
The variables total size will be this * dimproduct.
*/
size_t
NCD4_computeTypeSize(NCD4meta* builder, NCD4node* type)
{
    size_t size = 0;

    switch (type->sort) {
    case NCD4_TYPE:
	switch (type->subsort) {
	default: size = NCD4_typesize(type->meta.id); break;
        case NC_OPAQUE:
            size = (type->opaque.size == 0 ? sizeof(nc_vlen_t) : type->opaque.size);
  	    break;
        case NC_ENUM:
   	    size = NCD4_computeTypeSize(builder,type->basetype);
	    break;
        case NC_SEQ:
	    size = sizeof(nc_vlen_t);
	    break;
        case NC_STRUCT: {
	    int ret;
	    NCD4node* group = NCD4_groupFor(type);
	    if((ret = nc_inq_type(group->meta.id,type->meta.id,/*name*/NULL,&size)))
		return 0;
	    }; break;
        }
        break;
    default: break; /* ignore */
    }
    type->meta.memsize = size;
    return size;
}

static d4size_t
getpadding(d4size_t offset, size_t alignment)
{
    d4size_t rem = (alignment==0?0:(offset % alignment));
    d4size_t pad = (rem==0?0:(alignment - rem));
    return pad;
}

/* Compute the dap data size for each type; note that this
   is unlikely to be the same as the meta.memsize unless
   the type is atomic and is <= NC_UINT64.
*/

static int
markdapsize(NCD4meta* meta)
{
    size_t i,j;
    for(i=0;i<nclistlength(meta->allnodes);i++) {
	NCD4node* type = (NCD4node*)nclistget(meta->allnodes,i);
	size_t totalsize;
	if(type->sort != NCD4_TYPE) continue;
	switch (type->subsort) {
	case NC_STRUCT:
	    totalsize = 0;
            for(j=0;j<nclistlength(type->vars);j++) {
                NCD4node* field = (NCD4node*)nclistget(type->vars,j);
		size_t size = field->basetype->meta.dapsize;
	        if(size == 0) {
		    totalsize = 0;
		    break;
	        } else
		    totalsize += size;
	    }
	    type->meta.dapsize = totalsize;
	    break;
	case NC_SEQ:
	    type->meta.dapsize = 0; /* has no fixed size */
	    break;
	case NC_OPAQUE:
	    type->meta.dapsize = type->opaque.size;
	    break;
	case NC_ENUM:
	    type->meta.dapsize = type->basetype->meta.dapsize;
	    break;
	case NC_STRING:
	    type->meta.dapsize = 0; /* has no fixed size */
	    break;
	default:
	    assert(type->subsort <= NC_UINT64);
	    /* Already assigned */
	    break;
	}
    }
    return NC_NOERR;
}

int
NCD4_findvar(NC* ncp, int gid, int varid, NCD4node** varp, NCD4node** grpp)
{
    int ret = NC_NOERR;
    NCD4INFO* info = NULL;
    NCD4meta* meta = NULL;
    NCD4node* var = NULL;
    NCD4node* group = NULL;
    int grp_id;

    info = getdap(ncp);
    if(info == NULL)
	return THROW(NC_EBADID);
    meta = info->dmrmetadata;
    if(meta == NULL)
	return THROW(NC_EBADID);
    /* Locate var node via (grpid,varid) */
    grp_id = GROUPIDPART(gid);
    group = nclistget(meta->groupbyid, (size_t)grp_id);
    if(group == NULL)
	return THROW(NC_EBADID);
    var = nclistget(group->group.varbyid, (size_t)varid);
    if(var == NULL)
	return THROW(NC_EBADID);
    if(varp) *varp = var;
    if(grpp) *grpp = group;
    return THROW(ret);
}

static int
isnumerictype(int typesort)
{
    switch (typesort) {
    case NC_BYTE:
    case NC_SHORT:
    case NC_INT:
    case NC_FLOAT:
    case NC_DOUBLE:
    case NC_UBYTE:
    case NC_USHORT:
    case NC_UINT:
    case NC_INT64:
    case NC_UINT64:
	return 1;	
    default: break;
    }
    return 0;
}

static int
isfilltypecompatible(NCD4node* parenttype, NCD4node* attrtype)
{
    assert(parenttype->sort == NCD4_TYPE && attrtype->sort == NCD4_TYPE);
    if(isnumerictype(parenttype->subsort) && isnumerictype(attrtype->subsort))
        return 1;
    if(parenttype->subsort == NC_ENUM && attrtype->subsort == NC_STRING)
        return 1;
    return 0;
}
