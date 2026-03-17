/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

/* Parser actions for constraint expressions */

/* Since oc does not use the constraint parser,
   they functions all just abort if called.
*/

#include "config.h"
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "netcdf.h"

#include "nclist.h"
#include "ncbytes.h"
#include "dceconstraints.h"
#include "dceparselex.h"

static Object collectlist(Object list0, Object decl);

void
projections(DCEparsestate* state, Object list0)
{
    NClist* list = (NClist*)list0;
    if(list != NULL) {
        nclistfree(state->constraint->projections);
        state->constraint->projections = list;
    }
#ifdef DEBUG
fprintf(stderr,"	ce.projections: %s\n",
	dcetostring((DCEnode*)state->constraint->projections));
#endif
}

void
selections(DCEparsestate* state, Object list0)
{
    NClist* list = (NClist*)list0;
    if(list != NULL) {
        nclistfree(state->constraint->selections);
        state->constraint->selections = list;
    }
#ifdef DEBUG
fprintf(stderr,"	ce.selections: %s\n",
	dcetostring((DCEnode*)state->constraint->selections));
#endif
}


Object
projectionlist(DCEparsestate* state, Object list0, Object decl)
{
    return collectlist(list0,decl);
}

Object
projection(DCEparsestate* state, Object varorfcn)
{
    DCEprojection* p = (DCEprojection*)dcecreate(CES_PROJECT);
    CEsort tag = *(CEsort*)varorfcn;
    if(tag == CES_FCN)
	p->fcn = varorfcn;
    else
	p->var = varorfcn;
    p->discrim = tag;
#ifdef DEBUG
fprintf(stderr,"	ce.projection: %s\n",
	dcetostring((DCEnode*)p));
#endif
    return p;
}

Object
segmentlist(DCEparsestate* state, Object var0, Object decl)
{
    /* watch out: this is non-standard */
    NClist* list;
    DCEvar* v = (DCEvar*)var0;
    if(v==NULL) v = (DCEvar*)dcecreate(CES_VAR);
    list = v->segments;
    if(list == NULL) list = nclistnew();
    nclistpush(list,(void*)decl);
    v->segments = list;
    return v;
}

Object
segment(DCEparsestate* state, Object name, Object slices0)
{
    size_t i;
    DCEsegment* segment = (DCEsegment*)dcecreate(CES_SEGMENT);
    NClist* slices = (NClist*)slices0;
    segment->name = strdup((char*)name);
    if(slices != NULL && nclistlength(slices) > 0) {
	segment->rank = nclistlength(slices);
        segment->slicesdefined = 1; /* but not declsizes */
	for(i=0;i<nclistlength(slices);i++) {
	    DCEslice* slice = (DCEslice*)nclistget(slices,i);
	    segment->slices[i] = *slice;
	    free(slice);
	}
	nclistfree(slices);
    } else
        segment->slicesdefined = 0;
#ifdef DEBUG
fprintf(stderr,"	ce.segment: %s\n",
	dumpsegment(segment));
#endif
    return segment;
}


Object
rangelist(DCEparsestate* state, Object list0, Object decl)
{
    return collectlist(list0,decl);
}

Object
dap2_range(DCEparsestate* state, Object sfirst, Object sstride, Object slast)
{
    DCEslice* slice = (DCEslice*)dcecreate(CES_SLICE);
    unsigned long first=0,stride=0,last=0;

    /* Note: that incoming arguments are strings; we must convert to size_t;
       but we do know they are legal integers or NULL */
    if(sscanf((char*)sfirst,"%lu",&first) != 1) /* always defined */
	return NULL;
    if(slast != NULL) {
        if(sscanf((char*)slast,"%lu",&last) != 1)
	    return NULL;
    } else
	last = first;
    if(sstride != NULL) {
        if(sscanf((char*)sstride,"%lu",&stride) != 1)
	    return NULL;
    } else
	stride = 1; /* default */

    if(stride == 0)
    	dceerror(state,"Illegal index for dap2_range stride");
    if(last < first)
	dceerror(state,"Illegal index for dap2_range last index");
    slice->first  = first;
    slice->stride = (stride == 0 ? 1 : stride);
    slice->last   = last;
    slice->length  = (slice->last - slice->first) + 1;
    slice->count  = slice->length / slice->stride;
#ifdef DEBUG
fprintf(stderr,"	ce.slice: %s\n",
	dumpslice(slice));
#endif
    return slice;
}

Object
range1(DCEparsestate* state, Object rangenumber)
{
    int dap2_range = -1;
    if(sscanf((char*)rangenumber,"%u",&dap2_range) != 1)
	dap2_range = -1;
    if(dap2_range < 0) {
    	dceerror(state,"Illegal dap2_range index");
    }
    return rangenumber;
}

Object
clauselist(DCEparsestate* state, Object list0, Object decl)
{
    return collectlist(list0,decl);
}

Object
sel_clause(DCEparsestate* state, int selcase,
	   Object lhs, Object relop0, Object values)
{
    DCEselection* sel = (DCEselection*)dcecreate(CES_SELECT);
    sel->operator = (CEsort)relop0;
    sel->lhs = (DCEvalue*)lhs;
    if(selcase == 2) {/*singleton value*/
	sel->rhs = nclistnew();
	nclistpush(sel->rhs,(void*)values);
    } else
        sel->rhs = (NClist*)values;
    return sel;
}

Object
indexpath(DCEparsestate* state, Object list0, Object index)
{
    return collectlist(list0,index);
}

Object
array_indices(DCEparsestate* state, Object list0, Object indexno)
{
    DCEslice* slice;
    long long start = -1;
    NClist* list = (NClist*)list0;
    if(list == NULL) list = nclistnew();
    if(sscanf((char*)indexno,"%lld",&start) != 1)
	start = -1;
    if(start < 0) {
    	dceerror(state,"Illegal array index");
	start = 1;
    }
    slice = (DCEslice*)dcecreate(CES_SLICE);
    slice->first = (size_t)start;
    slice->stride = 1;
    slice->length = 1;
    slice->last = (size_t)start;
    slice->count = 1;
    nclistpush(list,(void*)slice);
    return list;
}

Object
indexer(DCEparsestate* state, Object name, Object indices)
{
    size_t i;
    NClist* list = (NClist*)indices;
    DCEsegment* seg = (DCEsegment*)dcecreate(CES_SEGMENT);
    seg->name = strdup((char*)name);
    for(i=0;i<nclistlength(list);i++) {
	DCEslice* slice = (DCEslice*)nclistget(list,i);
        seg->slices[i] = *slice;
	free(slice);
    }
    nclistfree(indices);
    return seg;
}

Object
function(DCEparsestate* state, Object fcnname, Object args)
{
    DCEfcn* fcn = (DCEfcn*)dcecreate(CES_FCN);
    fcn->name = nulldup((char*)fcnname);
    fcn->args = args;
    return fcn;
}

Object
arg_list(DCEparsestate* state, Object list0, Object decl)
{
    return collectlist(list0,decl);
}


Object
value_list(DCEparsestate* state, Object list0, Object decl)
{
    return collectlist(list0,decl);
}

Object
value(DCEparsestate* state, Object val)
{
    DCEvalue* ncvalue = (DCEvalue*)dcecreate(CES_VALUE);
    CEsort tag = *(CEsort*)val;
    switch (tag) {
    case CES_VAR: ncvalue->var = (DCEvar*)val; break;
    case CES_FCN: ncvalue->fcn = (DCEfcn*)val; break;
    case CES_CONST: ncvalue->constant = (DCEconstant*)val; break;
    default: abort(); break;
    }
    ncvalue->discrim = tag;
    return ncvalue;
}

Object
var(DCEparsestate* state, Object indexpath)
{
    DCEvar* v = (DCEvar*)dcecreate(CES_VAR);
    v->segments = (NClist*)indexpath;
    return v;
}

Object
constant(DCEparsestate* state, Object val, int tag)
{
    DCEconstant* con = (DCEconstant*)dcecreate(CES_CONST);
    char* text = (char*)val;
    char* endpoint = NULL;
    switch (tag) {
    case SCAN_STRINGCONST:
	con->discrim = CES_STR;
	con->text = nulldup(text);
	break;
    case SCAN_NUMBERCONST:
	con->intvalue = strtoll(text,&endpoint,10);
	if(*text != '\0' && *endpoint == '\0') {
	    con->discrim = CES_INT;
	} else {
	    con->floatvalue = strtod(text,&endpoint);
	    if(*text != '\0' && *endpoint == '\0')
	        con->discrim = CES_FLOAT;
	    else abort();
	}
	break;
    default: abort(); break;
    }
    return con;
}

static Object
collectlist(Object list0, Object decl)
{
    NClist* list = (NClist*)list0;
    if(list == NULL) list = nclistnew();
    nclistpush(list,(void*)decl);
    return list;
}

Object
makeselectiontag(CEsort tag)
{
    return (Object) tag;
}

int
dceerror(DCEparsestate* state, char* msg)
{
  strncpy(state->errorbuf,msg,1023);
  state->errorcode=NC_EDAPCONSTRAINT;
  return 0;
}

static void
dce_parse_cleanup(DCEparsestate* state)
{
    if(state == NULL) return;
    dcelexcleanup(&state->lexstate); /* will free */
    /* Free up the state */
    free(state);
}

static DCEparsestate*
ce_parse_init(char* input, DCEconstraint* constraint)
{
  DCEparsestate* state = (DCEparsestate*)calloc(1,sizeof(DCEparsestate));;
  if(state==NULL) return (DCEparsestate*)NULL;

  if(input==NULL) {
    dceerror(state,"ce_parse_init: no input buffer");
  } else {
    state->errorbuf[0] = '\0';
    state->errorcode = 0;
    dcelexinit(input,&state->lexstate);
	state->constraint = constraint;
  }
  return state;
}

#ifdef PARSEDEBUG
extern int dcedebug;
#endif

/* Wrapper for ceparse */
int
dapceparse(char* input, DCEconstraint* constraint, char** errmsgp)
{
    DCEparsestate* state;
    int errcode = 0;

#ifdef PARSEDEBUG
dcedebug = 1;
#endif

    if(input != NULL) {
#ifdef DEBUG
fprintf(stderr,"dceeparse: input=%s\n",input);
#endif
        state = ce_parse_init(input,constraint);
        if(dceparse(state) == 0) {
#ifdef DEBUG
if(nclistlength(constraint->projections) > 0)
fprintf(stderr,"dceeparse: projections=%s\n",
        dcetostring((DCEnode*)constraint->projections));
#endif
#ifdef DEBUG
if(nclistlength(constraint->selections)  > 0)
fprintf(stderr,"dceeparse: selections=%s\n",
	dumpselections(constraint->selections));
#endif
	} else {
	    if(errmsgp) *errmsgp = nulldup(state->errorbuf);
	}
	errcode = state->errorcode;
        dce_parse_cleanup(state);
    }
    return errcode;
}

#ifdef PARSEDEBUG
Object
debugobject(Object o)
{
    return o;
}
#endif
