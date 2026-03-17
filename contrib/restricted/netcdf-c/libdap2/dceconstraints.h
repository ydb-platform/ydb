/*********************************************************************
  *   Copyright 2018, UCAR/Unidata
  *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
  *********************************************************************/

#ifndef DCECONSTRAINTS_H
#define DCECONSTRAINTS_H 1

#include "ceconstraints.h"

/* Provide a universal cast type containing common fields */

/* Define the common "supertype */
typedef struct DCEnode {
    CEsort sort;    
} DCEnode;

/* The slice structure is assumed common to all uses.
   It is initially established from [first:stride:last]
*/
typedef struct DCEslice {
    DCEnode node;
    size_t first; 
    size_t stride;
    size_t length;
    size_t last;   /* first + length - 1*/
    size_t count;  /* (length + (stride-1))/ stride == actual # of elements returned to client*/
    size_t declsize;  /* from defining dimension, if any.*/
} DCEslice;

typedef struct DCEsegment {
    DCEnode node;
    char* name; 
    int slicesdefined; /*1=>slice counts defined, except declsize*/
    int slicesdeclized; /*1=>slice declsize defined */
    size_t rank;
    DCEslice slices[NC_MAX_VAR_DIMS];    
    void* annotation;
} DCEsegment;

typedef struct DCEfcn {
    DCEnode node;
    char* name; 
    NClist* args;
} DCEfcn;

typedef struct DCEvar {
    DCEnode node;
    NClist* segments;
    void* annotation;
} DCEvar;

typedef struct DCEconstant {
    DCEnode node;
    CEsort discrim;
    char* text;
    long long intvalue;
    double floatvalue;
} DCEconstant;

typedef struct DCEvalue {
    DCEnode node;
    CEsort discrim;
    /* Do not bother with a union */
    DCEconstant* constant;
    DCEvar* var;
    DCEfcn* fcn;
} DCEvalue;

typedef struct DCEselection {
    DCEnode node;
    CEsort operator;
    DCEvalue* lhs;
    NClist* rhs;
} DCEselection;

typedef struct DCEprojection {
    DCEnode node;
    CEsort discrim;
    /* Do not bother with a union */
    DCEvar* var;
    DCEfcn* fcn;
} DCEprojection;

typedef struct DCEconstraint {
    DCEnode node;
    NClist* projections;
    NClist* selections;
} DCEconstraint;


extern int dceparseconstraints(char* constraints, DCEconstraint* DCEonstraint);
extern int dceslicecompose(DCEslice* s1, DCEslice* s2, DCEslice* sr);
extern int dcemergeprojectionlists(NClist* dst, NClist* src);

extern DCEnode* dceclone(DCEnode* node);
extern NClist* dceclonelist(NClist* list);

extern void dcefree(DCEnode* node);
extern void dcefreelist(NClist* list);

extern char* dcetostring(DCEnode* node);
extern char* dcelisttostring(NClist* list,char*);
extern void dcetobuffer(DCEnode* node, NCbytes* buf);
extern void dcelisttobuffer(NClist* list, NCbytes* buf,char*);
extern char* dcerawtostring(void*);
extern char* dcerawlisttostring(NClist*);

extern NClist* dceallnodes(DCEnode* node, CEsort which);

extern DCEnode* dcecreate(CEsort sort);

extern void dcemakewholeslice(DCEslice* slice, size_t declsize);
extern void dcemakewholeprojection(DCEprojection*);

extern int dceiswholesegment(DCEsegment*);
extern int dceiswholeslice(DCEslice*);
extern int dceiswholeseglist(NClist*);
extern int dceiswholeprojection(DCEprojection*);
extern int dcesamepath(NClist* list1, NClist* list2);
extern int dcemergeprojections(DCEprojection* dst, DCEprojection* src);

extern void dcesegment_transpose(DCEsegment* seg,
				 size_t* start,
				 size_t* count,
				 size_t* stride,
				 size_t* sizes
				);


/* Find leftmost index of segment slices
   s.t. that index and all up to last
   satisfy dceiswholeslice
*/
extern size_t dcesafeindex(DCEsegment* seg, size_t start, size_t stop);
   
/* Compute segment size for start up to stop */
extern size_t dcesegmentsize(DCEsegment*, size_t start, size_t stop);

/* Convert a DCE projection/selection/constraint instance into a string
   that can be used with a url. Caller must free returned string.
*/

extern char* dcebuildprojectionstring(NClist* projections);
extern char* dcebuildselectionstring(NClist* selections);
extern char* dcebuildconstraintstring(DCEconstraint* constraints);

extern int dceverbose;

#endif /*DCECONSTRAINTS_H*/
