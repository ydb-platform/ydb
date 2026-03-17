/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef OCCONSTRAINTS_H
#define OCCONSTRAINTS_H 1

/*! Specifies an OCslice. */
typedef struct OCslice {
    size_t first;
    size_t count;
    size_t stride;
    size_t stop; /* == first + count */
    size_t declsize;  /* from defining dimension, if any.*/
} OCslice;

/*! Specifies a form of path where each element can have a set of associated indices */
typedef struct OCpath {
    NClist* names;
    NClist* indexsets; /* oclist<oclist<Slice>> */
} OCpath;

/*! Specifies a ProjectionClause. */
typedef struct OCprojectionclause {
    char* target; /* "variable name" as mentioned in the projection */
    NClist* indexsets; /* oclist<oclist<OCslice>> */
    struct OCnode* node; /* node with name matching target, if any. */
    int    gridconstraint; /*  used only for testing purposes */
} OCprojectionclause;

/*! Selection is the node type for selection expression trees */
typedef struct OCselectionclause {
    int op;
    char* value;
    struct OCselectionclause* lhs;
    NClist* rhs;
} OCselectionclause;


#endif /*OCCONSTRAINTS_H*/
