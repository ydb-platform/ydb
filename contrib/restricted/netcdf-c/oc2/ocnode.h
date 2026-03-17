/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef OCNODE_H
#define OCNODE_H

/*! Specifies the Diminfo. */
/* Track info purely about declared dimensions.
   More information is included in the Dimdata structure (dim.h)
*/
typedef struct OCdiminfo {
    struct OCnode* array;   /* defining array node (if known)*/
    size_t arrayindex;	    /* rank position ofthis dimension in the array*/
    size_t declsize;	    /* from DDS*/
} OCdiminfo;

/*! Specifies the Arrayinfo.*/
typedef struct OCarrayinfo {
    /* The complete set of dimension info applicable to this node*/
    NClist*  dimensions;
    /* convenience (because they are computed so often*/
    size_t rank; /* == |dimensions|*/
    size_t* sizes;
} OCarrayinfo;

/*! Specifies Attribute info */
typedef struct OCattribute {
    char*   name;
    OCtype etype; /* type of the attribute */
    size_t  nvalues;
    char**  values;  /* |values| = nvalues*sizeof(char**)*/
} OCattribute;

/*! Specifies the Attinfo.*/
/* This is the form as it comes out of the DAS parser*/
typedef struct OCattinfo {
    int isglobal; /* is this supposed to be a global attribute set?*/
    int isdods;   /* is this a global DODS_XXX  attribute set */
    NClist* values; /* oclist<char*>*/
    struct OCnode* var; /* containing var else null */
} OCattinfo;

/*! Specifies the OCnode. */
struct OCnode {
    OCheader header; /* class=OC_Node */
    OCtype	    octype;
    OCtype          etype; /* essentially the dap type from the dds*/
    char*           name;
    char*           fullname;
    struct OCnode*  container; /* this node is subnode of container */
    struct OCnode*  root;      /* root node of tree containing this node */
    struct OCtree*  tree;      /* !NULL iff this is a root node */
    struct OCnode*  datadds;   /* correlated datadds node, if any */
    OCdiminfo       dim;       /* octype == OC_Dimension*/
    OCarrayinfo     array;     /* octype == {OC_Structure, OC_Primitive}*/
    OCattinfo       att;       /* octype == OC_Attribute */
    /* primary edge info*/
    NClist* subnodes; /*oclist<OCnode*>*/
    /*int     attributed;*/ /* 1 if merge was done*/
    NClist* attributes; /* oclist<OCattribute*>*/
    OCdata* data; /* Defined only if this node is a top-level atomic variable*/
};

#if SIZEOF_SIZE_T == 4
#define OCINDETERMINATE  ((size_t)0xffffffff)
#endif
#if SIZEOF_SIZE_T == 8
#define OCINDETERMINATE  ((size_t)0xffffffffffffffff)
#endif

extern OCnode* ocnode_new(char* name, OCtype ptype, OCnode* root);
extern void occollectpathtonode(OCnode* node, NClist* path);
extern void occomputefullnames(OCnode* root);
extern void occomputesemantics(NClist*);
extern void ocaddattribute(OCattribute* attr, OCnode* parent);
extern OCattribute* ocmakeattribute(char* name, OCtype ptype, NClist* values);
extern size_t ocsetsize(OCnode* node);
extern OCerror occorrelate(OCnode*,OCnode*);
extern void ocmarkcacheable(OCstate* state, OCnode* ddsroot);

extern void octree_free(struct OCtree* tree);
extern void ocroot_free(OCnode* root);
extern void ocnodes_free(NClist*);

/* Merge DAS with DDS or DATADDS*/
extern OCerror ocddsdasmerge(struct OCstate*, OCnode* das, OCnode* dds);

#endif /*OCNODE_H*/
