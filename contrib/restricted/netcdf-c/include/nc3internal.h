/*
 *      Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */
#ifndef _NC3INTERNAL_
#define _NC3INTERNAL_

/*
 *      netcdf library 'private' data structures, objects and interfaces
 */
#include "config.h"
#include <stddef.h>     /* size_t */
#ifndef HAVE_STDINT_H
#  error #include "pstdint.h"  /* attempts to define uint32_t etc portably */
#else
#  include <stdint.h>
#endif /* HAVE_STDINT_H */
#include "netcdf.h"
#ifdef USE_PARALLEL
#include "netcdf_par.h"
#endif /* USE_PARALLEL */

/* Always needed */
#include "nc.h"

#include "nchashmap.h"

#ifndef NC_ARRAY_GROWBY
#define NC_ARRAY_GROWBY 4
#endif

/*
 * The extern size of an empty
 * netcdf version 1 file.
 * The initial value of ncp->xsz.
 */
/* For classic */
#define MIN_NC3_XSZ 32
/* For cdf5 */
#define MIN_NC5_XSZ 48

/* Forward */
struct ncio;
typedef struct NC3_INFO NC3_INFO;

/*
 *  The internal data types
 */
typedef enum {
    NC_UNSPECIFIED = 0,
/* future       NC_BITFIELD = 7, */
/*      NC_STRING =     8,      */
    NC_DIMENSION =  10,
    NC_VARIABLE =   11,
    NC_ATTRIBUTE =  12
} NCtype;

/*
 * NC dimension structure
 */
typedef struct {
    /* all xdr'd */
    NC_string* name;
    size_t size;
} NC_dim;

typedef struct NC_dimarray {
    size_t nalloc;          /* number allocated >= nelems */
    /* below gets xdr'd */
    /* NCtype type = NC_DIMENSION */
    size_t nelems;          /* length of the array */
    NC_hashmap *hashmap;
    NC_dim **value;
} NC_dimarray;

/* Begin defined in dim.c */

extern void
free_NC_dim(NC_dim *dimp);

extern NC_dim *
new_x_NC_dim(NC_string *name);

extern int
find_NC_Udim(const NC_dimarray *ncap, NC_dim **dimpp);

/* dimarray */

extern void
free_NC_dimarrayV0(NC_dimarray *ncap);

extern void
free_NC_dimarrayV(NC_dimarray *ncap);

extern int
dup_NC_dimarrayV(NC_dimarray *ncap, const NC_dimarray *ref);

extern NC_dim *
elem_NC_dimarray(const NC_dimarray *ncap, size_t elem);

/* End defined in dim.c */

/*
 * NC attribute
 */
typedef struct {
    size_t xsz;             /* amount of space at xvalue */
    /* begin xdr */
    NC_string *name;
    nc_type type;           /* the discriminant */
    size_t nelems;          /* length of the array */
    void *xvalue;           /* the actual data, in external representation */
    /* end xdr */
} NC_attr;

typedef struct NC_attrarray {
    size_t nalloc;          /* number allocated >= nelems */
    /* begin xdr */
    /* NCtype type = NC_ATTRIBUTE */
    size_t nelems;          /* length of the array */
    NC_attr **value;
    /* end xdr */
} NC_attrarray;

/* Begin defined in attr.c */

extern void
free_NC_attr(NC_attr *attrp);

extern NC_attr *
new_x_NC_attr(
    NC_string *strp,
    nc_type type,
    size_t nelems);

extern NC_attr **
NC_findattr(const NC_attrarray *ncap, const char *name);

/* attrarray */

extern void
free_NC_attrarrayV0(NC_attrarray *ncap);

extern void
free_NC_attrarrayV(NC_attrarray *ncap);

extern int
dup_NC_attrarrayV(NC_attrarray *ncap, const NC_attrarray *ref);

extern NC_attr *
elem_NC_attrarray(const NC_attrarray *ncap, size_t elem);

/* End defined in attr.c */


/*
 * NC variable: description and data
 */
typedef struct NC_var {
    size_t xsz;             /* xszof 1 element */
    size_t *shape; /* compiled info: dim->size of each dim */
    off_t *dsizes; /* compiled info: the right to left product of shape */
    /* begin xdr */
    NC_string* name;
    /* next two: formerly NC_iarray *assoc */ /* user definition */
    size_t ndims;   /* assoc->count */
    int *dimids;    /* assoc->value */
    NC_attrarray attrs;
    nc_type type;           /* the discriminant */
    long long len;          /* the total length originally allocated */
    off_t begin;
    /* end xdr */
    int no_fill;            /* whether fill mode is ON or OFF */
} NC_var;

typedef struct NC_vararray {
    size_t nalloc;          /* number allocated >= nelems */
    /* begin xdr */
    /* NCtype type = NC_VARIABLE */
    size_t nelems;          /* length of the array */
    NC_hashmap *hashmap;
    NC_var **value;
} NC_vararray;

/* Begin defined in var.c */

extern void
free_NC_var(NC_var *varp);

extern NC_var *
new_x_NC_var(
    NC_string *strp,
    size_t ndims);

/* vararray */

extern void
free_NC_vararrayV0(NC_vararray *ncap);

extern void
free_NC_vararrayV(NC_vararray *ncap);

extern int
dup_NC_vararrayV(NC_vararray *ncap, const NC_vararray *ref);

extern int
NC_var_shape(NC_var *varp, const NC_dimarray *dims);

extern int
NC_findvar(const NC_vararray *ncap, const char *name, NC_var **varpp);

extern int
NC_check_vlen(NC_var *varp, long long vlen_max);

extern int
NC_lookupvar(NC3_INFO* ncp, int varid, NC_var **varp);

/* End defined in var.c */

#define IS_RECVAR(vp)                                           \
    ((vp)->shape != NULL ? (*(vp)->shape == NC_UNLIMITED) : 0 )

struct NC3_INFO {
    /* contains the previous NC during redef. */
    NC3_INFO *old;
    int flags; /* mode flags */
    int state; /* state transitions flags */
#	define NC_CREAT 0x1      /* in create phase, cleared by ncendef */
#	define NC_INDEF 0x2      /* in define mode, cleared by ncendef */
#	define NC_NSYNC 0x4      /* synchronise numrecs on change */
#	define NC_HSYNC 0x8      /* synchronise whole header on change */
#	define NC_NDIRTY 0x10  /* numrecs has changed */
#	define NC_HDIRTY 0x20  /* header info has changed */
/* NC_NOFILL defined in netcdf.h, historical interface */
#if 0
#	define NC_NOFILL 0x100   /**< Argument to nc_set_fill() to turn off filling of data. */
#endif
    struct ncio* nciop;
    size_t chunk;   /* largest extent this layer will request from ncio->get() */
    size_t xsz;     /* external size of this header, == var[0].begin */
    off_t begin_var; /* position of the first (non-record) var */
    off_t begin_rec; /* position of the first 'record' */
    /* Don't constrain maximum size of record unnecessarily */
#if SIZEOF_OFF_T > SIZEOF_SIZE_T
    off_t recsize;   /* length of 'record' */
#else
    size_t recsize;  /* length of 'record' */
#endif
    /* below gets xdr'd */
    size_t numrecs; /* number of 'records' allocated */
    NC_dimarray dims;
    NC_attrarray attrs;
    NC_vararray vars;
};

#define NC_readonly(ncp)                        \
    (!fIsSet((ncp)->nciop->ioflags, NC_WRITE))

#define NC_set_readonly(ncp)                    \
    fClr((ncp)->flags, NC_WRITE)

#define NC_IsNew(ncp)                           \
    fIsSet((ncp)->state, NC_CREAT)

#define NC_indef(ncp)                                   \
    (NC_IsNew(ncp) || fIsSet((ncp)->state, NC_INDEF))

#define set_NC_ndirty(ncp)                      \
    fSet((ncp)->state, NC_NDIRTY)

#define NC_ndirty(ncp)                          \
    fIsSet((ncp)->state, NC_NDIRTY)

#define set_NC_hdirty(ncp)                      \
    fSet((ncp)->state, NC_HDIRTY)

#define NC_hdirty(ncp)                          \
    fIsSet((ncp)->state, NC_HDIRTY)

#define NC_dofill(ncp)                          \
    (!fIsSet((ncp)->state, NC_NOFILL))

#define NC_doHsync(ncp)                         \
    fIsSet((ncp)->state, NC_HSYNC)

#define NC_doNsync(ncp)                         \
    fIsSet((ncp)->state, NC_NSYNC)

#  define NC_get_numrecs(nc3i)                  \
    ((nc3i)->numrecs)

#  define NC_set_numrecs(nc3i, nrecs)           \
    {(nc3i)->numrecs = (nrecs);}

#  define NC_increase_numrecs(nc3i, nrecs)                              \
    {if((nrecs) > (nc3i)->numrecs) ((nc3i)->numrecs = (nrecs));}

/* Begin defined in nc.c */

extern int
nc3_cktype(int mode, nc_type datatype);

extern size_t
ncx_howmany(nc_type type, size_t xbufsize);

extern int
read_numrecs(NC3_INFO* ncp);

extern int
write_numrecs(NC3_INFO* ncp);

extern int
NC_sync(NC3_INFO* ncp);

extern int
NC_calcsize(const NC3_INFO* ncp, off_t *filesizep);

extern int
NC3_inq_default_fill_value(int xtype, void *fillp);

extern int
NC3_inq_var_fill(const NC_var *varp, void *fill_value);

/* End defined in nc.c */
/* Begin defined in v1hpg.c */

extern size_t
ncx_len_NC(const NC3_INFO* ncp, size_t sizeof_off_t);

extern int
ncx_put_NC(const NC3_INFO* ncp, void **xpp, off_t offset, size_t extent);

extern int
nc_get_NC(NC3_INFO* ncp);

/* End defined in v1hpg.c */
/* Begin defined in putget.c */

extern int
fill_NC_var(NC3_INFO* ncp, const NC_var *varp, long long varsize, size_t recno);

extern int
nc_inq_rec(int ncid, size_t *nrecvars, int *recvarids, size_t *recsizes);

extern int
nc_get_rec(int ncid, size_t recnum, void **datap);

extern int
nc_put_rec(int ncid, size_t recnum, void *const *datap);

/* End defined in putget.c */

extern int
NC_check_vlens(NC3_INFO *ncp);

extern int
NC_check_voffs(NC3_INFO *ncp);

/* Define accessors for the dispatchdata */
#define NC3_DATA(nc) ((NC3_INFO*)(nc)->dispatchdata)
#define NC3_DATA_SET(nc,data) ((nc)->dispatchdata = (void*)(data))

#endif /* _NC3INTERNAL_ */
