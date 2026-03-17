/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Header$
 *********************************************************************/

#ifndef NCPROPLIST_H
#define NCPROPLIST_H

#ifndef OPTEXPORT
#ifdef NETCDF_PROPLIST_H
#define OPTEXPORT static
#else /*!NETCDF_PROPLIST_H*/
#ifdef _WIN32
#define OPTEXPORT __declspec(dllexport)
#else
#define OPTEXPORT extern
#endif
#endif /*NETCDF_PROPLIST_H*/
#endif /*OPTEXPORT*/

/**************************************************/
/*
This is used to store a property list mapping a small number of
keys to objects. The uintptr_t type is used to ensure that the value can be a pointer or a
small string upto sizeof(uintptr_t) - 1 (for trailing nul) or an integer constant.

There are two operations that may be defined for the property:
1. reclaiming the value when proplist is free'd and property value points to allocated data of arbitrary complexity.
2. coping the value (for cloning) if it points to allocated data of arbitrary complexity.

The fact that the number of keys is small makes it feasible to use
linear search.  This is currently only used for plugins, but may be
extended to other uses.
*/

/*! Proplist-related structs.
  NOTES:
  1. 'value' is the an arbitrary uintptr_t integer or void* pointer.

  WARNINGS:
  1. It is critical that |uintptr_t| == |void*|
*/

#define NCPROPSMAXKEY 31 /* characters; assert (NCPROPSMAXKEY+1)/8 == 0*/

/* Opaque forward */
struct NCPpair;

/* This function performs all of the following operations on a complex type */
typedef enum NCPtypeop {NCP_RECLAIM=1,NCP_COPY=2} NCPtypeop;

/* There are three possible types for a property value */
typedef enum NCPtype {
	NCP_CONST=0,  /* Value is a simple uintptr_t constant */
	NCP_BYTES=2,  /* Value points to a counted sequence of bytes; If a string,
			then it includes the nul term character */
	NCP_COMPLEX=3 /* Value points to an arbitrarily complex structure */
} NCPtype;

/* (Returns < 0 => error) (>= 0 => success) */
typedef int (*NCPtypefcn)(NCPtypeop op, struct NCPpair* input, struct NCPpair* output);

/* Expose this prefix of NCProperty; used in clone and lookup */
/* Hold just the key+value pair */
typedef struct NCPpair {
    char key[NCPROPSMAXKEY+1]; /* copy of the key string; +1 for trailing nul */
    NCPtype sort;
    uintptr_t value;
    uintptr_t size;        /* size = |value| as ptr to memory, if string, then include trailing nul */
} NCPpair;

/* The property list proper is a sequence of these objects */
typedef struct NCPproperty {
    NCPpair pair;        /* Allowed by C language standard */
    uintptr_t userdata;  /* extra data for the type function */
    NCPtypefcn typefcn;  /* Process type operations */
} NCPproperty;

/*
The property list object.
*/
typedef struct NCproplist {
  size_t alloc; /* allocated space to hold properties */
  size_t count; /* # of defined properties */
  NCPproperty* properties;
} NCproplist;

/**************************************************/
/* Extended API */

#if defined(_cplusplus_) || defined(__cplusplus__)
extern "C" {
#endif

/* All int valued functions return < 0 if error; >= 0 otherwise */


/* Create, free, etc. */
OPTEXPORT NCproplist* ncproplistnew(void);
OPTEXPORT int ncproplistfree(NCproplist*);

/* Insert properties */
OPTEXPORT int ncproplistadd(NCproplist* plist,const char* key, uintptr_t value); /* use when reclaim not needed */
OPTEXPORT int ncproplistaddstring(NCproplist* plist, const char* key, const char* str); /* use when value is simple string (char*) */

/* Insert an instance of type NCP_BYTES */
OPTEXPORT int ncproplistaddbytes(NCproplist* plist, const char* key, void* value, uintptr_t size);

/* Add instance of a complex type */
OPTEXPORT int ncproplistaddx(NCproplist* plist, const char* key, void* value, uintptr_t size, uintptr_t userdata, NCPtypefcn typefcn);

/* clone; keys are copies and values are copied using the NCPtypefcn */
OPTEXPORT int ncproplistclone(const NCproplist* src, NCproplist* clone);

/* 
Lookup key and return value.
@return ::NC_NOERR if found ::NC_EINVAL otherwise; returns the data in datap if !null
*/
OPTEXPORT int ncproplistget(const NCproplist*, const char* key, uintptr_t* datap, uintptr_t* sizep);

/* Iteration support */

/* Return the number of properties in the property list */
#define ncproplistlen(plist) (((NCproplist)(plist))->count)

/* get the ith key+value */
OPTEXPORT int ncproplistith(const NCproplist*, size_t i, char* const * keyp, uintptr_t const * valuep, uintptr_t* sizep);

#if defined(_CPLUSPLUS_) || defined(__CPLUSPLUS__)
}
#endif

#endif /*!NCPROPLIST_H*/ /* WARNING: Do not remove the !; used in building netcdf_proplist.h  */
