/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose: This file contains declarations which are visible only within
 *          the H5B package.  Source files outside the H5B package should
 *          include H5Bprivate.h instead.
 */
#if !(defined H5B_FRIEND || defined H5B_MODULE)
#error "Do not include this file outside the H5B package!"
#endif

#ifndef H5Bpkg_H
#define H5Bpkg_H

/* Get package's private header */
#include "H5Bprivate.h"

/* Other private headers needed by this file */
#include "H5ACprivate.h" /* Metadata cache            */
#include "H5FLprivate.h" /* Free Lists                           */

/**************************/
/* Package Private Macros */
/**************************/

/* Get the native key at a given index */
#define H5B_NKEY(b, shared, idx) ((b)->native + (shared)->nkey[(idx)])
#define LEVEL_BITS               8 /* # of bits for node level: 1 byte */

/****************************/
/* Package Private Typedefs */
/****************************/

/* The B-tree node as stored in memory...  */
typedef struct H5B_t {
    H5AC_info_t cache_info; /* Information for H5AC cache functions */
                            /* MUST be first field in structure */
    H5UC_t  *rc_shared;     /* Ref-counted shared info */
    unsigned level;         /* Node level */
    unsigned nchildren;     /* Number of child pointers */
    haddr_t  left;          /* Address of left sibling */
    haddr_t  right;         /* Address of right sibling */
    uint8_t *native;        /* Array of keys in native format */
    haddr_t *child;         /* 2k child pointers */
} H5B_t;

/* Callback info for loading a B-tree node into the cache */
typedef struct H5B_cache_ud_t {
    H5F_t                    *f;         /* File that B-tree node is within */
    const struct H5B_class_t *type;      /* Type of tree */
    H5UC_t                   *rc_shared; /* Ref-counted shared info */
} H5B_cache_ud_t;

/*****************************/
/* Package Private Variables */
/*****************************/

/* Declare a free list to manage the haddr_t sequence information */
H5FL_SEQ_EXTERN(haddr_t);

/* Declare a PQ free list to manage the native block information */
H5FL_BLK_EXTERN(native_block);

/* Declare a free list to manage the H5B_t struct */
H5FL_EXTERN(H5B_t);

/******************************/
/* Package Private Prototypes */
/******************************/
H5_DLL herr_t H5B__node_dest(H5B_t *bt);
#ifdef H5B_DEBUG
herr_t H5B__assert(H5F_t *f, haddr_t addr, const H5B_class_t *type, void *udata);
#endif

#endif /*H5Bpkg_H*/
