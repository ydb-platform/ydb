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

/*-------------------------------------------------------------------------
 *
 * Created:		H5Bprivate.h
 *
 * Purpose:		Private non-prototype header.
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5Bprivate_H
#define H5Bprivate_H

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions			*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5UCprivate.h" /* Reference counted object functions	*/

/**************************/
/* Library Private Macros */
/**************************/

/*
 * Feature: Define this constant if you want to check B-tree consistency
 *	    after each B-tree operation.  Note that this slows down the
 *	    library considerably! Debugging the B-tree depends on assert()
 *	    being enabled.
 */
#ifdef NDEBUG
#undef H5B_DEBUG
#endif

/****************************/
/* Library Private Typedefs */
/****************************/

/* B-tree IDs for various internal things. */
/* Note - if more of these are added, any 'K' values (for internal or leaf
 * nodes) they use will need to be stored in the file somewhere. -QAK
 */
typedef enum H5B_subid_t {
    H5B_SNODE_ID = 0, /*B-tree is for symbol table nodes	     */
    H5B_CHUNK_ID = 1, /*B-tree is for chunked dataset storage	     */
    H5B_NUM_BTREE_ID  /* Number of B-tree key IDs (must be last)   */
} H5B_subid_t;

/* Define return values from B-tree insertion callbacks */
typedef enum H5B_ins_t {
    H5B_INS_ERROR  = -1, /*error return value			     */
    H5B_INS_NOOP   = 0,  /*insert made no changes		     */
    H5B_INS_LEFT   = 1,  /*insert new node to left of cur node	     */
    H5B_INS_RIGHT  = 2,  /*insert new node to right of cur node	     */
    H5B_INS_CHANGE = 3,  /*change child address for cur node	     */
    H5B_INS_FIRST  = 4,  /*insert first node in (sub)tree	     */
    H5B_INS_REMOVE = 5   /*remove current node			     */
} H5B_ins_t;

/* Enum for specifying the direction of the critical key in relation to the
 * child */
typedef enum H5B_dir_t {
    H5B_LEFT  = 0, /* Critical key is to the left */
    H5B_RIGHT = 1  /* Critical key is to the right */
} H5B_dir_t;

/* Define the operator callback function pointer for H5B_iterate() */
typedef int (*H5B_operator_t)(H5F_t *f, const void *_lt_key, haddr_t addr, const void *_rt_key, void *_udata);

/* Each B-tree has certain information that can be shared across all
 * the instances of nodes in that B-tree.
 */
typedef struct H5B_shared_t {
    const struct H5B_class_t *type;         /* Type of tree */
    unsigned                  two_k;        /* 2*"K" value for tree's nodes */
    size_t                    sizeof_rkey;  /* Size of raw (disk) key */
    size_t                    sizeof_rnode; /* Size of raw (disk) node */
    size_t                    sizeof_keys;  /* Size of native (memory) key node */
    size_t                    sizeof_addr;  /* Size of file address (in bytes) */
    size_t                    sizeof_len;   /* Size of file lengths (in bytes) */
    uint8_t                  *page;         /* Disk page */
    size_t                   *nkey;         /* Offsets of each native key in native key buffer */
    void                     *udata;        /* 'Local' info for a B-tree */
} H5B_shared_t;

/*
 * Each class of object that can be pointed to by a B-link tree has a
 * variable of this type that contains class variables and methods.  Each
 * tree has a K (1/2 rank) value on a per-file basis.  The file_create_parms
 * has an array of K values indexed by the `id' class field below.  The
 * array is initialized with the HDF5_BTREE_K_DEFAULT macro.
 */
typedef struct H5B_class_t {
    H5B_subid_t id;                                     /*id as found in file*/
    size_t      sizeof_nkey;                            /*size of native (memory) key*/
    H5UC_t *(*get_shared)(const H5F_t *, const void *); /*shared info for node */
    herr_t (*new_node)(H5F_t *, H5B_ins_t, void *, void *, void *, haddr_t *);
    int (*cmp2)(void *, void *, void *); /*compare 2 keys */
    int (*cmp3)(void *, void *, void *); /*compare 3 keys */
    htri_t (*found)(H5F_t *, haddr_t, const void *, bool *, void *);

    /* insert new data */
    H5B_ins_t (*insert)(H5F_t *, haddr_t, void *, bool *, void *, void *, void *, bool *, haddr_t *);

    /* min insert uses min leaf, not new(), similarly for max insert */
    bool follow_min;
    bool follow_max;

    /* The direction of the key that is intrinsically associated with each node */
    H5B_dir_t critical_key;

    /* remove existing data */
    H5B_ins_t (*remove)(H5F_t *, haddr_t, void *, bool *, void *, void *, bool *);

    /* encode, decode, debug key values */
    herr_t (*decode)(const H5B_shared_t *, const uint8_t *, void *);
    herr_t (*encode)(const H5B_shared_t *, uint8_t *, const void *);
    herr_t (*debug_key)(FILE *, int, int, const void *, const void *);
} H5B_class_t;

/* Information about B-tree */
typedef struct H5B_info_t {
    hsize_t size;      /* Size of B-tree nodes */
    hsize_t num_nodes; /* Number of B-tree nodes */
} H5B_info_t;

/*****************************/
/* Library-private Variables */
/*****************************/

/***************************************/
/* Library-private Function Prototypes */
/***************************************/
H5_DLL herr_t H5B_create(H5F_t *f, const H5B_class_t *type, void *udata, haddr_t *addr_p /*out*/);
H5_DLL herr_t H5B_find(H5F_t *f, const H5B_class_t *type, haddr_t addr, bool *found, void *udata);
H5_DLL herr_t H5B_insert(H5F_t *f, const H5B_class_t *type, haddr_t addr, void *udata);
H5_DLL herr_t H5B_iterate(H5F_t *f, const H5B_class_t *type, haddr_t addr, H5B_operator_t op, void *udata);
H5_DLL herr_t H5B_get_info(H5F_t *f, const H5B_class_t *type, haddr_t addr, H5B_info_t *bt_info,
                           H5B_operator_t op, void *udata);
H5_DLL herr_t H5B_remove(H5F_t *f, const H5B_class_t *type, haddr_t addr, void *udata);
H5_DLL herr_t H5B_delete(H5F_t *f, const H5B_class_t *type, haddr_t addr, void *udata);
H5_DLL H5B_shared_t *H5B_shared_new(const H5F_t *f, const H5B_class_t *type, size_t sizeof_rkey);
H5_DLL herr_t        H5B_shared_free(void *_shared);
H5_DLL herr_t H5B_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5B_class_t *type,
                        void *udata);
H5_DLL htri_t H5B_valid(H5F_t *f, const H5B_class_t *type, haddr_t addr);
#endif /* H5Bprivate_H */
