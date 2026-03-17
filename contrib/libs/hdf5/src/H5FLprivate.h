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
 * Created:		H5FLprivate.h
 *
 * Purpose:		Private non-prototype header.
 *
 *-------------------------------------------------------------------------
 */
#ifndef H5FLprivate_H
#define H5FLprivate_H

/* Public headers needed by this file */

/* Private headers needed by this file */

/* Macros for turning off free lists in the library */
/*#define H5_NO_FREE_LISTS*/
#if defined H5_NO_FREE_LISTS || defined H5_USING_MEMCHECKER
#define H5_NO_REG_FREE_LISTS
#define H5_NO_ARR_FREE_LISTS
#define H5_NO_SEQ_FREE_LISTS
#define H5_NO_BLK_FREE_LISTS
#define H5_NO_FAC_FREE_LISTS
#endif /* H5_NO_FREE_LISTS */

/* Macro to track location where block was allocated from */
/* Uncomment next line to turn on tracking, but don't leave it on after
 * debugging is done because of the extra overhead it imposes.
 */
/* NOTE: This hasn't been extended to all the free-list allocation routines
 * yet. -QAK
 */
/* #define H5FL_TRACK */
#ifdef H5FL_TRACK

#ifndef H5_HAVE_CODESTACK
#error "Free list tracking requires code stack to be enabled"
#endif

/* Macro for inclusion in the free list allocation calls */
#define H5FL_TRACK_INFO , __FILE__, __func__, __LINE__

/* Macro for inclusion in internal free list allocation calls */
#define H5FL_TRACK_INFO_INT , call_file, call_func, call_line

/* Macro for inclusion in the free list allocation parameters */
#define H5FL_TRACK_PARAMS , const char *call_file, const char *call_func, int call_line

/* Forward declarations for structure fields */
struct H5CS_t;

/* Tracking information for each block */
typedef struct H5FL_track_t {
    struct H5CS_t       *stack; /* Function stack */
    char                *file;  /* Name of file containing calling function */
    char                *func;  /* Name of calling function */
    int                  line;  /* Line # within calling function */
    struct H5FL_track_t *next;  /* Pointer to next tracking block */
    struct H5FL_track_t *prev;  /* Pointer to previous tracking block */
} H5FL_track_t;

/* Macro for size of tracking information */
#define H5FL_TRACK_SIZE sizeof(H5FL_track_t)

#else /* H5FL_TRACK */
#define H5FL_TRACK_INFO
#define H5FL_TRACK_INFO_INT
#define H5FL_TRACK_PARAMS
#define H5FL_TRACK_SIZE 0
#endif /* H5FL_TRACK */

/*
 * Private datatypes.
 */

/* Data structure to store each block in free list */
typedef struct H5FL_reg_node_t {
    struct H5FL_reg_node_t *next; /* Pointer to next block in free list */
} H5FL_reg_node_t;

/* Data structure for free list of blocks */
typedef struct H5FL_reg_head_t {
    bool             init;      /* Whether the free list has been initialized */
    unsigned         allocated; /* Number of blocks allocated */
    unsigned         onlist;    /* Number of blocks on free list */
    const char      *name;      /* Name of the type */
    size_t           size;      /* Size of the blocks in the list */
    H5FL_reg_node_t *list;      /* List of free blocks */
} H5FL_reg_head_t;

/*
 * Macros for defining & using free lists for a type
 */
#define H5FL_REG_NAME(t) H5_##t##_reg_free_list
#ifndef H5_NO_REG_FREE_LISTS
/* Common macros for H5FL_DEFINE & H5FL_DEFINE_STATIC */
#define H5FL_DEFINE_COMMON(t) H5FL_reg_head_t H5FL_REG_NAME(t) = {0, 0, 0, #t, sizeof(t), NULL}

/* Declare a free list to manage objects of type 't' */
#define H5FL_DEFINE(t) H5_DLL H5FL_DEFINE_COMMON(t)

/* Reference a free list for type 't' defined in another file */
#define H5FL_EXTERN(t) H5_DLLVAR H5FL_reg_head_t H5FL_REG_NAME(t)

/* Declare a static free list to manage objects of type 't' */
#define H5FL_DEFINE_STATIC(t) static H5FL_DEFINE_COMMON(t)

/* Allocate an object of type 't' */
#define H5FL_MALLOC(t) (t *)H5FL_reg_malloc(&(H5FL_REG_NAME(t))H5FL_TRACK_INFO)

/* Allocate an object of type 't' and clear it to all zeros */
#define H5FL_CALLOC(t) (t *)H5FL_reg_calloc(&(H5FL_REG_NAME(t))H5FL_TRACK_INFO)

/* Free an object of type 't' */
#define H5FL_FREE(t, obj) (t *)H5FL_reg_free(&(H5FL_REG_NAME(t)), obj)

/* Re-allocating an object of type 't' is not defined, because these free-lists
 * only support fixed sized types, like structs, etc..
 */

#else /* H5_NO_REG_FREE_LISTS */
#include "H5MMprivate.h"
/* Common macro for H5FL_DEFINE & H5FL_DEFINE_STATIC */
#define H5FL_DEFINE_COMMON(t) int H5_ATTR_UNUSED H5FL_REG_NAME(t)

#define H5FL_DEFINE(t)        H5_DLL H5FL_DEFINE_COMMON(t)
#define H5FL_EXTERN(t)        H5_DLLVAR H5FL_DEFINE_COMMON(t)
#define H5FL_DEFINE_STATIC(t) static H5FL_DEFINE_COMMON(t)
#define H5FL_MALLOC(t)        (t *)H5MM_malloc(sizeof(t))
#define H5FL_CALLOC(t)        (t *)H5MM_calloc(sizeof(t))
#define H5FL_FREE(t, obj)     (t *)H5MM_xfree(obj)
#endif /* H5_NO_REG_FREE_LISTS */

/* Data structure to store information about each block allocated */
typedef union H5FL_blk_list_t {
    size_t                 size;    /* Size of the page */
    union H5FL_blk_list_t *next;    /* Pointer to next block in free list */
    double                 unused1; /* Unused normally, just here for alignment */
    haddr_t                unused2; /* Unused normally, just here for alignment */
} H5FL_blk_list_t;

/* Data structure for priority queue node of block free lists */
typedef struct H5FL_blk_node_t {
    size_t                  size;      /* Size of the blocks in the list */
    unsigned                allocated; /* Number of blocks of this size allocated */
    unsigned                onlist;    /* Number of blocks on free list */
    H5FL_blk_list_t        *list;      /* List of free blocks */
    struct H5FL_blk_node_t *next;      /* Pointer to next free list in queue */
    struct H5FL_blk_node_t *prev;      /* Pointer to previous free list in queue */
} H5FL_blk_node_t;

/* Data structure for priority queue of native block free lists */
typedef struct H5FL_blk_head_t {
    bool             init;      /* Whether the free list has been initialized */
    unsigned         allocated; /* Total number of blocks allocated */
    unsigned         onlist;    /* Total number of blocks on free list */
    size_t           list_mem;  /* Total amount of memory in blocks on free list */
    const char      *name;      /* Name of the type */
    H5FL_blk_node_t *head;      /* Pointer to first free list in queue */
} H5FL_blk_head_t;

/*
 * Macros for defining & using priority queues
 */
#define H5FL_BLK_NAME(t) H5_##t##_blk_free_list
#ifndef H5_NO_BLK_FREE_LISTS
/* Common macro for H5FL_BLK_DEFINE & H5FL_BLK_DEFINE_STATIC */
#define H5FL_BLK_DEFINE_COMMON(t) H5FL_blk_head_t H5FL_BLK_NAME(t) = {0, 0, 0, 0, #t "_blk", NULL}

/* Declare a free list to manage objects of type 't' */
#define H5FL_BLK_DEFINE(t) H5_DLL H5FL_BLK_DEFINE_COMMON(t)

/* Reference a free list for type 't' defined in another file */
#define H5FL_BLK_EXTERN(t) H5_DLLVAR H5FL_blk_head_t H5FL_BLK_NAME(t)

/* Declare a static free list to manage objects of type 't' */
#define H5FL_BLK_DEFINE_STATIC(t) static H5FL_BLK_DEFINE_COMMON(t)

/* Allocate a block of type 't' */
#define H5FL_BLK_MALLOC(t, size) (uint8_t *)H5FL_blk_malloc(&(H5FL_BLK_NAME(t)), size H5FL_TRACK_INFO)

/* Allocate a block of type 't' and clear it to zeros */
#define H5FL_BLK_CALLOC(t, size) (uint8_t *)H5FL_blk_calloc(&(H5FL_BLK_NAME(t)), size H5FL_TRACK_INFO)

/* Free a block of type 't' */
#define H5FL_BLK_FREE(t, blk) (uint8_t *)H5FL_blk_free(&(H5FL_BLK_NAME(t)), blk)

/* Re-allocate a block of type 't' */
#define H5FL_BLK_REALLOC(t, blk, new_size)                                                                   \
    (uint8_t *)H5FL_blk_realloc(&(H5FL_BLK_NAME(t)), blk, new_size H5FL_TRACK_INFO)

/* Check if there is a free block available to reuse */
#define H5FL_BLK_AVAIL(t, size) H5FL_blk_free_block_avail(&(H5FL_BLK_NAME(t)), size)

#else /* H5_NO_BLK_FREE_LISTS */
/* Common macro for H5FL_BLK_DEFINE & H5FL_BLK_DEFINE_STATIC */
#define H5FL_BLK_DEFINE_COMMON(t) int H5_ATTR_UNUSED H5FL_BLK_NAME(t)

#define H5FL_BLK_DEFINE(t)                 H5_DLL H5FL_BLK_DEFINE_COMMON(t)
#define H5FL_BLK_EXTERN(t)                 H5_DLLVAR H5FL_BLK_DEFINE_COMMON(t)
#define H5FL_BLK_DEFINE_STATIC(t)          static H5FL_BLK_DEFINE_COMMON(t)
#define H5FL_BLK_MALLOC(t, size)           (uint8_t *)H5MM_malloc(size)
#define H5FL_BLK_CALLOC(t, size)           (uint8_t *)H5MM_calloc(size)
#define H5FL_BLK_FREE(t, blk)              (uint8_t *)H5MM_xfree(blk)
#define H5FL_BLK_REALLOC(t, blk, new_size) (uint8_t *)H5MM_realloc(blk, new_size)
#define H5FL_BLK_AVAIL(t, size)            (false)
#endif /* H5_NO_BLK_FREE_LISTS */

/* Data structure to store each array in free list */
typedef union H5FL_arr_list_t {
    union H5FL_arr_list_t *next;    /* Pointer to next block in free list */
    size_t                 nelem;   /* Number of elements in this array */
    double                 unused1; /* Unused normally, just here for alignment */
    haddr_t                unused2; /* Unused normally, just here for alignment */
} H5FL_arr_list_t;

/* Data structure for each size of array element */
typedef struct H5FL_arr_node_t {
    size_t size;                /* Size of the blocks in the list (in bytes) */
                                /* (Note: base_size + <# of elem> * elem_size) */
    unsigned         allocated; /* Number of blocks allocated of this element size */
    unsigned         onlist;    /* Number of blocks on free list */
    H5FL_arr_list_t *list;      /* List of free blocks */
} H5FL_arr_node_t;

/* Data structure for free list of array blocks */
typedef struct H5FL_arr_head_t {
    bool             init;      /* Whether the free list has been initialized */
    unsigned         allocated; /* Total number of blocks allocated */
    size_t           list_mem;  /* Amount of memory in block on free list */
    const char      *name;      /* Name of the type */
    int              maxelem;   /* Maximum number of elements in an array */
    size_t           base_size; /* Size of the "base" object in the list */
    size_t           elem_size; /* Size of the array elements in the list */
    H5FL_arr_node_t *list_arr;  /* Array of lists of free blocks */
} H5FL_arr_head_t;

/*
 * Macros for defining & using free lists for an array of a type
 */
#define H5FL_ARR_NAME(t) H5_##t##_arr_free_list
#ifndef H5_NO_ARR_FREE_LISTS
/* Common macro for H5FL_ARR_DEFINE & H5FL_ARR_DEFINE_STATIC (and H5FL_BARR variants) */
#define H5FL_ARR_DEFINE_COMMON(b, t, m)                                                                      \
    H5FL_arr_head_t H5FL_ARR_NAME(t) = {0, 0, 0, #t "_arr", m + 1, b, sizeof(t), NULL}

/* Declare a free list to manage arrays of type 't' */
#define H5FL_ARR_DEFINE(t, m) H5_DLL H5FL_ARR_DEFINE_COMMON(0, t, m)

/* Declare a free list to manage base 'b' + arrays of type 't' */
#define H5FL_BARR_DEFINE(b, t, m) H5_DLL H5FL_ARR_DEFINE_COMMON(sizeof(b), t, m)

/* Reference a free list for arrays of type 't' defined in another file */
#define H5FL_ARR_EXTERN(t) H5_DLLVAR H5FL_arr_head_t H5FL_ARR_NAME(t)

/* Declare a static free list to manage arrays of type 't' */
#define H5FL_ARR_DEFINE_STATIC(t, m) static H5FL_ARR_DEFINE_COMMON(0, t, m)

/* Declare a static free list to manage base 'b' + arrays of type 't' */
#define H5FL_BARR_DEFINE_STATIC(b, t, m) static H5FL_ARR_DEFINE_COMMON(sizeof(b), t, m)

/* Allocate an array of type 't' */
#define H5FL_ARR_MALLOC(t, elem) H5FL_arr_malloc(&(H5FL_ARR_NAME(t)), elem H5FL_TRACK_INFO)

/* Allocate an array of type 't' and clear it to all zeros */
#define H5FL_ARR_CALLOC(t, elem) H5FL_arr_calloc(&(H5FL_ARR_NAME(t)), elem H5FL_TRACK_INFO)

/* Free an array of type 't' */
#define H5FL_ARR_FREE(t, obj) (t *)H5FL_arr_free(&(H5FL_ARR_NAME(t)), obj)

/* Re-allocate an array of type 't' */
#define H5FL_ARR_REALLOC(t, obj, new_elem)                                                                   \
    H5FL_arr_realloc(&(H5FL_ARR_NAME(t)), obj, new_elem H5FL_TRACK_INFO)

#else /* H5_NO_ARR_FREE_LISTS */
/* Common macro for H5FL_ARR_DEFINE & H5FL_ARR_DEFINE_STATIC (and H5FL_BARR variants) */
#define H5FL_ARR_DEFINE_COMMON(t, m) size_t H5FL_ARR_NAME(t)

#define H5FL_ARR_DEFINE(t, m)              H5_DLL H5FL_ARR_DEFINE_COMMON(t, m) = 0
#define H5FL_BARR_DEFINE(b, t, m)          H5_DLL H5FL_ARR_DEFINE_COMMON(t, m) = sizeof(b)
#define H5FL_ARR_EXTERN(t)                 H5_DLLVAR H5FL_ARR_DEFINE_COMMON(t, m)
#define H5FL_ARR_DEFINE_STATIC(t, m)       static H5FL_ARR_DEFINE_COMMON(t, m) = 0
#define H5FL_BARR_DEFINE_STATIC(b, t, m)   static H5FL_ARR_DEFINE_COMMON(t, m) = sizeof(b)
#define H5FL_ARR_MALLOC(t, elem)           H5MM_malloc(H5FL_ARR_NAME(t) + ((elem) * sizeof(t)))
#define H5FL_ARR_CALLOC(t, elem)           H5MM_calloc(H5FL_ARR_NAME(t) + ((elem) * sizeof(t)))
#define H5FL_ARR_FREE(t, obj)              (t *)H5MM_xfree(obj)
#define H5FL_ARR_REALLOC(t, obj, new_elem) H5MM_realloc(obj, H5FL_ARR_NAME(t) + ((new_elem) * sizeof(t)))
#endif /* H5_NO_ARR_FREE_LISTS */

/* Data structure for free list of sequence blocks */
typedef struct H5FL_seq_head_t {
    H5FL_blk_head_t queue; /* Priority queue of sequence blocks */
    size_t          size;  /* Size of the sequence elements in the list */
} H5FL_seq_head_t;

/*
 * Macros for defining & using free lists for a sequence of a type
 *
 * Sequences are like arrays, except they have no upper limit.
 *
 */
#define H5FL_SEQ_NAME(t) H5_##t##_seq_free_list
#ifndef H5_NO_SEQ_FREE_LISTS
/* Common macro for H5FL_SEQ_DEFINE & H5FL_SEQ_DEFINE_STATIC */
#define H5FL_SEQ_DEFINE_COMMON(t)                                                                            \
    H5FL_seq_head_t H5FL_SEQ_NAME(t) = {{0, 0, 0, 0, #t "_seq", NULL}, sizeof(t)}

/* Declare a free list to manage sequences of type 't' */
#define H5FL_SEQ_DEFINE(t) H5_DLL H5FL_SEQ_DEFINE_COMMON(t)

/* Reference a free list for sequences of type 't' defined in another file */
#define H5FL_SEQ_EXTERN(t) H5_DLLVAR H5FL_seq_head_t H5FL_SEQ_NAME(t)

/* Declare a static free list to manage sequences of type 't' */
#define H5FL_SEQ_DEFINE_STATIC(t) static H5FL_SEQ_DEFINE_COMMON(t)

/* Allocate a sequence of type 't' */
#define H5FL_SEQ_MALLOC(t, elem) (t *)H5FL_seq_malloc(&(H5FL_SEQ_NAME(t)), elem H5FL_TRACK_INFO)

/* Allocate a sequence of type 't' and clear it to all zeros */
#define H5FL_SEQ_CALLOC(t, elem) (t *)H5FL_seq_calloc(&(H5FL_SEQ_NAME(t)), elem H5FL_TRACK_INFO)

/* Free a sequence of type 't' */
#define H5FL_SEQ_FREE(t, obj) (t *)H5FL_seq_free(&(H5FL_SEQ_NAME(t)), obj)

/* Re-allocate a sequence of type 't' */
#define H5FL_SEQ_REALLOC(t, obj, new_elem)                                                                   \
    (t *)H5FL_seq_realloc(&(H5FL_SEQ_NAME(t)), obj, new_elem H5FL_TRACK_INFO)

#else /* H5_NO_SEQ_FREE_LISTS */
/* Common macro for H5FL_SEQ_DEFINE & H5FL_SEQ_DEFINE_STATIC */
#define H5FL_SEQ_DEFINE_COMMON(t) int H5_ATTR_UNUSED H5FL_SEQ_NAME(t)

#define H5FL_SEQ_DEFINE(t)                 H5_DLL H5FL_SEQ_DEFINE_COMMON(t)
#define H5FL_SEQ_EXTERN(t)                 H5_DLLVAR H5FL_SEQ_DEFINE_COMMON(t)
#define H5FL_SEQ_DEFINE_STATIC(t)          static H5FL_SEQ_DEFINE_COMMON(t)
#define H5FL_SEQ_MALLOC(t, elem)           (t *)H5MM_malloc((elem) * sizeof(t))
#define H5FL_SEQ_CALLOC(t, elem)           (t *)H5MM_calloc((elem) * sizeof(t))
#define H5FL_SEQ_FREE(t, obj)              (t *)H5MM_xfree(obj)
#define H5FL_SEQ_REALLOC(t, obj, new_elem) (t *)H5MM_realloc(obj, (new_elem) * sizeof(t))
#endif /* H5_NO_SEQ_FREE_LISTS */

/* Forward declarations of the data structures for free list block factory */
typedef struct H5FL_fac_gc_node_t H5FL_fac_gc_node_t;
typedef struct H5FL_fac_node_t    H5FL_fac_node_t;

/* Data structure for free list block factory */
typedef struct H5FL_fac_head_t {
    bool                init;      /* Whether the free list has been initialized */
    unsigned            allocated; /* Number of blocks allocated */
    unsigned            onlist;    /* Number of blocks on free list */
    size_t              size;      /* Size of the blocks in the list */
    H5FL_fac_node_t    *list;      /* List of free blocks */
    H5FL_fac_gc_node_t *prev_gc;   /* Previous garbage collection node in list */
} H5FL_fac_head_t;

/*
 * Macros for defining & using free list factories
 *
 * Factories are dynamically created free list managers for blocks of
 *      a particular size.
 *
 */
#ifndef H5_NO_FAC_FREE_LISTS
/* Allocate a block from a factory */
#define H5FL_FAC_MALLOC(f) H5FL_fac_malloc(f H5FL_TRACK_INFO)

/* Allocate a block from a factory and clear it to all zeros */
#define H5FL_FAC_CALLOC(f) H5FL_fac_calloc(f H5FL_TRACK_INFO)

/* Return a block to a factory */
#define H5FL_FAC_FREE(f, obj) H5FL_fac_free(f, obj)

#else /* H5_NO_FAC_FREE_LISTS */
#define H5FL_FAC_MALLOC(f)    H5MM_malloc(f->size)
#define H5FL_FAC_CALLOC(f)    H5MM_calloc(f->size)
#define H5FL_FAC_FREE(f, obj) H5MM_xfree(obj)
#endif /* H5_NO_FAC_FREE_LISTS */

/*
 * Library prototypes.
 */
/* Block free lists */
H5_DLL void  *H5FL_blk_malloc(H5FL_blk_head_t *head, size_t size H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void  *H5FL_blk_calloc(H5FL_blk_head_t *head, size_t size H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void  *H5FL_blk_free(H5FL_blk_head_t *head, void *block);
H5_DLL void  *H5FL_blk_realloc(H5FL_blk_head_t *head, void *block, size_t new_size H5FL_TRACK_PARAMS);
H5_DLL htri_t H5FL_blk_free_block_avail(H5FL_blk_head_t *head, size_t size);

/* Regular free lists */
H5_DLL void *H5FL_reg_malloc(H5FL_reg_head_t *head H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void *H5FL_reg_calloc(H5FL_reg_head_t *head H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void *H5FL_reg_free(H5FL_reg_head_t *head, void *obj);

/* Array free lists */
H5_DLL void *H5FL_arr_malloc(H5FL_arr_head_t *head, size_t elem H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void *H5FL_arr_calloc(H5FL_arr_head_t *head, size_t elem H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void *H5FL_arr_free(H5FL_arr_head_t *head, void *obj);
H5_DLL void *H5FL_arr_realloc(H5FL_arr_head_t *head, void *obj, size_t new_elem H5FL_TRACK_PARAMS);

/* Sequence free lists */
H5_DLL void *H5FL_seq_malloc(H5FL_seq_head_t *head, size_t elem H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void *H5FL_seq_calloc(H5FL_seq_head_t *head, size_t elem H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void *H5FL_seq_free(H5FL_seq_head_t *head, void *obj);
H5_DLL void *H5FL_seq_realloc(H5FL_seq_head_t *head, void *obj, size_t new_elem H5FL_TRACK_PARAMS);

/* Factory free lists */
H5_DLL H5FL_fac_head_t *H5FL_fac_init(size_t size);
H5_DLL void            *H5FL_fac_malloc(H5FL_fac_head_t *head H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void            *H5FL_fac_calloc(H5FL_fac_head_t *head H5FL_TRACK_PARAMS) H5_ATTR_MALLOC;
H5_DLL void            *H5FL_fac_free(H5FL_fac_head_t *head, void *obj);
H5_DLL herr_t           H5FL_fac_term(H5FL_fac_head_t *head);

/* General free list routines */
H5_DLL herr_t H5FL_garbage_coll(void);
H5_DLL herr_t H5FL_set_free_list_limits(int reg_global_lim, int reg_list_lim, int arr_global_lim,
                                        int arr_list_lim, int blk_global_lim, int blk_list_lim,
                                        int fac_global_lim, int fac_list_lim);
H5_DLL herr_t H5FL_get_free_list_sizes(size_t *reg_size, size_t *arr_size, size_t *blk_size,
                                       size_t *fac_size);
H5_DLL int    H5FL_term_interface(void);

#endif
