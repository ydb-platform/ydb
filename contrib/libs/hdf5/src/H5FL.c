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
 * Purpose: Manage priority queues of free-lists (of blocks of bytes).
 *      These are used in various places in the library which allocate and
 *      free differently blocks of bytes repeatedly.  Usually the same size
 *      of block is allocated and freed repeatedly in a loop, while writing out
 *      chunked data for example, but the blocks may also be of different sizes
 *      from different datasets and an attempt is made to optimize access to
 *      the proper free list of blocks by using these priority queues to
 *      move frequently accessed free lists to the head of the queue.
 */

#include "H5FLmodule.h" /* This source code file is part of the H5FL module */

/* #define H5FL_DEBUG */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MMprivate.h" /* Memory management			*/

/*
 * Private type definitions
 */

/*
    Default limits on how much memory can accumulate on each free list before
    it is garbage collected.
 */
static size_t H5FL_reg_glb_mem_lim = 1 * 1024 * 1024;  /* Default to 1MB limit on all regular free lists */
static size_t H5FL_reg_lst_mem_lim = 1 * 65536;        /* Default to 64KB limit on each regular free list */
static size_t H5FL_arr_glb_mem_lim = 4 * 1024 * 1024;  /* Default to 4MB limit on all array free lists */
static size_t H5FL_arr_lst_mem_lim = 4 * 65536;        /* Default to 256KB limit on each array free list */
static size_t H5FL_blk_glb_mem_lim = 16 * 1024 * 1024; /* Default to 16MB limit on all block free lists */
static size_t H5FL_blk_lst_mem_lim = 1024 * 1024; /* Default to 1024KB (1MB) limit on each block free list */
static size_t H5FL_fac_glb_mem_lim = 16 * 1024 * 1024; /* Default to 16MB limit on all factory free lists */
static size_t H5FL_fac_lst_mem_lim =
    1024 * 1024; /* Default to 1024KB (1MB) limit on each factory free list */

/* A garbage collection node for regular free lists */
typedef struct H5FL_reg_gc_node_t {
    H5FL_reg_head_t           *list; /* Pointer to the head of the list to garbage collect */
    struct H5FL_reg_gc_node_t *next; /* Pointer to the next node in the list of things to garbage collect */
} H5FL_reg_gc_node_t;

/* The garbage collection head for regular free lists */
typedef struct H5FL_reg_gc_list_t {
    size_t                     mem_freed; /* Amount of free memory on list */
    struct H5FL_reg_gc_node_t *first; /* Pointer to the first node in the list of things to garbage collect */
} H5FL_reg_gc_list_t;

/* The head of the list of things to garbage collect */
static H5FL_reg_gc_list_t H5FL_reg_gc_head = {0, NULL};

/* A garbage collection node for array free lists */
typedef struct H5FL_gc_arr_node_t {
    H5FL_arr_head_t           *list; /* Pointer to the head of the list to garbage collect */
    struct H5FL_gc_arr_node_t *next; /* Pointer to the next node in the list of things to garbage collect */
} H5FL_gc_arr_node_t;

/* The garbage collection head for array free lists */
typedef struct H5FL_gc_arr_list_t {
    size_t                     mem_freed; /* Amount of free memory on list */
    struct H5FL_gc_arr_node_t *first; /* Pointer to the first node in the list of things to garbage collect */
} H5FL_gc_arr_list_t;

/* The head of the list of array things to garbage collect */
static H5FL_gc_arr_list_t H5FL_arr_gc_head = {0, NULL};

/* A garbage collection node for blocks */
typedef struct H5FL_blk_gc_node_t {
    H5FL_blk_head_t           *pq;   /* Pointer to the head of the PQ to garbage collect */
    struct H5FL_blk_gc_node_t *next; /* Pointer to the next node in the list of things to garbage collect */
} H5FL_blk_gc_node_t;

/* The garbage collection head for blocks */
typedef struct H5FL_blk_gc_list_t {
    size_t                     mem_freed; /* Amount of free memory on list */
    struct H5FL_blk_gc_node_t *first; /* Pointer to the first node in the list of things to garbage collect */
} H5FL_blk_gc_list_t;

/* The head of the list of PQs to garbage collect */
static H5FL_blk_gc_list_t H5FL_blk_gc_head = {0, NULL};

/* A garbage collection node for factory free lists */
struct H5FL_fac_gc_node_t {
    H5FL_fac_head_t           *list; /* Pointer to the head of the list to garbage collect */
    struct H5FL_fac_gc_node_t *next; /* Pointer to the next node in the list of things to garbage collect */
};

/* The garbage collection head for factory free lists */
typedef struct H5FL_fac_gc_list_t {
    size_t                     mem_freed; /* Amount of free memory on list */
    struct H5FL_fac_gc_node_t *first; /* Pointer to the first node in the list of things to garbage collect */
} H5FL_fac_gc_list_t;

/* Data structure to store each block in factory free list */
struct H5FL_fac_node_t {
    struct H5FL_fac_node_t *next; /* Pointer to next block in free list */
};

/* The head of the list of factory things to garbage collect */
static H5FL_fac_gc_list_t H5FL_fac_gc_head = {0, NULL};

#ifdef H5FL_TRACK

/* Extra headers needed */
#include "H5CSprivate.h" /* Function stack			*/

/* Head of "outstanding allocations" list */
static H5FL_track_t *H5FL_out_head_g = NULL;
#endif /* H5FL_TRACK */

/* Forward declarations of local static functions */
static void            *H5FL__malloc(size_t mem_size);
static herr_t           H5FL__reg_init(H5FL_reg_head_t *head);
static herr_t           H5FL__reg_gc(void);
static herr_t           H5FL__reg_gc_list(H5FL_reg_head_t *head);
static int              H5FL__reg_term(void);
static H5FL_blk_node_t *H5FL__blk_find_list(H5FL_blk_node_t **head, size_t size);
static H5FL_blk_node_t *H5FL__blk_create_list(H5FL_blk_node_t **head, size_t size);
static herr_t           H5FL__blk_init(H5FL_blk_head_t *head);
static herr_t           H5FL__blk_gc_list(H5FL_blk_head_t *head);
static herr_t           H5FL__blk_gc(void);
static int              H5FL__blk_term(void);
static herr_t           H5FL__arr_init(H5FL_arr_head_t *head);
static herr_t           H5FL__arr_gc_list(H5FL_arr_head_t *head);
static herr_t           H5FL__arr_gc(void);
static int              H5FL__arr_term(void);
static herr_t           H5FL__fac_gc_list(H5FL_fac_head_t *head);
static herr_t           H5FL__fac_gc(void);
static int              H5FL__fac_term_all(void);

/* Declare a free list to manage the H5FL_blk_node_t struct */
H5FL_DEFINE(H5FL_blk_node_t);

/* Declare a free list to manage the H5FL_fac_gc_node_t struct */
H5FL_DEFINE_STATIC(H5FL_fac_gc_node_t);

/* Declare a free list to manage the H5FL_fac_head_t struct */
H5FL_DEFINE(H5FL_fac_head_t);

/*--------------------------------------------------------------------------
 NAME
    H5FL_term_package
 PURPOSE
    Terminate various H5FL objects
 USAGE
    void H5FL_term_package()
 RETURNS
    Success:	Positive if any action might have caused a change in some
                other interface; zero otherwise.
        Failure:	Negative
 DESCRIPTION
    Release any resources allocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     Can't report errors...
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5FL_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

        /* Garbage collect any nodes on the free lists */
        (void)
    H5FL_garbage_coll();

    /* Shut down the various kinds of free lists */
    n += H5FL__reg_term();
    n += H5FL__fac_term_all();
    n += H5FL__arr_term();
    n += H5FL__blk_term();

#ifdef H5FL_TRACK
    /* If we haven't freed all the allocated memory, dump out the list now */
    if (n > 0 && H5FL_out_head_g) {
        H5FL_track_t *trk = H5FL_out_head_g;

        /* Dump information about all the outstanding allocations */
        while (trk != NULL) {
            /* Print information about the outstanding block */
            fprintf(stderr, "%s: Outstanding allocation:\n", __func__);
            fprintf(stderr, "\tPtr: %p, File: %s, Function: %s, Line: %d\n",
                    (((unsigned char *)trk) + sizeof(H5FL_track_t)), trk->file, trk->func, trk->line);
            H5CS_print_stack(trk->stack, stderr);

            /* Advance to next node */
            trk = trk->next;
        } /* end while */
    }     /* end if */
#endif    /* H5FL_TRACK */

    FUNC_LEAVE_NOAPI(n)
} /* end H5FL_term_package() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__malloc
 *
 * Purpose:	Attempt to allocate space using malloc.  If malloc fails, garbage
 *      collect and try again.  If malloc fails again, then return NULL.
 *
 * Return:	Success:	non-NULL
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FL__malloc(size_t mem_size)
{
    void *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Attempt to allocate the memory requested */
    if (NULL == (ret_value = H5MM_malloc(mem_size))) {
        /* If we can't allocate the memory now, try garbage collecting first */
        if (H5FL_garbage_coll() < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during allocation");

        /* Now try allocating the memory again */
        if (NULL == (ret_value = H5MM_malloc(mem_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for chunk");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__malloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__reg_init
 *
 * Purpose:	Initialize a free list for a certain type.  Right now, this just
 *      adds the free list to the list of things to garbage collect.
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__reg_init(H5FL_reg_head_t *head)
{
    H5FL_reg_gc_node_t *new_node;            /* Pointer to the node for the new list to garbage collect */
    herr_t              ret_value = SUCCEED; /* return value*/

    FUNC_ENTER_PACKAGE

    /* Allocate a new garbage collection node */
    if (NULL == (new_node = (H5FL_reg_gc_node_t *)H5MM_malloc(sizeof(H5FL_reg_gc_node_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Initialize the new garbage collection node */
    new_node->list = head;

    /* Link in to the garbage collection list */
    new_node->next         = H5FL_reg_gc_head.first;
    H5FL_reg_gc_head.first = new_node;

    /* Indicate that the free list is initialized */
    head->init = true;

    /* Make certain that the space allocated is large enough to store a free list pointer (eventually) */
    if (head->size < sizeof(H5FL_reg_node_t))
        head->size = sizeof(H5FL_reg_node_t);

        /* Make certain there's room for tracking information, if any */
#ifdef H5FL_TRACK
    head->size += sizeof(H5FL_track_t);
#endif /* H5FL_TRACK */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__reg_init() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_reg_free
 *
 * Purpose:	Release an object & put on free list
 *
 * Return:	Always returns NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_reg_free(H5FL_reg_head_t *head, void *obj)
{
    void *ret_value = NULL; /* Return value */

    /* NOINIT OK here because this must be called after H5FL_reg_malloc/calloc
     * -NAF */
    FUNC_ENTER_NOAPI_NOINIT

    /* Double check parameters */
    assert(head);
    assert(obj);

#ifdef H5FL_TRACK
    {
        H5FL_track_t *trk = obj = ((unsigned char *)obj) - sizeof(H5FL_track_t);

        /* Free tracking information about the allocation location */
        H5CS_close_stack(trk->stack);
        /* The 'func' & 'file' strings are statically allocated (by the compiler)
         * and are not allocated, so there's no need to free them.
         */
        trk->file = NULL;
        trk->func = NULL;

        /* Remove from "outstanding allocations" list */
        if (trk == H5FL_out_head_g) {
            H5FL_out_head_g = H5FL_out_head_g->next;
            if (H5FL_out_head_g)
                H5FL_out_head_g->prev = NULL;
        } /* end if */
        else {
            trk->prev->next = trk->next;
            if (trk->next)
                trk->next->prev = trk->prev;
        } /* end else */
    }
#endif /* H5FL_TRACK */

#ifdef H5FL_DEBUG
    memset(obj, 255, head->size);
#endif /* H5FL_DEBUG */

    /* Make certain that the free list is initialized */
    assert(head->init);

    /* Link into the free list */
    ((H5FL_reg_node_t *)obj)->next = head->list;

    /* Point free list at the node freed */
    head->list = (H5FL_reg_node_t *)obj;

    /* Increment the number of blocks on free list */
    head->onlist++;

    /* Increment the amount of "regular" freed memory globally */
    H5FL_reg_gc_head.mem_freed += head->size;

    /* Check for exceeding free list memory use limits */
    /* First check this particular list */
    if (head->onlist * head->size > H5FL_reg_lst_mem_lim)
        if (H5FL__reg_gc_list(head) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

    /* Then check the global amount memory on regular free lists */
    if (H5FL_reg_gc_head.mem_freed > H5FL_reg_glb_mem_lim)
        if (H5FL__reg_gc() < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_reg_free() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_reg_malloc
 *
 * Purpose:	Allocate a block on a free list
 *
 * Return:	Success:	Pointer to a valid object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_reg_malloc(H5FL_reg_head_t *head H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to object to return */

    FUNC_ENTER_NOAPI(NULL)

    /* Double check parameters */
    assert(head);

    /* Make certain the list is initialized first */
    if (!head->init)
        if (H5FL__reg_init(head) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, NULL, "can't initialize 'regular' blocks");

    /* Check for nodes available on the free list first */
    if (head->list != NULL) {
        /* Get a pointer to the block on the free list */
        ret_value = (void *)(head->list);

        /* Remove node from free list */
        head->list = head->list->next;

        /* Decrement the number of blocks & memory on free list */
        head->onlist--;

        /* Decrement the amount of global "regular" free list memory in use */
        H5FL_reg_gc_head.mem_freed -= (head->size);
    } /* end if */
    /* Otherwise allocate a node */
    else {
        if (NULL == (ret_value = H5FL__malloc(head->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

        /* Increment the number of blocks allocated in list */
        head->allocated++;
    } /* end else */

#ifdef H5FL_TRACK
    /* Copy allocation location information */
    ((H5FL_track_t *)ret_value)->stack = H5CS_copy_stack();
    assert(((H5FL_track_t *)ret_value)->stack);
    /* The 'call_func' & 'call_file' strings are statically allocated (by the compiler)
     * there's no need to duplicate them.
     */
    ((H5FL_track_t *)ret_value)->file = call_file;
    ((H5FL_track_t *)ret_value)->func = call_func;
    ((H5FL_track_t *)ret_value)->line = call_line;

    /* Add to "outstanding allocations" list */
    ((H5FL_track_t *)ret_value)->prev = NULL;
    ((H5FL_track_t *)ret_value)->next = H5FL_out_head_g;
    if (H5FL_out_head_g)
        H5FL_out_head_g->prev = (H5FL_track_t *)ret_value;
    H5FL_out_head_g = (H5FL_track_t *)ret_value;

    /* Adjust for allocation tracking information */
    ret_value = ((unsigned char *)ret_value) + sizeof(H5FL_track_t);
#endif /* H5FL_TRACK */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_reg_malloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_reg_calloc
 *
 * Purpose:	Allocate a block on a free list and clear it to zeros
 *
 * Return:	Success:	Pointer to a valid object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_reg_calloc(H5FL_reg_head_t *head H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to object to return */

    FUNC_ENTER_NOAPI(NULL)

    /* Double check parameters */
    assert(head);

    /* Allocate the block */
    if (NULL == (ret_value = H5FL_reg_malloc(head H5FL_TRACK_INFO_INT)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Clear to zeros */
    /* (Accommodate tracking information, if present) */
    memset(ret_value, 0, head->size - H5FL_TRACK_SIZE);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_reg_calloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__reg_gc_list
 *
 * Purpose:	Garbage collect on a particular object free list
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__reg_gc_list(H5FL_reg_head_t *head)
{
    H5FL_reg_node_t *free_list; /* Pointer to nodes in free list being garbage collected */

    FUNC_ENTER_PACKAGE_NOERR

    /* For each free list being garbage collected, walk through the nodes and free them */
    free_list = head->list;
    while (free_list != NULL) {
        H5FL_reg_node_t *tmp; /* Temporary node pointer */

        /* Get the pointer to the next node */
        tmp = free_list->next;

        /* Free the block */
        H5MM_free(free_list);

        /* Advance to the next node */
        free_list = tmp;
    } /* end while */

    /* Decrement the count of nodes allocated and free the node */
    head->allocated -= head->onlist;

    /* Decrement global count of free memory on "regular" lists */
    H5FL_reg_gc_head.mem_freed -= (head->onlist * head->size);

    /* Indicate no free nodes on the free list */
    head->list   = NULL;
    head->onlist = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FL__reg_gc_list() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__reg_gc
 *
 * Purpose:	Garbage collect on all the object free lists
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__reg_gc(void)
{
    H5FL_reg_gc_node_t *gc_node;             /* Pointer into the list of things to garbage collect */
    herr_t              ret_value = SUCCEED; /* return value*/

    FUNC_ENTER_PACKAGE

    /* Walk through all the free lists, free()'ing the nodes */
    gc_node = H5FL_reg_gc_head.first;
    while (gc_node != NULL) {
        /* Release the free nodes on the list */
        if (H5FL__reg_gc_list(gc_node->list) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "garbage collection of list failed");

        /* Go on to the next free list to garbage collect */
        gc_node = gc_node->next;
    } /* end while */

    /* Double check that all the memory on the free lists is recycled */
    assert(H5FL_reg_gc_head.mem_freed == 0);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__reg_gc() */

/*--------------------------------------------------------------------------
 NAME
    H5FL_reg_term
 PURPOSE
    Terminate various H5FL object free lists
 USAGE
    int H5FL_reg_term()
 RETURNS
    Success:	Positive if any action might have caused a change in some
                other interface; zero otherwise.
        Failure:	Negative
 DESCRIPTION
    Release any resources allocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     Can't report errors...
 EXAMPLES
 REVISION LOG
        If a list cannot be freed because something is using it then return
        zero (failure to free a list doesn't affect any other part of the
        library). If some other layer frees something during its termination
        it will return non-zero, which will cause this function to get called
        again to reclaim this layer's memory.
--------------------------------------------------------------------------*/
static int
H5FL__reg_term(void)
{
    H5FL_reg_gc_node_t *left; /* pointer to garbage collection lists with work left */

    FUNC_ENTER_PACKAGE_NOERR

    /* Free the nodes on the garbage collection list, keeping nodes with allocations outstanding */
    left = NULL;
    while (H5FL_reg_gc_head.first != NULL) {
        H5FL_reg_gc_node_t *tmp; /* Temporary pointer to a garbage collection node */

        /* Get a copy of the next node */
        tmp = H5FL_reg_gc_head.first->next;

#ifdef H5FL_DEBUG
        printf("%s: head->name = %s, head->allocated = %d\n", __func__, H5FL_reg_gc_head.first->list->name,
               (int)H5FL_reg_gc_head.first->list->allocated);
#endif /* H5FL_DEBUG */
        /* Check if the list has allocations outstanding */
        if (H5FL_reg_gc_head.first->list->allocated > 0) {
            /* Add free list to the list of nodes with allocations open still */
            H5FL_reg_gc_head.first->next = left;
            left                         = H5FL_reg_gc_head.first;
        } /* end if */
        /* No allocations left open for list, get rid of it */
        else {
            /* Reset the "initialized" flag, in case we restart this list somehow (I don't know how..) */
            H5FL_reg_gc_head.first->list->init = false;

            /* Free the node from the garbage collection list */
            H5MM_xfree(H5FL_reg_gc_head.first);
        } /* end else */

        H5FL_reg_gc_head.first = tmp;
    } /* end while */

    /* Point to the list of nodes left with allocations open, if any */
    H5FL_reg_gc_head.first = left;

    FUNC_LEAVE_NOAPI(H5FL_reg_gc_head.first != NULL ? 1 : 0)
} /* end H5FL__reg_term() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__blk_find_list
 *
 * Purpose:	Finds the free list for blocks of a given size.  Also moves that
 *      free list node to the head of the priority queue (if it isn't there
 *      already).  This routine does not manage the actual free list, it just
 *      works with the priority queue.
 *
 * Return:	Success:	valid pointer to the free list node
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FL_blk_node_t *
H5FL__blk_find_list(H5FL_blk_node_t **head, size_t size)
{
    H5FL_blk_node_t *temp = NULL; /* Temp. pointer to node in the native list */

    FUNC_ENTER_PACKAGE_NOERR

    /* Find the correct free list */
    temp = *head;

    /* Check if the node is at the head of the list */
    if (temp && temp->size != size) {
        temp = temp->next;

        while (temp != NULL) {
            /* Check if we found the correct node */
            if (temp->size == size) {
                /* Take the node found out of it's current position */
                if (temp->next == NULL) {
                    temp->prev->next = NULL;
                } /* end if */
                else {
                    temp->prev->next = temp->next;
                    temp->next->prev = temp->prev;
                } /* end else */

                /* Move the found node to the head of the list */
                temp->prev    = NULL;
                temp->next    = *head;
                (*head)->prev = temp;
                *head         = temp;

                /* Get out */
                break;
            } /* end if */

            temp = temp->next;
        } /* end while */
    }     /* end if */

    FUNC_LEAVE_NOAPI(temp)
} /* end H5FL__blk_find_list() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__blk_create_list
 *
 * Purpose:	Creates a new free list for blocks of the given size at the
 *      head of the priority queue.
 *
 * Return:	Success:	valid pointer to the free list node
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FL_blk_node_t *
H5FL__blk_create_list(H5FL_blk_node_t **head, size_t size)
{
    H5FL_blk_node_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate room for the new free list node */
    if (NULL == (ret_value = H5FL_CALLOC(H5FL_blk_node_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "memory allocation failed for chunk info");

    /* Set the correct values for the new free list */
    ret_value->size = size;

    /* Attach to head of priority queue */
    if (NULL == *head)
        *head = ret_value;
    else {
        ret_value->next = *head;
        (*head)->prev   = ret_value;
        *head           = ret_value;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__blk_create_list() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__blk_init
 *
 * Purpose:	Initialize a priority queue of a certain type.  Right now, this just
 *      adds the PQ to the list of things to garbage collect.
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__blk_init(H5FL_blk_head_t *head)
{
    H5FL_blk_gc_node_t *new_node;            /* Pointer to the node for the new list to garbage collect */
    herr_t              ret_value = SUCCEED; /* return value*/

    FUNC_ENTER_PACKAGE

    /* Allocate a new garbage collection node */
    if (NULL == (new_node = (H5FL_blk_gc_node_t *)H5MM_malloc(sizeof(H5FL_blk_gc_node_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Initialize the new garbage collection node */
    new_node->pq = head;

    /* Link in to the garbage collection list */
    new_node->next         = H5FL_blk_gc_head.first;
    H5FL_blk_gc_head.first = new_node;

    /* Indicate that the PQ is initialized */
    head->init = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__blk_init() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_blk_free_block_avail
 *
 * Purpose:	Checks if a free block of the appropriate size is available
 *      for a given list.
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5FL_blk_free_block_avail(H5FL_blk_head_t *head, size_t size)
{
    H5FL_blk_node_t *free_list;        /* The free list of nodes of correct size */
    htri_t           ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Double check parameters */
    assert(head);

    /* check if there is a free list for blocks of this size */
    /* and if there are any blocks available on the list */
    if ((free_list = H5FL__blk_find_list(&(head->head), size)) != NULL && free_list->list != NULL)
        ret_value = true;
    else
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_blk_free_block_avail() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_blk_malloc
 *
 * Purpose:	Allocates memory for a block.  This routine is used
 *      instead of malloc because the block can be kept on a free list so
 *      they don't thrash malloc/free as much.
 *
 * Return:	Success:	valid pointer to the block
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_blk_malloc(H5FL_blk_head_t *head, size_t size H5FL_TRACK_PARAMS)
{
    H5FL_blk_node_t *free_list;        /* The free list of nodes of correct size */
    H5FL_blk_list_t *temp;             /* Temp. ptr to the new native list allocated */
    void            *ret_value = NULL; /* Pointer to the block to return to the user */

    FUNC_ENTER_NOAPI(NULL)

    /* Double check parameters */
    assert(head);
    assert(size);

    /* Make certain the list is initialized first */
    if (!head->init)
        if (H5FL__blk_init(head) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, NULL, "can't initialize 'block' list");

    /* check if there is a free list for blocks of this size */
    /* and if there are any blocks available on the list */
    if (NULL != (free_list = H5FL__blk_find_list(&(head->head), size)) && NULL != free_list->list) {
        /* Remove the first node from the free list */
        temp            = free_list->list;
        free_list->list = free_list->list->next;

        /* Decrement the number of blocks & memory used on free list */
        free_list->onlist--;
        head->onlist--;
        head->list_mem -= size;

        /* Decrement the amount of global "block" free list memory in use */
        H5FL_blk_gc_head.mem_freed -= size;
    } /* end if */
    /* No free list available, or there are no nodes on the list, allocate a new node to give to the user */
    else {
        /* Check if there was no free list for native blocks of this size */
        if (NULL == free_list)
            /* Create a new list node and insert it to the queue */
            free_list = H5FL__blk_create_list(&(head->head), size);
        assert(free_list);

        /* Allocate new node, with room for the page info header and the actual page data */
        if (NULL ==
            (temp = (H5FL_blk_list_t *)H5FL__malloc(sizeof(H5FL_blk_list_t) + H5FL_TRACK_SIZE + size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for chunk");

        /* Increment the number of blocks of this size */
        free_list->allocated++;

        /* Increment the total number of blocks allocated */
        head->allocated++;
    } /* end else */

    /* Initialize the block allocated */
    temp->size = size;

    /* Set the return value to the block itself */
    ret_value = ((char *)temp) + sizeof(H5FL_blk_list_t);

#ifdef H5FL_TRACK
    /* Copy allocation location information */
    ((H5FL_track_t *)ret_value)->stack = H5CS_copy_stack();
    assert(((H5FL_track_t *)ret_value)->stack);
    /* The 'call_func' & 'call_file' strings are statically allocated (by the compiler)
     * there's no need to duplicate them.
     */
    ((H5FL_track_t *)ret_value)->file = call_file;
    ((H5FL_track_t *)ret_value)->func = call_func;
    ((H5FL_track_t *)ret_value)->line = call_line;

    /* Add to "outstanding allocations" list */
    ((H5FL_track_t *)ret_value)->prev = NULL;
    ((H5FL_track_t *)ret_value)->next = H5FL_out_head_g;
    if (H5FL_out_head_g)
        H5FL_out_head_g->prev = (H5FL_track_t *)ret_value;
    H5FL_out_head_g = (H5FL_track_t *)ret_value;

    /* Adjust for allocation tracking information */
    ret_value = ((unsigned char *)ret_value) + sizeof(H5FL_track_t);
#endif /* H5FL_TRACK */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_blk_malloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_blk_calloc
 *
 * Purpose:	Allocates memory for a block and clear it to zeros.
 *      This routine is used
 *      instead of malloc because the block can be kept on a free list so
 *      they don't thrash malloc/free as much.
 *
 * Return:	Success:	valid pointer to the block
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_blk_calloc(H5FL_blk_head_t *head, size_t size H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return to the user */

    FUNC_ENTER_NOAPI(NULL)

    /* Double check parameters */
    assert(head);
    assert(size);

    /* Allocate the block */
    if (NULL == (ret_value = H5FL_blk_malloc(head, size H5FL_TRACK_INFO_INT)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Clear the block to zeros */
    memset(ret_value, 0, size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_blk_calloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_blk_free
 *
 * Purpose:	Releases memory for a block.  This routine is used
 *      instead of free because the blocks can be kept on a free list so
 *      they don't thrash malloc/free as much.
 *
 * Return:	Success:	NULL
 *
 *		Failure:	never fails
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_blk_free(H5FL_blk_head_t *head, void *block)
{
    H5FL_blk_node_t *free_list;        /* The free list of nodes of correct size */
    H5FL_blk_list_t *temp;             /* Temp. ptr to the new free list node allocated */
    size_t           free_size;        /* Size of the block freed */
    void            *ret_value = NULL; /* Return value */

    /* NOINIT OK here because this must be called after H5FL_blk_malloc/calloc
     * -NAF */
    FUNC_ENTER_NOAPI_NOINIT

    /* Double check parameters */
    assert(head);
    assert(block);

#ifdef H5FL_TRACK
    {
        unsigned char *block_ptr = ((unsigned char *)block) - sizeof(H5FL_track_t);
        H5FL_track_t   trk;

        H5MM_memcpy(&trk, block_ptr, sizeof(H5FL_track_t));

        /* Free tracking information about the allocation location */
        H5CS_close_stack(trk.stack);
        /* The 'func' & 'file' strings are statically allocated (by the compiler)
         * and are not allocated, so there's no need to free them.
         */
        trk.file = NULL;
        trk.func = NULL;

        /* Remove from "outstanding allocations" list */
        if ((void *)block_ptr == (void *)H5FL_out_head_g) {
            H5FL_out_head_g = H5FL_out_head_g->next;
            if (H5FL_out_head_g)
                H5FL_out_head_g->prev = NULL;
        } /* end if */
        else {
            trk.prev->next = trk.next;
            if (trk.next)
                trk.next->prev = trk.prev;
        } /* end else */

        H5MM_memcpy(block_ptr, &trk, sizeof(H5FL_track_t));
    }
#endif /* H5FL_TRACK */

    /* Get the pointer to the native block info header in front of the native block to free */
    temp = (H5FL_blk_list_t *)((
        void *)((unsigned char *)block -
                (sizeof(H5FL_blk_list_t) +
                 H5FL_TRACK_SIZE))); /*lint !e826 Pointer-to-pointer cast is appropriate here */

    /* Save the block's size for later */
    free_size = temp->size;

#ifdef H5FL_DEBUG
    memset(temp, 255, free_size + sizeof(H5FL_blk_list_t) + H5FL_TRACK_SIZE);
#endif /* H5FL_DEBUG */

    /* Check if there is a free list for native blocks of this size */
    if (NULL == (free_list = H5FL__blk_find_list(&(head->head), free_size)))
        /* No free list available, create a new list node and insert it to the queue */
        free_list = H5FL__blk_create_list(&(head->head), free_size);
    if (NULL == free_list)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "couldn't create new list node");

    /* Prepend the free'd native block to the front of the free list */
    temp->next      = free_list->list; /* Note: Overwrites the size field in union */
    free_list->list = temp;

    /* Increment the number of blocks on free list */
    free_list->onlist++;
    head->onlist++;
    head->list_mem += free_size;

    /* Increment the amount of "block" freed memory globally */
    H5FL_blk_gc_head.mem_freed += free_size;

    /* Check for exceeding free list memory use limits */
    /* First check this particular list */
    if (head->list_mem > H5FL_blk_lst_mem_lim)
        if (H5FL__blk_gc_list(head) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

    /* Then check the global amount memory on block free lists */
    if (H5FL_blk_gc_head.mem_freed > H5FL_blk_glb_mem_lim)
        if (H5FL__blk_gc() < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_blk_free() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_blk_realloc
 *
 * Purpose:	Resizes a block.  This does things the straightforward, simple way,
 *      not actually using realloc.
 *
 * Return:	Success:	NULL
 *
 *		Failure:	never fails
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_blk_realloc(H5FL_blk_head_t *head, void *block, size_t new_size H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Double check parameters */
    assert(head);
    assert(new_size);

    /* Check if we are actually re-allocating a block */
    if (block != NULL) {
        H5FL_blk_list_t *temp; /* Temp. ptr to the new block node allocated */

        /* Get the pointer to the chunk info header in front of the chunk to free */
        temp = (H5FL_blk_list_t *)((
            void *)((unsigned char *)block -
                    (sizeof(H5FL_blk_list_t) +
                     H5FL_TRACK_SIZE))); /*lint !e826 Pointer-to-pointer cast is appropriate here */

        /* check if we are actually changing the size of the buffer */
        if (new_size != temp->size) {
            size_t blk_size; /* Temporary block size */

            if ((ret_value = H5FL_blk_malloc(head, new_size H5FL_TRACK_INFO_INT)) == NULL)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for block");
            blk_size = MIN(new_size, temp->size);
            H5MM_memcpy(ret_value, block, blk_size);
            H5FL_blk_free(head, block);
        } /* end if */
        else {
#ifdef H5FL_TRACK
            {
                unsigned char *block_ptr = ((unsigned char *)block) - sizeof(H5FL_track_t);
                H5FL_track_t   trk;

                H5MM_memcpy(&trk, block_ptr, sizeof(H5FL_track_t));

                /* Release previous tracking information */
                H5CS_close_stack(trk.stack);
                /* The 'func' & 'file' strings are statically allocated (by the compiler)
                 * and are not allocated, so there's no need to free them.
                 */
                trk.file = NULL;
                trk.func = NULL;

                /* Store new tracking information */
                trk.stack = H5CS_copy_stack();
                assert(trk.stack);
                /* The 'call_func' & 'call_file' strings are statically allocated (by the compiler)
                 * there's no need to duplicate them.
                 */
                trk.file = call_file;
                trk.func = call_func;
                trk.line = call_line;

                H5MM_memcpy(block_ptr, &trk, sizeof(H5FL_track_t));
            }
#endif /* H5FL_TRACK */
            ret_value = block;
        } /* end if */
    }     /* end if */
    /* Not re-allocating, just allocate a fresh block */
    else
        ret_value = H5FL_blk_malloc(head, new_size H5FL_TRACK_INFO_INT);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_blk_realloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__blk_gc_list
 *
 * Purpose:	Garbage collect a priority queue
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__blk_gc_list(H5FL_blk_head_t *head)
{
    H5FL_blk_node_t *blk_head; /* Temp. ptr to the free list page node */

    FUNC_ENTER_PACKAGE_NOERR

    /* Loop through all the nodes in the block free list queue */
    blk_head = head->head;
    while (blk_head != NULL) {
        H5FL_blk_node_t *blk_next; /* Temp. ptr to the next free list node */
        H5FL_blk_list_t *list;     /* The free list of native nodes of a particular size */

        /* Sanity check */
        assert((blk_head->onlist && blk_head->list) || (0 == blk_head->onlist && NULL == blk_head->list));

        /* Loop through all the blocks in the free list, freeing them */
        list = blk_head->list;
        while (list != NULL) {
            H5FL_blk_list_t *next; /* Temp. ptr to the free list list node */

            /* Get the pointer to the next node */
            next = list->next;

            /* Free the block */
            H5MM_free(list);

            /* Advance to the next node */
            list = next;
        } /* end while */

        /* Decrement the number of blocks allocated from this list */
        blk_head->allocated -= blk_head->onlist;
        head->allocated -= blk_head->onlist;

        /* Decrement count of free memory on this "block" list */
        head->list_mem -= (blk_head->onlist * blk_head->size);

        /* Decrement global count of free memory on "block" lists */
        H5FL_blk_gc_head.mem_freed -= (blk_head->onlist * blk_head->size);

        /* Indicate no free nodes on the free list */
        blk_head->list   = NULL;
        blk_head->onlist = 0;

        /* Get pointer to next node */
        blk_next = blk_head->next;

        /* Check for list completely unused now */
        if (0 == blk_head->allocated) {
            /* Patch this node out of the PQ */
            if (head->head == blk_head)
                head->head = blk_head->next;
            if (blk_head->prev)
                blk_head->prev->next = blk_head->next;
            if (blk_head->next)
                blk_head->next->prev = blk_head->prev;

            /* Free the free list node */
            H5FL_FREE(H5FL_blk_node_t, blk_head);
        } /* end if */

        /* Advance to the next node */
        blk_head = blk_next;
    } /* end while */

    /* Indicate no free nodes on the free list */
    head->onlist = 0;

    /* Double check that all the memory on this list is recycled */
    assert(0 == head->list_mem);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FL__blk_gc_list() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__blk_gc
 *
 * Purpose:	Garbage collect on all the priority queues
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__blk_gc(void)
{
    H5FL_blk_gc_node_t *gc_node;             /* Pointer into the list of things to garbage collect */
    herr_t              ret_value = SUCCEED; /* return value*/

    FUNC_ENTER_PACKAGE

    /* Walk through all the free lists, free()'ing the nodes */
    gc_node = H5FL_blk_gc_head.first;
    while (gc_node != NULL) {
        /* For each free list being garbage collected, walk through the nodes and free them */
        if (H5FL__blk_gc_list(gc_node->pq) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "garbage collection of list failed");

        /* Go on to the next free list to garbage collect */
        gc_node = gc_node->next;
    } /* end while */

    /* Double check that all the memory on the free lists are recycled */
    assert(H5FL_blk_gc_head.mem_freed == 0);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__blk_gc() */

/*--------------------------------------------------------------------------
 NAME
    H5FL__blk_term
 PURPOSE
    Terminate various H5FL_blk objects
 USAGE
    void H5FL__blk_term()
 RETURNS
    Success:	Positive if any action might have caused a change in some
                other interface; zero otherwise.
        Failure:	Negative
 DESCRIPTION
    Release any resources allocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     Can't report errors...
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5FL__blk_term(void)
{
    H5FL_blk_gc_node_t *left; /* pointer to garbage collection lists with work left */

    FUNC_ENTER_PACKAGE_NOERR

    /* Free the nodes on the garbage collection list, keeping nodes with allocations outstanding */
    left = NULL;
    while (H5FL_blk_gc_head.first != NULL) {
        H5FL_blk_gc_node_t *tmp; /* Temporary pointer to a garbage collection node */

        tmp = H5FL_blk_gc_head.first->next;

#ifdef H5FL_DEBUG
        printf("%s: head->name = %s, head->allocated = %d\n", __func__, H5FL_blk_gc_head.first->pq->name,
               (int)H5FL_blk_gc_head.first->pq->allocated);
#endif /* H5FL_DEBUG */

        /* Check if the list has allocations outstanding */
        if (H5FL_blk_gc_head.first->pq->allocated > 0) {
            /* Add free list to the list of nodes with allocations open still */
            H5FL_blk_gc_head.first->next = left;
            left                         = H5FL_blk_gc_head.first;
        } /* end if */
        /* No allocations left open for list, get rid of it */
        else {
            /* Reset the "initialized" flag, in case we restart this list somehow (I don't know how..) */
            H5FL_blk_gc_head.first->pq->init = false;

            /* Free the node from the garbage collection list */
            H5MM_free(H5FL_blk_gc_head.first);
        } /* end else */

        H5FL_blk_gc_head.first = tmp;
    } /* end while */

    /* Point to the list of nodes left with allocations open, if any */
    H5FL_blk_gc_head.first = left;

    FUNC_LEAVE_NOAPI(H5FL_blk_gc_head.first != NULL ? 1 : 0)
} /* end H5FL__blk_term() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__arr_init
 *
 * Purpose:	Initialize a free list for a arrays of certain type.  Right now,
 *      this just adds the free list to the list of things to garbage collect.
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__arr_init(H5FL_arr_head_t *head)
{
    H5FL_gc_arr_node_t *new_node;            /* Pointer to the node for the new list to garbage collect */
    size_t              u;                   /* Local index variable */
    herr_t              ret_value = SUCCEED; /* return value*/

    FUNC_ENTER_PACKAGE

    /* Allocate a new garbage collection node */
    if (NULL == (new_node = (H5FL_gc_arr_node_t *)H5MM_malloc(sizeof(H5FL_gc_arr_node_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Initialize the new garbage collection node */
    new_node->list = head;

    /* Link in to the garbage collection list */
    new_node->next         = H5FL_arr_gc_head.first;
    H5FL_arr_gc_head.first = new_node;

    /* Allocate room for the free lists */
    if (NULL ==
        (head->list_arr = (H5FL_arr_node_t *)H5MM_calloc((size_t)head->maxelem * sizeof(H5FL_arr_node_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Initialize the size of each array */
    for (u = 0; u < (size_t)head->maxelem; u++)
        head->list_arr[u].size = head->base_size + (head->elem_size * u);

    /* Indicate that the free list is initialized */
    head->init = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__arr_init() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_arr_free
 *
 * Purpose:	Release an array of objects & put on free list
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_arr_free(H5FL_arr_head_t *head, void *obj)
{
    H5FL_arr_list_t *temp;             /* Temp. ptr to the new free list node allocated */
    size_t           mem_size;         /* Size of memory being freed */
    size_t           free_nelem;       /* Number of elements in node being free'd */
    void            *ret_value = NULL; /* Return value */

    /* NOINIT OK here because this must be called after H5FL_arr_malloc/calloc
     * -NAF */
    FUNC_ENTER_NOAPI_NOINIT

    /* The H5MM_xfree code allows obj to null */
    if (!obj)
        HGOTO_DONE(NULL);

    /* Double check parameters */
    assert(head);

    /* Make certain that the free list is initialized */
    assert(head->init);

#ifdef H5FL_TRACK
    {
        unsigned char *block_ptr = ((unsigned char *)obj) - sizeof(H5FL_track_t);
        H5FL_track_t   trk;

        H5MM_memcpy(&trk, block_ptr, sizeof(H5FL_track_t));

        /* Free tracking information about the allocation location */
        H5CS_close_stack(trk.stack);
        /* The 'func' & 'file' strings are statically allocated (by the compiler)
         * and are not allocated, so there's no need to free them.
         */
        trk.file = NULL;
        trk.func = NULL;

        /* Remove from "outstanding allocations" list */
        if ((void *)block_ptr == H5FL_out_head_g) {
            H5FL_out_head_g = H5FL_out_head_g->next;
            if (H5FL_out_head_g)
                H5FL_out_head_g->prev = NULL;
        } /* end if */
        else {
            trk.prev->next = trk.next;
            if (trk.next)
                trk.next->prev = trk.prev;
        } /* end else */

        H5MM_memcpy(block_ptr, &trk, sizeof(H5FL_track_t));
    }
#endif

    /* Get the pointer to the info header in front of the block to free */
    temp = (H5FL_arr_list_t *)((
        void *)((unsigned char *)obj -
                (sizeof(H5FL_arr_list_t) +
                 H5FL_TRACK_SIZE))); /*lint !e826 Pointer-to-pointer cast is appropriate here */

    /* Get the number of elements */
    free_nelem = temp->nelem;

    /* Double-check that there is enough room for arrays of this size */
    assert((int)free_nelem <= head->maxelem);

    /* Link into the free list */
    temp->next = head->list_arr[free_nelem].list;

    /* Point free list at the node freed */
    head->list_arr[free_nelem].list = temp;

    /* Get the size of arrays with this many elements */
    mem_size = head->list_arr[free_nelem].size;

    /* Increment the number of blocks & memory used on free list */
    head->list_arr[free_nelem].onlist++;
    head->list_mem += mem_size;

    /* Increment the amount of "array" freed memory globally */
    H5FL_arr_gc_head.mem_freed += mem_size;

    /* Check for exceeding free list memory use limits */
    /* First check this particular list */
    if (head->list_mem > H5FL_arr_lst_mem_lim)
        if (H5FL__arr_gc_list(head) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

    /* Then check the global amount memory on array free lists */
    if (H5FL_arr_gc_head.mem_freed > H5FL_arr_glb_mem_lim)
        if (H5FL__arr_gc() < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_arr_free() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_arr_malloc
 *
 * Purpose:	Allocate an array of objects
 *
 * Return:	Success:	Pointer to a valid array object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_arr_malloc(H5FL_arr_head_t *head, size_t elem H5FL_TRACK_PARAMS)
{
    H5FL_arr_list_t *new_obj;          /* Pointer to the new free list node allocated */
    size_t           mem_size;         /* Size of memory block being recycled */
    void            *ret_value = NULL; /* Pointer to the block to return */

    FUNC_ENTER_NOAPI(NULL)

    /* Double check parameters */
    assert(head);
    assert(elem);

    /* Make certain the list is initialized first */
    if (!head->init)
        if (H5FL__arr_init(head) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, NULL, "can't initialize 'array' blocks");

    /* Sanity check that the number of elements is supported */
    assert(elem <= (unsigned)head->maxelem);

    /* Get the set of the memory block */
    mem_size = head->list_arr[elem].size;

    /* Check for nodes available on the free list first */
    if (head->list_arr[elem].list != NULL) {
        /* Get a pointer to the block on the free list */
        new_obj = head->list_arr[elem].list;

        /* Remove node from free list */
        head->list_arr[elem].list = head->list_arr[elem].list->next;

        /* Decrement the number of blocks & memory used on free list */
        head->list_arr[elem].onlist--;
        head->list_mem -= mem_size;

        /* Decrement the amount of global "array" free list memory in use */
        H5FL_arr_gc_head.mem_freed -= mem_size;

    } /* end if */
    /* Otherwise allocate a node */
    else {
        if (NULL ==
            (new_obj = (H5FL_arr_list_t *)H5FL__malloc(sizeof(H5FL_arr_list_t) + H5FL_TRACK_SIZE + mem_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

        /* Increment the number of blocks of this size */
        head->list_arr[elem].allocated++;

        /* Increment the number of blocks allocated in list, of all sizes */
        head->allocated++;
    } /* end else */

    /* Initialize the new object */
    new_obj->nelem = elem;

    /* Get a pointer to the new block */
    ret_value = ((char *)new_obj) + sizeof(H5FL_arr_list_t);

#ifdef H5FL_TRACK
    /* Copy allocation location information */
    ((H5FL_track_t *)ret_value)->stack = H5CS_copy_stack();
    assert(((H5FL_track_t *)ret_value)->stack);
    /* The 'call_func' & 'call_file' strings are statically allocated (by the compiler)
     * there's no need to duplicate them.
     */
    ((H5FL_track_t *)ret_value)->file = call_file;
    ((H5FL_track_t *)ret_value)->func = call_func;
    ((H5FL_track_t *)ret_value)->line = call_line;

    /* Add to "outstanding allocations" list */
    ((H5FL_track_t *)ret_value)->prev = NULL;
    ((H5FL_track_t *)ret_value)->next = H5FL_out_head_g;
    if (H5FL_out_head_g)
        H5FL_out_head_g->prev = (H5FL_track_t *)ret_value;
    H5FL_out_head_g = (H5FL_track_t *)ret_value;

    /* Adjust for allocation tracking information */
    ret_value = ((unsigned char *)ret_value) + sizeof(H5FL_track_t);
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_arr_malloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_arr_calloc
 *
 * Purpose:	Allocate an array of objects and clear it to zeros
 *
 * Return:	Success:	Pointer to a valid array object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_arr_calloc(H5FL_arr_head_t *head, size_t elem H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return */

    FUNC_ENTER_NOAPI(NULL)

    /* Double check parameters */
    assert(head);
    assert(elem);

    /* Allocate the array */
    if (NULL == (ret_value = H5FL_arr_malloc(head, elem H5FL_TRACK_INFO_INT)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Clear to zeros */
    memset(ret_value, 0, head->list_arr[elem].size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_arr_calloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_arr_realloc
 *
 * Purpose:	Reallocate an array of objects
 *
 * Return:	Success:	Pointer to a valid array object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_arr_realloc(H5FL_arr_head_t *head, void *obj, size_t new_elem H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return */

    FUNC_ENTER_NOAPI_NOERR

    /* Double check parameters */
    assert(head);
    assert(new_elem);

    /* Check if we are really allocating the object */
    if (obj == NULL)
        ret_value = H5FL_arr_malloc(head, new_elem H5FL_TRACK_INFO_INT);
    else {
        H5FL_arr_list_t *temp; /* Temp. ptr to the new free list node allocated */

        /* Sanity check that the number of elements is supported */
        assert((int)new_elem <= head->maxelem);

        /* Get the pointer to the info header in front of the block to free */
        temp = (H5FL_arr_list_t *)((
            void *)((unsigned char *)obj -
                    (sizeof(H5FL_arr_list_t) +
                     H5FL_TRACK_SIZE))); /*lint !e826 Pointer-to-pointer cast is appropriate here */

        /* Check if the size is really changing */
        if (temp->nelem != new_elem) {
            size_t blk_size; /* Size of block */

            /* Get the new array of objects */
            ret_value = H5FL_arr_malloc(head, new_elem H5FL_TRACK_INFO_INT);

            /* Copy the appropriate amount of elements */
            blk_size = head->list_arr[MIN(temp->nelem, new_elem)].size;
            H5MM_memcpy(ret_value, obj, blk_size);

            /* Free the old block */
            H5FL_arr_free(head, obj);
        } /* end if */
        else {
#ifdef H5FL_TRACK
            unsigned char *block_ptr = ((unsigned char *)obj) - sizeof(H5FL_track_t);
            H5FL_track_t   trk;

            H5MM_memcpy(&trk, block_ptr, sizeof(H5FL_track_t));

            /* Release previous tracking information */
            H5CS_close_stack(trk.stack);
            /* The 'func' & 'file' strings are statically allocated (by the compiler)
             * and are not allocated, so there's no need to free them.
             */
            trk.file = NULL;
            trk.func = NULL;

            /* Store new tracking information */
            trk.stack = H5CS_copy_stack();
            assert(trk.stack);
            /* The 'call_func' & 'call_file' strings are statically allocated (by the compiler)
             * there's no need to duplicate them.
             */
            trk.file = call_file;
            trk.func = call_func;
            trk.line = call_line;

            H5MM_memcpy(block_ptr, &trk, sizeof(H5FL_track_t));
#endif
            ret_value = obj;
        }
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_arr_realloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__arr_gc_list
 *
 * Purpose:	Garbage collect on an array object free list
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__arr_gc_list(H5FL_arr_head_t *head)
{
    unsigned u; /* Counter for array of free lists */

    FUNC_ENTER_PACKAGE_NOERR

    /* Walk through the array of free lists */
    for (u = 0; u < (unsigned)head->maxelem; u++) {
        if (head->list_arr[u].onlist > 0) {
            H5FL_arr_list_t *arr_free_list; /* Pointer to nodes in free list being garbage collected */

            /* For each free list being garbage collected, walk through the nodes and free them */
            arr_free_list = head->list_arr[u].list;
            while (arr_free_list != NULL) {
                H5FL_arr_list_t *tmp; /* Temporary node pointer */

                /* Get the pointer to the next node */
                tmp = arr_free_list->next;

                /* Free the node */
                H5MM_free(arr_free_list);

                /* Advance to the next node */
                arr_free_list = tmp;
            } /* end while */

            /* Decrement the count of nodes allocated */
            head->list_arr[u].allocated -= head->list_arr[u].onlist;
            head->allocated -= head->list_arr[u].onlist;

            /* Decrement count of free memory on this "array" list */
            head->list_mem -= (head->list_arr[u].onlist * head->list_arr[u].size);

            /* Decrement global count of free memory on "array" lists */
            H5FL_arr_gc_head.mem_freed -= (head->list_arr[u].onlist * head->list_arr[u].size);

            /* Indicate no free nodes on the free list */
            head->list_arr[u].list   = NULL;
            head->list_arr[u].onlist = 0;
        } /* end if */
    }     /* end for */

    /* Double check that all the memory on this list is recycled */
    assert(head->list_mem == 0);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FL__arr_gc_list() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__arr_gc
 *
 * Purpose:	Garbage collect on all the array object free lists
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__arr_gc(void)
{
    H5FL_gc_arr_node_t *gc_arr_node;         /* Pointer into the list of things to garbage collect */
    herr_t              ret_value = SUCCEED; /* return value*/

    FUNC_ENTER_PACKAGE

    /* Walk through all the free lists, free()'ing the nodes */
    gc_arr_node = H5FL_arr_gc_head.first;
    while (gc_arr_node != NULL) {
        /* Release the free nodes on the list */
        if (H5FL__arr_gc_list(gc_arr_node->list) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "garbage collection of list failed");

        /* Go on to the next free list to garbage collect */
        gc_arr_node = gc_arr_node->next;
    } /* end while */

    /* Double check that all the memory on the free lists are recycled */
    assert(H5FL_arr_gc_head.mem_freed == 0);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__arr_gc() */

/*--------------------------------------------------------------------------
 NAME
    H5FL__arr_term
 PURPOSE
    Terminate various H5FL array object free lists
 USAGE
    int H5FL__arr_term()
 RETURNS
    Success:	Positive if any action might have caused a change in some
                other interface; zero otherwise.
        Failure:	Negative
 DESCRIPTION
    Release any resources allocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     Can't report errors...
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5FL__arr_term(void)
{
    H5FL_gc_arr_node_t *left; /* pointer to garbage collection lists with work left */

    FUNC_ENTER_PACKAGE_NOERR

    /* Free the nodes on the garbage collection list, keeping nodes with allocations outstanding */
    left = NULL;
    while (H5FL_arr_gc_head.first != NULL) {
        H5FL_gc_arr_node_t *tmp; /* Temporary pointer to a garbage collection node */

        tmp = H5FL_arr_gc_head.first->next;

        /* Check if the list has allocations outstanding */
#ifdef H5FL_DEBUG
        printf("%s: head->name = %s, head->allocated = %d\n", __func__, H5FL_arr_gc_head.first->list->name,
               (int)H5FL_arr_gc_head.first->list->allocated);
#endif /* H5FL_DEBUG */
        if (H5FL_arr_gc_head.first->list->allocated > 0) {
            /* Add free list to the list of nodes with allocations open still */
            H5FL_arr_gc_head.first->next = left;
            left                         = H5FL_arr_gc_head.first;
        } /* end if */
        /* No allocations left open for list, get rid of it */
        else {
            /* Free the array of free lists */
            H5MM_xfree(H5FL_arr_gc_head.first->list->list_arr);

            /* Reset the "initialized" flag, in case we restart this list somehow (I don't know how..) */
            H5FL_arr_gc_head.first->list->init = false;

            /* Free the node from the garbage collection list */
            H5MM_free(H5FL_arr_gc_head.first);
        } /* end else */

        H5FL_arr_gc_head.first = tmp;
    } /* end while */

    /* Point to the list of nodes left with allocations open, if any */
    H5FL_arr_gc_head.first = left;

    FUNC_LEAVE_NOAPI(H5FL_arr_gc_head.first != NULL ? 1 : 0)
} /* end H5FL__arr_term() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_seq_free
 *
 * Purpose:	Release a sequence of objects & put on free list
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_seq_free(H5FL_seq_head_t *head, void *obj)
{
    /* NOINIT OK here because this must be called after H5FL_seq_malloc/calloc
     * -NAF */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Double check parameters */
    assert(head);
    assert(obj);

    /* Make certain that the free list is initialized */
    assert(head->queue.init);

    /* Use block routine */
    H5FL_blk_free(&(head->queue), obj);

    FUNC_LEAVE_NOAPI(NULL)
} /* end H5FL_seq_free() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_seq_malloc
 *
 * Purpose:	Allocate a sequence of objects
 *
 * Return:	Success:	Pointer to a valid sequence object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_seq_malloc(H5FL_seq_head_t *head, size_t elem H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return */

    FUNC_ENTER_NOAPI_NOERR

    /* Double check parameters */
    assert(head);
    assert(elem);

    /* Use block routine */
    ret_value = H5FL_blk_malloc(&(head->queue), head->size * elem H5FL_TRACK_INFO_INT);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_seq_malloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_seq_calloc
 *
 * Purpose:	Allocate a sequence of objects and clear it to zeros
 *
 * Return:	Success:	Pointer to a valid array object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_seq_calloc(H5FL_seq_head_t *head, size_t elem H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return */

    FUNC_ENTER_NOAPI_NOERR

    /* Double check parameters */
    assert(head);
    assert(elem);

    /* Use block routine */
    ret_value = H5FL_blk_calloc(&(head->queue), head->size * elem H5FL_TRACK_INFO_INT);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_seq_calloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_seq_realloc
 *
 * Purpose:	Reallocate a sequence of objects
 *
 * Return:	Success:	Pointer to a valid sequence object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_seq_realloc(H5FL_seq_head_t *head, void *obj, size_t new_elem H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return */

    FUNC_ENTER_NOAPI_NOERR

    /* Double check parameters */
    assert(head);
    assert(new_elem);

    /* Use block routine */
    ret_value = H5FL_blk_realloc(&(head->queue), obj, head->size * new_elem H5FL_TRACK_INFO_INT);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_seq_realloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_fac_init
 *
 * Purpose:	Initialize a block factory
 *
 * Return:	Success:	Pointer to factory object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5FL_fac_head_t *
H5FL_fac_init(size_t size)
{
    H5FL_fac_gc_node_t *new_node  = NULL; /* Pointer to the node for the new list to garbage collect */
    H5FL_fac_head_t    *factory   = NULL; /* Pointer to new block factory */
    H5FL_fac_head_t    *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Sanity check */
    assert(size > 0);

    /* Allocate room for the new factory */
    if (NULL == (factory = (H5FL_fac_head_t *)H5FL_CALLOC(H5FL_fac_head_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for factory object");

    /* Set size of blocks for factory */
    factory->size = size;

    /* Allocate a new garbage collection node */
    if (NULL == (new_node = (H5FL_fac_gc_node_t *)H5FL_MALLOC(H5FL_fac_gc_node_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Initialize the new garbage collection node */
    new_node->list = factory;

    /* Link in to the garbage collection list */
    new_node->next         = H5FL_fac_gc_head.first;
    H5FL_fac_gc_head.first = new_node;
    if (new_node->next)
        new_node->next->list->prev_gc = new_node;
    /* The new factory's prev_gc field will be set to NULL */

    /* Make certain that the space allocated is large enough to store a free list pointer (eventually) */
    if (factory->size < sizeof(H5FL_fac_node_t))
        factory->size = sizeof(H5FL_fac_node_t);

        /* Make certain there's room for tracking information, if any */
#ifdef H5FL_TRACK
    factory->size += sizeof(H5FL_track_t);
#endif /* H5FL_TRACK */

    /* Indicate that the free list is initialized */
    factory->init = true;

    /* Set return value */
    ret_value = factory;

done:
    if (!ret_value) {
        if (factory)
            factory = H5FL_FREE(H5FL_fac_head_t, factory);
        if (new_node)
            new_node = H5FL_FREE(H5FL_fac_gc_node_t, new_node);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_fac_init() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_fac_free
 *
 * Purpose:	Release a block back to a factory & put on free list
 *
 * Return:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_fac_free(H5FL_fac_head_t *head, void *obj)
{
    void *ret_value = NULL; /* Return value */

    /* NOINIT OK here because this must be called after H5FL_fac_init -NAF */
    FUNC_ENTER_NOAPI_NOINIT

    /* Double check parameters */
    assert(head);
    assert(obj);

#ifdef H5FL_TRACK
    {
        H5FL_track_t *trk = obj = ((unsigned char *)obj) - sizeof(H5FL_track_t);

        /* Free tracking information about the allocation location */
        H5CS_close_stack(trk->stack);
        /* The 'func' & 'file' strings are statically allocated (by the compiler)
         * and are not allocated, so there's no need to free them.
         */
        trk->file = NULL;
        trk->func = NULL;

        /* Remove from "outstanding allocations" list */
        if (trk == H5FL_out_head_g) {
            H5FL_out_head_g = H5FL_out_head_g->next;
            if (H5FL_out_head_g)
                H5FL_out_head_g->prev = NULL;
        } /* end if */
        else {
            trk->prev->next = trk->next;
            if (trk->next)
                trk->next->prev = trk->prev;
        } /* end else */
    }
#endif /* H5FL_TRACK */

#ifdef H5FL_DEBUG
    memset(obj, 255, head->size);
#endif /* H5FL_DEBUG */

    /* Make certain that the free list is initialized */
    assert(head->init);

    /* Link into the free list */
    ((H5FL_fac_node_t *)obj)->next = head->list;

    /* Point free list at the node freed */
    head->list = (H5FL_fac_node_t *)obj;

    /* Increment the number of blocks on free list */
    head->onlist++;

    /* Increment the amount of "factory" freed memory globally */
    H5FL_fac_gc_head.mem_freed += head->size;

    /* Check for exceeding free list memory use limits */
    /* First check this particular list */
    if (head->onlist * head->size > H5FL_fac_lst_mem_lim)
        if (H5FL__fac_gc_list(head) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

    /* Then check the global amount memory on factory free lists */
    if (H5FL_fac_gc_head.mem_freed > H5FL_fac_glb_mem_lim)
        if (H5FL__fac_gc() < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, NULL, "garbage collection failed during free");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_fac_free() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_fac_malloc
 *
 * Purpose:	Allocate a block from a factory
 *
 * Return:	Success:	Pointer to a valid sequence object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_fac_malloc(H5FL_fac_head_t *head H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return */

    /* NOINIT OK here because this must be called after H5FL_fac_init -NAF */
    FUNC_ENTER_NOAPI_NOINIT

    /* Double check parameters */
    assert(head);
    assert(head->init);

    /* Check for nodes available on the free list first */
    if (head->list != NULL) {
        /* Get a pointer to the block on the free list */
        ret_value = (void *)(head->list);

        /* Remove node from free list */
        head->list = head->list->next;

        /* Decrement the number of blocks & memory on free list */
        head->onlist--;

        /* Decrement the amount of global "factory" free list memory in use */
        H5FL_fac_gc_head.mem_freed -= (head->size);
    } /* end if */
    /* Otherwise allocate a node */
    else {
        if (NULL == (ret_value = H5FL__malloc(head->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

        /* Increment the number of blocks allocated in list */
        head->allocated++;
    } /* end else */

#ifdef H5FL_TRACK
    /* Copy allocation location information */
    ((H5FL_track_t *)ret_value)->stack = H5CS_copy_stack();
    assert(((H5FL_track_t *)ret_value)->stack);
    /* The 'call_func' & 'call_file' strings are statically allocated (by the compiler)
     * there's no need to duplicate them.
     */
    ((H5FL_track_t *)ret_value)->file = call_file;
    ((H5FL_track_t *)ret_value)->func = call_func;
    ((H5FL_track_t *)ret_value)->line = call_line;

    /* Add to "outstanding allocations" list */
    ((H5FL_track_t *)ret_value)->prev = NULL;
    ((H5FL_track_t *)ret_value)->next = H5FL_out_head_g;
    if (H5FL_out_head_g)
        H5FL_out_head_g->prev = (H5FL_track_t *)ret_value;
    H5FL_out_head_g = (H5FL_track_t *)ret_value;

    /* Adjust for allocation tracking information */
    ret_value = ((unsigned char *)ret_value) + sizeof(H5FL_track_t);
#endif /* H5FL_TRACK */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_fac_malloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_fac_calloc
 *
 * Purpose:	Allocate a block from a factory and clear it to zeros
 *
 * Return:	Success:	Pointer to a valid array object
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5FL_fac_calloc(H5FL_fac_head_t *head H5FL_TRACK_PARAMS)
{
    void *ret_value = NULL; /* Pointer to the block to return */

    /* NOINIT OK here because this must be called after H5FL_fac_init -NAF */
    FUNC_ENTER_NOAPI_NOINIT

    /* Double check parameters */
    assert(head);

    /* Allocate the block */
    if (NULL == (ret_value = H5FL_fac_malloc(head H5FL_TRACK_INFO_INT)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Clear to zeros */
    /* (Accommodate tracking information, if present) */
    memset(ret_value, 0, head->size - H5FL_TRACK_SIZE);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_fac_calloc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__fac_gc_list
 *
 * Purpose:	Garbage collect on a particular factory free list
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__fac_gc_list(H5FL_fac_head_t *head)
{
    H5FL_fac_node_t *free_list; /* Pointer to nodes in free list being garbage collected */

    FUNC_ENTER_PACKAGE_NOERR

    /* For each free list being garbage collected, walk through the nodes and free them */
    free_list = head->list;
    while (free_list != NULL) {
        H5FL_fac_node_t *tmp; /* Temporary node pointer */

        /* Get the pointer to the next node */
        tmp = free_list->next;

        /* Free the block */
        H5MM_free(free_list);

        /* Advance to the next node */
        free_list = tmp;
    } /* end while */

    /* Decrement the count of nodes allocated and free the node */
    head->allocated -= head->onlist;

    /* Decrement global count of free memory on "factory" lists */
    H5FL_fac_gc_head.mem_freed -= (head->onlist * head->size);

    /* Indicate no free nodes on the free list */
    head->list   = NULL;
    head->onlist = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FL__fac_gc_list() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__fac_gc
 *
 * Purpose:	Garbage collect on all the factory free lists
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FL__fac_gc(void)
{
    H5FL_fac_gc_node_t *gc_node;             /* Pointer into the list of things to garbage collect */
    herr_t              ret_value = SUCCEED; /* return value*/

    FUNC_ENTER_NOAPI_NOINIT

    /* Walk through all the free lists, free()'ing the nodes */
    gc_node = H5FL_fac_gc_head.first;
    while (gc_node != NULL) {
        /* Release the free nodes on the list */
        if (H5FL__fac_gc_list(gc_node->list) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "garbage collection of list failed");

        /* Go on to the next free list to garbage collect */
        gc_node = gc_node->next;
    } /* end while */

    /* Double check that all the memory on the free lists is recycled */
    assert(H5FL_fac_gc_head.mem_freed == 0);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL__fac_gc() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_fac_term
 *
 * Purpose:	Terminate a block factory
 *
 * Return:	Success:	non-negative
 * 		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FL_fac_term(H5FL_fac_head_t *factory)
{
    H5FL_fac_gc_node_t *tmp;                 /* Temporary pointer to a garbage collection node */
    herr_t              ret_value = SUCCEED; /* Return value */

    /* NOINIT OK here because this must be called after H5FL_fac_init -NAF */
    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    assert(factory);

    /* Garbage collect all the blocks in the factory's free list */
    if (H5FL__fac_gc_list(factory) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "garbage collection of factory failed");

    /* Verify that all the blocks have been freed */
    if (factory->allocated > 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "factory still has objects allocated");

    /* Unlink block free list for factory from global free list */
    if (factory->prev_gc) {
        H5FL_fac_gc_node_t *last =
            factory->prev_gc; /* Garbage collection node before the one being removed */

        assert(last->next->list == factory);
        tmp        = last->next->next;
        last->next = H5FL_FREE(H5FL_fac_gc_node_t, last->next);
        last->next = tmp;
        if (tmp)
            tmp->list->prev_gc = last;
    }
    else {
        assert(H5FL_fac_gc_head.first->list == factory);
        tmp                    = H5FL_fac_gc_head.first->next;
        H5FL_fac_gc_head.first = H5FL_FREE(H5FL_fac_gc_node_t, H5FL_fac_gc_head.first);
        H5FL_fac_gc_head.first = tmp;
        if (tmp)
            tmp->list->prev_gc = NULL;
    } /* end else */

    /* Free factory info */
    factory = H5FL_FREE(H5FL_fac_head_t, factory);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_fac_term() */

/*-------------------------------------------------------------------------
 * Function:	H5FL__fac_term_all
 *
 * Purpose:	Terminate all block factories
 *
 * Return:	0.  There should never be any outstanding allocations
 *              when this is called.
 *
 *-------------------------------------------------------------------------
 */
static int
H5FL__fac_term_all(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Free the nodes on the garbage collection list */
    while (H5FL_fac_gc_head.first != NULL) {
        H5FL_fac_gc_node_t *tmp; /* Temporary pointer to a garbage collection node */

        tmp = H5FL_fac_gc_head.first->next;

#ifdef H5FL_DEBUG
        printf("%s: head->size = %d, head->allocated = %d\n", __func__,
               (int)H5FL_fac_gc_head.first->list->size, (int)H5FL_fac_gc_head.first->list->allocated);
#endif /* H5FL_DEBUG */

        /* The list cannot have any allocations outstanding */
        assert(H5FL_fac_gc_head.first->list->allocated == 0);

        /* Reset the "initialized" flag, in case we restart this list somehow (I don't know how..) */
        H5FL_fac_gc_head.first->list->init = false;

        /* Free the node from the garbage collection list */
        H5FL_fac_gc_head.first = H5FL_FREE(H5FL_fac_gc_node_t, H5FL_fac_gc_head.first);

        H5FL_fac_gc_head.first = tmp;
    } /* end while */

    FUNC_LEAVE_NOAPI(0)
} /* end H5FL__fac_term_all() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_garbage_coll
 *
 * Purpose:	Garbage collect on all the free lists
 *
 * Return:	Success:	Non-negative
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FL_garbage_coll(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Garbage collect the free lists for array objects */
    if (H5FL__arr_gc() < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "can't garbage collect array objects");

    /* Garbage collect free lists for blocks */
    if (H5FL__blk_gc() < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "can't garbage collect block objects");

    /* Garbage collect the free lists for regular objects */
    if (H5FL__reg_gc() < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "can't garbage collect regular objects");

    /* Garbage collect the free lists for factory objects */
    if (H5FL__fac_gc() < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGC, FAIL, "can't garbage collect factory objects");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_garbage_coll() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_set_free_list_limits
 *
 * Purpose:	Sets limits on the different kinds of free lists.  Setting a value
 *      of -1 for a limit means no limit of that type.  These limits are global
 *      for the entire library.  Each "global" limit only applies to free lists
 *      of that type, so if an application sets a limit of 1 MB on each of the
 *      global lists, up to 3 MB of total storage might be allocated (1MB on
 *      each of regular, array and block type lists).
 *
 * Parameters:
 *  int reg_global_lim;  IN: The limit on all "regular" free list memory used
 *  int reg_list_lim;    IN: The limit on memory used in each "regular" free list
 *  int arr_global_lim;  IN: The limit on all "array" free list memory used
 *  int arr_list_lim;    IN: The limit on memory used in each "array" free list
 *  int blk_global_lim;  IN: The limit on all "block" free list memory used
 *  int blk_list_lim;    IN: The limit on memory used in each "block" free list
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FL_set_free_list_limits(int reg_global_lim, int reg_list_lim, int arr_global_lim, int arr_list_lim,
                          int blk_global_lim, int blk_list_lim, int fac_global_lim, int fac_list_lim)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Set the limit variables */
    /* limit on all regular free lists */
    H5FL_reg_glb_mem_lim = (reg_global_lim == -1 ? UINT_MAX : (size_t)reg_global_lim);
    /* limit on each regular free list */
    H5FL_reg_lst_mem_lim = (reg_list_lim == -1 ? UINT_MAX : (size_t)reg_list_lim);
    /* limit on all array free lists */
    H5FL_arr_glb_mem_lim = (arr_global_lim == -1 ? UINT_MAX : (size_t)arr_global_lim);
    /* limit on each array free list */
    H5FL_arr_lst_mem_lim = (arr_list_lim == -1 ? UINT_MAX : (size_t)arr_list_lim);
    /* limit on all block free lists */
    H5FL_blk_glb_mem_lim = (blk_global_lim == -1 ? UINT_MAX : (size_t)blk_global_lim);
    /* limit on each block free list */
    H5FL_blk_lst_mem_lim = (blk_list_lim == -1 ? UINT_MAX : (size_t)blk_list_lim);
    /* limit on all factory free lists */
    H5FL_fac_glb_mem_lim = (fac_global_lim == -1 ? UINT_MAX : (size_t)fac_global_lim);
    /* limit on each factory free list */
    H5FL_fac_lst_mem_lim = (fac_list_lim == -1 ? UINT_MAX : (size_t)fac_list_lim);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FL_set_free_list_limits() */

/*-------------------------------------------------------------------------
 * Function:	H5FL_get_free_list_sizes
 *
 * Purpose:	Gets the current size of the different kinds of free lists.
 *      These lists are global for the entire library.  The size returned
 *      included nodes that are freed and awaiting garbage collection /
 *      reallocation.
 *
 * Parameters:
 *  size_t *reg_size;    OUT: The current size of all "regular" free list memory used
 *  size_t *arr_size;    OUT: The current size of all "array" free list memory used
 *  size_t *blk_size;    OUT: The current size of all "block" free list memory used
 *  size_t *fac_size;    OUT: The current size of all "factory" free list memory used
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FL_get_free_list_sizes(size_t *reg_size, size_t *arr_size, size_t *blk_size, size_t *fac_size)
{
    FUNC_ENTER_NOAPI_NOERR

    /* Retrieve the amount of "regular" memory used */
    if (reg_size) {
        H5FL_reg_gc_node_t *gc_node; /* Pointer into the list of lists */

        /* Walk through all the free lists, counting the amount of memory */
        *reg_size = 0;
        gc_node   = H5FL_reg_gc_head.first;
        while (gc_node != NULL) {
            H5FL_reg_head_t *reg_list = gc_node->list; /* Head of list */

            /* Sanity check */
            assert(reg_list->init);

            /* Add the amount of memory for this list */
            *reg_size += (reg_list->size * reg_list->allocated);

            /* Go on to the next free list */
            gc_node = gc_node->next;
        } /* end while */
    }     /* end if */

    /* Retrieve the amount of "array" memory used */
    if (arr_size) {
        H5FL_gc_arr_node_t *gc_arr_node; /* Pointer into the list of things to garbage collect */

        /* Walk through all the free lists, counting the amount of memory */
        *arr_size   = 0;
        gc_arr_node = H5FL_arr_gc_head.first;
        while (gc_arr_node != NULL) {
            H5FL_arr_head_t *head = gc_arr_node->list; /* Head of array list elements */

            /* Sanity check */
            assert(head->init);

            /* Check for any allocated elements in this list */
            if (head->allocated > 0) {
                unsigned u;

                /* Walk through the free lists for array sizes */
                for (u = 0; u < (unsigned)head->maxelem; u++)
                    /* Add the amount of memory for this size */
                    *arr_size += head->list_arr[u].allocated * head->list_arr[u].size;
            } /* end if */

            /* Go on to the next free list */
            gc_arr_node = gc_arr_node->next;
        } /* end while */
    }     /* end if */

    /* Retrieve the amount of "block" memory used */
    if (blk_size) {
        H5FL_blk_gc_node_t *gc_blk_node; /* Pointer into the list of things */

        /* Walk through all the free lists */
        gc_blk_node = H5FL_blk_gc_head.first;
        *blk_size   = 0;
        while (gc_blk_node != NULL) {
            H5FL_blk_node_t *blk_head; /* Temp. ptr to the free list block node */

            /* Loop through all the nodes in the block free list queue */
            blk_head = gc_blk_node->pq->head;
            while (blk_head != NULL) {
                /* Add size of blocks on this list */
                *blk_size += (blk_head->allocated * blk_head->size);

                /* Get pointer to next node */
                blk_head = blk_head->next;
            } /* end while */

            /* Go on to the next free list */
            gc_blk_node = gc_blk_node->next;
        } /* end while */
    }     /* end if */

    /* Retrieve the amount of "factory" memory used */
    if (fac_size) {
        H5FL_fac_gc_node_t *gc_fac_node; /* Pointer into the list of things to garbage collect */

        /* Walk through all the free lists */
        gc_fac_node = H5FL_fac_gc_head.first;
        *fac_size   = 0;
        while (gc_fac_node != NULL) {
            H5FL_fac_head_t *fac_head = gc_fac_node->list; /* Head node for factory list */

            /* Add size of blocks on this list */
            *fac_size += (fac_head->allocated * fac_head->size);

            /* Go on to the next free list to garbage collect */
            gc_fac_node = gc_fac_node->next;
        } /* end while */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FL_get_free_list_sizes() */
