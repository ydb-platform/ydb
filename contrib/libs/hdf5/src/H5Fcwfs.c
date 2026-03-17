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
 * Purpose:	Each file has a small cache of global heap collections called
 *		the CWFS list and recently accessed collections with free
 *		space appear on this list.  As collections are accessed the
 *		collection is moved toward the front of the list.  New
 *		collections are added to the front of the list while old
 *		collections are added to the end of the list.
 *
 *		The collection model reduces the overhead which would be
 *		incurred if the global heap were a single object, and the
 *		CWFS list allows the library to cheaply choose a collection
 *		for a new object based on object size, amount of free space
 *		in the collection, and temporal locality.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Fmodule.h" /* This source code file is part of the H5F module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* File access				*/
#include "H5HGprivate.h" /* Global heaps				*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/

/****************/
/* Local Macros */
/****************/

/*
 * Maximum length of the CWFS list, the list of remembered collections that
 * have free space.
 */
#define H5F_NCWFS 16

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5F_cwfs_add
 *
 * Purpose:	Add a global heap collection to the CWFS for a file.
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_cwfs_add(H5F_t *f, H5HG_heap_t *heap)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(f);
    assert(f->shared);
    assert(heap);

    /*
     * Add the new heap to the CWFS list, removing some other entry if
     * necessary to make room. We remove the right-most entry that has less
     * free space than this heap.
     */
    if (NULL == f->shared->cwfs) {
        if (NULL == (f->shared->cwfs = (H5HG_heap_t **)H5MM_malloc(H5F_NCWFS * sizeof(H5HG_heap_t *))))
            HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "can't allocate CWFS for file");
        f->shared->cwfs[0] = heap;
        f->shared->ncwfs   = 1;
    }
    else if (H5F_NCWFS == f->shared->ncwfs) {
        int i; /* Local index variable */

        for (i = H5F_NCWFS - 1; i >= 0; --i)
            if (H5HG_FREE_SIZE(f->shared->cwfs[i]) < H5HG_FREE_SIZE(heap)) {
                memmove(f->shared->cwfs + 1, f->shared->cwfs, (size_t)i * sizeof(H5HG_heap_t *));
                f->shared->cwfs[0] = heap;
                break;
            } /* end if */
    }
    else {
        memmove(f->shared->cwfs + 1, f->shared->cwfs, f->shared->ncwfs * sizeof(H5HG_heap_t *));
        f->shared->cwfs[0] = heap;
        f->shared->ncwfs += 1;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F_cwfs_add() */

/*-------------------------------------------------------------------------
 * Function:	H5F_cwfs_find_free_heap
 *
 * Purpose:	Find a global heap collection with free space for storing
 *		a new object.
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_cwfs_find_free_heap(H5F_t *f, size_t need, haddr_t *addr)
{
    unsigned cwfsno;              /* Local index for iterating over collections */
    bool     found     = false;   /* Flag to indicate a heap with enough space was found */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(f);
    assert(f->shared);
    assert(addr);

    /* Note that we don't have metadata cache locks on the entries in
     * f->shared->cwfs.
     *
     * In the current situation, this doesn't matter, as we are single
     * threaded, and as best I can tell, entries are added to and deleted
     * from f->shared->cwfs as they are added to and deleted from the
     * metadata cache.
     *
     * To be proper, we should either lock each entry in f->shared->cwfs
     * as we examine it, or lock the whole array.  However, at present
     * I don't see the point as there will be significant overhead,
     * and protecting and unprotecting all the collections in the global
     * heap on a regular basis will skew the replacement policy.
     *
     *                                        JRM - 5/24/04
     */
    for (cwfsno = 0; cwfsno < f->shared->ncwfs; cwfsno++)
        if (H5HG_FREE_SIZE(f->shared->cwfs[cwfsno]) >= need) {
            *addr = H5HG_ADDR(f->shared->cwfs[cwfsno]);
            found = true;
            break;
        } /* end if */

    /*
     * If we didn't find any collection with enough free space the check if
     * we can extend any of the collections to make enough room.
     */
    if (!found) {
        size_t new_need;

        for (cwfsno = 0; cwfsno < f->shared->ncwfs; cwfsno++) {
            new_need = need;
            new_need -= H5HG_FREE_SIZE(f->shared->cwfs[cwfsno]);
            new_need = MAX(H5HG_SIZE(f->shared->cwfs[cwfsno]), new_need);

            if ((H5HG_SIZE(f->shared->cwfs[cwfsno]) + new_need) <= H5HG_MAXSIZE) {
                htri_t was_extended; /* Whether the heap was extended */

                was_extended =
                    H5MF_try_extend(f, H5FD_MEM_GHEAP, H5HG_ADDR(f->shared->cwfs[cwfsno]),
                                    (hsize_t)H5HG_SIZE(f->shared->cwfs[cwfsno]), (hsize_t)new_need);
                if (was_extended < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "error trying to extend heap");
                else if (was_extended == true) {
                    if (H5HG_extend(f, H5HG_ADDR(f->shared->cwfs[cwfsno]), new_need) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL,
                                    "unable to extend global heap collection");
                    *addr = H5HG_ADDR(f->shared->cwfs[cwfsno]);
                    found = true;
                    break;
                } /* end if */
            }     /* end if */
        }         /* end for */
    }             /* end if */

    if (found) {
        /* Move the collection forward in the CWFS list, if it's not
         * already at the front
         */
        if (cwfsno > 0) {
            H5HG_heap_t *tmp = f->shared->cwfs[cwfsno];

            f->shared->cwfs[cwfsno]     = f->shared->cwfs[cwfsno - 1];
            f->shared->cwfs[cwfsno - 1] = tmp;
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F_cwfs_find_free_heap() */

/*-------------------------------------------------------------------------
 * Function:	H5F_cwfs_advance_heap
 *
 * Purpose:	Advance a heap in the CWFS
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_cwfs_advance_heap(H5F_t *f, H5HG_heap_t *heap, bool add_heap)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Check args */
    assert(f);
    assert(f->shared);
    assert(heap);

    for (u = 0; u < f->shared->ncwfs; u++)
        if (f->shared->cwfs[u] == heap) {
            if (u) {
                f->shared->cwfs[u]     = f->shared->cwfs[u - 1];
                f->shared->cwfs[u - 1] = heap;
            } /* end if */
            break;
        } /* end if */
    if (add_heap && u >= f->shared->ncwfs) {
        f->shared->ncwfs                      = MIN(f->shared->ncwfs + 1, H5F_NCWFS);
        f->shared->cwfs[f->shared->ncwfs - 1] = heap;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F_cwfs_advance_heap() */

/*-------------------------------------------------------------------------
 * Function:	H5F_cwfs_remove_heap
 *
 * Purpose:	Remove a heap from the CWFS
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_cwfs_remove_heap(H5F_shared_t *shared, H5HG_heap_t *heap)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Check args */
    assert(shared);
    assert(heap);

    /* Remove the heap from the CWFS list */
    for (u = 0; u < shared->ncwfs; u++) {
        if (shared->cwfs[u] == heap) {
            shared->ncwfs -= 1;
            memmove(shared->cwfs + u, shared->cwfs + u + 1, (shared->ncwfs - u) * sizeof(H5HG_heap_t *));
            break;
        } /* end if */
    }     /* end for */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F_cwfs_remove_heap() */
