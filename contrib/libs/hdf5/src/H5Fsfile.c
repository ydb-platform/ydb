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

#include "H5Fmodule.h" /* This source code file is part of the H5F module */

/* Packages needed by this file... */
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* File access				*/
#include "H5FDprivate.h" /* File drivers				*/
#include "H5FLprivate.h" /* Free lists                           */

/* PRIVATE TYPEDEFS */

/* Struct for tracking "shared" file structs */
typedef struct H5F_sfile_node_t {
    H5F_shared_t            *shared; /* Pointer to "shared" file struct */
    struct H5F_sfile_node_t *next;   /* Pointer to next node */
} H5F_sfile_node_t;

/* PRIVATE PROTOTYPES */

/* PRIVATE VARIABLES */

/* Declare a free list to manage the H5F_sfile_node_t struct */
H5FL_DEFINE_STATIC(H5F_sfile_node_t);

/* Declare a local variable to track the shared file information */
static H5F_sfile_node_t *H5F_sfile_head_s = NULL;

/*-------------------------------------------------------------------------
 * Function:    H5F_sfile_assert_num
 *
 * Purpose:     Sanity checking that shared file list is empty
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5F_sfile_assert_num(unsigned H5_ATTR_NDEBUG_UNUSED n)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* The only useful work this function does is asserting so when NDEBUG
     * is defined it's a no-op.
     */
#ifndef NDEBUG
    if (n == 0) {
        assert(H5F_sfile_head_s == NULL);
    }
    else {
        unsigned          count; /* Number of open shared files */
        H5F_sfile_node_t *curr;  /* Current shared file node */

        /* Iterate through low-level files for matching low-level file info */
        curr  = H5F_sfile_head_s;
        count = 0;
        while (curr) {
            /* Increment # of open shared file structs */
            count++;

            /* Advance to next shared file node */
            curr = curr->next;
        }

        assert(count == n);
    }
#endif

    FUNC_LEAVE_NOAPI_VOID
} /* H5F_sfile_assert_num() */

/*-------------------------------------------------------------------------
 * Function:    H5F__sfile_add
 *
 * Purpose:     Add a "shared" file struct to the list of open files
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__sfile_add(H5F_shared_t *shared)
{
    H5F_sfile_node_t *new_shared;          /* New shared file node */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(shared);

    /* Allocate new shared file node */
    if (NULL == (new_shared = H5FL_CALLOC(H5F_sfile_node_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Set shared file value */
    new_shared->shared = shared;

    /* Prepend to list of shared files open */
    new_shared->next = H5F_sfile_head_s;
    H5F_sfile_head_s = new_shared;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__sfile_add() */

/*-------------------------------------------------------------------------
 * Function:    H5F__sfile_search
 *
 * Purpose:     Search for a "shared" file with low-level file info that
 *              matches
 *
 * Return:      Non-NULL on success / NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5F_shared_t *
H5F__sfile_search(H5FD_t *lf)
{
    H5F_sfile_node_t *curr;             /* Current shared file node */
    H5F_shared_t     *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(lf);

    /* Iterate through low-level files for matching low-level file info */
    curr = H5F_sfile_head_s;
    while (curr) {
        /* Check for match */
        if (0 == H5FD_cmp(curr->shared->lf, lf))
            HGOTO_DONE(curr->shared);

        /* Advance to next shared file node */
        curr = curr->next;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__sfile_search() */

/*-------------------------------------------------------------------------
 * Function:    H5F__sfile_remove
 *
 * Purpose:     Remove a "shared" file struct from the list of open files
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F__sfile_remove(H5F_shared_t *shared)
{
    H5F_sfile_node_t *curr;                /* Current shared file node */
    H5F_sfile_node_t *last;                /* Last shared file node */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(shared);

    /* Locate shared file node with correct shared file */
    last = NULL;
    curr = H5F_sfile_head_s;
    while (curr && curr->shared != shared) {
        /* Advance to next node */
        last = curr;
        curr = curr->next;
    } /* end while */

    /* Indicate error if the node wasn't found */
    if (curr == NULL)
        HGOTO_ERROR(H5E_FILE, H5E_NOTFOUND, FAIL, "can't find shared file info");

    /* Remove node found from list */
    if (last != NULL)
        /* Removing middle or tail node in list */
        last->next = curr->next;
    else
        /* Removing head node in list */
        H5F_sfile_head_s = curr->next;

    /* Release the shared file node struct */
    /* (the shared file info itself is freed elsewhere) */
    curr = H5FL_FREE(H5F_sfile_node_t, curr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__sfile_remove() */
