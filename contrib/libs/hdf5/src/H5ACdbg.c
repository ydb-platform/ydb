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
 * Created:             H5ACdbg.c
 *
 * Purpose:             Functions for debugging the metadata cache
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/
#include "H5ACmodule.h" /* This source code file is part of the H5AC module */
#define H5F_FRIEND      /* Suppress error about including H5Fpkg            */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions                    */
#include "H5ACpkg.h"    /* Metadata cache                       */
#include "H5Eprivate.h" /* Error handling                       */
#include "H5Fpkg.h"     /* Files				*/

/****************/
/* Local Macros */
/****************/

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
 * Function:    H5AC_stats
 *
 * Purpose:     Prints statistics about the cache.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_stats(const H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);

    /* at present, this can't fail */
    (void)H5C_stats(f->shared->cache, H5F_OPEN_NAME(f), false);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC_stats() */

#ifndef NDEBUG

/*-------------------------------------------------------------------------
 * Function:    H5AC_dump_cache
 *
 * Purpose:     Dumps a summary of the contents of the metadata cache
 *              to stdout.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_dump_cache(const H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);

    if (H5C_dump_cache(f->shared->cache, H5F_OPEN_NAME(f)) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_dump_cache() failed.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC_dump_cache() */
#endif /* NDEBUG */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC_get_entry_ptr_from_addr()
 *
 * Purpose:     Debugging function that attempts to look up an entry in the
 *              cache by its file address, and if found, returns a pointer
 *              to the entry in *entry_ptr_ptr.  If the entry is not in the
 *              cache, *entry_ptr_ptr is set to NULL.
 *
 *              WARNING: This call should be used only in debugging
 *                       routines, and it should be avoided when
 *                       possible.
 *
 *                       Further, if we ever multi-thread the cache,
 *                       this routine will have to be either discarded
 *                       or heavily re-worked.
 *
 *                       Finally, keep in mind that the entry whose
 *                       pointer is obtained in this fashion may not
 *                       be in a stable state.
 *
 *              Note that this function is only defined if NDEBUG
 *              is not defined.
 *
 *              As heavy use of this function is almost certainly a
 *              bad idea, the metadata cache tracks the number of
 *              successful calls to this function, and (if
 *              H5C_DO_SANITY_CHECKS is defined) displays any
 *              non-zero count on cache shutdown.
 *
 *              This function is just a wrapper that calls the H5C
 *              version of the function.
 *
 * Return:      FAIL if error is detected, SUCCEED otherwise.
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
herr_t
H5AC_get_entry_ptr_from_addr(const H5F_t *f, haddr_t addr, void **entry_ptr_ptr)
{
    H5C_t *cache_ptr;           /* Ptr to cache */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;

    if (H5C_get_entry_ptr_from_addr(cache_ptr, addr, entry_ptr_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_get_entry_ptr_from_addr() failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC_get_entry_ptr_from_addr() */
#endif /* NDEBUG */

/*-------------------------------------------------------------------------
 * Function:    H5AC_flush_dependency_exists()
 *
 * Purpose:     Test to see if a flush dependency relationship exists
 *              between the supplied parent and child.  Both parties
 *              are indicated by addresses so as to avoid the necessity
 *              of protect / unprotect calls prior to this call.
 *
 *              If either the parent or the child is not in the metadata
 *              cache, the function sets *fd_exists_ptr to false.
 *
 *              If both are in the cache, the child's list of parents is
 *              searched for the proposed parent.  If the proposed parent
 *              is found in the child's parent list, the function sets
 *              *fd_exists_ptr to true.  In all other non-error cases,
 *              the function sets *fd_exists_ptr false.
 *
 * Return:      SUCCEED on success/FAIL on failure.  Note that
 *              *fd_exists_ptr is undefined on failure.
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
herr_t
H5AC_flush_dependency_exists(H5F_t *f, haddr_t parent_addr, haddr_t child_addr, bool *fd_exists_ptr)
{
    H5C_t *cache_ptr;        /* Ptr to cache */
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;

    ret_value = H5C_flush_dependency_exists(cache_ptr, parent_addr, child_addr, fd_exists_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC_flush_dependency_exists() */
#endif /* NDEBUG */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC_verify_entry_type()
 *
 * Purpose:     Debugging function that attempts to look up an entry in the
 *              cache by its file address, and if found, test to see if its
 *              type field contains the expected value.
 *
 *              If the specified entry is in cache, *in_cache_ptr is set
 *              to true, and *type_ok_ptr is set to true or false depending
 *		on whether the entries type field matches the
 *		expected_type parameter
 *
 *              If the target entry is not in cache, *in_cache_ptr is
 *              set to false, and *type_ok_ptr is undefined.
 *
 *              Note that this function is only defined if NDEBUG
 *              is not defined.
 *
 *              This function is just a wrapper that calls the H5C
 *              version of the function.
 *
 * Return:      FAIL if error is detected, SUCCEED otherwise.
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
herr_t
H5AC_verify_entry_type(const H5F_t *f, haddr_t addr, const H5AC_class_t *expected_type, bool *in_cache_ptr,
                       bool *type_ok_ptr)
{
    H5C_t *cache_ptr;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;

    if (H5C_verify_entry_type(cache_ptr, addr, expected_type, in_cache_ptr, type_ok_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_verify_entry_type() failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC_verify_entry_type() */
#endif /* NDEBUG */

/*-------------------------------------------------------------------------
 * Function:    H5AC_get_serialization_in_progress
 *
 * Purpose:     Return the current value of
 *              cache_ptr->serialization_in_progress.
 *
 * Return:      Current value of cache_ptr->serialization_in_progress.
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
bool
H5AC_get_serialization_in_progress(H5F_t *f)
{
    H5C_t *cache_ptr;
    bool   ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;

    /* Set return value */
    ret_value = H5C_get_serialization_in_progress(cache_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC_get_serialization_in_progress() */
#endif /* NDEBUG */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC_cache_is_clean()
 *
 * Purpose:     Debugging function that verifies that all rings in the
 *              metadata cache are clean from the outermost ring, inwards
 *              to the inner ring specified.
 *
 *              Returns true if all specified rings are clean, and false
 *              if not.  Throws an assertion failure on error.
 *
 * Return:      true if the indicated ring(s) are clean, and false otherwise.
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
bool
H5AC_cache_is_clean(const H5F_t *f, H5AC_ring_t inner_ring)
{
    H5C_t *cache_ptr;
    bool   ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;

    ret_value = H5C_cache_is_clean(cache_ptr, inner_ring);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC_cache_is_clean() */
#endif /* NDEBUG */
