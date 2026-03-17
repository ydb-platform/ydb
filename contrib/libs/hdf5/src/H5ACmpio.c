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
 * Created:             H5ACmpio.c
 *
 * Purpose:             Functions in this file implement support for parallel
 *                      I/O cache functionality
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5ACmodule.h" /* This source code file is part of the H5AC module */
#define H5F_FRIEND      /*suppress error about including H5Fpkg	  */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5ACpkg.h"     /* Metadata cache			*/
#include "H5Cprivate.h"  /* Cache                                */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* Files				*/
#include "H5MMprivate.h" /* Memory management                    */

#ifdef H5_HAVE_PARALLEL

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/****************************************************************************
 *
 * structure H5AC_slist_entry_t
 *
 * The dirty entry list maintained via the d_slist_ptr field of H5AC_aux_t
 * and the cleaned entry list maintained via the c_slist_ptr field of
 * H5AC_aux_t are just lists of the file offsets of the dirty/cleaned
 * entries.  Unfortunately, the slist code makes us define a dynamically
 * allocated structure to store these offsets in.  This structure serves
 * that purpose.  Its fields are as follows:
 *
 * addr:	file offset of a metadata entry.  Entries are added to this
 *		list (if they aren't there already) when they are marked
 *		dirty in an unprotect, inserted, or moved.  They are
 *		removed when they appear in a clean entries broadcast.
 *
 ****************************************************************************/
typedef struct H5AC_slist_entry_t {
    haddr_t addr;
} H5AC_slist_entry_t;

/* User data for address list building callbacks */
typedef struct H5AC_addr_list_ud_t {
    H5AC_aux_t *aux_ptr;      /* 'Auxiliary' parallel cache info */
    haddr_t    *addr_buf_ptr; /* Array to store addresses */
    unsigned    u;            /* Counter for position in array */
} H5AC_addr_list_ud_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5AC__broadcast_candidate_list(H5AC_t *cache_ptr, unsigned *num_entries_ptr,
                                             haddr_t **haddr_buf_ptr_ptr);
static herr_t H5AC__broadcast_clean_list(H5AC_t *cache_ptr);
static herr_t H5AC__construct_candidate_list(H5AC_t *cache_ptr, H5AC_aux_t *aux_ptr, int sync_point_op);
static herr_t H5AC__copy_candidate_list_to_buffer(const H5AC_t *cache_ptr, unsigned *num_entries_ptr,
                                                  haddr_t **haddr_buf_ptr_ptr);
static herr_t H5AC__propagate_and_apply_candidate_list(H5F_t *f);
static herr_t H5AC__propagate_flushed_and_still_clean_entries_list(H5F_t *f);
static herr_t H5AC__receive_haddr_list(MPI_Comm mpi_comm, unsigned *num_entries_ptr,
                                       haddr_t **haddr_buf_ptr_ptr);
static herr_t H5AC__receive_candidate_list(const H5AC_t *cache_ptr, unsigned *num_entries_ptr,
                                           haddr_t **haddr_buf_ptr_ptr);
static herr_t H5AC__receive_and_apply_clean_list(H5F_t *f);
static herr_t H5AC__tidy_cache_0_lists(H5AC_t *cache_ptr, unsigned num_candidates,
                                       haddr_t *candidates_list_ptr);
static herr_t H5AC__rsp__dist_md_write__flush(H5F_t *f);
static herr_t H5AC__rsp__dist_md_write__flush_to_min_clean(H5F_t *f);
static herr_t H5AC__rsp__p0_only__flush(H5F_t *f);
static herr_t H5AC__rsp__p0_only__flush_to_min_clean(H5F_t *f);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5AC_aux_t struct */
H5FL_DEFINE(H5AC_aux_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5AC_slist_entry_t struct */
H5FL_DEFINE_STATIC(H5AC_slist_entry_t);

/*-------------------------------------------------------------------------
 * Function:    H5AC__set_sync_point_done_callback
 *
 * Purpose:     Set the value of the sync_point_done callback.  This
 *		callback is used by the parallel test code to verify
 *		that the expected writes and only the expected writes
 *		take place during a sync point.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__set_sync_point_done_callback(H5C_t *cache_ptr, H5AC_sync_point_done_cb_t sync_point_done)
{
    H5AC_aux_t *aux_ptr;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(cache_ptr);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);

    aux_ptr->sync_point_done = sync_point_done;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC__set_sync_point_done_callback() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__set_write_done_callback
 *
 * Purpose:     Set the value of the write_done callback.  This callback
 *              is used to improve performance of the parallel test bed
 *              for the cache.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__set_write_done_callback(H5C_t *cache_ptr, H5AC_write_done_cb_t write_done)
{
    H5AC_aux_t *aux_ptr;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(cache_ptr);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);

    aux_ptr->write_done = write_done;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC__set_write_done_callback() */

/*-------------------------------------------------------------------------
 * Function:    H5AC_add_candidate()
 *
 * Purpose:     Add the supplied metadata entry address to the candidate
 *		list.  Verify that each entry added does not appear in
 *		the list prior to its insertion.
 *
 *		This function is intended for used in constructing list
 *		of entried to be flushed during sync points.  It shouldn't
 *		be called anywhere else.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_add_candidate(H5AC_t *cache_ptr, haddr_t addr)
{
    H5AC_aux_t         *aux_ptr;
    H5AC_slist_entry_t *slist_entry_ptr = NULL;
    herr_t              ret_value       = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);
    assert(aux_ptr->candidate_slist_ptr != NULL);

    /* Construct an entry for the supplied address, and insert
     * it into the candidate slist.
     */
    if (NULL == (slist_entry_ptr = H5FL_MALLOC(H5AC_slist_entry_t)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "Can't allocate candidate slist entry");
    slist_entry_ptr->addr = addr;

    if (H5SL_insert(aux_ptr->candidate_slist_ptr, slist_entry_ptr, &(slist_entry_ptr->addr)) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "can't insert entry into dirty entry slist");

done:
    /* Clean up on error */
    if (ret_value < 0)
        if (slist_entry_ptr)
            slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC_add_candidate() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__broadcast_candidate_list()
 *
 * Purpose:     Broadcast the contents of the process 0 candidate entry
 *		slist.  In passing, also remove all entries from said
 *		list.  As the application of this will be handled by
 *		the same functions on all processes, construct and
 *		return a copy of the list in the same format as that
 *		received by the other processes.  Note that if this
 *		copy is returned in *haddr_buf_ptr_ptr, the caller
 *		must free it.
 *
 *		This function must only be called by the process with
 *		MPI_rank 0.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__broadcast_candidate_list(H5AC_t *cache_ptr, unsigned *num_entries_ptr, haddr_t **haddr_buf_ptr_ptr)
{
    H5AC_aux_t *aux_ptr       = NULL;
    haddr_t    *haddr_buf_ptr = NULL;
    int         mpi_result;
    unsigned    num_entries;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->mpi_rank == 0);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);
    assert(aux_ptr->candidate_slist_ptr != NULL);
    assert(num_entries_ptr != NULL);
    assert(*num_entries_ptr == 0);
    assert(haddr_buf_ptr_ptr != NULL);
    assert(*haddr_buf_ptr_ptr == NULL);

    /* First broadcast the number of entries in the list so that the
     * receivers can set up buffers to receive them.  If there aren't
     * any, we are done.
     */
    num_entries = (unsigned)H5SL_count(aux_ptr->candidate_slist_ptr);
    if (MPI_SUCCESS != (mpi_result = MPI_Bcast(&num_entries, 1, MPI_UNSIGNED, 0, aux_ptr->mpi_comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)

    if (num_entries > 0) {
        size_t   buf_size        = 0;
        unsigned chk_num_entries = 0;

        /* convert the candidate list into the format we
         * are used to receiving from process 0, and also load it
         * into a buffer for transmission.
         */
        if (H5AC__copy_candidate_list_to_buffer(cache_ptr, &chk_num_entries, &haddr_buf_ptr) < 0) {
            /* Push an error, but still participate in following MPI_Bcast */
            HDONE_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't construct candidate buffer.");
        }
        assert(chk_num_entries == num_entries);
        assert(haddr_buf_ptr != NULL);

        /* Now broadcast the list of candidate entries */
        buf_size = sizeof(haddr_t) * num_entries;
        if (MPI_SUCCESS !=
            (mpi_result = MPI_Bcast((void *)haddr_buf_ptr, (int)buf_size, MPI_BYTE, 0, aux_ptr->mpi_comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)
    } /* end if */

    /* Pass the number of entries and the buffer pointer
     * back to the caller.  Do this so that we can use the same code
     * to apply the candidate list to all the processes.
     */
    *num_entries_ptr   = num_entries;
    *haddr_buf_ptr_ptr = haddr_buf_ptr;

done:
    if (ret_value < 0)
        if (haddr_buf_ptr)
            haddr_buf_ptr = (haddr_t *)H5MM_xfree((void *)haddr_buf_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__broadcast_candidate_list() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__broadcast_clean_list_cb()
 *
 * Purpose:     Skip list callback for building array of addresses for
 *              broadcasting the clean list.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__broadcast_clean_list_cb(void *_item, void H5_ATTR_UNUSED *_key, void *_udata)
{
    H5AC_slist_entry_t  *slist_entry_ptr = (H5AC_slist_entry_t *)_item;   /* Address of item */
    H5AC_addr_list_ud_t *udata           = (H5AC_addr_list_ud_t *)_udata; /* Context for callback */
    haddr_t              addr;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(slist_entry_ptr);
    assert(udata);

    /* Store the entry's address in the buffer */
    addr                          = slist_entry_ptr->addr;
    udata->addr_buf_ptr[udata->u] = addr;
    udata->u++;

    /* now release the entry */
    slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

    /* and also remove the matching entry from the dirtied list
     * if it exists.
     */
    if (NULL !=
        (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(udata->aux_ptr->d_slist_ptr, (void *)(&addr))))
        slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC__broadcast_clean_list_cb() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__broadcast_clean_list()
 *
 * Purpose:     Broadcast the contents of the process 0 cleaned entry
 *		slist.  In passing, also remove all entries from said
 *		list, and also remove any matching entries from the dirtied
 *		slist.
 *
 *		This function must only be called by the process with
 *		MPI_rank 0.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__broadcast_clean_list(H5AC_t *cache_ptr)
{
    haddr_t    *addr_buf_ptr = NULL;
    H5AC_aux_t *aux_ptr;
    int         mpi_result;
    unsigned    num_entries = 0;
    herr_t      ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->mpi_rank == 0);
    assert(aux_ptr->c_slist_ptr != NULL);

    /* First broadcast the number of entries in the list so that the
     * receives can set up a buffer to receive them.  If there aren't
     * any, we are done.
     */
    num_entries = (unsigned)H5SL_count(aux_ptr->c_slist_ptr);
    if (MPI_SUCCESS != (mpi_result = MPI_Bcast(&num_entries, 1, MPI_UNSIGNED, 0, aux_ptr->mpi_comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)

    if (num_entries > 0) {
        H5AC_addr_list_ud_t udata;
        size_t              buf_size;

        /* allocate a buffer to store the list of entry base addresses in */
        buf_size = sizeof(haddr_t) * num_entries;
        if (NULL == (addr_buf_ptr = (haddr_t *)H5MM_malloc(buf_size))) {
            /* Push an error, but still participate in following MPI_Bcast */
            HDONE_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for addr buffer");
        }
        else {
            /* Set up user data for callback */
            udata.aux_ptr      = aux_ptr;
            udata.addr_buf_ptr = addr_buf_ptr;
            udata.u            = 0;

            /* Free all the clean list entries, building the address list in the callback */
            /* (Callback also removes the matching entries from the dirtied list) */
            if (H5SL_free(aux_ptr->c_slist_ptr, H5AC__broadcast_clean_list_cb, &udata) < 0) {
                /* Push an error, but still participate in following MPI_Bcast */
                HDONE_ERROR(H5E_CACHE, H5E_CANTFREE, FAIL, "Can't build address list for clean entries");
            }
        }

        /* Now broadcast the list of cleaned entries */
        if (MPI_SUCCESS !=
            (mpi_result = MPI_Bcast((void *)addr_buf_ptr, (int)buf_size, MPI_BYTE, 0, aux_ptr->mpi_comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)
    } /* end if */

    /* if it is defined, call the sync point done callback.  Note
     * that this callback is defined purely for testing purposes,
     * and should be undefined under normal operating circumstances.
     */
    if (aux_ptr->sync_point_done)
        (aux_ptr->sync_point_done)(num_entries, addr_buf_ptr);

done:
    if (addr_buf_ptr)
        addr_buf_ptr = (haddr_t *)H5MM_xfree((void *)addr_buf_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__broadcast_clean_list() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__construct_candidate_list()
 *
 * Purpose:     In the parallel case when the metadata_write_strategy is
 *		H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED, process 0 uses
 *		this function to construct the list of cache entries to
 *		be flushed.  This list is then propagated to the other
 *		caches, and then flushed in a distributed fashion.
 *
 *		The sync_point_op parameter is used to determine the extent
 *		of the flush.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__construct_candidate_list(H5AC_t *cache_ptr, H5AC_aux_t H5_ATTR_NDEBUG_UNUSED *aux_ptr,
                               int sync_point_op)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr != NULL);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);
    assert((sync_point_op == H5AC_SYNC_POINT_OP__FLUSH_CACHE) || (aux_ptr->mpi_rank == 0));
    assert(aux_ptr->d_slist_ptr != NULL);
    assert(aux_ptr->c_slist_ptr != NULL);
    assert(H5SL_count(aux_ptr->c_slist_ptr) == 0);
    assert(aux_ptr->candidate_slist_ptr != NULL);
    assert(H5SL_count(aux_ptr->candidate_slist_ptr) == 0);
    assert((sync_point_op == H5AC_SYNC_POINT_OP__FLUSH_TO_MIN_CLEAN) ||
           (sync_point_op == H5AC_SYNC_POINT_OP__FLUSH_CACHE));

    switch (sync_point_op) {
        case H5AC_SYNC_POINT_OP__FLUSH_TO_MIN_CLEAN:
            if (H5C_construct_candidate_list__min_clean((H5C_t *)cache_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_construct_candidate_list__min_clean() failed.");
            break;

        case H5AC_SYNC_POINT_OP__FLUSH_CACHE:
            if (H5C_construct_candidate_list__clean_cache((H5C_t *)cache_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL,
                            "H5C_construct_candidate_list__clean_cache() failed.");
            break;

        default:
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unknown sync point operation.");
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__construct_candidate_list() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__copy_candidate_list_to_buffer_cb
 *
 * Purpose:     Skip list callback for building array of addresses for
 *              broadcasting the candidate list.
 *
 * Return:	Return SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__copy_candidate_list_to_buffer_cb(void *_item, void H5_ATTR_UNUSED *_key, void *_udata)
{
    H5AC_slist_entry_t  *slist_entry_ptr = (H5AC_slist_entry_t *)_item;   /* Address of item */
    H5AC_addr_list_ud_t *udata           = (H5AC_addr_list_ud_t *)_udata; /* Context for callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(slist_entry_ptr);
    assert(udata);

    /* Store the entry's address in the buffer */
    udata->addr_buf_ptr[udata->u] = slist_entry_ptr->addr;
    udata->u++;

    /* now release the entry */
    slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC__copy_candidate_list_to_buffer_cb() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__copy_candidate_list_to_buffer
 *
 * Purpose:     Allocate buffer(s) and copy the contents of the candidate
 *		entry slist into it (them).  In passing, remove all
 *		entries from the candidate slist.  Note that the
 *		candidate slist must not be empty.
 *
 *		If MPI_Offset_buf_ptr_ptr is not NULL, allocate a buffer
 *		of MPI_Offset, copy the contents of the candidate
 *		entry list into it with the appropriate conversions,
 *		and return the base address of the buffer in
 *		*MPI_Offset_buf_ptr.  Note that this is the buffer
 *		used by process 0 to transmit the list of entries to
 *		be flushed to all other processes (in this file group).
 *
 *		Similarly, allocate a buffer of haddr_t, load the contents
 *		of the candidate list into this buffer, and return its
 *		base address in *haddr_buf_ptr_ptr.  Note that this
 *		latter buffer is constructed unconditionally.
 *
 *		In passing, also remove all entries from the candidate
 *		entry slist.
 *
 * Return:	Return SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__copy_candidate_list_to_buffer(const H5AC_t *cache_ptr, unsigned *num_entries_ptr,
                                    haddr_t **haddr_buf_ptr_ptr)
{
    H5AC_aux_t         *aux_ptr = NULL;
    H5AC_addr_list_ud_t udata;
    haddr_t            *haddr_buf_ptr = NULL;
    size_t              buf_size;
    unsigned            num_entries = 0;
    herr_t              ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);
    assert(aux_ptr->candidate_slist_ptr != NULL);
    assert(H5SL_count(aux_ptr->candidate_slist_ptr) > 0);
    assert(num_entries_ptr != NULL);
    assert(*num_entries_ptr == 0);
    assert(haddr_buf_ptr_ptr != NULL);
    assert(*haddr_buf_ptr_ptr == NULL);

    num_entries = (unsigned)H5SL_count(aux_ptr->candidate_slist_ptr);

    /* allocate a buffer(s) to store the list of candidate entry
     * base addresses in
     */
    buf_size = sizeof(haddr_t) * num_entries;
    if (NULL == (haddr_buf_ptr = (haddr_t *)H5MM_malloc(buf_size)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for haddr buffer");

    /* Set up user data for callback */
    udata.aux_ptr      = aux_ptr;
    udata.addr_buf_ptr = haddr_buf_ptr;
    udata.u            = 0;

    /* Free all the candidate list entries, building the address list in the callback */
    if (H5SL_free(aux_ptr->candidate_slist_ptr, H5AC__copy_candidate_list_to_buffer_cb, &udata) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFREE, FAIL, "Can't build address list for candidate entries");

    /* Pass the number of entries and the buffer pointer
     * back to the caller.
     */
    *num_entries_ptr   = num_entries;
    *haddr_buf_ptr_ptr = haddr_buf_ptr;

done:
    if (ret_value < 0)
        if (haddr_buf_ptr)
            haddr_buf_ptr = (haddr_t *)H5MM_xfree((void *)haddr_buf_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__copy_candidate_list_to_buffer() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__log_deleted_entry()
 *
 * Purpose:     Log an entry which has been deleted.
 *
 *		Only called for mpi_rank 0. We must make sure that the entry
 *              doesn't appear in the cleaned or dirty entry lists.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__log_deleted_entry(const H5AC_info_t *entry_ptr)
{
    H5AC_t             *cache_ptr;
    H5AC_aux_t         *aux_ptr;
    H5AC_slist_entry_t *slist_entry_ptr = NULL;
    haddr_t             addr;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(entry_ptr);
    addr      = entry_ptr->addr;
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->mpi_rank == 0);
    assert(aux_ptr->d_slist_ptr != NULL);
    assert(aux_ptr->c_slist_ptr != NULL);

    /* if the entry appears in the dirtied entry slist, remove it. */
    if (NULL != (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->d_slist_ptr, (void *)(&addr))))
        slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

    /* if the entry appears in the cleaned entry slist, remove it. */
    if (NULL != (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->c_slist_ptr, (void *)(&addr))))
        slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC__log_deleted_entry() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__log_dirtied_entry()
 *
 * Purpose:     Update the dirty_bytes count for a newly dirtied entry.
 *
 *		If mpi_rank isn't 0, this simply means adding the size
 *		of the entries to the dirty_bytes count.
 *
 *		If mpi_rank is 0, we must first check to see if the entry
 *		appears in the dirty entries slist.  If it is, do nothing.
 *		If it isn't, add the size to the dirty_bytes count, add the
 *		entry to the dirty entries slist, and remove it from the
 *		cleaned list (if it is present there).
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__log_dirtied_entry(const H5AC_info_t *entry_ptr)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry_ptr);
    assert(entry_ptr->is_dirty == false);
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);

    if (aux_ptr->mpi_rank == 0) {
        H5AC_slist_entry_t *slist_entry_ptr;
        haddr_t             addr = entry_ptr->addr;

        /* Sanity checks */
        assert(aux_ptr->d_slist_ptr != NULL);
        assert(aux_ptr->c_slist_ptr != NULL);

        if (NULL == H5SL_search(aux_ptr->d_slist_ptr, (void *)(&addr))) {
            /* insert the address of the entry in the dirty entry list, and
             * add its size to the dirty_bytes count.
             */
            if (NULL == (slist_entry_ptr = H5FL_MALLOC(H5AC_slist_entry_t)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "Can't allocate dirty slist entry .");
            slist_entry_ptr->addr = addr;

            if (H5SL_insert(aux_ptr->d_slist_ptr, slist_entry_ptr, &(slist_entry_ptr->addr)) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "can't insert entry into dirty entry slist.");

            aux_ptr->dirty_bytes += entry_ptr->size;
#ifdef H5AC_DEBUG_DIRTY_BYTES_CREATION
            aux_ptr->unprotect_dirty_bytes += entry_ptr->size;
            aux_ptr->unprotect_dirty_bytes_updates += 1;
#endif    /* H5AC_DEBUG_DIRTY_BYTES_CREATION */
        } /* end if */

        /* the entry is dirty.  If it exists on the cleaned entries list,
         * remove it.
         */
        if (NULL !=
            (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->c_slist_ptr, (void *)(&addr))))
            slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);
    } /* end if */
    else {
        aux_ptr->dirty_bytes += entry_ptr->size;
#ifdef H5AC_DEBUG_DIRTY_BYTES_CREATION
        aux_ptr->unprotect_dirty_bytes += entry_ptr->size;
        aux_ptr->unprotect_dirty_bytes_updates += 1;
#endif /* H5AC_DEBUG_DIRTY_BYTES_CREATION */
    }  /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__log_dirtied_entry() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__log_cleaned_entry()
 *
 * Purpose:     Treat this operation as a 'clear' and remove the entry
 * 		from both the cleaned and dirtied lists if it is present.
 *		Reduces the dirty_bytes count by the size of the entry.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__log_cleaned_entry(const H5AC_info_t *entry_ptr)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(entry_ptr);
    assert(entry_ptr->is_dirty == false);
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);

    if (aux_ptr->mpi_rank == 0) {
        H5AC_slist_entry_t *slist_entry_ptr;
        haddr_t             addr = entry_ptr->addr;

        /* Sanity checks */
        assert(aux_ptr->d_slist_ptr != NULL);
        assert(aux_ptr->c_slist_ptr != NULL);

        /* Remove it from both the cleaned list and the dirtied list.  */
        if (NULL !=
            (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->c_slist_ptr, (void *)(&addr))))
            slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);
        if (NULL !=
            (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->d_slist_ptr, (void *)(&addr))))
            slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

    } /* end if */

    /* Decrement the dirty byte count */
    aux_ptr->dirty_bytes -= entry_ptr->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC__log_cleaned_entry() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__log_flushed_entry()
 *
 * Purpose:     Update the clean entry slist for the flush of an entry --
 *		specifically, if the entry has been cleared, remove it
 * 		from both the cleaned and dirtied lists if it is present.
 *		Otherwise, if the entry was dirty, insert the indicated
 *		entry address in the clean slist if it isn't there already.
 *
 *		This function is only used in PHDF5, and should only
 *		be called for the process with mpi rank 0.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__log_flushed_entry(H5C_t *cache_ptr, haddr_t addr, bool was_dirty, unsigned flags)
{
    bool                cleared;
    H5AC_aux_t         *aux_ptr;
    H5AC_slist_entry_t *slist_entry_ptr = NULL;
    herr_t              ret_value       = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->mpi_rank == 0);
    assert(aux_ptr->c_slist_ptr != NULL);

    /* Set local flags */
    cleared = ((flags & H5C__FLUSH_CLEAR_ONLY_FLAG) != 0);

    if (cleared) {
        /* If the entry has been cleared, must remove it from both the
         * cleaned list and the dirtied list.
         */
        if (NULL !=
            (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->c_slist_ptr, (void *)(&addr))))
            slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);
        if (NULL !=
            (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->d_slist_ptr, (void *)(&addr))))
            slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);
    } /* end if */
    else if (was_dirty) {
        if (NULL == H5SL_search(aux_ptr->c_slist_ptr, (void *)(&addr))) {
            /* insert the address of the entry in the clean entry list. */
            if (NULL == (slist_entry_ptr = H5FL_MALLOC(H5AC_slist_entry_t)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "Can't allocate clean slist entry .");
            slist_entry_ptr->addr = addr;

            if (H5SL_insert(aux_ptr->c_slist_ptr, slist_entry_ptr, &(slist_entry_ptr->addr)) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "can't insert entry into clean entry slist.");
        } /* end if */
    }     /* end else-if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__log_flushed_entry() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__log_inserted_entry()
 *
 * Purpose:     Update the dirty_bytes count for a newly inserted entry.
 *
 *		If mpi_rank isn't 0, this simply means adding the size
 *		of the entry to the dirty_bytes count.
 *
 *		If mpi_rank is 0, we must also add the entry to the
 *		dirty entries slist.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__log_inserted_entry(const H5AC_info_t *entry_ptr)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry_ptr);
    cache_ptr = entry_ptr->cache_ptr;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);

    if (aux_ptr->mpi_rank == 0) {
        H5AC_slist_entry_t *slist_entry_ptr;

        assert(aux_ptr->d_slist_ptr != NULL);
        assert(aux_ptr->c_slist_ptr != NULL);

        /* Entry to insert should not be in dirty list currently */
        if (NULL != H5SL_search(aux_ptr->d_slist_ptr, (const void *)(&entry_ptr->addr)))
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Inserted entry already in dirty slist.");

        /* insert the address of the entry in the dirty entry list, and
         * add its size to the dirty_bytes count.
         */
        if (NULL == (slist_entry_ptr = H5FL_MALLOC(H5AC_slist_entry_t)))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "Can't allocate dirty slist entry .");
        slist_entry_ptr->addr = entry_ptr->addr;
        if (H5SL_insert(aux_ptr->d_slist_ptr, slist_entry_ptr, &(slist_entry_ptr->addr)) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "can't insert entry into dirty entry slist.");

        /* Entry to insert should not be in clean list either */
        if (NULL != H5SL_search(aux_ptr->c_slist_ptr, (const void *)(&entry_ptr->addr)))
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Inserted entry in clean slist.");
    } /* end if */

    aux_ptr->dirty_bytes += entry_ptr->size;

#ifdef H5AC_DEBUG_DIRTY_BYTES_CREATION
    aux_ptr->insert_dirty_bytes += entry_ptr->size;
    aux_ptr->insert_dirty_bytes_updates += 1;
#endif /* H5AC_DEBUG_DIRTY_BYTES_CREATION */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__log_inserted_entry() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__log_moved_entry()
 *
 * Purpose:     Update the dirty_bytes count for a moved entry.
 *
 *		WARNING
 *
 *		At present, the way that the move call is used ensures
 *		that the moved entry is present in all caches by
 *		moving in a collective operation and immediately after
 *		unprotecting the target entry.
 *
 *		This function uses this invariant, and will cause arcane
 *		failures if it is not met.  If maintaining this invariant
 *		becomes impossible, we will have to rework this function
 *		extensively, and likely include a bit of IPC for
 *		synchronization.  A better option might be to subsume
 *		move in the unprotect operation.
 *
 *		Given that the target entry is in all caches, the function
 *		proceeds as follows:
 *
 *		For processes with mpi rank other 0, it simply checks to
 *		see if the entry was dirty prior to the move, and adds
 *		the entries size to the dirty bytes count.
 *
 *		In the process with mpi rank 0, the function first checks
 *		to see if the entry was dirty prior to the move.  If it
 *		was, and if the entry doesn't appear in the dirtied list
 *		under its old address, it adds the entry's size to the
 *		dirty bytes count.
 *
 *		The rank 0 process then removes any references to the
 *		entry under its old address from the clean and dirtied
 *		lists, and inserts an entry in the dirtied list under the
 *		new address.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__log_moved_entry(const H5F_t *f, haddr_t old_addr, haddr_t new_addr)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    bool        entry_in_cache;
    bool        entry_dirty;
    size_t      entry_size;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = (H5AC_t *)f->shared->cache;
    assert(cache_ptr);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);

    /* get entry status, size, etc here */
    if (H5C_get_entry_status(f, old_addr, &entry_size, &entry_in_cache, &entry_dirty, NULL, NULL, NULL, NULL,
                             NULL, NULL) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't get entry status.");
    if (!entry_in_cache)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "entry not in cache.");

    if (aux_ptr->mpi_rank == 0) {
        H5AC_slist_entry_t *slist_entry_ptr;

        assert(aux_ptr->d_slist_ptr != NULL);
        assert(aux_ptr->c_slist_ptr != NULL);

        /* if the entry appears in the cleaned entry slist, under its old
         * address, remove it.
         */
        if (NULL !=
            (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->c_slist_ptr, (void *)(&old_addr))))
            slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, slist_entry_ptr);

        /* if the entry appears in the dirtied entry slist under its old
         * address, remove it, but don't free it. Set addr to new_addr.
         */
        if (NULL !=
            (slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->d_slist_ptr, (void *)(&old_addr))))
            slist_entry_ptr->addr = new_addr;
        else {
            /* otherwise, allocate a new entry that is ready
             * for insertion, and increment dirty_bytes.
             *
             * Note that the fact that the entry wasn't in the dirtied
             * list under its old address implies that it must have
             * been clean to start with.
             */
            assert(!entry_dirty);
            if (NULL == (slist_entry_ptr = H5FL_MALLOC(H5AC_slist_entry_t)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "Can't allocate dirty slist entry .");
            slist_entry_ptr->addr = new_addr;

            aux_ptr->dirty_bytes += entry_size;

#ifdef H5AC_DEBUG_DIRTY_BYTES_CREATION
            aux_ptr->move_dirty_bytes += entry_size;
            aux_ptr->move_dirty_bytes_updates += 1;
#endif    /* H5AC_DEBUG_DIRTY_BYTES_CREATION */
        } /* end else */

        /* insert / reinsert the entry in the dirty slist */
        if (H5SL_insert(aux_ptr->d_slist_ptr, slist_entry_ptr, &(slist_entry_ptr->addr)) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "can't insert entry into dirty entry slist.");
    } /* end if */
    else if (!entry_dirty) {
        aux_ptr->dirty_bytes += entry_size;

#ifdef H5AC_DEBUG_DIRTY_BYTES_CREATION
        aux_ptr->move_dirty_bytes += entry_size;
        aux_ptr->move_dirty_bytes_updates += 1;
#endif /* H5AC_DEBUG_DIRTY_BYTES_CREATION */
    }  /* end else-if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__log_moved_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__propagate_and_apply_candidate_list
 *
 * Purpose:     Prior to the addition of support for multiple metadata
 *		write strategies, in PHDF5, only the metadata cache with
 *		mpi rank 0 was allowed to write to file.  All other
 *		metadata caches on processes with rank greater than 0
 *		were required to retain dirty entries until they were
 *		notified that the entry was clean.
 *
 *		This constraint is relaxed with the distributed
 *		metadata write strategy, in which a list of candidate
 *		metadata cache entries is constructed by the process 0
 *		cache and then distributed to the caches of all the other
 *		processes.  Once the listed is distributed, many (if not
 *		all) processes writing writing a unique subset of the
 *		entries, and marking the remainder clean.  The subsets
 *		are chosen so that each entry in the list of candidates
 *		is written by exactly one cache, and all entries are
 *		marked as being clean in all caches.
 *
 *		While the list of candidate cache entries is prepared
 *		elsewhere, this function is the main routine for distributing
 *		and applying the list.  It must be run simultaneously on
 *		all processes that have the relevant file open.  To ensure
 *		proper synchronization, there is a barrier at the beginning
 *		of this function.
 *
 *		At present, this function is called under one of two
 *		circumstances:
 *
 *		1) Dirty byte creation exceeds some user specified value.
 *
 *		   While metadata reads may occur independently, all
 *		   operations writing metadata must be collective.  Thus
 *		   all metadata caches see the same sequence of operations,
 *                 and therefore the same dirty data creation.
 *
 *		   This fact is used to synchronize the caches for purposes
 *                 of propagating the list of candidate entries, by simply
 *		   calling this function from all caches whenever some user
 *		   specified threshold on dirty data is exceeded.  (the
 *		   process 0 cache creates the candidate list just before
 *		   calling this function).
 *
 *		2) Under direct user control -- this operation must be
 *		   collective.
 *
 *              The operations to be managed by this function are as
 * 		follows:
 *
 *		All processes:
 *
 *		1) Participate in an opening barrier.
 *
 *		For the process with mpi rank 0:
 *
 *		1) Load the contents of the candidate list
 *		   (candidate_slist_ptr) into a buffer, and broadcast that
 *		   buffer to all the other caches.  Clear the candidate
 *		   list in passing.
 *
 *		If there is a positive number of candidates, proceed with
 *		the following:
 *
 *		2) Apply the candidate entry list.
 *
 *		3) Particpate in a closing barrier.
 *
 *		4) Remove from the dirty list (d_slist_ptr) and from the
 *		   flushed and still clean entries list (c_slist_ptr),
 *                 all addresses that appeared in the candidate list, as
 *		   these entries are now clean.
 *
 *
 *		For all processes with mpi rank greater than 0:
 *
 *		1) Receive the candidate entry list broadcast
 *
 *		If there is a positive number of candidates, proceed with
 *		the following:
 *
 *		2) Apply the candidate entry list.
 *
 *		3) Particpate in a closing barrier.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__propagate_and_apply_candidate_list(H5F_t *f)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    haddr_t    *candidates_list_ptr = NULL;
    int         mpi_result;
    unsigned    num_candidates = 0;
    herr_t      ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);

    /* to prevent "messages from the future" we must synchronize all
     * processes before we write any entries.
     */
    if (MPI_SUCCESS != (mpi_result = MPI_Barrier(aux_ptr->mpi_comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_result)

    if (aux_ptr->mpi_rank == 0) {
        if (H5AC__broadcast_candidate_list(cache_ptr, &num_candidates, &candidates_list_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't broadcast candidate slist.");

        assert(H5SL_count(aux_ptr->candidate_slist_ptr) == 0);
    } /* end if */
    else {
        if (H5AC__receive_candidate_list(cache_ptr, &num_candidates, &candidates_list_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't receive candidate broadcast.");
    } /* end else */

    if (num_candidates > 0) {
        herr_t result;

        /* all processes apply the candidate list.
         * H5C_apply_candidate_list() handles the details of
         * distributing the writes across the processes.
         */

        /* Enable writes during this operation */
        aux_ptr->write_permitted = true;

        /* Apply the candidate list */
        result = H5C_apply_candidate_list(f, cache_ptr, num_candidates, candidates_list_ptr,
                                          aux_ptr->mpi_rank, aux_ptr->mpi_size);

        /* Disable writes again */
        aux_ptr->write_permitted = false;

        /* Check for error on the write operation */
        if (result < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't apply candidate list.");

        /* this code exists primarily for the test bed -- it allows us to
         * enforce posix semantics on the server that pretends to be a
         * file system in our parallel tests.
         */
        if (aux_ptr->write_done)
            (aux_ptr->write_done)();

        /* To prevent "messages from the past" we must synchronize all
         * processes again before we go on.
         */
        if (MPI_SUCCESS != (mpi_result = MPI_Barrier(aux_ptr->mpi_comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_result)

        /* if this is process zero, tidy up the dirtied,
         * and flushed and still clean lists.
         */
        if (aux_ptr->mpi_rank == 0)
            if (H5AC__tidy_cache_0_lists(cache_ptr, num_candidates, candidates_list_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't tidy up process 0 lists.");
    } /* end if */

    /* if it is defined, call the sync point done callback.  Note
     * that this callback is defined purely for testing purposes,
     * and should be undefined under normal operating circumstances.
     */
    if (aux_ptr->sync_point_done)
        (aux_ptr->sync_point_done)(num_candidates, candidates_list_ptr);

done:
    if (candidates_list_ptr)
        candidates_list_ptr = (haddr_t *)H5MM_xfree((void *)candidates_list_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__propagate_and_apply_candidate_list() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__propagate_flushed_and_still_clean_entries_list
 *
 * Purpose:     In PHDF5, if the process 0 only metadata write strategy
 *		is selected, only the metadata cache with mpi rank 0 is
 *		allowed to write to file.  All other metadata caches on
 *		processes with rank greater than 0 must retain dirty
 *		entries until they are notified that the entry is now
 *		clean.
 *
 *		This function is the main routine for handling this
 *		notification procedure.  It must be called
 *		simultaneously on all processes that have the relevant
 *		file open.  To this end, it is called only during a
 *		sync point, with a barrier prior to the call.
 *
 *		Note that any metadata entry writes by process 0 will
 *		occur after the barrier and just before this call.
 *
 *		Typically, calls to this function will be triggered in
 *		one of two ways:
 *
 *		1) Dirty byte creation exceeds some user specified value.
 *
 *		   While metadata reads may occur independently, all
 *		   operations writing metadata must be collective.  Thus
 *		   all metadata caches see the same sequence of operations,
 *                 and therefore the same dirty data creation.
 *
 *		   This fact is used to synchronize the caches for purposes
 *                 of propagating the list of flushed and still clean
 *		   entries, by simply calling this function from all
 *		   caches whenever some user specified threshold on dirty
 *		   data is exceeded.
 *
 *		2) Under direct user control -- this operation must be
 *		   collective.
 *
 *              The operations to be managed by this function are as
 * 		follows:
 *
 *		For the process with mpi rank 0:
 *
 *		1) Load the contents of the flushed and still clean entries
 *		   list (c_slist_ptr) into a buffer, and broadcast that
 *		   buffer to all the other caches.
 *
 *		2) Clear the flushed and still clean entries list
 *                 (c_slist_ptr).
 *
 *
 *		For all processes with mpi rank greater than 0:
 *
 *		1) Receive the flushed and still clean entries list broadcast
 *
 *		2) Mark the specified entries as clean.
 *
 *
 *		For all processes:
 *
 *		1) Reset the dirtied bytes count to 0.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__propagate_flushed_and_still_clean_entries_list(H5F_t *f)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__PROCESS_0_ONLY);

    if (aux_ptr->mpi_rank == 0) {
        if (H5AC__broadcast_clean_list(cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't broadcast clean slist.");
        assert(H5SL_count(aux_ptr->c_slist_ptr) == 0);
    } /* end if */
    else if (H5AC__receive_and_apply_clean_list(f) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't receive and/or process clean slist broadcast.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__propagate_flushed_and_still_clean_entries_list() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__receive_haddr_list()
 *
 * Purpose:     Receive the list of entry addresses from process 0,
 *		and return it in a buffer pointed to by *haddr_buf_ptr_ptr.
 *		Note that the caller must free this buffer if it is
 *		returned.
 *
 *		This function must only be called by the process with
 *		MPI_rank greater than 0.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__receive_haddr_list(MPI_Comm mpi_comm, unsigned *num_entries_ptr, haddr_t **haddr_buf_ptr_ptr)
{
    haddr_t *haddr_buf_ptr = NULL;
    int      mpi_result;
    unsigned num_entries;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(num_entries_ptr != NULL);
    assert(*num_entries_ptr == 0);
    assert(haddr_buf_ptr_ptr != NULL);
    assert(*haddr_buf_ptr_ptr == NULL);

    /* First receive the number of entries in the list so that we
     * can set up a buffer to receive them.  If there aren't
     * any, we are done.
     */
    if (MPI_SUCCESS != (mpi_result = MPI_Bcast(&num_entries, 1, MPI_UNSIGNED, 0, mpi_comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)

    if (num_entries > 0) {
        size_t buf_size;

        /* allocate buffers to store the list of entry base addresses in */
        buf_size = sizeof(haddr_t) * num_entries;
        if (NULL == (haddr_buf_ptr = (haddr_t *)H5MM_malloc(buf_size))) {
            /* Push an error, but still participate in following MPI_Bcast */
            HDONE_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for haddr buffer");
        }

        /* Now receive the list of candidate entries */
        if (MPI_SUCCESS !=
            (mpi_result = MPI_Bcast((void *)haddr_buf_ptr, (int)buf_size, MPI_BYTE, 0, mpi_comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)
    } /* end if */

    /* finally, pass the number of entries and the buffer pointer
     * back to the caller.
     */
    *num_entries_ptr   = num_entries;
    *haddr_buf_ptr_ptr = haddr_buf_ptr;

done:
    if (ret_value < 0)
        if (haddr_buf_ptr)
            haddr_buf_ptr = (haddr_t *)H5MM_xfree((void *)haddr_buf_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__receive_haddr_list() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__receive_and_apply_clean_list()
 *
 * Purpose:     Receive the list of cleaned entries from process 0,
 *		and mark the specified entries as clean.
 *
 *		This function must only be called by the process with
 *		MPI_rank greater than 0.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__receive_and_apply_clean_list(H5F_t *f)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    haddr_t    *haddr_buf_ptr = NULL;
    unsigned    num_entries   = 0;
    herr_t      ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->mpi_rank != 0);

    /* Retrieve the clean list from process 0 */
    if (H5AC__receive_haddr_list(aux_ptr->mpi_comm, &num_entries, &haddr_buf_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "can't receive clean list");

    if (num_entries > 0)
        /* Mark the indicated entries as clean */
        if (H5C_mark_entries_as_clean(f, num_entries, haddr_buf_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't mark entries clean.");

    /* if it is defined, call the sync point done callback.  Note
     * that this callback is defined purely for testing purposes,
     * and should be undefined under normal operating circumstances.
     */
    if (aux_ptr->sync_point_done)
        (aux_ptr->sync_point_done)(num_entries, haddr_buf_ptr);

done:
    if (haddr_buf_ptr)
        haddr_buf_ptr = (haddr_t *)H5MM_xfree((void *)haddr_buf_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__receive_and_apply_clean_list() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5AC__receive_candidate_list()
 *
 * Purpose:     Receive the list of candidate entries from process 0,
 *		and return it in a buffer pointed to by *haddr_buf_ptr_ptr.
 *		Note that the caller must free this buffer if it is
 *		returned.
 *
 *		This function must only be called by the process with
 *		MPI_rank greater than 0.
 *
 *		Return SUCCEED on success, and FAIL on failure.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__receive_candidate_list(const H5AC_t *cache_ptr, unsigned *num_entries_ptr, haddr_t **haddr_buf_ptr_ptr)
{
    H5AC_aux_t *aux_ptr;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->mpi_rank != 0);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);
    assert(num_entries_ptr != NULL);
    assert(*num_entries_ptr == 0);
    assert(haddr_buf_ptr_ptr != NULL);
    assert(*haddr_buf_ptr_ptr == NULL);

    /* Retrieve the candidate list from process 0 */
    if (H5AC__receive_haddr_list(aux_ptr->mpi_comm, num_entries_ptr, haddr_buf_ptr_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "can't receive clean list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__receive_candidate_list() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__rsp__dist_md_write__flush
 *
 * Purpose:     Routine for handling the details of running a sync point
 *		that is triggered by a flush -- which in turn must have been
 *		triggered by either a flush API call or a file close --
 *		when the distributed metadata write strategy is selected.
 *
 *		Upon entry, each process generates it own candidate list,
 *              being a sorted list of all dirty metadata entries currently
 *		in the metadata cache.  Note that this list must be idendical
 *		across all processes, as all processes see the same stream
 *		of dirty metadata coming in, and use the same lists of
 *		candidate entries at each sync point.  (At first glance, this
 *		argument sounds circular, but think of it in the sense of
 *		a recursive proof).
 *
 *		If this this list is empty, we are done, and the function
 *		returns
 *
 *		Otherwise, after the sorted list dirty metadata entries is
 *		constructed, each process uses the same algorithm to assign
 *		each entry on the candidate list to exactly one process for
 *		flushing.
 *
 *		At this point, all processes participate in a barrier to
 *		avoid messages from the past/future bugs.
 *
 *		Each process then flushes the entries assigned to it, and
 *		marks all other entries on the candidate list as clean.
 *
 *		Finally, all processes participate in a second barrier to
 *		avoid messages from the past/future bugs.
 *
 *		At the end of this process, process 0 and only process 0
 *		must tidy up its lists of dirtied and cleaned entries.
 *		These lists are not used in the distributed metadata write
 *		strategy, but they must be maintained should we shift
 *		to a strategy that uses them.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__rsp__dist_md_write__flush(H5F_t *f)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    haddr_t    *haddr_buf_ptr = NULL;
    int         mpi_result;
    unsigned    num_entries = 0;
    herr_t      ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);

    /* first construct the candidate list -- initially, this will be in the
     * form of a skip list.  We will convert it later.
     */
    if (H5C_construct_candidate_list__clean_cache(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't construct candidate list.");

    if (H5SL_count(aux_ptr->candidate_slist_ptr) > 0) {
        herr_t result;

        /* convert the candidate list into the format we
         * are used to receiving from process 0.
         */
        if (H5AC__copy_candidate_list_to_buffer(cache_ptr, &num_entries, &haddr_buf_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't construct candidate buffer.");

        /* Initial sync point barrier
         *
         * When flushing from within the close operation from a file,
         * it's possible to skip this barrier (on the second flush of the cache).
         */
        if (!H5CX_get_mpi_file_flushing())
            if (MPI_SUCCESS != (mpi_result = MPI_Barrier(aux_ptr->mpi_comm)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_result)

        /* Enable writes during this operation */
        aux_ptr->write_permitted = true;

        /* Apply the candidate list */
        result = H5C_apply_candidate_list(f, cache_ptr, num_entries, haddr_buf_ptr, aux_ptr->mpi_rank,
                                          aux_ptr->mpi_size);

        /* Disable writes again */
        aux_ptr->write_permitted = false;

        /* Check for error on the write operation */
        if (result < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't apply candidate list.");

        /* this code exists primarily for the test bed -- it allows us to
         * enforce posix semantics on the server that pretends to be a
         * file system in our parallel tests.
         */
        if (aux_ptr->write_done)
            (aux_ptr->write_done)();

        /* final sync point barrier */
        if (MPI_SUCCESS != (mpi_result = MPI_Barrier(aux_ptr->mpi_comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_result)

        /* if this is process zero, tidy up the dirtied,
         * and flushed and still clean lists.
         */
        if (aux_ptr->mpi_rank == 0)
            if (H5AC__tidy_cache_0_lists(cache_ptr, num_entries, haddr_buf_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't tidy up process 0 lists.");
    } /* end if */

    /* if it is defined, call the sync point done callback.  Note
     * that this callback is defined purely for testing purposes,
     * and should be undefined under normal operating circumstances.
     */
    if (aux_ptr->sync_point_done)
        (aux_ptr->sync_point_done)(num_entries, haddr_buf_ptr);

done:
    if (haddr_buf_ptr)
        haddr_buf_ptr = (haddr_t *)H5MM_xfree((void *)haddr_buf_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__rsp__dist_md_write__flush() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__rsp__dist_md_write__flush_to_min_clean
 *
 * Purpose:     Routine for handling the details of running a sync point
 *		triggered by the accumulation of dirty metadata (as
 *		opposed to a flush call to the API) when the distributed
 *		metadata write strategy is selected.
 *
 *		After invocation and initial sanity checking this function
 *		first checks to see if evictions are enabled -- if they
 *		are not, the function does nothing and returns.
 *
 *		Otherwise, process zero constructs a list of entries to
 *		be flushed in order to bring the process zero cache back
 *		within its min clean requirement.  Note that this list
 *		(the candidate list) may be empty.
 *
 *              Then, all processes participate in a barrier.
 *
 *		After the barrier, process 0 broadcasts the number of
 *		entries in the candidate list prepared above, and all
 *		other processes receive this number.
 *
 *		If this number is zero, we are done, and the function
 *		returns without further action.
 *
 *		Otherwise, process 0 broadcasts the sorted list of
 *		candidate entries, and all other processes receive it.
 *
 *		Then, each process uses the same algorithm to assign
 *		each entry on the candidate list to exactly one process
 *		for flushing.
 *
 *		Each process then flushes the entries assigned to it, and
 *		marks all other entries on the candidate list as clean.
 *
 *		Finally, all processes participate in a second barrier to
 *		avoid messages from the past/future bugs.
 *
 *		At the end of this process, process 0 and only process 0
 *		must tidy up its lists of dirtied and cleaned entries.
 *		These lists are not used in the distributed metadata write
 *		strategy, but they must be maintained should we shift
 *		to a strategy that uses them.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__rsp__dist_md_write__flush_to_min_clean(H5F_t *f)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    bool        evictions_enabled;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);

    /* Query if evictions are allowed */
    if (H5C_get_evictions_enabled((const H5C_t *)cache_ptr, &evictions_enabled) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "H5C_get_evictions_enabled() failed.");

    if (evictions_enabled) {
        /* construct candidate list -- process 0 only */
        if (aux_ptr->mpi_rank == 0) {
            /* If constructing candidate list fails, push an error but still participate
             * in collective operations during following candidate list propagation
             */
            if (H5AC__construct_candidate_list(cache_ptr, aux_ptr, H5AC_SYNC_POINT_OP__FLUSH_TO_MIN_CLEAN) <
                0)
                HDONE_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't construct candidate list.");
        }

        /* propagate and apply candidate list -- all processes */
        if (H5AC__propagate_and_apply_candidate_list(f) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't propagate and apply candidate list.");
    } /* evictions enabled */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__rsp__dist_md_write__flush_to_min_clean() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__rsp__p0_only__flush
 *
 * Purpose:     Routine for handling the details of running a sync point
 *		that is triggered a flush -- which in turn must have been
 *		triggered by either a flush API call or a file close --
 *		when the process 0 only metadata write strategy is selected.
 *
 *              First, all processes participate in a barrier.
 *
 *		Then process zero flushes all dirty entries, and broadcasts
 *		they number of clean entries (if any) to all the other
 *		caches.
 *
 *		If this number is zero, we are done.
 *
 *		Otherwise, process 0 broadcasts the list of cleaned
 *		entries, and all other processes which are part of this
 *		file group receive it, and mark the listed entries as
 *		clean in their caches.
 *
 *		Since all processes have the same set of dirty
 *		entries at the beginning of the sync point, and all
 *		entries that will be written are written before
 *		process zero broadcasts the number of cleaned entries,
 *		there is no need for a closing barrier.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__rsp__p0_only__flush(H5F_t *f)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    int         mpi_result;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__PROCESS_0_ONLY);

    /* To prevent "messages from the future" we must
     * synchronize all processes before we start the flush.
     * Hence the following barrier.
     *
     * However, when flushing from within the close operation from a file,
     * it's possible to skip this barrier (on the second flush of the cache).
     */
    if (!H5CX_get_mpi_file_flushing())
        if (MPI_SUCCESS != (mpi_result = MPI_Barrier(aux_ptr->mpi_comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_result)

    /* Flush data to disk, from rank 0 process */
    if (aux_ptr->mpi_rank == 0) {
        herr_t result;

        /* Enable writes during this operation */
        aux_ptr->write_permitted = true;

        /* Flush the cache */
        result = H5C_flush_cache(f, H5AC__NO_FLAGS_SET);

        /* Disable writes again */
        aux_ptr->write_permitted = false;

        /* Check for error on the write operation */
        if (result < 0) {
            /* If write operation fails, push an error but still participate
             * in collective operations during following cache entry
             * propagation
             */
            HDONE_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't flush.");
        }
        else {
            /* this code exists primarily for the test bed -- it allows us to
             * enforce POSIX semantics on the server that pretends to be a
             * file system in our parallel tests.
             */
            if (aux_ptr->write_done)
                (aux_ptr->write_done)();
        }
    } /* end if */

    /* Propagate cleaned entries to other ranks. */
    if (H5AC__propagate_flushed_and_still_clean_entries_list(f) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't propagate clean entries list.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__rsp__p0_only__flush() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__rsp__p0_only__flush_to_min_clean
 *
 * Purpose:     Routine for handling the details of running a sync point
 *		triggered by the accumulation of dirty metadata (as
 *		opposed to a flush call to the API) when the process 0
 *		only metadata write strategy is selected.
 *
 *		After invocation and initial sanity checking this function
 *		first checks to see if evictions are enabled -- if they
 *		are not, the function does nothing and returns.
 *
 *              Otherwise, all processes participate in a barrier.
 *
 *		After the barrier, if this is process 0, the function
 *		causes the cache to flush sufficient entries to get the
 *		cache back within its minimum clean fraction, and broadcast
 *		the number of entries which have been flushed since
 *		the last sync point, and are still clean.
 *
 *		If this number is zero, we are done.
 *
 *		Otherwise, process 0 broadcasts the list of cleaned
 *		entries, and all other processes which are part of this
 *		file group receive it, and mark the listed entries as
 *		clean in their caches.
 *
 *		Since all processes have the same set of dirty
 *		entries at the beginning of the sync point, and all
 *		entries that will be written are written before
 *		process zero broadcasts the number of cleaned entries,
 *		there is no need for a closing barrier.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__rsp__p0_only__flush_to_min_clean(H5F_t *f)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    bool        evictions_enabled;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__PROCESS_0_ONLY);

    /* Query if evictions are allowed */
    if (H5C_get_evictions_enabled((const H5C_t *)cache_ptr, &evictions_enabled) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "H5C_get_evictions_enabled() failed.");

    /* Flush if evictions are allowed -- following call
     * will cause process 0 to flush to min clean size,
     * and then propagate the newly clean entries to the
     * other processes.
     *
     * Otherwise, do nothing.
     */
    if (evictions_enabled) {
        int mpi_result;

        /* to prevent "messages from the future" we must synchronize all
         * processes before we start the flush.  This synchronization may
         * already be done -- hence the do_barrier parameter.
         */
        if (MPI_SUCCESS != (mpi_result = MPI_Barrier(aux_ptr->mpi_comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_result)

        if (0 == aux_ptr->mpi_rank) {
            herr_t result;

            /* here, process 0 flushes as many entries as necessary to
             * comply with the currently specified min clean size.
             * Note that it is quite possible that no entries will be
             * flushed.
             */

            /* Enable writes during this operation */
            aux_ptr->write_permitted = true;

            /* Flush the cache */
            result = H5C_flush_to_min_clean(f);

            /* Disable writes again */
            aux_ptr->write_permitted = false;

            /* Check for error on the write operation */
            if (result < 0) {
                /* If write operation fails, push an error but still participate
                 * in collective operations during following cache entry
                 * propagation
                 */
                HDONE_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C_flush_to_min_clean() failed.");
            }
            else {
                /* this call exists primarily for the test code -- it is used
                 * to enforce POSIX semantics on the process used to simulate
                 * reads and writes in t_cache.c.
                 */
                if (aux_ptr->write_done)
                    (aux_ptr->write_done)();
            }
        } /* end if */

        if (H5AC__propagate_flushed_and_still_clean_entries_list(f) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't propagate clean entries list.");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__rsp__p0_only__flush_to_min_clean() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__run_sync_point
 *
 * Purpose:     Top level routine for managing a sync point between all
 *		meta data caches in the parallel case.  Since all caches
 *		see the same sequence of dirty metadata, we simply count
 *		bytes of dirty metadata, and run a sync point whenever the
 *		number of dirty bytes of metadata seen since the last
 *		sync point exceeds a threshold that is common across all
 *		processes.  We also run sync points in response to
 *		HDF5 API calls triggering either a flush or a file close.
 *
 *		In earlier versions of PHDF5, only the metadata cache with
 *		mpi rank 0 was allowed to write to file.  All other
 *		metadata caches on processes with rank greater than 0 were
 *		required to retain dirty entries until they were notified
 *		that the entry is was clean.
 *
 *		This function was created to make it easier for us to
 *		experiment with other options, as it is a single point
 *		for the execution of sync points.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__run_sync_point(H5F_t *f, int sync_point_op)
{
    H5AC_t     *cache_ptr;
    H5AC_aux_t *aux_ptr;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f != NULL);
    cache_ptr = f->shared->cache;
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert((sync_point_op == H5AC_SYNC_POINT_OP__FLUSH_TO_MIN_CLEAN) ||
           (sync_point_op == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED));

#ifdef H5AC_DEBUG_DIRTY_BYTES_CREATION
    fprintf(stdout, "%d:%s...:%u: (u/uu/i/iu/m/mu) = %zu/%u/%zu/%u/%zu/%u\n", aux_ptr->mpi_rank,
            __func__ aux_ptr->dirty_bytes_propagations, aux_ptr->unprotect_dirty_bytes,
            aux_ptr->unprotect_dirty_bytes_updates, aux_ptr->insert_dirty_bytes,
            aux_ptr->insert_dirty_bytes_updates, aux_ptr->move_dirty_bytes,
            aux_ptr->move_dirty_bytes_updates);
#endif /* H5AC_DEBUG_DIRTY_BYTES_CREATION */

    /* Clear collective access flag on half of the entries in the cache and
     * mark them as independent in case they need to be evicted later. All
     * ranks are guaranteed to mark the same entries since we don't modify the
     * order of the collectively accessed entries except through collective
     * access.
     */
    if (H5C_clear_coll_entries(cache_ptr, true) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "H5C_clear_coll_entries() failed.");

    switch (aux_ptr->metadata_write_strategy) {
        case H5AC_METADATA_WRITE_STRATEGY__PROCESS_0_ONLY:
            switch (sync_point_op) {
                case H5AC_SYNC_POINT_OP__FLUSH_TO_MIN_CLEAN:
                    if (H5AC__rsp__p0_only__flush_to_min_clean(f) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL,
                                    "H5AC__rsp__p0_only__flush_to_min_clean() failed.");
                    break;

                case H5AC_SYNC_POINT_OP__FLUSH_CACHE:
                    if (H5AC__rsp__p0_only__flush(f) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL, "H5AC__rsp__p0_only__flush() failed.");
                    break;

                default:
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unknown flush op");
                    break;
            } /* end switch */
            break;

        case H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED:
            switch (sync_point_op) {
                case H5AC_SYNC_POINT_OP__FLUSH_TO_MIN_CLEAN:
                    if (H5AC__rsp__dist_md_write__flush_to_min_clean(f) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL,
                                    "H5AC__rsp__dist_md_write__flush_to_min_clean() failed.");
                    break;

                case H5AC_SYNC_POINT_OP__FLUSH_CACHE:
                    if (H5AC__rsp__dist_md_write__flush(f) < 0)
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTGET, FAIL,
                                    "H5AC__rsp__dist_md_write__flush() failed.");
                    break;

                default:
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "unknown flush op");
                    break;
            } /* end switch */
            break;

        default:
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown metadata write strategy.");
            break;
    } /* end switch */

    /* reset the dirty bytes count */
    aux_ptr->dirty_bytes = 0;

#ifdef H5AC_DEBUG_DIRTY_BYTES_CREATION
    aux_ptr->dirty_bytes_propagations += 1;
    aux_ptr->unprotect_dirty_bytes         = 0;
    aux_ptr->unprotect_dirty_bytes_updates = 0;
    aux_ptr->insert_dirty_bytes            = 0;
    aux_ptr->insert_dirty_bytes_updates    = 0;
    aux_ptr->move_dirty_bytes              = 0;
    aux_ptr->move_dirty_bytes_updates      = 0;
#endif /* H5AC_DEBUG_DIRTY_BYTES_CREATION */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__run_sync_point() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__tidy_cache_0_lists()
 *
 * Purpose:     In the distributed metadata write strategy, not all dirty
 *		entries are written by process 0 -- thus we must tidy
 *		up the dirtied, and flushed and still clean lists
 *		maintained by process zero after each sync point.
 *
 *		This procedure exists to tend to this issue.
 *
 *		At this point, all entries that process 0 cleared should
 *		have been removed from both the dirty and flushed and
 *		still clean lists, and entries that process 0 has flushed
 *		should have been removed from the dirtied list and added
 *		to the flushed and still clean list.
 *
 *		However, since the distributed metadata write strategy
 *		doesn't make use of these lists, the objective is simply
 *		to maintain these lists in consistent state that allows
 *		them to be used should the metadata write strategy change
 *		to one that uses these lists.
 *
 *		Thus for our purposes, all we need to do is remove from
 *		the dirtied and flushed and still clean lists all
 *		references to entries that appear in the candidate list.
 *
 * Return:      Success:        non-negative
 *
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__tidy_cache_0_lists(H5AC_t *cache_ptr, unsigned num_candidates, haddr_t *candidates_list_ptr)
{
    H5AC_aux_t *aux_ptr;
    unsigned    u;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(cache_ptr != NULL);
    aux_ptr = (H5AC_aux_t *)H5C_get_aux_ptr(cache_ptr);
    assert(aux_ptr != NULL);
    assert(aux_ptr->metadata_write_strategy == H5AC_METADATA_WRITE_STRATEGY__DISTRIBUTED);
    assert(aux_ptr->mpi_rank == 0);
    assert(num_candidates > 0);
    assert(candidates_list_ptr != NULL);

    /* clean up dirtied and flushed and still clean lists by removing
     * all entries on the candidate list.  Cleared entries should
     * have been removed from both the dirty and cleaned lists at
     * this point, flushed entries should have been added to the
     * cleaned list.  However, for this metadata write strategy,
     * we just want to remove all references to the candidate entries.
     */
    for (u = 0; u < num_candidates; u++) {
        H5AC_slist_entry_t *d_slist_entry_ptr;
        H5AC_slist_entry_t *c_slist_entry_ptr;
        haddr_t             addr;

        addr = candidates_list_ptr[u];

        /* addr may be either on the dirtied list, or on the flushed
         * and still clean list.  Remove it.
         */
        if (NULL !=
            (d_slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->d_slist_ptr, (void *)&addr)))
            d_slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, d_slist_entry_ptr);
        if (NULL !=
            (c_slist_entry_ptr = (H5AC_slist_entry_t *)H5SL_remove(aux_ptr->c_slist_ptr, (void *)&addr)))
            c_slist_entry_ptr = H5FL_FREE(H5AC_slist_entry_t, c_slist_entry_ptr);
    } /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5AC__tidy_cache_0_lists() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__flush_entries
 *
 * Purpose:     Flush the metadata cache associated with the specified file,
 *              only writing from rank 0, but propagating the cleaned entries
 *              to all ranks.
 *
 * Return:      Non-negative on success/Negative on failure if there was a
 *              request to flush all items and something was protected.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC__flush_entries(H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared->cache);

    /* Check if we have >1 ranks */
    if (H5C_get_aux_ptr(f->shared->cache))
        if (H5AC__run_sync_point(f, H5AC_SYNC_POINT_OP__FLUSH_CACHE) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't run sync point.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__flush_entries() */
#endif /* H5_HAVE_PARALLEL */
