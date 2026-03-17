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
 * Created:     H5Cimage.c
 *
 * Purpose:     Functions in this file are specific to the implementation
 *		of the metadata cache image feature.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Cmodule.h" /* This source code file is part of the H5C module */
#define H5F_FRIEND     /*suppress error about including H5Fpkg	  */

/***********/
/* Headers */
/***********/
#include "H5private.h" /* Generic Functions			*/
#ifdef H5_HAVE_PARALLEL
#define H5AC_FRIEND      /*suppress error about including H5ACpkg  */
#include "H5ACpkg.h"     /* Metadata cache                       */
#endif                   /* H5_HAVE_PARALLEL */
#include "H5Cpkg.h"      /* Cache				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* Files				*/
#include "H5FDprivate.h" /* File drivers				*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/

/****************/
/* Local Macros */
/****************/
#if H5C_DO_MEMORY_SANITY_CHECKS
#define H5C_IMAGE_EXTRA_SPACE  8
#define H5C_IMAGE_SANITY_VALUE "DeadBeef"
#else /* H5C_DO_MEMORY_SANITY_CHECKS */
#define H5C_IMAGE_EXTRA_SPACE 0
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

/* Cache image buffer components, on disk */
#define H5C__MDCI_BLOCK_SIGNATURE     "MDCI"
#define H5C__MDCI_BLOCK_SIGNATURE_LEN 4
#define H5C__MDCI_BLOCK_VERSION_0     0

/* Metadata cache image header flags -- max 8 bits */
#define H5C__MDCI_HEADER_HAVE_RESIZE_STATUS 0x01

/* Metadata cache image entry flags -- max 8 bits */
#define H5C__MDCI_ENTRY_DIRTY_FLAG        0x01
#define H5C__MDCI_ENTRY_IN_LRU_FLAG       0x02
#define H5C__MDCI_ENTRY_IS_FD_PARENT_FLAG 0x04
#define H5C__MDCI_ENTRY_IS_FD_CHILD_FLAG  0x08

/* Limits on flush dependency values, stored in 16-bit values on disk */
#define H5C__MDCI_MAX_FD_CHILDREN USHRT_MAX
#define H5C__MDCI_MAX_FD_PARENTS  USHRT_MAX

/* Maximum ring allowed in image */
#define H5C_MAX_RING_IN_IMAGE H5C_RING_MDFSM

/***********************************************************************
 *
 * Stats collection macros
 *
 * The following macros must handle stats collection when collection
 * is enabled, and evaluate to the empty string when it is not.
 *
 ***********************************************************************/
#if H5C_COLLECT_CACHE_STATS
/* clang-format off */
#define H5C__UPDATE_STATS_FOR_CACHE_IMAGE_CREATE(cache_ptr) \
do {                                                        \
    (cache_ptr)->images_created++;                          \
} while (0)
#define H5C__UPDATE_STATS_FOR_CACHE_IMAGE_READ(cache_ptr)  \
do {                                                       \
    /* make sure image len is still good */                \
    assert((cache_ptr)->image_len > 0);                  \
    (cache_ptr)->images_read++;                            \
} while (0)
#define H5C__UPDATE_STATS_FOR_CACHE_IMAGE_LOAD(cache_ptr)  \
do {                                                       \
    /* make sure image len is still good */                \
    assert((cache_ptr)->image_len > 0);                  \
    (cache_ptr)->images_loaded++;                          \
    (cache_ptr)->last_image_size = (cache_ptr)->image_len; \
} while (0)
/* clang-format on */
#else /* H5C_COLLECT_CACHE_STATS */
#define H5C__UPDATE_STATS_FOR_CACHE_IMAGE_CREATE(cache_ptr)
#define H5C__UPDATE_STATS_FOR_CACHE_IMAGE_READ(cache_ptr)
#define H5C__UPDATE_STATS_FOR_CACHE_IMAGE_LOAD(cache_ptr)
#endif /* H5C_COLLECT_CACHE_STATS */

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* Helper routines */
static size_t H5C__cache_image_block_entry_header_size(const H5F_t *f);
static size_t H5C__cache_image_block_header_size(const H5F_t *f);
static herr_t H5C__decode_cache_image_header(const H5F_t *f, H5C_t *cache_ptr, const uint8_t **buf);
#ifndef NDEBUG /* only used in assertions */
static herr_t H5C__decode_cache_image_entry(const H5F_t *f, const H5C_t *cache_ptr, const uint8_t **buf,
                                            unsigned entry_num);
#endif
static herr_t H5C__encode_cache_image_header(const H5F_t *f, const H5C_t *cache_ptr, uint8_t **buf);
static herr_t H5C__encode_cache_image_entry(H5F_t *f, H5C_t *cache_ptr, uint8_t **buf, unsigned entry_num);
static herr_t H5C__prep_for_file_close__compute_fd_heights(const H5C_t *cache_ptr);
static void   H5C__prep_for_file_close__compute_fd_heights_real(H5C_cache_entry_t *entry_ptr,
                                                                uint32_t           fd_height);
static herr_t H5C__prep_for_file_close__setup_image_entries_array(H5C_t *cache_ptr);
static herr_t H5C__prep_for_file_close__scan_entries(const H5F_t *f, H5C_t *cache_ptr);
static herr_t H5C__reconstruct_cache_contents(H5F_t *f, H5C_t *cache_ptr);
static H5C_cache_entry_t *H5C__reconstruct_cache_entry(const H5F_t *f, H5C_t *cache_ptr, const uint8_t **buf);
static herr_t             H5C__write_cache_image_superblock_msg(H5F_t *f, bool create);
static herr_t             H5C__read_cache_image(H5F_t *f, H5C_t *cache_ptr);
static herr_t             H5C__write_cache_image(H5F_t *f, const H5C_t *cache_ptr);
static herr_t             H5C__construct_cache_image_buffer(H5F_t *f, H5C_t *cache_ptr);
static herr_t             H5C__free_image_entries_array(H5C_t *cache_ptr);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage H5C_cache_entry_t objects */
H5FL_DEFINE(H5C_cache_entry_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5C_cache_image_pending()
 *
 * Purpose:     Tests to see if the load of a metadata cache image
 *              load is pending (i.e. will be executed on the next
 *              protect or insert)
 *
 *              Returns true if a cache image load is pending, and false
 *              if not.  Throws an assertion failure on error.
 *
 * Return:      true if a cache image load is pending, and false otherwise.
 *
 *-------------------------------------------------------------------------
 */
bool
H5C_cache_image_pending(const H5C_t *cache_ptr)
{
    bool ret_value = true; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(cache_ptr);

    ret_value = (cache_ptr->load_image && !cache_ptr->image_loaded);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_cache_image_pending() */

/*-------------------------------------------------------------------------
 * Function:    H5C_cache_image_status()
 *
 * Purpose:     Examine the metadata cache associated with the supplied
 *              instance of H5F_t to determine whether the load of a
 *              cache image has either been queued or executed, and if
 *              construction of a cache image has been requested.
 *
 *              This done, it set *load_ci_ptr to true if a cache image
 *              has either been loaded or a load has been requested, and
 *              to false otherwise.
 *
 *              Similarly, set *write_ci_ptr to true if construction of
 *              a cache image has been requested, and to false otherwise.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_cache_image_status(H5F_t *f, bool *load_ci_ptr, bool *write_ci_ptr)
{
    H5C_t *cache_ptr;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(load_ci_ptr);
    assert(write_ci_ptr);

    *load_ci_ptr  = cache_ptr->load_image || cache_ptr->image_loaded;
    *write_ci_ptr = cache_ptr->image_ctl.generate_image;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5C_cache_image_status() */

/*-------------------------------------------------------------------------
 * Function:    H5C__construct_cache_image_buffer()
 *
 * Purpose:     Allocate a buffer of size cache_ptr->image_len, and
 *		load it with an image of the metadata cache image block.
 *
 *		Note that by the time this function is called, the cache
 *		should have removed all entries from its data structures.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__construct_cache_image_buffer(H5F_t *f, H5C_t *cache_ptr)
{
    uint8_t *p; /* Pointer into image buffer */
    uint32_t chksum;
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(cache_ptr == f->shared->cache);
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);
    assert(cache_ptr->image_ctl.generate_image);
    assert(cache_ptr->num_entries_in_image > 0);
    assert(cache_ptr->index_len == 0);
    assert(cache_ptr->image_data_len > 0);
    assert(cache_ptr->image_data_len <= cache_ptr->image_len);

    /* Allocate the buffer in which to construct the cache image block */
    if (NULL == (cache_ptr->image_buffer = H5MM_malloc(cache_ptr->image_len + 1)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for cache image buffer");

    /* Construct the cache image block header image */
    p = (uint8_t *)cache_ptr->image_buffer;
    if (H5C__encode_cache_image_header(f, cache_ptr, &p) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTENCODE, FAIL, "header image construction failed");
    assert((size_t)(p - (uint8_t *)cache_ptr->image_buffer) < cache_ptr->image_data_len);

    /* Construct the cache entry images */
    for (u = 0; u < cache_ptr->num_entries_in_image; u++)
        if (H5C__encode_cache_image_entry(f, cache_ptr, &p, u) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTENCODE, FAIL, "entry image construction failed");
    assert((size_t)(p - (uint8_t *)cache_ptr->image_buffer) < cache_ptr->image_data_len);

    /* Construct the adaptive resize status image -- not yet */

    /* Compute the checksum and encode */
    chksum = H5_checksum_metadata(cache_ptr->image_buffer,
                                  (size_t)(cache_ptr->image_data_len - H5F_SIZEOF_CHKSUM), 0);
    UINT32ENCODE(p, chksum);
    assert((size_t)(p - (uint8_t *)cache_ptr->image_buffer) == cache_ptr->image_data_len);
    assert((size_t)(p - (uint8_t *)cache_ptr->image_buffer) <= cache_ptr->image_len);

#ifndef NDEBUG
    /* validate the metadata cache image we just constructed by decoding it
     * and comparing the result with the original data.
     */
    {
        uint32_t       old_chksum;
        const uint8_t *q;
        H5C_t         *fake_cache_ptr = NULL;
        unsigned       v;
        herr_t         status; /* Status from decoding */

        fake_cache_ptr = (H5C_t *)H5MM_malloc(sizeof(H5C_t));
        assert(fake_cache_ptr);

        /* needed for sanity checks */
        fake_cache_ptr->image_len = cache_ptr->image_len;
        q                         = (const uint8_t *)cache_ptr->image_buffer;
        status                    = H5C__decode_cache_image_header(f, fake_cache_ptr, &q);
        assert(status >= 0);

        assert(NULL != p);
        assert(fake_cache_ptr->num_entries_in_image == cache_ptr->num_entries_in_image);

        fake_cache_ptr->image_entries = (H5C_image_entry_t *)H5MM_malloc(
            sizeof(H5C_image_entry_t) * (size_t)(fake_cache_ptr->num_entries_in_image + 1));
        assert(fake_cache_ptr->image_entries);

        for (u = 0; u < fake_cache_ptr->num_entries_in_image; u++) {
            fake_cache_ptr->image_entries[u].image_ptr = NULL;

            /* touch up f->shared->cache to satisfy sanity checks... */
            f->shared->cache = fake_cache_ptr;
            status           = H5C__decode_cache_image_entry(f, fake_cache_ptr, &q, u);
            assert(status >= 0);

            /* ...and then return f->shared->cache to its correct value */
            f->shared->cache = cache_ptr;

            /* verify expected contents */
            assert(cache_ptr->image_entries[u].addr == fake_cache_ptr->image_entries[u].addr);
            assert(cache_ptr->image_entries[u].size == fake_cache_ptr->image_entries[u].size);
            assert(cache_ptr->image_entries[u].type_id == fake_cache_ptr->image_entries[u].type_id);
            assert(cache_ptr->image_entries[u].lru_rank == fake_cache_ptr->image_entries[u].lru_rank);
            assert(cache_ptr->image_entries[u].is_dirty == fake_cache_ptr->image_entries[u].is_dirty);
            /* don't check image_fd_height as it is not stored in
             * the metadata cache image block.
             */
            assert(cache_ptr->image_entries[u].fd_child_count ==
                   fake_cache_ptr->image_entries[u].fd_child_count);
            assert(cache_ptr->image_entries[u].fd_dirty_child_count ==
                   fake_cache_ptr->image_entries[u].fd_dirty_child_count);
            assert(cache_ptr->image_entries[u].fd_parent_count ==
                   fake_cache_ptr->image_entries[u].fd_parent_count);

            for (v = 0; v < cache_ptr->image_entries[u].fd_parent_count; v++)
                assert(cache_ptr->image_entries[u].fd_parent_addrs[v] ==
                       fake_cache_ptr->image_entries[u].fd_parent_addrs[v]);

            /* free the fd_parent_addrs array if it exists */
            if (fake_cache_ptr->image_entries[u].fd_parent_addrs) {
                assert(fake_cache_ptr->image_entries[u].fd_parent_count > 0);
                fake_cache_ptr->image_entries[u].fd_parent_addrs =
                    (haddr_t *)H5MM_xfree(fake_cache_ptr->image_entries[u].fd_parent_addrs);
                fake_cache_ptr->image_entries[u].fd_parent_count = 0;
            } /* end if */
            else
                assert(fake_cache_ptr->image_entries[u].fd_parent_count == 0);

            assert(cache_ptr->image_entries[u].image_ptr);
            assert(fake_cache_ptr->image_entries[u].image_ptr);
            assert(!memcmp(cache_ptr->image_entries[u].image_ptr, fake_cache_ptr->image_entries[u].image_ptr,
                           cache_ptr->image_entries[u].size));

            fake_cache_ptr->image_entries[u].image_ptr =
                H5MM_xfree(fake_cache_ptr->image_entries[u].image_ptr);
        } /* end for */

        assert((size_t)(q - (const uint8_t *)cache_ptr->image_buffer) ==
               cache_ptr->image_data_len - H5F_SIZEOF_CHKSUM);

        /* compute the checksum  */
        old_chksum = chksum;
        chksum     = H5_checksum_metadata(cache_ptr->image_buffer,
                                          (size_t)(cache_ptr->image_data_len - H5F_SIZEOF_CHKSUM), 0);
        assert(chksum == old_chksum);

        fake_cache_ptr->image_entries = (H5C_image_entry_t *)H5MM_xfree(fake_cache_ptr->image_entries);
        fake_cache_ptr                = (H5C_t *)H5MM_xfree(fake_cache_ptr);
    } /* end block */
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__construct_cache_image_buffer() */

/*-------------------------------------------------------------------------
 * Function:    H5C__generate_cache_image()
 *
 * Purpose:	Generate the cache image and write it to the file, if
 *		directed.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__generate_cache_image(H5F_t *f, H5C_t *cache_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(cache_ptr == f->shared->cache);
    assert(cache_ptr);

    /* Construct cache image */
    if (H5C__construct_cache_image_buffer(f, cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't create metadata cache image");

    /* Free image entries array */
    if (H5C__free_image_entries_array(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't free image entries array");

    /* Write cache image block if so configured */
    if (cache_ptr->image_ctl.flags & H5C_CI__GEN_MDC_IMAGE_BLK) {
        if (H5C__write_cache_image(f, cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "Can't write metadata cache image block to file");

        H5C__UPDATE_STATS_FOR_CACHE_IMAGE_CREATE(cache_ptr);
    } /* end if */

    /* Free cache image buffer */
    assert(cache_ptr->image_buffer);
    cache_ptr->image_buffer = H5MM_xfree(cache_ptr->image_buffer);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__generate_cache_image() */

/*-------------------------------------------------------------------------
 * Function:    H5C__free_image_entries_array
 *
 * Purpose:     If the image entries array exists, free the image
 *		associated with each entry, and then free the image
 *		entries array proper.
 *
 *		Note that by the time this function is called, the cache
 *		should have removed all entries from its data structures.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__free_image_entries_array(H5C_t *cache_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);
    assert(cache_ptr->image_ctl.generate_image);
    assert(cache_ptr->index_len == 0);

    /* Check for entries to free */
    if (cache_ptr->image_entries != NULL) {
        unsigned u; /* Local index variable */

        for (u = 0; u < cache_ptr->num_entries_in_image; u++) {
            H5C_image_entry_t *ie_ptr; /* Image entry to release */

            /* Get pointer to image entry */
            ie_ptr = &(cache_ptr->image_entries[u]);

            /* Sanity checks */
            assert(ie_ptr);
            assert(ie_ptr->image_ptr);

            /* Free the parent addrs array if appropriate */
            if (ie_ptr->fd_parent_addrs) {
                assert(ie_ptr->fd_parent_count > 0);

                ie_ptr->fd_parent_addrs = (haddr_t *)H5MM_xfree(ie_ptr->fd_parent_addrs);
            } /* end if */
            else
                assert(ie_ptr->fd_parent_count == 0);

            /* Free the image */
            ie_ptr->image_ptr = H5MM_xfree(ie_ptr->image_ptr);
        } /* end for */

        /* Free the image entries array */
        cache_ptr->image_entries = (H5C_image_entry_t *)H5MM_xfree(cache_ptr->image_entries);
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5C__free_image_entries_array() */

/*-------------------------------------------------------------------------
 * Function:    H5C__get_cache_image_config
 *
 * Purpose:     Copy the current configuration for cache image generation
 *              on file close into the instance of H5C_cache_image_ctl_t
 *              pointed to by config_ptr.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__get_cache_image_config(const H5C_t *cache_ptr, H5C_cache_image_ctl_t *config_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad cache_ptr on entry");
    if (config_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad config_ptr on entry");

    *config_ptr = cache_ptr->image_ctl;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__get_cache_image_config() */

/*-------------------------------------------------------------------------
 * Function:    H5C__read_cache_image
 *
 * Purpose:	Load the metadata cache image from the specified location
 *		in the file, and return it in the supplied buffer.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__read_cache_image(H5F_t *f, H5C_t *cache_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(cache_ptr);
    assert(H5_addr_defined(cache_ptr->image_addr));
    assert(cache_ptr->image_len > 0);
    assert(cache_ptr->image_buffer);

#ifdef H5_HAVE_PARALLEL
    {
        H5AC_aux_t *aux_ptr = (H5AC_aux_t *)cache_ptr->aux_ptr;
        int         mpi_result;

        if (NULL == aux_ptr || aux_ptr->mpi_rank == 0) {
#endif /* H5_HAVE_PARALLEL */

            /* Read the buffer (if serial access, or rank 0 of parallel access) */
            /* NOTE: if this block read is being performed on rank 0 only, throwing
             * an error here will cause other ranks to hang in the following MPI_Bcast.
             */
            if (H5F_block_read(f, H5FD_MEM_SUPER, cache_ptr->image_addr, cache_ptr->image_len,
                               cache_ptr->image_buffer) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_READERROR, FAIL, "Can't read metadata cache image block");

            H5C__UPDATE_STATS_FOR_CACHE_IMAGE_READ(cache_ptr);

#ifdef H5_HAVE_PARALLEL
            if (aux_ptr) {
                /* Broadcast cache image */
                if (MPI_SUCCESS != (mpi_result = MPI_Bcast(cache_ptr->image_buffer, (int)cache_ptr->image_len,
                                                           MPI_BYTE, 0, aux_ptr->mpi_comm)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)
            } /* end if */
        }     /* end if */
        else if (aux_ptr) {
            /* Retrieve the contents of the metadata cache image from process 0 */
            if (MPI_SUCCESS != (mpi_result = MPI_Bcast(cache_ptr->image_buffer, (int)cache_ptr->image_len,
                                                       MPI_BYTE, 0, aux_ptr->mpi_comm)))
                HMPI_GOTO_ERROR(FAIL, "can't receive cache image MPI_Bcast", mpi_result)
        } /* end else-if */
    }     /* end block */
#endif    /* H5_HAVE_PARALLEL */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__read_cache_image() */

/*-------------------------------------------------------------------------
 * Function:    H5C__load_cache_image
 *
 * Purpose:     Read the cache image superblock extension message and
 *		delete it if so directed.
 *
 *		Then load the cache image block at the specified location,
 *		decode it, and insert its contents into the metadata
 *		cache.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__load_cache_image(H5F_t *f)
{
    H5C_t *cache_ptr;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);

    /* If the image address is defined, load the image, decode it,
     * and insert its contents into the metadata cache.
     *
     * Note that under normal operating conditions, it is an error if the
     * image address is HADDR_UNDEF.  However, to facilitate testing,
     * we allow this special value of the image address which means that
     * no image exists, and that the load operation should be skipped
     * silently.
     */
    if (H5_addr_defined(cache_ptr->image_addr)) {
        /* Sanity checks */
        assert(cache_ptr->image_len > 0);
        assert(cache_ptr->image_buffer == NULL);

        /* Allocate space for the image */
        if (NULL == (cache_ptr->image_buffer = H5MM_malloc(cache_ptr->image_len + 1)))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for cache image buffer");

        /* Load the image from file */
        if (H5C__read_cache_image(f, cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_READERROR, FAIL, "Can't read metadata cache image block");

        /* Reconstruct cache contents, from image */
        if (H5C__reconstruct_cache_contents(f, cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTDECODE, FAIL, "Can't reconstruct cache contents from image block");

        /* Free the image buffer */
        cache_ptr->image_buffer = H5MM_xfree(cache_ptr->image_buffer);

        /* Update stats -- must do this now, as we are about
         * to discard the size of the cache image.
         */
        H5C__UPDATE_STATS_FOR_CACHE_IMAGE_LOAD(cache_ptr);

        cache_ptr->image_loaded = true;
    } /* end if */

    /* If directed, free the on disk metadata cache image */
    if (cache_ptr->delete_image) {
        if (H5F__super_ext_remove_msg(f, H5O_MDCI_MSG_ID) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL,
                        "can't remove metadata cache image message from superblock extension");

        /* Reset image block values */
        cache_ptr->image_len      = 0;
        cache_ptr->image_data_len = 0;
        cache_ptr->image_addr     = HADDR_UNDEF;
    } /* end if */

done:
    if (ret_value < 0) {
        if (H5_addr_defined(cache_ptr->image_addr))
            cache_ptr->image_buffer = H5MM_xfree(cache_ptr->image_buffer);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__load_cache_image() */

/*-------------------------------------------------------------------------
 * Function:    H5C_load_cache_image_on_next_protect()
 *
 * Purpose:     Note the fact that a metadata cache image superblock
 *		extension message exists, along with the base address
 *		and length of the metadata cache image block.
 *
 *		Once this notification is received the metadata cache
 *		image block must be read, decoded, and loaded into the
 *		cache on the next call to H5C_protect().
 *
 *		Further, if the file is opened R/W, the metadata cache
 *		image superblock extension message must be deleted from
 *		the superblock extension and the image block freed
 *
 *		Contrawise, if the file is opened R/O, the metadata
 *		cache image superblock extension message and image block
 *		must be left as is.  Further, any dirty entries in the
 *		cache image block must be marked as clean to avoid
 *		attempts to write them on file close.
 *
 * Return:      SUCCEED
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_load_cache_image_on_next_protect(H5F_t *f, haddr_t addr, hsize_t len, bool rw)
{
    H5C_t *cache_ptr;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);

    /* Set information needed to load cache image */
    cache_ptr->image_addr   = addr;
    cache_ptr->image_len    = len;
    cache_ptr->load_image   = true;
    cache_ptr->delete_image = rw;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5C_load_cache_image_on_next_protect() */

/*-------------------------------------------------------------------------
 * Function:    H5C__image_entry_cmp
 *
 * Purpose:     Comparison callback for qsort(3) on image entries.
 *		Entries are sorted first by flush dependency height,
 *		and then by LRU rank.
 *
 * Note:        Entries with a _greater_ flush dependency height should
 *		be sorted earlier than entries with lower heights, since
 *		leafs in the flush dependency graph are at height 0, and their
 *		parents need to be earlier in the image, so that they can
 *		construct their flush dependencies when decoded.
 *
 * Return:      An integer less than, equal to, or greater than zero if the
 *		first entry is considered to be respectively less than,
 *		equal to, or greater than the second.
 *
 *-------------------------------------------------------------------------
 */
static int
H5C__image_entry_cmp(const void *_entry1, const void *_entry2)
{
    const H5C_image_entry_t *entry1 =
        (const H5C_image_entry_t *)_entry1; /* Pointer to first image entry to compare */
    const H5C_image_entry_t *entry2 =
        (const H5C_image_entry_t *)_entry2; /* Pointer to second image entry to compare */
    int ret_value = 0;                      /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(entry1);
    assert(entry2);

    if (entry1->image_fd_height > entry2->image_fd_height)
        ret_value = -1;
    else if (entry1->image_fd_height < entry2->image_fd_height)
        ret_value = 1;
    else {
        /* Sanity check */
        assert(entry1->lru_rank >= -1);
        assert(entry2->lru_rank >= -1);

        if (entry1->lru_rank < entry2->lru_rank)
            ret_value = -1;
        else if (entry1->lru_rank > entry2->lru_rank)
            ret_value = 1;
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__image_entry_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5C__prep_image_for_file_close
 *
 * Purpose:     The objective of the call is to allow the metadata cache
 *		to do any preparatory work prior to generation of a
 *		cache image.
 *
 *		In particular, the cache must
 *
 *		1) serialize all its entries,
 *
 *		2) compute the size of the metadata cache image,
 *
 *		3) allocate space for the metadata cache image, and
 *
 *		4) setup the metadata cache image superblock extension
 *		   message with the address and size of the metadata
 *		   cache image.
 *
 *		The parallel case is complicated by the fact that
 *		while all metadata caches must contain the same set of
 *		dirty entries, there is no such requirement for clean
 *		entries or the order that entries appear in the LRU.
 *
 *		Thus, there is no requirement that different processes
 *		will construct cache images of the same size.
 *
 *		This is not a major issue as long as all processes include
 *		the same set of dirty entries in the cache -- as they
 *		currently do (note that this will change when we implement
 *		the ageout feature).  Since only the process zero cache
 *		writes the cache image, all that is necessary is to
 *		broadcast the process zero cache size for use in the
 *		superblock extension messages and cache image block
 *		allocations.
 *
 *		Note: At present, cache image is disabled in the
 *		parallel case as the new collective metadata write
 *		code must be modified to support cache image.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C__prep_image_for_file_close(H5F_t *f, bool *image_generated)
{
    H5C_t  *cache_ptr     = NULL;
    haddr_t eoa_frag_addr = HADDR_UNDEF;
    hsize_t eoa_frag_size = 0;
    herr_t  ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(image_generated);

    /* If the file is opened and closed without any access to
     * any group or data set, it is possible that the cache image (if
     * it exists) has not been read yet.  Do this now if required.
     */
    if (cache_ptr->load_image) {
        cache_ptr->load_image = false;
        if (H5C__load_cache_image(f) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTLOAD, FAIL, "can't load cache image");
    } /* end if */

    /* Before we start to generate the cache image (if requested), verify
     * that the superblock supports superblock extension messages, and
     * silently cancel any request for a cache image if it does not.
     *
     * Ideally, we would do this when the cache image is requested,
     * but the necessary information is not necessary available at that
     * time -- hence this last minute check.
     *
     * Note that under some error conditions, the superblock will be
     * undefined in this case as well -- if so, assume that the
     * superblock does not support superblock extension messages.
     * Also verify that the file's high_bound is at least release
     * 1.10.x, otherwise cancel the request for a cache image
     */
    if ((NULL == f->shared->sblock) || (f->shared->sblock->super_vers < HDF5_SUPERBLOCK_VERSION_2) ||
        (f->shared->high_bound < H5F_LIBVER_V110)) {
        H5C_cache_image_ctl_t default_image_ctl = H5C__DEFAULT_CACHE_IMAGE_CTL;

        cache_ptr->image_ctl = default_image_ctl;
        assert(!(cache_ptr->image_ctl.generate_image));
    } /* end if */

    /* Generate the cache image, if requested */
    if (cache_ptr->image_ctl.generate_image) {
        /* Create the cache image super block extension message.
         *
         * Note that the base address and length of the metadata cache
         * image are undefined at this point, and thus will have to be
         * updated later.
         *
         * Create the super block extension message now so that space
         * is allocated for it (if necessary) before we allocate space
         * for the cache image block.
         *
         * To simplify testing, do this only if the
         * H5C_CI__GEN_MDCI_SBE_MESG bit is set in
         * cache_ptr->image_ctl.flags.
         */
        if (cache_ptr->image_ctl.flags & H5C_CI__GEN_MDCI_SBE_MESG)
            if (H5C__write_cache_image_superblock_msg(f, true) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "creation of cache image SB mesg failed.");

        /* Serialize the cache */
        if (H5C__serialize_cache(f) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "serialization of the cache failed");

        /* Scan the cache and record data needed to construct the
         * cache image.  In particular, for each entry we must record:
         *
         * 1) rank in LRU (if entry is in LRU)
         *
         * 2) Whether the entry is dirty prior to flush of
         *    cache just prior to close.
         *
         * 3) Addresses of flush dependency parents (if any).
         *
         * 4) Number of flush dependency children (if any).
         *
         * In passing, also compute the size of the metadata cache
         * image.  With the recent modifications of the free space
         * manager code, this size should be correct.
         */
        if (H5C__prep_for_file_close__scan_entries(f, cache_ptr) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "H5C__prep_for_file_close__scan_entries failed");
        assert(HADDR_UNDEF == cache_ptr->image_addr);

#ifdef H5_HAVE_PARALLEL
        /* In the parallel case, overwrite the image_len with the
         * value computed by process 0.
         */
        if (cache_ptr->aux_ptr) { /* we have multiple processes */
            int         mpi_result;
            unsigned    p0_image_len;
            H5AC_aux_t *aux_ptr;

            aux_ptr = (H5AC_aux_t *)cache_ptr->aux_ptr;
            if (aux_ptr->mpi_rank == 0) {
                aux_ptr->p0_image_len = (unsigned)cache_ptr->image_data_len;
                p0_image_len          = aux_ptr->p0_image_len;

                if (MPI_SUCCESS !=
                    (mpi_result = MPI_Bcast(&p0_image_len, 1, MPI_UNSIGNED, 0, aux_ptr->mpi_comm)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)

                assert(p0_image_len == aux_ptr->p0_image_len);
            } /* end if */
            else {
                if (MPI_SUCCESS !=
                    (mpi_result = MPI_Bcast(&p0_image_len, 1, MPI_UNSIGNED, 0, aux_ptr->mpi_comm)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_result)

                aux_ptr->p0_image_len = p0_image_len;
            } /* end else */

            /* Allocate space for a cache image of size equal to that
             * computed by the process 0.  This may be different from
             * cache_ptr->image_data_len if mpi_rank != 0.  However, since
             * cache image write is suppressed on all processes other than
             * process 0, this doesn't matter.
             *
             * Note that we allocate the cache image directly from the file
             * driver so as to avoid unsettling the free space managers.
             */
            if (HADDR_UNDEF ==
                (cache_ptr->image_addr = H5FD_alloc(f->shared->lf, H5FD_MEM_SUPER, f, (hsize_t)p0_image_len,
                                                    &eoa_frag_addr, &eoa_frag_size)))
                HGOTO_ERROR(H5E_CACHE, H5E_NOSPACE, FAIL,
                            "can't allocate file space for metadata cache image");
        } /* end if */
        else
#endif /* H5_HAVE_PARALLEL */
            /* Allocate the cache image block.  Note that we allocate this
             * this space directly from the file driver so as to avoid
             * unsettling the free space managers.
             */
            if (HADDR_UNDEF == (cache_ptr->image_addr = H5FD_alloc(f->shared->lf, H5FD_MEM_SUPER, f,
                                                                   (hsize_t)(cache_ptr->image_data_len),
                                                                   &eoa_frag_addr, &eoa_frag_size)))
                HGOTO_ERROR(H5E_CACHE, H5E_NOSPACE, FAIL,
                            "can't allocate file space for metadata cache image");

        /* Make note of the eoa after allocation of the cache image
         * block.  This value is used for sanity checking when we
         * shutdown the self referential free space managers after
         * we destroy the metadata cache.
         */
        assert(HADDR_UNDEF == f->shared->eoa_post_mdci_fsalloc);
        if (HADDR_UNDEF == (f->shared->eoa_post_mdci_fsalloc = H5FD_get_eoa(f->shared->lf, H5FD_MEM_DEFAULT)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get file size");

        /* For now, drop any fragment left over from the allocation of the
         * image block on the ground.  A fragment should only be returned
         * if the underlying file alignment is greater than 1.
         *
         * Clean this up eventually by extending the size of the cache
         * image block to the next alignment boundary, and then setting
         * the image_data_len to the actual size of the cache_image.
         *
         * On the off chance that there is some other way to get a
         * a fragment on a cache image allocation, leave the following
         * assertion in the code so we will find out.
         */
        assert((eoa_frag_size == 0) || (f->shared->alignment != 1));

        /* Eventually it will be possible for the length of the cache image
         * block on file to be greater than the size of the data it
         * contains.  However, for now they must be the same.  Set
         * cache_ptr->image_len accordingly.
         */
        cache_ptr->image_len = cache_ptr->image_data_len;

        /* update the metadata cache image superblock extension
         * message with the new cache image block base address and
         * length.
         *
         * to simplify testing, do this only if the
         * H5C_CI__GEN_MDC_IMAGE_BLK bit is set in
         * cache_ptr->image_ctl.flags.
         */
        if (cache_ptr->image_ctl.flags & H5C_CI__GEN_MDC_IMAGE_BLK)
            if (H5C__write_cache_image_superblock_msg(f, false) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "update of cache image SB mesg failed");

        /* At this point:
         *
         *   1) space in the file for the metadata cache image
         *      is allocated,
         *
         *   2) the metadata cache image superblock extension
         *      message exists and (if so configured) contains
         *      the correct data,
         *
         *   3) All entries in the cache that will appear in the
         *      cache image are serialized with up to date images.
         *
         *      Since we just updated the cache image message,
         *      the super block extension message is dirty.  However,
         *      since the superblock and the superblock extension
         *      can't be included in the cache image, this is a non-
         *      issue.
         *
         *   4) All entries in the cache that will be include in
         *      the cache are marked as such, and we have a count
         *      of same.
         *
         *   5) Flush dependency heights are calculated for all
         *      entries that will be included in the cache image.
         *
         * If there are any entries to be included in the metadata cache
         * image, allocate, populate, and sort the image_entries array.
         *
         * If the metadata cache image will be empty, delete the
         * metadata cache image superblock extension message, set
         * cache_ptr->image_ctl.generate_image to false.  This will
         * allow the file close to continue normally without the
         * unnecessary generation of the metadata cache image.
         */
        if (cache_ptr->num_entries_in_image > 0) {
            if (H5C__prep_for_file_close__setup_image_entries_array(cache_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTINIT, FAIL, "can't setup image entries array.");

            /* Sort the entries */
            qsort(cache_ptr->image_entries, (size_t)cache_ptr->num_entries_in_image,
                  sizeof(H5C_image_entry_t), H5C__image_entry_cmp);
        }      /* end if */
        else { /* cancel creation of metadata cache image */
            assert(cache_ptr->image_entries == NULL);

            /* To avoid breaking the control flow tests, only delete
             * the mdci superblock extension message if the
             * H5C_CI__GEN_MDC_IMAGE_BLK flag is set in
             * cache_ptr->image_ctl.flags.
             */
            if (cache_ptr->image_ctl.flags & H5C_CI__GEN_MDC_IMAGE_BLK)
                if (H5F__super_ext_remove_msg(f, H5O_MDCI_MSG_ID) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL,
                                "can't remove MDC image msg from superblock ext");

            cache_ptr->image_ctl.generate_image = false;
        } /* end else */

        /* Indicate that a cache image was generated */
        *image_generated = true;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__prep_image_for_file_close() */

/*-------------------------------------------------------------------------
 * Function:    H5C_set_cache_image_config
 *
 * Purpose:	If *config_ptr contains valid data, copy it into the
 *		image_ctl field of *cache_ptr.  Make adjustments for
 *		changes in configuration as required.
 *
 *              If the file is open read only, silently
 *              force the cache image configuration to its default
 *              (which disables construction of a cache image).
 *
 *              Note that in addition to being inapplicable in the
 *              read only case, cache image is also inapplicable if
 *              the superblock does not support superblock extension
 *              messages.  Unfortunately, this information need not
 *              be available at this point. Thus we check for this
 *              later, in H5C_prep_for_file_close() and cancel the
 *              cache image request if appropriate.
 *
 *		Fail if the new configuration is invalid.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_set_cache_image_config(const H5F_t *f, H5C_t *cache_ptr, H5C_cache_image_ctl_t *config_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(f);

    /* Check arguments */
    if (cache_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad cache_ptr on entry");

    /* Validate the config: */
    if (H5C_validate_cache_image_config(config_ptr) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid cache image configuration");

#ifdef H5_HAVE_PARALLEL
    /* The collective metadata write code is not currently compatible
     * with cache image.  Until this is fixed, suppress cache image silently
     * if there is more than one process.
     */
    if (cache_ptr->aux_ptr) {
        H5C_cache_image_ctl_t default_image_ctl = H5C__DEFAULT_CACHE_IMAGE_CTL;

        cache_ptr->image_ctl = default_image_ctl;
        assert(!(cache_ptr->image_ctl.generate_image));
    }
    else {
#endif /* H5_HAVE_PARALLEL */
        /* A cache image can only be generated if the file is opened read / write
         * and the superblock supports superblock extension messages.
         *
         * However, the superblock version is not available at this point --
         * hence we can only check the former requirement now.  Do the latter
         * check just before we construct the image..
         *
         * If the file is opened read / write, apply the supplied configuration.
         *
         * If it is not, set the image configuration to the default, which has
         * the effect of silently disabling the cache image if it was requested.
         */
        if (H5F_INTENT(f) & H5F_ACC_RDWR)
            cache_ptr->image_ctl = *config_ptr;
        else {
            H5C_cache_image_ctl_t default_image_ctl = H5C__DEFAULT_CACHE_IMAGE_CTL;

            cache_ptr->image_ctl = default_image_ctl;
            assert(!(cache_ptr->image_ctl.generate_image));
        }
#ifdef H5_HAVE_PARALLEL
    }
#endif /* H5_HAVE_PARALLEL */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_set_cache_image_config() */

/*-------------------------------------------------------------------------
 * Function:    H5C_validate_cache_image_config()
 *
 * Purpose:	Run a sanity check on the provided instance of struct
 *		H5AC_cache_image_config_t.
 *
 *		Do nothing and return SUCCEED if no errors are detected,
 *		and flag an error and return FAIL otherwise.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5C_validate_cache_image_config(H5C_cache_image_ctl_t *ctl_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (ctl_ptr == NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "NULL ctl_ptr on entry");
    if (ctl_ptr->version != H5C__CURR_CACHE_IMAGE_CTL_VER)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Unknown cache image control version");

    /* At present, we do not support inclusion of the adaptive resize
     * configuration in the cache image.  Thus the save_resize_status
     * field must be false.
     */
    if (ctl_ptr->save_resize_status != false)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "unexpected value in save_resize_status field");

    /* At present, we do not support prefetched entry ageouts.  Thus
     * the entry_ageout field must be set to
     * H5AC__CACHE_IMAGE__ENTRY_AGEOUT__NONE.
     */
    if (ctl_ptr->entry_ageout != H5AC__CACHE_IMAGE__ENTRY_AGEOUT__NONE)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "unexpected value in entry_ageout field");

    if ((ctl_ptr->flags & ~H5C_CI__ALL_FLAGS) != 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "unknown flag set");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C_validate_cache_image_config() */

/*************************************************************************/
/**************************** Private Functions: *************************/
/*************************************************************************/

/*-------------------------------------------------------------------------
 * Function:    H5C__cache_image_block_entry_header_size
 *
 * Purpose:     Compute the size of the header of the metadata cache
 *		image block, and return the value.
 *
 * Return:      Size of the header section of the metadata cache image
 *		block in bytes.
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5C__cache_image_block_entry_header_size(const H5F_t *f)
{
    size_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set return value */
    ret_value = (size_t)(1 +                  /* type                     */
                         1 +                  /* flags                    */
                         1 +                  /* ring                     */
                         1 +                  /* age                      */
                         2 +                  /* dependency child count   */
                         2 +                  /* dirty dep child count    */
                         2 +                  /* dependency parent count  */
                         4 +                  /* index in LRU             */
                         H5F_SIZEOF_ADDR(f) + /* entry offset             */
                         H5F_SIZEOF_SIZE(f)); /* entry length             */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__cache_image_block_entry_header_size() */

/*-------------------------------------------------------------------------
 * Function:    H5C__cache_image_block_header_size
 *
 * Purpose:     Compute the size of the header of the metadata cache
 *		image block, and return the value.
 *
 * Return:      Size of the header section of the metadata cache image
 *		block in bytes.
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5C__cache_image_block_header_size(const H5F_t *f)
{
    size_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set return value */
    ret_value = (size_t)(4 +                  /* signature           */
                         1 +                  /* version             */
                         1 +                  /* flags               */
                         H5F_SIZEOF_SIZE(f) + /* image data length   */
                         4);                  /* num_entries         */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__cache_image_block_header_size() */

/*-------------------------------------------------------------------------
 * Function:    H5C__decode_cache_image_header()
 *
 * Purpose:     Decode the metadata cache image buffer header from the
 *		supplied buffer and load the data into the supplied instance
 *		of H5C_t.  Advances the buffer pointer to the first byte
 *		after the header image, or unchanged on failure.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__decode_cache_image_header(const H5F_t *f, H5C_t *cache_ptr, const uint8_t **buf)
{
    uint8_t        version;
    uint8_t        flags;
    bool           have_resize_status = false;
    size_t         actual_header_len;
    size_t         expected_header_len;
    const uint8_t *p;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr);
    assert(buf);
    assert(*buf);

    /* Point to buffer to decode */
    p = *buf;

    /* Check signature */
    if (memcmp(p, H5C__MDCI_BLOCK_SIGNATURE, (size_t)H5C__MDCI_BLOCK_SIGNATURE_LEN) != 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad metadata cache image header signature");
    p += H5C__MDCI_BLOCK_SIGNATURE_LEN;

    /* Check version */
    version = *p++;
    if (version != (uint8_t)H5C__MDCI_BLOCK_VERSION_0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad metadata cache image version");

    /* Decode flags */
    flags = *p++;
    if (flags & H5C__MDCI_HEADER_HAVE_RESIZE_STATUS)
        have_resize_status = true;
    if (have_resize_status)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "MDC resize status not yet supported");

    /* Read image data length */
    H5F_DECODE_LENGTH(f, p, cache_ptr->image_data_len);

    /* For now -- will become <= eventually */
    if (cache_ptr->image_data_len != cache_ptr->image_len)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad metadata cache image data length");

    /* Read num entries */
    UINT32DECODE(p, cache_ptr->num_entries_in_image);
    if (cache_ptr->num_entries_in_image == 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad metadata cache entry count");

    /* Verify expected length of header */
    actual_header_len   = (size_t)(p - *buf);
    expected_header_len = H5C__cache_image_block_header_size(f);
    if (actual_header_len != expected_header_len)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad header image len");

    /* Update buffer pointer */
    *buf = p;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__decode_cache_image_header() */

#ifndef NDEBUG

/*-------------------------------------------------------------------------
 * Function:    H5C__decode_cache_image_entry()
 *
 * Purpose:     Decode the metadata cache image entry from the supplied
 *		buffer into the supplied instance of H5C_image_entry_t.
 *		This includes allocating a buffer for the entry image,
 *		loading it, and setting ie_ptr->image_ptr to point to
 *		the buffer.
 *
 *		Advances the buffer pointer to the first byte
 *		after the entry, or unchanged on failure.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__decode_cache_image_entry(const H5F_t *f, const H5C_t *cache_ptr, const uint8_t **buf, unsigned entry_num)
{
    bool               is_dirty     = false;
    bool               in_lru       = false; /* Only used in assertions */
    bool               is_fd_parent = false; /* Only used in assertions */
    bool               is_fd_child  = false; /* Only used in assertions */
    haddr_t            addr;
    hsize_t            size = 0;
    void              *image_ptr;
    uint8_t            flags = 0;
    uint8_t            type_id;
    uint8_t            ring;
    uint8_t            age;
    uint16_t           fd_child_count;
    uint16_t           fd_dirty_child_count;
    uint16_t           fd_parent_count;
    haddr_t           *fd_parent_addrs = NULL;
    int32_t            lru_rank;
    H5C_image_entry_t *ie_ptr = NULL;
    const uint8_t     *p;
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(cache_ptr == f->shared->cache);
    assert(cache_ptr);
    assert(buf);
    assert(*buf);
    assert(entry_num < cache_ptr->num_entries_in_image);
    ie_ptr = &(cache_ptr->image_entries[entry_num]);
    assert(ie_ptr);

    /* Get pointer to buffer */
    p = *buf;

    /* Decode type id */
    type_id = *p++;

    /* Decode flags */
    flags = *p++;
    if (flags & H5C__MDCI_ENTRY_DIRTY_FLAG)
        is_dirty = true;
    if (flags & H5C__MDCI_ENTRY_IN_LRU_FLAG)
        in_lru = true;
    if (flags & H5C__MDCI_ENTRY_IS_FD_PARENT_FLAG)
        is_fd_parent = true;
    if (flags & H5C__MDCI_ENTRY_IS_FD_CHILD_FLAG)
        is_fd_child = true;

    /* Decode ring */
    ring = *p++;
    assert(ring > (uint8_t)(H5C_RING_UNDEFINED));
    assert(ring < (uint8_t)(H5C_RING_NTYPES));

    /* Decode age */
    age = *p++;

    /* Decode dependency child count */
    UINT16DECODE(p, fd_child_count);
    assert((is_fd_parent && fd_child_count > 0) || (!is_fd_parent && fd_child_count == 0));

    /* Decode dirty dependency child count */
    UINT16DECODE(p, fd_dirty_child_count);
    if (fd_dirty_child_count > fd_child_count)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "invalid dirty flush dependency child count");

    /* Decode dependency parent count */
    UINT16DECODE(p, fd_parent_count);
    assert((is_fd_child && fd_parent_count > 0) || (!is_fd_child && fd_parent_count == 0));

    /* Decode index in LRU */
    INT32DECODE(p, lru_rank);
    assert((in_lru && lru_rank >= 0) || (!in_lru && lru_rank == -1));

    /* Decode entry offset */
    H5F_addr_decode(f, &p, &addr);
    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "invalid entry offset");

    /* Decode entry length */
    H5F_DECODE_LENGTH(f, p, size);
    if (size == 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "invalid entry size");

    /* Verify expected length of entry image */
    if ((size_t)(p - *buf) != H5C__cache_image_block_entry_header_size(f))
        HGOTO_ERROR(H5E_CACHE, H5E_BADSIZE, FAIL, "Bad entry image len");

    /* If parent count greater than zero, allocate array for parent
     * addresses, and decode addresses into the array.
     */
    if (fd_parent_count > 0) {
        int i; /* Local index variable */

        if (NULL == (fd_parent_addrs = (haddr_t *)H5MM_malloc((size_t)(fd_parent_count)*H5F_SIZEOF_ADDR(f))))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                        "memory allocation failed for fd parent addrs buffer");

        for (i = 0; i < fd_parent_count; i++) {
            H5F_addr_decode(f, &p, &(fd_parent_addrs[i]));
            if (!H5_addr_defined(fd_parent_addrs[i]))
                HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "invalid flush dependency parent offset");
        } /* end for */
    }     /* end if */

    /* Allocate buffer for entry image */
    if (NULL == (image_ptr = H5MM_malloc(size + H5C_IMAGE_EXTRA_SPACE)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for on disk image buffer");

#if H5C_DO_MEMORY_SANITY_CHECKS
    H5MM_memcpy(((uint8_t *)image_ptr) + size, H5C_IMAGE_SANITY_VALUE, H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

    /* Copy the entry image from the cache image block */
    H5MM_memcpy(image_ptr, p, size);
    p += size;

    /* Copy data into target */
    ie_ptr->addr                 = addr;
    ie_ptr->size                 = size;
    ie_ptr->ring                 = (H5C_ring_t)ring;
    ie_ptr->age                  = (int32_t)age;
    ie_ptr->type_id              = (int32_t)type_id;
    ie_ptr->lru_rank             = lru_rank;
    ie_ptr->is_dirty             = is_dirty;
    ie_ptr->fd_child_count       = (uint64_t)fd_child_count;
    ie_ptr->fd_dirty_child_count = (uint64_t)fd_dirty_child_count;
    ie_ptr->fd_parent_count      = (uint64_t)fd_parent_count;
    ie_ptr->fd_parent_addrs      = fd_parent_addrs;
    ie_ptr->image_ptr            = image_ptr;

    /* Update buffer pointer */
    *buf = p;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__decode_cache_image_entry() */
#endif

/*-------------------------------------------------------------------------
 * Function:    H5C__encode_cache_image_header()
 *
 * Purpose:     Encode the metadata cache image buffer header in the
 *		supplied buffer.  Updates buffer pointer to the first byte
 *		after the header image in the buffer, or unchanged on failure.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__encode_cache_image_header(const H5F_t *f, const H5C_t *cache_ptr, uint8_t **buf)
{
    size_t   actual_header_len;
    size_t   expected_header_len;
    uint8_t  flags = 0;
    uint8_t *p;                   /* Pointer into cache image buffer */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);
    assert(cache_ptr->image_ctl.generate_image);
    assert(cache_ptr->index_len == 0);
    assert(cache_ptr->image_data_len > 0);
    assert(cache_ptr->image_data_len <= cache_ptr->image_len);
    assert(buf);
    assert(*buf);

    /* Set pointer into buffer */
    p = *buf;

    /* write signature */
    H5MM_memcpy(p, H5C__MDCI_BLOCK_SIGNATURE, (size_t)H5C__MDCI_BLOCK_SIGNATURE_LEN);
    p += H5C__MDCI_BLOCK_SIGNATURE_LEN;

    /* write version */
    *p++ = (uint8_t)H5C__MDCI_BLOCK_VERSION_0;

    /* setup and write flags */

    /* at present we don't support saving resize status */
    assert(!cache_ptr->image_ctl.save_resize_status);
    if (cache_ptr->image_ctl.save_resize_status)
        flags |= H5C__MDCI_HEADER_HAVE_RESIZE_STATUS;

    *p++ = flags;

    /* Encode image data length */
    /* this must be true at present */
    assert(cache_ptr->image_len == cache_ptr->image_data_len);
    H5F_ENCODE_LENGTH(f, p, cache_ptr->image_data_len);

    /* write num entries */
    UINT32ENCODE(p, cache_ptr->num_entries_in_image);

    /* verify expected length of header */
    actual_header_len   = (size_t)(p - *buf);
    expected_header_len = H5C__cache_image_block_header_size(f);
    if (actual_header_len != expected_header_len)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad header image len");

    /* Update buffer pointer */
    *buf = p;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__encode_cache_image_header() */

/*-------------------------------------------------------------------------
 * Function:    H5C__encode_cache_image_entry()
 *
 * Purpose:     Encode the metadata cache image buffer header in the
 *		supplied buffer.  Updates buffer pointer to the first byte
 *		after the entry in the buffer, or unchanged on failure.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__encode_cache_image_entry(H5F_t *f, H5C_t *cache_ptr, uint8_t **buf, unsigned entry_num)
{
    H5C_image_entry_t *ie_ptr;              /* Pointer to entry to encode */
    uint8_t            flags = 0;           /* Flags for entry */
    uint8_t           *p;                   /* Pointer into cache image buffer */
    unsigned           u;                   /* Local index value */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(cache_ptr == f->shared->cache);
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);
    assert(cache_ptr->image_ctl.generate_image);
    assert(cache_ptr->index_len == 0);
    assert(buf);
    assert(*buf);
    assert(entry_num < cache_ptr->num_entries_in_image);
    ie_ptr = &(cache_ptr->image_entries[entry_num]);

    /* Get pointer to buffer to encode into */
    p = *buf;

    /* Encode type */
    if ((ie_ptr->type_id < 0) || (ie_ptr->type_id > 255))
        HGOTO_ERROR(H5E_CACHE, H5E_BADRANGE, FAIL, "type_id out of range.");
    *p++ = (uint8_t)(ie_ptr->type_id);

    /* Compose and encode flags */
    if (ie_ptr->is_dirty)
        flags |= H5C__MDCI_ENTRY_DIRTY_FLAG;
    if (ie_ptr->lru_rank > 0)
        flags |= H5C__MDCI_ENTRY_IN_LRU_FLAG;
    if (ie_ptr->fd_child_count > 0)
        flags |= H5C__MDCI_ENTRY_IS_FD_PARENT_FLAG;
    if (ie_ptr->fd_parent_count > 0)
        flags |= H5C__MDCI_ENTRY_IS_FD_CHILD_FLAG;
    *p++ = flags;

    /* Encode ring */
    *p++ = (uint8_t)(ie_ptr->ring);

    /* Encode age */
    *p++ = (uint8_t)(ie_ptr->age);

    /* Validate and encode dependency child count */
    if (ie_ptr->fd_child_count > H5C__MDCI_MAX_FD_CHILDREN)
        HGOTO_ERROR(H5E_CACHE, H5E_BADRANGE, FAIL, "fd_child_count out of range");
    UINT16ENCODE(p, (uint16_t)(ie_ptr->fd_child_count));

    /* Validate and encode dirty dependency child count */
    if (ie_ptr->fd_dirty_child_count > H5C__MDCI_MAX_FD_CHILDREN)
        HGOTO_ERROR(H5E_CACHE, H5E_BADRANGE, FAIL, "fd_dirty_child_count out of range");
    UINT16ENCODE(p, (uint16_t)(ie_ptr->fd_dirty_child_count));

    /* Validate and encode dependency parent count */
    if (ie_ptr->fd_parent_count > H5C__MDCI_MAX_FD_PARENTS)
        HGOTO_ERROR(H5E_CACHE, H5E_BADRANGE, FAIL, "fd_parent_count out of range");
    UINT16ENCODE(p, (uint16_t)(ie_ptr->fd_parent_count));

    /* Encode index in LRU */
    INT32ENCODE(p, ie_ptr->lru_rank);

    /* Encode entry offset */
    H5F_addr_encode(f, &p, ie_ptr->addr);

    /* Encode entry length */
    H5F_ENCODE_LENGTH(f, p, ie_ptr->size);

    /* Verify expected length of entry image */
    if ((size_t)(p - *buf) != H5C__cache_image_block_entry_header_size(f))
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "Bad entry image len");

    /* Encode dependency parent offsets -- if any */
    for (u = 0; u < ie_ptr->fd_parent_count; u++)
        H5F_addr_encode(f, &p, ie_ptr->fd_parent_addrs[u]);

    /* Copy entry image */
    H5MM_memcpy(p, ie_ptr->image_ptr, ie_ptr->size);
    p += ie_ptr->size;

    /* Update buffer pointer */
    *buf = p;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__encode_cache_image_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C__prep_for_file_close__compute_fd_heights
 *
 * Purpose:     The purpose of this function is to compute the flush
 *		dependency height of all entries that appear in the cache
 *		image.
 *
 *		At present, entries are included or excluded from the
 *		cache image depending upon the ring in which they reside.
 *		Thus there is no chance that one side of a flush dependency
 *		will be in the cache image, and the other side not.
 *
 *		However, once we start placing a limit on the size of the
 *		cache image, or start excluding prefetched entries from
 *		the cache image if they haven't been accessed in some
 *		number of file close / open cycles, this will no longer
 *		be the case.
 *
 *		In particular, if a flush dependency child is dirty, and
 *		one of its flush dependency parents is dirty and not in
 *		the cache image, then the flush dependency child cannot
 *		be in the cache image without violating flush ordering.
 *
 *		Observe that a clean flush dependency child can be either
 *		in or out of the cache image without effect on flush
 *		dependencies.
 *
 *		Similarly, a flush dependency parent can always be part
 *		of a cache image, regardless of whether it is clean or
 *		dirty -- but remember that a flush dependency parent can
 *		also be a flush dependency child.
 *
 *		Finally, note that for purposes of the cache image, flush
 *		dependency height ends when a flush dependency relation
 *		passes off the cache image.
 *
 *		On exit, the flush dependency height of each entry in the
 *		cache image should be calculated and stored in the cache
 *		entry.  Entries will be removed from the cache image if
 *		necessary to maintain flush ordering.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__prep_for_file_close__compute_fd_heights(const H5C_t *cache_ptr)
{
    H5C_cache_entry_t *entry_ptr;
    H5C_cache_entry_t *parent_ptr;
#ifndef NDEBUG
    unsigned entries_removed_from_image      = 0;
    unsigned external_parent_fd_refs_removed = 0;
    unsigned external_child_fd_refs_removed  = 0;
#endif
    bool     done = false;
    unsigned u; /* Local index variable */
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* sanity checks */
    assert(cache_ptr);

    /* Remove from the cache image all dirty entries that are
     * flush dependency children of dirty entries that are not in the
     * cache image.  Must do this, as if we fail to do so, the parent
     * will be written to file before the child.  Since it is possible
     * that the child will have dirty children of its own, this may take
     * multiple passes through the index list.
     */
    done = false;
    while (!done) {
        done      = true;
        entry_ptr = cache_ptr->il_head;
        while (entry_ptr != NULL) {
            /* Should this entry be in the image */
            if (entry_ptr->image_dirty && entry_ptr->include_in_image && (entry_ptr->fd_parent_count > 0)) {
                assert(entry_ptr->flush_dep_parent != NULL);
                for (u = 0; u < entry_ptr->flush_dep_nparents; u++) {
                    parent_ptr = entry_ptr->flush_dep_parent[u];

                    /* Sanity check parent */
                    assert(entry_ptr->ring == parent_ptr->ring);

                    if (parent_ptr->is_dirty && !parent_ptr->include_in_image &&
                        entry_ptr->include_in_image) {

                        /* Must remove child from image -- only do this once */
#ifndef NDEBUG
                        entries_removed_from_image++;
#endif
                        entry_ptr->include_in_image = false;
                    } /* end if */
                }     /* for */
            }         /* end if */

            entry_ptr = entry_ptr->il_next;
        } /* while ( entry_ptr != NULL ) */
    }     /* while ( ! done ) */

    /* at present, entries are included in the cache image if they reside
     * in a specified set of rings.  Thus it should be impossible for
     * entries_removed_from_image to be positive.  Assert that this is
     * so.  Note that this will change when we start aging entries out
     * of the cache image.
     */
    assert(entries_removed_from_image == 0);

    /* Next, remove from entries in the cache image, references to
     * flush dependency parents or children that are not in the cache image.
     */
    entry_ptr = cache_ptr->il_head;
    while (entry_ptr != NULL) {
        if (!entry_ptr->include_in_image && entry_ptr->flush_dep_nparents > 0) {
            assert(entry_ptr->flush_dep_parent != NULL);

            for (u = 0; u < entry_ptr->flush_dep_nparents; u++) {
                parent_ptr = entry_ptr->flush_dep_parent[u];

                /* Sanity check parent */
                assert(entry_ptr->ring == parent_ptr->ring);

                if (parent_ptr->include_in_image) {
                    /* Must remove reference to child */
                    assert(parent_ptr->fd_child_count > 0);
                    parent_ptr->fd_child_count--;

                    if (entry_ptr->is_dirty) {
                        assert(parent_ptr->fd_dirty_child_count > 0);
                        parent_ptr->fd_dirty_child_count--;
                    } /* end if */

#ifndef NDEBUG
                    external_child_fd_refs_removed++;
#endif
                } /* end if */
            }     /* for */
        }         /* end if */
        else if (entry_ptr->include_in_image && entry_ptr->flush_dep_nparents > 0) {
            /* Sanity checks */
            assert(entry_ptr->flush_dep_parent != NULL);
            assert(entry_ptr->flush_dep_nparents == entry_ptr->fd_parent_count);
            assert(entry_ptr->fd_parent_addrs);

            for (u = 0; u < entry_ptr->flush_dep_nparents; u++) {
                parent_ptr = entry_ptr->flush_dep_parent[u];

                /* Sanity check parent */
                assert(entry_ptr->ring == parent_ptr->ring);

                if (!parent_ptr->include_in_image) {
                    /* Must remove reference to parent */
                    assert(entry_ptr->fd_parent_count > 0);
                    parent_ptr->fd_child_count--;

                    assert(parent_ptr->addr == entry_ptr->fd_parent_addrs[u]);

                    entry_ptr->fd_parent_addrs[u] = HADDR_UNDEF;
#ifndef NDEBUG
                    external_parent_fd_refs_removed++;
#endif
                } /* end if */
            }     /* for */

            /* Touch up fd_parent_addrs array if necessary */
            if (entry_ptr->fd_parent_count == 0) {
                H5MM_xfree(entry_ptr->fd_parent_addrs);
                entry_ptr->fd_parent_addrs = NULL;
            } /* end if */
            else if (entry_ptr->flush_dep_nparents > entry_ptr->fd_parent_count) {
                haddr_t *old_fd_parent_addrs = entry_ptr->fd_parent_addrs;
                unsigned v;

                if (NULL == (entry_ptr->fd_parent_addrs = (haddr_t *)H5MM_calloc(
                                 sizeof(haddr_t) * (size_t)(entry_ptr->fd_parent_addrs))))
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                                "memory allocation failed for fd parent addr array");

                v = 0;
                for (u = 0; u < entry_ptr->flush_dep_nparents; u++) {
                    if (old_fd_parent_addrs[u] != HADDR_UNDEF) {
                        entry_ptr->fd_parent_addrs[v] = old_fd_parent_addrs[u];
                        v++;
                    } /* end if */
                }     /* end for */

                assert(v == entry_ptr->fd_parent_count);
            } /* end else-if */
        }     /* end else-if */

        entry_ptr = entry_ptr->il_next;
    } /* while (entry_ptr != NULL) */

    /* At present, no external parent or child flush dependency links
     * should exist -- hence the following assertions.  This will change
     * if we support ageout of entries in the cache image.
     */
    assert(external_child_fd_refs_removed == 0);
    assert(external_parent_fd_refs_removed == 0);

    /* At this point we should have removed all flush dependencies that
     * cross cache image boundaries.  Now compute the flush dependency
     * heights for all entries in the image.
     *
     * Until I can think of a better way, do this via a depth first
     * search implemented via a recursive function call.
     *
     * Note that entry_ptr->image_fd_height has already been initialized to 0
     * for all entries that may appear in the cache image.
     */
    entry_ptr = cache_ptr->il_head;
    while (entry_ptr != NULL) {
        if (entry_ptr->include_in_image && entry_ptr->fd_child_count == 0 && entry_ptr->fd_parent_count > 0) {
            for (u = 0; u < entry_ptr->fd_parent_count; u++) {
                parent_ptr = entry_ptr->flush_dep_parent[u];

                if (parent_ptr->include_in_image && parent_ptr->image_fd_height <= 0)
                    H5C__prep_for_file_close__compute_fd_heights_real(parent_ptr, 1);
            } /* end for */
        }     /* end if */

        entry_ptr = entry_ptr->il_next;
    } /* while (entry_ptr != NULL) */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__prep_for_file_close__compute_fd_heights() */

/*-------------------------------------------------------------------------
 * Function:    H5C__prep_for_file_close__compute_fd_heights_real
 *
 * Purpose:     H5C__prep_for_file_close__compute_fd_heights() prepares
 *		for the computation of flush dependency heights of all
 *		entries in the cache image, this function actually does
 *		it.
 *
 *		The basic observation behind this function is as follows:
 *
 *		Suppose you have an entry E with a flush dependency
 *		height of X.  Then the parents of E must all have
 *		flush dependency X + 1 or greater.
 *
 *		Use this observation to compute flush dependency height
 *		of all entries in the cache image via the following
 *		recursive algorithm:
 *
 *		1) On entry, set the flush dependency height of the
 *		   supplied cache entry to the supplied value.
 *
 *		2) Examine all the flush dependency parents of the
 *		   supplied entry.
 *
 *		   If the parent is in the cache image, and has flush
 *		   dependency height less than or equal to the flush
 *		   dependency height of the current entry, call the
 *		   recursive routine on the parent with flush dependency
 *		   height equal to the flush dependency height of the
 *		   child plus 1.
 *
 *		   Otherwise do nothing.
 *
 *		Observe that if the flush dependency height of all entries
 *		in the image is initialized to zero, and if this recursive
 *		function is called with flush dependency height 0 on all
 *		entries in the cache image with FD parents in the image,
 *		but without FD children in the image, the correct flush
 *		dependency height should be set for all entries in the
 *		cache image.
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
static void
H5C__prep_for_file_close__compute_fd_heights_real(H5C_cache_entry_t *entry_ptr, uint32_t fd_height)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(entry_ptr);
    assert(entry_ptr->include_in_image);
    assert((entry_ptr->image_fd_height == 0) || (entry_ptr->image_fd_height < fd_height));
    assert(((fd_height == 0) && (entry_ptr->fd_child_count == 0)) ||
           ((fd_height > 0) && (entry_ptr->fd_child_count > 0)));

    entry_ptr->image_fd_height = fd_height;
    if (entry_ptr->flush_dep_nparents > 0) {
        unsigned u;

        assert(entry_ptr->flush_dep_parent);
        for (u = 0; u < entry_ptr->fd_parent_count; u++) {
            H5C_cache_entry_t *parent_ptr;

            parent_ptr = entry_ptr->flush_dep_parent[u];

            if (parent_ptr->include_in_image && parent_ptr->image_fd_height <= fd_height)
                H5C__prep_for_file_close__compute_fd_heights_real(parent_ptr, fd_height + 1);
        } /* end for */
    }     /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* H5C__prep_for_file_close__compute_fd_heights_real() */

/*-------------------------------------------------------------------------
 * Function:    H5C__prep_for_file_close__setup_image_entries_array
 *
 * Purpose:     Allocate space for the image_entries array, and load
 *		each instance of H5C_image_entry_t in the array with
 *		the data necessary to construct the metadata cache image.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__prep_for_file_close__setup_image_entries_array(H5C_t *cache_ptr)
{
    H5C_cache_entry_t *entry_ptr;
    H5C_image_entry_t *image_entries = NULL;
#ifndef NDEBUG
    uint32_t entries_visited = 0;
#endif
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);
    assert(cache_ptr->pl_len == 0);
    assert(cache_ptr->num_entries_in_image > 0);
    assert(cache_ptr->image_entries == NULL);

    /* Allocate and initialize image_entries array */
    if (NULL == (image_entries = (H5C_image_entry_t *)H5MM_calloc(
                     sizeof(H5C_image_entry_t) * (size_t)(cache_ptr->num_entries_in_image + 1))))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL, "memory allocation failed for image_entries");

    /* Initialize (non-zero/NULL/false) fields */
    for (u = 0; u <= cache_ptr->num_entries_in_image; u++) {
        image_entries[u].addr    = HADDR_UNDEF;
        image_entries[u].ring    = H5C_RING_UNDEFINED;
        image_entries[u].type_id = -1;
    } /* end for */

    /* Scan each entry on the index list and populate the image_entries array */
    u         = 0;
    entry_ptr = cache_ptr->il_head;
    while (entry_ptr != NULL) {
        if (entry_ptr->include_in_image) {
            /* Since we have already serialized the cache, the following
             * should hold.
             */
            assert(entry_ptr->image_up_to_date);
            assert(entry_ptr->image_ptr);
            assert(entry_ptr->type);

            image_entries[u].addr = entry_ptr->addr;
            image_entries[u].size = entry_ptr->size;
            image_entries[u].ring = entry_ptr->ring;

            /* When a prefetched entry is included in the image, store
             * its underlying type id in the image entry, not
             * H5AC_PREFETCHED_ENTRY_ID.  In passing, also increment
             * the age (up to H5AC__CACHE_IMAGE__ENTRY_AGEOUT__MAX).
             */
            if (entry_ptr->type->id == H5AC_PREFETCHED_ENTRY_ID) {
                image_entries[u].type_id = entry_ptr->prefetch_type_id;

                if (entry_ptr->age >= H5AC__CACHE_IMAGE__ENTRY_AGEOUT__MAX)
                    image_entries[u].age = H5AC__CACHE_IMAGE__ENTRY_AGEOUT__MAX;
                else
                    image_entries[u].age = entry_ptr->age + 1;
            } /* end if */
            else {
                image_entries[u].type_id = entry_ptr->type->id;
                image_entries[u].age     = 0;
            } /* end else */

            image_entries[u].lru_rank             = entry_ptr->lru_rank;
            image_entries[u].is_dirty             = entry_ptr->is_dirty;
            image_entries[u].image_fd_height      = entry_ptr->image_fd_height;
            image_entries[u].fd_parent_count      = entry_ptr->fd_parent_count;
            image_entries[u].fd_parent_addrs      = entry_ptr->fd_parent_addrs;
            image_entries[u].fd_child_count       = entry_ptr->fd_child_count;
            image_entries[u].fd_dirty_child_count = entry_ptr->fd_dirty_child_count;
            image_entries[u].image_ptr            = entry_ptr->image_ptr;

            /* Null out entry_ptr->fd_parent_addrs and set
             * entry_ptr->fd_parent_count to zero so that ownership of the
             * flush dependency parents address array is transferred to the
             * image entry.
             */
            entry_ptr->fd_parent_count = 0;
            entry_ptr->fd_parent_addrs = NULL;

            u++;

            assert(u <= cache_ptr->num_entries_in_image);
        } /* end if */

#ifndef NDEBUG
        entries_visited++;
#endif

        entry_ptr = entry_ptr->il_next;
    } /* end while */

    /* Sanity checks */
    assert(entries_visited == cache_ptr->index_len);
    assert(u == cache_ptr->num_entries_in_image);

    assert(image_entries[u].fd_parent_addrs == NULL);
    assert(image_entries[u].image_ptr == NULL);

    cache_ptr->image_entries = image_entries;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__prep_for_file_close__setup_image_entries_array() */

/*-------------------------------------------------------------------------
 * Function:    H5C__prep_for_file_close__scan_entries
 *
 * Purpose:     Scan all entries in the metadata cache, and store all
 *		entry specific data required for construction of the
 *		metadata cache image block and likely to be discarded
 *		or modified during the cache flush on file close.
 *
 *		In particular, make note of:
 *			entry rank in LRU
 *			whether the entry is dirty
 *			base address of entry flush dependency parent,
 *			        if it exists.
 *			number of flush dependency children, if any.
 *
 *		Also, determine which entries are to be included in the
 *		metadata cache image.  At present, all entries other than
 *		the superblock, the superblock extension object header and
 *		its associated chunks (if any) are included.
 *
 *		Finally, compute the size of the metadata cache image
 *		block.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__prep_for_file_close__scan_entries(const H5F_t *f, H5C_t *cache_ptr)
{
    H5C_cache_entry_t *entry_ptr;
    bool               include_in_image;
    int                lru_rank = 1;
#ifndef NDEBUG
    unsigned entries_visited                  = 0;
    uint32_t num_entries_tentatively_in_image = 0;
#endif
    uint32_t num_entries_in_image = 0;
    size_t   image_len;
    size_t   entry_header_len;
    size_t   fd_parents_list_len;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->sblock);
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);
    assert(cache_ptr->pl_len == 0);

    /* Initialize image len to the size of the metadata cache image block
     * header.
     */
    image_len        = H5C__cache_image_block_header_size(f);
    entry_header_len = H5C__cache_image_block_entry_header_size(f);

    /* Scan each entry on the index list */
    entry_ptr = cache_ptr->il_head;
    while (entry_ptr != NULL) {
        /* Since we have already serialized the cache, the following
         * should hold.
         */
        assert(entry_ptr->image_up_to_date);
        assert(entry_ptr->image_ptr);

        /* Initially, we mark all entries in the rings included
         * in the cache image as being included in the in the
         * image.  Depending on circumstances, we may exclude some
         * of these entries later.
         */
        if (entry_ptr->ring > H5C_MAX_RING_IN_IMAGE)
            include_in_image = false;
        else
            include_in_image = true;
        entry_ptr->include_in_image = include_in_image;

        if (include_in_image) {
            entry_ptr->lru_rank        = -1;
            entry_ptr->image_dirty     = entry_ptr->is_dirty;
            entry_ptr->image_fd_height = 0; /* will compute this later */

            /* Initially, include all flush dependency parents in the
             * the list of flush dependencies to be stored in the
             * image.  We may remove some or all of these later.
             */
            if (entry_ptr->flush_dep_nparents > 0) {
                /* The parents addresses array may already exist -- reallocate
                 * as needed.
                 */
                if (entry_ptr->flush_dep_nparents == entry_ptr->fd_parent_count) {
                    /* parent addresses array should already be allocated
                     * and of the correct size.
                     */
                    assert(entry_ptr->fd_parent_addrs);
                } /* end if */
                else if (entry_ptr->fd_parent_count > 0) {
                    assert(entry_ptr->fd_parent_addrs);
                    entry_ptr->fd_parent_addrs = (haddr_t *)H5MM_xfree(entry_ptr->fd_parent_addrs);
                } /* end else-if */
                else {
                    assert(entry_ptr->fd_parent_count == 0);
                    assert(entry_ptr->fd_parent_addrs == NULL);
                } /* end else */

                entry_ptr->fd_parent_count = entry_ptr->flush_dep_nparents;
                if (NULL == entry_ptr->fd_parent_addrs)
                    if (NULL == (entry_ptr->fd_parent_addrs = (haddr_t *)H5MM_malloc(
                                     sizeof(haddr_t) * (size_t)(entry_ptr->fd_parent_count))))
                        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                                    "memory allocation failed for fd parent addrs buffer");

                for (int i = 0; i < (int)(entry_ptr->fd_parent_count); i++) {
                    entry_ptr->fd_parent_addrs[i] = entry_ptr->flush_dep_parent[i]->addr;
                    assert(H5_addr_defined(entry_ptr->fd_parent_addrs[i]));
                } /* end for */
            }     /* end if */
            else if (entry_ptr->fd_parent_count > 0) {
                assert(entry_ptr->fd_parent_addrs);
                entry_ptr->fd_parent_addrs = (haddr_t *)H5MM_xfree(entry_ptr->fd_parent_addrs);
            } /* end else-if */
            else
                assert(entry_ptr->fd_parent_addrs == NULL);

            /* Initially, all flush dependency children are included int
             * the count of flush dependency child relationships to be
             * represented in the cache image.  Some or all of these
             * may be dropped from the image later.
             */
            if (entry_ptr->flush_dep_nchildren > 0) {
                if (!entry_ptr->is_pinned)
                    HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "encountered unpinned fd parent?!?");

                entry_ptr->fd_child_count       = entry_ptr->flush_dep_nchildren;
                entry_ptr->fd_dirty_child_count = entry_ptr->flush_dep_ndirty_children;
            } /* end if */

#ifndef NDEBUG
            num_entries_tentatively_in_image++;
#endif
        } /* end if */

#ifndef NDEBUG
        entries_visited++;
#endif
        entry_ptr = entry_ptr->il_next;
    } /* end while */
    assert(entries_visited == cache_ptr->index_len);

    /* Now compute the flush dependency heights of all flush dependency
     * relationships to be represented in the image.
     *
     * If all entries in the target rings are included in the
     * image, the flush dependency heights are simply the heights
     * of all flush dependencies in the target rings.
     *
     * However, if we restrict appearance in the cache image either
     * by number of entries in the image, restrictions on the number
     * of times a prefetched entry can appear in an image, or image
     * size, it is possible that flush dependency parents or children
     * of entries that are in the image may not be included in the
     * the image.  In this case, we must prune all flush dependency
     * relationships that cross the image boundary, and all exclude
     * from the image all dirty flush dependency children that have
     * a dirty flush dependency parent that is not in the image.
     * This is necessary to preserve the required flush ordering.
     *
     * These details are tended to by the following call to
     * H5C__prep_for_file_close__compute_fd_heights().  Because the
     * exact contents of the image cannot be known until after this
     * call, computation of the image size is delayed.
     */
    if (H5C__prep_for_file_close__compute_fd_heights(cache_ptr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "computation of flush dependency heights failed?!?");

        /* At this point, all entries that will appear in the cache
         * image should be marked correctly.  Compute the size of the
         * cache image.
         */
#ifndef NDEBUG
    entries_visited = 0;
#endif
    entry_ptr = cache_ptr->il_head;
    while (entry_ptr != NULL) {
        if (entry_ptr->include_in_image) {
            if (entry_ptr->fd_parent_count > 0)
                fd_parents_list_len = (size_t)(H5F_SIZEOF_ADDR(f) * entry_ptr->fd_parent_count);
            else
                fd_parents_list_len = (size_t)0;

            image_len += entry_header_len + fd_parents_list_len + entry_ptr->size;
            num_entries_in_image++;
        } /* end if */

#ifndef NDEBUG
        entries_visited++;
#endif
        entry_ptr = entry_ptr->il_next;
    } /* end while */
    assert(entries_visited == cache_ptr->index_len);
    assert(num_entries_in_image <= num_entries_tentatively_in_image);

#ifndef NDEBUG
    {
        unsigned j = 0;
        for (int i = H5C_MAX_RING_IN_IMAGE + 1; i <= H5C_RING_SB; i++)
            j += cache_ptr->index_ring_len[i];

        /* This will change */
        assert(entries_visited == (num_entries_tentatively_in_image + j));
    }
#endif

    cache_ptr->num_entries_in_image = num_entries_in_image;
#ifndef NDEBUG
    entries_visited = 0;
#endif

    /* Now scan the LRU list to set the lru_rank fields of all entries
     * on the LRU.
     *
     * Note that we start with rank 1, and increment by 1 with each
     * entry on the LRU.
     *
     * Note that manually pinned entryies will have lru_rank -1,
     * and no flush dependency.  Putting these entries at the head of
     * the reconstructed LRU should be appropriate.
     */
    entry_ptr = cache_ptr->LRU_head_ptr;
    while (entry_ptr != NULL) {
        assert(entry_ptr->type != NULL);

        /* to avoid confusion, don't set lru_rank on epoch markers.
         * Note that we still increment the lru_rank, so that the holes
         * in the sequence of entries on the LRU will indicate the
         * locations of epoch markers (if any) when we reconstruct
         * the LRU.
         *
         * Do not set lru_rank or increment lru_rank for entries
         * that will not be included in the cache image.
         */
        if (entry_ptr->type->id == H5AC_EPOCH_MARKER_ID)
            lru_rank++;
        else if (entry_ptr->include_in_image) {
            entry_ptr->lru_rank = lru_rank;
            lru_rank++;
        } /* end else-if */

#ifndef NDEBUG
        entries_visited++;
#endif
        entry_ptr = entry_ptr->next;
    } /* end while */
    assert(entries_visited == cache_ptr->LRU_list_len);

    image_len += H5F_SIZEOF_CHKSUM;
    cache_ptr->image_data_len = image_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__prep_for_file_close__scan_entries() */

/*-------------------------------------------------------------------------
 * Function:    H5C__reconstruct_cache_contents()
 *
 * Purpose:     Scan the image buffer, and create a prefetched
 *		cache entry for every entry in the buffer.  Insert the
 *		prefetched entries in the index and the LRU, and
 *		reconstruct any flush dependencies.  Order the entries
 *		in the LRU as indicated by the stored lru_ranks.
 *
 * Return:      SUCCEED on success, and FAIL on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__reconstruct_cache_contents(H5F_t *f, H5C_t *cache_ptr)
{
    H5C_cache_entry_t *pf_entry_ptr;        /* Pointer to prefetched entry */
    H5C_cache_entry_t *parent_ptr;          /* Pointer to parent of prefetched entry */
    const uint8_t     *p;                   /* Pointer into image buffer */
    unsigned           u, v;                /* Local index variable */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(cache_ptr == f->shared->cache);
    assert(cache_ptr);
    assert(cache_ptr->image_buffer);
    assert(cache_ptr->image_len > 0);

    /* Decode metadata cache image header */
    p = (uint8_t *)cache_ptr->image_buffer;
    if (H5C__decode_cache_image_header(f, cache_ptr, &p) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTDECODE, FAIL, "cache image header decode failed");
    assert((size_t)(p - (uint8_t *)cache_ptr->image_buffer) < cache_ptr->image_len);

    /* The image_data_len and # of entries should be defined now */
    assert(cache_ptr->image_data_len > 0);
    assert(cache_ptr->image_data_len <= cache_ptr->image_len);
    assert(cache_ptr->num_entries_in_image > 0);

    /* Reconstruct entries in image */
    for (u = 0; u < cache_ptr->num_entries_in_image; u++) {
        /* Create the prefetched entry described by the ith
         * entry in cache_ptr->image_entrise.
         */
        if (NULL == (pf_entry_ptr = H5C__reconstruct_cache_entry(f, cache_ptr, &p)))
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "reconstruction of cache entry failed");

        /* Note that we make no checks on available cache space before
         * inserting the reconstructed entry into the metadata cache.
         *
         * This is OK since the cache must be almost empty at the beginning
         * of the process, and since we check cache size at the end of the
         * reconstruction process.
         */

        /* Insert the prefetched entry in the index */
        H5C__INSERT_IN_INDEX(cache_ptr, pf_entry_ptr, FAIL);

        /* If dirty, insert the entry into the slist. */
        if (pf_entry_ptr->is_dirty)
            H5C__INSERT_ENTRY_IN_SLIST(cache_ptr, pf_entry_ptr, FAIL);

        /* Append the entry to the LRU */
        H5C__UPDATE_RP_FOR_INSERT_APPEND(cache_ptr, pf_entry_ptr, FAIL);

        H5C__UPDATE_STATS_FOR_PREFETCH(cache_ptr, pf_entry_ptr->is_dirty);

        /* If the prefetched entry is the child in one or more flush
         * dependency relationships, recreate those flush dependencies.
         */
        for (v = 0; v < pf_entry_ptr->fd_parent_count; v++) {
            /* Sanity checks */
            assert(pf_entry_ptr->fd_parent_addrs);
            assert(H5_addr_defined(pf_entry_ptr->fd_parent_addrs[v]));

            /* Find the parent entry */
            parent_ptr = NULL;
            H5C__SEARCH_INDEX(cache_ptr, pf_entry_ptr->fd_parent_addrs[v], parent_ptr, FAIL);
            if (parent_ptr == NULL)
                HGOTO_ERROR(H5E_CACHE, H5E_NOTFOUND, FAIL, "fd parent not in cache?!?");

            /* Sanity checks */
            assert(parent_ptr->addr == pf_entry_ptr->fd_parent_addrs[v]);
            assert(parent_ptr->lru_rank == -1);

            /* Must protect parent entry to set up a flush dependency.
             * Do this now, and then uprotect when done.
             */
            H5C__UPDATE_RP_FOR_PROTECT(cache_ptr, parent_ptr, FAIL);
            parent_ptr->is_protected = true;

            /* Setup the flush dependency */
            if (H5C_create_flush_dependency(parent_ptr, pf_entry_ptr) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTDEPEND, FAIL, "Can't restore flush dependency");

            /* And now unprotect */
            H5C__UPDATE_RP_FOR_UNPROTECT(cache_ptr, parent_ptr, FAIL);
            parent_ptr->is_protected = false;
        } /* end for */
    }     /* end for */

#ifndef NDEBUG
    /* Scan the cache entries, and verify that each entry has
     * the expected flush dependency status.
     */
    pf_entry_ptr = cache_ptr->il_head;
    while (pf_entry_ptr != NULL) {
        assert((pf_entry_ptr->prefetched && pf_entry_ptr->type == H5AC_PREFETCHED_ENTRY) ||
               (!pf_entry_ptr->prefetched && pf_entry_ptr->type != H5AC_PREFETCHED_ENTRY));
        if (pf_entry_ptr->type == H5AC_PREFETCHED_ENTRY)
            assert(pf_entry_ptr->fd_parent_count == pf_entry_ptr->flush_dep_nparents);

        for (v = 0; v < pf_entry_ptr->fd_parent_count; v++) {
            parent_ptr = pf_entry_ptr->flush_dep_parent[v];
            assert(parent_ptr);
            assert(pf_entry_ptr->fd_parent_addrs);
            assert(pf_entry_ptr->fd_parent_addrs[v] == parent_ptr->addr);
            assert(parent_ptr->flush_dep_nchildren > 0);
        } /* end for */

        if (pf_entry_ptr->type == H5AC_PREFETCHED_ENTRY) {
            assert(pf_entry_ptr->fd_child_count == pf_entry_ptr->flush_dep_nchildren);
            assert(pf_entry_ptr->fd_dirty_child_count == pf_entry_ptr->flush_dep_ndirty_children);
        } /* end if */

        pf_entry_ptr = pf_entry_ptr->il_next;
    } /* end while */

    /* Scan the LRU, and verify the expected ordering of the
     * prefetched entries.
     */
    {
        int                lru_rank_holes = 0;
        H5C_cache_entry_t *entry_ptr;
        int                i; /* Local index variable */

        i         = -1;
        entry_ptr = cache_ptr->LRU_head_ptr;
        while (entry_ptr != NULL) {
            assert(entry_ptr->type != NULL);

            if (entry_ptr->prefetched) {
                assert(entry_ptr->lru_rank != 0);
                assert((entry_ptr->lru_rank == -1) || (entry_ptr->lru_rank > i));

                if ((entry_ptr->lru_rank > 1) && (entry_ptr->lru_rank > i + 1))
                    lru_rank_holes += entry_ptr->lru_rank - (i + 1);
                i = entry_ptr->lru_rank;
            } /* end if */

            entry_ptr = entry_ptr->next;
        } /* end while */

        /* Holes in the sequences of LRU ranks can appear due to epoch
         * markers.  They are left in to allow re-insertion of the
         * epoch markers on reconstruction of the cache -- thus
         * the following sanity check will have to be revised when
         * we add code to store and restore adaptive resize status.
         */
        assert(lru_rank_holes <= H5C__MAX_EPOCH_MARKERS);
    } /* end block */
#endif

    /* Check to see if the cache is oversize, and evict entries as
     * necessary to remain within limits.
     */
    if (cache_ptr->index_size >= cache_ptr->max_cache_size) {
        /* cache is oversized -- call H5C__make_space_in_cache() with zero
         * space needed to repair the situation if possible.
         */
        bool write_permitted = false;

        if (cache_ptr->check_write_permitted && (cache_ptr->check_write_permitted)(f, &write_permitted) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, FAIL, "Can't get write_permitted");
        else
            write_permitted = cache_ptr->write_permitted;

        if (H5C__make_space_in_cache(f, 0, write_permitted) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTPROTECT, FAIL, "H5C__make_space_in_cache failed");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__reconstruct_cache_contents() */

/*-------------------------------------------------------------------------
 * Function:    H5C__reconstruct_cache_entry()
 *
 * Purpose:     Allocate a prefetched metadata cache entry and initialize
 *		it from image buffer.
 *
 *		Return a pointer to the newly allocated cache entry,
 *		or NULL on failure.
 *
 * Return:      Pointer to the new instance of H5C_cache_entry on success,
 *		or NULL on failure.
 *
 *-------------------------------------------------------------------------
 */
static H5C_cache_entry_t *
H5C__reconstruct_cache_entry(const H5F_t *f, H5C_t *cache_ptr, const uint8_t **buf)
{
    H5C_cache_entry_t *pf_entry_ptr = NULL; /* Reconstructed cache entry */
    uint8_t            flags        = 0;
    bool               is_dirty     = false;
#ifndef NDEBUG /* only used in assertions */
    bool in_lru       = false;
    bool is_fd_parent = false;
    bool is_fd_child  = false;
#endif
    const uint8_t     *p;
    bool               file_is_rw;
    H5C_cache_entry_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(cache_ptr);
    assert(cache_ptr->num_entries_in_image > 0);
    assert(buf && *buf);

    /* Key R/W access off of whether the image will be deleted */
    file_is_rw = cache_ptr->delete_image;

    /* Allocate space for the prefetched cache entry */
    if (NULL == (pf_entry_ptr = H5FL_CALLOC(H5C_cache_entry_t)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL, "memory allocation failed for prefetched cache entry");

    /* Get pointer to buffer */
    p = *buf;

    /* Decode type id */
    pf_entry_ptr->prefetch_type_id = *p++;

    /* Decode flags */
    flags = *p++;
    if (flags & H5C__MDCI_ENTRY_DIRTY_FLAG)
        is_dirty = true;
#ifndef NDEBUG /* only used in assertions */
    if (flags & H5C__MDCI_ENTRY_IN_LRU_FLAG)
        in_lru = true;
    if (flags & H5C__MDCI_ENTRY_IS_FD_PARENT_FLAG)
        is_fd_parent = true;
    if (flags & H5C__MDCI_ENTRY_IS_FD_CHILD_FLAG)
        is_fd_child = true;
#endif

    /* Force dirty entries to clean if the file read only -- must do
     * this as otherwise the cache will attempt to write them on file
     * close.  Since the file is R/O, the metadata cache image superblock
     * extension message and the cache image block will not be removed.
     * Hence no danger in this for subsequent opens.
     *
     * However, if the dirty entry (marked clean for purposes of the R/O
     * file open) is evicted and then referred to, the cache will read
     * either invalid or obsolete data from the file.  Handle this by
     * setting the prefetched_dirty field, and hiding such entries from
     * the eviction candidate selection algorithm.
     */
    pf_entry_ptr->is_dirty = (is_dirty && file_is_rw);

    /* Decode ring */
    pf_entry_ptr->ring = *p++;
    assert(pf_entry_ptr->ring > (uint8_t)(H5C_RING_UNDEFINED));
    assert(pf_entry_ptr->ring < (uint8_t)(H5C_RING_NTYPES));

    /* Decode age */
    pf_entry_ptr->age = *p++;

    /* Decode dependency child count */
    UINT16DECODE(p, pf_entry_ptr->fd_child_count);
    assert((is_fd_parent && pf_entry_ptr->fd_child_count > 0) ||
           (!is_fd_parent && pf_entry_ptr->fd_child_count == 0));

    /* Decode dirty dependency child count */
    UINT16DECODE(p, pf_entry_ptr->fd_dirty_child_count);
    if (!file_is_rw)
        pf_entry_ptr->fd_dirty_child_count = 0;
    if (pf_entry_ptr->fd_dirty_child_count > pf_entry_ptr->fd_child_count)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, NULL, "invalid dirty flush dependency child count");

    /* Decode dependency parent count */
    UINT16DECODE(p, pf_entry_ptr->fd_parent_count);
    assert((is_fd_child && pf_entry_ptr->fd_parent_count > 0) ||
           (!is_fd_child && pf_entry_ptr->fd_parent_count == 0));

    /* Decode index in LRU */
    INT32DECODE(p, pf_entry_ptr->lru_rank);
    assert((in_lru && pf_entry_ptr->lru_rank >= 0) || (!in_lru && pf_entry_ptr->lru_rank == -1));

    /* Decode entry offset */
    H5F_addr_decode(f, &p, &pf_entry_ptr->addr);
    if (!H5_addr_defined(pf_entry_ptr->addr))
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, NULL, "invalid entry offset");

    /* Decode entry length */
    H5F_DECODE_LENGTH(f, p, pf_entry_ptr->size);
    if (pf_entry_ptr->size == 0)
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, NULL, "invalid entry size");

    /* Verify expected length of entry image */
    if ((size_t)(p - *buf) != H5C__cache_image_block_entry_header_size(f))
        HGOTO_ERROR(H5E_CACHE, H5E_BADSIZE, NULL, "Bad entry image len");

    /* If parent count greater than zero, allocate array for parent
     * addresses, and decode addresses into the array.
     */
    if (pf_entry_ptr->fd_parent_count > 0) {
        unsigned u; /* Local index variable */

        if (NULL == (pf_entry_ptr->fd_parent_addrs = (haddr_t *)H5MM_malloc(
                         (size_t)(pf_entry_ptr->fd_parent_count) * H5F_SIZEOF_ADDR(f))))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL,
                        "memory allocation failed for fd parent addrs buffer");

        for (u = 0; u < pf_entry_ptr->fd_parent_count; u++) {
            H5F_addr_decode(f, &p, &(pf_entry_ptr->fd_parent_addrs[u]));
            if (!H5_addr_defined(pf_entry_ptr->fd_parent_addrs[u]))
                HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, NULL, "invalid flush dependency parent offset");
        } /* end for */
    }     /* end if */

    /* Allocate buffer for entry image */
    if (NULL == (pf_entry_ptr->image_ptr = H5MM_malloc(pf_entry_ptr->size + H5C_IMAGE_EXTRA_SPACE)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL, "memory allocation failed for on disk image buffer");
#if H5C_DO_MEMORY_SANITY_CHECKS
    H5MM_memcpy(((uint8_t *)pf_entry_ptr->image_ptr) + pf_entry_ptr->size, H5C_IMAGE_SANITY_VALUE,
                H5C_IMAGE_EXTRA_SPACE);
#endif /* H5C_DO_MEMORY_SANITY_CHECKS */

    /* Copy the entry image from the cache image block */
    H5MM_memcpy(pf_entry_ptr->image_ptr, p, pf_entry_ptr->size);
    p += pf_entry_ptr->size;

    /* Initialize the rest of the fields in the prefetched entry */
    /* (Only need to set non-zero/NULL/false fields, due to calloc() above) */
    pf_entry_ptr->cache_ptr        = cache_ptr;
    pf_entry_ptr->image_up_to_date = true;
    pf_entry_ptr->type             = H5AC_PREFETCHED_ENTRY;
    pf_entry_ptr->prefetched       = true;
    pf_entry_ptr->prefetched_dirty = is_dirty && (!file_is_rw);

    /* Sanity checks */
    assert(pf_entry_ptr->size > 0 && pf_entry_ptr->size < H5C_MAX_ENTRY_SIZE);

    /* Update buffer pointer */
    *buf = p;

    ret_value = pf_entry_ptr;

done:
    if (NULL == ret_value && pf_entry_ptr)
        pf_entry_ptr = H5FL_FREE(H5C_cache_entry_t, pf_entry_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__reconstruct_cache_entry() */

/*-------------------------------------------------------------------------
 * Function:    H5C__write_cache_image_superblock_msg
 *
 * Purpose:     Write the cache image superblock extension message,
 *		creating if specified.
 *
 *		In general, the size and location of the cache image block
 *		will be unknown at the time that the cache image superblock
 *		message is created.  A subsequent call to this routine will
 *		be used to write the correct data.
 *
 * Return:      Non-negative on success/Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__write_cache_image_superblock_msg(H5F_t *f, bool create)
{
    H5C_t     *cache_ptr;
    H5O_mdci_t mdci_msg; /* metadata cache image message */
                         /* to insert in the superblock  */
                         /* extension.			*/
    unsigned mesg_flags = H5O_MSG_FLAG_FAIL_IF_UNKNOWN_ALWAYS;
    herr_t   ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(f->shared);
    assert(f->shared->cache);
    cache_ptr = f->shared->cache;
    assert(cache_ptr);
    assert(cache_ptr->close_warning_received);

    /* Write data into the metadata cache image superblock extension message.
     * Note that this data will be bogus when we first create the message.
     * We will overwrite this data later in a second call to this function.
     */
    mdci_msg.addr = cache_ptr->image_addr;
#ifdef H5_HAVE_PARALLEL
    if (cache_ptr->aux_ptr) { /* we have multiple processes */
        H5AC_aux_t *aux_ptr;

        aux_ptr       = (H5AC_aux_t *)cache_ptr->aux_ptr;
        mdci_msg.size = aux_ptr->p0_image_len;
    } /* end if */
    else
#endif /* H5_HAVE_PARALLEL */
        mdci_msg.size = cache_ptr->image_len;

    /* Write metadata cache image message to superblock extension */
    if (H5F__super_ext_write_msg(f, H5O_MDCI_MSG_ID, &mdci_msg, create, mesg_flags) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_WRITEERROR, FAIL,
                    "can't write metadata cache image message to superblock extension");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__write_cache_image_superblock_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5C__write_cache_image
 *
 * Purpose:	Write the supplied metadata cache image to the specified
 *		location in file.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__write_cache_image(H5F_t *f, const H5C_t *cache_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(cache_ptr);
    assert(H5_addr_defined(cache_ptr->image_addr));
    assert(cache_ptr->image_len > 0);
    assert(cache_ptr->image_buffer);

#ifdef H5_HAVE_PARALLEL
    {
        H5AC_aux_t *aux_ptr = (H5AC_aux_t *)cache_ptr->aux_ptr;

        if (NULL == aux_ptr || aux_ptr->mpi_rank == 0) {
#endif /* H5_HAVE_PARALLEL */

            /* Write the buffer (if serial access, or rank 0 for parallel access) */
            if (H5F_block_write(f, H5FD_MEM_SUPER, cache_ptr->image_addr, cache_ptr->image_len,
                                cache_ptr->image_buffer) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_CANTFLUSH, FAIL, "can't write metadata cache image block to file");
#ifdef H5_HAVE_PARALLEL
        } /* end if */
    }     /* end block */
#endif    /* H5_HAVE_PARALLEL */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5C__write_cache_image() */
