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
 * Created:		H5Ochunk.c
 *
 * Purpose:		Object header chunk routines.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Omodule.h" /* This source code file is part of the H5O module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Opkg.h"     /* Object headers			*/

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

/* Declare the free list for H5O_chunk_proxy_t's */
H5FL_DEFINE(H5O_chunk_proxy_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_add
 *
 * Purpose:     Add new chunk for object header to metadata cache
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__chunk_add(H5F_t *f, H5O_t *oh, unsigned idx, unsigned cont_chunkno)
{
    H5O_chunk_proxy_t *chk_proxy = NULL; /* Proxy for chunk, to mark it dirty in the cache */
    H5O_chunk_proxy_t *cont_chk_proxy =
        NULL; /* Proxy for chunk containing continuation message that points to this chunk, if not chunk 0 */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(oh->cache_info.addr)

    /* check args */
    assert(f);
    assert(oh);
    assert(idx < oh->nchunks);
    assert(idx > 0);

    /* Allocate space for the object header data structure */
    if (NULL == (chk_proxy = H5FL_CALLOC(H5O_chunk_proxy_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Increment reference count on object header */
    if (H5O__inc_rc(oh) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINC, FAIL, "can't increment reference count on object header");

    /* Set the values in the chunk proxy */
    chk_proxy->f       = f;
    chk_proxy->oh      = oh;
    chk_proxy->chunkno = idx;

    /* Determine the parent of the chunk */
    if (cont_chunkno != 0) {
        if (NULL == (cont_chk_proxy = H5O__chunk_protect(f, oh, cont_chunkno)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header chunk");
        chk_proxy->fd_parent = cont_chk_proxy;
    } /* end else */

    /* Insert the chunk proxy into the cache */
    if (H5AC_insert_entry(f, H5AC_OHDR_CHK, oh->chunk[idx].addr, chk_proxy, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "unable to cache object header chunk");

    chk_proxy = NULL;

done:
    /* Cleanup on failure */
    if (ret_value < 0)
        if (chk_proxy && H5O__chunk_dest(chk_proxy) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "unable to destroy object header chunk");

    /* Release resources */
    if (cont_chk_proxy)
        if (H5O__chunk_unprotect(f, cont_chk_proxy, false) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to unprotect object header chunk");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O__chunk_add() */

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_protect
 *
 * Purpose:     Protect an object header chunk for modifications
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
H5O_chunk_proxy_t *
H5O__chunk_protect(H5F_t *f, H5O_t *oh, unsigned idx)
{
    H5O_chunk_proxy_t *chk_proxy = NULL; /* Proxy for protected chunk */
    H5O_chunk_proxy_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(oh->cache_info.addr)

    /* check args */
    assert(f);
    assert(oh);
    assert(idx < oh->nchunks);

    /* Check for protecting first chunk */
    if (0 == idx) {
        /* Create new "fake" chunk proxy for first chunk */
        /* (since the first chunk is already handled by the H5O_t object) */
        if (NULL == (chk_proxy = H5FL_CALLOC(H5O_chunk_proxy_t)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, NULL, "memory allocation failed");

        /* Increment reference count on object header */
        if (H5O__inc_rc(oh) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINC, NULL, "can't increment reference count on object header");

        /* Set chunk proxy fields */
        chk_proxy->f       = f;
        chk_proxy->oh      = oh;
        chk_proxy->chunkno = idx;
    } /* end if */
    else {
        H5O_chk_cache_ud_t chk_udata; /* User data for loading chunk */

        /* Construct the user data for protecting chunk proxy */
        /* (and _not_ decoding it) */
        memset(&chk_udata, 0, sizeof(chk_udata));
        chk_udata.oh      = oh;
        chk_udata.chunkno = idx;
        chk_udata.size    = oh->chunk[idx].size;

        /* Get the chunk proxy */
        if (NULL == (chk_proxy = (H5O_chunk_proxy_t *)H5AC_protect(f, H5AC_OHDR_CHK, oh->chunk[idx].addr,
                                                                   &chk_udata, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, NULL, "unable to load object header chunk");

        /* Sanity check */
        assert(chk_proxy->oh == oh);
        assert(chk_proxy->chunkno == idx);
    } /* end else */

    /* Set return value */
    ret_value = chk_proxy;

done:
    /* Cleanup on error */
    if (!ret_value)
        if (0 == idx && chk_proxy && H5O__chunk_dest(chk_proxy) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, NULL, "unable to destroy object header chunk");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O__chunk_protect() */

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_unprotect
 *
 * Purpose:     Unprotect an object header chunk after modifications
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__chunk_unprotect(H5F_t *f, H5O_chunk_proxy_t *chk_proxy, bool dirtied)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(chk_proxy);

    /* Check for releasing first chunk */
    if (0 == chk_proxy->chunkno) {
        /* Check for dirtying the first chunk */
        if (dirtied) {
            /* Mark object header as dirty in cache */
            if (H5AC_mark_entry_dirty(chk_proxy->oh) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTMARKDIRTY, FAIL, "unable to mark object header as dirty");
        } /* end else/if */

        /* Decrement reference count of object header */
        if (H5O__dec_rc(chk_proxy->oh) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "can't decrement reference count on object header");

        /* Free fake chunk proxy */
        chk_proxy = H5FL_FREE(H5O_chunk_proxy_t, chk_proxy);
    } /* end if */
    else {
        /* Release the chunk proxy from the cache, possibly marking it dirty */
        if (H5AC_unprotect(f, H5AC_OHDR_CHK, chk_proxy->oh->chunk[chk_proxy->chunkno].addr, chk_proxy,
                           (dirtied ? H5AC__DIRTIED_FLAG : H5AC__NO_FLAGS_SET)) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header chunk");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__chunk_unprotect() */

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_resize
 *
 * Purpose:     Resize an object header chunk
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__chunk_resize(H5O_t *oh, H5O_chunk_proxy_t *chk_proxy)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(oh);
    assert(chk_proxy);

    /* Check for resizing first chunk */
    if (0 == chk_proxy->chunkno) {
        /* Resize object header in cache */
        if (H5AC_resize_entry(oh, oh->chunk[0].size) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTRESIZE, FAIL, "unable to resize chunk in cache");
    } /* end if */
    else {
        /* Resize chunk in cache */
        if (H5AC_resize_entry(chk_proxy, oh->chunk[chk_proxy->chunkno].size) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTRESIZE, FAIL, "unable to resize chunk in cache");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__chunk_resize() */

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_update_idx
 *
 * Purpose:     Update the chunk index for a chunk proxy
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__chunk_update_idx(H5F_t *f, H5O_t *oh, unsigned idx)
{
    H5O_chunk_proxy_t *chk_proxy = NULL;    /* Proxy for chunk, to mark it dirty in the cache */
    H5O_chk_cache_ud_t chk_udata;           /* User data for loading chunk */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(oh->cache_info.addr)

    /* check args */
    assert(f);
    assert(oh);
    assert(idx < oh->nchunks);
    assert(idx > 0);

    /* Construct the user data for protecting chunk proxy */
    /* (and _not_ decoding it) */
    memset(&chk_udata, 0, sizeof(chk_udata));
    chk_udata.oh      = oh;
    chk_udata.chunkno = idx;
    chk_udata.size    = oh->chunk[idx].size;

    /* Get the chunk proxy */
    if (NULL == (chk_proxy = (H5O_chunk_proxy_t *)H5AC_protect(f, H5AC_OHDR_CHK, oh->chunk[idx].addr,
                                                               &chk_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header chunk");

    /* Update index for chunk proxy in cache */
    chk_proxy->chunkno = idx;

    /* Release the chunk proxy from the cache, marking it deleted */
    if (H5AC_unprotect(f, H5AC_OHDR_CHK, oh->chunk[idx].addr, chk_proxy, H5AC__DIRTIED_FLAG) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header chunk");

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O__chunk_update_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_delete
 *
 * Purpose:     Notify metadata cache that a chunk has been deleted
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__chunk_delete(H5F_t *f, H5O_t *oh, unsigned idx)
{
    H5O_chunk_proxy_t *chk_proxy   = NULL;               /* Proxy for chunk, to mark it dirty in the cache */
    unsigned           cache_flags = H5AC__DELETED_FLAG; /* Flags for unprotecting proxy */
    herr_t             ret_value   = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE_TAG(oh->cache_info.addr)

    /* check args */
    assert(f);
    assert(oh);
    assert(idx < oh->nchunks);
    assert(idx > 0);

    /* Get the chunk proxy */
    if (NULL == (chk_proxy = H5O__chunk_protect(f, oh, idx)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header chunk");

    /* Only free file space if not doing SWMR writes */
    if (!oh->swmr_write)
        cache_flags |= H5AC__DIRTIED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;

done:
    /* Release the chunk proxy from the cache, marking it deleted */
    if (chk_proxy && H5AC_unprotect(f, H5AC_OHDR_CHK, oh->chunk[idx].addr, chk_proxy, cache_flags) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header chunk");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O__chunk_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_dest
 *
 * Purpose:     Destroy a chunk proxy object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__chunk_dest(H5O_chunk_proxy_t *chk_proxy)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(chk_proxy);

    /* Decrement reference count of object header */
    if (H5O__dec_rc(chk_proxy->oh) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "can't decrement reference count on object header");

done:
    /* Release the chunk proxy object */
    chk_proxy = H5FL_FREE(H5O_chunk_proxy_t, chk_proxy);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__chunk_dest() */
