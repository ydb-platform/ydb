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
 * Created:     H5Cprefetched.c
 *
 * Purpose:     Metadata cache prefetched entry callbacks
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Cmodule.h" /* This source code file is part of the H5C module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata Cache                           */
#include "H5Cpkg.h"      /* Cache                                    */
#include "H5Eprivate.h"  /* Error Handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5MMprivate.h" /* Memory Management                        */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/****************************************************************************
 *
 * Declarations for prefetched cache entry callbacks.
 *
 ****************************************************************************/
static herr_t H5C__prefetched_entry_get_initial_load_size(void *udata_ptr, size_t *image_len_ptr);
static herr_t H5C__prefetched_entry_get_final_load_size(const void *image_ptr, size_t image_len,
                                                        void *udata_ptr, size_t *actual_len_ptr);
static htri_t H5C__prefetched_entry_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5C__prefetched_entry_deserialize(const void *image_ptr, size_t len, void *udata,
                                                bool *dirty_ptr);
static herr_t H5C__prefetched_entry_image_len(const void *thing, size_t *image_len_ptr);
static herr_t H5C__prefetched_entry_pre_serialize(H5F_t *f, void *thing, haddr_t addr, size_t len,
                                                  haddr_t *new_addr_ptr, size_t *new_len_ptr,
                                                  unsigned *flags_ptr);
static herr_t H5C__prefetched_entry_serialize(const H5F_t *f, void *image_ptr, size_t len, void *thing);
static herr_t H5C__prefetched_entry_notify(H5C_notify_action_t action, void *thing);
static herr_t H5C__prefetched_entry_free_icr(void *thing);
static herr_t H5C__prefetched_entry_fsf_size(const void *thing, hsize_t *fsf_size_ptr);

/*********************/
/* Package Variables */
/*********************/

/* Declare external the free list for H5C_cache_entry_t's */
H5FL_EXTERN(H5C_cache_entry_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

const H5AC_class_t H5AC_PREFETCHED_ENTRY[1] = {{
    /* id                       = */ H5AC_PREFETCHED_ENTRY_ID,
    /* name                     = */ "prefetched entry",
    /* mem_type                 = */ H5FD_MEM_DEFAULT, /* value doesn't matter */
    /* flags                    = */ H5AC__CLASS_NO_FLAGS_SET,
    /* get_initial_load_size    = */ H5C__prefetched_entry_get_initial_load_size,
    /* get_final_load_size      = */ H5C__prefetched_entry_get_final_load_size,
    /* verify_chksum            = */ H5C__prefetched_entry_verify_chksum,
    /* deserialize              = */ H5C__prefetched_entry_deserialize,
    /* image_len                = */ H5C__prefetched_entry_image_len,
    /* pre_serialize            = */ H5C__prefetched_entry_pre_serialize,
    /* serialize                = */ H5C__prefetched_entry_serialize,
    /* notify                   = */ H5C__prefetched_entry_notify,
    /* free_icr                 = */ H5C__prefetched_entry_free_icr,
    /* fsf_size                 = */ H5C__prefetched_entry_fsf_size,
}};

/***************************************************************************
 * With two exceptions, these functions should never be called, and thus
 * there is little point in documenting them separately as they all simply
 * throw an error.
 *
 * See header comments for the two exceptions (free_icr and notify).
 *
 *                                                     JRM - 8/13/15
 *
 ***************************************************************************/

static herr_t
H5C__prefetched_entry_get_initial_load_size(void H5_ATTR_UNUSED   *udata_ptr,
                                            size_t H5_ATTR_UNUSED *image_len_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5C__prefetched_entry_get_initial_load_size() */

static herr_t
H5C__prefetched_entry_get_final_load_size(const void H5_ATTR_UNUSED *image_ptr,
                                          size_t H5_ATTR_UNUSED image_len, void H5_ATTR_UNUSED *udata_ptr,
                                          size_t H5_ATTR_UNUSED *actual_len_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5C__prefetched_entry_get_final_load_size() */

static htri_t
H5C__prefetched_entry_verify_chksum(const void H5_ATTR_UNUSED *image_ptr, size_t H5_ATTR_UNUSED len,
                                    void H5_ATTR_UNUSED *udata_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5C__prefetched_verify_chksum() */

static void *
H5C__prefetched_entry_deserialize(const void H5_ATTR_UNUSED *image_ptr, size_t H5_ATTR_UNUSED len,
                                  void H5_ATTR_UNUSED *udata, bool H5_ATTR_UNUSED *dirty_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(NULL)
} /* end H5C__prefetched_entry_deserialize() */

static herr_t
H5C__prefetched_entry_image_len(const void H5_ATTR_UNUSED *thing, size_t H5_ATTR_UNUSED *image_len_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5C__prefetched_entry_image_len() */

static herr_t
H5C__prefetched_entry_pre_serialize(H5F_t H5_ATTR_UNUSED *f, void H5_ATTR_UNUSED *thing,
                                    haddr_t H5_ATTR_UNUSED addr, size_t H5_ATTR_UNUSED len,
                                    haddr_t H5_ATTR_UNUSED *new_addr_ptr, size_t H5_ATTR_UNUSED *new_len_ptr,
                                    unsigned H5_ATTR_UNUSED *flags_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5C__prefetched_entry_pre_serialize() */

static herr_t
H5C__prefetched_entry_serialize(const H5F_t H5_ATTR_UNUSED *f, void H5_ATTR_UNUSED *image_ptr,
                                size_t H5_ATTR_UNUSED len, void H5_ATTR_UNUSED *thing)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5C__prefetched_entry_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5C__prefetched_entry_notify
 *
 * Purpose:     On H5AC_NOTIFY_ACTION_BEFORE_EVICT, check to see if the
 *		target entry is a child in a flush dependency relationship.
 *		If it is, destroy that flush dependency relationship.
 *
 *		Ignore on all other notifications.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__prefetched_entry_notify(H5C_notify_action_t action, void *_thing)
{
    H5C_cache_entry_t *entry_ptr = (H5C_cache_entry_t *)_thing;
    unsigned           u;
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry_ptr);
    assert(entry_ptr->prefetched);

    switch (action) {
        case H5C_NOTIFY_ACTION_AFTER_INSERT:
        case H5C_NOTIFY_ACTION_AFTER_LOAD:
        case H5C_NOTIFY_ACTION_AFTER_FLUSH:
        case H5C_NOTIFY_ACTION_ENTRY_DIRTIED:
        case H5C_NOTIFY_ACTION_ENTRY_CLEANED:
        case H5C_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5C_NOTIFY_ACTION_CHILD_CLEANED:
        case H5C_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5C_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* do nothing */
            break;

        case H5C_NOTIFY_ACTION_BEFORE_EVICT:
            for (u = 0; u < entry_ptr->flush_dep_nparents; u++) {
                H5C_cache_entry_t *parent_ptr;

                /* Sanity checks */
                assert(entry_ptr->flush_dep_parent);
                parent_ptr = entry_ptr->flush_dep_parent[u];
                assert(parent_ptr);
                assert(parent_ptr->flush_dep_nchildren > 0);

                /* Destroy flush dependency with flush dependency parent */
                if (H5C_destroy_flush_dependency(parent_ptr, entry_ptr) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL,
                                "unable to destroy prefetched entry flush dependency");

                if (parent_ptr->prefetched) {
                    /* In prefetched entries, the fd_child_count field is
                     * used in sanity checks elsewhere.  Thus update this
                     * field to reflect the destruction of the flush
                     * dependency relationship.
                     */
                    assert(parent_ptr->fd_child_count > 0);
                    (parent_ptr->fd_child_count)--;
                } /* end if */
            }     /* end for */
            break;

        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5C__prefetched_entry_notify() */

/*-------------------------------------------------------------------------
 * Function:    H5C__prefetched_entry_free_icr
 *
 * Purpose:     Free the in core representation of the prefetched entry.
 *		Verify that the image buffer associated with the entry
 *		has been either transferred or freed.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5C__prefetched_entry_free_icr(void *_thing)
{
    H5C_cache_entry_t *entry_ptr = (H5C_cache_entry_t *)_thing;
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(entry_ptr);
    assert(entry_ptr->prefetched);

    /* Release array for flush dependency parent addresses */
    if (entry_ptr->fd_parent_addrs != NULL) {
        assert(entry_ptr->fd_parent_count > 0);
        entry_ptr->fd_parent_addrs = (haddr_t *)H5MM_xfree((void *)entry_ptr->fd_parent_addrs);
    } /* end if */
    else
        assert(entry_ptr->fd_parent_count == 0);

    if (entry_ptr->image_ptr != NULL)
        HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "prefetched entry image buffer still attached?");

    entry_ptr = H5FL_FREE(H5C_cache_entry_t, entry_ptr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5C__prefetched_entry_free_icr() */

static herr_t
H5C__prefetched_entry_fsf_size(const void H5_ATTR_UNUSED *thing, hsize_t H5_ATTR_UNUSED *fsf_size_ptr)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        HERROR(H5E_CACHE, H5E_SYSTEM, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5C__prefetched_entry_fsf_size() */
