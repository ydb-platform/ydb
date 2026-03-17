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
 * H5Iint.c - Private routines for handling IDs
 */

/****************/
/* Module Setup */
/****************/

#include "H5Imodule.h" /* This source code file is part of the H5I module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5Ipkg.h"      /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Tprivate.h"  /* Datatypes                                */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/****************/
/* Local Macros */
/****************/

/* Combine a Type number and an ID index into an ID */
#define H5I_MAKE(g, i) ((((hid_t)(g)&TYPE_MASK) << ID_BITS) | ((hid_t)(i)&ID_MASK))

/******************/
/* Local Typedefs */
/******************/

/* User data for iterator callback for retrieving an ID corresponding to an object pointer */
typedef struct {
    const void *object;   /* object pointer to search for */
    H5I_type_t  obj_type; /* type of object we are searching for */
    hid_t       ret_id;   /* ID returned */
} H5I_get_id_ud_t;

/* User data for iterator callback for ID iteration */
typedef struct {
    H5I_search_func_t user_func;  /* 'User' function to invoke */
    void             *user_udata; /* User data to pass to 'user' function */
    bool              app_ref;    /* Whether this is an appl. ref. call */
    H5I_type_t        obj_type;   /* Type of object we are iterating over */
} H5I_iterate_ud_t;

/* User data for H5I__clear_type_cb */
typedef struct {
    H5I_type_info_t *type_info; /* Pointer to the type's info to be cleared */
    bool             force;     /* Whether to always remove the ID */
    bool             app_ref;   /* Whether this is an appl. ref. call */
} H5I_clear_type_ud_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static void  *H5I__unwrap(void *object, H5I_type_t type);
static herr_t H5I__mark_node(void *_id, void *key, void *udata);
static void  *H5I__remove_common(H5I_type_info_t *type_info, hid_t id);
static int    H5I__dec_ref(hid_t id, void **request);
static int    H5I__dec_app_ref(hid_t id, void **request);
static int    H5I__dec_app_ref_always_close(hid_t id, void **request);
static int    H5I__find_id_cb(void *_item, void *_key, void *_udata);

/*********************/
/* Package Variables */
/*********************/

/* Declared extern in H5Ipkg.h and documented there */
H5I_type_info_t *H5I_type_info_array_g[H5I_MAX_NUM_TYPES];
int              H5I_next_type_g = (int)H5I_NTYPES;

/* Declare a free list to manage the H5I_id_info_t struct */
H5FL_DEFINE_STATIC(H5I_id_info_t);

/* Whether deletes are actually marks (for mark-and-sweep) */
static bool H5I_marking_s = false;

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5I_term_package
 *
 * Purpose:     Terminate the H5I interface: release all memory, reset all
 *              global variables to initial values. This only happens if all
 *              types have been destroyed from other interfaces.
 *
 * Return:      Success:    Positive if any action was taken that might
 *                          affect some other interface; zero otherwise.
 *
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5I_term_package(void)
{
    int in_use = 0; /* Number of ID types still in use */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    H5I_type_info_t *type_info = NULL; /* Pointer to ID type */
    int              i;

    /* Count the number of types still in use */
    for (i = 0; i < H5I_next_type_g; i++)
        if ((type_info = H5I_type_info_array_g[i]) && type_info->hash_table)
            in_use++;

    /* If no types are still being used then clean up */
    if (0 == in_use) {
        for (i = 0; i < H5I_next_type_g; i++) {
            type_info = H5I_type_info_array_g[i];
            if (type_info) {
                assert(NULL == type_info->hash_table);
                type_info                = H5MM_xfree(type_info);
                H5I_type_info_array_g[i] = NULL;
                in_use++;
            }
        }
    }

    FUNC_LEAVE_NOAPI(in_use)
} /* end H5I_term_package() */

/*-------------------------------------------------------------------------
 * Function:    H5I_register_type
 *
 * Purpose:     Creates a new type of ID's to give out.
 *              The class is initialized or its reference count is incremented
 *              (if it is already initialized).
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5I_register_type(const H5I_class_t *cls)
{
    H5I_type_info_t *type_info = NULL;    /* Pointer to the ID type*/
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(cls);
    assert(cls->type > 0 && (int)cls->type < H5I_MAX_NUM_TYPES);

    /* Initialize the type */
    if (NULL == H5I_type_info_array_g[cls->type]) {
        /* Allocate the type information for new type */
        if (NULL == (type_info = (H5I_type_info_t *)H5MM_calloc(sizeof(H5I_type_info_t))))
            HGOTO_ERROR(H5E_ID, H5E_CANTALLOC, FAIL, "ID type allocation failed");
        H5I_type_info_array_g[cls->type] = type_info;
    }
    else {
        /* Get the pointer to the existing type */
        type_info = H5I_type_info_array_g[cls->type];
    }

    /* Initialize the ID type structure for new types */
    if (type_info->init_count == 0) {
        type_info->cls          = cls;
        type_info->id_count     = 0;
        type_info->nextid       = cls->reserved;
        type_info->last_id_info = NULL;
        type_info->hash_table   = NULL;
    }

    /* Increment the count of the times this type has been initialized */
    type_info->init_count++;

done:
    /* Clean up on error */
    if (ret_value < 0)
        if (type_info)
            H5MM_free(type_info);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_register_type() */

/*-------------------------------------------------------------------------
 * Function:    H5I_nmembers
 *
 * Purpose:     Returns the number of members in a type.
 *
 * Return:      Success:    Number of members; zero if the type is empty
 *                          or has been deleted.
 *
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int64_t
H5I_nmembers(H5I_type_t type)
{
    H5I_type_info_t *type_info = NULL; /* Pointer to the ID type */
    int64_t          ret_value = 0;    /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Validate parameter */
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid type number");
    if (NULL == (type_info = H5I_type_info_array_g[type]) || type_info->init_count <= 0)
        HGOTO_DONE(0);

    /* Set return value */
    H5_CHECKED_ASSIGN(ret_value, int64_t, type_info->id_count, uint64_t);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_nmembers() */

/*-------------------------------------------------------------------------
 * Function:    H5I__unwrap
 *
 * Purpose:     Unwraps the object pointer for the 'item' that corresponds
 *              to an ID.
 *
 * Return:      Pointer to the unwrapped pointer (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static void *
H5I__unwrap(void *object, H5I_type_t type)
{
    void *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(object);

    /* The stored object pointer might be an H5VL_object_t, in which
     * case we'll need to get the wrapped object struct (H5F_t *, etc.).
     */
    if (H5I_FILE == type || H5I_GROUP == type || H5I_DATASET == type || H5I_ATTR == type) {
        const H5VL_object_t *vol_obj;

        vol_obj   = (const H5VL_object_t *)object;
        ret_value = H5VL_object_data(vol_obj);
    }
    else if (H5I_DATATYPE == type) {
        H5T_t *dt = (H5T_t *)object;

        ret_value = (void *)H5T_get_actual_type(dt);
    }
    else
        ret_value = object;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__unwrap() */

/*-------------------------------------------------------------------------
 * Function:    H5I_clear_type
 *
 * Purpose:     Removes all objects from the type, calling the free
 *              function for each object regardless of the reference count.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5I_clear_type(H5I_type_t type, bool force, bool app_ref)
{
    H5I_clear_type_ud_t udata; /* udata struct for callback */
    H5I_id_info_t      *item      = NULL;
    H5I_id_info_t      *tmp       = NULL;
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Validate parameters */
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid type number");

    udata.type_info = H5I_type_info_array_g[type];
    if (udata.type_info == NULL || udata.type_info->init_count <= 0)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "invalid type");

    /* Finish constructing udata */
    udata.force   = force;
    udata.app_ref = app_ref;

    /* Clearing a type is done in two phases (mark-and-sweep). This is because
     * the type's free callback can free other IDs, potentially corrupting
     * the data structure during the traversal.
     */

    /* Set marking flag */
    H5I_marking_s = true;

    /* Mark nodes for deletion */
    HASH_ITER(hh, udata.type_info->hash_table, item, tmp)
    {
        if (!item->marked)
            if (H5I__mark_node((void *)item, NULL, (void *)&udata) < 0)
                HGOTO_ERROR(H5E_ID, H5E_BADITER, FAIL, "iteration failed while clearing the ID type");
    }

    /* Unset marking flag */
    H5I_marking_s = false;

    /* Perform sweep */
    HASH_ITER(hh, udata.type_info->hash_table, item, tmp)
    {
        if (item->marked) {
            HASH_DELETE(hh, udata.type_info->hash_table, item);
            item = H5FL_FREE(H5I_id_info_t, item);
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_clear_type() */

/*-------------------------------------------------------------------------
 * Function:    H5I__mark_node
 *
 * Purpose:     Attempts to mark the node for freeing and calls the free
 *              function for the object, if any
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5I__mark_node(void *_info, void H5_ATTR_UNUSED *key, void *_udata)
{
    H5I_id_info_t       *info  = (H5I_id_info_t *)_info;        /* Current ID info being worked with */
    H5I_clear_type_ud_t *udata = (H5I_clear_type_ud_t *)_udata; /* udata struct */
    bool                 mark  = false;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(info);
    assert(udata);
    assert(udata->type_info);

    /* Do nothing to the object if the reference count is larger than
     * one and forcing is off.
     */
    if (udata->force || (info->count - (!udata->app_ref * info->app_count)) <= 1) {
        /* Check if this is an un-realized future object */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        if (info->is_future) {
            /* Discard the future object */
            if ((info->discard_cb)((void *)info->object) < 0) {
                if (udata->force) {
#ifdef H5I_DEBUG
                    if (H5DEBUG(I)) {
                        fprintf(H5DEBUG(I),
                                "H5I: discard type=%d obj=%p "
                                "failure ignored\n",
                                (int)udata->type_info->cls->type, info->object);
                    }
#endif /* H5I_DEBUG */

                    /* Indicate node should be removed from list */
                    mark = true;
                }
            }
            else {
                /* Indicate node should be removed from list */
                mark = true;
            }
        }
        else {
            /* Check for a 'free' function and call it, if it exists */
            if (udata->type_info->cls->free_func &&
                (udata->type_info->cls->free_func)((void *)info->object, H5_REQUEST_NULL) < 0) {
                if (udata->force) {
#ifdef H5I_DEBUG
                    if (H5DEBUG(I)) {
                        fprintf(H5DEBUG(I),
                                "H5I: free type=%d obj=%p "
                                "failure ignored\n",
                                (int)udata->type_info->cls->type, info->object);
                    }
#endif /* H5I_DEBUG */

                    /* Indicate node should be removed from list */
                    mark = true;
                }
            }
            else {
                /* Indicate node should be removed from list */
                mark = true;
            }
        }
        H5_GCC_CLANG_DIAG_ON("cast-qual")

        /* Remove ID if requested */
        if (mark) {
            /* Mark ID for deletion */
            info->marked = true;

            /* Decrement the number of IDs in the type */
            udata->type_info->id_count--;
        }
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5I__mark_node() */

/*-------------------------------------------------------------------------
 * Function:    H5I__destroy_type
 *
 * Purpose:     Destroys a type along with all IDs in that type
 *              regardless of their reference counts. Destroying IDs
 *              involves calling the free-func for each ID's object and
 *              then adding the ID struct to the ID free list.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5I__destroy_type(H5I_type_t type)
{
    H5I_type_info_t *type_info = NULL;    /* Pointer to the ID type */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Validate parameter */
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid type number");

    type_info = H5I_type_info_array_g[type];
    if (type_info == NULL || type_info->init_count <= 0)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "invalid type");

    /* Close/clear/destroy all IDs for this type */
    H5E_BEGIN_TRY
    {
        H5I_clear_type(type, true, false);
    }
    H5E_END_TRY /* don't care about errors */

    /* Check if we should release the ID class */
    if (type_info->cls->flags & H5I_CLASS_IS_APPLICATION)
        type_info->cls = H5MM_xfree_const(type_info->cls);

    HASH_CLEAR(hh, type_info->hash_table);
    type_info->hash_table = NULL;

    type_info = H5MM_xfree(type_info);

    H5I_type_info_array_g[type] = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__destroy_type() */

/*-------------------------------------------------------------------------
 * Function:    H5I__register
 *
 * Purpose:     Registers an OBJECT in a TYPE and returns an ID for it.
 *              This routine does _not_ check for unique-ness of the objects,
 *              if you register an object twice, you will get two different
 *              IDs for it.  This routine does make certain that each ID in a
 *              type is unique.  IDs are created by getting a unique number
 *              for the type the ID is in and incorporating the TYPE into
 *              the ID which is returned to the user.
 *
 *              IDs are marked as "future" if the realize_cb and discard_cb
 *              parameters are non-NULL.
 *
 * Return:      Success:    New object ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5I__register(H5I_type_t type, const void *object, bool app_ref, H5I_future_realize_func_t realize_cb,
              H5I_future_discard_func_t discard_cb)
{
    H5I_type_info_t *type_info = NULL;            /* Pointer to the type */
    H5I_id_info_t   *info      = NULL;            /* Pointer to the new ID information */
    hid_t            new_id    = H5I_INVALID_HID; /* New ID */
    hid_t            ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, H5I_INVALID_HID, "invalid type number");
    type_info = H5I_type_info_array_g[type];
    if ((NULL == type_info) || (type_info->init_count <= 0))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, H5I_INVALID_HID, "invalid type");
    if (NULL == (info = H5FL_CALLOC(H5I_id_info_t)))
        HGOTO_ERROR(H5E_ID, H5E_NOSPACE, H5I_INVALID_HID, "memory allocation failed");

    /* Create the struct & its ID */
    new_id           = H5I_MAKE(type, type_info->nextid);
    info->id         = new_id;
    info->count      = 1; /* initial reference count */
    info->app_count  = !!app_ref;
    info->object     = object;
    info->is_future  = (NULL != realize_cb);
    info->realize_cb = realize_cb;
    info->discard_cb = discard_cb;
    info->marked     = false;

    /* Insert into the type */
    HASH_ADD(hh, type_info->hash_table, id, sizeof(hid_t), info);
    type_info->id_count++;
    type_info->nextid++;

    /* Sanity check for the 'nextid' getting too large and wrapping around */
    assert(type_info->nextid <= ID_MASK);

    /* Set the most recent ID to this object */
    type_info->last_id_info = info;

    /* Set return value */
    ret_value = new_id;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__register() */

/*-------------------------------------------------------------------------
 * Function:    H5I_register
 *
 * Purpose:     Library-private wrapper for H5I__register.
 *
 * Return:      Success:    New object ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5I_register(H5I_type_t type, const void *object, bool app_ref)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    /* Sanity checks */
    assert(type >= H5I_FILE && type < H5I_NTYPES);
    assert(object);

    /* Retrieve ID for object */
    if (H5I_INVALID_HID == (ret_value = H5I__register(type, object, app_ref, NULL, NULL)))
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_register() */

/*-------------------------------------------------------------------------
 * Function:    H5I_register_using_existing_id
 *
 * Purpose:     Registers an OBJECT in a TYPE with the supplied ID for it.
 *              This routine will check to ensure the supplied ID is not already
 *              in use, and ensure that it is a valid ID for the given type,
 *              but will NOT check to ensure the OBJECT is not already
 *              registered (thus, it is possible to register one object under
 *              multiple IDs).
 *
 * NOTE:        Intended for use in refresh calls, where we have to close
 *              and re-open the underlying data, then hook the object back
 *              up to the original ID.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5I_register_using_existing_id(H5I_type_t type, void *object, bool app_ref, hid_t existing_id)
{
    H5I_type_info_t *type_info = NULL;    /* Pointer to the type */
    H5I_id_info_t   *info      = NULL;    /* Pointer to the new ID information */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(object);

    /* Make sure ID is not already in use */
    if (NULL != (info = H5I__find_id(existing_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADRANGE, FAIL, "ID already in use");

    /* Make sure type number is valid */
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid type number");

    /* Get type pointer from list of types */
    type_info = H5I_type_info_array_g[type];

    if (NULL == type_info || type_info->init_count <= 0)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "invalid type");

    /* Make sure requested ID belongs to object's type */
    if (H5I_TYPE(existing_id) != type)
        HGOTO_ERROR(H5E_ID, H5E_BADRANGE, FAIL, "invalid type for provided ID");

    /* Allocate new structure to house this ID */
    if (NULL == (info = H5FL_CALLOC(H5I_id_info_t)))
        HGOTO_ERROR(H5E_ID, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Create the struct & insert requested ID */
    info->id        = existing_id;
    info->count     = 1; /* initial reference count*/
    info->app_count = !!app_ref;
    info->object    = object;
    /* This API call is only used by the native VOL connector, which is
     * not asynchronous.
     */
    info->is_future  = false;
    info->realize_cb = NULL;
    info->discard_cb = NULL;
    info->marked     = false;

    /* Insert into the type */
    HASH_ADD(hh, type_info->hash_table, id, sizeof(hid_t), info);
    type_info->id_count++;

    /* Set the most recent ID to this object */
    type_info->last_id_info = info;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_register_using_existing_id() */

/*-------------------------------------------------------------------------
 * Function:    H5I_subst
 *
 * Purpose:     Substitute a new object pointer for the specified ID.
 *
 * Return:      Success:    Non-NULL previous object pointer associated
 *                          with the specified ID.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5I_subst(hid_t id, const void *new_object)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the ID's info */
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* General lookup of the ID */
    if (NULL == (info = H5I__find_id(id)))
        HGOTO_ERROR(H5E_ID, H5E_NOTFOUND, NULL, "can't get ID ref count");

    /* Get the old object pointer to return */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    ret_value = (void *)info->object;
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    /* Set the new object pointer for the ID */
    info->object = new_object;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_subst() */

/*-------------------------------------------------------------------------
 * Function:    H5I_object
 *
 * Purpose:     Find an object pointer for the specified ID.
 *
 * Return:      Success:    Non-NULL object pointer associated with the
 *                          specified ID
 *
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5I_object(hid_t id)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the ID info */
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* General lookup of the ID */
    if (NULL != (info = H5I__find_id(id))) {
        /* Get the object pointer to return */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        ret_value = (void *)info->object;
        H5_GCC_CLANG_DIAG_ON("cast-qual")
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_object() */

/*-------------------------------------------------------------------------
 * Function:    H5I_object_verify
 *
 * Purpose:     Find an object pointer for the specified ID, verifying that
 *              its in a particular type.
 *
 * Return:      Success:    Non-NULL object pointer associated with the
 *                          specified ID.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5I_object_verify(hid_t id, H5I_type_t type)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the ID info */
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(type >= 1 && (int)type < H5I_next_type_g);

    /* Verify that the type of the ID is correct & lookup the ID */
    if (type == H5I_TYPE(id) && NULL != (info = H5I__find_id(id))) {
        /* Get the object pointer to return */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        ret_value = (void *)info->object;
        H5_GCC_CLANG_DIAG_ON("cast-qual")
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5I_object_verify() */

/*-------------------------------------------------------------------------
 * Function:    H5I_get_type
 *
 * Purpose:     Given an object ID return the type to which it
 *              belongs.  The ID need not be the ID of an object which
 *              currently exists because the type number is encoded
 *              in the object ID.
 *
 * Return:      Success:    A positive integer (corresponding to an H5I_type_t
 *                          enum value for library ID types, but not for user
 *                          ID types).
 *              Failure:    H5I_BADID
 *
 *-------------------------------------------------------------------------
 */
H5I_type_t
H5I_get_type(hid_t id)
{
    H5I_type_t ret_value = H5I_BADID; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    if (id > 0)
        ret_value = H5I_TYPE(id);

    assert(ret_value >= H5I_BADID && (int)ret_value < H5I_next_type_g);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_get_type() */

/*-------------------------------------------------------------------------
 * Function:    H5I_is_file_object
 *
 * Purpose:     Convenience function to determine if an ID represents
 *              a file object.
 *
 *              In H5O calls, you can't use object_verify to ensure
 *              the ID was of the correct class since there's no
 *              H5I_OBJECT ID class.
 *
 * Return:      Success:    true/false
 *              Failure:    FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5I_is_file_object(hid_t id)
{
    H5I_type_t type      = H5I_get_type(id);
    htri_t     ret_value = FAIL;

    FUNC_ENTER_NOAPI(FAIL)

    /* Fail if the ID type is out of range */
    if (type < 1 || type >= H5I_NTYPES)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "ID type out of range");

    /* Return true if the ID is a file object (dataset, group, map, or committed
     * datatype), false otherwise.
     */
    if (H5I_DATASET == type || H5I_GROUP == type || H5I_MAP == type)
        ret_value = true;
    else if (H5I_DATATYPE == type) {

        H5T_t *dt = NULL;

        if (NULL == (dt = (H5T_t *)H5I_object(id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unable to get underlying datatype struct");

        ret_value = H5T_is_named(dt);
    }
    else
        ret_value = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5I_is_file_object() */

/*-------------------------------------------------------------------------
 * Function:    H5I__remove_verify
 *
 * Purpose:     Removes the specified ID from its type, first checking that
 *              the ID's type is the same as the ID type supplied as an argument
 *
 * Return:      Success:    A pointer to the object that was removed, the
 *                          same pointer which would have been found by
 *                          calling H5I_object().
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5I__remove_verify(hid_t id, H5I_type_t type)
{
    void *ret_value = NULL; /*return value            */

    FUNC_ENTER_PACKAGE_NOERR

    /* Argument checking will be performed by H5I_remove() */

    /* Verify that the type of the ID is correct */
    if (type == H5I_TYPE(id))
        ret_value = H5I_remove(id);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__remove_verify() */

/*-------------------------------------------------------------------------
 * Function:    H5I__remove_common
 *
 * Purpose:     Common code to remove a specified ID from its type.
 *
 * Return:      Success:    A pointer to the object that was removed, the
 *                          same pointer which would have been found by
 *                          calling H5I_object().
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5I__remove_common(H5I_type_info_t *type_info, hid_t id)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the current ID */
    void          *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(type_info);

    /* Delete or mark the node */
    HASH_FIND(hh, type_info->hash_table, &id, sizeof(hid_t), info);
    if (info) {
        assert(!info->marked);
        if (!H5I_marking_s)
            HASH_DELETE(hh, type_info->hash_table, info);
        else
            info->marked = true;
    }
    else
        HGOTO_ERROR(H5E_ID, H5E_CANTDELETE, NULL, "can't remove ID node from hash table");

    /* Check if this ID was the last one accessed */
    if (type_info->last_id_info == info)
        type_info->last_id_info = NULL;

    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    ret_value = (void *)info->object;
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    if (!H5I_marking_s)
        info = H5FL_FREE(H5I_id_info_t, info);

    /* Decrement the number of IDs in the type */
    (type_info->id_count)--;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__remove_common() */

/*-------------------------------------------------------------------------
 * Function:    H5I_remove
 *
 * Purpose:     Removes the specified ID from its type.
 *
 * Return:      Success:    A pointer to the object that was removed, the
 *                          same pointer which would have been found by
 *                          calling H5I_object().
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5I_remove(hid_t id)
{
    H5I_type_info_t *type_info = NULL;      /* Pointer to the ID type */
    H5I_type_t       type      = H5I_BADID; /* ID's type */
    void            *ret_value = NULL;      /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Check arguments */
    type = H5I_TYPE(id);
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "invalid type number");
    type_info = H5I_type_info_array_g[type];
    if (type_info == NULL || type_info->init_count <= 0)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, NULL, "invalid type");

    /* Remove the node from the type */
    if (NULL == (ret_value = H5I__remove_common(type_info, id)))
        HGOTO_ERROR(H5E_ID, H5E_CANTDELETE, NULL, "can't remove ID node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5I__dec_ref
 *
 * Purpose:     This will fail if the type is not a reference counted type.
 *              The ID type's 'free' function will be called for the ID
 *              if the reference count for the ID reaches 0 and a free
 *              function has been defined at type creation time.
 *
 * Note:        Allows for asynchronous 'close' operation on object, with
 *              request != H5_REQUEST_NULL.
 *
 * Return:      Success:    New reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5I__dec_ref(hid_t id, void **request)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the ID */
    int            ret_value = 0;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(id >= 0);

    /* General lookup of the ID */
    if (NULL == (info = H5I__find_id(id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "can't locate ID");

    /* If this is the last reference to the object then invoke the type's
     * free method on the object. If the free method is undefined or
     * successful then remove the object from the type; otherwise leave
     * the object in the type without decrementing the reference
     * count. If the reference count is more than one then decrement the
     * reference count without calling the free method.
     *
     * Beware: the free method may call other H5I functions.
     *
     * If an object is closing, we can remove the ID even though the free
     * method might fail.  This can happen when a mandatory filter fails to
     * write when a dataset is closed and the chunk cache is flushed to the
     * file.  We have to close the dataset anyway. (SLU - 2010/9/7)
     */
    if (1 == info->count) {
        H5I_type_info_t *type_info; /*ptr to the type    */

        /* Get the ID's type */
        type_info = H5I_type_info_array_g[H5I_TYPE(id)];

        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        if (!type_info->cls->free_func || (type_info->cls->free_func)((void *)info->object, request) >= 0) {
            /* Remove the node from the type */
            if (NULL == H5I__remove_common(type_info, id))
                HGOTO_ERROR(H5E_ID, H5E_CANTDELETE, (-1), "can't remove ID node");
            ret_value = 0;
        } /* end if */
        else
            ret_value = -1;
        H5_GCC_CLANG_DIAG_ON("cast-qual")
    } /* end if */
    else {
        --(info->count);
        ret_value = (int)info->count;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__dec_ref */

/*-------------------------------------------------------------------------
 * Function:    H5I_dec_ref
 *
 * Purpose:     Decrements the number of references outstanding for an ID.
 *
 * Return:      Success:    New reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_dec_ref(hid_t id)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Sanity check */
    assert(id >= 0);

    /* Synchronously decrement refcount on ID */
    if ((ret_value = H5I__dec_ref(id, H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't decrement ID ref count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_dec_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I__dec_app_ref
 *
 * Purpose:     Wrapper for case of modifying the application ref.
 *              count for an ID as well as normal reference count.
 *
 * Note:        Allows for asynchronous 'close' operation on object, with
 *              request != H5_REQUEST_NULL.
 *
 * Return:      Success:    New app. reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5I__dec_app_ref(hid_t id, void **request)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(id >= 0);

    /* Call regular decrement reference count routine */
    if ((ret_value = H5I__dec_ref(id, request)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't decrement ID ref count");

    /* Check if the ID still exists */
    if (ret_value > 0) {
        H5I_id_info_t *info = NULL; /* Pointer to the ID info */

        /* General lookup of the ID */
        if (NULL == (info = H5I__find_id(id)))
            HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "can't locate ID");

        /* Adjust app_ref */
        --(info->app_count);
        assert(info->count >= info->app_count);

        /* Set return value */
        ret_value = (int)info->app_count;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__dec_app_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I_dec_app_ref
 *
 * Purpose:     Wrapper for case of modifying the application ref. count for
 *              an ID as well as normal reference count.
 *
 * Return:      Success:    New app. reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_dec_app_ref(hid_t id)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Sanity check */
    assert(id >= 0);

    /* Synchronously decrement refcount on ID */
    if ((ret_value = H5I__dec_app_ref(id, H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't decrement ID ref count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_dec_app_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I_dec_app_ref_async
 *
 * Purpose:     Asynchronous wrapper for case of modifying the application ref.
 *              count for an ID as well as normal reference count.
 *
 * Note:        Allows for asynchronous 'close' operation on object, with
 *              token != H5_REQUEST_NULL.
 *
 * Return:      Success:    New app. reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_dec_app_ref_async(hid_t id, void **token)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Sanity check */
    assert(id >= 0);

    /* [Possibly] asynchronously decrement refcount on ID */
    if ((ret_value = H5I__dec_app_ref(id, token)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't asynchronously decrement ID ref count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_dec_app_ref_async() */

/*-------------------------------------------------------------------------
 * Function:    H5I__dec_app_ref_always_close
 *
 * Purpose:     Wrapper for case of always closing the ID, even when the free
 *              routine fails
 *
 * Note:        Allows for asynchronous 'close' operation on object, with
 *              request != H5_REQUEST_NULL.
 *
 * Return:      Success:    New app. reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5I__dec_app_ref_always_close(hid_t id, void **request)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(id >= 0);

    /* Call application decrement reference count routine */
    ret_value = H5I__dec_app_ref(id, request);

    /* Check for failure */
    if (ret_value < 0) {
        /*
         * If an object is closing, we can remove the ID even though the free
         * method might fail.  This can happen when a mandatory filter fails to
         * write when a dataset is closed and the chunk cache is flushed to the
         * file.  We have to close the dataset anyway. (SLU - 2010/9/7)
         */
        H5I_remove(id);

        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't decrement ID ref count");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__dec_app_ref_always_close() */

/*-------------------------------------------------------------------------
 * Function:    H5I_dec_app_ref_always_close
 *
 * Purpose:     Wrapper for case of always closing the ID, even when the free
 *              routine fails.
 *
 * Return:      Success:    New app. reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_dec_app_ref_always_close(hid_t id)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Sanity check */
    assert(id >= 0);

    /* Synchronously decrement refcount on ID */
    if ((ret_value = H5I__dec_app_ref_always_close(id, H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't decrement ID ref count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_dec_app_ref_always_close() */

/*-------------------------------------------------------------------------
 * Function:    H5I_dec_app_ref_always_close_async
 *
 * Purpose:     Asynchronous wrapper for case of always closing the ID, even
 *              when the free routine fails
 *
 * Note:        Allows for asynchronous 'close' operation on object, with
 *              token != H5_REQUEST_NULL.
 *
 * Return:      Success:    New app. reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_dec_app_ref_always_close_async(hid_t id, void **token)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Sanity check */
    assert(id >= 0);

    /* [Possibly] asynchronously decrement refcount on ID */
    if ((ret_value = H5I__dec_app_ref_always_close(id, token)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't asynchronously decrement ID ref count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_dec_app_ref_always_close_async() */

/*-------------------------------------------------------------------------
 * Function:    H5I_inc_ref
 *
 * Purpose:     Increment the reference count for an object.
 *
 * Return:      Success:    The new reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_inc_ref(hid_t id, bool app_ref)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the ID info */
    int            ret_value = 0;    /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Sanity check */
    assert(id >= 0);

    /* General lookup of the ID */
    if (NULL == (info = H5I__find_id(id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "can't locate ID");

    /* Adjust reference counts */
    ++(info->count);
    if (app_ref)
        ++(info->app_count);

    /* Set return value */
    ret_value = (int)(app_ref ? info->app_count : info->count);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_inc_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I_get_ref
 *
 * Purpose:     Retrieve the reference count for an object.
 *
 * Return:      Success:    The reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_get_ref(hid_t id, bool app_ref)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the ID */
    int            ret_value = 0;    /* Return value */

    FUNC_ENTER_NOAPI((-1))

    /* Sanity check */
    assert(id >= 0);

    /* General lookup of the ID */
    if (NULL == (info = H5I__find_id(id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "can't locate ID");

    /* Set return value */
    ret_value = (int)(app_ref ? info->app_count : info->count);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_get_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I__inc_type_ref
 *
 * Purpose:     Increment the reference count for an ID type.
 *
 * Return:      Success:    The new reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I__inc_type_ref(H5I_type_t type)
{
    H5I_type_info_t *type_info = NULL; /* Pointer to the type */
    int              ret_value = -1;   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(type > 0 && (int)type < H5I_next_type_g);

    /* Check arguments */
    type_info = H5I_type_info_array_g[type];
    if (NULL == type_info)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, (-1), "invalid type");

    /* Set return value */
    ret_value = (int)(++(type_info->init_count));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__inc_type_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I_dec_type_ref
 *
 * Purpose:     Decrements the reference count on an entire type of IDs.
 *              If the type reference count becomes zero then the type is
 *              destroyed along with all IDs in that type regardless of
 *              their reference counts. Destroying IDs involves calling
 *              the free-func for each ID's object and then adding the ID
 *              struct to the ID free list.
 *              Returns the number of references to the type on success; a
 *              return value of 0 means that the type will have to be
 *              re-initialized before it can be used again (and should probably
 *              be set to H5I_UNINIT).
 *
 * Return:      Success:    Number of references to type
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I_dec_type_ref(H5I_type_t type)
{
    H5I_type_info_t *type_info = NULL; /* Pointer to the ID type */
    herr_t           ret_value = 0;    /* Return value */

    FUNC_ENTER_NOAPI((-1))

    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, (-1), "invalid type number");

    type_info = H5I_type_info_array_g[type];
    if (type_info == NULL || type_info->init_count <= 0)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, (-1), "invalid type");

    /* Decrement the number of users of the ID type.  If this is the
     * last user of the type then release all IDs from the type and
     * free all memory it used.  The free function is invoked for each ID
     * being freed.
     */
    if (1 == type_info->init_count) {
        H5I__destroy_type(type);
        ret_value = 0;
    }
    else {
        --(type_info->init_count);
        ret_value = (herr_t)type_info->init_count;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_dec_type_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I__get_type_ref
 *
 * Purpose:     Retrieve the reference count for an ID type.
 *
 * Return:      Success:    The reference count
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5I__get_type_ref(H5I_type_t type)
{
    H5I_type_info_t *type_info = NULL; /* Pointer to the type  */
    int              ret_value = -1;   /* Return value         */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(type >= 0);

    /* Check arguments */
    type_info = H5I_type_info_array_g[type];
    if (!type_info)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, (-1), "invalid type");

    /* Set return value */
    ret_value = (int)type_info->init_count;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__get_type_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5I__iterate_cb
 *
 * Purpose:     Callback routine for H5I_iterate, invokes "user" callback
 *              function, and then sets return value, based on the result of
 *              that callback.
 *
 * Return:      Success:    H5_ITER_CONT (0) or H5_ITER_STOP (1)
 *              Failure:    H5_ITER_ERROR (-1)
 *
 *-------------------------------------------------------------------------
 */
static int
H5I__iterate_cb(void *_item, void H5_ATTR_UNUSED *_key, void *_udata)
{
    H5I_id_info_t    *info      = (H5I_id_info_t *)_item;     /* Pointer to the ID info */
    H5I_iterate_ud_t *udata     = (H5I_iterate_ud_t *)_udata; /* User data for callback */
    int               ret_value = H5_ITER_CONT;               /* Callback return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Only invoke the callback function if this ID is visible externally and
     * its reference count is positive.
     */
    if ((!udata->app_ref) || (info->app_count > 0)) {
        H5I_type_t type = udata->obj_type;
        void      *object;
        herr_t     cb_ret_val;

        /* The stored object pointer might be an H5VL_object_t, in which
         * case we'll need to get the wrapped object struct (H5F_t *, etc.).
         */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        object = H5I__unwrap((void *)info->object, type);
        H5_GCC_CLANG_DIAG_ON("cast-qual")

        /* Invoke callback function */
        cb_ret_val = (*udata->user_func)((void *)object, info->id, udata->user_udata);

        /* Set the return value based on the callback's return value */
        if (cb_ret_val > 0)
            ret_value = H5_ITER_STOP; /* terminate iteration early */
        else if (cb_ret_val < 0)
            ret_value = H5_ITER_ERROR; /* indicate failure (which terminates iteration) */
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__iterate_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5I_iterate
 *
 * Purpose:     Apply function FUNC to each member of type TYPE (with
 *              non-zero application reference count if app_ref is true).
 *              Stop if FUNC returns a non zero value (i.e. anything
 *              other than H5_ITER_CONT).
 *
 *              If FUNC returns a positive value (i.e. H5_ITER_STOP),
 *              return SUCCEED.
 *
 *              If FUNC returns a negative value (i.e. H5_ITER_ERROR),
 *              return FAIL.
 *
 *              The FUNC should take a pointer to the object and the
 *              udata as arguments and return non-zero to terminate
 *              siteration, and zero to continue.
 *
 * Limitation:  Currently there is no way to start the iteration from
 *              where a previous iteration left off.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5I_iterate(H5I_type_t type, H5I_search_func_t func, void *udata, bool app_ref)
{
    H5I_type_info_t *type_info = NULL;    /* Pointer to the type */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid type number");
    type_info = H5I_type_info_array_g[type];

    /* Only iterate through ID list if it is initialized and there are IDs in type */
    if (type_info && type_info->init_count > 0 && type_info->id_count > 0) {
        H5I_iterate_ud_t iter_udata; /* User data for iteration callback */
        H5I_id_info_t   *item = NULL;
        H5I_id_info_t   *tmp  = NULL;

        /* Set up iterator user data */
        iter_udata.user_func  = func;
        iter_udata.user_udata = udata;
        iter_udata.app_ref    = app_ref;
        iter_udata.obj_type   = type;

        /* Iterate over IDs */
        HASH_ITER(hh, type_info->hash_table, item, tmp)
        {
            if (!item->marked) {
                int ret = H5I__iterate_cb((void *)item, NULL, (void *)&iter_udata);
                if (H5_ITER_ERROR == ret)
                    HGOTO_ERROR(H5E_ID, H5E_BADITER, FAIL, "iteration failed");
                if (H5_ITER_STOP == ret)
                    break;
            }
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5I__find_id
 *
 * Purpose:     Given an object ID find the info struct that describes the
 *              object.
 *
 * Return:      Success:    A pointer to the object's info struct.
 *
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5I_id_info_t *
H5I__find_id(hid_t id)
{
    H5I_type_t       type;             /* ID's type */
    H5I_type_info_t *type_info = NULL; /* Pointer to the type */
    H5I_id_info_t   *id_info   = NULL; /* ID's info */
    H5I_id_info_t   *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    type = H5I_TYPE(id);
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_DONE(NULL);
    type_info = H5I_type_info_array_g[type];
    if (!type_info || type_info->init_count <= 0)
        HGOTO_DONE(NULL);

    /* Check for same ID as we have looked up last time */
    if (type_info->last_id_info && type_info->last_id_info->id == id)
        id_info = type_info->last_id_info;
    else {
        HASH_FIND(hh, type_info->hash_table, &id, sizeof(hid_t), id_info);

        /* Remember this ID */
        type_info->last_id_info = id_info;
    }

    /* Check if this is a future ID */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    if (id_info && id_info->is_future) {
        hid_t actual_id = H5I_INVALID_HID; /* ID for actual object */
        void *future_object;               /* Pointer to the future object */
        void *actual_object;               /* Pointer to the actual object */

        /* Invoke the realize callback, to get the actual object */
        if ((id_info->realize_cb)((void *)id_info->object, &actual_id) < 0)
            HGOTO_DONE(NULL);

        /* Verify that we received a valid ID, of the same type */
        if (H5I_INVALID_HID == actual_id)
            HGOTO_DONE(NULL);
        if (H5I_TYPE(id) != H5I_TYPE(actual_id))
            HGOTO_DONE(NULL);

        /* Swap the actual object in for the future object */
        future_object = (void *)id_info->object;
        actual_object = H5I__remove_common(type_info, actual_id);
        assert(actual_object);
        id_info->object = actual_object;

        /* Discard the future object */
        if ((id_info->discard_cb)(future_object) < 0)
            HGOTO_DONE(NULL);
        future_object = NULL;

        /* Change the ID from 'future' to 'actual' */
        id_info->is_future  = false;
        id_info->realize_cb = NULL;
        id_info->discard_cb = NULL;
    }
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    /* Set return value */
    ret_value = id_info;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__find_id() */

/*-------------------------------------------------------------------------
 * Function:    H5I__find_id_cb
 *
 * Purpose:     Callback for searching for an ID with a specific pointer
 *
 * Return:      Success:    H5_ITER_CONT (0) or H5_ITER_STOP (1)
 *              Failure:    H5_ITER_ERROR (-1)
 *
 *-------------------------------------------------------------------------
 */
static int
H5I__find_id_cb(void *_item, void H5_ATTR_UNUSED *_key, void *_udata)
{
    H5I_id_info_t   *info      = (H5I_id_info_t *)_item;    /* Pointer to the ID info */
    H5I_get_id_ud_t *udata     = (H5I_get_id_ud_t *)_udata; /* Pointer to user data */
    H5I_type_t       type      = udata->obj_type;
    const void      *object    = NULL;
    int              ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(info);
    assert(udata);

    /* Get a pointer to the VOL connector's data */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    object = H5I__unwrap((void *)info->object, type);
    H5_GCC_CLANG_DIAG_ON("cast-qual")

    /* Check for a match */
    if (object == udata->object) {
        udata->ret_id = info->id;
        ret_value     = H5_ITER_STOP;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__find_id_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5I_find_id
 *
 * Purpose:     Return the ID of an object by searching through the ID list
 *              for the type.
 *
 * Return:      SUCCEED/FAIL
 *              (id will be set to H5I_INVALID_HID on errors or not found)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5I_find_id(const void *object, H5I_type_t type, hid_t *id)
{
    H5I_type_info_t *type_info = NULL;    /* Pointer to the type */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(id);

    *id = H5I_INVALID_HID;

    type_info = H5I_type_info_array_g[type];
    if (!type_info || type_info->init_count <= 0)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "invalid type");

    /* Only iterate through ID list if it is initialized and there are IDs in type */
    if (type_info->init_count > 0 && type_info->id_count > 0) {
        H5I_get_id_ud_t udata; /* User data */
        H5I_id_info_t  *item = NULL;
        H5I_id_info_t  *tmp  = NULL;

        /* Set up iterator user data */
        udata.object   = object;
        udata.obj_type = type;
        udata.ret_id   = H5I_INVALID_HID;

        /* Iterate over IDs for the ID type */
        HASH_ITER(hh, type_info->hash_table, item, tmp)
        {
            int ret = H5I__find_id_cb((void *)item, NULL, (void *)&udata);
            if (H5_ITER_ERROR == ret)
                HGOTO_ERROR(H5E_ID, H5E_BADITER, FAIL, "iteration failed");
            if (H5_ITER_STOP == ret)
                break;
        }

        *id = udata.ret_id;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I_find_id() */
