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
 * H5I.c - Public routines for handling IDs
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
#include "H5Fprivate.h"  /* Files                                    */
#include "H5Ipkg.h"      /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Pprivate.h"  /* Property lists                           */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

typedef struct {
    H5I_search_func_t app_cb;  /* Application's callback routine */
    void             *app_key; /* Application's "key" (user data) */
    void             *ret_obj; /* Object to return */
} H5I_search_ud_t;

typedef struct {
    H5I_iterate_func_t op;      /* Application's callback routine */
    void              *op_data; /* Application's user data */
} H5I_iterate_pub_ud_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static int H5I__search_cb(void *obj, hid_t id, void *_udata);
static int H5I__iterate_pub_cb(void *obj, hid_t id, void *udata);

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
 * Function:    H5Iregister_type
 *
 * Purpose:     Public interface to H5I_register_type.  Creates a new type
 *              of ID's to give out.  A specific number (RESERVED) of type
 *              entries may be reserved to enable "constant" values to be handed
 *              out which are valid IDs in the type, but which do not map to any
 *              data structures and are not allocated dynamically later. HASH_SIZE is
 *              the minimum hash table size to use for the type. FREE_FUNC is
 *              called with an object pointer when the object is removed from
 *              the type.
 *
 * Return:      Success:    Type ID of the new type
 *              Failure:    H5I_BADID
 *
 *-------------------------------------------------------------------------
 */
H5I_type_t
H5Iregister_type(size_t H5_ATTR_DEBUG_API_USED hash_size, unsigned reserved, H5I_free_t free_func)
{
    H5I_class_t *cls       = NULL;      /* New ID class */
    H5I_type_t   new_type  = H5I_BADID; /* New ID type value */
    H5I_type_t   ret_value = H5I_BADID; /* Return value */

    FUNC_ENTER_API(H5I_BADID)
    H5TRACE3("It", "zIuIf", hash_size, reserved, free_func);

    /* Generate a new H5I_type_t value */

    /* Increment the number of types */
    if (H5I_next_type_g < H5I_MAX_NUM_TYPES) {
        new_type = (H5I_type_t)H5I_next_type_g;
        H5I_next_type_g++;
    }
    else {
        bool done; /* Indicate that search was successful */
        int  i;

        /* Look for a free type to give out */
        done = false;
        for (i = H5I_NTYPES; i < H5I_MAX_NUM_TYPES && done == false; i++) {
            if (NULL == H5I_type_info_array_g[i]) {
                /* Found a free type ID */
                new_type = (H5I_type_t)i;
                done     = true;
            }
        }

        /* Verify that we found a type to give out */
        if (done == false)
            HGOTO_ERROR(H5E_ID, H5E_NOSPACE, H5I_BADID, "Maximum number of ID types exceeded");
    }

    /* Allocate new ID class */
    if (NULL == (cls = H5MM_calloc(sizeof(H5I_class_t))))
        HGOTO_ERROR(H5E_ID, H5E_CANTALLOC, H5I_BADID, "ID class allocation failed");

    /* Initialize class fields */
    cls->type      = new_type;
    cls->flags     = H5I_CLASS_IS_APPLICATION;
    cls->reserved  = reserved;
    cls->free_func = free_func;

    /* Register the new ID class */
    if (H5I_register_type(cls) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTINIT, H5I_BADID, "can't initialize ID class");

    /* Set return value */
    ret_value = new_type;

done:
    /* Clean up on error */
    if (ret_value < 0)
        if (cls)
            cls = H5MM_xfree(cls);

    FUNC_LEAVE_API(ret_value)
} /* end H5Iregister_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Itype_exists
 *
 * Purpose:     Query function to inform the user if a given type is
 *              currently registered with the library.
 *
 * Return:      true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Itype_exists(H5I_type_t type)
{
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "It", type);

    /* Validate parameter */
    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "cannot call public function on library type");
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid type number");

    if (NULL == H5I_type_info_array_g[type])
        ret_value = false;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Itype_exists() */

/*-------------------------------------------------------------------------
 * Function:    H5Inmembers
 *
 * Purpose:     Returns the number of members in a type.  Public interface to
 *              H5I_nmembers.  The public interface throws an error if the
 *              supplied type does not exist.  This is different than the
 *              private interface, which will just return 0.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Inmembers(H5I_type_t type, hsize_t *num_members)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "It*h", type, num_members);

    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "cannot call public function on library type");

    /* Validate parameters.  This needs to be done here, instead of letting
     * the private interface handle it, because the public interface throws
     * an error when the supplied type does not exist.
     */
    if (type <= H5I_BADID || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid type number");
    if (NULL == H5I_type_info_array_g[type])
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "supplied type does not exist");

    if (num_members) {
        int64_t members;

        if ((members = H5I_nmembers(type)) < 0)
            HGOTO_ERROR(H5E_ID, H5E_CANTCOUNT, FAIL, "can't compute number of members");

        H5_CHECKED_ASSIGN(*num_members, hsize_t, members, int64_t);
    }

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Inmembers() */

/*-------------------------------------------------------------------------
 * Function:    H5Iclear_type
 *
 * Purpose:     Removes all objects from the type, calling the free
 *              function for each object regardless of the reference count.
 *              Public interface to H5I_clear_type.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Iclear_type(H5I_type_t type, hbool_t force)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "Itb", type, force);

    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "cannot call public function on library type");

    ret_value = H5I_clear_type(type, force, true);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iclear_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Idestroy_type
 *
 * Purpose:     Destroys a type along with all IDs in that type
 *              regardless of their reference counts. Destroying IDs
 *              involves calling the free-func for each ID's object and
 *              then adding the ID struct to the ID free list.  Public
 *              interface to H5I__destroy_type.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Idestroy_type(H5I_type_t type)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "It", type);

    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, FAIL, "cannot call public function on library type");

    ret_value = H5I__destroy_type(type);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Idestroy_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Iregister
 *
 * Purpose:     Register an object.
 *
 * Return:      Success:    New object ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Iregister(H5I_type_t type, const void *object)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "It*x", type, object);

    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, H5I_INVALID_HID, "cannot call public function on library type");

    /* Register the object */
    if ((ret_value = H5I__register(type, object, true, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iregister() */

/*-------------------------------------------------------------------------
 * Function:    H5Iregister_future
 *
 * Purpose:     Register a "future" object.
 *
 * Return:      Success:    New future object ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Iregister_future(H5I_type_t type, const void *object, H5I_future_realize_func_t realize_cb,
                   H5I_future_discard_func_t discard_cb)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE4("i", "It*xIRID", type, object, realize_cb, discard_cb);

    /* Check arguments */
    if (NULL == realize_cb)
        HGOTO_ERROR(H5E_ID, H5E_BADVALUE, H5I_INVALID_HID, "NULL pointer for realize_cb not allowed");
    if (NULL == discard_cb)
        HGOTO_ERROR(H5E_ID, H5E_BADVALUE, H5I_INVALID_HID, "NULL pointer for realize_cb not allowed");

    /* Register the future object */
    if ((ret_value = H5I__register(type, object, true, realize_cb, discard_cb)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iregister_future() */

/*-------------------------------------------------------------------------
 * Function:    H5Iobject_verify
 *
 * Purpose:     Find an object pointer for the specified ID, verifying that
 *              its in a particular type.  Public interface to
 *              H5I_object_verify.
 *
 * Return:      Success:    Non-NULL object pointer associated with the
 *                          specified ID.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5Iobject_verify(hid_t id, H5I_type_t type)
{
    void *ret_value = NULL; /* Return value */

    FUNC_ENTER_API(NULL)
    H5TRACE2("*x", "iIt", id, type);

    /* Validate parameters */
    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, NULL, "cannot call public function on library type");
    if (type < 1 || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, NULL, "identifier has invalid type");

    ret_value = H5I_object_verify(id, type);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iobject_verify() */

/*-------------------------------------------------------------------------
 * Function:    H5Iget_type
 *
 * Purpose:     The public version of H5I_get_type(), obtains a type number
 *              when given an ID.  The ID need not be the ID of an
 *              object which currently exists because the type number is
 *              encoded as part of the ID.
 *
 * Return:      Success:    A positive integer (corresponding to an H5I_type_t
 *                          enum value for library ID types, but not for user
 *                          ID types).
 *              Failure:    H5I_BADID
 *
 *-------------------------------------------------------------------------
 */
H5I_type_t
H5Iget_type(hid_t id)
{
    H5I_type_t ret_value = H5I_BADID; /* Return value */

    FUNC_ENTER_API(H5I_BADID)
    H5TRACE1("It", "i", id);

    ret_value = H5I_get_type(id);

    if (ret_value <= H5I_BADID || (int)ret_value >= H5I_next_type_g || NULL == H5I_object(id))
        HGOTO_DONE(H5I_BADID);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iget_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Iremove_verify
 *
 * Purpose:     Removes the specified ID from its type, first checking that the
 *              type of the ID and the type type are the same.  Public interface to
 *              H5I__remove_verify.
 *
 * Return:      Success:    A pointer to the object that was removed, the
 *                          same pointer which would have been found by
 *                          calling H5I_object().
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5Iremove_verify(hid_t id, H5I_type_t type)
{
    void *ret_value = NULL; /* Return value */

    FUNC_ENTER_API(NULL)
    H5TRACE2("*x", "iIt", id, type);

    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, NULL, "cannot call public function on library type");

    /* Remove the id */
    ret_value = H5I__remove_verify(id, type);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iremove_verify() */

/*-------------------------------------------------------------------------
 * Function:    H5Idec_ref
 *
 * Purpose:     Decrements the number of references outstanding for an ID.
 *              If the reference count for an ID reaches zero, the object
 *              will be closed.
 *
 * Return:      Success:    New reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5Idec_ref(hid_t id)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("Is", "i", id);

    /* Check arguments */
    if (id < 0)
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "invalid ID");

    /* Do actual decrement operation */
    if ((ret_value = H5I_dec_app_ref(id)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTDEC, (-1), "can't decrement ID ref count");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Idec_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5Iinc_ref
 *
 * Purpose:     Increments the number of references outstanding for an ID.
 *
 * Return:      Success:    New reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5Iinc_ref(hid_t id)
{
    int ret_value = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("Is", "i", id);

    /* Check arguments */
    if (id < 0)
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "invalid ID");

    /* Do actual increment operation */
    if ((ret_value = H5I_inc_ref(id, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTINC, (-1), "can't increment ID ref count");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iinc_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5Iget_ref
 *
 * Purpose:     Retrieves the number of references outstanding for an ID.
 *
 * Return:      Success:    Reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5Iget_ref(hid_t id)
{
    int ret_value = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("Is", "i", id);

    /* Check arguments */
    if (id < 0)
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "invalid ID");

    /* Do actual retrieve operation */
    if ((ret_value = H5I_get_ref(id, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTGET, (-1), "can't get ID ref count");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iget_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5Iinc_type_ref
 *
 * Purpose:     Increments the number of references outstanding for an ID type.
 *
 * Return:      Success:    New reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5Iinc_type_ref(H5I_type_t type)
{
    int ret_value = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("Is", "It", type);

    /* Check arguments */
    if (type <= 0 || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "invalid ID type");
    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, (-1), "cannot call public function on library type");

    /* Do actual increment operation */
    if ((ret_value = H5I__inc_type_ref(type)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTINC, (-1), "can't increment ID type ref count");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iinc_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5Idec_type_ref
 *
 * Purpose:     Decrements the reference count on an entire type of IDs.
 *              If the type reference count becomes zero then the type is
 *              destroyed along with all IDs in that type regardless of
 *              their reference counts. Destroying IDs involves calling
 *              the free-func for each ID's object and then adding the ID
 *              struct to the ID free list.  Public interface to
 *              H5I_dec_type_ref.
 *              Returns the number of references to the type on success; a
 *              return value of 0 means that the type will have to be
 *              re-initialized before it can be used again (and should probably
 *              be set to H5I_UNINIT).
 *
 * NOTE:        Using an error type to also represent a count is semantially
 *              incorrect. We should consider fixing this in a future major
 *              release (DER).
 *
 * Return:      Success:    Number of references to type
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Idec_type_ref(H5I_type_t type)
{
    herr_t ret_value = 0; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("e", "It", type);

    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, (-1), "cannot call public function on library type");

    ret_value = H5I_dec_type_ref(type);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Idec_type_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5Iget_type_ref
 *
 * Purpose:     Retrieves the number of references outstanding for a type.
 *
 * Return:      Success:    Reference count
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5Iget_type_ref(H5I_type_t type)
{
    int ret_value = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("Is", "It", type);

    /* Check arguments */
    if (type <= 0 || (int)type >= H5I_next_type_g)
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "invalid ID type");
    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, (-1), "cannot call public function on library type");

    /* Do actual retrieve operation */
    if ((ret_value = H5I__get_type_ref(type)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTGET, (-1), "can't get ID type ref count");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iget_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5Iis_valid
 *
 * Purpose:     Check if the given id is valid.  An id is valid if it is in
 *              use and has an application reference count of at least 1.
 *
 * Return:      true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Iis_valid(hid_t id)
{
    H5I_id_info_t *info      = NULL; /* Pointer to the ID info */
    htri_t         ret_value = true; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "i", id);

    /* Find the ID */
    if (NULL == (info = H5I__find_id(id)))
        ret_value = false;
    else if (!info->app_count) /* Check if the found id is an internal id */
        ret_value = false;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iis_valid() */

/*-------------------------------------------------------------------------
 * Function:    H5I__search_cb
 *
 * Purpose:     Callback routine for H5Isearch, when it calls H5I_iterate.
 *              Calls "user" callback search function, and then sets return
 *              value, based on the result of that callback.
 *
 * Return:      Success:    H5_ITER_CONT (0) or H5_ITER_STOP (1)
 *              Failure:    H5_ITER_ERROR (-1)
 *
 *-------------------------------------------------------------------------
 */
static int
H5I__search_cb(void *obj, hid_t id, void *_udata)
{
    H5I_search_ud_t *udata = (H5I_search_ud_t *)_udata; /* User data for callback */
    herr_t           cb_ret_val;                        /* User callback return value */
    int              ret_value = H5_ITER_ERROR;         /* Callback return value */

    FUNC_ENTER_PACKAGE_NOERR

    cb_ret_val = (*udata->app_cb)(obj, id, udata->app_key);

    /* Set the return value based on the callback's return value */
    if (cb_ret_val > 0) {
        ret_value      = H5_ITER_STOP; /* terminate iteration early */
        udata->ret_obj = obj;          /* also set out parameter */
    }
    else if (cb_ret_val < 0)
        ret_value = H5_ITER_ERROR; /* indicate failure (which terminates iteration) */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__search_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5Isearch
 *
 * Purpose:     Apply function FUNC to each member of type TYPE and return a
 *              pointer to the first object for which FUNC returns non-zero.
 *              The FUNC should take a pointer to the object and the KEY as
 *              arguments and return non-zero to terminate the search (zero
 *              to continue).  Public interface to H5I_search.
 *
 * Limitation:  Currently there is no way to start searching from where a
 *              previous search left off.
 *
 * Return:      Success:    The first object in the type for which FUNC
 *                          returns non-zero. NULL if FUNC returned zero
 *                          for every object in the type.
 *
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5Isearch(H5I_type_t type, H5I_search_func_t func, void *key)
{
    H5I_search_ud_t udata;            /* Context for iteration */
    void           *ret_value = NULL; /* Return value */

    FUNC_ENTER_API(NULL)
    H5TRACE3("*x", "ItIS*x", type, func, key);

    /* Check arguments */
    if (H5I_IS_LIB_TYPE(type))
        HGOTO_ERROR(H5E_ID, H5E_BADGROUP, NULL, "cannot call public function on library type");

    /* Set up udata struct */
    udata.app_cb  = func;
    udata.app_key = key;
    udata.ret_obj = NULL;

    /* Note that H5I_iterate returns an error code.  We ignore it
     * here, as we can't do anything with it without revising the API.
     */
    (void)H5I_iterate(type, H5I__search_cb, &udata, true);

    /* Set return value */
    ret_value = udata.ret_obj;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Isearch() */

/*-------------------------------------------------------------------------
 * Function:    H5I__iterate_pub_cb
 *
 * Purpose:     Callback routine for H5Iiterate, when it calls
 *              H5I_iterate.  Calls "user" callback search function, and
 *              then sets return value, based on the result of that
 *              callback.
 *
 * Return:      Success:    H5_ITER_CONT (0) or H5_ITER_STOP (1)
 *              Failure:    H5_ITER_ERROR (-1)
 *
 *-------------------------------------------------------------------------
 */
static int
H5I__iterate_pub_cb(void H5_ATTR_UNUSED *obj, hid_t id, void *_udata)
{
    H5I_iterate_pub_ud_t *udata      = (H5I_iterate_pub_ud_t *)_udata; /* User data for callback */
    herr_t                cb_ret_val = FAIL;                           /* User callback return value */
    int                   ret_value  = H5_ITER_ERROR;                  /* Callback return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Invoke the callback */
    cb_ret_val = (*udata->op)(id, udata->op_data);

    /* Set the return value based on the callback's return value */
    if (cb_ret_val > 0)
        ret_value = H5_ITER_STOP; /* terminate iteration early */
    else if (cb_ret_val < 0)
        ret_value = H5_ITER_ERROR; /* indicate failure (which terminates iteration) */
    else
        ret_value = H5_ITER_CONT; /* continue iteration */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__iterate_pub_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5Iiterate
 *
 * Purpose:     Call the callback function op for each member of the id
 *              type type.  op takes as parameters the id and a
 *              passthrough of op_data, and returns an herr_t.  A positive
 *              return from op will cause the iteration to stop and
 *              H5Iiterate will return the value returned by op.  A
 *              negative return from op will cause the iteration to stop
 *              and H5Iiterate will return failure.  A zero return from op
 *              will allow iteration to continue, as long as there are
 *              other ids remaining in type.
 *
 * Limitation:  Currently there is no way to start searching from where a
 *              previous search left off.
 *
 * Return:      The last value returned by op
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Iiterate(H5I_type_t type, H5I_iterate_func_t op, void *op_data)
{
    H5I_iterate_pub_ud_t int_udata;        /* Internal user data */
    herr_t               ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ItII*x", type, op, op_data);

    /* Set up udata struct */
    int_udata.op      = op;
    int_udata.op_data = op_data;

    /* Note that H5I_iterate returns an error code.  We ignore it
     * here, as we can't do anything with it without revising the API.
     */
    if ((ret_value = H5I_iterate(type, H5I__iterate_pub_cb, &int_udata, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_BADITER, FAIL, "can't iterate over ids");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iiterate() */

/*-------------------------------------------------------------------------
 * Function:    H5Iget_file_id
 *
 * Purpose:     Obtains the file ID given an object ID.  The user has to
 *              close this ID.
 *
 * Return:      Success:    The file ID associated with the object
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Iget_file_id(hid_t obj_id)
{
    H5I_type_t type;                        /* ID type */
    hid_t      ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", obj_id);

    /* Get object type */
    type = H5I_TYPE(obj_id);

    /* Call internal function */
    if (H5I_FILE == type || H5I_DATATYPE == type || H5I_GROUP == type || H5I_DATASET == type ||
        H5I_ATTR == type) {
        H5VL_object_t *vol_obj; /* Object of obj_id */

        /* Get the VOL object */
        if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
            HGOTO_ERROR(H5E_ID, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

        /* Get the file ID */
        if ((ret_value = H5F_get_file_id(vol_obj, type, true)) < 0)
            HGOTO_ERROR(H5E_ID, H5E_CANTGET, H5I_INVALID_HID, "can't retrieve file ID");
    }
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, H5I_INVALID_HID, "not an ID of a file object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iget_file_id() */

/*-------------------------------------------------------------------------
 * Function:    H5Iget_name
 *
 * Purpose:     Gets a name of an object from its ID.
 *
 * Return:      Success:    The length of the name
 *
 *              Failure:    -1
 *
 * Notes:
 *  If 'name' is non-NULL then write up to 'size' bytes into that
 *  buffer and always return the length of the entry name.
 *  Otherwise 'size' is ignored and the function does not store the name,
 *  just returning the number of characters required to store the name.
 *  If an error occurs then the buffer pointed to by 'name' (NULL or non-NULL)
 *  is unchanged and the function returns a negative value.
 *  If a zero is returned for the name's length, then there is no name
 *  associated with the ID.
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Iget_name(hid_t id, char *name /*out*/, size_t size)
{
    H5VL_object_t         *vol_obj = NULL; /* Object stored in ID */
    H5VL_object_get_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;
    size_t                 obj_name_len = 0;  /* Length of object's name */
    ssize_t                ret_value    = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE3("Zs", "ixz", id, name, size);

    /* Get the object pointer */
    if (NULL == (vol_obj = H5VL_vol_object(id)))
        HGOTO_ERROR(H5E_ID, H5E_BADTYPE, (-1), "invalid identifier");

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_OBJECT_GET_NAME;
    vol_cb_args.args.get_name.buf_size = size;
    vol_cb_args.args.get_name.buf      = name;
    vol_cb_args.args.get_name.name_len = &obj_name_len;

    /* Retrieve object's name */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTGET, (-1), "can't retrieve object name");

    /* Set return value */
    ret_value = (ssize_t)obj_name_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Iget_name() */
