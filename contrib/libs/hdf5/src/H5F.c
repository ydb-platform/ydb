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

/****************/
/* Module Setup */
/****************/

#include "H5Fmodule.h" /* This source code file is part of the H5F module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5Fpkg.h"      /* File access                              */
#include "H5FLprivate.h" /* Free lists                               */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for traversal routine to get ID counts */
typedef struct {
    size_t   obj_count; /* Number of objects counted so far */
    unsigned types;     /* Types of objects to be counted */
} H5F_trav_obj_cnt_t;

/* User data for traversal routine to get ID lists */
typedef struct {
    size_t max_objs;  /* Maximum # of IDs to record */
    hid_t *oid_list;  /* Array of recorded IDs*/
    size_t obj_count; /* Number of objects counted so far */
} H5F_trav_obj_ids_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Callback for getting object counts in a file */
static int H5F__get_all_count_cb(void H5_ATTR_UNUSED *obj_ptr, hid_t H5_ATTR_UNUSED obj_id, void *key);

/* Callback for getting IDs for open objects in a file */
static int H5F__get_all_ids_cb(void H5_ATTR_UNUSED *obj_ptr, hid_t obj_id, void *key);

/* Helper routines for sync/async API calls */
static herr_t H5F__post_open_api_common(H5VL_object_t *vol_obj, void **token_ptr);
static hid_t  H5F__create_api_common(const char *filename, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                                     void **token_ptr);
static hid_t  H5F__open_api_common(const char *filename, unsigned flags, hid_t fapl_id, void **token_ptr);
static hid_t  H5F__reopen_api_common(hid_t file_id, void **token_ptr);
static herr_t H5F__flush_api_common(hid_t object_id, H5F_scope_t scope, void **token_ptr,
                                    H5VL_object_t **_vol_obj_ptr);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5VL_t struct */
H5FL_EXTERN(H5VL_t);

/* Declare a free list to manage the H5VL_object_t struct */
H5FL_EXTERN(H5VL_object_t);

/*-------------------------------------------------------------------------
 * Function:    H5Fget_create_plist
 *
 * Purpose:     Get an ID for a copy of the file-creation property list for
 *              this file. This function returns an ID with a copy of the
 *              properties used to create a file.
 *
 * Return:      Success:    Object ID for a copy of the file creation
 *                          property list.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Fget_create_plist(hid_t file_id)
{
    H5VL_object_t       *vol_obj;                     /* File for file_id */
    H5VL_file_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", file_id);

    /* check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid file identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_FILE_GET_FCPL;
    vol_cb_args.args.get_fcpl.fcpl_id = H5I_INVALID_HID;

    /* Retrieve the file creation property list */
    if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOPY, H5I_INVALID_HID, "unable to retrieve file creation properties");

    /* Set return value */
    ret_value = vol_cb_args.args.get_fcpl.fcpl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_create_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_access_plist
 *
 * Purpose:     Returns a copy of the file access property list of the
 *              specified file.
 *
 *              NOTE: Make sure that, if you are going to overwrite
 *              information in the copied property list that was
 *              previously opened and assigned to the property list, then
 *              you must close it before overwriting the values.
 *
 * Return:      Success:    Object ID for a copy of the file access
 *                          property list.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Fget_access_plist(hid_t file_id)
{
    H5VL_object_t       *vol_obj;                     /* File for file_id */
    H5VL_file_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", file_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid file identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_FILE_GET_FAPL;
    vol_cb_args.args.get_fapl.fapl_id = H5I_INVALID_HID;

    /* Retrieve the file's access property list */
    if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, H5I_INVALID_HID, "can't get file access property list");

    /* Set return value */
    ret_value = vol_cb_args.args.get_fapl.fapl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_access_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5F__get_all_count_cb
 *
 * Purpose:     Get counter of all object types currently open.
 *
 * Return:      Success:    H5_ITER_CONT or H5_ITER_STOP
 *
 *              Failure:    H5_ITER_ERROR
 *
 *-------------------------------------------------------------------------
 */
static int
H5F__get_all_count_cb(void H5_ATTR_UNUSED *obj_ptr, hid_t H5_ATTR_UNUSED obj_id, void *key)
{
    H5F_trav_obj_cnt_t *udata     = (H5F_trav_obj_cnt_t *)key;
    int                 ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    udata->obj_count++;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F_get_all_count_cb */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_obj_count
 *
 * Purpose:     Public function returning the number of opened object IDs
 *              (files, datasets, groups and datatypes) in the same file.
 *
 * Return:      Success:    The number of opened object IDs
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Fget_obj_count(hid_t file_id, unsigned types)
{
    ssize_t ret_value = 0; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE2("Zs", "iIu", file_id, types);

    /* Check arguments */
    if (0 == (types & H5F_OBJ_ALL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "not an object type");

    /* Perform the query */
    /* If the 'special' ID wasn't passed in, just make a normal call to
     * count the IDs in the file.
     */
    if (file_id != (hid_t)H5F_OBJ_ALL) {
        H5VL_object_t       *vol_obj;     /* File for file_id */
        size_t               count = 0;   /* Object count */
        H5VL_file_get_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Get the file object */
        if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "not a file id");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type                  = H5VL_FILE_GET_OBJ_COUNT;
        vol_cb_args.args.get_obj_count.types = types;
        vol_cb_args.args.get_obj_count.count = &count;

        /* Get the count */
        if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, (-1), "unable to get object count in file(s)");

        /* Set return value */
        ret_value = (ssize_t)count;
    }
    /* If we passed in the 'special' ID, get the count for everything open in the
     * library, iterating over all open files and getting the object count for each.
     */
    else {
        H5F_trav_obj_cnt_t udata;

        /* Set up callback context */
        udata.types     = types | H5F_OBJ_LOCAL;
        udata.obj_count = 0;

        if (types & H5F_OBJ_FILE)
            if (H5I_iterate(H5I_FILE, H5F__get_all_count_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over file IDs failed");
        if (types & H5F_OBJ_DATASET)
            if (H5I_iterate(H5I_DATASET, H5F__get_all_count_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over dataset IDs failed");
        if (types & H5F_OBJ_GROUP)
            if (H5I_iterate(H5I_GROUP, H5F__get_all_count_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over group IDs failed");
        if (types & H5F_OBJ_DATATYPE)
            if (H5I_iterate(H5I_DATATYPE, H5F__get_all_count_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over datatype IDs failed");
        if (types & H5F_OBJ_ATTR)
            if (H5I_iterate(H5I_ATTR, H5F__get_all_count_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over attribute IDs failed");

        /* Set return value */
        ret_value = (ssize_t)udata.obj_count;
    } /* end else */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_obj_count() */

/*-------------------------------------------------------------------------
 * Function:    H5F__get_all_ids_cb
 *
 * Purpose:     Get IDs of all currently open objects of a given type.
 *
 * Return:      Success:    H5_ITER_CONT or H5_ITER_STOP
 *
 *              Failure:    H5_ITER_ERROR
 *
 *-------------------------------------------------------------------------
 */
static int
H5F__get_all_ids_cb(void H5_ATTR_UNUSED *obj_ptr, hid_t obj_id, void *key)
{
    H5F_trav_obj_ids_t *udata     = (H5F_trav_obj_ids_t *)key;
    int                 ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    if (udata->obj_count >= udata->max_objs)
        HGOTO_DONE(H5_ITER_STOP);

    /* Add the ID to the array */
    udata->oid_list[udata->obj_count] = obj_id;
    udata->obj_count++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F__get_all_ids_cb */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_object_ids
 *
 * Purpose:     Public function to return a list of opened object IDs.
 *
 * NOTE:        Type mismatch - You can ask for more objects than can be
 *              returned.
 *
 * NOTE:        Currently, the IDs' ref counts are not incremented.  Is this
 *              intentional and documented?
 *
 * Return:      Success:    The number of IDs in oid_list
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Fget_obj_ids(hid_t file_id, unsigned types, size_t max_objs, hid_t *oid_list /*out*/)
{
    ssize_t ret_value = 0; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE4("Zs", "iIuzx", file_id, types, max_objs, oid_list);

    /* Check arguments */
    if (0 == (types & H5F_OBJ_ALL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "not an object type");
    if (!oid_list)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "object ID list cannot be NULL");

    /* Perform the query */
    /* If the 'special' ID wasn't passed in, just make a normal VOL call to
     * get the IDs from the file.
     */
    if (file_id != (hid_t)H5F_OBJ_ALL) {
        H5VL_object_t       *vol_obj;     /* File for file_id */
        size_t               count = 0;   /* Object count */
        H5VL_file_get_args_t vol_cb_args; /* Arguments to VOL callback */

        /* get the file object */
        if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid file identifier");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type                   = H5VL_FILE_GET_OBJ_IDS;
        vol_cb_args.args.get_obj_ids.types    = types;
        vol_cb_args.args.get_obj_ids.max_objs = max_objs;
        vol_cb_args.args.get_obj_ids.oid_list = oid_list;
        vol_cb_args.args.get_obj_ids.count    = &count;

        /* Get the IDs */
        if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, (-1), "unable to get object ids in file(s)");

        /* Set return value */
        ret_value = (ssize_t)count;
    } /* end if */
    /* If we passed in the 'special' ID, get the count for everything open in the
     * library, iterating over all open files and getting the object count for each.
     *
     * XXX: Note that the RM states that passing in a negative value for max_objs
     *      gets you all the objects. This technically works, but is clearly wrong
     *      behavior since max_objs is an unsigned type.
     */
    else {
        H5F_trav_obj_ids_t udata;

        /* Set up callback context */
        udata.max_objs  = max_objs;
        udata.oid_list  = oid_list;
        udata.obj_count = 0;

        if (types & H5F_OBJ_FILE)
            if (H5I_iterate(H5I_FILE, H5F__get_all_ids_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over file IDs failed");
        if (types & H5F_OBJ_DATASET)
            if (H5I_iterate(H5I_DATASET, H5F__get_all_ids_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over dataset IDs failed");
        if (types & H5F_OBJ_GROUP)
            if (H5I_iterate(H5I_GROUP, H5F__get_all_ids_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over group IDs failed");
        if (types & H5F_OBJ_DATATYPE)
            if (H5I_iterate(H5I_DATATYPE, H5F__get_all_ids_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over datatype IDs failed");
        if (types & H5F_OBJ_ATTR)
            if (H5I_iterate(H5I_ATTR, H5F__get_all_ids_cb, &udata, true) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_BADITER, (-1), "iteration over attribute IDs failed");

        /* Set return value */
        ret_value = (ssize_t)udata.obj_count;
    } /* end else */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_obj_ids() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_vfd_handle
 *
 * Purpose:     Returns a pointer to the file handle of the low-level file
 *              driver.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_vfd_handle(hid_t file_id, hid_t fapl_id, void **file_handle /*out*/)
{
    H5VL_object_t                   *vol_obj;             /* File info */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iix", file_id, fapl_id, file_handle);

    /* Check args */
    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid file handle pointer");

    /* Get the file object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Set up VOL callback arguments */
    file_opt_args.get_vfd_handle.fapl_id     = fapl_id;
    file_opt_args.get_vfd_handle.file_handle = file_handle;
    vol_cb_args.op_type                      = H5VL_NATIVE_FILE_GET_VFD_HANDLE;
    vol_cb_args.args                         = &file_opt_args;

    /* Retrieve the VFD handle for the file */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get VFD handle");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_vfd_handle() */

/*-------------------------------------------------------------------------
 * Function:    H5Fis_accessible
 *
 * Purpose:     Check if the file can be opened with the given fapl.
 *
 * Return:      Success:    true/false
 *              Failure:    -1 (includes file does not exist)
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Fis_accessible(const char *filename, hid_t fapl_id)
{
    H5VL_file_specific_args_t vol_cb_args;           /* Arguments to VOL callback */
    bool                      is_accessible = false; /* Whether file is accessible */
    htri_t                    ret_value;             /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("t", "*si", filename, fapl_id);

    /* Check args */
    if (!filename || !*filename)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "no file name specified");

    /* Check the file access property list */
    if (H5P_DEFAULT == fapl_id)
        fapl_id = H5P_FILE_ACCESS_DEFAULT;
    else if (true != H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not file access property list");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                       = H5VL_FILE_IS_ACCESSIBLE;
    vol_cb_args.args.is_accessible.filename   = filename;
    vol_cb_args.args.is_accessible.fapl_id    = fapl_id;
    vol_cb_args.args.is_accessible.accessible = &is_accessible;

    /* Check if file is accessible */
    if (H5VL_file_specific(NULL, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_NOTHDF5, FAIL, "unable to determine if file is accessible as HDF5");

    /* Set return value */
    ret_value = (htri_t)is_accessible;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fis_accessible() */

/*-------------------------------------------------------------------------
 * Function:    H5F__post_open_api_common
 *
 * Purpose:     This is the common function for 'post open' operations
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__post_open_api_common(H5VL_object_t *vol_obj, void **token_ptr)
{
    uint64_t supported;           /* Whether 'post open' operation is supported by VOL connector */
    herr_t   ret_value = SUCCEED; /* Return value     */

    FUNC_ENTER_PACKAGE

    /* Check for 'post open' callback */
    supported = 0;
    if (H5VL_introspect_opt_query(vol_obj, H5VL_SUBCLS_FILE, H5VL_NATIVE_FILE_POST_OPEN, &supported) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't check for 'post open' operation");
    if (supported & H5VL_OPT_QUERY_SUPPORTED) {
        H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Set up VOL callback arguments */
        vol_cb_args.op_type = H5VL_NATIVE_FILE_POST_OPEN;
        vol_cb_args.args    = NULL;

        /* Make the 'post open' callback */
        if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to make file 'post open' callback");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__post_open_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5F__create_api_common
 *
 * Purpose:     This is the common function for creating new  HDF5 files.
 *
 * Return:      Success:    A file ID
 *              Failure:    H5I_INVALID_HID
 *-------------------------------------------------------------------------
 */
static hid_t
H5F__create_api_common(const char *filename, unsigned flags, hid_t fcpl_id, hid_t fapl_id, void **token_ptr)
{
    void                 *new_file = NULL;             /* File struct for new file                 */
    H5P_genplist_t       *plist;                       /* Property list pointer                    */
    H5VL_connector_prop_t connector_prop;              /* Property for VOL connector ID & info     */
    hid_t                 ret_value = H5I_INVALID_HID; /* Return value                             */

    FUNC_ENTER_PACKAGE

    /* Check/fix arguments */
    if (!filename || !*filename)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid file name");

    /* In this routine, we only accept the following flags:
     *          H5F_ACC_EXCL, H5F_ACC_TRUNC and H5F_ACC_SWMR_WRITE
     */
    if (flags & ~(H5F_ACC_EXCL | H5F_ACC_TRUNC | H5F_ACC_SWMR_WRITE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid flags");

    /* The H5F_ACC_EXCL and H5F_ACC_TRUNC flags are mutually exclusive */
    if ((flags & H5F_ACC_EXCL) && (flags & H5F_ACC_TRUNC))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "mutually exclusive flags for file creation");

    /* Check file creation property list */
    if (H5P_DEFAULT == fcpl_id)
        fcpl_id = H5P_FILE_CREATE_DEFAULT;
    else if (true != H5P_isa_class(fcpl_id, H5P_FILE_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not file create property list");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&fapl_id, H5P_CLS_FACC, H5I_INVALID_HID, true) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* Get the VOL info from the fapl */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a file access property list");
    if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL connector info");

    /* Stash a copy of the "top-level" connector property, before any pass-through
     *  connectors modify or unwrap it.
     */
    if (H5CX_set_vol_connector_prop(&connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, H5I_INVALID_HID, "can't set VOL connector info in API context");

    /* Adjust bit flags by turning on the creation bit and making sure that
     * the EXCL or TRUNC bit is set.  All newly-created files are opened for
     * reading and writing.
     */
    if (0 == (flags & (H5F_ACC_EXCL | H5F_ACC_TRUNC)))
        flags |= H5F_ACC_EXCL; /*default*/
    flags |= H5F_ACC_RDWR | H5F_ACC_CREAT;

    /* Create a new file or truncate an existing file through the VOL */
    if (NULL == (new_file = H5VL_file_create(&connector_prop, filename, flags, fcpl_id, fapl_id,
                                             H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID, "unable to create file");

    /* Get an ID for the file */
    if ((ret_value = H5VL_register_using_vol_id(H5I_FILE, new_file, connector_prop.connector_id, true)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register file handle");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__create_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Fcreate
 *
 * Purpose:     This is the primary function for creating HDF5 files . The
 *              flags parameter determines whether an existing file will be
 *              overwritten or not.  All newly created files are opened for
 *              both reading and writing.  All flags may be combined with the
 *              bit-wise OR operator (`|') to change the behavior of the file
 *              create call.
 *
 *              The more complex behaviors of a file's creation and access
 *              are controlled through the file-creation and file-access
 *              property lists.  The value of H5P_DEFAULT for a template
 *              value indicates that the library should use the default
 *              values for the appropriate template.
 *
 * See also:    H5Fpublic.h for the list of supported flags. H5Ppublic.h for
 *              the list of file creation and file access properties.
 *
 * Return:      Success:    A file ID
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Fcreate(const char *filename, unsigned flags, hid_t fcpl_id, hid_t fapl_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* File object */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE4("i", "*sIuii", filename, flags, fcpl_id, fapl_id);

    /* Create the file synchronously */
    if ((ret_value = H5F__create_api_common(filename, flags, fcpl_id, fapl_id, NULL)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously create file");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(ret_value)))
        HGOTO_ERROR(H5E_FILE, H5E_BADTYPE, H5I_INVALID_HID, "invalid object identifier");

    /* Perform 'post open' operation */
    if (H5F__post_open_api_common(vol_obj, NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "'post open' operation failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fcreate() */

/*-------------------------------------------------------------------------
 * Function:    H5Fcreate_async
 *
 * Purpose:     Asynchronous version of H5Fcreate
 *
 * See Also:    H5Fpublic.h for a list of possible values for FLAGS.
 *
 * Return:      Success:    A file ID
 *              Failure:    H5I_INVALID_HID
 *-------------------------------------------------------------------------
 */
hid_t
H5Fcreate_async(const char *app_file, const char *app_func, unsigned app_line, const char *filename,
                unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* File object */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE8("i", "*s*sIu*sIuiii", app_file, app_func, app_line, filename, flags, fcpl_id, fapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Create the file, possibly asynchronously */
    if ((ret_value = H5F__create_api_common(filename, flags, fcpl_id, fapl_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously create file");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(ret_value)))
        HGOTO_ERROR(H5E_FILE, H5E_BADTYPE, H5I_INVALID_HID, "invalid object identifier");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE8(__func__, "*s*sIu*sIuiii", app_file, app_func, app_line, filename, flags, fcpl_id, fapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_FILE, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on file ID");
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

    /* Reset token for 'post open' operation */
    /* (Unnecessary if create operation didn't change it, but not worth checking -QAK) */
    token = NULL;

    /* Perform 'post open' operation */
    if (H5F__post_open_api_common(vol_obj, token_ptr) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "'post open' operation failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE8(__func__, "*s*sIu*sIuiii", app_file, app_func, app_line, filename, flags, fcpl_id, fapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fcreate_async() */

/*-------------------------------------------------------------------------
 * Function:    H5F__open_api_common
 *
 * Purpose:     This is the common function for accessing existing HDF5
 *              files.
 *
 * Return:      Success:    A file ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5F__open_api_common(const char *filename, unsigned flags, hid_t fapl_id, void **token_ptr)
{
    void                 *new_file = NULL;             /* File struct for new file                 */
    H5P_genplist_t       *plist;                       /* Property list pointer                    */
    H5VL_connector_prop_t connector_prop;              /* Property for VOL connector ID & info     */
    hid_t                 ret_value = H5I_INVALID_HID; /* Return value                             */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!filename || !*filename)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid file name");
    /* Reject undefined flags (~H5F_ACC_PUBLIC_FLAGS) and the H5F_ACC_TRUNC & H5F_ACC_EXCL flags */
    if ((flags & ~H5F_ACC_PUBLIC_FLAGS) || (flags & H5F_ACC_TRUNC) || (flags & H5F_ACC_EXCL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid file open flags");
    /* XXX (VOL MERGE): Might want to move SWMR flag checks to H5F_open() */
    /* Asking for SWMR write access on a read-only file is invalid */
    if ((flags & H5F_ACC_SWMR_WRITE) && 0 == (flags & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID,
                    "SWMR write access on a file open for read-only access is not allowed");
    /* Asking for SWMR read access on a non-read-only file is invalid */
    if ((flags & H5F_ACC_SWMR_READ) && (flags & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID,
                    "SWMR read access on a file open for read-write access is not allowed");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&fapl_id, H5P_CLS_FACC, H5I_INVALID_HID, true) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* Get the VOL info from the fapl */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a file access property list");
    if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL connector info");

    /* Stash a copy of the "top-level" connector property, before any pass-through
     *  connectors modify or unwrap it.
     */
    if (H5CX_set_vol_connector_prop(&connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, H5I_INVALID_HID, "can't set VOL connector info in API context");

    /* Open the file through the VOL layer */
    if (NULL == (new_file = H5VL_file_open(&connector_prop, filename, flags, fapl_id,
                                           H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID, "unable to open file");

    /* Get an ID for the file */
    if ((ret_value = H5VL_register_using_vol_id(H5I_FILE, new_file, connector_prop.connector_id, true)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register file handle");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__open_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Fopen
 *
 * Purpose:     This is the primary function for accessing existing HDF5
 *              files.  The FLAGS argument determines whether writing to an
 *              existing file will be allowed or not.  All flags may be
 *              combined with the bit-wise OR operator (`|') to change the
 *              behavior of the file open call.  The more complex behaviors
 *              of a file's access are controlled through the file-access
 *              property list.
 *
 * See Also:    H5Fpublic.h for a list of possible values for FLAGS.
 *
 * Return:      Success:    A file ID
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Fopen(const char *filename, unsigned flags, hid_t fapl_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* File object */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "*sIui", filename, flags, fapl_id);

    /* Open the file synchronously */
    if ((ret_value = H5F__open_api_common(filename, flags, fapl_id, NULL)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID, "unable to synchronously open file");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(ret_value)))
        HGOTO_ERROR(H5E_FILE, H5E_BADTYPE, H5I_INVALID_HID, "invalid object identifier");

    /* Perform 'post open' operation */
    if (H5F__post_open_api_common(vol_obj, NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "'post open' operation failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fopen() */

/*-------------------------------------------------------------------------
 * Function:    H5Fopen_async
 *
 * Purpose:     Asynchronous version of H5Fopen
 *
 * See Also:    H5Fpublic.h for a list of possible values for FLAGS.
 *
 * Return:      Success:    A file ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Fopen_async(const char *app_file, const char *app_func, unsigned app_line, const char *filename,
              unsigned flags, hid_t fapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* File object */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "*s*sIu*sIuii", app_file, app_func, app_line, filename, flags, fapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the file, possibly asynchronously */
    if ((ret_value = H5F__open_api_common(filename, flags, fapl_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID, "unable to asynchronously open file");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(ret_value)))
        HGOTO_ERROR(H5E_FILE, H5E_BADTYPE, H5I_INVALID_HID, "invalid object identifier");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIu*sIuii", app_file, app_func, app_line, filename, flags, fapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_FILE, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on file ID");
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

    /* Reset token for 'post open' operation */
    /* (Unnecessary if create operation didn't change it, but not worth checking -QAK) */
    token = NULL;

    /* Perform 'post open' operation */
    if (H5F__post_open_api_common(vol_obj, token_ptr) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "'post open' operation failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIu*sIuii", app_file, app_func, app_line, filename, flags, fapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fopen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5F__flush_api_common
 *
 * Purpose:     This is the common function for flushing an HDF5 file.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__flush_api_common(hid_t object_id, H5F_scope_t scope, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5I_type_t                obj_type;               /* Type of object to use */
    H5VL_file_specific_args_t vol_cb_args;            /* Arguments to VOL callback */
    herr_t                    ret_value = SUCCEED;    /* Return value     */

    FUNC_ENTER_PACKAGE

    /* Get the type of object we're flushing + sanity check */
    obj_type = H5I_get_type(object_id);
    if (H5I_FILE != obj_type && H5I_GROUP != obj_type && H5I_DATATYPE != obj_type &&
        H5I_DATASET != obj_type && H5I_ATTR != obj_type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Get the file object */
    if (NULL == (*vol_obj_ptr = H5VL_vol_object(object_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_FILE_FLUSH;
    vol_cb_args.args.flush.obj_type = obj_type;
    vol_cb_args.args.flush.scope    = scope;

    /* Flush the object */
    if (H5VL_file_specific(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to flush file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F__flush_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Fflush
 *
 * Purpose:     Flushes all outstanding buffers of a file to disk but does
 *              not remove them from the cache.  The OBJECT_ID can be a file,
 *              dataset, group, attribute, or named data type.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fflush(hid_t object_id, H5F_scope_t scope)
{
    herr_t ret_value = SUCCEED; /* Return value     */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iFs", object_id, scope);

    /* Flush the file synchronously */
    if (H5F__flush_api_common(object_id, scope, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to synchronously flush file");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fflush() */

/*-------------------------------------------------------------------------
 * Function:    H5Fflush_async
 *
 * Purpose:     Asynchronous version of H5Fflush
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fflush_async(const char *app_file, const char *app_func, unsigned app_line, hid_t object_id,
               H5F_scope_t scope, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value     */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "*s*sIuiFsi", app_file, app_func, app_line, object_id, scope, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Flush the file asynchronously */
    if (H5F__flush_api_common(object_id, scope, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTFLUSH, FAIL, "unable to asynchronously flush file");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                H5ARG_TRACE6(__func__, "*s*sIuiFsi", app_file, app_func, app_line, object_id, scope, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fflush_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Fclose
 *
 * Purpose:     This function closes the file specified by FILE_ID by
 *              flushing all data to storage, and terminating access to the
 *              file through FILE_ID.  If objects (e.g., datasets, groups,
 *              etc.) are open in the file then the underlying storage is not
 *              closed until those objects are closed; however, all data for
 *              the file and the open objects is flushed.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fclose(hid_t file_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Check arguments */
    if (H5I_FILE != H5I_get_type(file_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file ID");

    /* Synchronously decrement reference count on ID.
     * When it reaches zero the file will be closed.
     */
    if (H5I_dec_app_ref(file_id) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "decrementing file ID failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fclose() */

/*-------------------------------------------------------------------------
 * Function:    H5Fclose_async
 *
 * Purpose:     Asynchronous version of H5Fclose
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fclose_async(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    H5VL_t        *connector = NULL;            /* VOL connector */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, file_id, es_id);

    /* Check arguments */
    if (H5I_FILE != H5I_get_type(file_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file ID");

    /* Prepare for possible asynchronous operation */
    if (H5ES_NONE != es_id) {
        /* Get file object's connector */
        if (NULL == (vol_obj = H5VL_vol_object(file_id)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get VOL object for file");

        /* Increase connector's refcount, so it doesn't get closed if closing
         * this file ID closes the file */
        connector = vol_obj->connector;
        H5VL_conn_inc_rc(connector);

        /* Point at token for operation to set up */
        token_ptr = &token;
    } /* end if */

    /* Asynchronously decrement reference count on ID.
     * When it reaches zero the file will be closed.
     */
    if (H5I_dec_app_ref_async(file_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "decrementing file ID failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, file_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    if (connector && H5VL_conn_dec_rc(connector) < 0)
        HDONE_ERROR(H5E_FILE, H5E_CANTDEC, FAIL, "can't decrement ref count on connector");

    FUNC_LEAVE_API(ret_value)
} /* end H5Fclose_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Fdelete
 *
 * Purpose:     Deletes an HDF5 file.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fdelete(const char *filename, hid_t fapl_id)
{
    H5P_genplist_t           *plist;                 /* Property list pointer */
    H5VL_connector_prop_t     connector_prop;        /* Property for VOL connector ID & info */
    H5VL_file_specific_args_t vol_cb_args;           /* Arguments to VOL callback */
    bool                      is_accessible = false; /* Whether file is accessible */
    herr_t                    ret_value     = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "*si", filename, fapl_id);

    /* Check args */
    if (!filename || !*filename)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "no file name specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&fapl_id, H5P_CLS_FACC, H5I_INVALID_HID, true) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the VOL info from the fapl */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(fapl_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
    if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get VOL connector info");

    /* Stash a copy of the "top-level" connector property, before any pass-through
     *  connectors modify or unwrap it.
     */
    if (H5CX_set_vol_connector_prop(&connector_prop) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set VOL connector info in API context");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                       = H5VL_FILE_IS_ACCESSIBLE;
    vol_cb_args.args.is_accessible.filename   = filename;
    vol_cb_args.args.is_accessible.fapl_id    = fapl_id;
    vol_cb_args.args.is_accessible.accessible = &is_accessible;

    /* Make sure this is HDF5 storage for this VOL connector */
    if (H5VL_file_specific(NULL, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_NOTHDF5, FAIL, "unable to determine if file is accessible as HDF5");
    if (!is_accessible)
        HGOTO_ERROR(H5E_FILE, H5E_NOTHDF5, FAIL, "not an HDF5 file");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type           = H5VL_FILE_DELETE;
    vol_cb_args.args.del.filename = filename;
    vol_cb_args.args.del.fapl_id  = fapl_id;

    /* Delete the file */
    if (H5VL_file_specific(NULL, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDELETEFILE, FAIL, "unable to delete the file");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fdelete() */

/*-------------------------------------------------------------------------
 * Function:    H5Fmount
 *
 * Purpose:     Mount file CHILD_ID onto the group specified by LOC_ID and
 *              NAME using mount properties PLIST_ID.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fmount(hid_t loc_id, const char *name, hid_t child_id, hid_t plist_id)
{
    H5VL_object_t             *loc_vol_obj   = NULL; /* Parent object        */
    H5VL_object_t             *child_vol_obj = NULL; /* Child object         */
    H5VL_group_specific_args_t vol_cb_args;          /* Arguments to VOL callback */
    void                      *grp = NULL;           /* Root group opened */
    H5I_type_t                 loc_type;             /* ID type of location  */
    int                        same_connector = 0; /* Whether parent and child files use the same connector */
    herr_t                     ret_value      = SUCCEED; /* Return value         */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*sii", loc_id, name, child_id, plist_id);

    /* Check arguments */
    loc_type = H5I_get_type(loc_id);
    if (H5I_FILE != loc_type && H5I_GROUP != loc_type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "loc_id parameter not a file or group ID");
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be the empty string");
    if (H5I_FILE != H5I_get_type(child_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "child_id parameter not a file ID");
    if (H5P_DEFAULT == plist_id)
        plist_id = H5P_FILE_MOUNT_DEFAULT;
    else if (true != H5P_isa_class(plist_id, H5P_FILE_MOUNT))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "plist_id is not a file mount property list ID");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Get the location object */
    /* Need to open the root group of a file, if a file ID was given as the
     *  'loc_id', because the 'mount' operation is a group specific operation.
     */
    if (H5I_FILE == loc_type) {
        H5VL_object_t    *vol_obj;    /* Object for loc_id (file) */
        H5VL_loc_params_t loc_params; /* Location parameters for object access */

        /* Get the location object */
        if (NULL == (vol_obj = (H5VL_object_t *)H5VL_vol_object(loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Set location parameters */
        loc_params.type     = H5VL_OBJECT_BY_SELF;
        loc_params.obj_type = loc_type;

        /* Open the root group object */
        if (NULL == (grp = H5VL_group_open(vol_obj, &loc_params, "/", H5P_GROUP_ACCESS_DEFAULT,
                                           H5P_DATASET_XFER_DEFAULT, NULL)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "unable to open group");

        /* Create a VOL object for the root group */
        if (NULL == (loc_vol_obj = H5VL_create_object(grp, vol_obj->connector)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "can't create VOL object for root group");
    } /* end if */
    else {
        assert(H5I_GROUP == loc_type);
        if (NULL == (loc_vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "could not get location object");
    } /* end else */

    /* Get the child object */
    if (NULL == (child_vol_obj = (H5VL_object_t *)H5I_object(child_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "could not get child object");

    /* Check if both objects are associated with the same VOL connector */
    if (H5VL_cmp_connector_cls(&same_connector, loc_vol_obj->connector->cls, child_vol_obj->connector->cls) <
        0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCOMPARE, FAIL, "can't compare connector classes");
    if (same_connector)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "can't mount file onto object from different VOL connector");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type         = H5VL_GROUP_MOUNT;
    vol_cb_args.args.mount.name = name;
    vol_cb_args.args.mount.child_file =
        child_vol_obj->data; /* Don't unwrap fully, so each connector can see its object */
    vol_cb_args.args.mount.fmpl_id = plist_id;

    /* Perform the mount operation */
    /* (This is on a group, so that the VOL framework always sees groups for
     *  the 'mount' operation, instead of mixing files and groups)
     */
    if (H5VL_group_specific(loc_vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "unable to mount file");

done:
    /* Clean up if we temporarily opened the root group for a file */
    if (grp) {
        assert(loc_vol_obj);
        if (H5VL_group_close(loc_vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "unable to release group");
        if (H5VL_free_object(loc_vol_obj) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTDEC, FAIL, "unable to free VOL object");
    } /* end if */

    FUNC_LEAVE_API(ret_value)
} /* end H5Fmount() */

/*-------------------------------------------------------------------------
 * Function:    H5Funmount
 *
 * Purpose:     Given a mount point, disassociate the mount point's file
 *              from the file mounted there. Do not close either file.
 *
 *              The mount point can either be the group in the parent or the
 *              root group of the mounted file (both groups have the same
 *              name). If the mount point was opened before the mount then
 *              it's the group in the parent, but if it was opened after the
 *              mount then it's the root group of the child.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Funmount(hid_t loc_id, const char *name)
{
    H5VL_object_t             *loc_vol_obj = NULL;  /* Parent object        */
    H5VL_group_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    void                      *grp = NULL;          /* Root group opened */
    H5I_type_t                 loc_type;            /* ID type of location  */
    herr_t                     ret_value = SUCCEED; /* Return value         */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", loc_id, name);

    /* Check arguments */
    loc_type = H5I_get_type(loc_id);
    if (H5I_FILE != loc_type && H5I_GROUP != loc_type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "loc_id parameter not a file or group ID");
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be the empty string");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Get the location object */
    /* Need to open the root group of a file, if a file ID was given as the
     *  'loc_id', because the 'mount' operation is a group specific operation.
     */
    if (H5I_FILE == loc_type) {
        H5VL_object_t    *vol_obj;    /* Object for loc_id (file) */
        H5VL_loc_params_t loc_params; /* Location parameters for object access */

        /* Get the location object */
        if (NULL == (vol_obj = (H5VL_object_t *)H5VL_vol_object(loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Set location parameters */
        loc_params.type     = H5VL_OBJECT_BY_SELF;
        loc_params.obj_type = loc_type;

        /* Open the root group object */
        if (NULL == (grp = H5VL_group_open(vol_obj, &loc_params, "/", H5P_GROUP_ACCESS_DEFAULT,
                                           H5P_DATASET_XFER_DEFAULT, NULL)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "unable to open group");

        /* Create a VOL object for the root group */
        if (NULL == (loc_vol_obj = H5VL_create_object(grp, vol_obj->connector)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTOPENOBJ, FAIL, "can't create VOL object for root group");
    } /* end if */
    else {
        assert(H5I_GROUP == loc_type);
        if (NULL == (loc_vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "could not get location object");
    } /* end else */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type           = H5VL_GROUP_UNMOUNT;
    vol_cb_args.args.unmount.name = name;

    /* Perform the unmount operation */
    /* (This is on a group, so that the VOL framework always sees groups for
     *  the 'unmount' operation, instead of mixing files and groups)
     */
    if (H5VL_group_specific(loc_vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "unable to unmount file");

done:
    /* Clean up if we temporarily opened the root group for a file */
    if (grp) {
        assert(loc_vol_obj);
        if (H5VL_group_close(loc_vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CLOSEERROR, FAIL, "unable to release group");
        if (H5VL_free_object(loc_vol_obj) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTDEC, FAIL, "unable to free VOL object");
    } /* end if */

    FUNC_LEAVE_API(ret_value)
} /* end H5Funmount() */

/*-------------------------------------------------------------------------
 * Function:    H5F__reopen_api_common
 *
 * Purpose:     This is the common function for reopening an HDF5 file
 *              files.
 *
 * Return:      Success:    A file ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5F__reopen_api_common(hid_t file_id, void **token_ptr)
{
    H5VL_object_t            *vol_obj = NULL;                /* Object for loc_id */
    H5VL_file_specific_args_t vol_cb_args;                   /* Arguments to VOL callback */
    void                     *reopen_file = NULL;            /* Pointer to the re-opened file object */
    hid_t                     ret_value   = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the file object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid file identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type          = H5VL_FILE_REOPEN;
    vol_cb_args.args.reopen.file = &reopen_file;

    /* Reopen the file */
    if (H5VL_file_specific(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "unable to reopen file via the VOL connector");

    /* Make sure that worked */
    if (NULL == reopen_file)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "unable to reopen file");

    /* Get an ID for the file */
    if ((ret_value = H5VL_register(H5I_FILE, reopen_file, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register file handle");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__reopen_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Freopen
 *
 * Purpose:     Reopen a file.  The new file handle which is returned points
 *              to the same file as the specified file handle.  Both handles
 *              share caches and other information.  The only difference
 *              between the handles is that the new handle is not mounted
 *              anywhere and no files are mounted on it.
 *
 * Return:      Success:    New file ID
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Freopen(hid_t file_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* File object */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", file_id);

    /* Reopen the file synchronously */
    if ((ret_value = H5F__reopen_api_common(file_id, NULL)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID, "unable to synchronously reopen file");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(ret_value)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, H5I_INVALID_HID, "can't get handle for re-opened file");

    /* Perform 'post open' operation */
    if (H5F__post_open_api_common(vol_obj, NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "'post open' operation failed");

done:
    /* XXX (VOL MERGE): If registration fails, file will not be closed */
    FUNC_LEAVE_API(ret_value)
} /* end H5Freopen() */

/*-------------------------------------------------------------------------
 * Function:    H5Freopen_async
 *
 * Purpose:     Asynchronous version of H5Freopen
 *
 * See Also:    H5Fpublic.h for a list of possible values for FLAGS.
 *
 * Return:      Success:    A file ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Freopen_async(const char *app_file, const char *app_func, unsigned app_line, hid_t file_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value;                   /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "*s*sIuii", app_file, app_func, app_line, file_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Reopen the file, possibly asynchronously */
    if ((ret_value = H5F__reopen_api_common(file_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, H5I_INVALID_HID, "unable to asynchronously reopen file");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(ret_value)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, H5I_INVALID_HID, "can't get handle for re-opened file");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, file_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_FILE, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on file ID");
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

    /* Reset token for 'post open' operation */
    /* (Unnecessary if create operation didn't change it, but not worth checking -QAK) */
    token = NULL;

    /* Perform 'post open' operation */
    if (H5F__post_open_api_common(vol_obj, token_ptr) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, H5I_INVALID_HID, "'post open' operation failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, file_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_FILE, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Freopen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_intent
 *
 * Purpose:     Public API to retrieve the file's 'intent' flags passed
 *              during H5Fopen()
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_intent(hid_t file_id, unsigned *intent_flags /*out*/)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, intent_flags);

    /* If no intent flags were passed in, exit quietly */
    if (intent_flags) {
        H5VL_object_t       *vol_obj;     /* File for file_id */
        H5VL_file_get_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Get the internal file structure */
        if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type               = H5VL_FILE_GET_INTENT;
        vol_cb_args.args.get_intent.flags = intent_flags;

        /* Get the flags */
        if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get file's intent flags");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_intent() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_fileno
 *
 * Purpose:     Public API to retrieve the file's 'file number' that uniquely
 *              identifies each open file.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_fileno(hid_t file_id, unsigned long *fnumber /*out*/)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, fnumber);

    /* If no fnumber pointer was passed in, exit quietly */
    if (fnumber) {
        H5VL_object_t       *vol_obj;     /* File for file_id */
        H5VL_file_get_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Get the internal file structure */
        if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type                = H5VL_FILE_GET_FILENO;
        vol_cb_args.args.get_fileno.fileno = fnumber;

        /* Get the 'file number' */
        if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get file's 'file number'");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_fileno() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_freespace
 *
 * Purpose:     Retrieves the amount of free space in the file.
 *
 * Return:      Success:    Amount of free space for type
 *              Failure:    -1
 *-------------------------------------------------------------------------
 */
hssize_t
H5Fget_freespace(hid_t file_id)
{
    H5VL_object_t                   *vol_obj = NULL;
    H5VL_optional_args_t             vol_cb_args;        /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;      /* Arguments for optional operation */
    hsize_t                          file_freespace = 0; /* Size of freespace in the file */
    hssize_t                         ret_value;          /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("Hs", "i", file_id);

    /* Get the file object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid file identifier");

    /* Set up VOL callback arguments */
    file_opt_args.get_freespace.size = &file_freespace;
    vol_cb_args.op_type              = H5VL_NATIVE_FILE_GET_FREE_SPACE;
    vol_cb_args.args                 = &file_opt_args;

    /* Get the amount of free space in the file */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, (-1), "unable to get file free space");

    /* Set return value */
    ret_value = (hssize_t)file_freespace;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_freespace() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_filesize
 *
 * Purpose:     Retrieves the file size of the HDF5 file. This function
 *              is called after an existing file is opened in order
 *              to learn the true size of the underlying file.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_filesize(hid_t file_id, hsize_t *size /*out*/)
{
    H5VL_object_t                   *vol_obj;             /* File info */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, size);

    /* Check args */
    if (!size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "size parameter cannot be NULL");
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.get_size.size = size;
    vol_cb_args.op_type         = H5VL_NATIVE_FILE_GET_SIZE;
    vol_cb_args.args            = &file_opt_args;

    /* Get the file size */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get file size");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_filesize() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_file_image
 *
 * Purpose:     If a buffer is provided (via the buf_ptr argument) and is
 *              big enough (size in buf_len argument), load *buf_ptr with
 *              an image of the open file whose ID is provided in the
 *              file_id parameter, and return the number of bytes copied
 *              to the buffer.
 *
 *              If the buffer exists, but is too small to contain an image
 *              of the indicated file, return a negative number.
 *
 *              Finally, if no buffer is provided, return the size of the
 *              buffer needed.  This value is simply the eoa of the target
 *              file.
 *
 *              Note that any user block is skipped.
 *
 *              Also note that the function may not be used on files
 *              opened with either the split/multi file driver or the
 *              family file driver.
 *
 *              In the former case, the sparse address space makes the
 *              get file image operation impractical, due to the size of
 *              the image typically required.
 *
 *              In the case of the family file driver, the problem is
 *              the driver message in the super block, which will prevent
 *              the image being opened with any driver other than the
 *              family file driver -- which negates the purpose of the
 *              operation.  This can be fixed, but no resources for
 *              this now.
 *
 * Return:      Success:    Bytes copied / number of bytes needed
 *              Failure:    -1
 *-------------------------------------------------------------------------
 */
ssize_t
H5Fget_file_image(hid_t file_id, void *buf /*out*/, size_t buf_len)
{
    H5VL_object_t                   *vol_obj;       /* File object for file ID  */
    H5VL_optional_args_t             vol_cb_args;   /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args; /* Arguments for optional operation */
    size_t                           image_len = 0; /* Size of image buffer */
    ssize_t                          ret_value;     /* Return value             */

    FUNC_ENTER_API((-1))
    H5TRACE3("Zs", "ixz", file_id, buf, buf_len);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.get_file_image.buf_size  = buf_len;
    file_opt_args.get_file_image.buf       = buf;
    file_opt_args.get_file_image.image_len = &image_len;
    vol_cb_args.op_type                    = H5VL_NATIVE_FILE_GET_FILE_IMAGE;
    vol_cb_args.args                       = &file_opt_args;

    /* Get the file image */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, (-1), "unable to get file image");

    /* Set return value */
    ret_value = (ssize_t)image_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_file_image() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_mdc_config
 *
 * Purpose:     Retrieves the current automatic cache resize configuration
 *              from the metadata cache, and return it in *config_ptr.
 *
 *              Note that the version field of *config_Ptr must be correctly
 *              filled in by the caller.  This allows us to adapt for
 *              obsolete versions of the structure.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_mdc_config(hid_t file_id, H5AC_cache_config_t *config /*out*/)
{
    H5VL_object_t                   *vol_obj = NULL;
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, config);

    /* Check args */
    if ((NULL == config) || (config->version != H5AC__CURR_CACHE_CONFIG_VERSION))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Bad config ptr");

    /* Get the file object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Set up VOL callback arguments */
    file_opt_args.get_mdc_config.config = config;
    vol_cb_args.op_type                 = H5VL_NATIVE_FILE_GET_MDC_CONF;
    vol_cb_args.args                    = &file_opt_args;

    /* Get the metadata cache configuration */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get metadata cache configuration");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_mdc_config() */

/*-------------------------------------------------------------------------
 * Function:    H5Fset_mdc_config
 *
 * Purpose:     Sets the current metadata cache automatic resize
 *              configuration, using the contents of the instance of
 *              H5AC_cache_config_t pointed to by config_ptr.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fset_mdc_config(hid_t file_id, const H5AC_cache_config_t *config_ptr)
{
    H5VL_object_t                   *vol_obj = NULL;
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*Cc", file_id, config_ptr);

    /* Get the file object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Set up VOL callback arguments */
    file_opt_args.set_mdc_config.config = config_ptr;
    vol_cb_args.op_type                 = H5VL_NATIVE_FILE_SET_MDC_CONFIG;
    vol_cb_args.args                    = &file_opt_args;

    /* Set the metadata cache configuration  */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "unable to set metadata cache configuration");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fset_mdc_config() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_mdc_hit_rate
 *
 * Purpose:     Retrieves the current hit rate from the metadata cache.
 *              This rate is the overall hit rate since the last time
 *              the hit rate statistics were reset either manually or
 *              automatically.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_mdc_hit_rate(hid_t file_id, double *hit_rate /*out*/)
{
    H5VL_object_t                   *vol_obj;
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, hit_rate);

    /* Check args */
    if (NULL == hit_rate)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL hit rate pointer");
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.get_mdc_hit_rate.hit_rate = hit_rate;
    vol_cb_args.op_type                     = H5VL_NATIVE_FILE_GET_MDC_HR;
    vol_cb_args.args                        = &file_opt_args;

    /* Get the current hit rate */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get MDC hit rate");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_mdc_hit_rate() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_mdc_size
 *
 * Purpose:     Retrieves the maximum size, minimum clean size, current
 *              size, and current number of entries from the metadata
 *              cache associated with the specified file.  If any of
 *              the ptr parameters are NULL, the associated datum is
 *              not returned.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_mdc_size(hid_t file_id, size_t *max_size /*out*/, size_t *min_clean_size /*out*/,
                size_t *cur_size /*out*/, int *cur_num_entries /*out*/)
{
    H5VL_object_t                   *vol_obj;
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    uint32_t                         index_len = 0;       /* Size of cache index */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ixxxx", file_id, max_size, min_clean_size, cur_size, cur_num_entries);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.get_mdc_size.max_size        = max_size;
    file_opt_args.get_mdc_size.min_clean_size  = min_clean_size;
    file_opt_args.get_mdc_size.cur_size        = cur_size;
    file_opt_args.get_mdc_size.cur_num_entries = &index_len;
    vol_cb_args.op_type                        = H5VL_NATIVE_FILE_GET_MDC_SIZE;
    vol_cb_args.args                           = &file_opt_args;

    /* Get the size data */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get MDC size");

    /* Set mis-matched return value */
    if (cur_num_entries)
        *cur_num_entries = (int)index_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_mdc_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Freset_mdc_hit_rate_stats
 *
 * Purpose:     Reset the hit rate statistic whose current value can
 *              be obtained via the H5Fget_mdc_hit_rate() call.  Note
 *              that this statistic will also be reset once per epoch
 *              by the automatic cache resize code if it is enabled.
 *
 *              It is probably a bad idea to call this function unless
 *              you are controlling cache size from your program instead
 *              of using our cache size control code.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Freset_mdc_hit_rate_stats(hid_t file_id)
{
    H5VL_object_t       *vol_obj = NULL;
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Get the file object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_FILE_RESET_MDC_HIT_RATE;
    vol_cb_args.args    = NULL;

    /* Reset the hit rate statistic */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't reset cache hit rate");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Freset_mdc_hit_rate_stats() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_name
 *
 * Purpose:     Gets the name of the file to which object OBJ_ID belongs.
 *              If 'name' is non-NULL then write up to 'size' bytes into that
 *              buffer and always return the length of the entry name.
 *              Otherwise `size' is ignored and the function does not store
 *              the name, just returning the number of characters required to
 *              store the name. If an error occurs then the buffer pointed to
 *              by 'name' (NULL or non-NULL) is unchanged and the function
 *              returns a negative value.
 *
 * Note:        This routine returns the name that was used to open the file,
 *              not the actual name after resolving symlinks, etc.
 *
 * Return:      Success:    The length of the file name
 *              Failure:    -1
 *-------------------------------------------------------------------------
 */
ssize_t
H5Fget_name(hid_t obj_id, char *name /*out*/, size_t size)
{
    H5VL_object_t       *vol_obj;     /* File for file_id */
    H5VL_file_get_args_t vol_cb_args; /* Arguments to VOL callback */
    H5I_type_t           type;
    size_t               file_name_len = 0;  /* Length of file name */
    ssize_t              ret_value     = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE3("Zs", "ixz", obj_id, name, size);

    /* Check the type */
    type = H5I_get_type(obj_id);
    if (H5I_FILE != type && H5I_GROUP != type && H5I_DATATYPE != type && H5I_DATASET != type &&
        H5I_ATTR != type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "not a file or file object");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid file identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                     = H5VL_FILE_GET_NAME;
    vol_cb_args.args.get_name.type          = type;
    vol_cb_args.args.get_name.buf_size      = size;
    vol_cb_args.args.get_name.buf           = name;
    vol_cb_args.args.get_name.file_name_len = &file_name_len;

    /* Get the filename via the VOL */
    if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, (-1), "unable to get file name");

    /* Set the return value */
    ret_value = (ssize_t)file_name_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_info2
 *
 * Purpose:     Gets general information about the file, including:
 *              1. Get storage size for superblock extension if there is one.
 *              2. Get the amount of btree and heap storage for entries
 *                 in the SOHM table if there is one.
 *              3. The amount of free space tracked in the file.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_info2(hid_t obj_id, H5F_info2_t *finfo /*out*/)
{
    H5VL_object_t                   *vol_obj = NULL;
    H5VL_optional_args_t             vol_cb_args;   /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args; /* Arguments for optional operation */
    H5I_type_t                       type;
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", obj_id, finfo);

    /* Check args */
    if (!finfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file info pointer can't be NULL");

    /* Check the type */
    type = H5I_get_type(obj_id);
    if (H5I_FILE != type && H5I_GROUP != type && H5I_DATATYPE != type && H5I_DATASET != type &&
        H5I_ATTR != type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set up VOL callback arguments */
    file_opt_args.get_info.type  = type;
    file_opt_args.get_info.finfo = finfo;
    vol_cb_args.op_type          = H5VL_NATIVE_FILE_GET_INFO;
    vol_cb_args.args             = &file_opt_args;

    /* Get the file information */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to retrieve file info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_info2() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_metadata_read_retry_info
 *
 * Purpose:     To retrieve the collection of read retries for metadata
 *              items with checksum.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_metadata_read_retry_info(hid_t file_id, H5F_retry_info_t *info /*out*/)
{
    H5VL_object_t                   *vol_obj = NULL;      /* File object for file ID */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, info);

    /* Check args */
    if (!info)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no info struct");

    /* Get the file pointer */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.get_metadata_read_retry_info.info = info;
    vol_cb_args.op_type                             = H5VL_NATIVE_FILE_GET_METADATA_READ_RETRY_INFO;
    vol_cb_args.args                                = &file_opt_args;

    /* Get the retry info */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get metadata read retry info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_metadata_read_retry_info() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_free_sections
 *
 * Purpose:     To get free-space section information for free-space manager with
 *              TYPE that is associated with file FILE_ID.
 *              If SECT_INFO is null, this routine returns the total # of free-space
 *              sections.
 *
 * Return:      Success:   The total # of free space sections
 *              Failure:   -1
 *-------------------------------------------------------------------------
 */
ssize_t
H5Fget_free_sections(hid_t file_id, H5F_mem_t type, size_t nsects, H5F_sect_info_t *sect_info /*out*/)
{
    H5VL_object_t                   *vol_obj = NULL;
    H5VL_optional_args_t             vol_cb_args;     /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;   /* Arguments for optional operation */
    size_t                           sect_count = 0;  /* Number of sections */
    ssize_t                          ret_value  = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE4("Zs", "iFmzx", file_id, type, nsects, sect_info);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid file identifier");
    if (sect_info && nsects == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "nsects must be > 0");

    /* Set up VOL callback arguments */
    file_opt_args.get_free_sections.type       = type;
    file_opt_args.get_free_sections.sect_info  = sect_info;
    file_opt_args.get_free_sections.nsects     = nsects;
    file_opt_args.get_free_sections.sect_count = &sect_count;
    vol_cb_args.op_type                        = H5VL_NATIVE_FILE_GET_FREE_SECTIONS;
    vol_cb_args.args                           = &file_opt_args;

    /* Get the free-space section information in the file */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, (-1), "unable to get file free sections");

    /* Set return value */
    ret_value = (ssize_t)sect_count;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_free_sections() */

/*-------------------------------------------------------------------------
 * Function:    H5Fclear_elink_file_cache
 *
 * Purpose:     Releases the external file cache associated with the
 *              provided file, potentially closing any cached files
 *              unless they are held open from somewhere\ else.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fclear_elink_file_cache(hid_t file_id)
{
    H5VL_object_t       *vol_obj;             /* File */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a file ID");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_FILE_CLEAR_ELINK_CACHE;
    vol_cb_args.args    = NULL;

    /* Release the EFC */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTRELEASE, FAIL, "can't release external file cache");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fclear_elink_file_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5Fstart_swmr_write
 *
 * Purpose:     To enable SWMR writing mode for the file
 *
 *              1) Refresh opened objects: part 1
 *              2) Flush & reset accumulator
 *              3) Mark the file in SWMR writing mode
 *              4) Set metadata read attempts and retries info
 *              5) Disable accumulator
 *              6) Evict all cache entries except the superblock
 *              7) Refresh opened objects (part 2)
 *              8) Unlock the file
 *
 *              Pre-conditions:
 *
 *              1) The file being opened has v3 superblock
 *              2) The file is opened with H5F_ACC_RDWR
 *              3) The file is not already marked for SWMR writing
 *              4) Current implementation for opened objects:
 *                  --only allow datasets and groups without attributes
 *                  --disallow named datatype with/without attributes
 *                  --disallow opened attributes attached to objects
 *
 * NOTE:        Currently, only opened groups and datasets are allowed
 *              when enabling SWMR via H5Fstart_swmr_write().
 *              Will later implement a different approach--
 *              set up flush dependency/proxy even for file opened without
 *              SWMR to resolve issues with opened objects.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fstart_swmr_write(hid_t file_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* File info */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hid_t identifier is not a file ID");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(file_id) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_FILE_START_SWMR_WRITE;
    vol_cb_args.args    = NULL;

    /* Start SWMR writing */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_SYSTEM, FAIL, "unable to start SWMR writing");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fstart_swmr_write() */

/*-------------------------------------------------------------------------
 * Function:    H5Fstart_mdc_logging
 *
 * Purpose:     Start metadata cache logging operations for a file.
 *                  - Logging must have been set up via the fapl.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fstart_mdc_logging(hid_t file_id)
{
    H5VL_object_t       *vol_obj;             /* File info */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Sanity check */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hid_t identifier is not a file ID");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_FILE_START_MDC_LOGGING;
    vol_cb_args.args    = NULL;

    /* Call mdc logging function */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_LOGGING, FAIL, "unable to start mdc logging");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fstart_mdc_logging() */

/*-------------------------------------------------------------------------
 * Function:    H5Fstop_mdc_logging
 *
 * Purpose:     Stop metadata cache logging operations for a file.
 *                  - Does not close the log file.
 *                  - Logging must have been set up via the fapl.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fstop_mdc_logging(hid_t file_id)
{
    H5VL_object_t       *vol_obj;             /* File info */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Sanity check */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hid_t identifier is not a file ID");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_FILE_STOP_MDC_LOGGING;
    vol_cb_args.args    = NULL;

    /* Call mdc logging function */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_LOGGING, FAIL, "unable to stop mdc logging");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fstop_mdc_logging() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_mdc_logging_status
 *
 * Purpose:     Get the logging flags. is_enabled determines if logging was
 *              set up via the fapl. is_currently_logging determines if
 *              log messages are being recorded at this time.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_mdc_logging_status(hid_t file_id, hbool_t *is_enabled /*out*/, hbool_t *is_currently_logging /*out*/)
{
    H5VL_object_t                   *vol_obj;             /* File info */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", file_id, is_enabled, is_currently_logging);

    /* Sanity check */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hid_t identifier is not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.get_mdc_logging_status.is_enabled           = is_enabled;
    file_opt_args.get_mdc_logging_status.is_currently_logging = is_currently_logging;
    vol_cb_args.op_type                                       = H5VL_NATIVE_FILE_GET_MDC_LOGGING_STATUS;
    vol_cb_args.args                                          = &file_opt_args;

    /* Call mdc logging function */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_LOGGING, FAIL, "unable to get logging status");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_mdc_logging_status() */

/*-------------------------------------------------------------------------
 * Function:    H5Fset_libver_bounds
 *
 * Purpose:     Set to a different low and high bounds while a file is open.
 *              This public routine is introduced in place of
 *              H5Fset_latest_format() starting release 1.10.2.
 *              See explanation for H5Fset_latest_format() in H5Fdeprec.c.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fset_libver_bounds(hid_t file_id, H5F_libver_t low, H5F_libver_t high)
{
    H5VL_object_t                   *vol_obj;             /* File as VOL object           */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value 				*/

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iFvFv", file_id, low, high);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "not a file ID");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(file_id) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    file_opt_args.set_libver_bounds.low  = low;
    file_opt_args.set_libver_bounds.high = high;
    vol_cb_args.op_type                  = H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS;
    vol_cb_args.args                     = &file_opt_args;

    /* Set the library's version bounds */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set library version bounds");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fset_libver_bounds() */

/*-------------------------------------------------------------------------
 * Function:    H5Fformat_convert (Internal)
 *
 * Purpose:     Downgrade the superblock version to v2 and
 *              downgrade persistent file space to non-persistent
 *              for 1.8 library.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fformat_convert(hid_t file_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* File */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "file_id parameter is not a valid file identifier");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(file_id) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_FILE_FORMAT_CONVERT;
    vol_cb_args.args    = NULL;

    /* Convert the format */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTCONVERT, FAIL, "can't convert file format");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fformat_convert() */

/*-------------------------------------------------------------------------
 * Function:    H5Freset_page_buffering_stats
 *
 * Purpose:     Resets statistics for the page buffer layer.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Freset_page_buffering_stats(hid_t file_id)
{
    H5VL_object_t       *vol_obj;             /* File to reset stats on */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", file_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_FILE_RESET_PAGE_BUFFERING_STATS;
    vol_cb_args.args    = NULL;

    /* Reset the statistics */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't reset stats for page buffering");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Freset_page_buffering_stats() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_page_buffering_stats
 *
 * Purpose:     Retrieves statistics for the page buffer layer.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_page_buffering_stats(hid_t file_id, unsigned accesses[2] /*out*/, unsigned hits[2] /*out*/,
                            unsigned misses[2] /*out*/, unsigned evictions[2] /*out*/,
                            unsigned bypasses[2] /*out*/)
{
    H5VL_object_t                   *vol_obj;             /* File object */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "ixxxxx", file_id, accesses, hits, misses, evictions, bypasses);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a file ID");
    if (NULL == accesses || NULL == hits || NULL == misses || NULL == evictions || NULL == bypasses)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL input parameters for stats");

    /* Set up VOL callback arguments */
    file_opt_args.get_page_buffering_stats.accesses  = accesses;
    file_opt_args.get_page_buffering_stats.hits      = hits;
    file_opt_args.get_page_buffering_stats.misses    = misses;
    file_opt_args.get_page_buffering_stats.evictions = evictions;
    file_opt_args.get_page_buffering_stats.bypasses  = bypasses;
    vol_cb_args.op_type                              = H5VL_NATIVE_FILE_GET_PAGE_BUFFERING_STATS;
    vol_cb_args.args                                 = &file_opt_args;

    /* Get the statistics */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't retrieve stats for page buffering");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_page_buffering_stats() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_mdc_image_info
 *
 * Purpose:     Retrieves the image_addr and image_len for the cache image in the file.
 *              image_addr:  --base address of the on disk metadata cache image
 *                           --HADDR_UNDEF if no cache image
 *              image_len:   --size of the on disk metadata cache image
 *                           --zero if no cache image
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_mdc_image_info(hid_t file_id, haddr_t *image_addr /*out*/, hsize_t *image_len /*out*/)
{
    H5VL_object_t                   *vol_obj;             /* File info */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", file_id, image_addr, image_len);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hid_t identifier is not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.get_mdc_image_info.addr = image_addr;
    file_opt_args.get_mdc_image_info.len  = image_len;
    vol_cb_args.op_type                   = H5VL_NATIVE_FILE_GET_MDC_IMAGE_INFO;
    vol_cb_args.args                      = &file_opt_args;

    /* Go get the address and size of the cache image */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't retrieve cache image info");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_mdc_image_info() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_eoa
 *
 * Purpose:     Gets the address of the first byte after the last
 *              allocated memory in the file.
 *              (See H5FDget_eoa() in H5FD.c)
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_eoa(hid_t file_id, haddr_t *eoa /*out*/)
{
    H5VL_object_t *vol_obj;             /* File info */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, eoa);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hid_t identifier is not a file ID");

    /* Only do work if valid pointer to fill in */
    if (eoa) {
        H5VL_optional_args_t             vol_cb_args;   /* Arguments to VOL callback */
        H5VL_native_file_optional_args_t file_opt_args; /* Arguments for optional operation */

        /* Set up VOL callback arguments */
        file_opt_args.get_eoa.eoa = eoa;
        vol_cb_args.op_type       = H5VL_NATIVE_FILE_GET_EOA;
        vol_cb_args.args          = &file_opt_args;

        /* Retrieve the EOA for the file */
        if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to get EOA");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5Fincrement_filesize
 *
 * Purpose:     Set the EOA for the file to the maximum of (EOA, EOF) + increment
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fincrement_filesize(hid_t file_id, hsize_t increment)
{
    H5VL_object_t                   *vol_obj;             /* File info */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ih", file_id, increment);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "hid_t identifier is not a file ID");

    /* Set up VOL callback arguments */
    file_opt_args.increment_filesize.increment = increment;
    vol_cb_args.op_type                        = H5VL_NATIVE_FILE_INCR_FILESIZE;
    vol_cb_args.args                           = &file_opt_args;

    /* Increment the file size */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "unable to increment file size");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fincrement_filesize() */

/*-------------------------------------------------------------------------
 * Function:    H5Fget_dset_no_attrs_hint
 *
 * Purpose:     Get the file-level setting to create minimized dataset object headers.
 *              Result is stored at pointer `minimize`.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_dset_no_attrs_hint(hid_t file_id, hbool_t *minimize /*out*/)
{
    H5VL_object_t                   *vol_obj;             /* File info */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", file_id, minimize);

    /* Check args */
    if (NULL == minimize)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "out pointer 'minimize' cannot be NULL");
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Set up VOL callback arguments */
    file_opt_args.get_min_dset_ohdr_flag.minimize = minimize;
    vol_cb_args.op_type                           = H5VL_NATIVE_FILE_GET_MIN_DSET_OHDR_FLAG;
    vol_cb_args.args                              = &file_opt_args;

    /* Get the dataset object header minimum size flag */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "unable to set file's dataset header minimization flag");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fget_dset_no_attrs_hint */

/*-------------------------------------------------------------------------
 * Function:    H5Fset_dset_no_attrs_hint
 *
 * Purpose:     Set the file-level setting to create minimized dataset object
 *              headers.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
herr_t
H5Fset_dset_no_attrs_hint(hid_t file_id, hbool_t minimize)
{
    H5VL_object_t                   *vol_obj;             /* File info */
    H5VL_optional_args_t             vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", file_id, minimize);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid file identifier");

    /* Set up VOL callback arguments */
    file_opt_args.set_min_dset_ohdr_flag.minimize = minimize;
    vol_cb_args.op_type                           = H5VL_NATIVE_FILE_SET_MIN_DSET_OHDR_FLAG;
    vol_cb_args.args                              = &file_opt_args;

    /* Set the 'minimize dataset object headers flag' */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "unable to set file's dataset header minimization flag");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Fset_dset_no_attrs_hint */
