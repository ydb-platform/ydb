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
 * Created:     H5O.c
 *
 * Purpose:     Public object header routines
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
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5Fprivate.h"  /* File access                              */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5Opkg.h"      /* Object headers                           */

#include "H5VLnative_private.h" /* Native VOL connector                     */

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

/* Helper routines for sync/async API calls */
static hid_t  H5O__open_api_common(hid_t loc_id, const char *name, hid_t lapl_id, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static hid_t  H5O__open_by_idx_api_common(hid_t loc_id, const char *group_name, H5_index_t idx_type,
                                          H5_iter_order_t order, hsize_t n, hid_t lapl_id, void **token_ptr,
                                          H5VL_object_t **_vol_obj_ptr);
static herr_t H5O__get_info_by_name_api_common(hid_t loc_id, const char *name, H5O_info2_t *oinfo /*out*/,
                                               unsigned fields, hid_t lapl_id, void **token_ptr,
                                               H5VL_object_t **_vol_obj_ptr);
static herr_t H5O__copy_api_common(hid_t src_loc_id, const char *src_name, hid_t dst_loc_id,
                                   const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static herr_t H5O__flush_api_common(hid_t obj_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static herr_t H5O__refresh_api_common(hid_t oid, void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static htri_t H5O__close_check_type(hid_t object_id);

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
 * Function:    H5O__open_api_common
 *
 * Purpose:     This is the common function for opening an object
 *
 * Return:      Success:    An open object identifier
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5O__open_api_common(hid_t loc_id, const char *name, hid_t lapl_id, void **token_ptr,
                     H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5I_type_t        opened_type;
    void             *opened_obj = NULL;
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID;

    FUNC_ENTER_PACKAGE

    /* Check args */

    /* name is checked in this H5VL_setup_name_args() */
    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, name, false, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Open the object */
    if (NULL == (opened_obj = H5VL_object_open(*vol_obj_ptr, &loc_params, &opened_type,
                                               H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open object");

    /* Get an atom for the object */
    if ((ret_value = H5VL_register(opened_type, opened_obj, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to atomize object handle");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__open_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Oopen
 *
 * Purpose:     Opens an object within an HDF5 file.
 *
 *              This function opens an object in the same way that H5Gopen2,
 *              H5Topen2, and H5Dopen2 do. However, H5Oopen doesn't require
 *              the type of object to be known beforehand. This can be
 *              useful in user-defined links, for instance, when only a
 *              path is known.
 *
 *              The opened object should be closed again with H5Oclose
 *              or H5Gclose, H5Tclose, or H5Dclose.
 *
 * Return:      Success:    An open object identifier
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Oopen(hid_t loc_id, const char *name, hid_t lapl_id)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "i*si", loc_id, name, lapl_id);

    /* Open the object synchronously */
    if ((ret_value = H5O__open_api_common(loc_id, name, lapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to synchronously open object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oopen() */

/*-------------------------------------------------------------------------
 * Function:    H5Oopen_async
 *
 * Purpose:     Asynchronous version of H5Oopen
 *
 * Return:      Success:    An open object identifier
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Oopen_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
              hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the object asynchronously */
    if ((ret_value = H5O__open_api_common(loc_id, name, lapl_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to asynchronously open object");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, lapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_OHDR, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on object ID");
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        }

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oopen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5O__open_by_idx_api_common
 *
 * Purpose:     This is the common function for opening an object within an index
 *
 * Return:      Success:    An open object identifier
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5O__open_by_idx_api_common(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                            hsize_t n, hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5I_type_t        opened_type;
    void             *opened_obj = NULL;
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID;

    FUNC_ENTER_PACKAGE

    /* Check args */
    /* group_name, idx_type, order are checked in H5VL_setup_idx-args() */
    /* Set up object access arguments */
    if (H5VL_setup_idx_args(loc_id, group_name, idx_type, order, n, false, lapl_id, vol_obj_ptr,
                            &loc_params) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Open the object */
    if (NULL == (opened_obj = H5VL_object_open(*vol_obj_ptr, &loc_params, &opened_type,
                                               H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open object");

    /* Get an ID for the object */
    if ((ret_value = H5VL_register(opened_type, opened_obj, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register object handle");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__open_by_idx_api_common() */

/*-------------------------------------------------------------------------
 * Function:	H5Oopen_by_idx
 *
 * Purpose:	Opens an object within an HDF5 file, according to the offset
 *              within an index.
 *
 *              This function opens an object in the same way that H5Gopen,
 *              H5Topen, and H5Dopen do. However, H5Oopen doesn't require
 *              the type of object to be known beforehand. This can be
 *              useful in user-defined links, for instance, when only a
 *              path is known.
 *
 *              The opened object should be closed again with H5Oclose
 *              or H5Gclose, H5Tclose, or H5Dclose.
 *
 * Return:	Success:	An open object identifier
 *		Failure:	H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Oopen_by_idx(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
               hid_t lapl_id)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE6("i", "i*sIiIohi", loc_id, group_name, idx_type, order, n, lapl_id);

    /* Open the object synchronously */
    if ((ret_value =
             H5O__open_by_idx_api_common(loc_id, group_name, idx_type, order, n, lapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to synchronously open object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oopen_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5Oopen_by_idx_async
 *
 * Purpose:     Asynchronous version of H5Oopen_by_idx
 *
 * Return:      Success:    An open object identifier
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Oopen_by_idx_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                     const char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                     hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE10("i", "*s*sIui*sIiIohii", app_file, app_func, app_line, loc_id, group_name, idx_type, order, n,
              lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the object asynchronously */
    if ((ret_value = H5O__open_by_idx_api_common(loc_id, group_name, idx_type, order, n, lapl_id, token_ptr,
                                                 &vol_obj)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to asynchronously open object");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIui*sIiIohii", app_file, app_func, app_line, loc_id, group_name, idx_type, order, n, lapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_OHDR, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on object ID");
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        }

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oopen_by_idx_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Oopen_by_token
 *
 * Purpose:     Same as H5Oopen_by_addr, but uses VOL-independent tokens.
 *
 * Return:      Success:    An open object identifier
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Oopen_by_token(hid_t loc_id, H5O_token_t token)
{
    H5VL_object_t    *vol_obj;                     /* Object of loc_id */
    H5I_type_t        vol_obj_type = H5I_BADID;    /* Object type of loc_id */
    H5I_type_t        opened_type;                 /* Opened object type */
    void             *opened_obj = NULL;           /* Opened object */
    H5VL_loc_params_t loc_params;                  /* Location parameters */
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "ik", loc_id, token);

    /* Check args */
    if (H5O_IS_TOKEN_UNDEF(token))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "can't open H5O_TOKEN_UNDEF");

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(loc_id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    loc_params.type                        = H5VL_OBJECT_BY_TOKEN;
    loc_params.loc_data.loc_by_token.token = &token;
    loc_params.obj_type                    = vol_obj_type;

    /* Open the object */
    if (NULL == (opened_obj = H5VL_object_open(vol_obj, &loc_params, &opened_type, H5P_DATASET_XFER_DEFAULT,
                                               H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open object");

    /* Register the object's ID */
    if ((ret_value = H5VL_register(opened_type, opened_obj, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register object handle");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oopen_by_token() */

/*-------------------------------------------------------------------------
 * Function:    H5O__copy_api_common
 *
 * Purpose:     This is the common function for copying an object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__copy_api_common(hid_t src_loc_id, const char *src_name, hid_t dst_loc_id, const char *dst_name,
                     hid_t ocpypl_id, hid_t lcpl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    /* dst_id */
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params2;

    /* src_id */
    H5VL_object_t    *vol_obj1 = NULL; /* object of src_id */
    H5VL_loc_params_t loc_params1;

    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!src_name || !*src_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no source name specified");
    if (!dst_name || !*dst_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no destination name specified");

    /* Get correct property lists */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;
    else if (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not link creation property list");

    /* Get object copy property list */
    if (H5P_DEFAULT == ocpypl_id)
        ocpypl_id = H5P_OBJECT_COPY_DEFAULT;
    else if (true != H5P_isa_class(ocpypl_id, H5P_OBJECT_COPY))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not object copy property list");

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Setup and check args */
    if (H5VL_setup_loc_args(src_loc_id, &vol_obj1, &loc_params1) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* get the object */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object(dst_loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    loc_params2.type     = H5VL_OBJECT_BY_SELF;
    loc_params2.obj_type = H5I_get_type(dst_loc_id);

    /* Copy the object */
    if (H5VL_object_copy(vol_obj1, &loc_params1, src_name, *vol_obj_ptr, &loc_params2, dst_name, ocpypl_id,
                         lcpl_id, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to copy object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__copy_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Ocopy
 *
 * Purpose:     Copy an object (group or dataset) to destination location
 *              within a file or cross files. PLIST_ID is a property list
 *              which is used to pass user options and properties to the
 *              copy. The name, dst_name, must not already be taken by some
 *              other object in the destination group.
 *
 *              H5Ocopy() will fail if the name of the destination object
 *                  exists in the destination group.  For example,
 *                  H5Ocopy(fid_src, "/dset", fid_dst, "/dset", ...)
 *                  will fail if "/dset" exists in the destination file
 *
 *              OPTIONS THAT HAVE BEEN IMPLEMENTED.
 *                  H5O_COPY_SHALLOW_HIERARCHY_FLAG
 *                      If this flag is specified, only immediate members of
 *                      the group are copied. Otherwise (default), it will
 *                      recursively copy all objects below the group
 *                  H5O_COPY_EXPAND_SOFT_LINK_FLAG
 *                      If this flag is specified, it will copy the objects
 *                      pointed by the soft links. Otherwise (default), it
 *                      will copy the soft link as they are
 *                  H5O_COPY_WITHOUT_ATTR_FLAG
 *                      If this flag is specified, it will copy object without
 *                      copying attributes. Otherwise (default), it will
 *                      copy object along with all its attributes
 *                  H5O_COPY_EXPAND_REFERENCE_FLAG
 *                      1) Copy object between two different files:
 *                          When this flag is specified, it will copy objects that
 *                          are pointed by the references and update the values of
 *                          references in the destination file.  Otherwise (default)
 *                          the values of references in the destination will set to
 *                          zero
 *                          The current implementation does not handle references
 *                          inside of other datatype structure. For example, if
 *                          a member of compound datatype is reference, H5Ocopy()
 *                          will copy that field as it is. It will not set the
 *                          value to zero as default is used nor copy the object
 *                          pointed by that field the flag is set
 *                      2) Copy object within the same file:
 *                          This flag does not have any effect to the H5Ocopy().
 *                          Datasets or attributes of references are copied as they
 *                          are, i.e. values of references of the destination object
 *                          are the same as the values of the source object
 *
 *              OPTIONS THAT MAY APPLY TO COPY IN THE FUTURE.
 *                  H5O_COPY_EXPAND_EXT_LINK_FLAG
 *                      If this flag is specified, it will expand the external links
 *                      into new objects, Otherwise (default), it will keep external
 *                      links as they are (default)
 *
 *              PROPERTIES THAT MAY APPLY TO COPY IN FUTURE
 *                  Change data layout such as chunk size
 *                  Add filter such as data compression.
 *                  Add an attribute to the copied object(s) that say the  date/time
 *                      for the copy or other information about the source file.
 *
 *              The intermediate group creation property should be passed in
 *              using the lcpl instead of the ocpypl.
 *
 * Usage:      H5Ocopy(src_loc_id, src_name, dst_loc_id, dst_name, ocpypl_id, lcpl_id)
 *             hid_t src_loc_id         IN: Source file or group identifier.
 *             const char *src_name     IN: Name of the source object to be copied
 *             hid_t dst_loc_id         IN: Destination file or group identifier
 *             const char *dst_name     IN: Name of the destination object
 *             hid_t ocpypl_id          IN: Properties which apply to the copy
 *             hid_t lcpl_id            IN: Properties which apply to the new hard link
 *
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ocopy(hid_t src_loc_id, const char *src_name, hid_t dst_loc_id, const char *dst_name, hid_t ocpypl_id,
        hid_t lcpl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*si*sii", src_loc_id, src_name, dst_loc_id, dst_name, ocpypl_id, lcpl_id);

    /* To copy an object synchronously */
    if (H5O__copy_api_common(src_loc_id, src_name, dst_loc_id, dst_name, ocpypl_id, lcpl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to synchronously copy object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ocopy() */

/*-------------------------------------------------------------------------
 * Function:    H5Ocopy_async
 *
 * Purpose:     Asynchronous version of H5Ocopy
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ocopy_async(const char *app_file, const char *app_func, unsigned app_line, hid_t src_loc_id,
              const char *src_name, hid_t dst_loc_id, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id,
              hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIui*si*siii", app_file, app_func, app_line, src_loc_id, src_name, dst_loc_id,
              dst_name, ocpypl_id, lcpl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* To copy an object asynchronously */
    if (H5O__copy_api_common(src_loc_id, src_name, dst_loc_id, dst_name, ocpypl_id, lcpl_id, token_ptr,
                             &vol_obj) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to asynchronously copy object");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIui*si*siii", app_file, app_func, app_line, src_loc_id, src_name, dst_loc_id, dst_name, ocpypl_id, lcpl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Ocopy_async() */

/*-------------------------------------------------------------------------
 * Function:    H5O__flush_api_common
 *
 * Purpose:     This is the common function for flushing an object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__flush_api_common(hid_t obj_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_object_specific_args_t vol_cb_args;          /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;           /* Location parameters for object access */
    herr_t                      ret_value = SUCCEED;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Setup and check args */
    if (H5VL_setup_loc_args(obj_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type           = H5VL_OBJECT_FLUSH;
    vol_cb_args.args.flush.obj_id = obj_id;

    /* Flush the object */
    if (H5VL_object_specific(*vol_obj_ptr, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to flush object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__flush_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Oflush
 *
 * Purpose:     Flushes all buffers associated with an object to disk.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oflush(hid_t obj_id)
{
    herr_t ret_value = SUCCEED; /* Return value     */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", obj_id);

    /* To flush an object synchronously */
    if (H5O__flush_api_common(obj_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to synchronously flush object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oflush() */

/*-------------------------------------------------------------------------
 * Function:    H5Oflush_async
 *
 * Purpose:     Asynchronous version of H5Oflush
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oflush_async(const char *app_file, const char *app_func, unsigned app_line, hid_t obj_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, obj_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Flush an object asynchronously */
    if (H5O__flush_api_common(obj_id, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTFLUSH, FAIL, "unable to asynchronously flush object");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, obj_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Oflush_async() */

/*-------------------------------------------------------------------------
 * Function:    H5O__refresh_api_common
 *
 * Purpose:     This is the common function for refreshing an object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__refresh_api_common(hid_t oid, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_object_specific_args_t vol_cb_args;          /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;           /* Location parameters for object access */
    herr_t                      ret_value = SUCCEED;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Setup and check args */
    if (H5VL_setup_loc_args(oid, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_OBJECT_REFRESH;
    vol_cb_args.args.refresh.obj_id = oid;

    /* Refresh the object */
    if (H5VL_object_specific(*vol_obj_ptr, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "unable to refresh object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__refresh_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Orefresh
 *
 * Purpose:     Refreshes all buffers associated with an object.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Orefresh(hid_t oid)
{
    herr_t ret_value = SUCCEED; /* Return value     */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", oid);

    /* To refresh an object synchronously */
    if (H5O__refresh_api_common(oid, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "unable to synchronously refresh object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Orefresh() */

/*-------------------------------------------------------------------------
 * Function:    H5Orefresh_async
 *
 * Purpose:     Asynchronous version of H5Orefresh
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Orefresh_async(const char *app_file, const char *app_func, unsigned app_line, hid_t oid, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, oid, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Refresh an object asynchronously */
    if (H5O__refresh_api_common(oid, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "unable to asynchronously refresh object");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, oid, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Orefresh_async() */

/*-------------------------------------------------------------------------
 * Function:	H5Olink
 *
 * Purpose:	Creates a hard link from NEW_NAME to the object specified
 *		by OBJ_ID using properties defined in the Link Creation
 *              Property List LCPL.
 *
 *		This function should be used to link objects that have just
 *              been created.
 *
 *		NEW_NAME is interpreted relative to
 *		NEW_LOC_ID, which is either a file ID or a
 *		group ID.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Olink(hid_t obj_id, hid_t new_loc_id, const char *new_name, hid_t lcpl_id, hid_t lapl_id)
{
    H5VL_object_t          *vol_obj1 = NULL; /* object of obj_id */
    H5VL_object_t          *vol_obj2 = NULL; /* object of new_loc_id */
    H5VL_object_t           tmp_vol_obj;     /* Temporary object */
    H5VL_link_create_args_t vol_cb_args;     /* Arguments to VOL callback */
    H5VL_loc_params_t       new_loc_params;
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ii*sii", obj_id, new_loc_id, new_name, lcpl_id, lapl_id);

    /* Check arguments */
    if (new_loc_id == H5L_SAME_LOC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                    "cannot use H5L_SAME_LOC when only one location is specified");
    if (!new_name || !*new_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
/* Avoid compiler warning on 32-bit machines */
#if H5_SIZEOF_SIZE_T > H5_SIZEOF_INT32_T
    if (strlen(new_name) > H5L_MAX_LINK_NAME_LEN)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "name too long");
#endif /* H5_SIZEOF_SIZE_T > H5_SIZEOF_INT32_T */
    if (lcpl_id != H5P_DEFAULT && (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a link creation property list");

    /* Get the link creation property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, obj_id, true) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up new location struct */
    new_loc_params.type                         = H5VL_OBJECT_BY_NAME;
    new_loc_params.obj_type                     = H5I_get_type(new_loc_id);
    new_loc_params.loc_data.loc_by_name.name    = new_name;
    new_loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Get the first location object */
    if (NULL == (vol_obj1 = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (H5L_SAME_LOC != new_loc_id)
        /* get the location object */
        if (NULL == (vol_obj2 = H5VL_vol_object(new_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Make sure that the VOL connectors are the same */
    if (vol_obj1 && vol_obj2) {
        int same_connector = 0;

        /* Check if both objects are associated with the same VOL connector */
        if (H5VL_cmp_connector_cls(&same_connector, vol_obj1->connector->cls, vol_obj2->connector->cls) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCOMPARE, FAIL, "can't compare connector classes");
        if (same_connector)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                        "Objects are accessed through different VOL connectors and can't be linked");
    } /* end if */

    /* Construct a temporary VOL object */
    tmp_vol_obj.data      = vol_obj2->data;
    tmp_vol_obj.connector = vol_obj1->connector;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                            = H5VL_LINK_CREATE_HARD;
    vol_cb_args.args.hard.curr_obj                 = vol_obj1->data;
    vol_cb_args.args.hard.curr_loc_params.type     = H5VL_OBJECT_BY_SELF;
    vol_cb_args.args.hard.curr_loc_params.obj_type = H5I_get_type(obj_id);

    /* Create a link to the object */
    if (H5VL_link_create(&vol_cb_args, &tmp_vol_obj, &new_loc_params, lcpl_id, lapl_id,
                         H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCREATE, FAIL, "unable to create link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Olink() */

/*-------------------------------------------------------------------------
 * Function:	H5Oincr_refcount
 *
 * Purpose:	Warning! This function is EXTREMELY DANGEROUS!
 *              Improper use can lead to FILE CORRUPTION, INACCESSIBLE DATA,
 *              and other VERY BAD THINGS!
 *
 *              This function increments the "hard link" reference count
 *              for an object. It should be used when a user-defined link
 *              that references an object by address is created. When the
 *              link is deleted, H5Odecr_refcount should be used.
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oincr_refcount(hid_t object_id)
{
    H5VL_object_t              *vol_obj;     /* Object of loc_id */
    H5VL_object_specific_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;  /* Location parameters for object access */
    herr_t                      ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", object_id);

    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(object_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(object_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(object_id) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_CHANGE_REF_COUNT;
    vol_cb_args.args.change_rc.delta = 1;

    /* Change the object's reference count */
    if (H5VL_object_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, FAIL, "modifying object link count failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5O_incr_refcount() */

/*-------------------------------------------------------------------------
 * Function:	H5Odecr_refcount
 *
 * Purpose:	Warning! This function is EXTREMELY DANGEROUS!
 *              Improper use can lead to FILE CORRUPTION, INACCESSIBLE DATA,
 *              and other VERY BAD THINGS!
 *
 *              This function decrements the "hard link" reference count
 *              for an object. It should be used when user-defined links
 *              that reference an object by address are deleted, and only
 *              after H5Oincr_refcount has already been used.
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Odecr_refcount(hid_t object_id)
{
    H5VL_object_t              *vol_obj;             /* Object of loc_id */
    H5VL_object_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;          /* Location parameters for object access */
    herr_t                      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", object_id);

    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(object_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(object_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(object_id) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_CHANGE_REF_COUNT;
    vol_cb_args.args.change_rc.delta = -1;

    /* Change the object's reference count */
    if (H5VL_object_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, FAIL, "modifying object link count failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Odecr_refcount() */

/*-------------------------------------------------------------------------
 * Function:    H5Oexists_by_name
 *
 * Purpose:     Determine if a linked-to object exists
 *
 * Return:      Success:    true/false
 *              Failure:    FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Oexists_by_name(hid_t loc_id, const char *name, hid_t lapl_id)
{
    H5VL_object_t              *vol_obj;            /* Object of loc_id */
    H5VL_object_specific_args_t vol_cb_args;        /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;         /* Location parameters for object access */
    bool                        obj_exists = false; /* Whether object exists */
    htri_t                      ret_value  = FAIL;  /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("t", "i*si", loc_id, name, lapl_id);

    /* Check args */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be an empty string");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set the location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type            = H5VL_OBJECT_EXISTS;
    vol_cb_args.args.exists.exists = &obj_exists;

    /* Check if the object exists */
    if (H5VL_object_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to determine if '%s' exists", name);

    /* Set return value */
    ret_value = (htri_t)obj_exists;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oexists_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_info3
 *
 * Purpose:     Retrieve information about an object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oget_info3(hid_t loc_id, H5O_info2_t *oinfo /*out*/, unsigned fields)
{
    H5VL_object_t         *vol_obj;     /* Object of loc_id */
    H5VL_object_get_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixIu", loc_id, oinfo, fields);

    /* Check args */
    if (!oinfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "oinfo parameter cannot be NULL");
    if (fields & ~H5O_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* Set location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_GET_INFO;
    vol_cb_args.args.get_info.oinfo  = oinfo;
    vol_cb_args.args.get_info.fields = fields;

    /* Retrieve the object's information */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get data model info for object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_info3() */

/*-------------------------------------------------------------------------
 * Function:    H5O__get_info_by_name_api_common
 *
 * Purpose:     This is the common function for retrieving information
 *              about an object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__get_info_by_name_api_common(hid_t loc_id, const char *name, H5O_info2_t *oinfo /*out*/, unsigned fields,
                                 hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_object_get_args_t vol_cb_args;               /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;                /* Location parameters for object access */
    herr_t                 ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (!oinfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "oinfo parameter cannot be NULL");
    if (fields & ~H5O_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* "name" is checked in H5VL_setup_name_args() */
    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, name, false, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_GET_INFO;
    vol_cb_args.args.get_info.oinfo  = oinfo;
    vol_cb_args.args.get_info.fields = fields;

    /* Retrieve the object's information */
    if (H5VL_object_get(*vol_obj_ptr, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get data model info for object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__get_info_by_name_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_info_by_name3
 *
 * Purpose:     Retrieve information about an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oget_info_by_name3(hid_t loc_id, const char *name, H5O_info2_t *oinfo /*out*/, unsigned fields,
                     hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*sxIui", loc_id, name, oinfo, fields, lapl_id);

    /* Retrieve object information synchronously */
    if (H5O__get_info_by_name_api_common(loc_id, name, oinfo, fields, lapl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't synchronously retrieve object info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_info_by_name3() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_info_by_name_async
 *
 * Purpose:     Asynchronous version of H5Oget_info_by_name3
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oget_info_by_name_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                          const char *name, H5O_info2_t *oinfo /*out*/, unsigned fields, hid_t lapl_id,
                          hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE9("e", "*s*sIui*sxIuii", app_file, app_func, app_line, loc_id, name, oinfo, fields, lapl_id,
             es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Retrieve group information asynchronously */
    if (H5O__get_info_by_name_api_common(loc_id, name, oinfo, fields, lapl_id, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't asynchronously retrieve object info");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE9(__func__, "*s*sIui*sxIuii", app_file, app_func, app_line, loc_id, name, oinfo, fields, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Oget_info_by_name_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_info_by_idx3
 *
 * Purpose:     Retrieve information about an object, according to
 *              the order of an index.
 *
 * Return:      Success:	Non-negative
 *              Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oget_info_by_idx3(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                    hsize_t n, H5O_info2_t *oinfo /*out*/, unsigned fields, hid_t lapl_id)
{
    H5VL_object_t         *vol_obj;     /* Object of loc_id */
    H5VL_object_get_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*sIiIohxIui", loc_id, group_name, idx_type, order, n, oinfo, fields, lapl_id);

    /* Check args */
    if (!group_name || !*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!oinfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no info struct");
    if (fields & ~H5O_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = group_name;
    loc_params.loc_data.loc_by_idx.idx_type = idx_type;
    loc_params.loc_data.loc_by_idx.order    = order;
    loc_params.loc_data.loc_by_idx.n        = n;
    loc_params.loc_data.loc_by_idx.lapl_id  = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_GET_INFO;
    vol_cb_args.args.get_info.oinfo  = oinfo;
    vol_cb_args.args.get_info.fields = fields;

    /* Retrieve the object's information */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get data model info for object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_info_by_idx3() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_native_info
 *
 * Purpose:     Retrieve native file format information about an object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oget_native_info(hid_t loc_id, H5O_native_info_t *oinfo /*out*/, unsigned fields)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    herr_t                             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixIu", loc_id, oinfo, fields);

    /* Check args */
    if (!oinfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "oinfo parameter cannot be NULL");
    if (fields & ~H5O_NATIVE_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* Set location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    obj_opt_args.get_native_info.fields = fields;
    obj_opt_args.get_native_info.ninfo  = oinfo;
    vol_cb_args.op_type                 = H5VL_NATIVE_OBJECT_GET_NATIVE_INFO;
    vol_cb_args.args                    = &obj_opt_args;

    /* Retrieve the object's information */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get native file format info for object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_native_info() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_native_info_by_name
 *
 * Purpose:     Retrieve native file format information about an object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oget_native_info_by_name(hid_t loc_id, const char *name, H5O_native_info_t *oinfo /*out*/, unsigned fields,
                           hid_t lapl_id)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    herr_t                             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*sxIui", loc_id, name, oinfo, fields, lapl_id);

    /* Check args */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be an empty string");
    if (!oinfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "oinfo parameter cannot be NULL");
    if (fields & ~H5O_NATIVE_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Fill out location struct */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    obj_opt_args.get_native_info.fields = fields;
    obj_opt_args.get_native_info.ninfo  = oinfo;
    vol_cb_args.op_type                 = H5VL_NATIVE_OBJECT_GET_NATIVE_INFO;
    vol_cb_args.args                    = &obj_opt_args;

    /* Retrieve the object's information */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get native file format info for object: '%s'", name);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_native_info_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_native_info_by_idx
 *
 * Purpose:     Retrieve native file format information about an object,
 *              according to the order of an index.
 *
 * Return:      Success:	Non-negative
 *              Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oget_native_info_by_idx(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                          hsize_t n, H5O_native_info_t *oinfo /*out*/, unsigned fields, hid_t lapl_id)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    herr_t                             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*sIiIohxIui", loc_id, group_name, idx_type, order, n, oinfo, fields, lapl_id);

    /* Check args */
    if (!group_name || !*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!oinfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no info struct");
    if (fields & ~H5O_NATIVE_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = group_name;
    loc_params.loc_data.loc_by_idx.idx_type = idx_type;
    loc_params.loc_data.loc_by_idx.order    = order;
    loc_params.loc_data.loc_by_idx.n        = n;
    loc_params.loc_data.loc_by_idx.lapl_id  = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    obj_opt_args.get_native_info.fields = fields;
    obj_opt_args.get_native_info.ninfo  = oinfo;
    vol_cb_args.op_type                 = H5VL_NATIVE_OBJECT_GET_NATIVE_INFO;
    vol_cb_args.args                    = &obj_opt_args;

    /* Retrieve the object's information */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get native file format info for object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_native_info_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5Oset_comment
 *
 * Purpose:     Gives the specified object a comment.  The COMMENT string
 *              should be a null terminated string.  An object can have only
 *              one comment at a time.  Passing NULL for the COMMENT argument
 *              will remove the comment property from the object.
 *
 * Note:        Deprecated in favor of using attributes on objects
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oset_comment(hid_t obj_id, const char *comment)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    herr_t                             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", obj_id, comment);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(obj_id) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Fill in location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(obj_id);

    /* Set up VOL callback arguments */
    obj_opt_args.set_comment.comment = comment;
    vol_cb_args.op_type              = H5VL_NATIVE_OBJECT_SET_COMMENT;
    vol_cb_args.args                 = &obj_opt_args;

    /* (Re)set the object's comment */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set comment for object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oset_comment() */

/*-------------------------------------------------------------------------
 * Function:    H5Oset_comment_by_name
 *
 * Purpose:     Gives the specified object a comment.  The COMMENT string
 *              should be a null terminated string.  An object can have only
 *              one comment at a time.  Passing NULL for the COMMENT argument
 *              will remove the comment property from the object.
 *
 * Note:        Deprecated in favor of using attributes on objects
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oset_comment_by_name(hid_t loc_id, const char *name, const char *comment, hid_t lapl_id)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    herr_t                             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*s*si", loc_id, name, comment, lapl_id);

    /* Check args */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Fill in location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    obj_opt_args.set_comment.comment = comment;
    vol_cb_args.op_type              = H5VL_NATIVE_OBJECT_SET_COMMENT;
    vol_cb_args.args                 = &obj_opt_args;

    /* (Re)set the object's comment */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set comment for object: '%s'", name);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oset_comment_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_comment
 *
 * Purpose:     Retrieve comment for an object.
 *
 * Return:      Success:    Number of bytes in the comment excluding the
 *                          null terminator. Zero if the object has no
 *                          comment.
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Oget_comment(hid_t obj_id, char *comment /*out*/, size_t bufsize)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    size_t                             comment_len = 0;  /* Length of comment string */
    ssize_t                            ret_value   = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE3("Zs", "ixz", obj_id, comment, bufsize);

    /* Get the object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid location identifier");

    /* Set fields in the location struct */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(obj_id);

    /* Set up VOL callback arguments */
    obj_opt_args.get_comment.buf         = comment;
    obj_opt_args.get_comment.buf_size    = bufsize;
    obj_opt_args.get_comment.comment_len = &comment_len;
    vol_cb_args.op_type                  = H5VL_NATIVE_OBJECT_GET_COMMENT;
    vol_cb_args.args                     = &obj_opt_args;

    /* Retrieve the object's comment */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, (-1), "can't get comment for object");

    /* Set return value */
    ret_value = (ssize_t)comment_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_comment() */

/*-------------------------------------------------------------------------
 * Function:    H5Oget_comment_by_name
 *
 * Purpose:     Retrieve comment for an object.
 *
 * Return:      Success:    Number of bytes in the comment excluding the
 *                          null terminator. Zero if the object has no
 *                          comment.
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Oget_comment_by_name(hid_t loc_id, const char *name, char *comment /*out*/, size_t bufsize, hid_t lapl_id)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    size_t                             comment_len = 0;  /* Length of comment string */
    ssize_t                            ret_value   = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE5("Zs", "i*sxzi", loc_id, name, comment, bufsize, lapl_id);

    /* Check args */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "no name");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, (-1), "can't set access property list info");

    /* Fill in location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid location identifier");

    /* Set up VOL callback arguments */
    obj_opt_args.get_comment.buf         = comment;
    obj_opt_args.get_comment.buf_size    = bufsize;
    obj_opt_args.get_comment.comment_len = &comment_len;
    vol_cb_args.op_type                  = H5VL_NATIVE_OBJECT_GET_COMMENT;
    vol_cb_args.args                     = &obj_opt_args;

    /* Retrieve the object's comment */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, (-1), "can't get comment for object: '%s'", name);

    /* Set return value */
    ret_value = (ssize_t)comment_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oget_comment_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Ovisit3
 *
 * Purpose:     Recursively visit an object and all the objects reachable
 *              from it.  If the starting object is a group, all the objects
 *              linked to from that group will be visited.  Links within
 *              each group are visited according to the order within the
 *              specified index (unless the specified index does not exist for
 *              a particular group, then the "name" index is used).
 *
 *              NOTE: Soft links and user-defined links are ignored during
 *              this operation.
 *
 *              NOTE: Each _object_ reachable from the initial group will only
 *              be visited once.  If multiple hard links point to the same
 *              object, the first link to the object's path (according to the
 *              iteration index and iteration order given) will be used to in
 *              the callback about the object.
 *
 *              NOTE: Add a parameter "fields" to indicate selection of
 *              object info to be retrieved to the callback "op".
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ovisit3(hid_t obj_id, H5_index_t idx_type, H5_iter_order_t order, H5O_iterate2_t op, void *op_data,
          unsigned fields)
{
    H5VL_object_t              *vol_obj;     /* Object of loc_id */
    H5VL_object_specific_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;  /* Location parameters for object access */
    herr_t                      ret_value;   /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iIiIoOI*xIu", obj_id, idx_type, order, op, op_data, fields);

    /* Check args */
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no callback operator specified");
    if (fields & ~H5O_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(obj_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_OBJECT_VISIT;
    vol_cb_args.args.visit.idx_type = idx_type;
    vol_cb_args.args.visit.order    = order;
    vol_cb_args.args.visit.op       = op;
    vol_cb_args.args.visit.op_data  = op_data;
    vol_cb_args.args.visit.fields   = fields;

    /* Visit the objects */
    if ((ret_value = H5VL_object_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                          H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "object iteration failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ovisit3() */

/*-------------------------------------------------------------------------
 * Function:    H5Ovisit_by_name3
 *
 * Purpose:     Recursively visit an object and all the objects reachable
 *              from it.  If the starting object is a group, all the objects
 *              linked to from that group will be visited.  Links within
 *              each group are visited according to the order within the
 *              specified index (unless the specified index does not exist for
 *              a particular group, then the "name" index is used).
 *
 *              NOTE: Soft links and user-defined links are ignored during
 *              this operation.
 *
 *              NOTE: Each _object_ reachable from the initial group will only
 *              be visited once.  If multiple hard links point to the same
 *              object, the first link to the object's path (according to the
 *              iteration index and iteration order given) will be used to in
 *              the callback about the object.
 *
 *              NOTE: Add a parameter "fields" to indicate selection of
 *              object info to be retrieved to the callback "op".
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ovisit_by_name3(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order,
                  H5O_iterate2_t op, void *op_data, unsigned fields, hid_t lapl_id)
{
    H5VL_object_t              *vol_obj;     /* Object of loc_id */
    H5VL_object_specific_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;  /* Location parameters for object access */
    herr_t                      ret_value;   /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*sIiIoOI*xIui", loc_id, obj_name, idx_type, order, op, op_data, fields, lapl_id);

    /* Check args */
    if (!obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "obj_name parameter cannot be NULL");
    if (!*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "obj_name parameter cannot be an empty string");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no callback operator specified");
    if (fields & ~H5O_INFO_ALL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fields");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set location parameters */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = obj_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_OBJECT_VISIT;
    vol_cb_args.args.visit.idx_type = idx_type;
    vol_cb_args.args.visit.order    = order;
    vol_cb_args.args.visit.op       = op;
    vol_cb_args.args.visit.op_data  = op_data;
    vol_cb_args.args.visit.fields   = fields;

    /* Visit the objects */
    if ((ret_value = H5VL_object_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                          H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "object iteration failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ovisit_by_name3() */

/*-------------------------------------------------------------------------
 * Function:    H5O__close_check_type
 *
 * Purpose:     This is the common function to validate an object
 *              before closing it.
 *
 * Return:      true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5O__close_check_type(hid_t object_id)
{
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for closeable object */
    switch (H5I_get_type(object_id)) {
        case H5I_GROUP:
        case H5I_DATATYPE:
        case H5I_DATASET:
        case H5I_MAP:
            if (H5I_object(object_id) == NULL)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid object");
            break;

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_FILE:
        case H5I_DATASPACE:
        case H5I_ATTR:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_EVENTSET:
        case H5I_NTYPES:
        default:
            HGOTO_DONE(false);
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__close_check_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Oclose
 *
 * Purpose:     Close an open file object.
 *
 *              This is the companion to H5Oopen. It is used to close any
 *              open object in an HDF5 file (but not IDs are that not file
 *              objects, such as property lists and dataspaces). It has
 *              the same effect as calling H5Gclose, H5Dclose, or H5Tclose.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oclose(hid_t object_id)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", object_id);

    /* Validate the object type before closing */
    if (H5O__close_check_type(object_id) <= 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "not a valid object");

    if (H5I_dec_app_ref(object_id) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "unable to close object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Oclose() */

/*-------------------------------------------------------------------------
 * Function:    H5Oclose_async
 *
 * Purpose:     Asynchronous version of H5Oclose
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oclose_async(const char *app_file, const char *app_func, unsigned app_line, hid_t object_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    H5VL_t        *connector = NULL;            /* VOL connector */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, object_id, es_id);

    /* Validate the object type before closing */
    if (H5O__close_check_type(object_id) <= 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "not a valid object");

    /* Prepare for possible asynchronous operation */
    if (H5ES_NONE != es_id) {
        /* Get file object's connector */
        if (NULL == (vol_obj = H5VL_vol_object(object_id)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get VOL object for object");

        /* Increase connector's refcount, so it doesn't get closed if closing
         * this object ID closes the file */
        connector = vol_obj->connector;
        H5VL_conn_inc_rc(connector);

        /* Point at token for operation to set up */
        token_ptr = &token;
    } /* end if */

    /* Asynchronously decrement reference count on ID.
     * When it reaches zero the object will be closed.
     */
    if (H5I_dec_app_ref_async(object_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCLOSEFILE, FAIL, "decrementing object ID failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, object_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    if (connector && H5VL_conn_dec_rc(connector) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "can't decrement ref count on connector");

    FUNC_LEAVE_API(ret_value)
} /* end H5Oclose_async() */

/*-------------------------------------------------------------------------
 * Function:    H5O__disable_mdc_flushes
 *
 * Purpose:     Private version of the metadata cache cork function.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__disable_mdc_flushes(H5O_loc_t *oloc)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (H5AC_cork(oloc->file, oloc->addr, H5AC__SET_CORK, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCORK, FAIL, "unable to cork object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__disable_mdc_flushes() */

/*-------------------------------------------------------------------------
 * Function:    H5Odisable_mdc_flushes
 *
 * Purpose:     "Cork" an object, keeping dirty entries associated with the
 *              object in the metadata cache.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Odisable_mdc_flushes(hid_t object_id)
{
    H5VL_object_t       *vol_obj;             /* Object of loc_id */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;          /* Location parameters */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", object_id);

    /* Make sure the ID is a file object */
    if (H5I_is_file_object(object_id) != true)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "ID is not a file object");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(object_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object ID");

    /* Fill in location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(object_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES;
    vol_cb_args.args    = NULL;

    /* Cork the object */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCORK, FAIL, "unable to cork object");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Odisable_mdc_flushes() */

/*-------------------------------------------------------------------------
 * Function:    H5O__enable_mdc_flushes
 *
 * Purpose:     Private version of the metadata cache uncork function.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__enable_mdc_flushes(H5O_loc_t *oloc)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (H5AC_cork(oloc->file, oloc->addr, H5AC__UNCORK, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNCORK, FAIL, "unable to uncork object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__enable_mdc_flushes() */

/*-------------------------------------------------------------------------
 * Function:    H5Oenable_mdc_flushes
 *
 * Purpose:     "Uncork" an object, allowing dirty entries associated with
 *              the object to be flushed.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oenable_mdc_flushes(hid_t object_id)
{
    H5VL_object_t       *vol_obj;             /* Object of loc_id */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;          /* Location parameters */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", object_id);

    /* Make sure the ID is a file object */
    if (H5I_is_file_object(object_id) != true)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "ID is not a file object");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(object_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object ID");

    /* Fill in location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(object_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES;
    vol_cb_args.args    = NULL;

    /* Uncork the object */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNCORK, FAIL, "unable to uncork object");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Oenable_mdc_flushes() */

/*-------------------------------------------------------------------------
 * Function:    H5O__are_mdc_flushes_disabled
 *
 * Purpose:     Private version of cork status getter.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__are_mdc_flushes_disabled(const H5O_loc_t *oloc, hbool_t *are_disabled)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(are_disabled);

    if (H5AC_cork(oloc->file, oloc->addr, H5AC__GET_CORKED, are_disabled) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to retrieve object's cork status");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__are_mdc_flushes_disabled() */

/*-------------------------------------------------------------------------
 * Function:    H5Oare_mdc_flushes_disabled
 *
 * Purpose:     Retrieve the object's "cork" status in the parameter "are_disabled":
 *                  true if mdc flushes for the object is disabled
 *                  false if mdc flushes for the object is not disabled
 *
 *              Return error if the parameter "are_disabled" is not supplied
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Oare_mdc_flushes_disabled(hid_t object_id, bool *are_disabled)
{
    H5VL_object_t                     *vol_obj;             /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args;        /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;          /* Location parameters */
    herr_t                             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*!", object_id, are_disabled);

    /* Sanity check */
    if (!are_disabled)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location from ID");

    /* Make sure the ID is a file object */
    if (H5I_is_file_object(object_id) != true)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "ID is not a file object");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(object_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object ID");

    /* Fill in location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(object_id);

    /* Set up VOL callback arguments */
    obj_opt_args.are_mdc_flushes_disabled.flag = are_disabled;
    vol_cb_args.op_type                        = H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED;
    vol_cb_args.args                           = &obj_opt_args;

    /* Get the cork status */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to retrieve object's cork status");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Oare_mdc_flushes_disabled() */

/*---------------------------------------------------------------------------
 * Function:    H5Otoken_cmp
 *
 * Purpose:     Compares two VOL connector object tokens
 *
 * Note:        Both object tokens must be from the same VOL connector class
 *
 * Return:      Success:    Non-negative, with *cmp_value set to positive if
 *                          token1 is greater than token2, negative if token2
 *                          is greater than token1 and zero if token1 and
 *                          token2 are equal.
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5Otoken_cmp(hid_t loc_id, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    H5VL_object_t *vol_obj;             /* VOL object for ID */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*k*k*Is", loc_id, token1, token2, cmp_value);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (NULL == cmp_value)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid cmp_value pointer");

    /* Compare the two tokens */
    if (H5VL_token_cmp(vol_obj, token1, token2, cmp_value) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOMPARE, FAIL, "object token comparison failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Otoken_cmp() */

/*---------------------------------------------------------------------------
 * Function:    H5Otoken_to_str
 *
 * Purpose:     Serialize a connector's object token into a string
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5Otoken_to_str(hid_t loc_id, const H5O_token_t *token, char **token_str)
{
    H5VL_object_t *vol_obj;             /* VOL object for ID */
    H5I_type_t     vol_obj_type;        /* VOL object's type */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*k**s", loc_id, token, token_str);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (NULL == token)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid token pointer");
    if (NULL == token_str)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid token string pointer");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(loc_id)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get underlying VOL object type");

    /* Serialize the token */
    if (H5VL_token_to_str(vol_obj, vol_obj_type, token, token_str) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSERIALIZE, FAIL, "object token serialization failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Otoken_to_str() */

/*---------------------------------------------------------------------------
 * Function:    H5Otoken_from_str
 *
 * Purpose:     Deserialize a string into a connector object token
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5Otoken_from_str(hid_t loc_id, const char *token_str, H5O_token_t *token)
{
    H5VL_object_t *vol_obj;             /* VOL object for ID */
    H5I_type_t     vol_obj_type;        /* VOL object's type */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*s*k", loc_id, token_str, token);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (NULL == token)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid token pointer");
    if (NULL == token_str)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid token string pointer");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(loc_id)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get underlying VOL object type");

    /* Deserialize the token */
    if (H5VL_token_from_str(vol_obj, vol_obj_type, token_str, token) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNSERIALIZE, FAIL, "object token deserialization failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Otoken_from_str() */
