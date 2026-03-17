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
 * Created:     H5Rdeprec.c
 *
 * Purpose:     Deprecated functions from the H5R interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Rmodule.h" /* This source code file is part of the H5R module */

/***********/
/* Headers */
/***********/
/* Public headers needed by this file */
#include "H5Ppublic.h" /* Property lists                           */

/* Private headers needed by this file */
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Oprivate.h"  /* Object headers                           */
#include "H5Rpkg.h"      /* References                               */
#include "H5Sprivate.h"  /* Dataspaces                               */

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
static herr_t H5R__encode_token_region_compat(H5F_t *f, const H5O_token_t *obj_token, size_t token_size,
                                              H5S_t *space, unsigned char *buf, size_t *nalloc);
static herr_t H5R__decode_token_compat(H5VL_object_t *vol_obj, H5I_type_t type, H5R_type_t ref_type,
                                       const unsigned char *buf, H5O_token_t *obj_token);

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
 * Function:    H5R__decode_token_compat
 *
 * Purpose:     Decode an object token. (native only)
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__decode_token_compat(H5VL_object_t *vol_obj, H5I_type_t type, H5R_type_t ref_type,
                         const unsigned char *buf, H5O_token_t *obj_token)
{
    hid_t                 file_id      = H5I_INVALID_HID; /* File ID for region reference */
    H5VL_object_t        *vol_obj_file = NULL;
    H5VL_file_cont_info_t cont_info    = {H5VL_CONTAINER_INFO_VERSION, 0, 0, 0};
    H5VL_file_get_args_t  vol_cb_args; /* Arguments to VOL callback */
    herr_t                ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifndef NDEBUG
    {
        bool is_native_vol_obj = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native_vol_obj);
    }
#endif /* NDEBUG */

    /* Get the file for the object */
    if ((file_id = H5F_get_file_id(vol_obj, type, false)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Retrieve VOL object */
    if (NULL == (vol_obj_file = H5VL_vol_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                 = H5VL_FILE_GET_CONT_INFO;
    vol_cb_args.args.get_cont_info.info = &cont_info;

    /* Get container info */
    if (H5VL_file_get(vol_obj_file, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "unable to get container info");

    if (ref_type == H5R_OBJECT1) {
        size_t buf_size = H5R_OBJ_REF_BUF_SIZE;

        /* Get object address */
        if (H5R__decode_token_obj_compat(buf, &buf_size, obj_token, cont_info.token_size) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "unable to get object token");
    } /* end if */
    else {
        size_t buf_size = H5R_DSET_REG_REF_BUF_SIZE;
        H5F_t *f        = NULL;

        /* Retrieve file from VOL object */
        if (NULL == (f = (H5F_t *)H5VL_object_data((const H5VL_object_t *)vol_obj_file)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

        /* Get object address */
        if (H5R__decode_token_region_compat(f, buf, &buf_size, obj_token, cont_info.token_size, NULL) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "unable to get object address");
    } /* end else */

done:
    if (file_id != H5I_INVALID_HID && H5I_dec_ref(file_id) < 0)
        HDONE_ERROR(H5E_REFERENCE, H5E_CANTDEC, FAIL, "unable to decrement refcount on file");
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__decode_token_compat() */

/*-------------------------------------------------------------------------
 * Function:    H5R__encode_token_region_compat
 *
 * Purpose:     Encode dataset selection and insert data into heap
 *              (native only).
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__encode_token_region_compat(H5F_t *f, const H5O_token_t *obj_token, size_t token_size, H5S_t *space,
                                unsigned char *buf, size_t *nalloc)
{
    size_t         buf_size;
    unsigned char *data      = NULL;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(obj_token);
    assert(token_size);
    assert(space);
    assert(nalloc);

    /* Get required buffer size */
    if (H5R__encode_heap(f, NULL, &buf_size, NULL, (size_t)0) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    if (buf && *nalloc >= buf_size) {
        ssize_t  data_size;
        uint8_t *p;

        /* Pass the correct encoding version for the selection depending on the
         * file libver bounds, this is later retrieved in H5S hyper encode */
        H5CX_set_libver_bounds(f);

        /* Zero the heap ID out, may leak heap space if user is re-using
         * reference and doesn't have garbage collection turned on
         */
        memset(buf, 0, buf_size);

        /* Get the amount of space required to serialize the selection */
        if ((data_size = H5S_SELECT_SERIAL_SIZE(space)) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTINIT, FAIL,
                        "Invalid amount of space for serializing selection");

        /* Increase buffer size to allow for the dataset token */
        data_size += (hssize_t)token_size;

        /* Allocate the space to store the serialized information */
        H5_CHECK_OVERFLOW(data_size, hssize_t, size_t);
        if (NULL == (data = (uint8_t *)H5MM_malloc((size_t)data_size)))
            HGOTO_ERROR(H5E_REFERENCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Serialize information for dataset OID into heap buffer */
        p = (uint8_t *)data;
        H5MM_memcpy(p, obj_token, token_size);
        p += token_size;

        /* Serialize the selection into heap buffer */
        if (H5S_SELECT_SERIALIZE(space, &p) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "Unable to serialize selection");

        /* Write to heap */
        if (H5R__encode_heap(f, buf, nalloc, data, (size_t)data_size) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    }
    *nalloc = buf_size;

done:
    H5MM_free(data);
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5R__encode_token_region_compat() */

#ifndef H5_NO_DEPRECATED_SYMBOLS
/*-------------------------------------------------------------------------
 * Function:    H5Rget_obj_type1
 *
 * Purpose:     Retrieves the type of the object that a reference points to.
 *
 * Return:      Success:    Object type (as defined in H5Gpublic.h)
 *              Failure:    H5G_UNKNOWN
 *
 *-------------------------------------------------------------------------
 */
H5G_obj_t
H5Rget_obj_type1(hid_t id, H5R_type_t ref_type, const void *ref)
{
    H5VL_object_t         *vol_obj      = NULL;                    /* Object of loc_id */
    H5I_type_t             vol_obj_type = H5I_BADID;               /* Object type of loc_id */
    H5VL_object_get_args_t vol_cb_args;                            /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;                             /* Location parameters */
    H5O_token_t            obj_token = {0};                        /* Object token */
    const unsigned char   *buf       = (const unsigned char *)ref; /* Reference buffer */
    H5O_type_t             obj_type  = H5O_TYPE_UNKNOWN;           /* Type of the referenced object */
    bool                   is_native_vol_obj; /* Whether the native VOL connector is in use */
    H5G_obj_t              ret_value;         /* Return value */

    FUNC_ENTER_API(H5G_UNKNOWN)
    H5TRACE3("Go", "iRt*x", id, ref_type, ref);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5G_UNKNOWN, "invalid reference pointer");
    if (ref_type != H5R_OBJECT1 && ref_type != H5R_DATASET_REGION1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5G_UNKNOWN, "invalid reference type");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5G_UNKNOWN, "invalid location identifier");

    /* Check if using native VOL connector */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL,
                    "can't determine if VOL object is native connector object");

    /* Must use native VOL connector for this operation */
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_REFERENCE, H5E_VOL, FAIL,
                    "H5Rget_obj_type1 is only meant to be used with the native VOL connector");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5G_UNKNOWN, "invalid location identifier");

    /* Get object token */
    if (H5R__decode_token_compat(vol_obj, vol_obj_type, ref_type, buf, &obj_token) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, H5G_UNKNOWN, "unable to get object token");

    /* Set location parameters */
    loc_params.type                        = H5VL_OBJECT_BY_TOKEN;
    loc_params.loc_data.loc_by_token.token = &obj_token;
    loc_params.obj_type                    = vol_obj_type;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_OBJECT_GET_TYPE;
    vol_cb_args.args.get_type.obj_type = &obj_type;

    /* Retrieve object's type */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, H5G_UNKNOWN, "can't retrieve object type");

    /* Set return value */
    ret_value = H5G_map_obj_type(obj_type);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Rget_obj_type1() */

/*-------------------------------------------------------------------------
 * Function:    H5Rdereference1
 *
 * Purpose:     Given a reference to some object, open that object and return
 *              an ID for that object.
 *
 * Return:      Success:    Valid ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Rdereference1(hid_t obj_id, H5R_type_t ref_type, const void *ref)
{
    H5VL_object_t       *vol_obj      = NULL;                     /* Object of loc_id */
    H5I_type_t           vol_obj_type = H5I_BADID;                /* Object type of loc_id */
    H5VL_loc_params_t    loc_params;                              /* Location parameters */
    H5O_token_t          obj_token = {0};                         /* Object token */
    H5I_type_t           opened_type;                             /* Opened object type */
    void                *opened_obj = NULL;                       /* Opened object */
    const unsigned char *buf        = (const unsigned char *)ref; /* Reference buffer */
    bool                 is_native_vol_obj;           /* Whether the native VOL connector is in use */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "iRt*x", obj_id, ref_type, ref);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid reference pointer");
    if (ref_type != H5R_OBJECT1 && ref_type != H5R_DATASET_REGION1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid reference type");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Check if using native VOL connector */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL,
                    "can't determine if VOL object is native connector object");

    /* Must use native VOL connector for this operation */
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_REFERENCE, H5E_VOL, FAIL,
                    "H5Rdereference1 is only meant to be used with the native VOL connector");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(obj_id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Get object token */
    if (H5R__decode_token_compat(vol_obj, vol_obj_type, ref_type, buf, &obj_token) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, H5I_INVALID_HID, "unable to get object token");

    /* Set location parameters */
    loc_params.type                        = H5VL_OBJECT_BY_TOKEN;
    loc_params.loc_data.loc_by_token.token = &obj_token;
    loc_params.obj_type                    = vol_obj_type;

    /* Dereference */
    if (NULL == (opened_obj = H5VL_object_open(vol_obj, &loc_params, &opened_type, H5P_DATASET_XFER_DEFAULT,
                                               H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open object by token");

    /* Register object */
    if ((ret_value = H5VL_register(opened_type, opened_obj, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register object handle");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Rdereference1() */

#endif /* H5_NO_DEPRECATED_SYMBOLS */

/*-------------------------------------------------------------------------
 * Function:    H5Rcreate
 *
 * Purpose:     Creates a particular type of reference specified with REF_TYPE,
 *              in the space pointed to by REF. The LOC_ID and NAME are used to
 *              locate the object pointed to and the SPACE_ID is used to choose
 *              the region pointed to (for Dataset Region references).
 *
 * Return:      Non-negative on success / negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Rcreate(void *ref, hid_t loc_id, const char *name, H5R_type_t ref_type, hid_t space_id)
{
    H5VL_object_t              *vol_obj      = NULL;      /* Object of loc_id */
    H5I_type_t                  vol_obj_type = H5I_BADID; /* Object type of loc_id */
    H5VL_object_specific_args_t obj_spec_vol_cb_args;     /* Arguments to VOL callback */
    H5VL_loc_params_t           loc_params;               /* Location parameters */
    H5O_token_t                 obj_token = {0};          /* Object token */
    H5VL_file_cont_info_t       cont_info = {H5VL_CONTAINER_INFO_VERSION, 0, 0, 0};
    H5VL_file_get_args_t        file_get_vol_cb_args;           /* Arguments to VOL callback */
    hid_t                       file_id      = H5I_INVALID_HID; /* File ID for region reference */
    void                       *vol_obj_file = NULL;
    bool           is_native_vol_obj = false; /* Whether the src file is using the native VOL connector */
    unsigned char *buf               = (unsigned char *)ref; /* Return reference pointer */
    herr_t         ret_value         = SUCCEED;              /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*xi*sRti", ref, loc_id, name, ref_type, space_id);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid reference pointer");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name given");
    if (ref_type != H5R_OBJECT1 && ref_type != H5R_DATASET_REGION1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid reference type");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Check if using native VOL connector */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't query if file uses native VOL connector");

    /* Must use native VOL connector for this operation */
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_REFERENCE, H5E_VOL, FAIL, "must use native VOL connector to create reference");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(loc_id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set location parameters */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    loc_params.obj_type                     = vol_obj_type;

    /* Set up VOL callback arguments */
    obj_spec_vol_cb_args.op_type               = H5VL_OBJECT_LOOKUP;
    obj_spec_vol_cb_args.args.lookup.token_ptr = &obj_token;

    /* Get the object token */
    if (H5VL_object_specific(vol_obj, &loc_params, &obj_spec_vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                             H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "unable to retrieve object token");

    /* Get the file for the object */
    if ((file_id = H5F_get_file_id(vol_obj, vol_obj_type, false)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Retrieve VOL object */
    if (NULL == (vol_obj_file = H5VL_vol_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    file_get_vol_cb_args.op_type                 = H5VL_FILE_GET_CONT_INFO;
    file_get_vol_cb_args.args.get_cont_info.info = &cont_info;

    /* Get container info */
    if (H5VL_file_get(vol_obj_file, &file_get_vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "unable to get container info");

    /* Create reference */
    if (ref_type == H5R_OBJECT1) {
        size_t buf_size = H5R_OBJ_REF_BUF_SIZE;

        if ((ret_value = H5R__encode_token_obj_compat((const H5O_token_t *)&obj_token, cont_info.token_size,
                                                      buf, &buf_size)) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, "unable to encode object reference");
    } /* end if */
    else {
        H5F_t *f        = NULL;
        H5S_t *space    = NULL; /* Pointer to dataspace containing region */
        size_t buf_size = H5R_DSET_REG_REF_BUF_SIZE;

        /* Retrieve space */
        if (space_id == H5I_INVALID_HID)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "reference region dataspace id must be valid");
        if (NULL == (space = (struct H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

        /* Retrieve file from VOL object */
        if (NULL == (f = (H5F_t *)H5VL_object_data((const H5VL_object_t *)vol_obj_file)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

        /* Encode dataset region */
        if ((ret_value = H5R__encode_token_region_compat(f, (const H5O_token_t *)&obj_token,
                                                         cont_info.token_size, space, buf, &buf_size)) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, "unable to encode region reference");
    } /* end else */

done:
    if (file_id != H5I_INVALID_HID && H5I_dec_ref(file_id) < 0)
        HDONE_ERROR(H5E_REFERENCE, H5E_CANTDEC, FAIL, "unable to decrement refcount on file");
    FUNC_LEAVE_API(ret_value)
} /* end H5Rcreate() */

/*-------------------------------------------------------------------------
 * Function:    H5Rget_obj_type2
 *
 * Purpose:     Given a reference to some object, this function returns the
 *              type of object pointed to.
 *
 * Return:      Non-negative on success / negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Rget_obj_type2(hid_t id, H5R_type_t ref_type, const void *ref, H5O_type_t *obj_type /*out*/)
{
    H5VL_object_t         *vol_obj      = NULL;                            /* Object of loc_id */
    H5I_type_t             vol_obj_type = H5I_BADID;                       /* Object type of loc_id */
    H5VL_object_get_args_t vol_cb_args;                                    /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;                                     /* Location parameters */
    H5O_token_t            obj_token         = {0};                        /* Object token */
    const unsigned char   *buf               = (const unsigned char *)ref; /* Reference pointer */
    bool                   is_native_vol_obj = false;   /* Whether the native VOL connector is in use */
    herr_t                 ret_value         = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iRt*xx", id, ref_type, ref, obj_type);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid reference pointer");
    if (ref_type != H5R_OBJECT1 && ref_type != H5R_DATASET_REGION1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid reference type");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Check if using native VOL connector */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL,
                    "can't determine if VOL object is native connector object");

    /* Must use native VOL connector for this operation */
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_REFERENCE, H5E_VOL, FAIL,
                    "H5Rget_obj_type2 is only meant to be used with the native VOL connector");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Get object token */
    if (H5R__decode_token_compat(vol_obj, vol_obj_type, ref_type, buf, &obj_token) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "unable to get object token");

    /* Set location parameters */
    loc_params.type                        = H5VL_OBJECT_BY_TOKEN;
    loc_params.loc_data.loc_by_token.token = &obj_token;
    loc_params.obj_type                    = vol_obj_type;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_OBJECT_GET_TYPE;
    vol_cb_args.args.get_type.obj_type = obj_type;

    /* Retrieve object's type */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't retrieve object type");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Rget_obj_type2() */

/*-------------------------------------------------------------------------
 * Function:    H5Rdereference2
 *
 * Purpose:     Given a reference to some object, open that object and return
 *              an ID for that object.
 *
 * Return:      Success:    Valid ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Rdereference2(hid_t obj_id, hid_t oapl_id, H5R_type_t ref_type, const void *ref)
{
    H5VL_object_t       *vol_obj      = NULL;                            /* Object of loc_id */
    H5I_type_t           vol_obj_type = H5I_BADID;                       /* Object type of loc_id */
    H5VL_loc_params_t    loc_params;                                     /* Location parameters */
    H5O_token_t          obj_token = {0};                                /* Object token */
    H5I_type_t           opened_type;                                    /* Opened object type */
    void                *opened_obj        = NULL;                       /* Opened object */
    const unsigned char *buf               = (const unsigned char *)ref; /* Reference pointer */
    bool                 is_native_vol_obj = false;           /* Whether the native VOL connector is in use */
    hid_t                ret_value         = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE4("i", "iiRt*x", obj_id, oapl_id, ref_type, ref);

    /* Check args */
    if (oapl_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a property list");
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid reference pointer");
    if (ref_type != H5R_OBJECT1 && ref_type != H5R_DATASET_REGION1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid reference type");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&oapl_id, H5P_CLS_DACC, obj_id, false) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid file identifier");

    /* Check if using native VOL connector */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL,
                    "can't determine if VOL object is native connector object");

    /* Must use native VOL connector for this operation */
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_REFERENCE, H5E_VOL, FAIL,
                    "H5Rdereference2 is only meant to be used with the native VOL connector");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(obj_id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Get object token */
    if (H5R__decode_token_compat(vol_obj, vol_obj_type, ref_type, buf, &obj_token) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, H5I_INVALID_HID, "unable to get object token");

    /* Set location parameters */
    loc_params.type                        = H5VL_OBJECT_BY_TOKEN;
    loc_params.loc_data.loc_by_token.token = &obj_token;
    loc_params.obj_type                    = vol_obj_type;

    /* Open object by token */
    if (NULL == (opened_obj = H5VL_object_open(vol_obj, &loc_params, &opened_type, H5P_DATASET_XFER_DEFAULT,
                                               H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open object by token");

    /* Register object */
    if ((ret_value = H5VL_register(opened_type, opened_obj, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register object handle");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Rdereference2() */

/*-------------------------------------------------------------------------
 * Function:    H5Rget_region
 *
 * Purpose:     Given a reference to some object, creates a copy of the dataset
 *              pointed to's dataspace and defines a selection in the copy
 *              which is the region pointed to.
 *
 * Return:      Success:    Valid ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Rget_region(hid_t id, H5R_type_t ref_type, const void *ref)
{
    H5VL_object_t        *vol_obj      = NULL;      /* Object of loc_id */
    H5I_type_t            vol_obj_type = H5I_BADID; /* Object type of loc_id */
    void                 *vol_obj_file = NULL;      /* VOL file */
    H5VL_file_cont_info_t cont_info    = {H5VL_CONTAINER_INFO_VERSION, 0, 0, 0};
    H5VL_file_get_args_t  vol_cb_args;                           /* Arguments to VOL callback */
    H5F_t                *f        = NULL;                       /* Native file */
    size_t                buf_size = H5R_DSET_REG_REF_BUF_SIZE;  /* Reference buffer size */
    H5S_t                *space    = NULL;                       /* Dataspace object */
    hid_t                 file_id  = H5I_INVALID_HID;            /* File ID for region reference */
    const unsigned char  *buf      = (const unsigned char *)ref; /* Reference pointer */
    bool  is_native_vol_obj        = false; /* Whether the src file is using the native VOL connector */
    hid_t ret_value;                        /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "iRt*x", id, ref_type, ref);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid reference pointer");
    if (ref_type != H5R_DATASET_REGION1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid reference type");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid file identifier");

    /* Check if using native VOL connector */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, H5I_INVALID_HID,
                    "can't query if file uses native VOL connector");

    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_REFERENCE, H5E_VOL, FAIL,
                    "H5Rget_region is only meant to be used with the native VOL connector");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Get the file for the object */
    if ((file_id = H5F_get_file_id(vol_obj, vol_obj_type, false)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a file or file object");

    /* Retrieve VOL object */
    if (NULL == (vol_obj_file = H5VL_vol_object(file_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                 = H5VL_FILE_GET_CONT_INFO;
    vol_cb_args.args.get_cont_info.info = &cont_info;

    /* Get container info */
    if (H5VL_file_get(vol_obj_file, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, H5I_INVALID_HID, "unable to get container info");

    /* Retrieve file from VOL object */
    if (NULL == (f = (H5F_t *)H5VL_object_data((const H5VL_object_t *)vol_obj_file)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid VOL object");

    /* Get the dataspace with the correct region selected */
    if (H5R__decode_token_region_compat(f, buf, &buf_size, NULL, cont_info.token_size, &space) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, H5I_INVALID_HID, "unable to get dataspace");

    /* Register */
    if ((ret_value = H5I_register(H5I_DATASPACE, space, true)) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register dataspace ID");

done:
    if (file_id != H5I_INVALID_HID && H5I_dec_ref(file_id) < 0)
        HDONE_ERROR(H5E_REFERENCE, H5E_CANTDEC, H5I_INVALID_HID, "unable to decrement refcount on file");
    FUNC_LEAVE_API(ret_value)
} /* end H5Rget_region1() */

/*-------------------------------------------------------------------------
 * Function:    H5Rget_name
 *
 * Purpose:     Given a reference to some object, determine a path to the
 *              object referenced in the file.
 *
 * Return:      Success:    Non-negative length of the path
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Rget_name(hid_t id, H5R_type_t ref_type, const void *ref, char *name /*out*/, size_t size)
{
    H5VL_object_t         *vol_obj      = NULL;                       /* Object of loc_id */
    H5I_type_t             vol_obj_type = H5I_BADID;                  /* Object type of loc_id */
    H5VL_object_get_args_t vol_cb_args;                               /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;                                /* Location parameters */
    H5O_token_t            obj_token    = {0};                        /* Object token */
    const unsigned char   *buf          = (const unsigned char *)ref; /* Reference pointer */
    size_t                 obj_name_len = 0;                          /* Length of object's name */
    ssize_t                ret_value    = -1;                         /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE5("Zs", "iRt*xxz", id, ref_type, ref, name, size);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "invalid reference pointer");
    if (ref_type != H5R_OBJECT1 && ref_type != H5R_DATASET_REGION1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "invalid reference type");

    /* Get the VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid file identifier");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(id)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid location identifier");

    /* Get object token */
    if (H5R__decode_token_compat(vol_obj, vol_obj_type, ref_type, buf, &obj_token) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, (-1), "unable to get object token");

    /* Set location parameters */
    loc_params.type                        = H5VL_OBJECT_BY_TOKEN;
    loc_params.loc_data.loc_by_token.token = &obj_token;
    loc_params.obj_type                    = vol_obj_type;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_OBJECT_GET_NAME;
    vol_cb_args.args.get_name.buf_size = size;
    vol_cb_args.args.get_name.buf      = name;
    vol_cb_args.args.get_name.name_len = &obj_name_len;

    /* Retrieve object's name */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, (-1), "can't retrieve object name");

    /* Set return value */
    ret_value = (ssize_t)obj_name_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Rget_name() */
