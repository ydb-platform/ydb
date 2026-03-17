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
 * Purpose:     The native VOL connector where access is to a single HDF5 file
 *              using HDF5 VFDs.
 */

/****************/
/* Module Setup */
/****************/

#define H5VL_FRIEND /* Suppress error about including H5VLpkg   */

/***********/
/* Headers */
/***********/

#include "H5private.h"  /* Generic Functions                        */
#include "H5Aprivate.h" /* Attributes                               */
#include "H5Dprivate.h" /* Datasets                                 */
#include "H5Eprivate.h" /* Error handling                           */
#include "H5Fprivate.h" /* Files                                    */
#include "H5Gprivate.h" /* Groups                                   */
#include "H5Iprivate.h" /* IDs                                      */
#include "H5Oprivate.h" /* Object headers                           */
#include "H5Pprivate.h" /* Property lists                           */
#include "H5Tprivate.h" /* Datatypes                                */
#include "H5VLpkg.h"    /* Virtual Object Layer                     */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/* The VOL connector identification number */
static hid_t H5VL_NATIVE_ID_g = H5I_INVALID_HID;

/* Prototypes */
static herr_t H5VL__native_term(void);

#define H5VL_NATIVE_CAP_FLAGS                                                                                \
    (H5VL_CAP_FLAG_NATIVE_FILES | H5VL_CAP_FLAG_ATTR_BASIC | H5VL_CAP_FLAG_ATTR_MORE |                       \
     H5VL_CAP_FLAG_DATASET_BASIC | H5VL_CAP_FLAG_DATASET_MORE | H5VL_CAP_FLAG_FILE_BASIC |                   \
     H5VL_CAP_FLAG_FILE_MORE | H5VL_CAP_FLAG_GROUP_BASIC | H5VL_CAP_FLAG_GROUP_MORE |                        \
     H5VL_CAP_FLAG_LINK_BASIC | H5VL_CAP_FLAG_LINK_MORE | H5VL_CAP_FLAG_OBJECT_BASIC |                       \
     H5VL_CAP_FLAG_OBJECT_MORE | H5VL_CAP_FLAG_REF_BASIC | H5VL_CAP_FLAG_REF_MORE | H5VL_CAP_FLAG_OBJ_REF |  \
     H5VL_CAP_FLAG_REG_REF | H5VL_CAP_FLAG_ATTR_REF | H5VL_CAP_FLAG_STORED_DATATYPES |                       \
     H5VL_CAP_FLAG_CREATION_ORDER | H5VL_CAP_FLAG_ITERATE | H5VL_CAP_FLAG_STORAGE_SIZE |                     \
     H5VL_CAP_FLAG_BY_IDX | H5VL_CAP_FLAG_GET_PLIST | H5VL_CAP_FLAG_FLUSH_REFRESH |                          \
     H5VL_CAP_FLAG_EXTERNAL_LINKS | H5VL_CAP_FLAG_HARD_LINKS | H5VL_CAP_FLAG_SOFT_LINKS |                    \
     H5VL_CAP_FLAG_UD_LINKS | H5VL_CAP_FLAG_TRACK_TIMES | H5VL_CAP_FLAG_MOUNT | H5VL_CAP_FLAG_FILTERS |      \
     H5VL_CAP_FLAG_FILL_VALUES)

/* Native VOL connector class struct */
static const H5VL_class_t H5VL_native_cls_g = {
    H5VL_VERSION,          /* VOL class struct version */
    H5VL_NATIVE_VALUE,     /* value        */
    H5VL_NATIVE_NAME,      /* name         */
    H5VL_NATIVE_VERSION,   /* connector version */
    H5VL_NATIVE_CAP_FLAGS, /* capability flags */
    NULL,                  /* initialize   */
    H5VL__native_term,     /* terminate    */
    {
        /* info_cls */
        (size_t)0, /* info size    */
        NULL,      /* info copy    */
        NULL,      /* info compare */
        NULL,      /* info free    */
        NULL,      /* info to str  */
        NULL       /* str to info  */
    },
    {
        /* wrap_cls */
        NULL, /* get_object   */
        NULL, /* get_wrap_ctx */
        NULL, /* wrap_object  */
        NULL, /* unwrap_object */
        NULL  /* free_wrap_ctx */
    },
    {
        /* attribute_cls */
        H5VL__native_attr_create,   /* create       */
        H5VL__native_attr_open,     /* open         */
        H5VL__native_attr_read,     /* read         */
        H5VL__native_attr_write,    /* write        */
        H5VL__native_attr_get,      /* get          */
        H5VL__native_attr_specific, /* specific     */
        H5VL__native_attr_optional, /* optional     */
        H5VL__native_attr_close     /* close        */
    },
    {
        /* dataset_cls */
        H5VL__native_dataset_create,   /* create       */
        H5VL__native_dataset_open,     /* open         */
        H5VL__native_dataset_read,     /* read         */
        H5VL__native_dataset_write,    /* write        */
        H5VL__native_dataset_get,      /* get          */
        H5VL__native_dataset_specific, /* specific     */
        H5VL__native_dataset_optional, /* optional     */
        H5VL__native_dataset_close     /* close        */
    },
    {
        /* datatype_cls */
        H5VL__native_datatype_commit,   /* commit       */
        H5VL__native_datatype_open,     /* open         */
        H5VL__native_datatype_get,      /* get          */
        H5VL__native_datatype_specific, /* specific     */
        NULL,                           /* optional     */
        H5VL__native_datatype_close     /* close        */
    },
    {
        /* file_cls */
        H5VL__native_file_create,   /* create       */
        H5VL__native_file_open,     /* open         */
        H5VL__native_file_get,      /* get          */
        H5VL__native_file_specific, /* specific     */
        H5VL__native_file_optional, /* optional     */
        H5VL__native_file_close     /* close        */
    },
    {
        /* group_cls */
        H5VL__native_group_create,   /* create       */
        H5VL__native_group_open,     /* open         */
        H5VL__native_group_get,      /* get          */
        H5VL__native_group_specific, /* specific     */
        H5VL__native_group_optional, /* optional     */
        H5VL__native_group_close     /* close        */
    },
    {
        /* link_cls */
        H5VL__native_link_create,   /* create       */
        H5VL__native_link_copy,     /* copy         */
        H5VL__native_link_move,     /* move         */
        H5VL__native_link_get,      /* get          */
        H5VL__native_link_specific, /* specific     */
        NULL                        /* optional     */
    },
    {
        /* object_cls */
        H5VL__native_object_open,     /* open         */
        H5VL__native_object_copy,     /* copy         */
        H5VL__native_object_get,      /* get          */
        H5VL__native_object_specific, /* specific     */
        H5VL__native_object_optional  /* optional     */
    },
    {
        /* introspect_cls */
        H5VL__native_introspect_get_conn_cls,  /* get_conn_cls */
        H5VL__native_introspect_get_cap_flags, /* get_cap_flags */
        H5VL__native_introspect_opt_query,     /* opt_query    */
    },
    {
        /* request_cls */
        NULL, /* wait         */
        NULL, /* notify       */
        NULL, /* cancel       */
        NULL, /* specific     */
        NULL, /* optional     */
        NULL  /* free         */
    },
    {
        /* blob_cls */
        H5VL__native_blob_put,      /* put */
        H5VL__native_blob_get,      /* get */
        H5VL__native_blob_specific, /* specific */
        NULL                        /* optional */
    },
    {
        /* token_cls */
        H5VL__native_token_cmp,    /* cmp            */
        H5VL__native_token_to_str, /* to_str         */
        H5VL__native_str_to_token  /* from_str       */
    },
    NULL /* optional     */
};

/*-------------------------------------------------------------------------
 * Function:    H5VL_native_register
 *
 * Purpose:     Register the native VOL connector and retrieve an ID for it.
 *
 * Return:      Success:    The ID for the native connector
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VL_native_register(void)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    /* Register the native VOL connector, if it isn't already */
    if (H5I_INVALID_HID == H5VL_NATIVE_ID_g)
        if ((H5VL_NATIVE_ID_g =
                 H5VL__register_connector(&H5VL_native_cls_g, true, H5P_VOL_INITIALIZE_DEFAULT)) < 0)
            HGOTO_ERROR(H5E_VOL, H5E_CANTINSERT, H5I_INVALID_HID, "can't create ID for native VOL connector");

    /* Set return value */
    ret_value = H5VL_NATIVE_ID_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_native_register() */

/*---------------------------------------------------------------------------
 * Function:    H5VL__native_term
 *
 * Purpose:     Shut down the native VOL
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL__native_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VOL ID */
    H5VL_NATIVE_ID_g = H5I_INVALID_HID;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5VL__native_term() */

/*---------------------------------------------------------------------------
 * Function:    H5VL__native_introspect_get_conn_cls
 *
 * Purpose:     Query the connector class.
 *
 * Note:        This routine is in this file so that it can return the address
 *              of the statically declared class struct.
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL__native_introspect_get_conn_cls(void H5_ATTR_UNUSED *obj, H5VL_get_conn_lvl_t H5_ATTR_UNUSED lvl,
                                     const H5VL_class_t **conn_cls)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(conn_cls);

    /* Retrieve the native VOL connector class */
    *conn_cls = &H5VL_native_cls_g;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5VL__native_introspect_get_conn_cls() */

/*---------------------------------------------------------------------------
 * Function:    H5VL__native_introspect_get_cap_flags
 *
 * Purpose:     Query the capability flags for this connector.
 *
 * Note:        This routine is in this file so that it can return the field
 *              from the statically declared class struct.
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL__native_introspect_get_cap_flags(const void H5_ATTR_UNUSED *info, uint64_t *cap_flags)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(cap_flags);

    /* Set the flags from the connector's field */
    *cap_flags = H5VL_native_cls_g.cap_flags;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5VL__native_introspect_get_cap_flags() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_native_get_file_addr_len
 *
 * Purpose:     Convenience function to get a file's address length from a
 *              location ID. Useful when you have to encode/decode addresses
 *              to/from tokens.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_native_get_file_addr_len(hid_t loc_id, size_t *addr_len)
{
    H5I_type_t vol_obj_type = H5I_BADID; /* Object type of loc_id */
    void      *vol_obj      = NULL;      /* VOL Object of loc_id */
    herr_t     ret_value    = SUCCEED;   /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check arguments */
    assert(addr_len);

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(loc_id)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Retrieve underlying VOL object */
    if (NULL == (vol_obj = H5VL_object(loc_id)))
        HGOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Retrieve file address length */
    if (H5VL__native_get_file_addr_len(vol_obj, vol_obj_type, addr_len) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get file address length");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_native_get_file_addr_len() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_get_file_addr_len
 *
 * Purpose:     Convenience function to get a file's address length from a
 *              VOL object. Useful when you have to encode/decode addresses
 *              to/from tokens.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_get_file_addr_len(void *obj, H5I_type_t obj_type, size_t *addr_len)
{
    H5F_t *file      = NULL; /* File struct pointer */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* check arguments */
    assert(obj);
    assert(addr_len);

    /* Retrieve file from the VOL object */
    if (H5VL_native_get_file_struct(obj, obj_type, &file) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "couldn't get file from VOL object");

    /* Get the length of an address in this file */
    *addr_len = H5F_SIZEOF_ADDR(file);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_get_file_addr_len() */

/*-------------------------------------------------------------------------
 * Function:    H5VLnative_addr_to_token
 *
 * Purpose:     Converts a native VOL haddr_t address to an abstract VOL token.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VLnative_addr_to_token(hid_t loc_id, haddr_t addr, H5O_token_t *token)
{
    H5I_type_t vol_obj_type = H5I_BADID; /* Object type of loc_id */
    void      *vol_obj      = NULL;      /* VOL Object of loc_id */
    herr_t     ret_value    = SUCCEED;   /* Return value         */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ia*k", loc_id, addr, token);

    /* Check args */
    if (NULL == token)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "token pointer can't be NULL");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(loc_id)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Retrieve underlying VOL object */
    if (NULL == (vol_obj = H5VL_object(loc_id)))
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get underlying VOL object");

#ifndef NDEBUG
    {
        H5VL_object_t *vol_obj_container;
        bool           is_native_vol_obj;

        /* Get the location object */
        if (NULL == (vol_obj_container = (H5VL_object_t *)H5I_object(loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Make sure that the VOL object is a native connector object */
        if (H5VL_object_is_native(vol_obj_container, &is_native_vol_obj) < 0)
            HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL,
                        "can't determine if VOL object is native connector object");

        assert(is_native_vol_obj && "not a native VOL connector object");
    }
#endif

    /* Convert the haddr_t to an object token */
    if (H5VL_native_addr_to_token(vol_obj, vol_obj_type, addr, token) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTSERIALIZE, FAIL, "couldn't serialize haddr_t into object token");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLnative_addr_to_token() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_native_addr_to_token
 *
 * Purpose:     Converts a native VOL haddr_t address to an abstract VOL token.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_native_addr_to_token(void *obj, H5I_type_t obj_type, haddr_t addr, H5O_token_t *token)
{
    uint8_t *p;
    size_t   addr_len  = 0; /* Size of haddr_t      */
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(obj);
    assert(token);

    /* Get the length of an haddr_t in the file */
    if (H5VL__native_get_file_addr_len(obj, obj_type, &addr_len) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "couldn't get length of haddr_t from VOL object");

    /* Ensure that token is initialized */
    memset(token, 0, sizeof(H5O_token_t));

    /* Encode token */
    p = (uint8_t *)token;
    H5F_addr_encode_len(addr_len, &p, addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_native_addr_to_token() */

/*-------------------------------------------------------------------------
 * Function:    H5VLnative_token_to_addr
 *
 * Purpose:     Converts an abstract VOL token to a native VOL haddr_t address.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VLnative_token_to_addr(hid_t loc_id, H5O_token_t token, haddr_t *addr)
{
    H5I_type_t vol_obj_type = H5I_BADID; /* Object type of loc_id */
    void      *vol_obj      = NULL;      /* VOL Object of loc_id */
    herr_t     ret_value    = SUCCEED;   /* Return value         */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ik*a", loc_id, token, addr);

    /* Check args */
    if (NULL == addr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr pointer can't be NULL");

    /* Get object type */
    if ((vol_obj_type = H5I_get_type(loc_id)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Retrieve underlying VOL object */
    if (NULL == (vol_obj = H5VL_object(loc_id)))
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get underlying VOL object");

#ifndef NDEBUG
    {
        H5VL_object_t *vol_obj_container;
        bool           is_native_vol_obj;

        /* Get the location object */
        if (NULL == (vol_obj_container = (H5VL_object_t *)H5I_object(loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Make sure that the VOL object is a native connector object */
        if (H5VL_object_is_native(vol_obj_container, &is_native_vol_obj) < 0)
            HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL,
                        "can't determine if VOL object is native connector object");

        assert(is_native_vol_obj && "not a native VOL connector object");
    }
#endif

    /* Convert the object token to an haddr_t */
    if (H5VL_native_token_to_addr(vol_obj, vol_obj_type, token, addr) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTUNSERIALIZE, FAIL, "couldn't deserialize object token into haddr_t");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLnative_token_to_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_native_token_to_addr
 *
 * Purpose:     Converts an abstract VOL token to a native VOL haddr_t address.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_native_token_to_addr(void *obj, H5I_type_t obj_type, H5O_token_t token, haddr_t *addr)
{
    const uint8_t *p;
    size_t         addr_len  = 0; /* Size of haddr_t      */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(obj);
    assert(addr);

    /* Get the length of an haddr_t in the file */
    if (H5VL__native_get_file_addr_len(obj, obj_type, &addr_len) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "couldn't get length of haddr_t from VOL object");

    /* Decode token */
    p = (const uint8_t *)&token;
    H5F_addr_decode_len(addr_len, &p, addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL_native_token_to_addr() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_native_get_file_struct
 *
 * Purpose:     Utility routine to get file struct for an object
 *
 * Returns:     SUCCEED/FAIL
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL_native_get_file_struct(void *obj, H5I_type_t type, H5F_t **file)
{
    H5O_loc_t *oloc      = NULL;    /* Object location for ID   */
    herr_t     ret_value = SUCCEED; /* Return value             */

    FUNC_ENTER_NOAPI(FAIL)

    *file = NULL;

    switch (type) {
        case H5I_FILE:
            *file = (H5F_t *)obj;
            break;

        case H5I_GROUP:
            oloc = H5G_oloc((H5G_t *)obj);
            break;

        case H5I_DATATYPE:
            oloc = H5T_oloc((H5T_t *)obj);
            break;

        case H5I_DATASET:
            oloc = H5D_oloc((H5D_t *)obj);
            break;

        case H5I_ATTR:
            oloc = H5A_oloc((H5A_t *)obj);
            break;

        case H5I_MAP:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "maps not supported in native VOL connector");

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_DATASPACE:
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
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");
    } /* end switch */

    /* Set return value for objects (not files) */
    if (oloc)
        *file = oloc->file;

    /* Couldn't find a file struct */
    if (!*file)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "object is not associated with a file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5VL_native_get_file_struct */
