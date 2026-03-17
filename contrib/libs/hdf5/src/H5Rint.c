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

#include "H5Rmodule.h" /* This source code file is part of the H5R module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5HGprivate.h" /* Global Heaps                             */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Oprivate.h"  /* Object headers                           */
#include "H5Rpkg.h"      /* References                               */
#include "H5Sprivate.h"  /* Dataspaces                               */
#include "H5Tprivate.h"  /* Datatypes                                */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

#define H5R_MAX_STRING_LEN (1 << 16) /* Max encoded string length    */

/* Encode macro */
#define H5R_ENCODE(func, val, buf, buf_size, actual, m)                                                      \
    do {                                                                                                     \
        size_t __nalloc = buf_size;                                                                          \
        if (func(val, buf, &__nalloc) < 0)                                                                   \
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, m);                                             \
        if (buf && buf_size >= __nalloc) {                                                                   \
            buf += __nalloc;                                                                                 \
            buf_size -= __nalloc;                                                                            \
        }                                                                                                    \
        actual += __nalloc;                                                                                  \
    } while (0)

#define H5R_ENCODE_VAR(func, var, size, buf, buf_size, actual, m)                                            \
    do {                                                                                                     \
        size_t __nalloc = buf_size;                                                                          \
        if (func(var, size, buf, &__nalloc) < 0)                                                             \
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, m);                                             \
        if (buf && buf_size >= __nalloc) {                                                                   \
            p += __nalloc;                                                                                   \
            buf_size -= __nalloc;                                                                            \
        }                                                                                                    \
        actual += __nalloc;                                                                                  \
    } while (0)

/* Decode macro */
#define H5R_DECODE(func, val, buf, buf_size, actual, m)                                                      \
    do {                                                                                                     \
        size_t __nbytes = buf_size;                                                                          \
        if (func(buf, &__nbytes, val) < 0)                                                                   \
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, m);                                             \
        buf += __nbytes;                                                                                     \
        buf_size -= __nbytes;                                                                                \
        actual += __nbytes;                                                                                  \
    } while (0)

#define H5R_DECODE_VAR(func, var, size, buf, buf_size, actual, m)                                            \
    do {                                                                                                     \
        size_t __nbytes = buf_size;                                                                          \
        if (func(buf, &__nbytes, var, size) < 0)                                                             \
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, m);                                             \
        p += __nbytes;                                                                                       \
        buf_size -= __nbytes;                                                                                \
        actual += __nbytes;                                                                                  \
    } while (0)

/* Debug */
#ifdef H5R_DEBUG
#define H5R_LOG_DEBUG(...)                                                                                   \
    do {                                                                                                     \
        fprintf(stdout, " # %s(): ", __func__);                                                              \
        fprintf(stdout, __VA_ARGS__);                                                                        \
        fprintf(stdout, "\n");                                                                               \
        fflush(stdout);                                                                                      \
    } while (0)
static const char *
H5R__print_token(const H5O_token_t token)
{
    static char string[64];

    /* Print the raw token. */
    snprintf(string, 64, "%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X:%02X",
             (unsigned char)token.__data[15], (unsigned char)token.__data[14],
             (unsigned char)token.__data[13], (unsigned char)token.__data[12],
             (unsigned char)token.__data[11], (unsigned char)token.__data[10], (unsigned char)token.__data[9],
             (unsigned char)token.__data[8], (unsigned char)token.__data[7], (unsigned char)token.__data[6],
             (unsigned char)token.__data[5], (unsigned char)token.__data[4], (unsigned char)token.__data[3],
             (unsigned char)token.__data[2], (unsigned char)token.__data[1], (unsigned char)token.__data[0]);

    return string;
}
#else
#define H5R_LOG_DEBUG(...)                                                                                   \
    do {                                                                                                     \
    } while (0)
#endif

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5R__encode_obj_token(const H5O_token_t *obj_token, size_t token_size, unsigned char *buf,
                                    size_t *nalloc);
static herr_t H5R__decode_obj_token(const unsigned char *buf, size_t *nbytes, H5O_token_t *obj_token,
                                    uint8_t *token_size);
static herr_t H5R__encode_region(H5S_t *space, unsigned char *buf, size_t *nalloc);
static herr_t H5R__decode_region(const unsigned char *buf, size_t *nbytes, H5S_t **space_ptr);
static herr_t H5R__encode_string(const char *string, unsigned char *buf, size_t *nalloc);
static herr_t H5R__decode_string(const unsigned char *buf, size_t *nbytes, char **string_ptr);

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
 * Function:    H5R_init
 *
 * Purpose:     Initialize the interface from some other layer.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
herr_t
H5R_init(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check, if assert fails, H5R_REF_BUF_SIZE must be increased */
    HDcompile_assert(sizeof(H5R_ref_priv_t) <= H5R_REF_BUF_SIZE);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5R__create_object
 *
 * Purpose:     Creates an object reference
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__create_object(const H5O_token_t *obj_token, size_t token_size, H5R_ref_priv_t *ref)
{
    size_t encode_size;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(ref);

    /* Create new reference */
    ref->info.obj.filename = NULL;
    ref->loc_id            = H5I_INVALID_HID;
    ref->type              = (uint8_t)H5R_OBJECT2;
    if (H5R__set_obj_token(ref, obj_token, token_size) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, FAIL, "unable to set object token");

    /* Cache encoding size (assume no external reference) */
    if (H5R__encode(NULL, ref, NULL, &encode_size, 0) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, "unable to determine encoding size");
    ref->encode_size = (uint32_t)encode_size;

    H5R_LOG_DEBUG("Created object reference, %d, filename=%s, obj_addr=%s, encode size=%u",
                  (int)sizeof(H5R_ref_priv_t), ref->info.obj.filename, H5R__print_token(ref->info.obj.token),
                  ref->encode_size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__create_object() */

/*-------------------------------------------------------------------------
 * Function:    H5R__create_region
 *
 * Purpose:     Creates a region reference
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__create_region(const H5O_token_t *obj_token, size_t token_size, H5S_t *space, H5R_ref_priv_t *ref)
{
    size_t encode_size;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(space);
    assert(ref);

    /* Create new reference */
    ref->info.obj.filename = NULL;
    if (NULL == (ref->info.reg.space = H5S_copy(space, false, true)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "unable to copy dataspace");

    ref->loc_id = H5I_INVALID_HID;
    ref->type   = (uint8_t)H5R_DATASET_REGION2;
    if (H5R__set_obj_token(ref, obj_token, token_size) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, FAIL, "unable to set object token");

    /* Cache encoding size (assume no external reference) */
    if (H5R__encode(NULL, ref, NULL, &encode_size, 0) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, "unable to determine encoding size");
    ref->encode_size = (uint32_t)encode_size;

    H5R_LOG_DEBUG("Created region reference, %d, filename=%s, obj_addr=%s, encode size=%u",
                  (int)sizeof(H5R_ref_priv_t), ref->info.obj.filename, H5R__print_token(ref->info.obj.token),
                  ref->encode_size);

done:
    if (ret_value < 0)
        if (ref->info.reg.space) {
            H5S_close(ref->info.reg.space);
            ref->info.reg.space = NULL;
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5R__create_region */

/*-------------------------------------------------------------------------
 * Function:    H5R__create_attr
 *
 * Purpose:     Creates an attribute reference
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__create_attr(const H5O_token_t *obj_token, size_t token_size, const char *attr_name, H5R_ref_priv_t *ref)
{
    size_t encode_size;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(attr_name);
    assert(ref);

    /* Make sure that attribute name is not longer than supported encode size */
    if (strlen(attr_name) > H5R_MAX_STRING_LEN)
        HGOTO_ERROR(H5E_REFERENCE, H5E_ARGS, FAIL, "attribute name too long (%d > %d)",
                    (int)strlen(attr_name), H5R_MAX_STRING_LEN);

    /* Create new reference */
    ref->info.obj.filename = NULL;
    if (NULL == (ref->info.attr.name = strdup(attr_name)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "Cannot copy attribute name");

    ref->loc_id = H5I_INVALID_HID;
    ref->type   = (uint8_t)H5R_ATTR;
    if (H5R__set_obj_token(ref, obj_token, token_size) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, FAIL, "unable to set object token");

    /* Cache encoding size (assume no external reference) */
    if (H5R__encode(NULL, ref, NULL, &encode_size, 0) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, "unable to determine encoding size");
    ref->encode_size = (uint32_t)encode_size;

    H5R_LOG_DEBUG("Created attribute reference, %d, filename=%s, obj_addr=%s, attr name=%s, encode size=%u",
                  (int)sizeof(H5R_ref_priv_t), ref->info.obj.filename, H5R__print_token(ref->info.obj.token),
                  ref->info.attr.name, ref->encode_size);

done:
    if (ret_value < 0) {
        H5MM_xfree(ref->info.attr.name);
        ref->info.attr.name = NULL;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5R__create_attr */

/*-------------------------------------------------------------------------
 * Function:    H5R__destroy
 *
 * Purpose:     Destroy a reference
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__destroy(H5R_ref_priv_t *ref)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(ref != NULL);

    H5R_LOG_DEBUG("Destroying reference, filename=%s, obj_addr=%s, encode size=%u", ref->info.obj.filename,
                  H5R__print_token(ref->info.obj.token), ref->encode_size);

    H5MM_xfree(ref->info.obj.filename);
    ref->info.obj.filename = NULL;

    switch (ref->type) {
        case H5R_OBJECT2:
            break;

        case H5R_DATASET_REGION2:
            if (H5S_close(ref->info.reg.space) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTFREE, FAIL, "Cannot close dataspace");
            ref->info.reg.space = NULL;
            break;

        case H5R_ATTR:
            H5MM_xfree(ref->info.attr.name);
            ref->info.attr.name = NULL;
            break;

        case H5R_OBJECT1:
        case H5R_DATASET_REGION1:
            break;
        case H5R_BADTYPE:
        case H5R_MAXTYPE:
            assert("invalid reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (invalid reference type)");

        default:
            assert("unknown reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (unknown reference type)");
    } /* end switch */

    /* Decrement refcount of attached loc_id */
    if (ref->type && (ref->loc_id != H5I_INVALID_HID)) {
        if (ref->app_ref) {
            if (H5I_dec_app_ref(ref->loc_id) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDEC, FAIL, "decrementing location ID failed");
        }
        else {
            if (H5I_dec_ref(ref->loc_id) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDEC, FAIL, "decrementing location ID failed");
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__destroy() */

/*-------------------------------------------------------------------------
 * Function:    H5R__set_loc_id
 *
 * Purpose:     Attach location ID to reference and increment location refcount.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__set_loc_id(H5R_ref_priv_t *ref, hid_t id, bool inc_ref, bool app_ref)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(ref != NULL);
    assert(id != H5I_INVALID_HID);

    /* If a location ID was previously assigned, decrement refcount and
     * assign new one */
    if ((ref->loc_id != H5I_INVALID_HID)) {
        if (ref->app_ref) {
            if (H5I_dec_app_ref(ref->loc_id) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDEC, FAIL, "decrementing location ID failed");
        }
        else {
            if (H5I_dec_ref(ref->loc_id) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDEC, FAIL, "decrementing location ID failed");
        }
    }
    ref->loc_id = id;

    /* Prevent location ID from being freed until reference is destroyed,
     * set app_ref if necessary as references are exposed to users and are
     * expected to be destroyed, this allows the loc_id to be cleanly released
     * on shutdown if users fail to call H5Rdestroy(). */
    if (inc_ref && H5I_inc_ref(ref->loc_id, app_ref) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTINC, FAIL, "incrementing location ID failed");
    ref->app_ref = app_ref;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__set_loc_id() */

/*-------------------------------------------------------------------------
 * Function:    H5R__get_loc_id
 *
 * Purpose:     Retrieve location ID attached to existing reference.
 *
 * Return:      Valid ID on success / H5I_INVALID_HID on failure
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5R__get_loc_id(const H5R_ref_priv_t *ref)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(ref != NULL);

    ret_value = ref->loc_id;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__get_loc_id() */

/*-------------------------------------------------------------------------
 * Function:    H5R__reopen_file
 *
 * Purpose:     Re-open referenced file using file access property list.
 *
 * Return:      Valid ID on success / H5I_INVALID_HID on failure
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5R__reopen_file(H5R_ref_priv_t *ref, hid_t fapl_id)
{
    H5P_genplist_t       *plist;           /* Property list for FAPL */
    void                 *new_file = NULL; /* File object opened */
    H5VL_connector_prop_t connector_prop;  /* Property for VOL connector ID & info     */
    H5VL_object_t        *vol_obj = NULL;  /* VOL object for file */
    uint64_t              supported;       /* Whether 'post open' operation is supported by VOL connector */
    hid_t                 ret_value = H5I_INVALID_HID;

    FUNC_ENTER_PACKAGE

    /* TODO add search path */

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&fapl_id, H5P_CLS_FACC, H5I_INVALID_HID, true) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* Get the VOL info from the fapl */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a file access property list");
    if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL connector info");

    /* Stash a copy of the "top-level" connector property, before any pass-through
     *  connectors modify or unwrap it.
     */
    if (H5CX_set_vol_connector_prop(&connector_prop) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, H5I_INVALID_HID,
                    "can't set VOL connector info in API context");

    /* Open the file */
    /* (Must open file read-write to allow for object modifications) */
    if (NULL == (new_file = H5VL_file_open(&connector_prop, H5R_REF_FILENAME(ref), H5F_ACC_RDWR, fapl_id,
                                           H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTOPENFILE, H5I_INVALID_HID, "unable to open file");

    /* Get an ID for the file */
    if ((ret_value = H5VL_register_using_vol_id(H5I_FILE, new_file, connector_prop.connector_id, true)) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register file handle");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(ret_value)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, H5I_INVALID_HID, "invalid object identifier");

    /* Make the 'post open' callback */
    supported = 0;
    if (H5VL_introspect_opt_query(vol_obj, H5VL_SUBCLS_FILE, H5VL_NATIVE_FILE_POST_OPEN, &supported) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, H5I_INVALID_HID, "can't check for 'post open' operation");
    if (supported & H5VL_OPT_QUERY_SUPPORTED) {
        H5VL_optional_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Set up VOL callback arguments */
        vol_cb_args.op_type = H5VL_NATIVE_FILE_POST_OPEN;
        vol_cb_args.args    = NULL;

        /* Make the 'post open' callback */
        if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTINIT, H5I_INVALID_HID,
                        "unable to make file 'post open' callback");
    } /* end if */

    /* Attach loc_id to reference */
    if (H5R__set_loc_id((H5R_ref_priv_t *)ref, ret_value, false, true) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, H5I_INVALID_HID, "unable to attach location id to reference");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__reopen_file() */

/*-------------------------------------------------------------------------
 * Function:    H5R__get_type
 *
 * Purpose:     Given a reference to some object, return the type of that
 *              reference.
 *
 * Return:      Type of the reference
 *
 *-------------------------------------------------------------------------
 */
H5R_type_t
H5R__get_type(const H5R_ref_priv_t *ref)
{
    H5R_type_t ret_value = H5R_BADTYPE;

    FUNC_ENTER_PACKAGE_NOERR

    assert(ref != NULL);
    ret_value = (H5R_type_t)ref->type;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__get_type() */

/*-------------------------------------------------------------------------
 * Function:    H5R__equal
 *
 * Purpose:     Compare two references
 *
 * Return:      true if equal, false if unequal, FAIL if error
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5R__equal(const H5R_ref_priv_t *ref1, const H5R_ref_priv_t *ref2)
{
    htri_t ret_value = true;

    FUNC_ENTER_PACKAGE

    assert(ref1 != NULL);
    assert(ref2 != NULL);

    /* Compare reference types */
    if (ref1->type != ref2->type)
        HGOTO_DONE(false);

    /* Compare object addresses */
    if (ref1->token_size != ref2->token_size)
        HGOTO_DONE(false);
    if (0 != memcmp(&ref1->info.obj.token, &ref2->info.obj.token, ref1->token_size))
        HGOTO_DONE(false);

    /* Compare filenames */
    if ((ref1->info.obj.filename && (NULL == ref2->info.obj.filename)) ||
        ((NULL == ref1->info.obj.filename) && ref2->info.obj.filename))
        HGOTO_DONE(false);
    if (ref1->info.obj.filename && ref1->info.obj.filename &&
        (0 != strcmp(ref1->info.obj.filename, ref2->info.obj.filename)))
        HGOTO_DONE(false);

    switch (ref1->type) {
        case H5R_OBJECT2:
            break;
        case H5R_DATASET_REGION2:
            if ((ret_value = H5S_extent_equal(ref1->info.reg.space, ref2->info.reg.space)) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOMPARE, FAIL, "cannot compare dataspace extents");
            break;
        case H5R_ATTR:
            assert(ref1->info.attr.name && ref2->info.attr.name);
            if (0 != strcmp(ref1->info.attr.name, ref2->info.attr.name))
                HGOTO_DONE(false);
            break;
        case H5R_OBJECT1:
        case H5R_DATASET_REGION1:
        case H5R_BADTYPE:
        case H5R_MAXTYPE:
            assert("invalid reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (invalid reference type)");
        default:
            assert("unknown reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (unknown reference type)");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__equal() */

/*-------------------------------------------------------------------------
 * Function:    H5R__copy
 *
 * Purpose:     Copy a reference
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__copy(const H5R_ref_priv_t *src_ref, H5R_ref_priv_t *dst_ref)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert((src_ref != NULL) && (dst_ref != NULL));

    H5MM_memcpy(&dst_ref->info.obj.token, &src_ref->info.obj.token, sizeof(H5O_token_t));
    dst_ref->encode_size = src_ref->encode_size;
    dst_ref->type        = src_ref->type;
    dst_ref->token_size  = src_ref->token_size;

    switch (src_ref->type) {
        case H5R_OBJECT2:
            break;
        case H5R_DATASET_REGION2:
            if (NULL == (dst_ref->info.reg.space = H5S_copy(src_ref->info.reg.space, false, true)))
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "unable to copy dataspace");
            break;
        case H5R_ATTR:
            if (NULL == (dst_ref->info.attr.name = strdup(src_ref->info.attr.name)))
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "Cannot copy attribute name");
            break;
        case H5R_OBJECT1:
        case H5R_DATASET_REGION1:
            assert("invalid reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (invalid reference type)");
        case H5R_BADTYPE:
        case H5R_MAXTYPE:
        default:
            assert("unknown reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (unknown reference type)");
    } /* end switch */

    /* We only need to keep a copy of the filename if we don't have the loc_id */
    if (src_ref->loc_id == H5I_INVALID_HID) {
        assert(src_ref->info.obj.filename);

        if (NULL == (dst_ref->info.obj.filename = strdup(src_ref->info.obj.filename)))
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "Cannot copy filename");
        dst_ref->loc_id = H5I_INVALID_HID;
    }
    else {
        dst_ref->info.obj.filename = NULL;

        /* Set location ID and hold reference to it */
        dst_ref->loc_id = src_ref->loc_id;
        if (H5I_inc_ref(dst_ref->loc_id, true) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTINC, FAIL, "incrementing location ID failed");
        dst_ref->app_ref = true;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__copy() */

/*-------------------------------------------------------------------------
 * Function:    H5R__get_obj_token
 *
 * Purpose:     Given a reference to some object, get the encoded token.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__get_obj_token(const H5R_ref_priv_t *ref, H5O_token_t *obj_token, size_t *token_size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(ref != NULL);
    assert(ref->token_size <= H5O_MAX_TOKEN_SIZE);

    if (obj_token) {
        if (0 == ref->token_size)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "NULL token size");
        H5MM_memcpy(obj_token, &ref->info.obj.token, sizeof(H5O_token_t));
    }
    if (token_size)
        *token_size = ref->token_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__get_obj_token() */

/*-------------------------------------------------------------------------
 * Function:    H5R__set_obj_token
 *
 * Purpose:     Given a reference to some object, set the encoded token.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__set_obj_token(H5R_ref_priv_t *ref, const H5O_token_t *obj_token, size_t token_size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(ref != NULL);
    assert(obj_token);
    assert(token_size);
    assert(token_size <= H5O_MAX_TOKEN_SIZE);

    H5MM_memcpy(&ref->info.obj.token, obj_token, token_size);
    assert(token_size <= 255);
    ref->token_size = (uint8_t)token_size;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__set_obj_token() */

/*-------------------------------------------------------------------------
 * Function:    H5R__get_region
 *
 * Purpose:     Given a reference to some object, creates a copy of the
 *              dataset pointed to's dataspace and defines a selection in
 *              the copy which is the region pointed to.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__get_region(const H5R_ref_priv_t *ref, H5S_t *space)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(ref != NULL);
    assert(ref->type == H5R_DATASET_REGION2);
    assert(space);

    /* Copy reference selection to destination */
    if (H5S_select_copy(space, ref->info.reg.space, false) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOPY, FAIL, "unable to copy selection");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__get_region() */

/*-------------------------------------------------------------------------
 * Function:    H5R__get_file_name
 *
 * Purpose:     Given a reference to some object, determine a file name of
 *              the object located into.
 *
 * Return:      Non-negative length of the path on success / -1 on failure
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5R__get_file_name(const H5R_ref_priv_t *ref, char *buf, size_t size)
{
    size_t  copy_len;
    ssize_t ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(ref != NULL);

    /* Return if that reference has no filename set */
    if (!ref->info.obj.filename)
        HGOTO_ERROR(H5E_REFERENCE, H5E_ARGS, (-1), "no filename available for that reference");

    /* Get the file name length */
    copy_len = strlen(ref->info.obj.filename);
    assert(copy_len <= H5R_MAX_STRING_LEN);

    /* Copy the file name */
    if (buf) {
        copy_len = MIN(copy_len, size - 1);
        H5MM_memcpy(buf, ref->info.obj.filename, copy_len);
        buf[copy_len] = '\0';
    }
    ret_value = (ssize_t)(copy_len + 1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__get_file_name() */

/*-------------------------------------------------------------------------
 * Function:    H5R__get_attr_name
 *
 * Purpose:     Given a reference to some attribute, determine its name.
 *
 * Return:      Non-negative length of the path on success / -1 on failure
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5R__get_attr_name(const H5R_ref_priv_t *ref, char *buf, size_t size)
{
    ssize_t ret_value = -1; /* Return value */
    size_t  attr_name_len;  /* Length of the attribute name */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(ref != NULL);
    assert(ref->type == H5R_ATTR);

    /* Get the attribute name length */
    attr_name_len = strlen(ref->info.attr.name);
    assert(attr_name_len <= H5R_MAX_STRING_LEN);

    /* Get the attribute name */
    if (buf) {
        size_t copy_len = MIN(attr_name_len, size - 1);
        H5MM_memcpy(buf, ref->info.attr.name, copy_len);
        buf[copy_len] = '\0';
    }

    ret_value = (ssize_t)(attr_name_len + 1);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__get_attr_name() */

/*-------------------------------------------------------------------------
 * Function:    H5R__encode
 *
 * Purpose:     Private function for H5Rencode
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__encode(const char *filename, const H5R_ref_priv_t *ref, unsigned char *buf, size_t *nalloc,
            unsigned flags)
{
    uint8_t *p        = (uint8_t *)buf;
    size_t   buf_size = 0, encode_size = 0;
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(ref);
    assert(nalloc);

    /**
     * Encoding format:
     * | Reference type (8 bits) | Flags (8 bits) | Token (token size)
     *    |                         |
     *    |                         |----> H5R_IS_EXTERNAL: File info
     *    |
     *    |----> H5R_DATASET_REGION2: Serialized selection
     *    |
     *    |----> H5R_ATTR: Attribute name len + name
     *
     */

    /* Don't encode if buffer size isn't big enough or buffer is empty */
    if (buf && *nalloc >= H5R_ENCODE_HEADER_SIZE) {
        /* Encode the type of the reference */
        *p++ = (uint8_t)ref->type;

        /* Encode the flags */
        *p++ = (uint8_t)flags;

        buf_size = *nalloc - H5R_ENCODE_HEADER_SIZE;
    } /* end if */
    encode_size += H5R_ENCODE_HEADER_SIZE;

    /* Encode object token */
    H5R_ENCODE_VAR(H5R__encode_obj_token, &ref->info.obj.token, ref->token_size, p, buf_size, encode_size,
                   "Cannot encode object address");

    /**
     * TODO Encode VOL info
     * When we have a better way of storing blobs, we should add
     * support for referencing files in external VOLs.
     * There are currently multiple limitations:
     *   - avoid duplicating VOL info on each reference
     *   - must query terminal VOL connector to avoid passthrough confusion
     */
    if (flags & H5R_IS_EXTERNAL)
        /* Encode file name */
        H5R_ENCODE(H5R__encode_string, filename, p, buf_size, encode_size, "Cannot encode filename");

    switch (ref->type) {
        case H5R_OBJECT2:
            break;

        case H5R_DATASET_REGION2:
            /* Encode dataspace */
            H5R_ENCODE(H5R__encode_region, ref->info.reg.space, p, buf_size, encode_size,
                       "Cannot encode region");
            break;

        case H5R_ATTR:
            /* Encode attribute name */
            H5R_ENCODE(H5R__encode_string, ref->info.attr.name, p, buf_size, encode_size,
                       "Cannot encode attribute name");
            break;

        case H5R_OBJECT1:
        case H5R_DATASET_REGION1:
        case H5R_BADTYPE:
        case H5R_MAXTYPE:
            assert("invalid reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (invalid reference type)");

        default:
            assert("unknown reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (unknown reference type)");
    } /* end switch */

    H5R_LOG_DEBUG("Encoded reference, filename=%s, obj_addr=%s, encode size=%u", ref->info.obj.filename,
                  H5R__print_token(ref->info.obj.token), encode_size);

    *nalloc = encode_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__encode() */

/*-------------------------------------------------------------------------
 * Function:    H5R__decode
 *
 * Purpose:     Private function for H5Rdecode
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__decode(const unsigned char *buf, size_t *nbytes, H5R_ref_priv_t *ref)
{
    const uint8_t *p        = (const uint8_t *)buf;
    size_t         buf_size = 0, decode_size = 0;
    uint8_t        flags;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(buf);
    assert(nbytes);
    assert(ref);

    /* Don't decode if buffer size isn't big enough */
    buf_size = *nbytes;
    if (buf_size < H5R_ENCODE_HEADER_SIZE)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");

    /* Set new reference */
    ref->type = (int8_t)*p++;
    if (ref->type <= H5R_BADTYPE || ref->type >= H5R_MAXTYPE)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid reference type");

    /* Set flags */
    flags = *p++;

    buf_size -= H5R_ENCODE_HEADER_SIZE;
    decode_size += H5R_ENCODE_HEADER_SIZE;

    /* Decode object token */
    H5R_DECODE_VAR(H5R__decode_obj_token, &ref->info.obj.token, &ref->token_size, p, buf_size, decode_size,
                   "Cannot decode object address");

    /* We do not need to store the filename if the reference is internal */
    if (flags & H5R_IS_EXTERNAL) {
        /* Decode file name */
        H5R_DECODE(H5R__decode_string, &ref->info.obj.filename, p, buf_size, decode_size,
                   "Cannot decode filename");
    }
    else
        ref->info.obj.filename = NULL;

    switch (ref->type) {
        case H5R_OBJECT2:
            break;
        case H5R_DATASET_REGION2:
            /* Decode dataspace */
            H5R_DECODE(H5R__decode_region, &ref->info.reg.space, p, buf_size, decode_size,
                       "Cannot decode region");
            break;
        case H5R_ATTR:
            /* Decode attribute name */
            H5R_DECODE(H5R__decode_string, &ref->info.attr.name, p, buf_size, decode_size,
                       "Cannot decode attribute name");
            break;
        case H5R_OBJECT1:
        case H5R_DATASET_REGION1:
        case H5R_BADTYPE:
        case H5R_MAXTYPE:
            assert("invalid reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (invalid reference type)");
        default:
            assert("unknown reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (unknown reference type)");
    } /* end switch */

    /* Set loc ID to invalid */
    ref->loc_id = H5I_INVALID_HID;

    /* Set encoding size */
    ref->encode_size = (uint32_t)decode_size;

    H5R_LOG_DEBUG("Decoded reference, filename=%s, obj_addr=%s, encode size=%u", ref->info.obj.filename,
                  H5R__print_token(ref->info.obj.token), ref->encode_size);

    /* Set output info */
    *nbytes = decode_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__decode() */

/*-------------------------------------------------------------------------
 * Function:    H5R__encode_obj_token
 *
 * Purpose:     Encode an object address.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__encode_obj_token(const H5O_token_t *obj_token, size_t token_size, unsigned char *buf, size_t *nalloc)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    assert(nalloc);

    /* Don't encode if buffer size isn't big enough or buffer is empty */
    if (buf && *nalloc >= token_size) {
        uint8_t *p = (uint8_t *)buf;

        /* Encode token size */
        *p++ = (uint8_t)(token_size & 0xff);

        /* Encode token */
        H5MM_memcpy(p, obj_token, token_size);
    }
    *nalloc = token_size + sizeof(uint8_t);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__encode_obj_token() */

/*-------------------------------------------------------------------------
 * Function:    H5R__decode_obj_token
 *
 * Purpose:     Decode an object address.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__decode_obj_token(const unsigned char *buf, size_t *nbytes, H5O_token_t *obj_token, uint8_t *token_size)
{
    const uint8_t *p         = (const uint8_t *)buf;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(buf);
    assert(nbytes);
    assert(obj_token);
    assert(token_size);

    /* Don't decode if buffer size isn't big enough */
    if (*nbytes < sizeof(uint8_t))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");

    /* Get token size */
    *token_size = *p++;
    if (*token_size > sizeof(H5O_token_t))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Invalid token size (%u)", *token_size);

    /* Make sure that token is initialized */
    memset(obj_token, 0, sizeof(H5O_token_t));

    /* Decode token */
    H5MM_memcpy(obj_token, p, *token_size);

    *nbytes = (size_t)(*token_size + sizeof(uint8_t));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__decode_obj_token() */

/*-------------------------------------------------------------------------
 * Function:    H5R__encode_region
 *
 * Purpose:     Encode a selection.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__encode_region(H5S_t *space, unsigned char *buf, size_t *nalloc)
{
    uint8_t *p         = NULL; /* Pointer to data to store */
    hssize_t buf_size  = 0;
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(space);
    assert(nalloc);

    /* Get the amount of space required to serialize the selection */
    if ((buf_size = H5S_SELECT_SERIAL_SIZE(space)) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL,
                    "Cannot determine amount of space needed for serializing selection");

    /* Don't encode if buffer size isn't big enough or buffer is empty */
    if (buf && *nalloc >= ((size_t)buf_size + 2 * sizeof(uint32_t))) {
        int rank;
        p = (uint8_t *)buf;

        /* Encode the size for safety check */
        UINT32ENCODE(p, (uint32_t)buf_size);

        /* Encode the extent rank */
        if ((rank = H5S_get_simple_extent_ndims(space)) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't get extent rank for selection");
        UINT32ENCODE(p, (uint32_t)rank);

        /* Serialize the selection */
        if (H5S_SELECT_SERIALIZE(space, (unsigned char **)&p) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, "can't serialize selection");
    } /* end if */
    *nalloc = (size_t)buf_size + 2 * sizeof(uint32_t);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__encode_region() */

/*-------------------------------------------------------------------------
 * Function:    H5R__decode_region
 *
 * Purpose:     Decode a selection.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__decode_region(const unsigned char *buf, size_t *nbytes, H5S_t **space_ptr)
{
    const uint8_t *p        = (const uint8_t *)buf;
    const uint8_t *p_end    = p + *nbytes - 1;
    size_t         buf_size = 0;
    unsigned       rank;
    H5S_t         *space;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(buf);
    assert(nbytes);
    assert(space_ptr);

    /* Don't decode if buffer size isn't big enough */
    if (*nbytes < (2 * sizeof(uint32_t)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");

    /* Decode the selection size */
    UINT32DECODE(p, buf_size);
    buf_size += sizeof(uint32_t);

    /* Decode the extent rank */
    UINT32DECODE(p, rank);
    buf_size += sizeof(uint32_t);

    /* Don't decode if buffer size isn't big enough */
    if (*nbytes < buf_size)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");

    /* Deserialize the selection (dataspaces need the extent rank information) */
    if (NULL == (space = H5S_create(H5S_SIMPLE)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");
    if (H5S_set_extent_simple(space, rank, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, FAIL, "can't set extent rank for selection");

    if (p - 1 > p_end)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Ran off end of buffer while decoding");

    if (H5S_SELECT_DESERIALIZE(&space, &p, (size_t)(p_end - p + 1)) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "can't deserialize selection");

    *nbytes    = buf_size;
    *space_ptr = space;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__decode_region() */

/*-------------------------------------------------------------------------
 * Function:    H5R__encode_string
 *
 * Purpose:     Encode a string.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__encode_string(const char *string, unsigned char *buf, size_t *nalloc)
{
    size_t string_len, buf_size;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(string);
    assert(nalloc);

    /* Get the amount of space required to serialize the string */
    string_len = strlen(string);
    if (string_len > H5R_MAX_STRING_LEN)
        HGOTO_ERROR(H5E_REFERENCE, H5E_ARGS, FAIL, "string too long");

    /* Compute buffer size, allow for the attribute name length and object address */
    buf_size = string_len + sizeof(uint16_t);

    if (buf && *nalloc >= buf_size) {
        uint8_t *p = (uint8_t *)buf;
        /* Serialize information for string length into the buffer */
        UINT16ENCODE(p, string_len);
        /* Copy the string into the buffer */
        H5MM_memcpy(p, string, string_len);
    }
    *nalloc = buf_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__encode_string() */

/*-------------------------------------------------------------------------
 * Function:    H5R__decode_string
 *
 * Purpose:     Decode a string.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5R__decode_string(const unsigned char *buf, size_t *nbytes, char **string_ptr)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t         string_len;
    char          *string    = NULL;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(buf);
    assert(nbytes);
    assert(string_ptr);

    /* Don't decode if buffer size isn't big enough */
    if (*nbytes < sizeof(uint16_t))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");

    /* Get the string length */
    UINT16DECODE(p, string_len);
    assert(string_len <= H5R_MAX_STRING_LEN);

    /* Allocate the string */
    if (NULL == (string = (char *)H5MM_malloc(string_len + 1)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTALLOC, FAIL, "Cannot allocate string");

    /* Copy the string */
    H5MM_memcpy(string, p, string_len);
    string[string_len] = '\0';

    *string_ptr = string;
    *nbytes     = sizeof(uint16_t) + string_len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__decode_string() */

/*-------------------------------------------------------------------------
 * Function:    H5R__encode_heap
 *
 * Purpose:     Encode data and insert into heap (native only).
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__encode_heap(H5F_t *f, unsigned char *buf, size_t *nalloc, const unsigned char *data, size_t data_size)
{
    size_t buf_size;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(nalloc);

    buf_size = H5HG_HEAP_ID_SIZE(f);
    if (buf && *nalloc >= buf_size) {
        H5HG_t   hobjid;
        uint8_t *p = (uint8_t *)buf;

        /* Write the reference information to disk (allocates space also) */
        if (H5HG_insert(f, data_size, data, &hobjid) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_WRITEERROR, FAIL, "Unable to write reference information");

        /* Encode the heap information */
        H5F_addr_encode(f, &p, hobjid.addr);
        UINT32ENCODE(p, hobjid.idx);
    } /* end if */
    *nalloc = buf_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__encode_heap() */

/*-------------------------------------------------------------------------
 * Function:    H5R__decode_heap
 *
 * Purpose:     Decode data inserted into heap (native only).
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__decode_heap(H5F_t *f, const unsigned char *buf, size_t *nbytes, unsigned char **data_ptr,
                 size_t *data_size)
{
    const uint8_t *p = (const uint8_t *)buf;
    H5HG_t         hobjid;
    size_t         buf_size;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(buf);
    assert(nbytes);
    assert(data_ptr);

    buf_size = H5HG_HEAP_ID_SIZE(f);
    /* Don't decode if buffer size isn't big enough */
    if (*nbytes < buf_size)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");

    /* Get the heap information */
    H5F_addr_decode(f, &p, &(hobjid.addr));
    if (!H5_addr_defined(hobjid.addr) || hobjid.addr == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Undefined reference pointer");
    UINT32DECODE(p, hobjid.idx);

    /* Read the information from disk */
    if (NULL == (*data_ptr = (unsigned char *)H5HG_read(f, &hobjid, (void *)*data_ptr, data_size)))
        HGOTO_ERROR(H5E_REFERENCE, H5E_READERROR, FAIL, "Unable to read reference data");

    *nbytes = buf_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__decode_heap() */

/*-------------------------------------------------------------------------
 * Function:    H5R__encode_token_obj_compat
 *
 * Purpose:     Encode an object token. (native only)
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__encode_token_obj_compat(const H5O_token_t *obj_token, size_t token_size, unsigned char *buf,
                             size_t *nalloc)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    assert(obj_token);
    assert(token_size);
    assert(nalloc);

    /* Don't encode if buffer size isn't big enough or buffer is empty */
    if (buf && *nalloc >= token_size)
        H5MM_memcpy(buf, obj_token, token_size);

    *nalloc = token_size;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__encode_token_obj_compat() */

/*-------------------------------------------------------------------------
 * Function:    H5R__decode_token_obj_compat
 *
 * Purpose:     Decode an object token. (native only)
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__decode_token_obj_compat(const unsigned char *buf, size_t *nbytes, H5O_token_t *obj_token,
                             size_t token_size)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(buf);
    assert(nbytes);
    assert(obj_token);
    assert(token_size);

    /* Don't decode if buffer size isn't big enough */
    if (*nbytes < token_size)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Buffer size is too small");

    H5MM_memcpy(obj_token, buf, token_size);

    *nbytes = token_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5R__decode_token_obj_compat() */

/*-------------------------------------------------------------------------
 * Function:    H5R__decode_token_region_compat
 *
 * Purpose:     Decode dataset selection from data inserted into heap
 *              (native only).
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5R__decode_token_region_compat(H5F_t *f, const unsigned char *buf, size_t *nbytes, H5O_token_t *obj_token,
                                size_t token_size, H5S_t **space_ptr)
{
    unsigned char *data  = NULL;
    H5O_token_t    token = {0};
    size_t         data_size;
    const uint8_t *p         = NULL;
    const uint8_t *p_end     = NULL;
    H5S_t         *space     = NULL;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(buf);
    assert(nbytes);
    assert(token_size);

    /* Read from heap */
    if (H5R__decode_heap(f, buf, nbytes, &data, &data_size) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Get object address */
    p     = (const uint8_t *)data;
    p_end = p + data_size - 1;
    H5MM_memcpy(&token, p, token_size);
    p += token_size;

    if (space_ptr) {
        H5O_loc_t oloc; /* Object location */

        /* Initialize the object location */
        H5O_loc_reset(&oloc);
        oloc.file = f;

        if (H5VL_native_token_to_addr(f, H5I_FILE, token, &oloc.addr) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTUNSERIALIZE, FAIL,
                        "can't deserialize object token into address");

        /* Open and copy the dataset's dataspace */
        if (NULL == (space = H5S_read(&oloc)))
            HGOTO_ERROR(H5E_REFERENCE, H5E_NOTFOUND, FAIL, "not found");

        /* Unserialize the selection */

        if (p - 1 >= p_end)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Ran off end of buffer while deserializing");

        if (H5S_SELECT_DESERIALIZE(&space, &p, (size_t)(p_end - p + 1)) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "can't deserialize selection");

        *space_ptr = space;
    }
    if (obj_token)
        H5MM_memcpy(obj_token, &token, sizeof(H5O_token_t));

done:
    H5MM_free(data);

    if (ret_value < 0) {
        if (space && H5S_close(space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release dataspace");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5R__decode_token_region_compat() */
