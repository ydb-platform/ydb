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
 * Created:		H5Plapl.c
 *
 * Purpose:		Link access property list class routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Ppkg.h"      /* Property lists                           */
#include "H5VMprivate.h" /* Vector Functions                         */

/****************/
/* Local Macros */
/****************/

/* ========  Link access properties ======== */
/* Definitions for number of soft links to traverse */
#define H5L_ACS_NLINKS_SIZE sizeof(size_t)
#define H5L_ACS_NLINKS_DEF  H5L_NUM_LINKS /*max symlinks to follow per lookup  */
#define H5L_ACS_NLINKS_ENC  H5P__encode_size_t
#define H5L_ACS_NLINKS_DEC  H5P__decode_size_t

/* Definitions for external link prefix */
#define H5L_ACS_ELINK_PREFIX_SIZE  sizeof(char *)
#define H5L_ACS_ELINK_PREFIX_DEF   NULL /*default is no prefix */
#define H5L_ACS_ELINK_PREFIX_SET   H5P__lacc_elink_pref_set
#define H5L_ACS_ELINK_PREFIX_GET   H5P__lacc_elink_pref_get
#define H5L_ACS_ELINK_PREFIX_ENC   H5P__lacc_elink_pref_enc
#define H5L_ACS_ELINK_PREFIX_DEC   H5P__lacc_elink_pref_dec
#define H5L_ACS_ELINK_PREFIX_DEL   H5P__lacc_elink_pref_del
#define H5L_ACS_ELINK_PREFIX_COPY  H5P__lacc_elink_pref_copy
#define H5L_ACS_ELINK_PREFIX_CMP   H5P__lacc_elink_pref_cmp
#define H5L_ACS_ELINK_PREFIX_CLOSE H5P__lacc_elink_pref_close

/* Definitions for setting fapl of external link access */
#define H5L_ACS_ELINK_FAPL_SIZE  sizeof(hid_t)
#define H5L_ACS_ELINK_FAPL_DEF   H5P_DEFAULT
#define H5L_ACS_ELINK_FAPL_SET   H5P__lacc_elink_fapl_set
#define H5L_ACS_ELINK_FAPL_GET   H5P__lacc_elink_fapl_get
#define H5L_ACS_ELINK_FAPL_ENC   H5P__lacc_elink_fapl_enc
#define H5L_ACS_ELINK_FAPL_DEC   H5P__lacc_elink_fapl_dec
#define H5L_ACS_ELINK_FAPL_DEL   H5P__lacc_elink_fapl_del
#define H5L_ACS_ELINK_FAPL_COPY  H5P__lacc_elink_fapl_copy
#define H5L_ACS_ELINK_FAPL_CMP   H5P__lacc_elink_fapl_cmp
#define H5L_ACS_ELINK_FAPL_CLOSE H5P__lacc_elink_fapl_close

/* Definitions for file access flags for external link traversal */
#define H5L_ACS_ELINK_FLAGS_SIZE sizeof(unsigned)
#define H5L_ACS_ELINK_FLAGS_DEF  H5F_ACC_DEFAULT
#define H5L_ACS_ELINK_FLAGS_ENC  H5P__encode_unsigned
#define H5L_ACS_ELINK_FLAGS_DEC  H5P__decode_unsigned

/* Definitions for callback function for external link traversal */
#define H5L_ACS_ELINK_CB_SIZE sizeof(H5L_elink_cb_t)
#define H5L_ACS_ELINK_CB_DEF                                                                                 \
    {                                                                                                        \
        NULL, NULL                                                                                           \
    }

#ifdef H5_HAVE_PARALLEL
/* Definition for reading metadata collectively */
#define H5L_ACS_COLL_MD_READ_SIZE sizeof(H5P_coll_md_read_flag_t)
#define H5L_ACS_COLL_MD_READ_DEF  H5P_USER_FALSE
#define H5L_ACS_COLL_MD_READ_ENC  H5P__encode_coll_md_read_flag_t
#define H5L_ACS_COLL_MD_READ_DEC  H5P__decode_coll_md_read_flag_t
#endif /* H5_HAVE_PARALLEL */

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Property class callbacks */
static herr_t H5P__lacc_reg_prop(H5P_genclass_t *pclass);

/* Property list callbacks */
static herr_t H5P__lacc_elink_pref_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__lacc_elink_pref_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__lacc_elink_pref_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__lacc_elink_pref_dec(const void **_pp, void *value);
static herr_t H5P__lacc_elink_pref_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__lacc_elink_pref_copy(const char *name, size_t size, void *value);
static int    H5P__lacc_elink_pref_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__lacc_elink_pref_close(const char *name, size_t size, void *value);
static herr_t H5P__lacc_elink_fapl_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__lacc_elink_fapl_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__lacc_elink_fapl_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__lacc_elink_fapl_dec(const void **_pp, void *value);
static herr_t H5P__lacc_elink_fapl_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__lacc_elink_fapl_copy(const char *name, size_t size, void *value);
static int    H5P__lacc_elink_fapl_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__lacc_elink_fapl_close(const char *name, size_t size, void *value);

/*********************/
/* Package Variables */
/*********************/

/* Dataset creation property list class library initialization object */
const H5P_libclass_t H5P_CLS_LACC[1] = {{
    "link access",        /* Class name for debugging     */
    H5P_TYPE_LINK_ACCESS, /* Class type                   */

    &H5P_CLS_ROOT_g,           /* Parent class                 */
    &H5P_CLS_LINK_ACCESS_g,    /* Pointer to class             */
    &H5P_CLS_LINK_ACCESS_ID_g, /* Pointer to class ID          */
    &H5P_LST_LINK_ACCESS_ID_g, /* Pointer to default property list ID */
    H5P__lacc_reg_prop,        /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Property value defaults */
static const size_t H5L_def_nlinks_g = H5L_ACS_NLINKS_DEF; /* Default number of soft links to traverse */
static const char  *H5L_def_elink_prefix_g =
    H5L_ACS_ELINK_PREFIX_DEF;                                     /* Default external link prefix string */
static const hid_t    H5L_def_fapl_id_g = H5L_ACS_ELINK_FAPL_DEF; /* Default fapl for external link access */
static const unsigned H5L_def_elink_flags_g =
    H5L_ACS_ELINK_FLAGS_DEF; /* Default file access flags for external link traversal */
static const H5L_elink_cb_t H5L_def_elink_cb_g =
    H5L_ACS_ELINK_CB_DEF; /* Default external link traversal callback */
#ifdef H5_HAVE_PARALLEL
static const H5P_coll_md_read_flag_t H5L_def_coll_md_read_g =
    H5L_ACS_COLL_MD_READ_DEF; /* Default setting for the collective metedata read flag */
#endif                        /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_reg_prop
 *
 * Purpose:     Register the dataset creation property list class's properties
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_reg_prop(H5P_genclass_t *pclass)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Register property for number of links traversed */
    if (H5P__register_real(pclass, H5L_ACS_NLINKS_NAME, H5L_ACS_NLINKS_SIZE, &H5L_def_nlinks_g, NULL, NULL,
                           NULL, H5L_ACS_NLINKS_ENC, H5L_ACS_NLINKS_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register property for external link prefix */
    if (H5P__register_real(pclass, H5L_ACS_ELINK_PREFIX_NAME, H5L_ACS_ELINK_PREFIX_SIZE,
                           &H5L_def_elink_prefix_g, NULL, H5L_ACS_ELINK_PREFIX_SET, H5L_ACS_ELINK_PREFIX_GET,
                           H5L_ACS_ELINK_PREFIX_ENC, H5L_ACS_ELINK_PREFIX_DEC, H5L_ACS_ELINK_PREFIX_DEL,
                           H5L_ACS_ELINK_PREFIX_COPY, H5L_ACS_ELINK_PREFIX_CMP,
                           H5L_ACS_ELINK_PREFIX_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register fapl for link access */
    if (H5P__register_real(pclass, H5L_ACS_ELINK_FAPL_NAME, H5L_ACS_ELINK_FAPL_SIZE, &H5L_def_fapl_id_g, NULL,
                           H5L_ACS_ELINK_FAPL_SET, H5L_ACS_ELINK_FAPL_GET, H5L_ACS_ELINK_FAPL_ENC,
                           H5L_ACS_ELINK_FAPL_DEC, H5L_ACS_ELINK_FAPL_DEL, H5L_ACS_ELINK_FAPL_COPY,
                           H5L_ACS_ELINK_FAPL_CMP, H5L_ACS_ELINK_FAPL_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register property for external link file access flags */
    if (H5P__register_real(pclass, H5L_ACS_ELINK_FLAGS_NAME, H5L_ACS_ELINK_FLAGS_SIZE, &H5L_def_elink_flags_g,
                           NULL, NULL, NULL, H5L_ACS_ELINK_FLAGS_ENC, H5L_ACS_ELINK_FLAGS_DEC, NULL, NULL,
                           NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register property for external link file traversal callback */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5L_ACS_ELINK_CB_NAME, H5L_ACS_ELINK_CB_SIZE, &H5L_def_elink_cb_g, NULL,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

#ifdef H5_HAVE_PARALLEL
    /* Register the metadata collective read flag */
    if (H5P__register_real(pclass, H5_COLL_MD_READ_FLAG_NAME, H5L_ACS_COLL_MD_READ_SIZE,
                           &H5L_def_coll_md_read_g, NULL, NULL, NULL, H5L_ACS_COLL_MD_READ_ENC,
                           H5L_ACS_COLL_MD_READ_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");
#endif /* H5_HAVE_PARALLEL */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_reg_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_fapl_set
 *
 * Purpose:     Copies an external link FAPL property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_fapl_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    hid_t  l_fapl_id;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Get the FAPL ID */
    l_fapl_id = *(const hid_t *)value;

    /* Duplicate the FAPL, if it's non-default */
    if (l_fapl_id != H5P_DEFAULT) {
        H5P_genplist_t *l_fapl_plist;

        if (NULL == (l_fapl_plist = (H5P_genplist_t *)H5P_object_verify(l_fapl_id, H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "can't get property list");
        if (((*(hid_t *)value) = H5P_copy_plist(l_fapl_plist, false)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to copy file access property list");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_fapl_get
 *
 * Purpose:     Copies an external link FAPL property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_fapl_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    hid_t  l_fapl_id;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Get the FAPL ID */
    l_fapl_id = *(const hid_t *)value;

    /* Duplicate the FAPL, if it's non-default */
    if (l_fapl_id != H5P_DEFAULT) {
        H5P_genplist_t *l_fapl_plist;

        if (NULL == (l_fapl_plist = (H5P_genplist_t *)H5P_object_verify(l_fapl_id, H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "can't get property list");
        if (((*(hid_t *)value) = H5P_copy_plist(l_fapl_plist, false)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to copy file access property list");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_get() */

/*-------------------------------------------------------------------------
 * Function:       H5P__lacc_elink_fapl_enc
 *
 * Purpose:        Callback routine which is called whenever the elink FAPL
 *                 property in the dataset access property list is
 *                 encoded.
 *
 * Return:         Success:        Non-negative
 *	               Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_fapl_enc(const void *value, void **_pp, size_t *size)
{
    const hid_t    *elink_fapl = (const hid_t *)value; /* Property to encode */
    uint8_t       **pp         = (uint8_t **)_pp;
    H5P_genplist_t *fapl_plist;                 /* Pointer to property list */
    bool            non_default_fapl = false;   /* Whether the FAPL is non-default */
    size_t          fapl_size        = 0;       /* FAPL's encoded size */
    herr_t          ret_value        = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for non-default FAPL */
    if (*elink_fapl != H5P_DEFAULT) {
        if (NULL == (fapl_plist = (H5P_genplist_t *)H5P_object_verify(*elink_fapl, H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get property list");
        non_default_fapl = true;
    } /* end if */

    if (NULL != *pp) {
        /* Store whether the FAPL is non-default */
        *(*pp)++ = (uint8_t)non_default_fapl;
    } /* end if */

    /* Encode the property list, if non-default */
    /* (if *pp == NULL, will only compute the size) */
    if (non_default_fapl) {
        if (H5P__encode(fapl_plist, true, NULL, &fapl_size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, FAIL, "can't encode property list");

        if (*pp) {
            uint64_t enc_value;
            unsigned enc_size;

            /* encode the length of the plist */
            enc_value = (uint64_t)fapl_size;
            enc_size  = H5VM_limit_enc_size(enc_value);
            assert(enc_size < 256);
            *(*pp)++ = (uint8_t)enc_size;
            UINT64ENCODE_VAR(*pp, enc_value, enc_size);

            /* encode the plist */
            if (H5P__encode(fapl_plist, true, *pp, &fapl_size) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, FAIL, "can't encode property list");

            *pp += fapl_size;
        }
        fapl_size += (1 + H5VM_limit_enc_size((uint64_t)fapl_size));
    } /* end if */

    *size += (1 + fapl_size); /* Non-default flag, plus encoded property list size */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__lacc_elink_fapl_dec
 *
 * Purpose:        Callback routine which is called whenever the elink FAPL
 *                 property in the dataset access property list is
 *                 decoded.
 *
 * Return:         Success:        Non-negative
 *	               Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_fapl_dec(const void **_pp, void *_value)
{
    hid_t          *elink_fapl = (hid_t *)_value; /* The elink FAPL value */
    const uint8_t **pp         = (const uint8_t **)_pp;
    bool            non_default_fapl;    /* Whether the FAPL is non-default */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(pp);
    assert(*pp);
    assert(elink_fapl);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Determine if the FAPL is non-default */
    non_default_fapl = (bool)*(*pp)++;

    if (non_default_fapl) {
        size_t   fapl_size = 0; /* Encoded size of property list */
        unsigned enc_size;
        uint64_t enc_value;

        /* Decode the plist length */
        enc_size = *(*pp)++;
        assert(enc_size < 256);
        UINT64DECODE_VAR(*pp, enc_value, enc_size);
        fapl_size = (size_t)enc_value;

        /* Decode the property list */
        if ((*elink_fapl = H5P__decode(*pp)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTDECODE, FAIL, "can't decode property");

        *pp += fapl_size;
    } /* end if */
    else
        *elink_fapl = H5P_DEFAULT;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_dec() */

/*--------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_fapl_del
 *
 * Purpose:     Close the FAPL for link access
 *
 * Return:      Success:        Non-negative
 * 	            Failure:        Negative
 *
 *--------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_fapl_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    hid_t  l_fapl_id;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Get the FAPL ID */
    l_fapl_id = (*(const hid_t *)value);

    /* Close the FAPL */
    if (l_fapl_id != H5P_DEFAULT && H5I_dec_ref(l_fapl_id) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "unable to close ID for file access property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_del() */

/*--------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_fapl_copy
 *
 * Purpose:     Copy the FAPL for link access
 *
 * Return:      Success:        Non-negative
 * 	            Failure:        Negative
 *
 *--------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_fapl_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    hid_t  l_fapl_id;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Get the FAPL ID */
    l_fapl_id = (*(const hid_t *)value);

    /* Duplicate the FAPL, if it's non-default */
    if (l_fapl_id != H5P_DEFAULT) {
        H5P_genplist_t *l_fapl_plist;

        if (NULL == (l_fapl_plist = (H5P_genplist_t *)H5P_object_verify(l_fapl_id, H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "can't get property list");
        if (((*(hid_t *)value) = H5P_copy_plist(l_fapl_plist, false)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to copy file access property list");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__lacc_elink_fapl_cmp
 *
 * Purpose:        Callback routine which is called whenever the elink FAPL
 *                 property in the link access property list is
 *                 compared.
 *
 * Return:         zero if VALUE1 and VALUE2 are equal, non zero otherwise.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__lacc_elink_fapl_cmp(const void *value1, const void *value2, size_t H5_ATTR_UNUSED size)
{
    const hid_t    *fapl1 = (const hid_t *)value1;
    const hid_t    *fapl2 = (const hid_t *)value2;
    H5P_genplist_t *obj1, *obj2; /* Property lists to compare */
    int             ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check for comparison with default value */
    if (*fapl1 == 0 && *fapl2 > 0)
        HGOTO_DONE(1);
    if (*fapl1 > 0 && *fapl2 == 0)
        HGOTO_DONE(-1);

    /* Get the property list objects */
    obj1 = (H5P_genplist_t *)H5I_object(*fapl1);
    obj2 = (H5P_genplist_t *)H5I_object(*fapl2);

    /* Check for NULL property lists */
    if (obj1 == NULL && obj2 != NULL)
        HGOTO_DONE(1);
    if (obj1 != NULL && obj2 == NULL)
        HGOTO_DONE(-1);
    if (obj1 && obj2) {
        herr_t H5_ATTR_NDEBUG_UNUSED status;

        status = H5P__cmp_plist(obj1, obj2, &ret_value);
        assert(status >= 0);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_cmp() */

/*--------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_fapl_close
 *
 * Purpose:     Close the FAPL for link access
 *
 * Return:      Success:        Non-negative
 * 	            Failure:        Negative
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_fapl_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    hid_t  l_fapl_id;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Get the FAPL ID */
    l_fapl_id = (*(const hid_t *)value);

    /* Close the FAPL */
    if ((l_fapl_id > H5P_DEFAULT) && (H5I_dec_ref(l_fapl_id) < 0))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "unable to close ID for file access property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_fapl_close() */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_pref_set
 *
 * Purpose:     Copies an external link prefix property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_pref_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);

    /* Copy the prefix */
    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__lacc_elink_pref_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_pref_get
 *
 * Purpose:     Copies an external link prefix property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_pref_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);

    /* Copy the prefix */
    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__lacc_elink_pref_get() */

/*-------------------------------------------------------------------------
 * Function:       H5P__lacc_elink_pref_enc
 *
 * Purpose:        Callback routine which is called whenever the elink flags
 *                 property in the dataset access property list is
 *                 encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_pref_enc(const void *value, void **_pp, size_t *size)
{
    const char *elink_pref = *(const char *const *)value;
    uint8_t   **pp         = (uint8_t **)_pp;
    size_t      len        = 0;
    uint64_t    enc_value;
    unsigned    enc_size;

    FUNC_ENTER_PACKAGE_NOERR

    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* calculate prefix length */
    if (NULL != elink_pref)
        len = strlen(elink_pref);

    enc_value = (uint64_t)len;
    enc_size  = H5VM_limit_enc_size(enc_value);
    assert(enc_size < 256);

    if (NULL != *pp) {
        /* encode the length of the prefix */
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* encode the prefix */
        if (NULL != elink_pref) {
            H5MM_memcpy(*(char **)pp, elink_pref, len);
            *pp += len;
        } /* end if */
    }     /* end if */

    *size += (1 + enc_size);
    if (NULL != elink_pref)
        *size += len;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__lacc_elink_pref_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__lacc_elink_pref_dec
 *
 * Purpose:        Callback routine which is called whenever the elink prefix
 *                 property in the dataset access property list is
 *                 decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_pref_dec(const void **_pp, void *_value)
{
    char          **elink_pref = (char **)_value;
    const uint8_t **pp         = (const uint8_t **)_pp;
    size_t          len;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(pp);
    assert(*pp);
    assert(elink_pref);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Decode the value */
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    len = (size_t)enc_value;

    if (0 != len) {
        /* Make a copy of the user's prefix string */
        if (NULL == (*elink_pref = (char *)H5MM_malloc(len + 1)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "memory allocation failed for prefix");
        strncpy(*elink_pref, *(const char **)pp, len);
        (*elink_pref)[len] = '\0';

        *pp += len;
    } /* end if */
    else
        *elink_pref = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_pref_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_pref_del
 *
 * Purpose:     Frees memory used to store the external link prefix string
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_pref_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__lacc_elink_pref_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_pref_copy
 *
 * Purpose:     Creates a copy of the external link prefix string
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_pref_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__lacc_elink_pref_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__lacc_elink_pref_cmp
 *
 * Purpose:        Callback routine which is called whenever the elink prefix
 *                 property in the dataset creation property list is
 *                 compared.
 *
 * Return:         zero if VALUE1 and VALUE2 are equal, non zero otherwise.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__lacc_elink_pref_cmp(const void *value1, const void *value2, size_t H5_ATTR_UNUSED size)
{
    const char *pref1     = *(const char *const *)value1;
    const char *pref2     = *(const char *const *)value2;
    int         ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    if (NULL == pref1 && NULL != pref2)
        HGOTO_DONE(1);
    if (NULL != pref1 && NULL == pref2)
        HGOTO_DONE(-1);
    if (NULL != pref1 && NULL != pref2)
        ret_value = strcmp(pref1, pref2);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__lacc_elink_pref_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__lacc_elink_pref_close
 *
 * Purpose:     Frees memory used to store the external link prefix string
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__lacc_elink_pref_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__lacc_elink_pref_close() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_nlinks
 *
 * Purpose:     Set the number of soft or UD link traversals allowed before
 *              the library assumes it has found a cycle and aborts the
 *              traversal.
 *
 *              The limit on soft or UD link traversals is designed to
 *              terminate link traversal if one or more links form a cycle.
 *              However, users may have a file with a legitimate path
 *              formed of a large number of soft or user-defined links.
 *              This property can be used to allow traversal of as many
 *              links as desired.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_nlinks(hid_t plist_id, size_t nlinks)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iz", plist_id, nlinks);

    if (nlinks <= 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "number of links must be positive");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set number of links */
    if (H5P_set(plist, H5L_ACS_NLINKS_NAME, &nlinks) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set nlink info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_nlinks() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_nlinks
 *
 * Purpose:	Gets the number of soft or user-defined links that can be
 *              traversed before a failure occurs.
 *
 *              Retrieves the current setting for the nlinks property on
 *              the given property list.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_nlinks(hid_t plist_id, size_t *nlinks /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, nlinks);

    if (!nlinks)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pointer passed in");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the current number of links */
    if (H5P_get(plist, H5L_ACS_NLINKS_NAME, nlinks) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get number of links");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_elink_prefix
 *
 * Purpose:     Set a prefix to be applied to the path of any external links
 *              traversed.  The prefix is appended to the filename stored
 *              in the external link.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_elink_prefix(hid_t plist_id, const char *prefix)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", plist_id, prefix);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set prefix */
    if (H5P_set(plist, H5L_ACS_ELINK_PREFIX_NAME, &prefix) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set prefix info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_elink_prefix() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_elink_prefix
 *
 * Purpose:	Gets the prefix to be applied to any external link
 *              traversals made using this property list.
 *
 *              If the pointer is not NULL, it points to a user-allocated
 *              buffer.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Pget_elink_prefix(hid_t plist_id, char *prefix /*out*/, size_t size)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    char           *my_prefix; /* Library's copy of the prefix */
    size_t          len;       /* Length of prefix string */
    ssize_t         ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("Zs", "ixz", plist_id, prefix, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the current prefix */
    if (H5P_peek(plist, H5L_ACS_ELINK_PREFIX_NAME, &my_prefix) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get external link prefix");

    /* Check for prefix being set */
    if (my_prefix) {
        /* Copy to user's buffer, if given */
        len = strlen(my_prefix);
        if (prefix) {
            strncpy(prefix, my_prefix, size);
            if (len >= size)
                prefix[size - 1] = '\0';
        } /* end if */
    }     /* end if */
    else
        len = 0;

    /* Set return value */
    ret_value = (ssize_t)len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_elink_prefix() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_elink_fapl
 *
 * Purpose:     Sets the file access property list for link access
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_elink_fapl(hid_t lapl_id, hid_t fapl_id)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ii", lapl_id, fapl_id);

    /* Check arguments */
    if (NULL == (plist = H5P_object_verify(lapl_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a link access property list");

    /* Set the file access property list for the link access */
    if (H5P_set(plist, H5L_ACS_ELINK_FAPL_NAME, &fapl_id) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set fapl for link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_elink_fapl() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_elink_fapl
 *
 * Purpose:	Gets the file access property list identifier that is
 *		set for link access property.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Pget_elink_fapl(hid_t lapl_id)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    hid_t           ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("i", "i", lapl_id);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(lapl_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (H5P_get(plist, H5L_ACS_ELINK_FAPL_NAME, &ret_value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fapl for links");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_elink_fapl() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_elink_acc_flags
 *
 * Purpose:     Sets the file access flags to be used when traversing an
 *              external link.  This should be either H5F_ACC_RDONLY or
 *              H5F_ACC_RDWR, or H5F_ACC_DEFAULT to unset the value.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_elink_acc_flags(hid_t lapl_id, unsigned flags)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iIu", lapl_id, flags);

    /* Check that flags are valid */
    if ((flags != H5F_ACC_RDWR) && (flags != (H5F_ACC_RDWR | H5F_ACC_SWMR_WRITE)) &&
        (flags != H5F_ACC_RDONLY) && (flags != (H5F_ACC_RDONLY | H5F_ACC_SWMR_READ)) &&
        (flags != H5F_ACC_DEFAULT))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid file open flags");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(lapl_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set flags */
    if (H5P_set(plist, H5L_ACS_ELINK_FLAGS_NAME, &flags) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set access flags");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_elink_acc_flags() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_elink_acc_flags
 *
 * Purpose:     Gets the file access flags to be used when traversing an
 *              external link.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_elink_acc_flags(hid_t lapl_id, unsigned *flags /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", lapl_id, flags);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(lapl_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get flags */
    if (flags)
        if (H5P_get(plist, H5L_ACS_ELINK_FLAGS_NAME, flags) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, 0, "can't get access flags");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_elink_acc_flags() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_elink_cb
 *
 * Purpose:     Sets the file access flags to be used when traversing an
 *              external link.  This should be either H5F_ACC_RDONLY or
 *              H5F_ACC_RDWR.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_elink_cb(hid_t lapl_id, H5L_elink_traverse_t func, void *op_data)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5L_elink_cb_t  cb_info;             /* Callback info struct */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iLt*x", lapl_id, func, op_data);

    /* Check if the callback function is NULL and the user data is non-NULL.
     * This is almost certainly an error as the user data will not be used. */
    if (!func && op_data)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "callback is NULL while user data is not");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(lapl_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Populate the callback info struct */
    cb_info.func      = func;
    cb_info.user_data = op_data;

    /* Set callback info */
    if (H5P_set(plist, H5L_ACS_ELINK_CB_NAME, &cb_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set callback info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_elink_acc_flags() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_elink_cb
 *
 * Purpose:     Gets the file access flags to be used when traversing an
 *              external link.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_elink_cb(hid_t lapl_id, H5L_elink_traverse_t *func /*out*/, void **op_data /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5L_elink_cb_t  cb_info;             /* Callback info struct */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", lapl_id, func, op_data);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(lapl_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get callback_info */
    if (H5P_get(plist, H5L_ACS_ELINK_CB_NAME, &cb_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get callback info");

    if (func)
        *func = cb_info.func;
    if (op_data)
        *op_data = cb_info.user_data;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_elink_cb() */
