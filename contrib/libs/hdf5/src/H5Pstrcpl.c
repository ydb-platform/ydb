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
 * Created:		H5Pstrcpl.c
 *
 * Purpose:		String creation property list class routines
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
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Fprivate.h" /* Files */
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Ppkg.h"     /* Property lists		  	*/

/****************/
/* Local Macros */
/****************/

/* ========  String creation properties ======== */
/* Definitions for character set encoding property */
#define H5P_STRCRT_CHAR_ENCODING_SIZE sizeof(H5T_cset_t)
#define H5P_STRCRT_CHAR_ENCODING_DEF  H5F_DEFAULT_CSET
#define H5P_STRCRT_CHAR_ENCODING_ENC  H5P__strcrt_char_encoding_enc
#define H5P_STRCRT_CHAR_ENCODING_DEC  H5P__strcrt_char_encoding_dec

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
static herr_t H5P__strcrt_reg_prop(H5P_genclass_t *pclass);

/* encode & decode callbacks */
static herr_t H5P__strcrt_char_encoding_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__strcrt_char_encoding_dec(const void **_pp, void *value);

/*********************/
/* Package Variables */
/*********************/

/* String creation property list class library initialization object */
const H5P_libclass_t H5P_CLS_STRCRT[1] = {{
    "string create",        /* Class name for debugging     */
    H5P_TYPE_STRING_CREATE, /* Class type                   */

    &H5P_CLS_ROOT_g,             /* Parent class                 */
    &H5P_CLS_STRING_CREATE_g,    /* Pointer to class             */
    &H5P_CLS_STRING_CREATE_ID_g, /* Pointer to class ID          */
    NULL,                        /* Pointer to default property list ID */
    H5P__strcrt_reg_prop,        /* Default property registration routine */

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
static const H5T_cset_t H5P_def_char_encoding_g =
    H5P_STRCRT_CHAR_ENCODING_DEF; /* Default character set encoding */

/*-------------------------------------------------------------------------
 * Function:    H5P__strcrt_reg_prop
 *
 * Purpose:     Register the string creation property list class's properties
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__strcrt_reg_prop(H5P_genclass_t *pclass)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Register character encoding */
    if (H5P__register_real(pclass, H5P_STRCRT_CHAR_ENCODING_NAME, H5P_STRCRT_CHAR_ENCODING_SIZE,
                           &H5P_def_char_encoding_g, NULL, NULL, NULL, H5P_STRCRT_CHAR_ENCODING_ENC,
                           H5P_STRCRT_CHAR_ENCODING_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__strcrt_reg_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_char_encoding
 *
 * Purpose:     Sets the character encoding of the string.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_char_encoding(hid_t plist_id, H5T_cset_t encoding)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iTc", plist_id, encoding);

    /* Check arguments */
    if (encoding <= H5T_CSET_ERROR || encoding >= H5T_NCSET)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "character encoding is not valid");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_STRING_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set the character encoding */
    if (H5P_set(plist, H5P_STRCRT_CHAR_ENCODING_NAME, &encoding) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set character encoding");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5P_set_char_encoding() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_char_encoding
 *
 * Purpose:     Gets the character encoding of the string.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_char_encoding(hid_t plist_id, H5T_cset_t *encoding /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, encoding);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_STRING_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value */
    if (encoding)
        if (H5P_get(plist, H5P_STRCRT_CHAR_ENCODING_NAME, encoding) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get character encoding flag");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_char_encoding() */

/*-------------------------------------------------------------------------
 * Function:       H5P__strcrt_char_encoding_enc
 *
 * Purpose:        Callback routine which is called whenever the character
 *                 set encoding property in the string create property list
 *                 is encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__strcrt_char_encoding_enc(const void *value, void **_pp, size_t *size)
{
    const H5T_cset_t *encoding = (const H5T_cset_t *)value; /* Create local alias for values */
    uint8_t         **pp       = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(encoding);
    assert(size);

    if (NULL != *pp)
        /* Encode character set encoding */
        *(*pp)++ = (uint8_t)*encoding;

    /* Size of character set encoding */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__strcrt_char_encoding_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__strcrt_char_encoding_dec
 *
 * Purpose:        Callback routine which is called whenever the character
 *                 set encoding property in the string create property list
 *                 is decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__strcrt_char_encoding_dec(const void **_pp, void *_value)
{
    H5T_cset_t     *encoding = (H5T_cset_t *)_value; /* Character set encoding */
    const uint8_t **pp       = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(encoding);

    /* Decode character set encoding */
    *encoding = (H5T_cset_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__strcrt_char_encoding_dec() */
