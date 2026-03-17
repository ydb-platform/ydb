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
 * Purpose:	Generic Property Functions
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* Files		  	        */
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Ppkg.h"      /* Property lists		  	*/
#include "H5VMprivate.h" /* Vector functions			*/

/****************/
/* Local Macros */
/****************/

/* Version # of encoded property lists */
#define H5P_ENCODE_VERS 0

/******************/
/* Local Typedefs */
/******************/

/* Typedef for iterator when encoding a property list */
typedef struct {
    bool    encode;       /* Whether the property list should be encoded */
    size_t *enc_size_ptr; /* Pointer to size of encoded buffer */
    void  **pp;           /* Pointer to encoding buffer pointer */
} H5P_enc_iter_ud_t;

/********************/
/* Local Prototypes */
/********************/

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
 * Function:       H5P__encode_size_t
 *
 * Purpose:        Generic encoding callback routine for 'size_t' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__encode_size_t(const void *value, void **_pp, size_t *size)
{
    uint64_t  enc_value = (uint64_t) * (const size_t *)value; /* Property value to encode */
    uint8_t **pp        = (uint8_t **)_pp;
    unsigned  enc_size  = H5VM_limit_enc_size(enc_value); /* Size of encoded property */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    assert(enc_size < 256);
    assert(size);

    if (NULL != *pp) {
        /* Encode the size */
        *(*pp)++ = (uint8_t)enc_size;

        /* Encode the value */
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);
    } /* end if */

    /* Set size needed for encoding */
    *size += (1 + enc_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_size_t() */

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_hsize_t
 *
 * Purpose:        Generic encoding callback routine for 'hsize_t' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__encode_hsize_t(const void *value, void **_pp, size_t *size)
{
    uint64_t  enc_value = (uint64_t) * (const hsize_t *)value; /* Property value to encode */
    unsigned  enc_size  = H5VM_limit_enc_size(enc_value);      /* Size of encoded property */
    uint8_t **pp        = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    HDcompile_assert(sizeof(hsize_t) <= sizeof(uint64_t));
    assert(enc_size < 256);
    assert(size);

    if (NULL != *pp) {
        *(*pp)++ = (uint8_t)enc_size;

        /* Encode the value */
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);
    } /* end if */

    /* Set size needed for encoding */
    *size += (1 + enc_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_hsize_t() */

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_unsigned
 *
 * Purpose:        Generic encoding callback routine for 'unsigned' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__encode_unsigned(const void *value, void **_pp, size_t *size)
{
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(value);
    assert(size);

    if (NULL != *pp) {
        /* Encode the size */
        *(*pp)++ = (uint8_t)sizeof(unsigned);

        /* Encode the value */
        H5_ENCODE_UNSIGNED(*pp, *(const unsigned *)value);
    } /* end if */

    /* Set size needed for encoding */
    *size += (1 + sizeof(unsigned));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_unsigned() */

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_uint8_t
 *
 * Purpose:        Generic encoding callback routine for 'uint8_t' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__encode_uint8_t(const void *value, void **_pp, size_t *size)
{
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(value);
    assert(size);

    if (NULL != *pp) {
        /* Encode the value */
        *(*pp)++ = *(const uint8_t *)value;
    } /* end if */

    /* Set size needed for encoding */
    *size += 1;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_uint8_t() */

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_bool
 *
 * Purpose:        Generic encoding callback routine for 'bool' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__encode_bool(const void *value, void **_pp, size_t *size)
{
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(value);
    assert(size);

    if (NULL != *pp)
        /* Encode the value */
        *(*pp)++ = (uint8_t) * (const bool *)value;

    /* Set size needed for encoding */
    *size += 1;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_bool() */

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_double
 *
 * Purpose:        Generic encoding callback routine for 'double' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__encode_double(const void *value, void **_pp, size_t *size)
{
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(value);
    assert(size);

    if (NULL != *pp) {
        /* Encode the size */
        *(*pp)++ = (uint8_t)sizeof(double);

        /* Encode the value */
        H5_ENCODE_DOUBLE(*pp, *(const double *)value);
    } /* end if */

    /* Set size needed for encoding */
    *size += (1 + sizeof(double));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_double() */

/*--------------------------------------------------------------------------
 NAME
    H5P__encode_cb
 PURPOSE
    Internal callback routine when iterating over properties while encoding
    a property list.
 USAGE
    int H5P__encode_cb(item, key, udata)
        H5P_genprop_t *prop;        IN: Pointer to the property
        void *udata;                IN/OUT: Pointer to iteration data from user
 RETURNS
    Success: H5_ITER_CONT
    Fail: H5_ITER_ERROR
 DESCRIPTION
    This routine encodes a property in a property list
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5P__encode_cb(H5P_genprop_t *prop, void *_udata)
{
    H5P_enc_iter_ud_t *udata     = (H5P_enc_iter_ud_t *)_udata; /* Pointer to user data */
    int                ret_value = H5_ITER_CONT;                /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(prop);
    assert(udata);

    /* Check if this property can be encoded */
    if (prop->encode) {
        size_t prop_name_len;  /* Length of property's name */
        size_t prop_value_len; /* Encoded size of property's value */

        /* Encode (or not, if the 'encode' flag is off) the property's name */
        prop_name_len = strlen(prop->name) + 1;
        if (udata->encode) {
            strcpy((char *)*(udata->pp), prop->name);
            *(uint8_t **)(udata->pp) += prop_name_len;
        } /* end if */
        *(udata->enc_size_ptr) += prop_name_len;

        /* Encode (or not, if *(udata->pp) is NULL) the property value */
        prop_value_len = 0;
        if ((prop->encode)(prop->value, udata->pp, &prop_value_len) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, H5_ITER_ERROR, "property encoding routine failed");
        *(udata->enc_size_ptr) += prop_value_len;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__encode_cb() */

/*-------------------------------------------------------------------------
 NAME
    H5P__encode
 PURPOSE
    Internal routine to encode a property list into a binary buffer.
 USAGE
    herr_t H5P__encode(plist, enc_all_prop, buf, nalloc)
        const H5P_genplist_t *plist;  IN: Property list to encode
        bool enc_all_prop;  IN: Whether to encode all properties (true),
                or just non-default (i.e. changed) properties (false).
        uint8_t *buf;    OUT: buffer to hold the encoded plist
        size_t *nalloc;  IN/OUT: size of buffer needed to encode plist
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Encodes a property list into a binary buffer. If the buffer is NULL, then
    the call will set the size needed to encode the plist in nalloc. Otherwise
    the routine will encode the plist in buf.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__encode(const H5P_genplist_t *plist, bool enc_all_prop, void *buf, size_t *nalloc)
{
    H5P_enc_iter_ud_t udata;                 /* User data for property iteration callback */
    uint8_t          *p = (uint8_t *)buf;    /* Temporary pointer to encoding buffer */
    int               idx;                   /* Index of property to start at */
    size_t            encode_size = 0;       /* Size of buffer needed to encode properties */
    bool              encode      = true;    /* Whether the property list should be encoded */
    herr_t            ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    if (NULL == nalloc)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad allocation size pointer");

    /* If the buffer is NULL, then this call to H5P__encode will return how much
     * space is needed to encode a property.
     */
    if (NULL == p)
        encode = false;

    /* Encode property list description info */
    if (encode) {
        /* Version # of property list encoding */
        *p++ = (uint8_t)H5P_ENCODE_VERS;

        /* Type of property list */
        *p++ = (uint8_t)plist->pclass->type;
    } /* end if */
    encode_size += 2;

    /* Initialize user data for iteration callback */
    udata.encode       = encode;
    udata.enc_size_ptr = &encode_size;
    udata.pp           = (void **)&p;

    /* Iterate over all properties in property list, encoding them */
    idx = 0;
    if (H5P__iterate_plist(plist, enc_all_prop, &idx, H5P__encode_cb, &udata) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADITER, FAIL, "can't iterate over properties");

    /* Encode a terminator for list of properties */
    if (encode)
        *p++ = 0;
    encode_size++;

    /* Set the size of the buffer needed/used to encode the property list */
    *nalloc = encode_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__encode() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_size_t
 *
 * Purpose:        Generic decoding callback routine for 'size_t' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__decode_size_t(const void **_pp, void *_value)
{
    size_t         *value = (size_t *)_value; /* Property value to return */
    const uint8_t **pp    = (const uint8_t **)_pp;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Decode the value */
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    H5_CHECKED_ASSIGN(*value, size_t, enc_value, uint64_t);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__decode_size_t() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_hsize_t
 *
 * Purpose:        Generic decoding callback routine for 'hsize_t' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__decode_hsize_t(const void **_pp, void *_value)
{
    hsize_t        *value = (hsize_t *)_value; /* Property value to return */
    const uint8_t **pp    = (const uint8_t **)_pp;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    HDcompile_assert(sizeof(hsize_t) <= sizeof(uint64_t));
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Decode the value */
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    H5_CHECKED_ASSIGN(*value, hsize_t, enc_value, uint64_t);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__decode_hsize_t() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_unsigned
 *
 * Purpose:        Generic decoding callback routine for 'unsigned' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__decode_unsigned(const void **_pp, void *_value)
{
    unsigned       *value = (unsigned *)_value; /* Property value to return */
    const uint8_t **pp    = (const uint8_t **)_pp;
    unsigned        enc_size;            /* Size of encoded property */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the size */
    enc_size = *(*pp)++;
    if (enc_size != sizeof(unsigned))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "unsigned value can't be decoded");

    H5_DECODE_UNSIGNED(*pp, *value);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__decode_unsigned() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_uint8_t
 *
 * Purpose:        Generic decoding callback routine for 'uint8_t' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__decode_uint8_t(const void **_pp, void *_value)
{
    uint8_t        *value     = (uint8_t *)_value; /* Property value to return */
    const uint8_t **pp        = (const uint8_t **)_pp;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the value */
    *value = *(*pp)++;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__decode_uint8_t() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_bool
 *
 * Purpose:        Generic decoding callback routine for 'bool' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__decode_bool(const void **_pp, void *_value)
{
    bool           *value     = (bool *)_value; /* Property value to return */
    const uint8_t **pp        = (const uint8_t **)_pp;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the value */
    *value = (bool)*(*pp)++;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__decode_bool() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_double
 *
 * Purpose:        Generic decoding callback routine for 'double' properties.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__decode_double(const void **_pp, void *_value)
{
    double         *value = (double *)_value; /* Property value to return */
    const uint8_t **pp    = (const uint8_t **)_pp;
    unsigned        enc_size;            /* Size of encoded property */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the size */
    enc_size = *(*pp)++;
    if (enc_size != sizeof(double))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "double value can't be decoded");

    H5_DECODE_DOUBLE(*pp, *value);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__decode_double() */

/*-------------------------------------------------------------------------
 NAME
    H5P__decode
 PURPOSE
    Internal routine to decode a property list from a binary buffer.
 USAGE
    H5P_genplist_t *H5P__decode(buf)
        const void *buf;    IN: buffer that holds the encoded plist
 RETURNS
    Returns non-negative ID of new property list object on success, negative
        on failure.
 DESCRIPTION
     Decodes a property list from a binary buffer. The contents of the buffer
     contain the values for the corresponding properties of the plist. The decode
     callback of a certain property decodes its value from the buffer and sets it
     in the property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     Properties in the property list that are not encoded in the serialized
     form retain their default value.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5P__decode(const void *buf)
{
    H5P_genplist_t  *plist;                            /* Property list to decode into */
    void            *value_buf = NULL;                 /* Pointer to buffer to use when decoding values */
    const uint8_t   *p         = (const uint8_t *)buf; /* Current pointer into buffer */
    H5P_plist_type_t type;                             /* Type of encoded property list */
    hid_t            plist_id       = -1;              /* ID of new property list */
    size_t           value_buf_size = 0;               /* Size of current value buffer */
    uint8_t          vers;                             /* Version of encoded property list */
    hid_t            ret_value = H5I_INVALID_HID;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    if (NULL == p)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "decode buffer is NULL");

    /* Get the version number of the encoded property list */
    vers = (uint8_t)*p++;
    if ((uint8_t)H5P_ENCODE_VERS != vers)
        HGOTO_ERROR(H5E_PLIST, H5E_VERSION, FAIL, "bad version # of encoded information, expected %u, got %u",
                    (unsigned)H5P_ENCODE_VERS, (unsigned)vers);

    /* Get the type of the property list */
    type = (H5P_plist_type_t)*p++;
    if (type <= H5P_TYPE_USER || type >= H5P_TYPE_MAX_TYPE)
        HGOTO_ERROR(H5E_PLIST, H5E_BADRANGE, FAIL, "bad type of encoded information: %u", (unsigned)type);

    /* Create new property list of the specified type */
    if ((plist_id = H5P__new_plist_of_type(type)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_VERSION, FAIL, "can't create property list of type: %u\n", (unsigned)type);

    /* Get the property list object */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a property class");

    /* Loop over encoded properties, deserializing their values */
    while (p) {
        H5P_genprop_t *prop; /* Pointer to property with same name */
        const char    *name; /* Pointer to property list name */

        /* Check for end of serialized list of properties */
        if (0 == *p)
            break;

        /* Get property list name */
        name = (const char *)p;
        p += strlen(name) + 1;

        /* Find property with name */
        if (NULL == (prop = H5P__find_prop_plist(plist, name)))
            HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property doesn't exist: '%s'", name);

        /* Check if we should increase the size of the value buffer */
        if (prop->size > value_buf_size) {
            if (NULL == (value_buf = H5MM_realloc(value_buf, prop->size)))
                HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "decoding buffer allocation failed");
            value_buf_size = prop->size;
        } /* end if */

        /* Decode serialized value */
        if (prop->decode) {
            if ((prop->decode)((const void **)&p, value_buf) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTDECODE, FAIL,
                            "property decoding routine failed, property: '%s'", name);
        } /* end if */
        else
            HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "no decode callback for property: '%s'", name);

        /* Set the value for the property */
        if (H5P_poke(plist, name, value_buf) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value for property: '%s'", name);
    } /* end while */

    /* Set return value */
    ret_value = plist_id;

done:
    /* Release resources */
    if (value_buf)
        value_buf = H5MM_xfree(value_buf);

    /* Cleanup on error */
    if (ret_value < 0) {
        if (plist_id > 0 && H5I_dec_ref(plist_id) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, FAIL,
                        "unable to close partially initialized property list");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__decode() */
