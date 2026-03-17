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
 * Purpose:     Object token callbacks for the native VOL connector
 */

/****************/
/* Module Setup */
/****************/

/***********/
/* Headers */
/***********/
#include "H5private.h"          /* Generic Functions        */
#include "H5Eprivate.h"         /* Error handling           */
#include "H5MMprivate.h"        /* Memory handling          */
#include "H5VLnative_private.h" /* Native VOL connector     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

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

/*---------------------------------------------------------------------------
 * Function:    H5VL__native_token_cmp
 *
 * Purpose:     Compare two of the connector's object tokens, setting
 *              *cmp_value, following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    (can't fail)
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL__native_token_cmp(void H5_ATTR_UNUSED *obj, const H5O_token_t *token1, const H5O_token_t *token2,
                       int *cmp_value)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check parameters */
    assert(token1);
    assert(token2);

    *cmp_value = memcmp(token1, token2, sizeof(H5O_token_t));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_token_cmp() */

/*---------------------------------------------------------------------------
 * Function:    H5VL__native_token_to_str
 *
 * Purpose:     Serialize an object token into a string
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL__native_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token, char **token_str)
{
    haddr_t addr;
    size_t  addr_ndigits;
    herr_t  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    assert(obj);
    assert(token);

    if (H5VL_native_token_to_addr(obj, obj_type, *token, &addr) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, FAIL, "can't convert object token to address");

    if (addr == 0)
        addr_ndigits = 1;
    else
        addr_ndigits = (size_t)(floor(log10((double)addr)) + 1);

    if (NULL == (*token_str = H5MM_malloc(addr_ndigits + 1)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate buffer for token string");

    snprintf(*token_str, addr_ndigits + 1, "%" PRIuHADDR, addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_token_to_str() */

/*---------------------------------------------------------------------------
 * Function:    H5VL__native_str_to_token
 *
 * Purpose:     Deserialize a string into an object token
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL__native_str_to_token(void *obj, H5I_type_t obj_type, const char *token_str, H5O_token_t *token)
{
    haddr_t addr;
    herr_t  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    assert(token_str);

    sscanf(token_str, "%" PRIuHADDR, &addr);

    if (H5VL_native_addr_to_token(obj, obj_type, addr, token) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, FAIL, "can't convert address to object token");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_str_to_token() */
