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
 * H5Itest.c - ID testing functions
 */

/****************/
/* Module Setup */
/****************/

#include "H5Imodule.h" /* This source code file is part of the H5I module */
#define H5I_TESTING    /*suppress warning about H5I testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Ipkg.h"      /* IDs                                      */

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

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5I__get_name_test
 *
 * Purpose:     Testing version of H5Iget_name()
 *
 * Return:      Success: The length of name.
 *              Failure: -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5I__get_name_test(hid_t id, char *name /*out*/, size_t size, bool *cached)
{
    H5VL_object_t *vol_obj;                 /* Object of id */
    H5G_loc_t      loc;                     /* Object location */
    bool           api_ctx_pushed  = false; /* Whether API context pushed */
    bool           vol_wrapper_set = false; /* Whether the VOL object wrapping context was set up */
    size_t         name_len        = 0;     /* Length of name */
    ssize_t        ret_value       = -1;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, (-1), "can't set API context");
    api_ctx_pushed = true;

    /* Get the object pointer */
    if (NULL == (vol_obj = H5VL_vol_object(id)))
        HGOTO_ERROR(H5E_ID, H5E_BADTYPE, (-1), "invalid identifier");

    /* Set wrapper info in API context */
    if (H5VL_set_vol_wrapper(vol_obj) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTSET, (-1), "can't set VOL wrapper info");
    vol_wrapper_set = true;

    /* Get object location */
    if (H5G_loc(id, &loc) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTGET, (-1), "can't retrieve object location");

    /* Call internal group routine to retrieve object's name */
    if (H5G_get_name(&loc, name, size, &name_len, cached) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTGET, (-1), "can't retrieve object name");

    /* Set return value */
    ret_value = (ssize_t)name_len;

done:
    /* Reset object wrapping info in API context */
    if (vol_wrapper_set && H5VL_reset_vol_wrapper() < 0)
        HDONE_ERROR(H5E_ID, H5E_CANTRESET, (-1), "can't reset VOL wrapper info");

    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, (-1), "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5I__get_name_test() */
