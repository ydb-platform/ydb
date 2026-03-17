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
 * Module Info: This module contains the functionality for setting & querying
 *      the datatype padding for the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Tpkg.h"     /* Datatypes				*/

/*-------------------------------------------------------------------------
 * Function:	H5Tget_pad
 *
 * Purpose:	Gets the least significant pad type and the most significant
 *		pad type and returns their values through the LSB and MSB
 *		arguments, either of which may be the null pointer.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tget_pad(hid_t type_id, H5T_pad_t *lsb /*out*/, H5T_pad_t *msb /*out*/)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", type_id, lsb, msb);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (!H5T_IS_ATOMIC(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "operation not defined for specified data type");

    /* Get values */
    if (lsb)
        *lsb = dt->shared->u.atomic.lsb_pad;
    if (msb)
        *msb = dt->shared->u.atomic.msb_pad;

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tset_pad
 *
 * Purpose:	Sets the LSB and MSB pad types.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_pad(hid_t type_id, H5T_pad_t lsb, H5T_pad_t msb)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iTpTp", type_id, lsb, msb);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "data type is read-only");
    if (lsb < H5T_PAD_ZERO || lsb >= H5T_NPAD || msb < H5T_PAD_ZERO || msb >= H5T_NPAD)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pad type");
    if (H5T_ENUM == dt->shared->type && dt->shared->u.enumer.nmembs > 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not allowed after members are defined");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (!H5T_IS_ATOMIC(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "operation not defined for specified data type");

    /* Commit */
    dt->shared->u.atomic.lsb_pad = lsb;
    dt->shared->u.atomic.msb_pad = msb;

done:
    FUNC_LEAVE_API(ret_value)
}
