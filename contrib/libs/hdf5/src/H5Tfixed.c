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
 * Module Info: This module contains the functionality for fixed-point (i.e.
 *      integer) datatypes in the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"  /*generic functions			  */
#include "H5Eprivate.h" /*error handling			  */
#include "H5Iprivate.h" /*ID functions		   		  */
#include "H5Tpkg.h"     /*data-type functions			  */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_sign
 *
 * Purpose:	Retrieves the sign type for an integer type.
 *
 * Return:	Success:	The sign type.
 *
 *		Failure:	H5T_SGN_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_sign_t
H5Tget_sign(hid_t type_id)
{
    H5T_t     *dt = NULL;
    H5T_sign_t ret_value;

    FUNC_ENTER_API(H5T_SGN_ERROR)
    H5TRACE1("Ts", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_SGN_ERROR, "not an integer datatype");

    ret_value = H5T_get_sign(dt);

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5T_get_sign
 *
 * Purpose:	Private function for H5Tget_sign.  Retrieves the sign type
 *              for an integer type.
 *
 * Return:	Success:	The sign type.
 *
 *		Failure:	H5T_SGN_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_sign_t
H5T_get_sign(H5T_t const *dt)
{
    H5T_sign_t ret_value = H5T_SGN_ERROR; /* Return value */

    FUNC_ENTER_NOAPI(H5T_SGN_ERROR)

    assert(dt);

    /* Defer to parent */
    while (dt->shared->parent)
        dt = dt->shared->parent;

    /* Check args */
    if (H5T_INTEGER != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, H5T_SGN_ERROR, "operation not defined for datatype class");

    /* Sign */
    ret_value = dt->shared->u.atomic.u.i.sign;

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tset_sign
 *
 * Purpose:	Sets the sign property for an integer.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_sign(hid_t type_id, H5T_sign_t sign)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iTs", type_id, sign);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an integer datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "datatype is read-only");
    if (sign < H5T_SGN_NONE || sign >= H5T_NSGN)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "illegal sign type");
    if (H5T_ENUM == dt->shared->type && dt->shared->u.enumer.nmembs > 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not allowed after members are defined");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_INTEGER != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not defined for datatype class");

    /* Commit */
    dt->shared->u.atomic.u.i.sign = sign;

done:
    FUNC_LEAVE_API(ret_value)
}
