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
 *      the character set (cset) for the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"  /*generic functions			  */
#include "H5Eprivate.h" /*error handling			  */
#include "H5Iprivate.h" /*ID functions		   		  */
#include "H5Tpkg.h"     /*data-type functions			  */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_cset
 *
 * Purpose:	HDF5 is able to distinguish between character sets of
 *		different nationalities and to convert between them to the
 *		extent possible.
 *
 * Return:	Success:	The character set of a string type.
 *
 *		Failure:	H5T_CSET_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_cset_t
H5Tget_cset(hid_t type_id)
{
    H5T_t     *dt;
    H5T_cset_t ret_value;

    FUNC_ENTER_API(H5T_CSET_ERROR)
    H5TRACE1("Tc", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_CSET_ERROR, "not a data type");
    while (dt->shared->parent && !H5T_IS_STRING(dt->shared))
        dt = dt->shared->parent; /*defer to parent*/
    if (!H5T_IS_STRING(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, H5T_CSET_ERROR,
                    "operation not defined for data type class");

    /* result */
    if (H5T_IS_FIXED_STRING(dt->shared))
        ret_value = dt->shared->u.atomic.u.s.cset;
    else
        ret_value = dt->shared->u.vlen.cset;

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tset_cset
 *
 * Purpose:	HDF5 is able to distinguish between character sets of
 *		different nationalities and to convert between them to the
 *		extent possible.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_cset(hid_t type_id, H5T_cset_t cset)
{
    H5T_t *dt;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iTc", type_id, cset);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "data type is read-only");
    if (cset < H5T_CSET_ASCII || cset >= H5T_NCSET)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "illegal character set type");
    while (dt->shared->parent && !H5T_IS_STRING(dt->shared))
        dt = dt->shared->parent; /*defer to parent*/
    if (!H5T_IS_STRING(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "operation not defined for data type class");

    /* Commit */
    if (H5T_IS_FIXED_STRING(dt->shared))
        dt->shared->u.atomic.u.s.cset = cset;
    else
        dt->shared->u.vlen.cset = cset;

done:
    FUNC_LEAVE_API(ret_value)
}
