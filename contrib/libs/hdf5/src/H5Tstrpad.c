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
 *      the datatype string padding for the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Tpkg.h"     /* Datatypes				*/

/*-------------------------------------------------------------------------
 * Function:	H5Tget_strpad
 *
 * Purpose:	The method used to store character strings differs with the
 *		programming language: C usually null terminates strings while
 *		Fortran left-justifies and space-pads strings.	This property
 *		defines the storage mechanism for the string.
 *
 * Return:	Success:	The character set of a string type.
 *
 *		Failure:	H5T_STR_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_str_t
H5Tget_strpad(hid_t type_id)
{
    H5T_t    *dt = NULL;
    H5T_str_t ret_value;

    FUNC_ENTER_API(H5T_STR_ERROR)
    H5TRACE1("Tz", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_STR_ERROR, "not a datatype");
    while (dt->shared->parent && !H5T_IS_STRING(dt->shared))
        dt = dt->shared->parent; /*defer to parent*/
    if (!H5T_IS_STRING(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, H5T_STR_ERROR, "operation not defined for datatype class");

    /* result */
    if (H5T_IS_FIXED_STRING(dt->shared))
        ret_value = dt->shared->u.atomic.u.s.pad;
    else
        ret_value = dt->shared->u.vlen.pad;

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tset_strpad
 *
 * Purpose:	The method used to store character strings differs with the
 *		programming language: C usually null terminates strings while
 *		Fortran left-justifies and space-pads strings.	This property
 *		defines the storage mechanism for the string.
 *
 *		When converting from a long string to a short string if the
 *		short string is H5T_STR_NULLPAD or H5T_STR_SPACEPAD then the
 *		string is simply truncated; otherwise if the short string is
 *		H5T_STR_NULLTERM it will be truncated and a null terminator
 *		is appended.
 *
 *		When converting from a short string to a long string, the
 *		long string is padded on the end by appending nulls or
 *		spaces.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_strpad(hid_t type_id, H5T_str_t strpad)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iTz", type_id, strpad);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "datatype is read-only");
    if (strpad < H5T_STR_NULLTERM || strpad >= H5T_NSTR)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "illegal string pad type");
    while (dt->shared->parent && !H5T_IS_STRING(dt->shared))
        dt = dt->shared->parent; /*defer to parent*/
    if (!H5T_IS_STRING(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "operation not defined for datatype class");

    /* Commit */
    if (H5T_IS_FIXED_STRING(dt->shared))
        dt->shared->u.atomic.u.s.pad = strpad;
    else
        dt->shared->u.vlen.pad = strpad;

done:
    FUNC_LEAVE_API(ret_value)
}
