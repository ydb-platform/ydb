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
 * Module Info: This module contains the functionality for opaque
 *      datatypes in the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Tpkg.h"      /* Datatypes				*/

/*-------------------------------------------------------------------------
 * Function:	H5Tset_tag
 *
 * Purpose:	Tag an opaque datatype with a unique ASCII identifier.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_tag(hid_t type_id, const char *tag)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", type_id, tag);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "data type is read-only");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_OPAQUE != dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an opaque data type");
    if (!tag)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no tag");
    if (strlen(tag) >= H5T_OPAQUE_TAG_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "tag too long");

    /* Commit */
    H5MM_xfree(dt->shared->u.opaque.tag);
    dt->shared->u.opaque.tag = H5MM_strdup(tag);

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5Tget_tag
 *
 * Purpose:	Get the tag associated with an opaque datatype.
 *
 * Return:	A pointer to an allocated string. The caller should free
 *              the string. NULL is returned for errors.
 *
 *-------------------------------------------------------------------------
 */
char *
H5Tget_tag(hid_t type_id)
{
    H5T_t *dt = NULL;
    char  *ret_value;

    FUNC_ENTER_API(NULL)
    H5TRACE1("*s", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data type");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_OPAQUE != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "operation not defined for data type class");

    /* result */
    if (NULL == (ret_value = H5MM_strdup(dt->shared->u.opaque.tag)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

done:
    FUNC_LEAVE_API(ret_value)
}
