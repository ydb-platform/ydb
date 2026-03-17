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
 *      the datatype precision for the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Tpkg.h"     /* Datatypes				*/

/* Static local functions */
static herr_t H5T__set_precision(const H5T_t *dt, size_t prec);

/*-------------------------------------------------------------------------
 * Function:	H5Tget_precision
 *
 * Purpose:	Gets the precision of a datatype.  The precision is
 *		the number of significant bits which, unless padding is
 *		present, is 8 times larger than the value returned by
 *		H5Tget_size().
 *
 * Return:	Success:	Number of significant bits
 *
 *		Failure:	0 (all atomic types have at least one
 *				significant bit)
 *
 *-------------------------------------------------------------------------
 */
size_t
H5Tget_precision(hid_t type_id)
{
    H5T_t *dt;
    size_t ret_value;

    FUNC_ENTER_API(0)
    H5TRACE1("z", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not a datatype");

    /* Get precision */
    if ((ret_value = H5T_get_precision(dt)) == 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, 0, "can't get precision for specified datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_precision() */

/*-------------------------------------------------------------------------
 * Function:	H5T_get_precision
 *
 * Purpose:	Gets the precision of a datatype.  The precision is
 *		the number of significant bits which, unless padding is
 *		present, is 8 times larger than the value returned by
 *		H5Tget_size().
 *
 * Return:	Success:	Number of significant bits
 *		Failure:	0 (all atomic types have at least one
 *				significant bit)
 *
 *-------------------------------------------------------------------------
 */
size_t
H5T_get_precision(const H5T_t *dt)
{
    size_t ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI(0)

    /* Defer to parent*/
    while (dt->shared->parent)
        dt = dt->shared->parent;
    if (!H5T_IS_ATOMIC(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, 0, "operation not defined for specified datatype");

    /* Precision */
    ret_value = dt->shared->u.atomic.prec;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_precision() */

/*-------------------------------------------------------------------------
 * Function:	H5Tset_precision
 *
 * Purpose:	Sets the precision of a datatype.  The precision is
 *		the number of significant bits which, unless padding is
 *		present, is 8 times larger than the value returned by
 *		H5Tget_size().
 *
 *		If the precision is increased then the offset is decreased
 *		and then the size is increased to insure that significant
 *		bits do not "hang over" the edge of the datatype.
 *
 *		The precision property of strings is read-only.
 *
 *		When decreasing the precision of a floating point type, set
 *		the locations and sizes of the sign, mantissa, and exponent
 *		fields first.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_precision(hid_t type_id, size_t prec)
{
    H5T_t *dt        = NULL;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iz", type_id, prec);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is read-only");
    if (NULL != dt->vol_obj)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is committed");
    if (prec == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "precision must be positive");
    if (H5T_ENUM == dt->shared->type && dt->shared->u.enumer.nmembs > 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "operation not allowed after members are defined");
    if (H5T_STRING == dt->shared->type)
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "precision for this type is read-only");
    if (H5T_COMPOUND == dt->shared->type || H5T_OPAQUE == dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "operation not defined for specified datatype");

    /* Do the work */
    if (H5T__set_precision(dt, prec) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "unable to set precision");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5T__set_precision
 *
 * Purpose:	Sets the precision of a datatype.  The precision is
 *		the number of significant bits which, unless padding is
 *		present, is 8 times larger than the value returned by
 *		H5Tget_size().
 *
 *		If the precision is increased then the offset is decreased
 *		and then the size is increased to insure that significant
 *		bits do not "hang over" the edge of the datatype.
 *
 *		The precision property of strings is read-only.
 *
 *		When decreasing the precision of a floating point type, set
 *		the locations and sizes of the sign, mantissa, and exponent
 *		fields first.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__set_precision(const H5T_t *dt, size_t prec)
{
    size_t offset, size;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dt);
    assert(prec > 0);
    assert(H5T_OPAQUE != dt->shared->type);
    assert(H5T_COMPOUND != dt->shared->type);
    assert(H5T_STRING != dt->shared->type);
    assert(!(H5T_ENUM == dt->shared->type && 0 == dt->shared->u.enumer.nmembs));

    if (dt->shared->parent) {
        if (H5T__set_precision(dt->shared->parent, prec) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "unable to set precision for base type");

        /* Adjust size of datatype appropriately */
        if (dt->shared->type == H5T_ARRAY)
            dt->shared->size = dt->shared->parent->shared->size * dt->shared->u.array.nelem;
        else if (dt->shared->type != H5T_VLEN)
            dt->shared->size = dt->shared->parent->shared->size;
    }
    else {
        if (H5T_IS_ATOMIC(dt->shared)) {
            /* Adjust the offset and size */
            offset = dt->shared->u.atomic.offset;
            size   = dt->shared->size;
            if (prec > 8 * size)
                offset = 0;
            else if (offset + prec > 8 * size)
                offset = 8 * size - prec;
            if (prec > 8 * size)
                size = (prec + 7) / 8;

            /* Check that things are still kosher */
            switch (dt->shared->type) {
                case H5T_INTEGER:
                case H5T_TIME:
                case H5T_BITFIELD:
                    /* nothing to check */
                    break;

                case H5T_FLOAT:
                    /*
                     * The sign, mantissa, and exponent fields should be adjusted
                     * first when decreasing the precision of a floating point
                     * type.
                     */
                    if (dt->shared->u.atomic.u.f.sign >= prec + offset ||
                        dt->shared->u.atomic.u.f.epos + dt->shared->u.atomic.u.f.esize > prec + offset ||
                        dt->shared->u.atomic.u.f.mpos + dt->shared->u.atomic.u.f.msize > prec + offset)
                        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                    "adjust sign, mantissa, and exponent fields first");
                    break;

                case H5T_NO_CLASS:
                case H5T_STRING:
                case H5T_OPAQUE:
                case H5T_COMPOUND:
                case H5T_REFERENCE:
                case H5T_ENUM:
                case H5T_VLEN:
                case H5T_ARRAY:
                case H5T_NCLASSES:
                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "operation not defined for datatype class");
            } /* end switch */

            /* Commit */
            dt->shared->size            = size;
            dt->shared->u.atomic.offset = offset;
            dt->shared->u.atomic.prec   = prec;
        } /* end if */
        else
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not defined for specified datatype");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
}
