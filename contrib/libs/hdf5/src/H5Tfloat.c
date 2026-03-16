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
 * Module Info: This module contains the functionality for floating-point
 *      datatypes in the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"  /*generic functions			  */
#include "H5Eprivate.h" /*error handling			  */
#include "H5Iprivate.h" /*ID functions		   		  */
#include "H5Tpkg.h"     /*data-type functions			  */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_fields
 *
 * Purpose:	Returns information about the locations of the various bit
 *		fields of a floating point datatype.  The field positions
 *		are bit positions in the significant region of the datatype.
 *		Bits are numbered with the least significant bit number zero.
 *
 *		Any (or even all) of the arguments can be null pointers.
 *
 * Return:	Success:	Non-negative, field locations and sizes are
 *				returned through the arguments.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tget_fields(hid_t type_id, size_t *spos /*out*/, size_t *epos /*out*/, size_t *esize /*out*/,
              size_t *mpos /*out*/, size_t *msize /*out*/)
{
    H5T_t *dt;                  /* Datatype */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "ixxxxx", type_id, spos, epos, esize, mpos, msize);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "operation not defined for datatype class");

    /* Get values */
    if (spos)
        *spos = dt->shared->u.atomic.u.f.sign;
    if (epos)
        *epos = dt->shared->u.atomic.u.f.epos;
    if (esize)
        *esize = dt->shared->u.atomic.u.f.esize;
    if (mpos)
        *mpos = dt->shared->u.atomic.u.f.mpos;
    if (msize)
        *msize = dt->shared->u.atomic.u.f.msize;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_fields() */

/*-------------------------------------------------------------------------
 * Function:	H5Tset_fields
 *
 * Purpose:	Sets the locations and sizes of the various floating point
 *		bit fields.  The field positions are bit positions in the
 *		significant region of the datatype.  Bits are numbered with
 *		the least significant bit number zero.
 *
 *		Fields are not allowed to extend beyond the number of bits of
 *		precision, nor are they allowed to overlap with one another.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_fields(hid_t type_id, size_t spos, size_t epos, size_t esize, size_t mpos, size_t msize)
{
    H5T_t *dt;                  /* Datatype */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "izzzzz", type_id, spos, epos, esize, mpos, msize);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is read-only");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "operation not defined for datatype class");
    if (epos + esize > dt->shared->u.atomic.prec)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "exponent bit field size/location is invalid");
    if (mpos + msize > dt->shared->u.atomic.prec)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "mantissa bit field size/location is invalid");
    if (spos >= dt->shared->u.atomic.prec)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "sign location is not valid");

    /* Check for overlap */
    if (spos >= epos && spos < epos + esize)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "sign bit appears within exponent field");
    if (spos >= mpos && spos < mpos + msize)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "sign bit appears within mantissa field");
    if ((mpos < epos && mpos + msize > epos) || (epos < mpos && epos + esize > mpos))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "exponent and mantissa fields overlap");

    /* Commit */
    dt->shared->u.atomic.u.f.sign  = spos;
    dt->shared->u.atomic.u.f.epos  = epos;
    dt->shared->u.atomic.u.f.mpos  = mpos;
    dt->shared->u.atomic.u.f.esize = esize;
    dt->shared->u.atomic.u.f.msize = msize;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tset_fields() */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_ebias
 *
 * Purpose:	Retrieves the exponent bias of a floating-point type.
 *
 * Return:	Success:	The bias
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
size_t
H5Tget_ebias(hid_t type_id)
{
    H5T_t *dt;        /* Datatype */
    size_t ret_value; /* Return value */

    FUNC_ENTER_API(0)
    H5TRACE1("z", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not a datatype");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, 0, "operation not defined for datatype class");

    /* bias */
    H5_CHECKED_ASSIGN(ret_value, size_t, dt->shared->u.atomic.u.f.ebias, uint64_t);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_ebias() */

/*-------------------------------------------------------------------------
 * Function:	H5Tset_ebias
 *
 * Purpose:	Sets the exponent bias of a floating-point type.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_ebias(hid_t type_id, size_t ebias)
{
    H5T_t *dt;                  /* Datatype */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iz", type_id, ebias);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is read-only");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "operation not defined for datatype class");

    /* Commit */
    dt->shared->u.atomic.u.f.ebias = ebias;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tset_ebias() */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_norm
 *
 * Purpose:	Returns the mantisssa normalization of a floating-point data
 *		type.
 *
 * Return:	Success:	Normalization ID
 *
 *		Failure:	H5T_NORM_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_norm_t
H5Tget_norm(hid_t type_id)
{
    H5T_t     *dt;        /* Datatype */
    H5T_norm_t ret_value; /* Return value */

    FUNC_ENTER_API(H5T_NORM_ERROR)
    H5TRACE1("Tn", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_NORM_ERROR, "not a datatype");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, H5T_NORM_ERROR, "operation not defined for datatype class");

    /* norm */
    ret_value = dt->shared->u.atomic.u.f.norm;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_norm() */

/*-------------------------------------------------------------------------
 * Function:	H5Tset_norm
 *
 * Purpose:	Sets the mantissa normalization method for a floating point
 *		datatype.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_norm(hid_t type_id, H5T_norm_t norm)
{
    H5T_t *dt;                  /* Datatype */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iTn", type_id, norm);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is read-only");
    if (norm < H5T_NORM_IMPLIED || norm > H5T_NORM_NONE)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "illegal normalization");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "operation not defined for datatype class");

    /* Commit */
    dt->shared->u.atomic.u.f.norm = norm;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tset_norm() */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_inpad
 *
 * Purpose:	If any internal bits of a floating point type are unused
 *		(that is, those significant bits which are not part of the
 *		sign, exponent, or mantissa) then they will be filled
 *		according to the value of this property.
 *
 * Return:	Success:	The internal padding type.
 *
 *		Failure:	H5T_PAD_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_pad_t
H5Tget_inpad(hid_t type_id)
{
    H5T_t    *dt;        /* Datatype */
    H5T_pad_t ret_value; /* Return value */

    FUNC_ENTER_API(H5T_PAD_ERROR)
    H5TRACE1("Tp", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_PAD_ERROR, "not a datatype");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, H5T_PAD_ERROR, "operation not defined for datatype class");

    /* pad */
    ret_value = dt->shared->u.atomic.u.f.pad;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_inpad() */

/*-------------------------------------------------------------------------
 * Function:	H5Tset_inpad
 *
 * Purpose:	If any internal bits of a floating point type are unused
 *		(that is, those significant bits which are not part of the
 *		sign, exponent, or mantissa) then they will be filled
 *		according to the value of this property.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_inpad(hid_t type_id, H5T_pad_t pad)
{
    H5T_t *dt;                  /* Datatype */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iTp", type_id, pad);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is read-only");
    if (pad < H5T_PAD_ZERO || pad >= H5T_NPAD)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "illegal internal pad type");
    while (dt->shared->parent)
        dt = dt->shared->parent; /*defer to parent*/
    if (H5T_FLOAT != dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "operation not defined for datatype class");

    /* Commit */
    dt->shared->u.atomic.u.f.pad = pad;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tset_inpad() */
