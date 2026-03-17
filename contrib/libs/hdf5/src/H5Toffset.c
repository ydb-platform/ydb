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
 *      the datatype offset for the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Tpkg.h"     /* Datatypes				*/

/* Static local functions */
static herr_t H5T__set_offset(const H5T_t *dt, size_t offset);

/*-------------------------------------------------------------------------
 * Function:	H5Tget_offset
 *
 * Purpose:	Retrieves the bit offset of the first significant bit.	The
 *		significant bits of an atomic datum can be offset from the
 *		beginning of the memory for that datum by an amount of
 *		padding. The `offset' property specifies the number of bits
 *		of padding that appear to the "right of" the value.  That is,
 *		if we have a 32-bit datum with 16-bits of precision having
 *		the value 0x1122 then it will be laid out in memory as (from
 *		small byte address toward larger byte addresses):
 *
 *		    Big	     Big       Little	Little
 *		    Endian   Endian    Endian	Endian
 *		    offset=0 offset=16 offset=0 offset=16
 *
 *		0:  [ pad]   [0x11]    [0x22]	[ pad]
 *		1:  [ pad]   [0x22]    [0x11]	[ pad]
 *		2:  [0x11]   [ pad]    [ pad]	[0x22]
 *		3:  [0x22]   [ pad]    [ pad]	[0x11]
 *
 * Return:	Success:	The offset (non-negative)
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Tget_offset(hid_t type_id)
{
    H5T_t *dt;
    int    ret_value;

    FUNC_ENTER_API(-1)
    H5TRACE1("Is", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an atomic data type");

    /* Get offset */
    if ((ret_value = H5T_get_offset(dt)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "can't get offset for specified datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_offset() */

/*-------------------------------------------------------------------------
 * Function:	H5T_get_offset
 *
 * Purpose:	Retrieves the bit offset of the first significant bit.	The
 *		significant bits of an atomic datum can be offset from the
 *		beginning of the memory for that datum by an amount of
 *		padding. The `offset' property specifies the number of bits
 *		of padding that appear to the "right of" the value.  That is,
 *		if we have a 32-bit datum with 16-bits of precision having
 *		the value 0x1122 then it will be laid out in memory as (from
 *		small byte address toward larger byte addresses):
 *
 *		    Big	     Big       Little	Little
 *		    Endian   Endian    Endian	Endian
 *		    offset=0 offset=16 offset=0 offset=16
 *
 *		0:  [ pad]   [0x11]    [0x22]	[ pad]
 *		1:  [ pad]   [0x22]    [0x11]	[ pad]
 *		2:  [0x11]   [ pad]    [ pad]	[0x22]
 *		3:  [0x22]   [ pad]    [ pad]	[0x11]
 *
 * Return:	Success:	The offset (non-negative)
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5T_get_offset(const H5T_t *dt)
{
    int ret_value = -1; /* Return value */

    FUNC_ENTER_NOAPI(-1)

    /* Defer to parent*/
    while (dt->shared->parent)
        dt = dt->shared->parent;
    if (!H5T_IS_ATOMIC(dt->shared))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "operation not defined for specified data type");

    /* Offset */
    ret_value = (int)dt->shared->u.atomic.offset;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_offset() */

/*-------------------------------------------------------------------------
 * Function:	H5Tset_offset
 *
 * Purpose:	Sets the bit offset of the first significant bit.  The
 *		significant bits of an atomic datum can be offset from the
 *		beginning of the memory for that datum by an amount of
 *		padding. The `offset' property specifies the number of bits
 *		of padding that appear to the "right of" the value.  That is,
 *		if we have a 32-bit datum with 16-bits of precision having
 *		the value 0x1122 then it will be laid out in memory as (from
 *		small byte address toward larger byte addresses):
 *
 *		    Big	     Big       Little	Little
 *		    Endian   Endian    Endian	Endian
 *		    offset=0 offset=16 offset=0 offset=16
 *
 *		0:  [ pad]   [0x11]    [0x22]	[ pad]
 *		1:  [ pad]   [0x22]    [0x11]	[ pad]
 *		2:  [0x11]   [ pad]    [ pad]	[0x22]
 *		3:  [0x22]   [ pad]    [ pad]	[0x11]
 *
 *		If the offset is incremented then the total size is
 *		incremented also if necessary to prevent significant bits of
 *		the value from hanging over the edge of the data type.
 *
 *		The offset of an H5T_STRING cannot be set to anything but
 *		zero.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_offset(hid_t type_id, size_t offset)
{
    H5T_t *dt;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iz", type_id, offset);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an atomic data type");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "data type is read-only");
    if (H5T_STRING == dt->shared->type && offset != 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "offset must be zero for this type");
    if (H5T_ENUM == dt->shared->type && dt->shared->u.enumer.nmembs > 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not allowed after members are defined");
    if (H5T_COMPOUND == dt->shared->type || H5T_REFERENCE == dt->shared->type ||
        H5T_OPAQUE == dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "operation not defined for this datatype");

    /* Do the real work */
    if (H5T__set_offset(dt, offset) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to set offset");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5T__set_offset
 *
 * Purpose:	Sets the bit offset of the first significant bit.  The
 *		significant bits of an atomic datum can be offset from the
 *		beginning of the memory for that datum by an amount of
 *		padding. The `offset' property specifies the number of bits
 *		of padding that appear to the "right of" the value.  That is,
 *		if we have a 32-bit datum with 16-bits of precision having
 *		the value 0x1122 then it will be laid out in memory as (from
 *		small byte address toward larger byte addresses):
 *
 *		    Big	     Big       Little	Little
 *		    Endian   Endian    Endian	Endian
 *		    offset=0 offset=16 offset=0 offset=16
 *
 *		0:  [ pad]   [0x11]    [0x22]	[ pad]
 *		1:  [ pad]   [0x22]    [0x11]	[ pad]
 *		2:  [0x11]   [ pad]    [ pad]	[0x22]
 *		3:  [0x22]   [ pad]    [ pad]	[0x11]
 *
 *		If the offset is incremented then the total size is
 *		incremented also if necessary to prevent significant bits of
 *		the value from hanging over the edge of the data type.
 *
 *		The offset of an H5T_STRING cannot be set to anything but
 *		zero.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__set_offset(const H5T_t *dt, size_t offset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dt);
    assert(H5T_STRING != dt->shared->type || 0 == offset);
    assert(H5T_REFERENCE != dt->shared->type);
    assert(H5T_OPAQUE != dt->shared->type);
    assert(H5T_COMPOUND != dt->shared->type);
    assert(!(H5T_ENUM == dt->shared->type && 0 == dt->shared->u.enumer.nmembs));

    if (dt->shared->parent) {
        if (H5T__set_offset(dt->shared->parent, offset) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to set offset for base type");

        /* Adjust size of datatype appropriately */
        if (dt->shared->type == H5T_ARRAY)
            dt->shared->size = dt->shared->parent->shared->size * dt->shared->u.array.nelem;
        else if (dt->shared->type != H5T_VLEN)
            dt->shared->size = dt->shared->parent->shared->size;
    }
    else {
        if (offset + dt->shared->u.atomic.prec > 8 * dt->shared->size)
            dt->shared->size = (offset + dt->shared->u.atomic.prec + 7) / 8;
        dt->shared->u.atomic.offset = offset;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}
