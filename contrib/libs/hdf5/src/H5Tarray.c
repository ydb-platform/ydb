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
 * Module Info: This module contains the functionality for array datatypes in
 *      the H5T interface.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling			*/
#include "H5Iprivate.h" /* IDs					*/
#include "H5Tpkg.h"     /* Datatypes				*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Public Variables */
/*********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5Tarray_create2
 *
 * Purpose:	Create a new array datatype based on the specified BASE_TYPE.
 *		The type is an array with NDIMS dimensionality and the size of the
 *      array is DIMS. The total member size should be relatively small.
 *      Array datatypes are currently limited to H5S_MAX_RANK number of
 *      dimensions and must have the number of dimensions set greater than
 *      0. (i.e. 0 > ndims <= H5S_MAX_RANK)  All dimensions sizes must be greater
 *      than 0 also.
 *
 * Return:	Success:	ID of new array datatype
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tarray_create2(hid_t base_id, unsigned ndims, const hsize_t dim[/* ndims */])
{
    H5T_t   *base;      /* base datatype	*/
    H5T_t   *dt = NULL; /* new array datatype	*/
    unsigned u;         /* local index variable */
    hid_t    ret_value; /* return value	*/

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "iIu*h", base_id, ndims, dim);

    /* Check args */
    if (ndims < 1 || ndims > H5S_MAX_RANK)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid dimensionality");
    if (!dim)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no dimensions specified");
    for (u = 0; u < ndims; u++)
        if (!(dim[u] > 0))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "zero-sized dimension specified");
    if (NULL == (base = (H5T_t *)H5I_object_verify(base_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not an valid base datatype");

    /* Create the array datatype */
    if (NULL == (dt = H5T__array_create(base, ndims, dim)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to create datatype");

    /* Register the type */
    if ((ret_value = H5I_register(H5I_DATATYPE, dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register datatype");

done:
    if (ret_value < 0)
        if (dt && H5T_close_real(dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, H5I_INVALID_HID, "can't release datatype");

    FUNC_LEAVE_API(ret_value)
} /* end H5Tarray_create2() */

/*-------------------------------------------------------------------------
 * Function:	H5T__array_create
 *
 * Purpose:	Internal routine to create a new array data type based on the
 *      specified BASE_TYPE.  The type is an array with NDIMS dimensionality
 *      and the size of the array is DIMS.
 *      Array datatypes are currently limited to H5S_MAX_RANK number
 *      of dimensions.
 *
 * Return:	Success:	ID of new array data type
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T__array_create(H5T_t *base, unsigned ndims, const hsize_t dim[/* ndims */])
{
    unsigned u;                /* Local index variable */
    H5T_t   *ret_value = NULL; /* New array data type	*/

    FUNC_ENTER_PACKAGE

    assert(base);
    assert(ndims <= H5S_MAX_RANK);
    assert(dim);

    /* Build new type */
    if (NULL == (ret_value = H5T__alloc()))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    ret_value->shared->type = H5T_ARRAY;

    /* Copy the base type of the array */
    if (NULL == (ret_value->shared->parent = H5T_copy(base, H5T_COPY_ALL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "unable to copy base datatype");

    /* Set the array parameters */
    ret_value->shared->u.array.ndims = ndims;

    /* Copy the array dimensions & compute the # of elements in the array */
    for (u = 0, ret_value->shared->u.array.nelem = 1; u < ndims; u++) {
        H5_CHECKED_ASSIGN(ret_value->shared->u.array.dim[u], size_t, dim[u], hsize_t);
        ret_value->shared->u.array.nelem *= (size_t)dim[u];
    } /* end for */

    /* Set the array's size (number of elements * element datatype's size) */
    ret_value->shared->size = ret_value->shared->parent->shared->size * ret_value->shared->u.array.nelem;

    /* Set the "force conversion" flag if the base datatype indicates */
    if (base->shared->force_conv == true)
        ret_value->shared->force_conv = true;

    /* Array datatypes need a later version of the datatype object header message */
    ret_value->shared->version = MAX(base->shared->version, H5O_DTYPE_VERSION_2);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__array_create */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_array_ndims
 *
 * Purpose:	Query the number of dimensions for an array datatype.
 *
 * Return:	Success:	Number of dimensions of the array datatype
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Tget_array_ndims(hid_t type_id)
{
    H5T_t *dt;        /* pointer to array datatype	*/
    int    ret_value; /* return value			*/

    FUNC_ENTER_API(FAIL)
    H5TRACE1("Is", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (dt->shared->type != H5T_ARRAY)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an array datatype");

    /* Retrieve the number of dimensions */
    ret_value = H5T__get_array_ndims(dt);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_array_ndims */

/*-------------------------------------------------------------------------
 * Function:	H5T__get_array_ndims
 *
 * Purpose:	Private function for H5T__get_array_ndims.  Query the number
 *              of dimensions for an array datatype.
 *
 * Return:	Success:	Number of dimensions of the array datatype
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5T__get_array_ndims(const H5T_t *dt)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(dt);
    assert(dt->shared->type == H5T_ARRAY);

    /* Retrieve the number of dimensions */
    FUNC_LEAVE_NOAPI((int)dt->shared->u.array.ndims)
} /* end H5T__get_array_ndims */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_array_dims2
 *
 * Purpose:	Query the sizes of dimensions for an array datatype.
 *
 * Return:	Success:	Number of dimensions of the array type
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Tget_array_dims2(hid_t type_id, hsize_t dims[] /*out*/)
{
    H5T_t *dt;        /* pointer to array data type	*/
    int    ret_value; /* return value			*/

    FUNC_ENTER_API(FAIL)
    H5TRACE2("Is", "ix", type_id, dims);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (dt->shared->type != H5T_ARRAY)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an array datatype");

    /* Retrieve the sizes of the dimensions */
    if ((ret_value = H5T__get_array_dims(dt, dims)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unable to get dimension sizes");
done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_array_dims2() */

/*-------------------------------------------------------------------------
 * Function:	H5T__get_array_dims
 *
 * Purpose:	Private function for H5T__get_array_dims.  Query the sizes
 *              of dimensions for an array datatype.
 *
 * Return:	Success:	Number of dimensions of the array type
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5T__get_array_dims(const H5T_t *dt, hsize_t dims[])
{
    unsigned u; /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    assert(dt);
    assert(dt->shared->type == H5T_ARRAY);

    /* Retrieve the sizes of the dimensions */
    if (dims)
        for (u = 0; u < dt->shared->u.array.ndims; u++)
            dims[u] = dt->shared->u.array.dim[u];

    /* Pass along the array rank as the return value */
    FUNC_LEAVE_NOAPI((int)dt->shared->u.array.ndims)
} /* end H5T__get_array_dims */

#ifndef H5_NO_DEPRECATED_SYMBOLS

/*-------------------------------------------------------------------------
 * Function:	H5Tarray_create1
 *
 * Purpose:	Create a new array datatype based on the specified BASE_TYPE.
 *		The type is an array with NDIMS dimensionality and the size of the
 *      array is DIMS. The total member size should be relatively small.
 *      Array datatypes are currently limited to H5S_MAX_RANK number of
 *      dimensions and must have the number of dimensions set greater than
 *      0. (i.e. 0 > ndims <= H5S_MAX_RANK)  All dimensions sizes must be greater
 *      than 0 also.
 *
 * Return:	Success:	ID of new array datatype
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tarray_create1(hid_t base_id, int ndims, const hsize_t dim[/* ndims */],
                 const int H5_ATTR_UNUSED perm[/* ndims */])
{
    H5T_t   *base;      /* base datatype	*/
    H5T_t   *dt = NULL; /* new array datatype	*/
    unsigned u;         /* local index variable */
    hid_t    ret_value; /* return value	*/

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE4("i", "iIs*h*Is", base_id, ndims, dim, perm);

    /* Check args */
    if (ndims < 1 || ndims > H5S_MAX_RANK)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid dimensionality");
    if (!dim)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no dimensions specified");
    for (u = 0; u < (unsigned)ndims; u++)
        if (!(dim[u] > 0))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "zero-sized dimension specified");
    if (NULL == (base = (H5T_t *)H5I_object_verify(base_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not an valid base datatype");

    /* Create the array datatype */
    if (NULL == (dt = H5T__array_create(base, (unsigned)ndims, dim)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to create datatype");

    /* Register the type */
    if ((ret_value = H5I_register(H5I_DATATYPE, dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register datatype");

done:
    if (ret_value < 0)
        if (dt && H5T_close_real(dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, H5I_INVALID_HID, "can't release datatype");

    FUNC_LEAVE_API(ret_value)
} /* end H5Tarray_create1() */

/*-------------------------------------------------------------------------
 * Function:	H5Tget_array_dims1
 *
 * Purpose:	Query the sizes of dimensions for an array datatype.
 *
 * Return:	Success:	Number of dimensions of the array type
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Tget_array_dims1(hid_t type_id, hsize_t dims[] /*out*/, int H5_ATTR_UNUSED perm[] /*out*/)
{
    H5T_t *dt;        /* Array datatype to query	*/
    int    ret_value; /* return value			*/

    FUNC_ENTER_API(FAIL)
    H5TRACE3("Is", "ixx", type_id, dims, perm);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (dt->shared->type != H5T_ARRAY)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an array datatype");

    /* Retrieve the sizes of the dimensions */
    if ((ret_value = H5T__get_array_dims(dt, dims)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unable to get dimension sizes");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_array_dims1() */
#endif /* H5_NO_DEPRECATED_SYMBOLS */
