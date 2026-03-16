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
 *      the datatype byte order for the H5T interface.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
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
static herr_t H5T__set_order(H5T_t *dtype, H5T_order_t order);

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
 * Function:	H5Tget_order
 *
 * Purpose:	Returns the byte order of a datatype.
 *
 * Return:	Success:	A byte order constant.  If the type is compound
 *				and its members have mixed orders, this function
 *				returns H5T_ORDER_MIXED.
 *		Failure:	H5T_ORDER_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_order_t
H5Tget_order(hid_t type_id)
{
    H5T_t      *dt;        /* Datatype to query */
    H5T_order_t ret_value; /* Return value */

    FUNC_ENTER_API(H5T_ORDER_ERROR)
    H5TRACE1("To", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, H5T_ORDER_ERROR, "not a datatype");

    /* Get order */
    if (H5T_ORDER_ERROR == (ret_value = H5T_get_order(dt)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, H5T_ORDER_ERROR, "can't get order for specified datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_order() */

/*-------------------------------------------------------------------------
 * Function:	H5T_get_order
 *
 * Purpose:	Returns the byte order of a datatype.
 *
 * Return:	Success:	A byte order constant
 *		Failure:	H5T_ORDER_ERROR (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_order_t
H5T_get_order(const H5T_t *dtype)
{
    H5T_order_t ret_value = H5T_ORDER_NONE; /* Return value */

    FUNC_ENTER_NOAPI(H5T_ORDER_ERROR)

    /* Defer to parent */
    while (dtype->shared->parent)
        dtype = dtype->shared->parent;

    /* Set order for atomic type. */
    if (H5T_IS_ATOMIC(dtype->shared))
        ret_value = dtype->shared->u.atomic.order;
    else {
        /* Check for compound datatype */
        if (H5T_COMPOUND == dtype->shared->type) {
            H5T_order_t memb_order = H5T_ORDER_NONE;
            int         nmemb; /* Number of members in compound & enum types */
            int         i;     /* Local index variable */

            /* Retrieve the number of members */
            if ((nmemb = H5T_get_nmembers(dtype)) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_ORDER_ERROR,
                            "can't get number of members from compound data type");

            /* Get order for each compound member type. */
            for (i = 0; i < nmemb; i++) {
                /* Get order for member */
                if ((memb_order = H5T_get_order(dtype->shared->u.compnd.memb[i].type)) == H5T_ORDER_ERROR)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5T_ORDER_ERROR,
                                "can't get order for compound member");

                /* Ignore the H5T_ORDER_NONE, write down the first non H5T_ORDER_NONE order. */
                if (memb_order != H5T_ORDER_NONE && ret_value == H5T_ORDER_NONE)
                    ret_value = memb_order;

                /* If the orders are mixed, stop the loop and report it.
                 * (H5T_ORDER_NONE is ignored)
                 */
                if (memb_order != H5T_ORDER_NONE && ret_value != H5T_ORDER_NONE && memb_order != ret_value) {
                    ret_value = H5T_ORDER_MIXED;
                    break;
                } /* end if */
            }     /* end for */
        }         /* end if */
    }             /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_order() */

/*-------------------------------------------------------------------------
 * Function:	H5Tset_order
 *
 * Purpose:	Sets the byte order for a datatype.
 *
 * Notes:	There are some restrictions on this operation:
 *		1. For enum type, members shouldn't be defined yet.
 *		2. H5T_ORDER_NONE only works for reference and fixed-length
 *			string.
 *		3. For opaque type, the order will be ignored.
 *		4. For compound type, all restrictions above apply to the
 *			members.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_order(hid_t type_id, H5T_order_t order)
{
    H5T_t *dt        = NULL;    /* Datatype to modify */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iTo", type_id, order);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "not a datatype");
    if (order < H5T_ORDER_LE || order > H5T_ORDER_NONE || order == H5T_ORDER_MIXED)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "illegal byte order");
    if (NULL != dt->vol_obj)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is already committed");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "datatype is read-only");

    /* Call internal routine to set the order */
    if (H5T__set_order(dt, order) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "can't set order");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tset_order() */

/*-------------------------------------------------------------------------
 * Function:	H5T__set_order
 *
 * Purpose:	Private function to set the byte order for a datatype.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__set_order(H5T_t *dtype, H5T_order_t order)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (H5T_ENUM == dtype->shared->type && dtype->shared->u.enumer.nmembs > 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "operation not allowed after enum members are defined");

    /* For derived data type, defer to parent */
    while (dtype->shared->parent)
        dtype = dtype->shared->parent;

    /* Check for setting order on inappropriate datatype */
    if (order == H5T_ORDER_NONE && !(H5T_REFERENCE == dtype->shared->type ||
                                     H5T_OPAQUE == dtype->shared->type || H5T_IS_FIXED_STRING(dtype->shared)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "illegal byte order for type");

    /* For atomic data type */
    if (H5T_IS_ATOMIC(dtype->shared))
        dtype->shared->u.atomic.order = order;
    else {
        /* Check for compound datatype */
        if (H5T_COMPOUND == dtype->shared->type) {
            int nmemb; /* Number of members in type */
            int i;     /* Local index variable */

            /* Retrieve the number of fields in the compound datatype */
            if ((nmemb = H5T_get_nmembers(dtype)) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL,
                            "can't get number of members from compound data type");

            /* Check for uninitialized compound datatype */
            if (nmemb == 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_UNINITIALIZED, FAIL, "no member is in the compound data type");

            /* Loop through all fields of compound type, setting the order */
            for (i = 0; i < nmemb; i++)
                if (H5T__set_order(dtype->shared->u.compnd.memb[i].type, order) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't set order for compound member");
        } /* end if */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__set_order() */
