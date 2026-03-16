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
 * Purpose:	Dataspace selection testing functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Smodule.h" /* This source code file is part of the H5S module */
#define H5S_TESTING    /*suppress warning about H5S testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Spkg.h"     /* Dataspaces 				*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

/*--------------------------------------------------------------------------
 NAME
    H5S__get_rebuild_status_test
 PURPOSE
    Determine the status of the diminfo_valid field (whether we know the
    selection information for an equivalent single hyperslab selection)
    before and after calling H5S__hyper_rebuild.
 USAGE
    herr_t H5S__get_rebuild_status_test(space_id, status1, status2)
        hid_t space_id;          IN:  dataspace id
        H5S_diminfo_valid_t *status1; OUT: status before calling
                                           H5S__hyper_rebuild
        H5S_diminfo_valid_t *status2; OUT: status after calling
                                           H5S__hyper_rebuild
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Query the status of rebuilding the hyperslab
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S__get_rebuild_status_test(hid_t space_id, H5S_diminfo_valid_t *status1, H5S_diminfo_valid_t *status2)
{
    H5S_t *space;               /* Pointer to 1st dataspace */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(status1);
    assert(status2);

    /* Get dataspace structures */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    *status1 = space->select.sel_info.hslab->diminfo_valid;

    /* Fully rebuild diminfo, if necessary */
    if (*status1 == H5S_DIMINFO_VALID_NO)
        H5S__hyper_rebuild(space);

    *status2 = space->select.sel_info.hslab->diminfo_valid;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__get_rebuild_status_test() */

/*--------------------------------------------------------------------------
 NAME
    H5S__get_diminfo_status_test
 PURPOSE
    Determine the status of the diminfo_valid field (whether we know the
    selection information for an equivalent single hyperslab selection)
 USAGE
    herr_t H5S__get_diminfo_status_test(space_id, status)
        hid_t space_id;          IN:  dataspace id
        H5S_diminfo_valid_t *status; OUT: status of diminfo_valid
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Query the status of rebuilding the hyperslab
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S__get_diminfo_status_test(hid_t space_id, H5S_diminfo_valid_t *status)
{
    H5S_t *space;               /* Pointer to 1st dataspace */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(status);

    /* Get dataspace structures */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    *status = space->select.sel_info.hslab->diminfo_valid;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__get_diminfo_status_test() */

/*--------------------------------------------------------------------------
 NAME
    H5S__check_spans_tail_ptr
 PURPOSE
    Determine if the tail pointer of the spans are correctly set
 USAGE
    herr_t H5S__check_spans_tail_ptr(span_lst)
        const H5S_hyper_span_info_t *span_lst;  IN: the spans to check for taill pointers
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspaces has tail pointers of each
    dimension correctly set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Only check the hyperslab selection
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__check_spans_tail_ptr(const H5S_hyper_span_info_t *span_lst)
{
    H5S_hyper_span_t *cur_elem;
    H5S_hyper_span_t *actual_tail = NULL;
    htri_t            ret_value   = true; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(span_lst);

    cur_elem = span_lst->head;
    while (cur_elem) {
        actual_tail = cur_elem;

        /* check the next dimension of lower order */
        if (NULL != cur_elem->down)
            if ((ret_value = H5S__check_spans_tail_ptr(cur_elem->down)) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                            "the selection has inconsistent tail pointers");

        cur_elem = cur_elem->next;
    } /* end while */
    if (actual_tail != span_lst->tail)
        HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                    "the selection has inconsistent tail pointers");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__check_spans_tail_ptr */

/*--------------------------------------------------------------------------
 NAME
    H5S__check_points_tail_ptr
 PURPOSE
    Determine if the tail pointer of the points list are correctly set
 USAGE
    herr_t H5S__check_points_tail_ptr(pnt_lst)
        const H5S_pnt_list_t *pnt_lst;  IN: the points list to check for taill pointers
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspaces has tail pointers correctly set.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Only check the points selection
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__check_points_tail_ptr(const H5S_pnt_list_t *pnt_lst)
{
    H5S_pnt_node_t *cur_elem;
    H5S_pnt_node_t *actual_tail = NULL;
    htri_t          ret_value   = true; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(pnt_lst);

    cur_elem = pnt_lst->head;
    while (cur_elem) {
        actual_tail = cur_elem;
        cur_elem    = cur_elem->next;
    } /* end while */
    if (actual_tail != pnt_lst->tail)
        HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                    "the selection has inconsistent tail pointers");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__check_points_tail_ptr */

/*--------------------------------------------------------------------------
 NAME
    H5S__check_internal_consistency
 PURPOSE
    Determine if internal data structures are consistent
 USAGE
    herr_t H5S__check_internal_consistency(space)
        const H5S_t *space;         IN: 1st Dataspace pointer to compare
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspaces has consistent
    state of internal data structure.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Currently only check the hyperslab selection
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__check_internal_consistency(const H5S_t *space)
{
    hsize_t  low_bounds[H5S_MAX_RANK];
    hsize_t  high_bounds[H5S_MAX_RANK];
    unsigned u;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);

    if (space->select.type->type == H5S_SEL_NONE)
        HGOTO_DONE(ret_value);

    /* Initialize the inputs */
    for (u = 0; u < space->extent.rank; u++) {
        low_bounds[u]  = HSIZET_MAX;
        high_bounds[u] = 0;
    } /* end for */

    /* Check the bound box */
    if (H5S_get_select_bounds(space, low_bounds, high_bounds) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL, "the bound box could not be retrieved");

    if (space->select.type->type == H5S_SEL_HYPERSLABS) {
        H5S_hyper_sel_t *hslab = space->select.sel_info.hslab;

        if (space->select.sel_info.hslab->diminfo_valid == H5S_DIMINFO_VALID_YES) {
            for (u = 0; u < space->extent.rank; u++) {
                if ((hsize_t)((hssize_t)hslab->diminfo.low_bounds[u] + space->select.offset[u]) !=
                    low_bounds[u])
                    HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                                "the lower bound box of the selection is inconsistent");
                if ((hsize_t)((hssize_t)hslab->diminfo.high_bounds[u] + space->select.offset[u]) !=
                    high_bounds[u])
                    HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                                "the higher bound box of the selection is inconsistent");
            } /* end for */
        }     /* end if */
        else {
            for (u = 0; u < space->extent.rank; u++) {
                if ((hsize_t)((hssize_t)hslab->span_lst->low_bounds[u] + space->select.offset[u]) !=
                    low_bounds[u])
                    HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                                "the lower bound box of the selection is inconsistent");
                if ((hsize_t)((hssize_t)hslab->span_lst->high_bounds[u] + space->select.offset[u]) !=
                    high_bounds[u])
                    HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                                "the higher bound box of the selection is inconsistent");
            } /* end for */
        }     /* end else */

        /* check the tail pointer */
        if ((NULL != hslab) && (NULL != hslab->span_lst))
            if (H5S__check_spans_tail_ptr(hslab->span_lst) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                            "the selection has inconsistent tail pointers");
    } /* end if */
    else if (space->select.type->type == H5S_SEL_POINTS) {
        H5S_pnt_list_t *pnt_lst = space->select.sel_info.pnt_lst;

        if (NULL != pnt_lst)
            if (H5S__check_points_tail_ptr(pnt_lst) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                            "the selection has inconsistent tail pointers");
    } /* end else-if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__check_internal_consistency */

/*--------------------------------------------------------------------------
 NAME
    H5S__internal_consistency_test
 PURPOSE
    Determine if states of internal data structures are consistent
 USAGE
    htri_t H5S__internal_consistency_test(hid_t space_id)
        hid_t space_id;          IN:  dataspace id
 RETURNS
    Non-negative true/false on success, negative on failure
 DESCRIPTION
    Check the states of internal data structures of the hyperslab, and see
    whether they are consistent or not
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S__internal_consistency_test(hid_t space_id)
{
    H5S_t *space;            /* Pointer to 1st dataspace */
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get dataspace structures */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Check if the dataspace selections are the same shape */
    if (FAIL == H5S__check_internal_consistency(space))
        HGOTO_ERROR(H5E_DATASPACE, H5E_INCONSISTENTSTATE, FAIL,
                    "The dataspace has inconsistent internal state");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__internal_consistency_test() */
