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
 * Purpose:	Dataspace selection functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Smodule.h" /* This source code file is part of the H5S module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Dprivate.h"  /* Datasets				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Spkg.h"      /* Dataspaces 				*/
#include "H5VMprivate.h" /* Vector and array functions		*/

/****************/
/* Local Macros */
/****************/

/* All the valid public flags to H5Ssel_iter_create() */
#define H5S_SEL_ITER_ALL_PUBLIC_FLAGS (H5S_SEL_ITER_GET_SEQ_LIST_SORTED | H5S_SEL_ITER_SHARE_WITH_DATASPACE)

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

/* Declare a free list to manage the H5S_sel_iter_t struct */
H5FL_DEFINE(H5S_sel_iter_t);

/* Declare extern free list to manage sequences of size_t */
H5FL_SEQ_EXTERN(size_t);

/* Declare extern free list to manage sequences of hsize_t */
H5FL_SEQ_EXTERN(hsize_t);

/*******************/
/* Local Variables */
/*******************/

/*--------------------------------------------------------------------------
 NAME
    H5S_select_offset
 PURPOSE
    Set the selection offset for a datapace
 USAGE
    herr_t H5S_select_offset(space, offset)
        H5S_t *space;	        IN/OUT: Dataspace object to set selection offset
        const hssize_t *offset; IN: Offset to position the selection at
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Sets the selection offset for the dataspace
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Only works for simple dataspaces currently
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_offset(H5S_t *space, const hssize_t *offset)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);
    assert(0 < space->extent.rank && space->extent.rank <= H5S_MAX_RANK);
    assert(offset);

    /* Copy the offset over */
    H5MM_memcpy(space->select.offset, offset, sizeof(hssize_t) * space->extent.rank);

    /* Indicate that the offset was changed */
    space->select.offset_changed = true;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S_select_offset() */

/*--------------------------------------------------------------------------
 NAME
    H5Soffset_simple
 PURPOSE
    Changes the offset of a selection within a simple dataspace extent
 USAGE
    herr_t H5Soffset_simple(space_id, offset)
        hid_t space_id;	        IN: Dataspace object to reset
        const hssize_t *offset; IN: Offset to position the selection at
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
        This function creates an offset for the selection within an extent, allowing
    the same shaped selection to be moved to different locations within a
    dataspace without requiring it to be re-defined.
--------------------------------------------------------------------------*/
herr_t
H5Soffset_simple(hid_t space_id, const hssize_t *offset)
{
    H5S_t *space;               /* Dataspace to modify */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*Hs", space_id, offset);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "not a dataspace");
    if (space->extent.rank == 0 ||
        (H5S_GET_EXTENT_TYPE(space) == H5S_SCALAR || H5S_GET_EXTENT_TYPE(space) == H5S_NULL))
        HGOTO_ERROR(H5E_ID, H5E_UNSUPPORTED, FAIL, "can't set offset on scalar or null dataspace");
    if (offset == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no offset specified");

    /* Set the selection offset */
    if (H5S_select_offset(space, offset) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't set offset");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Soffset_simple() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_copy
 PURPOSE
    Copy a selection from one dataspace to another
 USAGE
    herr_t H5Sselect_copy(dst, src)
        hid_t   dst;              OUT: ID of the destination dataspace
        hid_t   src;              IN:  ID of the source dataspace

 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Copies all the selection information (including offset) from the source
    dataspace to the destination dataspace.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sselect_copy(hid_t dst_id, hid_t src_id)
{
    H5S_t *src;
    H5S_t *dst;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ii", dst_id, src_id);

    /* Check args */
    if (NULL == (src = (H5S_t *)H5I_object_verify(src_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == (dst = (H5S_t *)H5I_object_verify(dst_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Copy */
    if (H5S_select_copy(dst, src, false) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy selection");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_copy() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_copy
 PURPOSE
    Copy a selection from one dataspace to another
 USAGE
    herr_t H5S_select_copy(dst, src, share_selection)
        H5S_t *dst;  OUT: Pointer to the destination dataspace
        H5S_t *src;  IN: Pointer to the source dataspace
        bool;     IN: Whether to share the selection between the dataspaces
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Copies all the selection information (include offset) from the source
    dataspace to the destination dataspace.

    If the SHARE_SELECTION flag is set, then the selection can be shared
    between the source and destination dataspaces.  (This should only occur in
    situations where the destination dataspace will immediately change to a new
    selection)
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_copy(H5S_t *dst, const H5S_t *src, bool share_selection)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(dst);
    assert(src);

    /* Release the current selection */
    if (H5S_SELECT_RELEASE(dst) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection");

    /* Copy regular fields */
    dst->select = src->select;

    /* Perform correct type of copy based on the type of selection */
    if ((ret_value = (*src->select.type->copy)(dst, src, share_selection)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy selection specific information");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_copy() */

/*-------------------------------------------------------------------------
 * Function:	H5S_select_release
 *
 * Purpose:	Releases all memory associated with a dataspace selection.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 * Note: This routine participates in the "Inlining C function pointers"
 *      pattern, don't call it directly, use the appropriate macro
 *      defined in H5Sprivate.h.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S_select_release(H5S_t *ds)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    assert(ds);

    /* Call the selection type's release function */
    if ((ds->select.type) && ((ret_value = (*ds->select.type->release)(ds)) < 0))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_release() */

/*-------------------------------------------------------------------------
 * Function:	H5S_select_serial_size
 *
 * Purpose:	Determines the number of bytes required to store the current
 *              selection
 *
 * Return:	Non-negative on success/Negative on failure
 *
 * Note: This routine participates in the "Inlining C function pointers"
 *      pattern, don't call it directly, use the appropriate macro
 *      defined in H5Sprivate.h.
 *
 *-------------------------------------------------------------------------
 */
hssize_t
H5S_select_serial_size(H5S_t *space)
{
    hssize_t ret_value = -1; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(space);

    /* Call the selection type's serial_size function */
    ret_value = (*space->select.type->serial_size)(space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_serial_size() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_serialize
 PURPOSE
    Serialize the selection for a dataspace into a buffer
 USAGE
    herr_t H5S_select_serialize(space, p)
        const H5S_t *space;     IN: Dataspace with selection to serialize
        uint8_t **p;            OUT: Pointer to buffer to put serialized
                                selection.  Will be advanced to end of
                                serialized selection.
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Calls the appropriate dataspace selection callback to serialize the
    current selection into a buffer.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_serialize(H5S_t *space, uint8_t **p)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(space);
    assert(p);

    /* Call the selection type's serialize function */
    ret_value = (*space->select.type->serialize)(space, p);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_serialize() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_select_npoints
 PURPOSE
    Get the number of elements in current selection
 USAGE
    hssize_t H5Sget_select_npoints(dsid)
        hid_t dsid;             IN: Dataspace ID of selection to query
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Returns the number of elements in current selection for dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hssize_t
H5Sget_select_npoints(hid_t spaceid)
{
    H5S_t   *space;     /* Dataspace to modify selection of */
    hssize_t ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("Hs", "i", spaceid);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    ret_value = (hssize_t)H5S_GET_SELECT_NPOINTS(space);

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Sget_select_npoints() */

/*--------------------------------------------------------------------------
 NAME
    H5S_get_select_npoints
 PURPOSE
    Get the number of elements in current selection
 USAGE
    hsize_t H5Sget_select_npoints(space)
        H5S_t *space;             IN: Dataspace of selection to query
 RETURNS
    The number of elements in selection on success, 0 on failure
 DESCRIPTION
    Returns the number of elements in current selection for dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5_ATTR_PURE hsize_t
H5S_get_select_npoints(const H5S_t *space)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);

    FUNC_LEAVE_NOAPI(space->select.num_elem)
} /* end H5S_get_select_npoints() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_valid
 PURPOSE
    Check whether the selection fits within the extent, with the current
    offset defined.
 USAGE
    htri_t H5Sselect_void(dsid)
        hid_t dsid;             IN: Dataspace ID to query
 RETURNS
    true if the selection fits within the extent, false if it does not and
        Negative on an error.
 DESCRIPTION
    Determines if the current selection at the current offset fits within the
    extent for the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5Sselect_valid(hid_t spaceid)
{
    H5S_t *space;     /* Dataspace to modify selection of */
    htri_t ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "i", spaceid);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    ret_value = H5S_SELECT_VALID(space);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_valid() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_valid
 PURPOSE
    Check whether the selection fits within the extent, with the current
    offset defined.
 USAGE
    htri_t H5S_select_void(space)
        H5S_t *space;           IN: Dataspace to query
 RETURNS
    true if the selection fits within the extent, false if it does not and
        Negative on an error.
 DESCRIPTION
    Determines if the current selection at the current offset fits within the
    extent for the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S_select_valid(const H5S_t *space)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(space);

    ret_value = (*space->select.type->is_valid)(space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_valid() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_deserialize
 PURPOSE
    Deserialize the current selection from a user-provided buffer into a real
        selection in the dataspace.
 USAGE
    herr_t H5S_select_deserialize(space, p)
        H5S_t **space;          IN/OUT: Dataspace pointer to place
                                selection into.  Will be allocated if not
                                provided.
        uint8 **p;              OUT: Pointer to buffer holding serialized
                                selection.  Will be advanced to end of
                                serialized selection.
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Deserializes the current selection into a buffer.  (Primarily for retrieving
    from disk).  This routine just hands off to the appropriate routine for each
    type of selection.  The format of the serialized information is shown in
    the H5S_select_serialize() header.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_deserialize(H5S_t **space, const uint8_t **p, const size_t p_size)
{
    uint32_t       sel_type;                                   /* Pointer to the selection type */
    herr_t         ret_value = FAIL;                           /* Return value */
    const uint8_t *p_end     = *p + p_size - 1;                /* Pointer to last valid byte in buffer */
    bool           skip = (p_size == SIZE_MAX ? true : false); /* If p_size is unknown, skip buffer checks */
    FUNC_ENTER_NOAPI(FAIL)

    assert(space);

    /* Selection-type specific coding is moved to the callbacks. */

    /* Decode selection type */
    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, *p, sizeof(uint32_t), p_end))
        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding selection type");
    UINT32DECODE(*p, sel_type);

    /* Make routine for selection type */
    switch (sel_type) {
        case H5S_SEL_POINTS: /* Sequence of points selected */
            ret_value = (*H5S_sel_point->deserialize)(space, p, p_size - sizeof(uint32_t), skip);
            break;

        case H5S_SEL_HYPERSLABS: /* Hyperslab selection defined */
            ret_value = (*H5S_sel_hyper->deserialize)(space, p, p_size - sizeof(uint32_t), skip);
            break;

        case H5S_SEL_ALL: /* Entire extent selected */
            ret_value = (*H5S_sel_all->deserialize)(space, p, p_size - sizeof(uint32_t), skip);
            break;

        case H5S_SEL_NONE: /* Nothing selected */
            ret_value = (*H5S_sel_none->deserialize)(space, p, p_size - sizeof(uint32_t), skip);
            break;

        default:
            break;
    }

    if (ret_value < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTLOAD, FAIL, "can't deserialize selection");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_deserialize() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_select_bounds
 PURPOSE
    Gets the bounding box containing the selection.
 USAGE
    herr_t H5S_get_select_bounds(space, start, end)
        hid_t dsid;             IN: Dataspace ID of selection to query
        hsize_t start[];        OUT: Starting coordinate of bounding box
        hsize_t end[];          OUT: Opposite coordinate of bounding box
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the bounding box containing the current selection and places
    it into the user's buffers.  The start and end buffers must be large
    enough to hold the dataspace rank number of coordinates.  The bounding box
    exactly contains the selection, ie. if a 2-D element selection is currently
    defined with the following points: (4,5), (6,8) (10,7), the bounding box
    with be (4, 5), (10, 8).  Calling this function on a "none" selection
    returns fail.
        The bounding box calculations _does_ include the current offset of the
    selection within the dataspace extent.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sget_select_bounds(hid_t spaceid, hsize_t start[] /*out*/, hsize_t end[] /*out*/)
{
    H5S_t *space;     /* Dataspace to modify selection of */
    herr_t ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", spaceid, start, end);

    /* Check args */
    if (start == NULL || end == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pointer");
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    ret_value = H5S_SELECT_BOUNDS(space, start, end);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sget_select_bounds() */

/*--------------------------------------------------------------------------
 NAME
    H5S_get_select_bounds
 PURPOSE
    Gets the bounding box containing the selection.
 USAGE
    herr_t H5S_get_select_bounds(space, start, end)
        H5S_t *space;           IN: Dataspace ID of selection to query
        hsize_t *start;         OUT: Starting coordinate of bounding box
        hsize_t *end;           OUT: Opposite coordinate of bounding box
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the bounding box containing the current selection and places
    it into the user's buffers.  The start and end buffers must be large
    enough to hold the dataspace rank number of coordinates.  The bounding box
    exactly contains the selection, ie. if a 2-D element selection is currently
    defined with the following points: (4,5), (6,8) (10,7), the bounding box
    with be (4, 5), (10, 8).  Calling this function on a "none" selection
    returns fail.
        The bounding box calculations _does_ include the current offset of the
    selection within the dataspace extent.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_get_select_bounds(const H5S_t *space, hsize_t *start, hsize_t *end)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);
    assert(start);
    assert(end);

    ret_value = (*space->select.type->bounds)(space, start, end);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_get_select_bounds() */

/*--------------------------------------------------------------------------
 NAME
    H5S_get_select_offset
 PURPOSE
    Gets the linear offset of the first element for the selection.
 USAGE
    herr_t H5S_get_select_offset(space, offset)
        const H5S_t *space;     IN: Dataspace pointer of selection to query
        hsize_t *offset;        OUT: Linear offset of first element in selection
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the linear offset (in "units" of elements) of the first element
    selected within the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The offset calculation _does_ include the current offset of the
    selection within the dataspace extent.

        Calling this function on a "none" selection returns fail.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_get_select_offset(const H5S_t *space, hsize_t *offset)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);
    assert(offset);

    ret_value = (*space->select.type->offset)(space, offset);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_get_select_offset() */

/*--------------------------------------------------------------------------
 NAME
    H5S_get_select_unlim_dim
 PURPOSE
    Gets the unlimited dimension in the selection, or -1 if there is no
    unlimited dimension.
 USAGE
    int H5S_get_select_unlim_dim(space)
        const H5S_t *space;     IN: Dataspace pointer of selection to query
 RETURNS
    Unlimited dimension in the selection, or -1 if there is no unlimited
    dimension (never fails)
 DESCRIPTION
    Gets the unlimited dimension in the selection, or -1 if there is no
    unlimited dimension.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        Currently only implemented for hyperslab selections, all others
        simply return -1.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5S_get_select_unlim_dim(const H5S_t *space)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);

    ret_value = (*space->select.type->unlim_dim)(space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_get_select_unlim_dim() */

/*--------------------------------------------------------------------------
 NAME
    H5S_get_select_num_elem_non_unlim
 PURPOSE
    Gets the number of elements in the non-unlimited dimensions
 USAGE
    herr_t H5S_get_select_num_elem_non_unlim(space,num_elem_non_unlim)
        H5S_t *space;           IN: Dataspace pointer to check
        hsize_t *num_elem_non_unlim; OUT: Number of elements in the non-unlimited dimensions
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Returns the number of elements in a slice through the non-unlimited
    dimensions of the selection.  Fails if the selection has no unlimited
    dimension.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_get_select_num_elem_non_unlim(const H5S_t *space, hsize_t *num_elem_non_unlim)
{
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(space);
    assert(num_elem_non_unlim);

    /* Check for selection callback */
    if (!space->select.type->num_elem_non_unlim)
        HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL,
                    "selection type has no num_elem_non_unlim callback");

    /* Make selection callback */
    if ((*space->select.type->num_elem_non_unlim)(space, num_elem_non_unlim) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL,
                    "can't get number of elements in non-unlimited dimension");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_get_select_unlim_dim() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_is_contiguous
 PURPOSE
    Determines if a selection is contiguous in the dataspace
 USAGE
    htri_t H5S_select_is_contiguous(space)
        const H5S_t *space;             IN: Dataspace of selection to query
 RETURNS
    Non-negative (true/false) on success, negative on failure
 DESCRIPTION
    Checks the selection to determine if the points to iterated over will be
    contiguous in the particular dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S_select_is_contiguous(const H5S_t *space)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);

    ret_value = (*space->select.type->is_contiguous)(space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_is_contiguous() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_is_single
 PURPOSE
    Determines if a selection is a single block in the dataspace
 USAGE
    htri_t H5S_select_is_single(space)
        const H5S_t *space;             IN: Dataspace of selection to query
 RETURNS
    Non-negative (true/false) on success, negative on failure
 DESCRIPTION
    Checks the selection to determine if it occupies a single block in the
    particular dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S_select_is_single(const H5S_t *space)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);

    ret_value = (*space->select.type->is_single)(space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_is_single() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_is_regular
 PURPOSE
    Determines if a selection is "regular"  in the dataspace
 USAGE
    htri_t H5S_select_is_regular(space)
        const H5S_t *space;             IN: Dataspace of selection to query
 RETURNS
    Non-negative (true/false) on success, negative on failure
 DESCRIPTION
    Checks the selection to determine if it is "regular" (i.e. a single
    block or a strided pattern) in the particular dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S_select_is_regular(H5S_t *space)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);

    ret_value = (*space->select.type->is_regular)(space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_is_regular() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_adjust_u
 PURPOSE
    Adjust a selection by subtracting an offset
 USAGE
    herr_t H5S_select_adjust_u(space, offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to adjust
        const hsize_t *offset; IN: Offset to subtract
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves a selection by subtracting an offset from it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_adjust_u(H5S_t *space, const hsize_t *offset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);
    assert(offset);

    /* Perform operation */
    ret_value = (*space->select.type->adjust_u)(space, offset);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_adjust_u() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_adjust_s
 PURPOSE
    Adjust a selection by subtracting an offset
 USAGE
    herr_t H5S_select_adjust_u(space, offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to adjust
        const hssize_t *offset; IN: Offset to subtract
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves a selection by subtracting an offset from it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_adjust_s(H5S_t *space, const hssize_t *offset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);
    assert(offset);

    /* Perform operation */
    ret_value = (*space->select.type->adjust_s)(space, offset);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_adjust_s() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_adjust
 PURPOSE
    Adjust a selection by subtracting an offset
 USAGE
    herr_t H5Sselect_adjust_u(space_id, offset)
        hid_t space_id;        IN: ID of dataspace to adjust
        const hsize_t *offset; IN: Offset to subtract
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves a selection by subtracting an offset from it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sselect_adjust(hid_t space_id, const hssize_t *offset)
{
    H5S_t   *space;
    hsize_t  low_bounds[H5S_MAX_RANK];
    hsize_t  high_bounds[H5S_MAX_RANK];
    unsigned u;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*Hs", space_id, offset);

    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == offset)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "NULL offset pointer");

    /* Check bounds */
    if (H5S_SELECT_BOUNDS(space, low_bounds, high_bounds) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get selection bounds");
    for (u = 0; u < space->extent.rank; u++)
        if (offset[u] > (hssize_t)low_bounds[u])
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "adjustment would move selection below zero offset");

    if (H5S_select_adjust_s(space, offset) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't adjust selection");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_adjust() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_project_scalar
 PURPOSE
    Project a single element selection for a scalar dataspace
 USAGE
    herr_t H5S_select_project_scalar(space, offset)
        const H5S_t *space;             IN: Pointer to dataspace to project
        hsize_t *offset;                IN/OUT: Offset of projected point
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Projects a selection of a single element into a scalar dataspace, computing
    the offset of the element in the original selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_project_scalar(const H5S_t *space, hsize_t *offset)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);
    assert(offset);

    ret_value = (*space->select.type->project_scalar)(space, offset);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_project_scalar() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_project_simple
 PURPOSE
    Project a selection onto/into a dataspace of different rank
 USAGE
    herr_t H5S_select_project_simple(space, new_space, offset)
        const H5S_t *space;             IN: Pointer to dataspace to project
        H5S_t *new_space;               IN/OUT: Pointer to dataspace projected onto
        hsize_t *offset;                IN/OUT: Offset of projected point
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Projects a selection onto/into a simple dataspace, computing
    the offset of the first element in the original selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_project_simple(const H5S_t *space, H5S_t *new_space, hsize_t *offset)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);
    assert(new_space);
    assert(offset);

    ret_value = (*space->select.type->project_simple)(space, new_space, offset);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_project_simple() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_iter_init
 PURPOSE
    Initializes iteration information for a selection.
 USAGE
    herr_t H5S_select_iter_init(sel_iter, space, elmt_size, flags)
        H5S_sel_iter_t *sel_iter; OUT: Selection iterator to initialize.
        H5S_t *space;           IN: Dataspace object containing selection to
                                    iterate over
        size_t elmt_size;       IN: Size of elements in the selection
        unsigned flags;         IN: Flags to control iteration behavior
 RETURNS
     Non-negative on success, negative on failure.
 DESCRIPTION
    Initialize the selection iterator object to point to the first element
    in the dataspace's selection.
--------------------------------------------------------------------------*/
herr_t
H5S_select_iter_init(H5S_sel_iter_t *sel_iter, H5S_t *space, size_t elmt_size, unsigned flags)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(sel_iter);
    assert(space);

    /* Initialize common information */

    /* Save the dataspace's rank */
    sel_iter->rank = space->extent.rank;

    /* If dims > 0, copy the dataspace dimensions & selection offset */
    if (sel_iter->rank > 0) {
        H5MM_memcpy(sel_iter->dims, space->extent.size, sizeof(hsize_t) * space->extent.rank);
        H5MM_memcpy(sel_iter->sel_off, space->select.offset, sizeof(hsize_t) * space->extent.rank);
    }

    /* Save the element size */
    sel_iter->elmt_size = elmt_size;

    /* Initialize the number of elements to iterate over */
    sel_iter->elmt_left = space->select.num_elem;

    /* Set the flags for the iterator */
    sel_iter->flags = flags;

    /* Call initialization routine for selection type */
    ret_value = (*space->select.type->iter_init)(space, sel_iter);
    assert(sel_iter->type);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_iter_init() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_iter_coords
 PURPOSE
    Get the coordinates of the current iterator position
 USAGE
    herr_t H5S_select_iter_coords(sel_iter,coords)
        H5S_sel_iter_t *sel_iter; IN: Selection iterator to query
        hsize_t *coords;         OUT: Array to place iterator coordinates in
 RETURNS
    Non-negative on success, negative on failure.
 DESCRIPTION
    The current location of the iterator within the selection is placed in
    the COORDS array.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_iter_coords(const H5S_sel_iter_t *sel_iter, hsize_t *coords)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(sel_iter);
    assert(coords);

    /* Call iter_coords routine for selection type */
    ret_value = (*sel_iter->type->iter_coords)(sel_iter, coords);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_iter_coords() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_iter_nelmts
 PURPOSE
    Get the number of elements left to iterate over in selection
 USAGE
    hssize_t H5S_select_iter_nelmts(sel_iter)
        H5S_sel_iter_t *sel_iter; IN: Selection iterator to query
 RETURNS
    The number of elements in selection on success, 0 on failure
 DESCRIPTION
    Returns the number of elements in current selection for dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hsize_t
H5S_select_iter_nelmts(const H5S_sel_iter_t *sel_iter)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(sel_iter);

    /* Call iter_nelmts routine for selection type */
    ret_value = (*sel_iter->type->iter_nelmts)(sel_iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_iter_nelmts() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_iter_next
 PURPOSE
    Advance selection iterator to next element
 USAGE
    herr_t H5S_select_iter_next(iter, nelem)
        H5S_sel_iter_t *iter;   IN/OUT: Selection iterator to change
        size_t nelem;           IN: Number of elements to advance by
 RETURNS
    Non-negative on success, negative on failure.
 DESCRIPTION
    Move the current element for the selection iterator to the NELEM'th next
    element in the selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_iter_next(H5S_sel_iter_t *iter, size_t nelem)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(iter);
    assert(nelem > 0);

    /* Call iter_next routine for selection type */
    ret_value = (*iter->type->iter_next)(iter, nelem);

    /* Decrement the number of elements left in selection */
    iter->elmt_left -= nelem;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_iter_next() */

/*-------------------------------------------------------------------------
 * Function:	H5S_select_iter_get_seq_list
 *
 * Purpose:	Retrieves the next sequence of offset/length pairs for an
 *              iterator on a dataspace
 *
 * Return:	Non-negative on success/Negative on failure
 *
 * Note: This routine participates in the "Inlining C function pointers"
 *      pattern, don't call it directly, use the appropriate macro
 *      defined in H5Sprivate.h.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S_select_iter_get_seq_list(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelmts, size_t *nseq,
                             size_t *nelmts, hsize_t *off, size_t *len)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    assert(iter);

    /* Call the selection type's get_seq_list function */
    if ((ret_value = (*iter->type->iter_get_seq_list)(iter, maxseq, maxelmts, nseq, nelmts, off, len)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "unable to get selection sequence list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_iter_get_seq_list() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_iter_release
 PURPOSE
    Release a selection iterator's resources.
 USAGE
    herr_t H5S_select_iter_release(sel_iter)
        H5S_sel_iter_t *sel_iter; IN: Selection iterator to query
 RETURNS
    The number of elements in selection on success, 0 on failure
 DESCRIPTION
    Returns the number of elements in current selection for dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_iter_release(H5S_sel_iter_t *sel_iter)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(sel_iter);

    /* Call selection type-specific release routine */
    ret_value = (*sel_iter->type->iter_release)(sel_iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_iter_release() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_iterate
 PURPOSE
    Iterate over the selected elements in a memory buffer.
 USAGE
    herr_t H5S_select_iterate(buf, type, space, operator, operator_data)
        void *buf;      IN/OUT: Buffer containing elements to iterate over
        H5T_t *type;    IN: Datatype of BUF array.
        H5S_t *space;   IN: Dataspace object containing selection to iterate over
        H5D_operator_t op; IN: Function pointer to the routine to be
                                called for each element in BUF iterated over.
        void *operator_data;    IN/OUT: Pointer to any user-defined data
                                associated with the operation.
 RETURNS
    Returns the return value of the last operator if it was non-zero, or zero
    if all elements were processed. Otherwise returns a negative value.
 DESCRIPTION
    Iterates over the selected elements in a memory buffer, calling the user's
    callback function for each element.  The selection in the dataspace is
    modified so that any elements already iterated over are removed from the
    selection if the iteration is interrupted (by the H5D_operator_t function
    returning non-zero) in the "middle" of the iteration and may be re-started
    by the user where it left off.

    NOTE: Until "subtracting" elements from a selection is implemented,
        the selection is not modified.
--------------------------------------------------------------------------*/
herr_t
H5S_select_iterate(void *buf, const H5T_t *type, H5S_t *space, const H5S_sel_iter_op_t *op, void *op_data)
{
    H5S_sel_iter_t *iter      = NULL;         /* Selection iteration info */
    bool            iter_init = false;        /* Selection iteration info has been initialized */
    hsize_t        *off       = NULL;         /* Array to store sequence offsets */
    size_t         *len       = NULL;         /* Array to store sequence lengths */
    hssize_t        nelmts;                   /* Number of elements in selection */
    hsize_t         space_size[H5S_MAX_RANK]; /* Dataspace size */
    size_t          max_elem;                 /* Maximum number of elements allowed in sequences */
    size_t          elmt_size;                /* Datatype size */
    unsigned        ndims;                    /* Number of dimensions in dataspace */
    herr_t          user_ret  = 0;            /* User's return value */
    herr_t          ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(buf);
    assert(type);
    assert(space);
    assert(op);

    /* Get the datatype size */
    if (0 == (elmt_size = H5T_get_size(type)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADSIZE, FAIL, "datatype size invalid");

    /* Allocate the selection iterator */
    if (NULL == (iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");

    /* Initialize iterator */
    if (H5S_select_iter_init(iter, space, elmt_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
    iter_init = true; /* Selection iteration info has been initialized */

    /* Get the number of elements in selection */
    if ((nelmts = (hssize_t)H5S_GET_SELECT_NPOINTS(space)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL, "can't get number of elements selected");

    /* Get the rank of the dataspace */
    ndims = space->extent.rank;

    if (ndims > 0) {
        /* Copy the size of the space */
        assert(space->extent.size);
        H5MM_memcpy(space_size, space->extent.size, ndims * sizeof(hsize_t));
    } /* end if */
    space_size[ndims] = elmt_size;

    /* Compute the maximum number of bytes required */
    H5_CHECKED_ASSIGN(max_elem, size_t, nelmts, hssize_t);

    /* Allocate the offset & length arrays */
    if (NULL == (len = H5FL_SEQ_MALLOC(size_t, H5D_IO_VECTOR_SIZE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate length vector array");
    if (NULL == (off = H5FL_SEQ_MALLOC(hsize_t, H5D_IO_VECTOR_SIZE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate offset vector array");

    /* Loop, while elements left in selection */
    while (max_elem > 0 && user_ret == 0) {
        size_t nelem;    /* Number of elements used in sequences */
        size_t nseq;     /* Number of sequences generated */
        size_t curr_seq; /* Current sequence being worked on */

        /* Get the sequences of bytes */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, (size_t)H5D_IO_VECTOR_SIZE, max_elem, &nseq, &nelem, off,
                                         len) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, FAIL, "sequence length generation failed");

        /* Loop, while sequences left to process */
        for (curr_seq = 0; curr_seq < nseq && user_ret == 0; curr_seq++) {
            hsize_t curr_off; /* Current offset within sequence */
            size_t  curr_len; /* Length of bytes left to process in sequence */

            /* Get the current offset */
            curr_off = off[curr_seq];

            /* Get the number of bytes in sequence */
            curr_len = len[curr_seq];

            /* Loop, while bytes left in sequence */
            while (curr_len > 0 && user_ret == 0) {
                hsize_t  coords[H5S_MAX_RANK]; /* Coordinates of element in dataspace */
                hsize_t  tmp_off;              /* Temporary offset within sequence */
                uint8_t *loc;                  /* Current element location in buffer */
                int      i;                    /* Local Index variable */

                /* Compute the coordinate from the offset */
                for (i = (int)ndims, tmp_off = curr_off; i >= 0; i--) {
                    coords[i] = tmp_off % space_size[i];
                    tmp_off /= space_size[i];
                } /* end for */

                /* Get the location within the user's buffer */
                loc = (unsigned char *)buf + curr_off;

                /* Check which type of callback to make */
                switch (op->op_type) {
                    case H5S_SEL_ITER_OP_APP:
                        /* Make the application callback */
                        user_ret = (op->u.app_op.op)(loc, op->u.app_op.type_id, ndims, coords, op_data);
                        break;

                    case H5S_SEL_ITER_OP_LIB:
                        /* Call the library's callback */
                        user_ret = (op->u.lib_op)(loc, type, ndims, coords, op_data);
                        break;

                    default:
                        HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unsupported op type");
                } /* end switch */

                /* Check for error return from iterator */
                if (user_ret < 0)
                    HERROR(H5E_DATASPACE, H5E_CANTNEXT, "iteration operator failed");

                /* Increment offset in dataspace */
                curr_off += elmt_size;

                /* Decrement number of bytes left in sequence */
                curr_len -= elmt_size;
            } /* end while */
        }     /* end for */

        /* Decrement number of elements left to process */
        max_elem -= nelem;
    } /* end while */

    /* Set return value */
    ret_value = user_ret;

done:
    /* Release resources, if allocated */
    if (len)
        len = H5FL_SEQ_FREE(size_t, len);
    if (off)
        off = H5FL_SEQ_FREE(hsize_t, off);

    /* Release selection iterator */
    if (iter_init && H5S_SELECT_ITER_RELEASE(iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
    if (iter)
        iter = H5FL_FREE(H5S_sel_iter_t, iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_iterate() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_select_type
 PURPOSE
    Retrieve the type of selection in a dataspace
 USAGE
    H5S_sel_type H5Sget_select_type(space_id)
        hid_t space_id;	        IN: Dataspace object to query
 RETURNS
    Non-negative on success/Negative on failure.  Return value is from the
    set of values in the H5S_sel_type enumerated type.
 DESCRIPTION
        This function retrieves the type of selection currently defined for
    a dataspace.
--------------------------------------------------------------------------*/
H5S_sel_type
H5Sget_select_type(hid_t space_id)
{
    H5S_t       *space;     /* dataspace to modify */
    H5S_sel_type ret_value; /* Return value */

    FUNC_ENTER_API(H5S_SEL_ERROR)
    H5TRACE1("St", "i", space_id);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, H5S_SEL_ERROR, "not a dataspace");

    /* Set return value */
    ret_value = H5S_GET_SELECT_TYPE(space);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sget_select_type() */

/*--------------------------------------------------------------------------
 NAME
    H5S_get_select_type
 PURPOSE
    Retrieve the type of selection in a dataspace
 USAGE
    H5S_sel_type H5Sget_select_type(space)
        const H5S_t *space;	        IN: Dataspace object to query
 RETURNS
    Non-negative on success/Negative on failure.  Return value is from the
    set of values in the H5S_sel_type enumerated type.
 DESCRIPTION
        This function retrieves the type of selection currently defined for
    a dataspace.
 COMMENTS
     This routine participates in the "Inlining C function pointers"
        pattern, don't call it directly, use the appropriate macro
        defined in H5Sprivate.h.
--------------------------------------------------------------------------*/
H5_ATTR_PURE H5S_sel_type
H5S_get_select_type(const H5S_t *space)
{
    H5S_sel_type ret_value = H5S_SEL_ERROR; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(space);

    /* Set return value */
    ret_value = H5S_GET_SELECT_TYPE(space);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_get_select_type() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_shape_same
 PURPOSE
    Check if two selections are the same shape
 USAGE
    htri_t H5S_select_shape_same(space1, space2)
        const H5S_t *space1;         IN: 1st Dataspace pointer to compare
        const H5S_t *space2;         IN: 2nd Dataspace pointer to compare
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspaces are the same
    dimensionality and shape.

    This is primarily used for reading the entire selection in one swoop.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This routine participates in the "Inlining C function pointers" pattern,
    don't call it directly, use the appropriate macro defined in H5Sprivate.h.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S_select_shape_same(H5S_t *space1, H5S_t *space2)
{
    H5S_sel_iter_t *iter_a      = NULL;  /* Selection a iteration info */
    H5S_sel_iter_t *iter_b      = NULL;  /* Selection b iteration info */
    bool            iter_a_init = false; /* Selection a iteration info has been initialized */
    bool            iter_b_init = false; /* Selection b iteration info has been initialized */
    htri_t          ret_value   = true;  /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(space1);
    assert(space2);

    /* Check for different number of elements selected */
    if (H5S_GET_SELECT_NPOINTS(space1) != H5S_GET_SELECT_NPOINTS(space2))
        HGOTO_DONE(false);

    /* Check special cases if both dataspaces aren't scalar */
    /* (If only one is, the number of selected points check is sufficient) */
    if (space1->extent.rank > 0 && space2->extent.rank > 0) {
        H5S_t       *space_a;      /* Dataspace with larger rank */
        H5S_t       *space_b;      /* Dataspace with smaller rank */
        unsigned     space_a_rank; /* Number of dimensions of dataspace A */
        unsigned     space_b_rank; /* Number of dimensions of dataspace B */
        int          space_a_dim;  /* Current dimension in dataspace A */
        int          space_b_dim;  /* Current dimension in dataspace B */
        H5S_sel_type sel_a_type;   /* Selection type for dataspace A */
        H5S_sel_type sel_b_type;   /* Selection type for dataspace B */

        /* Need to be able to handle spaces of different rank:
         *
         * To simplify logic, let space_a point to the element of the set
         * {space1, space2} with the largest rank or space1 if the ranks
         * are identical.
         *
         * Similarly, let space_b point to the element of {space1, space2}
         * with the smallest rank, or space2 if they are identical.
         *
         * Let:  space_a_rank be the rank of space_a,
         *       space_b_rank be the rank of space_b,
         *
         * Set all this up here.
         */
        if (space1->extent.rank >= space2->extent.rank) {
            space_a = space1;
            space_b = space2;
        } /* end if */
        else {
            space_a = space2;
            space_b = space1;
        } /* end else */
        space_a_rank = space_a->extent.rank;
        space_b_rank = space_b->extent.rank;
        assert(space_a_rank >= space_b_rank);
        assert(space_b_rank > 0);

        /* Get selection type for both dataspaces */
        sel_a_type = H5S_GET_SELECT_TYPE(space_a);
        sel_b_type = H5S_GET_SELECT_TYPE(space_b);

        /* If selections aren't "none", compare their bounds */
        if (sel_a_type != H5S_SEL_NONE && sel_b_type != H5S_SEL_NONE) {
            hsize_t low_a[H5S_MAX_RANK];  /* Low bound of selection in dataspace a */
            hsize_t low_b[H5S_MAX_RANK];  /* Low bound of selection in dataspace b */
            hsize_t high_a[H5S_MAX_RANK]; /* High bound of selection in dataspace a */
            hsize_t high_b[H5S_MAX_RANK]; /* High bound of selection in dataspace b */

            /* Get low & high bounds for both dataspaces */
            if (H5S_SELECT_BOUNDS(space_a, low_a, high_a) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL,
                            "can't get selection bounds for first dataspace");
            if (H5S_SELECT_BOUNDS(space_b, low_b, high_b) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL,
                            "can't get selection bounds for second dataspace");

            /* Check that the range between the low & high bounds are the same */
            space_a_dim = (int)space_a_rank - 1;
            space_b_dim = (int)space_b_rank - 1;
            while (space_b_dim >= 0) {
                /* Sanity check */
                assert(low_a[space_a_dim] <= high_a[space_a_dim]);
                assert(low_a[space_b_dim] <= high_a[space_b_dim]);

                /* Verify that the ranges are the same */
                if ((high_a[space_a_dim] - low_a[space_a_dim]) != (high_b[space_b_dim] - low_b[space_b_dim]))
                    HGOTO_DONE(false);

                /* Go to next dimension */
                space_a_dim--;
                space_b_dim--;
            } /* end while */

            /* Check that the rest of the ranges in space a are "flat" */
            while (space_a_dim >= 0) {
                /* Sanity check */
                assert(low_a[space_a_dim] <= high_a[space_a_dim]);

                /* This range should be flat to be the same in a lower dimension */
                if (low_a[space_a_dim] != high_a[space_a_dim])
                    HGOTO_DONE(false);

                space_a_dim--;
            } /* end while */

            /* Check for a single block in each selection */
            if (H5S_SELECT_IS_SINGLE(space_a) && H5S_SELECT_IS_SINGLE(space_b)) {
                /* If both selections are a single block and their bounds are
                 * the same, then the selections are the same, even if the
                 * selection types are different.
                 */
                HGOTO_DONE(true);
            } /* end if */
        }     /* end if */

        /* If the dataspaces have the same selection type, use the selection's
         * shape_same operator.
         */
        if (sel_a_type == sel_b_type)
            ret_value = (*space_a->select.type->shape_same)(space_a, space_b);
        /* Otherwise, iterate through all the blocks in the selection */
        else {
            hsize_t  start_a[H5S_MAX_RANK]; /* Start point of selection block in dataspace a */
            hsize_t  start_b[H5S_MAX_RANK]; /* Start point of selection block in dataspace b */
            hsize_t  end_a[H5S_MAX_RANK];   /* End point of selection block in dataspace a */
            hsize_t  end_b[H5S_MAX_RANK];   /* End point of selection block in dataspace b */
            hssize_t offset[H5S_MAX_RANK];  /* Offset of selection b blocks relative to selection a blocks */
            bool     first_block = true;    /* Flag to indicate the first block */

            /* Allocate the selection iterators */
            if (NULL == (iter_a = H5FL_MALLOC(H5S_sel_iter_t)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");
            if (NULL == (iter_b = H5FL_MALLOC(H5S_sel_iter_t)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");

            /* Initialize iterator for each dataspace selection
             * Use '0' for element size instead of actual element size to indicate
             * that the selection iterator shouldn't be "flattened", since we
             * aren't actually going to be doing I/O with the iterators.
             */
            if (H5S_select_iter_init(iter_a, space_a, (size_t)0, 0) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator a");
            iter_a_init = true;
            if (H5S_select_iter_init(iter_b, space_b, (size_t)0, 0) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator b");
            iter_b_init = true;

            /* Iterate over all the blocks in each selection */
            while (1) {
                htri_t status_a, status_b; /* Status from next block checks */

                /* Get the current block for each selection iterator */
                if (H5S_SELECT_ITER_BLOCK(iter_a, start_a, end_a) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "unable to get iterator block a");
                if (H5S_SELECT_ITER_BLOCK(iter_b, start_b, end_b) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "unable to get iterator block b");

                space_a_dim = (int)space_a_rank - 1;
                space_b_dim = (int)space_b_rank - 1;

                /* The first block only compares the sizes and sets the
                 * relative offsets for later blocks
                 */
                if (first_block) {
                    /* If the block sizes in the common dimensions from
                     * each selection don't match, get out
                     */
                    while (space_b_dim >= 0) {
                        if ((end_a[space_a_dim] - start_a[space_a_dim]) !=
                            (end_b[space_b_dim] - start_b[space_b_dim]))
                            HGOTO_DONE(false);

                        /* Set the relative locations of the selections */
                        offset[space_a_dim] = (hssize_t)start_b[space_b_dim] - (hssize_t)start_a[space_a_dim];

                        space_a_dim--;
                        space_b_dim--;
                    } /* end while */

                    /* Similarly, if the block size in any dimension that appears only
                     * in space_a is not equal to 1, get out.
                     */
                    while (space_a_dim >= 0) {
                        if (start_a[space_a_dim] != end_a[space_a_dim])
                            HGOTO_DONE(false);

                        space_a_dim--;
                    } /* end while */

                    /* Reset "first block" flag */
                    first_block = false;
                } /* end if */
                /* Check over the blocks for each selection */
                else {
                    /* For dimensions that space_a and space_b have in common: */
                    while (space_b_dim >= 0) {
                        /* Check if the blocks are in the same relative location */
                        if ((hsize_t)((hssize_t)start_a[space_a_dim] + offset[space_a_dim]) !=
                            start_b[space_b_dim])
                            HGOTO_DONE(false);

                        /* If the block sizes from each selection doesn't match, get out */
                        if ((end_a[space_a_dim] - start_a[space_a_dim]) !=
                            (end_b[space_b_dim] - start_b[space_b_dim]))
                            HGOTO_DONE(false);

                        space_a_dim--;
                        space_b_dim--;
                    } /* end while */

                    /* For dimensions that appear only in space_a: */
                    while (space_a_dim >= 0) {
                        /* If the block size isn't 1, get out */
                        if (start_a[space_a_dim] != end_a[space_a_dim])
                            HGOTO_DONE(false);

                        space_a_dim--;
                    } /* end while */
                }     /* end else */

                /* Check if we are able to advance to the next selection block */
                if ((status_a = H5S_SELECT_ITER_HAS_NEXT_BLOCK(iter_a)) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, FAIL, "unable to check iterator block a");

                if ((status_b = H5S_SELECT_ITER_HAS_NEXT_BLOCK(iter_b)) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, FAIL, "unable to check iterator block b");

                /* Did we run out of blocks at the same time? */
                if ((status_a == false) && (status_b == false))
                    break;
                else if (status_a != status_b)
                    HGOTO_DONE(false);
                else {
                    /* Advance to next block in selection iterators */
                    if (H5S_SELECT_ITER_NEXT_BLOCK(iter_a) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, FAIL,
                                    "unable to advance to next iterator block a");

                    if (H5S_SELECT_ITER_NEXT_BLOCK(iter_b) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, FAIL,
                                    "unable to advance to next iterator block b");
                } /* end else */
            }     /* end while */
        }         /* end else */
    }             /* end if */

done:
    if (iter_a_init && H5S_SELECT_ITER_RELEASE(iter_a) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator a");
    if (iter_a)
        iter_a = H5FL_FREE(H5S_sel_iter_t, iter_a);
    if (iter_b_init && H5S_SELECT_ITER_RELEASE(iter_b) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator b");
    if (iter_b)
        iter_b = H5FL_FREE(H5S_sel_iter_t, iter_b);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_shape_same() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_shape_same
 PURPOSE
    Check if two selections are the same shape
 USAGE
    htri_t H5Sselect_shape_same(space1_id, space2_id)
        hid_t space1_id;         IN: ID of 1st Dataspace pointer to compare
        hid_t space2_id;         IN: ID of 2nd Dataspace pointer to compare
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspaces are the same
    dimensionality and shape.
    This is primarily used for reading the entire selection in one swoop.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5Sselect_shape_same(hid_t space1_id, hid_t space2_id)
{
    H5S_t *space1, *space2; /* Dataspaces to compare */
    htri_t ret_value;       /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("t", "ii", space1_id, space2_id);

    if (NULL == (space1 = (H5S_t *)H5I_object_verify(space1_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == (space2 = (H5S_t *)H5I_object_verify(space2_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    if ((ret_value = H5S_select_shape_same(space1, space2)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't compare selections");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_shape_same() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_intersect_block
 PURPOSE
    Check if current selection intersects with a block
 USAGE
    htri_t H5S_select_intersect_block(space, start, end)
        const H5S_t *space;      IN: Dataspace to compare
        const hsize_t *start;    IN: Starting coordinate of block
        const hsize_t *end;      IN: Opposite ("ending") coordinate of block
 RETURNS
    true / false / FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspace intersects with
    the block given.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Assumes that start & end block bounds are _inclusive_, so start == end
    value OK.

    This routine participates in the "Inlining C function pointers" pattern,
    don't call it directly, use the appropriate macro defined in H5Sprivate.h.
--------------------------------------------------------------------------*/
htri_t
H5S_select_intersect_block(H5S_t *space, const hsize_t *start, const hsize_t *end)
{
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(space);
    assert(start);
    assert(end);

    /* If selections aren't "none", compare their bounds */
    if (H5S_SEL_NONE != H5S_GET_SELECT_TYPE(space)) {
        hsize_t  low[H5S_MAX_RANK];  /* Low bound of selection in dataspace */
        hsize_t  high[H5S_MAX_RANK]; /* High bound of selection in dataspace */
        unsigned u;                  /* Local index variable */

        /* Get low & high bounds for dataspace selection */
        if (H5S_SELECT_BOUNDS(space, low, high) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get selection bounds for dataspace");

        /* Loop over selection bounds and block, checking for overlap */
        for (u = 0; u < space->extent.rank; u++)
            /* If selection bounds & block don't overlap, can leave now */
            if (!H5S_RANGE_OVERLAP(low[u], high[u], start[u], end[u]))
                HGOTO_DONE(false);
    } /* end if */

    /* Call selection type's intersect routine */
    if ((ret_value = (*space->select.type->intersect_block)(space, start, end)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't intersect block with selection");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_intersect_block() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_intersect_block
 PURPOSE
    Check if current selection intersects with a block
 USAGE
    htri_t H5Sselect_intersect_block(space_id, start, end)
        hid_t space1_id;         IN: ID of dataspace pointer to compare
        const hsize_t *start;    IN: Starting coordinate of block
        const hsize_t *end;      IN: Opposite ("ending") coordinate of block
 RETURNS
    true / false / FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspace intersects with
    the block given.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Assumes that start & end block bounds are _inclusive_, so start == end
    value OK.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5Sselect_intersect_block(hid_t space_id, const hsize_t *start, const hsize_t *end)
{
    H5S_t   *space;            /* Dataspace to query */
    unsigned u;                /* Local index value */
    htri_t   ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("t", "i*h*h", space_id, start, end);

    /* Check arguments */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == start)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "block start array pointer is NULL");
    if (NULL == end)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "block end array pointer is NULL");

    /* Range check start & end values */
    for (u = 0; u < space->extent.rank; u++)
        if (start[u] > end[u])
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "block start[%u] (%llu) > end[%u] (%llu)", u,
                        (unsigned long long)start[u], u, (unsigned long long)end[u]);

    /* Call internal routine to do comparison */
    if ((ret_value = H5S_select_intersect_block(space, start, end)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't compare selection and block");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_intersect_block() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_construct_projection

 PURPOSE
    Given a dataspace a of rank n with some selection, construct a new
    dataspace b of rank m (m != n), with the selection in a being
    topologically identical to that in b (as verified by
    H5S_select_shape_same().

    This function exists, as some I/O code chokes on topologically
    identical selections with different ranks.  At least to begin
    with, we will deal with the issue by constructing projections
    of the memory dataspace with ranks equaling those of the file
    dataspace.

    Note that if m > n, it is possible that the starting point in the
    buffer associated with the memory dataspace will have to be
    adjusted to match the projected dataspace. In this case, the amount
    of adjustment to be applied to the buffer will be returned via the
    buf_adj parameter, if supplied.

 USAGE
    htri_t H5S_select_construct_projection(base_space,
                                           new_space_ptr,
                                           new_space_rank,
                                           element_size,
                                           buf_adj)
        const H5S_t *base_space;     IN: Ptr to Dataspace to project
        H5S_t ** new_space_ptr;     OUT: Ptr to location in which to return
                                         the address of the projected space
        int new_space_rank;	     IN: Rank of the projected space.
        hsize_t element_size;        IN: size of each element in the selection
        ptrdiff_t buf_adj;          OUT: amount of adjustment to be applied
                                         to buffer associated with memory
                                         dataspace

 RETURNS
    Non-negative on success/Negative on failure.

 DESCRIPTION
    Construct a new dataspace and associated selection which is a
    projection of the supplied dataspace and associated selection into
    the specified rank.  Return it in *new_space_ptr.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    The selection in the supplied base_space has thickness 1 in all
    dimensions greater than new_space_rank.  Note that here we count
    dimensions from the fastest changing coordinate to the slowest
    changing changing coordinate.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_construct_projection(H5S_t *base_space, H5S_t **new_space_ptr, unsigned new_space_rank,
                                hsize_t element_size, ptrdiff_t *buf_adj)
{
    H5S_t   *new_space = NULL;                         /* New dataspace constructed */
    hsize_t  base_space_dims[H5S_MAX_RANK];            /* Current dimensions of base dataspace */
    hsize_t  base_space_maxdims[H5S_MAX_RANK];         /* Maximum dimensions of base dataspace */
    int      sbase_space_rank;                         /* Signed # of dimensions of base dataspace */
    unsigned base_space_rank;                          /* # of dimensions of base dataspace */
    hsize_t  projected_space_element_offset = 0;       /* Offset of selected element in projected buffer */
    herr_t   ret_value                      = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(base_space != NULL);
    assert((H5S_GET_EXTENT_TYPE(base_space) == H5S_SCALAR) ||
           (H5S_GET_EXTENT_TYPE(base_space) == H5S_SIMPLE));
    assert(new_space_ptr != NULL);
    assert((new_space_rank != 0) || (H5S_GET_SELECT_NPOINTS(base_space) <= 1));
    assert(new_space_rank <= H5S_MAX_RANK);
    assert(element_size > 0);

    /* Get the extent info for the base dataspace */
    if ((sbase_space_rank = H5S_get_simple_extent_dims(base_space, base_space_dims, base_space_maxdims)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "unable to get dimensionality of base space");
    base_space_rank = (unsigned)sbase_space_rank;
    assert(base_space_rank != new_space_rank);

    /* Check if projected space is scalar */
    if (new_space_rank == 0) {
        hssize_t npoints; /* Number of points selected */

        /* Retrieve the number of elements selected */
        if ((npoints = (hssize_t)H5S_GET_SELECT_NPOINTS(base_space)) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "unable to get number of points selected");
        assert(npoints <= 1);

        /* Create new scalar dataspace */
        if (NULL == (new_space = H5S_create(H5S_SCALAR)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "unable to create scalar dataspace");

        /* No need to register the dataspace(i.e. get an ID) as
         * we will just be discarding it shortly.
         */

        /* Selection for the new space will be either all or
         * none, depending on whether the base space has 0 or
         * 1 elements selected.
         *
         * Observe that the base space can't have more than
         * one selected element, since its selection has the
         * same shape as the file dataspace, and that data
         * space is scalar.
         */
        if (1 == npoints) {
            /* Assuming that the selection in the base dataspace is not
             * empty, we must compute the offset of the selected item in
             * the buffer associated with the base dataspace.
             *
             * Since the new space rank is zero, we know that the
             * the base space must have rank at least 1 -- and
             * hence it is a simple dataspace.  However, the
             * selection, may be either point, hyperspace, or all.
             *
             */
            if (H5S_SELECT_PROJECT_SCALAR(base_space, &projected_space_element_offset) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "unable to project scalar selection");
        } /* end if */
        else {
            assert(0 == npoints);

            if (H5S_select_none(new_space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't delete default selection");
        }                                         /* end else */
    }                                             /* end if */
    else {                                        /* projected space must be simple */
        hsize_t  new_space_dims[H5S_MAX_RANK];    /* Current dimensions for new dataspace */
        hsize_t  new_space_maxdims[H5S_MAX_RANK]; /* Maximum dimensions for new dataspace */
        unsigned rank_diff;                       /* Difference in ranks */

        /* Set up the dimensions of the new, projected dataspace.
         *
         * How we do this depends on whether we are projecting up into
         * increased dimensions, or down into a reduced number of
         * dimensions.
         *
         * If we are projecting up (the first half of the following
         * if statement), we copy the dimensions of the base data
         * space into the fastest changing dimensions of the new
         * projected dataspace, and set the remaining dimensions to
         * one.
         *
         * If we are projecting down (the second half of the following
         * if statement), we just copy the dimensions with the most
         * quickly changing dimensions into the dims for the projected
         * data set.
         *
         * This works, because H5S_select_shape_same() will return
         * true on selections of different rank iff:
         *
         * 1) the selection in the lower rank dataspace matches that
         *    in the dimensions with the fastest changing indices in
         *    the larger rank dataspace, and
         *
         * 2) the selection has thickness 1 in all ranks that appear
         *    only in the higher rank dataspace (i.e. those with
         *    more slowly changing indices).
         */
        if (new_space_rank > base_space_rank) {
            hsize_t tmp_dim_size = 1; /* Temporary dimension value, for filling arrays */

            /* we must copy the dimensions of the base space into
             * the fastest changing dimensions of the new space,
             * and set the remaining dimensions to 1
             */
            rank_diff = new_space_rank - base_space_rank;
            H5VM_array_fill(new_space_dims, &tmp_dim_size, sizeof(tmp_dim_size), rank_diff);
            H5VM_array_fill(new_space_maxdims, &tmp_dim_size, sizeof(tmp_dim_size), rank_diff);
            H5MM_memcpy(&new_space_dims[rank_diff], base_space_dims,
                        sizeof(new_space_dims[0]) * base_space_rank);
            H5MM_memcpy(&new_space_maxdims[rank_diff], base_space_maxdims,
                        sizeof(new_space_maxdims[0]) * base_space_rank);
        }      /* end if */
        else { /* new_space_rank < base_space_rank */
            /* we must copy the fastest changing dimension of the
             * base space into the dimensions of the new space.
             */
            rank_diff = base_space_rank - new_space_rank;
            H5MM_memcpy(new_space_dims, &base_space_dims[rank_diff],
                        sizeof(new_space_dims[0]) * new_space_rank);
            H5MM_memcpy(new_space_maxdims, &base_space_maxdims[rank_diff],
                        sizeof(new_space_maxdims[0]) * new_space_rank);
        } /* end else */

        /* now have the new space rank and dimensions set up --
         * so we can create the new simple dataspace.
         */
        if (NULL == (new_space = H5S_create_simple(new_space_rank, new_space_dims, new_space_maxdims)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create simple dataspace");

        /* No need to register the dataspace(i.e. get an ID) as
         * we will just be discarding it shortly.
         */

        /* If we get this far, we have successfully created the projected
         * dataspace.  We must now project the selection in the base
         * dataspace into the projected dataspace.
         */
        if (H5S_SELECT_PROJECT_SIMPLE(base_space, new_space, &projected_space_element_offset) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "unable to project simple selection");

        /* If we get this far, we have created the new dataspace, and projected
         * the selection in the base dataspace into the new dataspace.
         *
         * If the base dataspace is simple, check to see if the
         * offset_changed flag on the base selection has been set -- if so,
         * project the offset into the new dataspace and set the
         * offset_changed flag.
         */
        if (H5S_GET_EXTENT_TYPE(base_space) == H5S_SIMPLE && base_space->select.offset_changed) {
            if (new_space_rank > base_space_rank) {
                memset(new_space->select.offset, 0, sizeof(new_space->select.offset[0]) * rank_diff);
                H5MM_memcpy(&new_space->select.offset[rank_diff], base_space->select.offset,
                            sizeof(new_space->select.offset[0]) * base_space_rank);
            } /* end if */
            else
                H5MM_memcpy(new_space->select.offset, &base_space->select.offset[rank_diff],
                            sizeof(new_space->select.offset[0]) * new_space_rank);

            /* Propagate the offset changed flag into the new dataspace. */
            new_space->select.offset_changed = true;
        } /* end if */
    }     /* end else */

    /* If we have done the projection correctly, the following assertion
     * should hold.
     */
    assert(true == H5S_select_shape_same(base_space, new_space));

    /* load the address of the new space into *new_space_ptr */
    *new_space_ptr = new_space;

    /* return the buffer adjustment amount if required */
    if (buf_adj != NULL) {
        if (new_space_rank < base_space_rank) {
            *buf_adj = (ptrdiff_t)(projected_space_element_offset * element_size);
        }
        else
            /* No adjustment necessary */
            *buf_adj = 0;
    }

done:
    /* Cleanup on error */
    if (ret_value < 0)
        if (new_space && H5S_close(new_space) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release dataspace");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_construct_projection() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_fill
 PURPOSE
    Fill a selection in memory with a value
 USAGE
    herr_t H5S_select_fill(fill,fill_size,space,buf)
        const void *fill;       IN: Pointer to fill value to use
        size_t fill_size;       IN: Size of elements in memory buffer & size of
                                    fill value
        H5S_t *space;           IN: Dataspace describing memory buffer &
                                    containing selection to use.
        void *buf;              IN/OUT: Memory buffer to fill selection in
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Use the selection in the dataspace to fill elements in a memory buffer.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    The memory buffer elements are assumed to have the same datatype as the
    fill value being placed into them.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_fill(const void *fill, size_t fill_size, H5S_t *space, void *_buf)
{
    H5S_sel_iter_t *iter      = NULL;    /* Selection iteration info */
    bool            iter_init = false;   /* Selection iteration info has been initialized */
    hsize_t        *off       = NULL;    /* Array to store sequence offsets */
    size_t         *len       = NULL;    /* Array to store sequence lengths */
    hssize_t        nelmts;              /* Number of elements in selection */
    size_t          max_elem;            /* Total number of elements in selection */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(fill);
    assert(fill_size > 0);
    assert(space);
    assert(_buf);

    /* Allocate the selection iterator */
    if (NULL == (iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");

    /* Initialize iterator */
    if (H5S_select_iter_init(iter, space, fill_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
    iter_init = true; /* Selection iteration info has been initialized */

    /* Get the number of elements in selection */
    if ((nelmts = (hssize_t)H5S_GET_SELECT_NPOINTS(space)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL, "can't get number of elements selected");

    /* Compute the number of bytes to process */
    H5_CHECKED_ASSIGN(max_elem, size_t, nelmts, hssize_t);

    /* Allocate the offset & length arrays */
    if (NULL == (len = H5FL_SEQ_MALLOC(size_t, H5D_IO_VECTOR_SIZE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate length vector array");
    if (NULL == (off = H5FL_SEQ_MALLOC(hsize_t, H5D_IO_VECTOR_SIZE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate offset vector array");

    /* Loop, while elements left in selection */
    while (max_elem > 0) {
        size_t nseq;     /* Number of sequences generated */
        size_t curr_seq; /* Current sequence being worked on */
        size_t nelem;    /* Number of elements used in sequences */

        /* Get the sequences of bytes */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, (size_t)H5D_IO_VECTOR_SIZE, max_elem, &nseq, &nelem, off,
                                         len) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, FAIL, "sequence length generation failed");

        /* Loop over sequences */
        for (curr_seq = 0; curr_seq < nseq; curr_seq++) {
            uint8_t *buf; /* Current location in buffer */

            /* Get offset in memory buffer */
            buf = (uint8_t *)_buf + off[curr_seq];

            /* Fill each sequence in memory with fill value */
            assert((len[curr_seq] % fill_size) == 0);
            H5VM_array_fill(buf, fill, fill_size, (len[curr_seq] / fill_size));
        } /* end for */

        /* Decrement number of elements left to process */
        max_elem -= nelem;
    } /* end while */

done:
    /* Release resources, if allocated */
    if (len)
        len = H5FL_SEQ_FREE(size_t, len);
    if (off)
        off = H5FL_SEQ_FREE(hsize_t, off);

    /* Release selection iterator */
    if (iter_init && H5S_SELECT_ITER_RELEASE(iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
    if (iter)
        iter = H5FL_FREE(H5S_sel_iter_t, iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_fill() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_project_intersection

 PURPOSE
    Projects the intersection of of the selections of src_space and
    src_intersect_space within the selection of src_space as a selection
    within the selection of dst_space

 USAGE
    herr_t H5S_select_project_intersection(src_space,dst_space,src_intersect_space,proj_space,share_selection)
        H5S_t *src_space;       IN: Selection that is mapped to dst_space, and intersected with
src_intersect_space H5S_t *dst_space;       IN: Selection that is mapped to src_space H5S_t
*src_intersect_space; IN: Selection whose intersection with src_space is projected to dst_space to obtain the
result H5S_t **new_space_ptr;  OUT: Will contain the result (intersection of src_intersect_space and src_space
projected from src_space to dst_space) after the operation bool share_selection; IN: Whether we are allowed
to share structures inside dst_space with proj_space

 RETURNS
    Non-negative on success/Negative on failure.

 DESCRIPTION
    Projects the intersection of of the selections of src_space and
    src_intersect_space within the selection of src_space as a selection
    within the selection of dst_space.  The result is placed in the
    selection of new_space_ptr.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_project_intersection(H5S_t *src_space, H5S_t *dst_space, H5S_t *src_intersect_space,
                                H5S_t **new_space_ptr, bool share_selection)
{
    H5S_t          *new_space               = NULL;    /* New dataspace constructed */
    H5S_t          *tmp_src_intersect_space = NULL;    /* Temporary SIS converted from points->hyperslabs */
    H5S_sel_iter_t *ss_iter                 = NULL;    /* Selection iterator for src_space */
    bool            ss_iter_init            = false;   /* Whether ss_iter has been initialized */
    H5S_sel_iter_t *ds_iter                 = NULL;    /* Selection iterator for dst_space */
    bool            ds_iter_init            = false;   /* Whether ds_iter has been initialized */
    herr_t          ret_value               = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(src_space);
    assert(dst_space);
    assert(src_intersect_space);
    assert(new_space_ptr);
    assert(H5S_GET_SELECT_NPOINTS(src_space) == H5S_GET_SELECT_NPOINTS(dst_space));
    assert(H5S_GET_EXTENT_NDIMS(src_space) == H5S_GET_EXTENT_NDIMS(src_intersect_space));

    if (NULL == (ss_iter = H5FL_CALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");
    if (NULL == (ds_iter = H5FL_CALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");

    /* Create new space, using dst extent.  Start with "all" selection. */
    if (NULL == (new_space = H5S_create(H5S_SIMPLE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "unable to create output dataspace");
    if (H5S__extent_copy_real(&new_space->extent, &dst_space->extent, true) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "unable to copy destination space extent");

    /* If the intersecting space is "all", the intersection must be equal to the
     * source space and the projection must be equal to the destination space */
    if (H5S_GET_SELECT_TYPE(src_intersect_space) == H5S_SEL_ALL) {
        /* Copy the destination selection. */
        if (H5S_select_copy(new_space, dst_space, false) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy destination space selection");
    } /* end if */
    /* If any of the selections contain no elements, the projection must be
     * "none" */
    else if ((H5S_GET_SELECT_NPOINTS(src_intersect_space) == 0) || (H5S_GET_SELECT_NPOINTS(src_space) == 0) ||
             (H5S_GET_SELECT_NPOINTS(dst_space) == 0)) {
        /* Change to "none" selection */
        if (H5S_select_none(new_space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't change selection");
    } /* end if */
    else {
        /* Handle scalar dataspaces.  It should not be possible for the source
         * intersect space or the source space to be scalar since scalar spaces
         * only support all or none selections, and both of those cases are
         * covered above, and the source intersect space must have the same
         * rank, so it also cannot be scalar, as scalar dataspaces have a rank
         * of 0. */
        assert(H5S_GET_EXTENT_TYPE(src_space) != H5S_SCALAR);
        assert(H5S_GET_EXTENT_TYPE(src_intersect_space) != H5S_SCALAR);

        /* Check for scalar dst_space.  In this case we simply check if the
         * (single) point selected in src_space intersects src_intersect_space,
         * if so select all in new_space, otherwise select none. */
        if (H5S_GET_EXTENT_TYPE(dst_space) == H5S_SCALAR) {
            hsize_t coords_start[H5S_MAX_RANK];
            hsize_t coords_end[H5S_MAX_RANK];
            htri_t  intersect;

            /* Get source space bounds.  Should be a single point. */
            if (H5S_SELECT_BOUNDS(src_space, coords_start, coords_end) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get source space bounds");
            assert(0 == memcmp(coords_start, coords_end,
                               H5S_GET_EXTENT_NDIMS(src_space) * sizeof(coords_start[0])));

            /* Check for intersection */
            if ((intersect = H5S_SELECT_INTERSECT_BLOCK(src_intersect_space, coords_start, coords_end)) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't check for intersection");

            /* Select all or none as appropriate */
            if (intersect) {
                if (H5S_select_all(new_space, true) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSET, FAIL, "can't select all");
            } /* end if */
            else if (H5S_select_none(new_space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't change selection");
        } /* end if */
        else {
            /* If the source intersect space is a point selection, convert it to a
             * hyperslab (discarding ordering).  We can get away with this because
             * the order does not matter for the source intersect space */
            /* Maybe we should just leave it as a point selection for the point by
             * point algorithm?  The search through the selection in
             * H5S_SELECT_INTERSECT_BLOCK will likely be O(N) either way.  -NAF */
            if (H5S_GET_SELECT_TYPE(src_intersect_space) == H5S_SEL_POINTS) {
                H5S_pnt_node_t *curr_pnt = src_intersect_space->select.sel_info.pnt_lst->head;

                /* Create dataspace and copy extent */
                if (NULL == (tmp_src_intersect_space = H5S_create(H5S_SIMPLE)))
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL,
                                "unable to create temporary source intersect dataspace");
                if (H5S__extent_copy_real(&tmp_src_intersect_space->extent, &src_intersect_space->extent,
                                          false) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL,
                                "unable to copy source intersect space extent");

                /* Iterate over points */
                for (curr_pnt = src_intersect_space->select.sel_info.pnt_lst->head; curr_pnt;
                     curr_pnt = curr_pnt->next)
                    /* Add point to hyperslab selection */
                    if (H5S_hyper_add_span_element(tmp_src_intersect_space, src_intersect_space->extent.rank,
                                                   curr_pnt->pnt) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                    "can't add point to temporary dataspace selection");

                /* Redirect local src_intersect_space pointer (will not affect
                 * calling function) */
                src_intersect_space = tmp_src_intersect_space;
            } /* end for */

            /* By this point, src_intersect_space must be a hyperslab selection */
            assert(H5S_GET_SELECT_TYPE(src_intersect_space) == H5S_SEL_HYPERSLABS);

            /* If either the source space or the destination space is a point
             * selection, iterate element by element */
            if ((H5S_GET_SELECT_TYPE(src_space) == H5S_SEL_POINTS) ||
                (H5S_GET_SELECT_TYPE(dst_space) == H5S_SEL_POINTS)) {
                hsize_t coords[H5S_MAX_RANK];
                htri_t  intersect;

                /* Start with "none" selection */
                if (H5S_select_none(new_space) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't change selection");

                /* Initialize iterators */
                if (H5S_select_iter_init(ss_iter, src_space, 1, H5S_SEL_ITER_SHARE_WITH_DATASPACE) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL,
                                "can't initialize source space selection iterator");
                ss_iter_init = true;
                if (H5S_select_iter_init(ds_iter, dst_space, 1, H5S_SEL_ITER_SHARE_WITH_DATASPACE) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL,
                                "can't initialize destination space selection iterator");
                ds_iter_init = true;

                /* Iterate over points */
                do {
                    assert(ss_iter->elmt_left > 0);
                    assert(ss_iter->elmt_left > 0);

                    /* Get SS coords */
                    if (H5S_SELECT_ITER_COORDS(ss_iter, coords) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL,
                                    "can't get source selection coordinates");

                    /* Check for intersection */
                    if ((intersect = H5S_SELECT_INTERSECT_BLOCK(src_intersect_space, coords, coords)) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOMPARE, FAIL, "can't check for intersection");

                    /* Add point if it intersects */
                    if (intersect) {
                        /* Get DS coords */
                        if (H5S_SELECT_ITER_COORDS(ds_iter, coords) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL,
                                        "can't get destination selection coordinates");

                        /* Add point to new_space */
                        if (H5S_select_elements(new_space, H5S_SELECT_APPEND, 1, coords) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL,
                                        "can't add point to new selection");
                    } /* end if */

                    /* Advance iterators */
                    if (H5S_SELECT_ITER_NEXT(ss_iter, 1) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, FAIL,
                                    "can't advacne source selection iterator");
                    ss_iter->elmt_left--;
                    if (H5S_SELECT_ITER_NEXT(ds_iter, 1) < 0)
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, FAIL,
                                    "can't advacne destination selection iterator");
                    ds_iter->elmt_left--;
                } while (ss_iter->elmt_left > 0);
                assert(H5S_SELECT_ITER_NELMTS(ds_iter) == 0);
            } /* end if */
            else {
                assert(H5S_GET_SELECT_TYPE(src_space) != H5S_SEL_NONE);
                assert(H5S_GET_SELECT_TYPE(dst_space) != H5S_SEL_NONE);

                /* Source and destination selections are all or hyperslab,
                 * intersecting selection is hyperslab.  Call the hyperslab routine
                 * to project to another hyperslab selection. */
                if (H5S__hyper_project_intersection(src_space, dst_space, src_intersect_space, new_space,
                                                    share_selection) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL,
                                "can't project hyperslab onto destination selection");
            } /* end else */
        }     /* end else */
    }         /* end else */

    /* load the address of the new space into *new_space_ptr */
    *new_space_ptr = new_space;

done:
    /* Cleanup on error */
    if (ret_value < 0)
        if (new_space && H5S_close(new_space) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release dataspace");

    /* General cleanup */
    if (tmp_src_intersect_space && H5S_close(tmp_src_intersect_space) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release temporary dataspace");
    if (ss_iter_init && H5S_SELECT_ITER_RELEASE(ss_iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release source selection iterator");
    if (ds_iter_init && H5S_SELECT_ITER_RELEASE(ds_iter) < 0)
        HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release destination selection iterator");

    ss_iter = H5FL_FREE(H5S_sel_iter_t, ss_iter);
    ds_iter = H5FL_FREE(H5S_sel_iter_t, ds_iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_project_intersection() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_project_intersection

 PURPOSE
    Projects the intersection of of the selections of src_space_id and
    src_intersect_space_id within the selection of src_space_id as a
    selection within the selection of dst_space_id.

 USAGE
    hid_t H5Sselect_project_intersection(src_space_id,dst_space_d,src_intersect_space_id)
        hid_t src_space_id;         IN: Selection that is mapped to dst_space_id, and intersected with
src_intersect_space_id hid_t dst_space_id;         IN: Selection that is mapped to src_space_id hid_t
src_intersect_space_id; IN: Selection whose intersection with src_space_id is projected to dst_space_id to
obtain the result

 RETURNS
    A dataspace with a selection equal to the intersection of
    src_intersect_space_id and src_space_id projected from src_space to
    dst_space on success, negative on failure.

 DESCRIPTION
    Projects the intersection of of the selections of src_space and
    src_intersect_space within the selection of src_space as a selection
    within the selection of dst_space.  The result is placed in the
    selection of new_space_ptr.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5Sselect_project_intersection(hid_t src_space_id, hid_t dst_space_id, hid_t src_intersect_space_id)
{
    H5S_t *src_space, *dst_space, *src_intersect_space; /* Input dataspaces */
    H5S_t *proj_space = NULL;                           /* Output dataspace */
    hid_t  ret_value;                                   /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("i", "iii", src_space_id, dst_space_id, src_intersect_space_id);

    /* Check args */
    if (NULL == (src_space = (H5S_t *)H5I_object_verify(src_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == (dst_space = (H5S_t *)H5I_object_verify(dst_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == (src_intersect_space = (H5S_t *)H5I_object_verify(src_intersect_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Check numbers of points selected matches in source and destination */
    if (H5S_GET_SELECT_NPOINTS(src_space) != H5S_GET_SELECT_NPOINTS(dst_space))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                    "number of points selected in source space does not match that in destination space");

    /* Check numbers of dimensions matches in source and source intersect spaces
     */
    if (H5S_GET_EXTENT_NDIMS(src_space) != H5S_GET_EXTENT_NDIMS(src_intersect_space))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                    "rank of source space does not match rank of source intersect space");

    /* Perform operation */
    if (H5S_select_project_intersection(src_space, dst_space, src_intersect_space, &proj_space, false) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCLIP, FAIL, "can't project dataspace intersection");

    /* Register */
    if ((ret_value = H5I_register(H5I_DATASPACE, proj_space, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register dataspace ID");

done:
    if (ret_value < 0)
        if (proj_space && H5S_close(proj_space) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release dataspace");

    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_project_intersection() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_subtract

 PURPOSE
    Subtract one selection from another

 USAGE
    herr_t H5S_select_subtract(space,subtract_space)
        H5S_t *space;           IN/OUT: Selection to be operated on
        H5S_t *subtract_space;  IN: Selection that will be subtracted from space

 RETURNS
    Non-negative on success/Negative on failure.

 DESCRIPTION
    Removes any and all portions of space that are also present in
    subtract_space.  In essence, performs an A_NOT_B operation with the
    two selections.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_subtract(H5S_t *space, H5S_t *subtract_space)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(space);
    assert(subtract_space);

    /* If either space is using the none selection, then we do not need to do
     * anything */
    if ((space->select.type->type != H5S_SEL_NONE) && (subtract_space->select.type->type != H5S_SEL_NONE)) {
        /* If subtract_space is using the all selection, set space to none */
        if (subtract_space->select.type->type == H5S_SEL_ALL) {
            /* Change to "none" selection */
            if (H5S_select_none(space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't change selection");
        } /* end if */
        /* If either selection is a point selection, fail currently */
        else if ((subtract_space->select.type->type == H5S_SEL_POINTS) ||
                 (space->select.type->type == H5S_SEL_POINTS)) {
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "point selections not currently supported");
        } /* end if */
        else {
            /* Check for all selection in space, convert to hyperslab */
            if (space->select.type->type == H5S_SEL_ALL) {
                /* Convert current "all" selection to "real" hyperslab selection */
                /* Then allow operation to proceed */
                hsize_t  tmp_start[H5S_MAX_RANK];  /* Temporary start information */
                hsize_t  tmp_stride[H5S_MAX_RANK]; /* Temporary stride information */
                hsize_t  tmp_count[H5S_MAX_RANK];  /* Temporary count information */
                hsize_t  tmp_block[H5S_MAX_RANK];  /* Temporary block information */
                unsigned u;                        /* Local index variable */

                /* Fill in temporary information for the dimensions */
                for (u = 0; u < space->extent.rank; u++) {
                    tmp_start[u]  = 0;
                    tmp_stride[u] = 1;
                    tmp_count[u]  = 1;
                    tmp_block[u]  = space->extent.size[u];
                } /* end for */

                /* Convert to hyperslab selection */
                if (H5S_select_hyperslab(space, H5S_SELECT_SET, tmp_start, tmp_stride, tmp_count, tmp_block) <
                    0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTSELECT, FAIL, "can't convert selection");
            } /* end if */

            assert(space->select.type->type == H5S_SEL_HYPERSLABS);
            assert(subtract_space->select.type->type == H5S_SEL_HYPERSLABS);

            /* Both spaces are now hyperslabs, perform the operation */
            if (H5S__modify_select(space, H5S_SELECT_NOTB, subtract_space) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCLIP, FAIL, "can't subtract hyperslab");
        } /* end else */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_subtract() */

/*--------------------------------------------------------------------------
 NAME
    H5Ssel_iter_create
 PURPOSE
    Create a dataspace selection iterator for a dataspace's selection
 USAGE
    hid_t H5Ssel_iter_create(space)
        hid_t   space;  IN: ID of the dataspace with selection to iterate over
 RETURNS
    Valid dataspace selection iterator ID on success, H5I_INVALID_HID on failure
 DESCRIPTION
    Creates a selection iterator and initializes it to start at the first
    element selected in the dataspace.
 PROGRAMMER
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5Ssel_iter_create(hid_t space_id, size_t elmt_size, unsigned flags)
{
    H5S_t          *space;     /* Dataspace with selection to iterate over */
    H5S_sel_iter_t *sel_iter;  /* Selection iterator created */
    hid_t           ret_value; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "izIu", space_id, elmt_size, flags);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, H5I_INVALID_HID, "not a dataspace");
    if (elmt_size == 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, H5I_INVALID_HID, "element size must be greater than 0");
    if (flags != (flags & H5S_SEL_ITER_ALL_PUBLIC_FLAGS))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, H5I_INVALID_HID, "invalid selection iterator flag");

    /* Allocate the iterator */
    if (NULL == (sel_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, H5I_INVALID_HID, "can't allocate selection iterator");

    /* Add flag to indicate that this iterator is from an API call */
    flags |= H5S_SEL_ITER_API_CALL;

    /* Initialize the selection iterator */
    if (H5S_select_iter_init(sel_iter, space, elmt_size, flags) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, H5I_INVALID_HID, "unable to initialize selection iterator");

    /* Register */
    if ((ret_value = H5I_register(H5I_SPACE_SEL_ITER, sel_iter, true)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTREGISTER, H5I_INVALID_HID,
                    "unable to register dataspace selection iterator ID");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ssel_iter_create() */

/*--------------------------------------------------------------------------
 NAME
    H5Ssel_iter_get_seq_list
 PURPOSE
    Retrieve a list of offset / length sequences for the elements in an iterator
 USAGE
    herr_t H5Ssel_iter_get_seq_list(sel_iter_id, maxseq, maxbytes, nseq, nbytes, off, len)
        hid_t   sel_iter_id;  IN: ID of the dataspace selection iterator to retrieve sequence from
        size_t  maxseq;       IN: Max. # of sequences to retrieve
        size_t  maxelmts;     IN: Max. # of elements to retrieve in sequences
        size_t *nseq;         OUT: # of sequences retrieved
        size_t *nelmts;       OUT: # of elements retrieved, in all sequences
        hsize_t *off;         OUT: Array of sequence offsets
        size_t *len;          OUT: Array of sequence lengths
 RETURNS
    Non-negative on success / Negative on failure
 DESCRIPTION
    Retrieve a list of offset / length pairs (a list of "sequences") matching
    the selected elements for an iterator, according to the iteration order for
    the iterator.  The lengths returned are in _bytes_, not elements.

    Note that the iteration order for "all" and "hyperslab" selections is
    row-major (i.e. "C-ordered"), but the iteration order for "point"
    selections is "in order selected", unless the H5S_SEL_ITER_GET_SEQ_LIST_SORTED
    flag is passed to H5Sset_iter_create for a point selection.

    MAXSEQ and MAXELMTS specify the most sequences or bytes possible to
    place into the OFF and LEN arrays. *NSEQ and *NELMTS return the actual
    number of sequences and bytes put into the arrays.

    Each call to H5Ssel_iter_get_seq_list() will retrieve the next set
    of sequences for the selection being iterated over.

    The total number of bytes possible to retrieve from a selection iterator
    is the 'elmt_size' passed to H5Ssel_iter_create multiplied by the number
    of elements selected in the dataspace the iterator was created from
    (which can be retrieved with H5Sget_select_npoints).  When there are no
    further sequences of elements to retrieve, calls to this routine will
    set *NSEQ and *NELMTS to zero.
 PROGRAMMER
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Ssel_iter_get_seq_list(hid_t sel_iter_id, size_t maxseq, size_t maxelmts, size_t *nseq /*out*/,
                         size_t *nelmts /*out*/, hsize_t *off /*out*/, size_t *len /*out*/)
{
    H5S_sel_iter_t *sel_iter;            /* Dataspace selection iterator to operate on */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "izzxxxx", sel_iter_id, maxseq, maxelmts, nseq, nelmts, off, len);

    /* Check args */
    if (NULL == (sel_iter = (H5S_sel_iter_t *)H5I_object_verify(sel_iter_id, H5I_SPACE_SEL_ITER)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace selection iterator");
    if (NULL == nseq)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "'nseq' pointer is NULL");
    if (NULL == nelmts)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "'nbytes' pointer is NULL");
    if (NULL == off)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "offset array pointer is NULL");
    if (NULL == len)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "length array pointer is NULL");

    /* Get the sequences of bytes */
    if (maxseq > 0 && maxelmts > 0 && sel_iter->elmt_left > 0) {
        if (H5S_SELECT_ITER_GET_SEQ_LIST(sel_iter, maxseq, maxelmts, nseq, nelmts, off, len) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed");
    } /* end if */
    else
        *nseq = *nelmts = 0;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ssel_iter_get_seq_list() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_contig_block

 PURPOSE
    Determines if a selection is a single contiguous block, and returns the
    offset and length (in elements) if it is

 USAGE
    herr_t H5S_select_contig_block(space, is_contig, off, len)
        H5S_t   *space;      IN: Selection to check
        bool *is_contig;  OUT: Whether the selection is contiguous
        hsize_t *off;        OUT: Offset of selection
        size_t  *len;        OUT: Length of selection

 RETURNS
    Non-negative on success/Negative on failure.

 DESCRIPTION
    Determines if a selection is a single contiguous block, and returns the
    offset and length (in elements) if it is.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_contig_block(H5S_t *space, bool *is_contig, hsize_t *off, size_t *len)
{
    H5S_sel_iter_t *iter      = NULL;  /* Selection iterator */
    bool            iter_init = false; /* Selection iteration info has been initialized */
    size_t          nseq_tmp;
    size_t          nelem_tmp;
    hsize_t         sel_off;
    size_t          sel_len;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(space);

    /* Allocate and initialize the iterator */
    if (NULL == (iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate iterator");
    if (H5S_select_iter_init(iter, space, 1, 0) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize memory selection information");
    iter_init = true;

    /* Get list of sequences for selection, to check if it is contiguous */
    if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, (size_t)1, (size_t)-1, &nseq_tmp, &nelem_tmp, &sel_off, &sel_len) <
        0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTNEXT, FAIL, "sequence length generation failed");

    /* If the first sequence includes all the elements selected in this piece, it it contiguous */
    H5_CHECK_OVERFLOW(space->select.num_elem, hsize_t, size_t);
    if (sel_len == (size_t)space->select.num_elem) {
        if (is_contig)
            *is_contig = true;
        if (off)
            *off = sel_off;
        if (len)
            *len = sel_len;
    }
    else if (is_contig)
        *is_contig = false;

done:
    if (iter_init && H5S_SELECT_ITER_RELEASE(iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (iter)
        iter = H5FL_FREE(H5S_sel_iter_t, iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_contig_block() */

/*--------------------------------------------------------------------------
 NAME
    H5Ssel_iter_reset
 PURPOSE
    Resets a dataspace selection iterator back to an initial state.
 USAGE
    herr_t H5Ssel_iter_reset(sel_iter_id)
        hid_t   sel_iter_id;  IN: ID of the dataspace selection iterator to
                                  reset
        hid_t   space_id;     IN: ID of the dataspace with selection to
                                  iterate over
 RETURNS
    Non-negative on success / Negative on failure
 DESCRIPTION
    Resets a dataspace selection iterator back to an initial state so that
    the iterator may be used for iteration once again.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Ssel_iter_reset(hid_t sel_iter_id, hid_t space_id)
{
    H5S_sel_iter_t *sel_iter;
    H5S_t          *space;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ii", sel_iter_id, space_id);

    /* Check args */
    if (NULL == (sel_iter = (H5S_sel_iter_t *)H5I_object_verify(sel_iter_id, H5I_SPACE_SEL_ITER)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace selection iterator");
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Call selection type-specific release routine */
    if (H5S_SELECT_ITER_RELEASE(sel_iter) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL,
                    "problem releasing a selection iterator's type-specific info");

    /* Simply re-initialize iterator */
    if (H5S_select_iter_init(sel_iter, space, sel_iter->elmt_size, sel_iter->flags) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to re-initialize selection iterator");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ssel_iter_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5S__sel_iter_close_cb
 *
 * Purpose:     Called when the ref count reaches zero on a selection iterator's ID
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S__sel_iter_close_cb(H5S_sel_iter_t *_sel_iter, void H5_ATTR_UNUSED **request)
{
    H5S_sel_iter_t *sel_iter  = (H5S_sel_iter_t *)_sel_iter; /* The selection iterator to close */
    herr_t          ret_value = SUCCEED;                     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(sel_iter);

    /* Close the selection iterator object */
    if (H5S_sel_iter_close(sel_iter) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CLOSEERROR, FAIL, "unable to close selection iterator");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__sel_iter_close_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5S_sel_iter_close
 *
 * Purpose:	Releases a dataspace selection iterator and its memory.
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S_sel_iter_close(H5S_sel_iter_t *sel_iter)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(sel_iter);

    /* Call selection type-specific release routine */
    if (H5S_SELECT_ITER_RELEASE(sel_iter) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL,
                    "problem releasing a selection iterator's type-specific info");

    /* Release the structure */
    sel_iter = H5FL_FREE(H5S_sel_iter_t, sel_iter);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_sel_iter_close() */

/*--------------------------------------------------------------------------
 NAME
    H5Ssel_iter_close
 PURPOSE
    Close a dataspace selection iterator
 USAGE
    herr_t H5Ssel_iter_close(sel_iter_id)
        hid_t   sel_iter_id;  IN: ID of the dataspace selection iterator to close
 RETURNS
    Non-negative on success / Negative on failure
 DESCRIPTION
    Close a dataspace selection iterator, releasing its state.
 PROGRAMMER
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Ssel_iter_close(hid_t sel_iter_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", sel_iter_id);

    /* Check args */
    if (NULL == H5I_object_verify(sel_iter_id, H5I_SPACE_SEL_ITER))
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "not a dataspace selection iterator");

    /* When the reference count reaches zero the resources are freed */
    if (H5I_dec_app_ref(sel_iter_id) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDEC, FAIL, "problem freeing dataspace selection iterator ID");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ssel_iter_close() */
