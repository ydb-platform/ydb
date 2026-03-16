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
 * Purpose:     Point selection dataspace I/O functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Smodule.h" /* This source code file is part of the H5S module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5Iprivate.h"  /* ID Functions                             */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Spkg.h"      /* Dataspace functions                      */
#include "H5VMprivate.h" /* Vector functions                         */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Define alias for hsize_t, for allocating H5S_pnt_node_t + point objects */
/* (Makes it easier to understand the alloc / free calls) */
typedef hsize_t hcoords_t;

/********************/
/* Local Prototypes */
/********************/
static herr_t          H5S__point_add(H5S_t *space, H5S_seloper_t op, size_t num_elem, const hsize_t *coord);
static H5S_pnt_list_t *H5S__copy_pnt_list(const H5S_pnt_list_t *src, unsigned rank);
static void            H5S__free_pnt_list(H5S_pnt_list_t *pnt_lst);

/* Selection callbacks */
static herr_t   H5S__point_copy(H5S_t *dst, const H5S_t *src, bool share_selection);
static herr_t   H5S__point_release(H5S_t *space);
static htri_t   H5S__point_is_valid(const H5S_t *space);
static hssize_t H5S__point_serial_size(H5S_t *space);
static herr_t   H5S__point_serialize(H5S_t *space, uint8_t **p);
static herr_t   H5S__point_deserialize(H5S_t **space, const uint8_t **p, const size_t p_size, bool skip);
static herr_t   H5S__point_bounds(const H5S_t *space, hsize_t *start, hsize_t *end);
static herr_t   H5S__point_offset(const H5S_t *space, hsize_t *off);
static int      H5S__point_unlim_dim(const H5S_t *space);
static htri_t   H5S__point_is_contiguous(const H5S_t *space);
static htri_t   H5S__point_is_single(const H5S_t *space);
static htri_t   H5S__point_is_regular(H5S_t *space);
static htri_t   H5S__point_shape_same(H5S_t *space1, H5S_t *space2);
static htri_t   H5S__point_intersect_block(H5S_t *space, const hsize_t *start, const hsize_t *end);
static herr_t   H5S__point_adjust_u(H5S_t *space, const hsize_t *offset);
static herr_t   H5S__point_adjust_s(H5S_t *space, const hssize_t *offset);
static herr_t   H5S__point_project_scalar(const H5S_t *space, hsize_t *offset);
static herr_t   H5S__point_project_simple(const H5S_t *space, H5S_t *new_space, hsize_t *offset);
static herr_t   H5S__point_iter_init(H5S_t *space, H5S_sel_iter_t *iter);
static herr_t   H5S__point_get_version_enc_size(const H5S_t *space, uint32_t *version, uint8_t *enc_size);

/* Selection iteration callbacks */
static herr_t  H5S__point_iter_coords(const H5S_sel_iter_t *iter, hsize_t *coords);
static herr_t  H5S__point_iter_block(const H5S_sel_iter_t *iter, hsize_t *start, hsize_t *end);
static hsize_t H5S__point_iter_nelmts(const H5S_sel_iter_t *iter);
static htri_t  H5S__point_iter_has_next_block(const H5S_sel_iter_t *iter);
static herr_t  H5S__point_iter_next(H5S_sel_iter_t *sel_iter, size_t nelem);
static herr_t  H5S__point_iter_next_block(H5S_sel_iter_t *sel_iter);
static herr_t H5S__point_iter_get_seq_list(H5S_sel_iter_t *iter, size_t maxseq, size_t maxbytes, size_t *nseq,
                                           size_t *nbytes, hsize_t *off, size_t *len);
static herr_t H5S__point_iter_release(H5S_sel_iter_t *sel_iter);

/*****************************/
/* Library Private Variables */
/*****************************/

/*********************/
/* Package Variables */
/*********************/

/* Selection properties for point selections */
const H5S_select_class_t H5S_sel_point[1] = {{
    H5S_SEL_POINTS,

    /* Methods on selection */
    H5S__point_copy,
    H5S__point_release,
    H5S__point_is_valid,
    H5S__point_serial_size,
    H5S__point_serialize,
    H5S__point_deserialize,
    H5S__point_bounds,
    H5S__point_offset,
    H5S__point_unlim_dim,
    NULL,
    H5S__point_is_contiguous,
    H5S__point_is_single,
    H5S__point_is_regular,
    H5S__point_shape_same,
    H5S__point_intersect_block,
    H5S__point_adjust_u,
    H5S__point_adjust_s,
    H5S__point_project_scalar,
    H5S__point_project_simple,
    H5S__point_iter_init,
}};

/* Format version bounds for dataspace point selection */
const unsigned H5O_sds_point_ver_bounds[] = {
    H5S_POINT_VERSION_1, /* H5F_LIBVER_EARLIEST */
    H5S_POINT_VERSION_1, /* H5F_LIBVER_V18 */
    H5S_POINT_VERSION_1, /* H5F_LIBVER_V110 */
    H5S_POINT_VERSION_2, /* H5F_LIBVER_V112 */
    H5S_POINT_VERSION_2  /* H5F_LIBVER_LATEST */
};

/*******************/
/* Local Variables */
/*******************/

/* Iteration properties for point selections */
static const H5S_sel_iter_class_t H5S_sel_iter_point[1] = {{
    H5S_SEL_POINTS,

    /* Methods on selection iterator */
    H5S__point_iter_coords,
    H5S__point_iter_block,
    H5S__point_iter_nelmts,
    H5S__point_iter_has_next_block,
    H5S__point_iter_next,
    H5S__point_iter_next_block,
    H5S__point_iter_get_seq_list,
    H5S__point_iter_release,
}};

/* Declare a free list to manage the H5S_pnt_node_t + hcoords_t array struct */
H5FL_BARR_DEFINE_STATIC(H5S_pnt_node_t, hcoords_t, H5S_MAX_RANK);

/* Declare a free list to manage the H5S_pnt_list_t struct */
H5FL_DEFINE_STATIC(H5S_pnt_list_t);

/*-------------------------------------------------------------------------
 * Function:    H5S__point_iter_init
 *
 * Purpose:     Initializes iteration information for point selection.
 *
 * Return:      Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__point_iter_init(H5S_t *space, H5S_sel_iter_t *iter)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space && H5S_SEL_POINTS == H5S_GET_SELECT_TYPE(space));
    assert(iter);

    /* If this iterator is created from an API call, by default we clone the
     *  selection now, as the dataspace could be modified or go out of scope.
     *
     *  However, if the H5S_SEL_ITER_SHARE_WITH_DATASPACE flag is given,
     *  the selection is shared between the selection iterator and the
     *  dataspace.  In this case, the application _must_not_ modify or
     *  close the dataspace that the iterator is operating on, or undefined
     *  behavior will occur.
     */
    if ((iter->flags & H5S_SEL_ITER_API_CALL) && !(iter->flags & H5S_SEL_ITER_SHARE_WITH_DATASPACE)) {
        /* Copy the point list */
        if (NULL ==
            (iter->u.pnt.pnt_lst = H5S__copy_pnt_list(space->select.sel_info.pnt_lst, space->extent.rank)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy point list");
    } /* end if */
    else
        /* OK to share point list for internal iterations */
        iter->u.pnt.pnt_lst = space->select.sel_info.pnt_lst;

    /* Start at the head of the list of points */
    iter->u.pnt.curr = iter->u.pnt.pnt_lst->head;

    /* Initialize type of selection iterator */
    iter->type = H5S_sel_iter_point;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_iter_init() */

/*-------------------------------------------------------------------------
 * Function:    H5S__point_iter_coords
 *
 * Purpose:     Retrieve the current coordinates of iterator for current
 *              selection
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__point_iter_coords(const H5S_sel_iter_t *iter, hsize_t *coords)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(coords);

    /* Copy the offset of the current point */
    H5MM_memcpy(coords, iter->u.pnt.curr->pnt, sizeof(hsize_t) * iter->rank);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_iter_coords() */

/*-------------------------------------------------------------------------
 * Function:    H5S__point_iter_block
 *
 * Purpose:     Retrieve the current block of iterator for current
 *              selection
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__point_iter_block(const H5S_sel_iter_t *iter, hsize_t *start, hsize_t *end)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(start);
    assert(end);

    /* Copy the current point as a block */
    H5MM_memcpy(start, iter->u.pnt.curr->pnt, sizeof(hsize_t) * iter->rank);
    H5MM_memcpy(end, iter->u.pnt.curr->pnt, sizeof(hsize_t) * iter->rank);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_iter_block() */

/*-------------------------------------------------------------------------
 * Function:    H5S__point_iter_nelmts
 *
 * Purpose:     Return number of elements left to process in iterator
 *
 * Return:      Non-negative number of elements on success, zero on failure
 *
 *-------------------------------------------------------------------------
 */
static hsize_t
H5S__point_iter_nelmts(const H5S_sel_iter_t *iter)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);

    FUNC_LEAVE_NOAPI(iter->elmt_left)
} /* end H5S__point_iter_nelmts() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_iter_has_next_block
 PURPOSE
    Check if there is another block left in the current iterator
 USAGE
    htri_t H5S__point_iter_has_next_block(iter)
        const H5S_sel_iter_t *iter;       IN: Pointer to selection iterator
 RETURNS
    Non-negative (true/false) on success/Negative on failure
 DESCRIPTION
    Check if there is another block available in the selection iterator.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__point_iter_has_next_block(const H5S_sel_iter_t *iter)
{
    htri_t ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);

    /* Check if there is another point in the list */
    if (iter->u.pnt.curr->next == NULL)
        HGOTO_DONE(false);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_iter_has_next_block() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_iter_next
 PURPOSE
    Increment selection iterator
 USAGE
    herr_t H5S__point_iter_next(iter, nelem)
        H5S_sel_iter_t *iter;       IN: Pointer to selection iterator
        size_t nelem;               IN: Number of elements to advance by
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Advance selection iterator to the NELEM'th next element in the selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_iter_next(H5S_sel_iter_t *iter, size_t nelem)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(nelem > 0);

    /* Increment the iterator */
    while (nelem > 0) {
        iter->u.pnt.curr = iter->u.pnt.curr->next;
        nelem--;
    } /* end while */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_iter_next() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_iter_next_block
 PURPOSE
    Increment selection iterator to next block
 USAGE
    herr_t H5S__point_iter_next_block(iter)
        H5S_sel_iter_t *iter;       IN: Pointer to selection iterator
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Advance selection iterator to the next block in the selection.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_iter_next_block(H5S_sel_iter_t *iter)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);

    /* Increment the iterator */
    iter->u.pnt.curr = iter->u.pnt.curr->next;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_iter_next_block() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_iter_get_seq_list
 PURPOSE
    Create a list of offsets & lengths for a selection
 USAGE
    herr_t H5S__point_iter_get_seq_list(iter,maxseq,maxelem,nseq,nelem,off,len)
        H5S_sel_iter_t *iter;   IN/OUT: Selection iterator describing last
                                    position of interest in selection.
        size_t maxseq;          IN: Maximum number of sequences to generate
        size_t maxelem;         IN: Maximum number of elements to include in the
                                    generated sequences
        size_t *nseq;           OUT: Actual number of sequences generated
        size_t *nelem;          OUT: Actual number of elements in sequences generated
        hsize_t *off;           OUT: Array of offsets (in bytes)
        size_t *len;            OUT: Array of lengths (in bytes)
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Use the selection in the dataspace to generate a list of byte offsets and
    lengths for the region(s) selected.  Start/Restart from the position in the
    ITER parameter.  The number of sequences generated is limited by the MAXSEQ
    parameter and the number of sequences actually generated is stored in the
    NSEQ parameter.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_iter_get_seq_list(H5S_sel_iter_t *iter, size_t maxseq, size_t maxelem, size_t *nseq, size_t *nelem,
                             hsize_t *off, size_t *len)
{
    size_t          io_left;             /* The number of bytes left in the selection */
    size_t          start_io_left;       /* The initial number of bytes left in the selection */
    H5S_pnt_node_t *node;                /* Point node */
    unsigned        ndims;               /* Dimensionality of dataspace*/
    hsize_t         acc;                 /* Coordinate accumulator */
    hsize_t         loc;                 /* Coordinate offset */
    size_t          curr_seq;            /* Current sequence being operated on */
    int             i;                   /* Local index variable */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);
    assert(maxseq > 0);
    assert(maxelem > 0);
    assert(nseq);
    assert(nelem);
    assert(off);
    assert(len);

    /* Choose the minimum number of bytes to sequence through */
    H5_CHECK_OVERFLOW(iter->elmt_left, hsize_t, size_t);
    start_io_left = io_left = (size_t)MIN(iter->elmt_left, maxelem);

    /* Get the dataspace's rank */
    ndims = iter->rank;

    /* Walk through the points in the selection, starting at the current */
    /*  location in the iterator */
    node     = iter->u.pnt.curr;
    curr_seq = 0;
    while (NULL != node) {
        /* Compute the offset of each selected point in the buffer */
        for (i = (int)(ndims - 1), acc = iter->elmt_size, loc = 0; i >= 0; i--) {
            loc += (hsize_t)((hssize_t)node->pnt[i] + iter->sel_off[i]) * acc;
            acc *= iter->dims[i];
        } /* end for */

        /* Check if this is a later point in the selection */
        if (curr_seq > 0) {
            /* If a sorted sequence is requested, make certain we don't go backwards in the offset */
            if ((iter->flags & H5S_SEL_ITER_GET_SEQ_LIST_SORTED) && loc < off[curr_seq - 1])
                break;

            /* Check if this point extends the previous sequence */
            /* (Unlikely, but possible) */
            if (loc == (off[curr_seq - 1] + len[curr_seq - 1])) {
                /* Extend the previous sequence */
                len[curr_seq - 1] += iter->elmt_size;
            } /* end if */
            else {
                /* Add a new sequence */
                off[curr_seq] = loc;
                len[curr_seq] = iter->elmt_size;

                /* Increment sequence count */
                curr_seq++;
            } /* end else */
        }     /* end if */
        else {
            /* Add a new sequence */
            off[curr_seq] = loc;
            len[curr_seq] = iter->elmt_size;

            /* Increment sequence count */
            curr_seq++;
        } /* end else */

        /* Decrement number of elements left to process */
        io_left--;

        /* Move the iterator */
        iter->u.pnt.curr = node->next;
        iter->elmt_left--;

        /* Check if we're finished with all sequences */
        if (curr_seq == maxseq)
            break;

        /* Check if we're finished with all the elements available */
        if (io_left == 0)
            break;

        /* Advance to the next point */
        node = node->next;
    } /* end while */

    /* Set the number of sequences generated */
    *nseq = curr_seq;

    /* Set the number of elements used */
    *nelem = start_io_left - io_left;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_iter_get_seq_list() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_iter_release
 PURPOSE
    Release point selection iterator information for a dataspace
 USAGE
    herr_t H5S__point_iter_release(iter)
        H5S_sel_iter_t *iter;       IN: Pointer to selection iterator
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Releases all information for a dataspace point selection iterator
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_iter_release(H5S_sel_iter_t *iter)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(iter);

    /* If this iterator copied the point list, we must free it */
    if ((iter->flags & H5S_SEL_ITER_API_CALL) && !(iter->flags & H5S_SEL_ITER_SHARE_WITH_DATASPACE))
        H5S__free_pnt_list(iter->u.pnt.pnt_lst);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_iter_release() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_add
 PURPOSE
    Add a series of elements to a point selection
 USAGE
    herr_t H5S__point_add(space, num_elem, coord)
        H5S_t *space;           IN: Dataspace of selection to modify
        size_t num_elem;        IN: Number of elements in COORD array.
        const hsize_t *coord[];    IN: The location of each element selected
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    This function adds elements to the current point selection for a dataspace
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_add(H5S_t *space, H5S_seloper_t op, size_t num_elem, const hsize_t *coord)
{
    H5S_pnt_node_t *top = NULL, *curr = NULL, *new_node = NULL; /* Point selection nodes */
    unsigned        u;                                          /* Counter */
    herr_t          ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(space);
    assert(num_elem > 0);
    assert(coord);
    assert(op == H5S_SELECT_SET || op == H5S_SELECT_APPEND || op == H5S_SELECT_PREPEND);

    for (u = 0; u < num_elem; u++) {
        unsigned dim; /* Counter for dimensions */

        /* Allocate space for the new node */
        if (NULL == (new_node = (H5S_pnt_node_t *)H5FL_ARR_MALLOC(hcoords_t, space->extent.rank)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate point node");

        /* Initialize fields in node */
        new_node->next = NULL;

        /* Copy over the coordinates */
        H5MM_memcpy(new_node->pnt, coord + (u * space->extent.rank), (space->extent.rank * sizeof(hsize_t)));

        /* Link into list */
        if (top == NULL)
            top = new_node;
        else
            curr->next = new_node;
        curr = new_node;

        /* Update bound box */
        /* (Note: when op is H5S_SELECT_SET, the bound box has been reset
         *      inside H5S_select_elements, the only caller of this function.
         *      So the following bound box update procedure works correctly
         *      for the SET operation)
         */
        for (dim = 0; dim < space->extent.rank; dim++) {
            space->select.sel_info.pnt_lst->low_bounds[dim] =
                MIN(space->select.sel_info.pnt_lst->low_bounds[dim], curr->pnt[dim]);
            space->select.sel_info.pnt_lst->high_bounds[dim] =
                MAX(space->select.sel_info.pnt_lst->high_bounds[dim], curr->pnt[dim]);
        } /* end for */
    }     /* end for */
    new_node = NULL;

    /* Insert the list of points selected in the proper place */
    if (op == H5S_SELECT_SET || op == H5S_SELECT_PREPEND) {
        /* Append current list, if there is one */
        if (NULL != space->select.sel_info.pnt_lst->head)
            curr->next = space->select.sel_info.pnt_lst->head;

        /* Put new list in point selection */
        space->select.sel_info.pnt_lst->head = top;

        /* Change the tail pointer if tail has not been set */
        if (NULL == space->select.sel_info.pnt_lst->tail)
            space->select.sel_info.pnt_lst->tail = curr;
    }                             /* end if */
    else {                        /* op==H5S_SELECT_APPEND */
        H5S_pnt_node_t *tmp_node; /* Temporary point selection node */

        tmp_node = space->select.sel_info.pnt_lst->head;
        if (tmp_node != NULL) {
            assert(space->select.sel_info.pnt_lst->tail);
            space->select.sel_info.pnt_lst->tail->next = top;
        } /* end if */
        else
            space->select.sel_info.pnt_lst->head = top;
        space->select.sel_info.pnt_lst->tail = curr;
    } /* end else */

    /* Set the number of elements in the new selection */
    if (op == H5S_SELECT_SET)
        space->select.num_elem = num_elem;
    else
        space->select.num_elem += num_elem;

done:
    if (ret_value < 0) {
        /* Release possibly partially initialized new node */
        if (new_node)
            new_node = (H5S_pnt_node_t *)H5FL_ARR_FREE(hcoords_t, new_node);

        /* Release possible linked list of nodes */
        while (top) {
            curr = top->next;
            top  = (H5S_pnt_node_t *)H5FL_ARR_FREE(hcoords_t, top);
            top  = curr;
        } /* end while */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_add() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_release
 PURPOSE
    Release point selection information for a dataspace
 USAGE
    herr_t H5S__point_release(space)
        H5S_t *space;       IN: Pointer to dataspace
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Releases all point selection information for a dataspace
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_release(H5S_t *space)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(space);

    /* Free the point list */
    H5S__free_pnt_list(space->select.sel_info.pnt_lst);

    /* Reset the point list header */
    space->select.sel_info.pnt_lst = NULL;

    /* Reset the number of elements in the selection */
    space->select.num_elem = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_release() */

/*--------------------------------------------------------------------------
 NAME
    H5S_select_elements
 PURPOSE
    Specify a series of elements in the dataspace to select
 USAGE
    herr_t H5S_select_elements(dsid, op, num_elem, coord)
        hid_t dsid;             IN: Dataspace ID of selection to modify
        H5S_seloper_t op;       IN: Operation to perform on current selection
        size_t num_elem;        IN: Number of elements in COORD array.
        const hsize_t *coord;   IN: The location of each element selected
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    This function selects array elements to be included in the selection for
    the dataspace.  The COORD array is a 2-D array of size <dataspace rank>
    by NUM_ELEM (ie. a list of coordinates in the dataspace).  The order of
    the element coordinates in the COORD array specifies the order that the
    array elements are iterated through when I/O is performed.  Duplicate
    coordinates are not checked for.  The selection operator, OP, determines
    how the new selection is to be combined with the existing selection for
    the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5S_select_elements(H5S_t *space, H5S_seloper_t op, size_t num_elem, const hsize_t *coord)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(space);
    assert(num_elem);
    assert(coord);
    assert(op == H5S_SELECT_SET || op == H5S_SELECT_APPEND || op == H5S_SELECT_PREPEND);

    /* If we are setting a new selection, remove current selection first */
    if (op == H5S_SELECT_SET || H5S_GET_SELECT_TYPE(space) != H5S_SEL_POINTS)
        if (H5S_SELECT_RELEASE(space) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't release point selection");

    /* Allocate space for the point selection information if necessary */
    if (H5S_GET_SELECT_TYPE(space) != H5S_SEL_POINTS || space->select.sel_info.pnt_lst == NULL) {
        hsize_t tmp = HSIZET_MAX;

        if (NULL == (space->select.sel_info.pnt_lst = H5FL_CALLOC(H5S_pnt_list_t)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "can't allocate element information");

        /* Set the bound box to the default value */
        H5VM_array_fill(space->select.sel_info.pnt_lst->low_bounds, &tmp, sizeof(hsize_t),
                        space->extent.rank);
        memset(space->select.sel_info.pnt_lst->high_bounds, 0, sizeof(hsize_t) * space->extent.rank);
    }

    /* Add points to selection */
    if (H5S__point_add(space, op, num_elem, coord) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINSERT, FAIL, "can't insert elements");

    /* Set selection type */
    space->select.type = H5S_sel_point;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_select_elements() */

/*--------------------------------------------------------------------------
 NAME
    H5S__copy_pnt_list
 PURPOSE
    Copy a point selection list
 USAGE
    H5S_pnt_list_t *H5S__copy_pnt_list(src)
        const H5S_pnt_list_t *src;      IN: Pointer to the source point list
        unsigned rank;                  IN: # of dimensions for points
 RETURNS
    Non-NULL pointer to new point list on success / NULL on failure
 DESCRIPTION
    Copies point selection information from the source point list to newly
    created point list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5S_pnt_list_t *
H5S__copy_pnt_list(const H5S_pnt_list_t *src, unsigned rank)
{
    H5S_pnt_list_t *dst = NULL;       /* New point list */
    H5S_pnt_node_t *curr, *new_tail;  /* Point information nodes */
    H5S_pnt_list_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(src);
    assert(rank > 0);

    /* Allocate room for the head of the point list */
    if (NULL == (dst = H5FL_MALLOC(H5S_pnt_list_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate point list node");

    curr     = src->head;
    new_tail = NULL;
    while (curr) {
        H5S_pnt_node_t *new_node; /* New point information node */

        /* Create new point */
        if (NULL == (new_node = (H5S_pnt_node_t *)H5FL_ARR_MALLOC(hcoords_t, rank)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, NULL, "can't allocate point node");
        new_node->next = NULL;

        /* Copy over the point's coordinates */
        H5MM_memcpy(new_node->pnt, curr->pnt, (rank * sizeof(hsize_t)));

        /* Keep the order the same when copying */
        if (NULL == new_tail)
            new_tail = dst->head = new_node;
        else {
            new_tail->next = new_node;
            new_tail       = new_node;
        } /* end else */

        curr = curr->next;
    } /* end while */
    dst->tail = new_tail;

    /* Copy the selection bounds */
    H5MM_memcpy(dst->high_bounds, src->high_bounds, (rank * sizeof(hsize_t)));
    H5MM_memcpy(dst->low_bounds, src->low_bounds, (rank * sizeof(hsize_t)));

    /* Clear cached iteration point */
    dst->last_idx     = 0;
    dst->last_idx_pnt = NULL;

    /* Set return value */
    ret_value = dst;

done:
    if (NULL == ret_value && dst)
        H5S__free_pnt_list(dst);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__copy_pnt_list() */

/*--------------------------------------------------------------------------
 NAME
    H5S__free_pnt_list
 PURPOSE
    Free a point selection list
 USAGE
    void H5S__free_pnt_list(pnt_lst)
        H5S_pnt_list_t *pnt_lst;        IN: Pointer to the point list to free
 RETURNS
    None
 DESCRIPTION
    Frees point selection information from the point list
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static void
H5S__free_pnt_list(H5S_pnt_list_t *pnt_lst)
{
    H5S_pnt_node_t *curr; /* Point information nodes */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pnt_lst);

    /* Traverse the list, freeing all memory */
    curr = pnt_lst->head;
    while (curr) {
        H5S_pnt_node_t *tmp_node = curr;

        curr     = curr->next;
        tmp_node = (H5S_pnt_node_t *)H5FL_ARR_FREE(hcoords_t, tmp_node);
    } /* end while */

    H5FL_FREE(H5S_pnt_list_t, pnt_lst);

    FUNC_LEAVE_NOAPI_VOID
} /* end H5S__free_pnt_list() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_copy
 PURPOSE
    Copy a selection from one dataspace to another
 USAGE
    herr_t H5S__point_copy(dst, src, share_selection)
        H5S_t *dst;             OUT: Pointer to the destination dataspace
        H5S_t *src;             IN: Pointer to the source dataspace
        bool share_selection; IN: Whether to share the selection between the dataspaces
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Copies all the point selection information from the source
    dataspace to the destination dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_copy(H5S_t *dst, const H5S_t *src, bool H5_ATTR_UNUSED share_selection)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(src);
    assert(dst);

    /* Allocate room for the head of the point list */
    if (NULL ==
        (dst->select.sel_info.pnt_lst = H5S__copy_pnt_list(src->select.sel_info.pnt_lst, src->extent.rank)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOPY, FAIL, "can't copy point list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_copy() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_is_valid
 PURPOSE
    Check whether the selection fits within the extent, with the current
    offset defined.
 USAGE
    htri_t H5S__point_is_valid(space);
        const H5S_t *space;             IN: Dataspace pointer to query
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
static htri_t
H5S__point_is_valid(const H5S_t *space)
{
    unsigned u;                /* Counter */
    htri_t   ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);

    /* Check each dimension */
    for (u = 0; u < space->extent.rank; u++) {
        /* Bounds check the selected point + offset against the extent */
        if ((space->select.sel_info.pnt_lst->high_bounds[u] + (hsize_t)space->select.offset[u]) >
            space->extent.size[u])
            HGOTO_DONE(false);
        if (((hssize_t)space->select.sel_info.pnt_lst->low_bounds[u] + space->select.offset[u]) < 0)
            HGOTO_DONE(false);
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_is_valid() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_select_elem_npoints
 PURPOSE
    Get the number of points in current element selection
 USAGE
    hssize_t H5Sget_select_elem_npoints(dsid)
        hid_t dsid;             IN: Dataspace ID of selection to query
 RETURNS
    The number of element points in selection on success, negative on failure
 DESCRIPTION
    Returns the number of element points in current selection for dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hssize_t
H5Sget_select_elem_npoints(hid_t spaceid)
{
    H5S_t   *space;     /* Dataspace to modify selection of */
    hssize_t ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("Hs", "i", spaceid);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_GET_SELECT_TYPE(space) != H5S_SEL_POINTS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an element selection");

    ret_value = (hssize_t)H5S_GET_SELECT_NPOINTS(space);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sget_select_elem_npoints() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_get_version_enc_size
 PURPOSE
    Determine the version and the size (2, 4 or 8 bytes) to encode point selection info
 USAGE
    hssize_t H5S__point_get_version_enc_size(space, version, enc_size)
        const H5S_t *space:         IN: Dataspace ID of selection to query
        uint32_t *version:          OUT: The version to use for encoding
        uint8_t *enc_size:          OUT: The size to use for encoding
 RETURNS
    The version and the size to encode point selection info
 DESCRIPTION
    Determine the version to use for encoding points selection info based
    on the following:
    (1) the low/high bounds setting in fapl
    (2) whether the number of points or selection high bounds exceeds H5S_UINT32_MAX or not

    Determine the encoded size based on version:
    --For version 2, the encoded size of point selection info is determined
      by the maximum size for:
        (a) storing the number of points
        (b) storing the selection high bounds

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_get_version_enc_size(const H5S_t *space, uint32_t *version, uint8_t *enc_size)
{
    bool         count_up_version = false;   /* Whether number of points exceed H5S_UINT32_MAX */
    bool         bound_up_version = false;   /* Whether high bounds exceed H5S_UINT32_MAX */
    H5F_libver_t low_bound;                  /* The 'low' bound of library format versions */
    H5F_libver_t high_bound;                 /* The 'high' bound of library format versions */
    uint32_t     tmp_version;                /* Local temporary version */
    hsize_t      bounds_start[H5S_MAX_RANK]; /* Starting coordinate of bounding box */
    hsize_t      bounds_end[H5S_MAX_RANK];   /* Opposite coordinate of bounding box */
    hsize_t      max_size = 0;               /* Maximum selection size */
    unsigned     u;                          /* Local index variable */
    herr_t       ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get bounding box for the selection */
    memset(bounds_end, 0, sizeof(bounds_end));
    if (H5S__point_bounds(space, bounds_start, bounds_end) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't get selection bounds");

    /* Determine whether number of points or high bounds exceeds (2^32 - 1) */
    if (space->select.num_elem > H5S_UINT32_MAX)
        count_up_version = true;
    else
        for (u = 0; u < space->extent.rank; u++)
            if (bounds_end[u] > H5S_UINT32_MAX) {
                bound_up_version = true;
                break;
            } /* end if */

    /* If exceed (2^32 -1) */
    if (count_up_version || bound_up_version)
        tmp_version = H5S_POINT_VERSION_2;
    else
        tmp_version = H5S_POINT_VERSION_1;

    /* Get the file's low/high bounds */
    if (H5CX_get_libver_bounds(&low_bound, &high_bound) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get low/high bounds from API context");

    /* Upgrade to the version indicated by the file's low bound if higher */
    tmp_version = MAX(tmp_version, H5O_sds_point_ver_bounds[low_bound]);

    /* Version bounds check */
    if (tmp_version > H5O_sds_point_ver_bounds[high_bound]) {
        if (count_up_version)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                        "The number of points in point selection exceeds 2^32");
        else if (bound_up_version)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL,
                        "The end of bounding box in point selection exceeds 2^32");
        else
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "Dataspace point selection version out of bounds");
    } /* end if */

    /* Set the version to return */
    *version = tmp_version;

    /* Get the encoded size use based on version */
    switch (tmp_version) {
        case H5S_POINT_VERSION_1:
            *enc_size = H5S_SELECT_INFO_ENC_SIZE_4;
            break;

        case H5S_POINT_VERSION_2:
            /* Find max for num_elem and bounds_end[] */
            max_size = space->select.num_elem;
            for (u = 0; u < space->extent.rank; u++)
                if (bounds_end[u] > max_size)
                    max_size = bounds_end[u];

            /* Determine the encoding size */
            if (max_size > H5S_UINT32_MAX)
                *enc_size = H5S_SELECT_INFO_ENC_SIZE_8;
            else if (max_size > H5S_UINT16_MAX)
                *enc_size = H5S_SELECT_INFO_ENC_SIZE_4;
            else
                *enc_size = H5S_SELECT_INFO_ENC_SIZE_2;
            break;

        default:
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown point info size");
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__point_get_version_enc_size() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_serial_size
 PURPOSE
    Determine the number of bytes needed to store the serialized point selection
    information.
 USAGE
    hssize_t H5S__point_serial_size(space)
        const H5S_t *space;     IN: Dataspace pointer to query
 RETURNS
    The number of bytes required on success, negative on an error.
 DESCRIPTION
    Determines the number of bytes required to serialize the current point
    selection information for storage on disk.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static hssize_t
H5S__point_serial_size(H5S_t *space)
{
    uint32_t version;        /* Version number */
    uint8_t  enc_size;       /* Encoded size of point selection info */
    hssize_t ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(space);

    /* Determine the version and encoded size for point selection */
    if (H5S__point_get_version_enc_size(space, &version, &enc_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't determine version and enc_size");

    /* Basic number of bytes required to serialize point selection: */
    if (version >= H5S_POINT_VERSION_2)
        /*
         *  <type (4 bytes)> + <version (4 bytes)> +
         *  <size of point info (1 byte)> + rank (4 bytes)>
         */
        ret_value = 13;
    else
        /*
         *  <type (4 bytes)> + <version (4 bytes)> + <padding (4 bytes)> +
         *  <length (4 bytes)> + <rank (4 bytes)>
         */
        ret_value = 20;

    /* <num points (depend on enc_size)> */
    ret_value += enc_size;

    /* Count points in selection */
    ret_value += (hssize_t)(enc_size * space->extent.rank * space->select.num_elem);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_serial_size() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_serialize
 PURPOSE
    Serialize the current selection into a user-provided buffer.
 USAGE
    herr_t H5S__point_serialize(space, p)
        H5S_t *space;           IN: Dataspace with selection to serialize
        uint8_t **p;            OUT: Pointer to buffer to put serialized
                                selection.  Will be advanced to end of
                                serialized selection.
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Serializes the current element selection into a buffer.  (Primarily for
    storing on disk).
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_serialize(H5S_t *space, uint8_t **p)
{
    H5S_pnt_node_t *curr;                /* Point information nodes */
    uint8_t        *pp;                  /* Local pointer for encoding */
    uint8_t        *lenp = NULL;         /* pointer to length location for later storage */
    uint32_t        len  = 0;            /* number of bytes used */
    unsigned        u;                   /* local counting variable */
    uint32_t        version;             /* Version number */
    uint8_t         enc_size;            /* Encoded size of point selection info */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);
    assert(p);
    pp = (*p);
    assert(pp);

    /* Determine the version and encoded size for point selection info */
    if (H5S__point_get_version_enc_size(space, &version, &enc_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't determine version and enc_size");

    /* Store the preamble information */
    UINT32ENCODE(pp, (uint32_t)H5S_GET_SELECT_TYPE(space)); /* Store the type of selection */

    UINT32ENCODE(pp, version); /* Store the version number */
    if (version >= 2) {
        *(pp)++ = enc_size; /* Store size of point info */
    }
    else {
        assert(version == H5S_POINT_VERSION_1);
        UINT32ENCODE(pp, (uint32_t)0); /* Store the un-used padding */
        lenp = pp;                     /* Keep the pointer to the length location for later */
        pp += 4;                       /* Skip over space for length */
        len += 8;                      /* Add in advance # of bytes for num of dimensions and num elements  */
    }

    /* Encode number of dimensions */
    UINT32ENCODE(pp, (uint32_t)space->extent.rank);

    switch (enc_size) {
        case H5S_SELECT_INFO_ENC_SIZE_2:
            assert(version == H5S_POINT_VERSION_2);

            /* Encode number of elements */
            UINT16ENCODE(pp, (uint16_t)space->select.num_elem);

            /* Encode each point in selection */
            curr = space->select.sel_info.pnt_lst->head;
            while (curr != NULL) {
                /* Encode each point */
                for (u = 0; u < space->extent.rank; u++)
                    UINT16ENCODE(pp, (uint16_t)curr->pnt[u]);
                curr = curr->next;
            } /* end while */
            break;

        case H5S_SELECT_INFO_ENC_SIZE_4:
            assert(version == H5S_POINT_VERSION_1 || version == H5S_POINT_VERSION_2);

            /* Encode number of elements */
            UINT32ENCODE(pp, (uint32_t)space->select.num_elem);

            /* Encode each point in selection */
            curr = space->select.sel_info.pnt_lst->head;
            while (curr != NULL) {
                /* Encode each point */
                for (u = 0; u < space->extent.rank; u++)
                    UINT32ENCODE(pp, (uint32_t)curr->pnt[u]);
                curr = curr->next;
            } /* end while */

            /* Add 4 bytes times the rank for each element selected */
            if (version == H5S_POINT_VERSION_1)
                len += (uint32_t)space->select.num_elem * 4 * space->extent.rank;
            break;

        case H5S_SELECT_INFO_ENC_SIZE_8:
            assert(version == H5S_POINT_VERSION_2);

            /* Encode number of elements */
            UINT64ENCODE(pp, space->select.num_elem);

            /* Encode each point in selection */
            curr = space->select.sel_info.pnt_lst->head;
            while (curr != NULL) {
                /* Encode each point */
                for (u = 0; u < space->extent.rank; u++)
                    UINT64ENCODE(pp, curr->pnt[u]);
                curr = curr->next;
            } /* end while */
            break;

        default:
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown point info size");
            break;

    } /* end switch */

    if (version == H5S_POINT_VERSION_1)
        UINT32ENCODE(lenp, (uint32_t)len); /* Store the length of the extra information */

    /* Update encoding pointer */
    *p = pp;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__point_serialize() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_deserialize
 PURPOSE
    Deserialize the current selection from a user-provided buffer.
 USAGE
    herr_t H5S__point_deserialize(space, p)
        H5S_t **space;          IN/OUT: Dataspace pointer to place
                                selection into
        uint8 **p;              OUT: Pointer to buffer holding serialized
                                selection.  Will be advanced to end of
                                serialized selection.
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Deserializes the current selection into a buffer.  (Primarily for retrieving
    from disk).
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_deserialize(H5S_t **space, const uint8_t **p, const size_t p_size, bool skip)
{
    H5S_t *tmp_space = NULL;                 /* Pointer to actual dataspace to use,
                                                either *space or a newly allocated one */
    hsize_t        dims[H5S_MAX_RANK];       /* Dimension sizes */
    uint32_t       version;                  /* Version number */
    uint8_t        enc_size = 0;             /* Encoded size of selection info */
    hsize_t       *coord    = NULL, *tcoord; /* Pointer to array of elements */
    const uint8_t *pp;                       /* Local pointer for decoding */
    uint64_t       num_elem = 0;             /* Number of elements in selection */
    unsigned       rank;                     /* Rank of points */
    unsigned       i, j;                     /* local counting variables */
    size_t         enc_type_size;
    size_t         coordinate_buffer_requirement;
    herr_t         ret_value = SUCCEED;         /* Return value */
    const uint8_t *p_end     = *p + p_size - 1; /* Pointer to last valid byte in buffer */
    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(p);
    pp = (*p);
    assert(pp);

    /* As part of the efforts to push all selection-type specific coding
       to the callbacks, the coding for the allocation of a null dataspace
       is moved from H5S_select_deserialize() in H5Sselect.c to here.
       This is needed for decoding virtual layout in H5O__layout_decode() */
    /* Allocate space if not provided */
    if (!*space) {
        if (NULL == (tmp_space = H5S_create(H5S_SIMPLE)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create dataspace");
    } /* end if */
    else
        tmp_space = *space;

    /* Decode version */
    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint32_t), p_end))
        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding selection version");
    UINT32DECODE(pp, version);

    if (version < H5S_POINT_VERSION_1 || version > H5S_POINT_VERSION_LATEST)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "bad version number for point selection");

    if (version >= (uint32_t)H5S_POINT_VERSION_2) {
        /* Decode size of point info */
        if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 1, p_end))
            HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding point info");
        enc_size = *(pp)++;
    }
    else {
        /* Skip over the remainder of the header */
        if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, 8, p_end))
            HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                        "buffer overflow while decoding selection headers");
        pp += 8;
        enc_size = H5S_SELECT_INFO_ENC_SIZE_4;
    }

    /* Check encoded size */
    if (enc_size & ~H5S_SELECT_INFO_ENC_SIZE_BITS)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTLOAD, FAIL, "unknown size of point/offset info for selection");

    /* Decode the rank of the point selection */
    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint32_t), p_end))
        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL, "buffer overflow while decoding selection rank");
    UINT32DECODE(pp, rank);

    if (!*space) {
        /* Patch the rank of the allocated dataspace */
        (void)memset(dims, 0, (size_t)rank * sizeof(dims[0]));
        if (H5S_set_extent_simple(tmp_space, rank, dims, NULL) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "can't set dimensions");
    } /* end if */
    else
        /* Verify the rank of the provided dataspace */
        if (rank != tmp_space->extent.rank)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL,
                        "rank of serialized selection does not match dataspace");

    /* decode the number of points */
    switch (enc_size) {
        case H5S_SELECT_INFO_ENC_SIZE_2:
            if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint16_t), p_end))
                HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                            "buffer overflow while decoding number of points");

            UINT16DECODE(pp, num_elem);
            break;
        case H5S_SELECT_INFO_ENC_SIZE_4:
            if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint32_t), p_end))
                HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                            "buffer overflow while decoding number of points");

            UINT32DECODE(pp, num_elem);
            break;
        case H5S_SELECT_INFO_ENC_SIZE_8:
            if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, sizeof(uint64_t), p_end))
                HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                            "buffer overflow while decoding number of points");

            UINT64DECODE(pp, num_elem);
            break;
        default:
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown point info size");
            break;
    } /* end switch */

    /* Allocate space for the coordinates */
    if (NULL == (coord = (hsize_t *)H5MM_malloc(num_elem * rank * sizeof(hsize_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "can't allocate coordinate information");

    /* Determine necessary size of buffer for coordinates */
    enc_type_size = 0;

    switch (enc_size) {
        case H5S_SELECT_INFO_ENC_SIZE_2:
            enc_type_size = sizeof(uint16_t);
            break;
        case H5S_SELECT_INFO_ENC_SIZE_4:
            enc_type_size = sizeof(uint32_t);
            break;
        case H5S_SELECT_INFO_ENC_SIZE_8:
            enc_type_size = sizeof(uint64_t);
            break;
        default:
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown point info size");
            break;
    }

    coordinate_buffer_requirement = num_elem * rank * enc_type_size;

    if (H5_IS_KNOWN_BUFFER_OVERFLOW(skip, pp, coordinate_buffer_requirement, p_end))
        HGOTO_ERROR(H5E_DATASPACE, H5E_OVERFLOW, FAIL,
                    "buffer overflow while decoding selection coordinates");

    /* Retrieve the coordinates from the buffer */
    for (tcoord = coord, i = 0; i < num_elem; i++)
        for (j = 0; j < (unsigned)rank; j++, tcoord++)
            switch (enc_size) {
                case H5S_SELECT_INFO_ENC_SIZE_2:
                    UINT16DECODE(pp, *tcoord);
                    break;
                case H5S_SELECT_INFO_ENC_SIZE_4:
                    UINT32DECODE(pp, *tcoord);
                    break;
                case H5S_SELECT_INFO_ENC_SIZE_8:
                    UINT64DECODE(pp, *tcoord);
                    break;
                default:
                    HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "unknown point info size");
                    break;
            } /* end switch */

    /* Select points */
    if (H5S_select_elements(tmp_space, H5S_SELECT_SET, num_elem, (const hsize_t *)coord) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't change selection");

    /* Update decoding pointer */
    *p = pp;

    /* Return space to the caller if allocated */
    if (!*space)
        *space = tmp_space;

done:
    /* Free temporary space if not passed to caller (only happens on error) */
    if (!*space && tmp_space)
        if (H5S_close(tmp_space) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTFREE, FAIL, "can't close dataspace");

    /* Free the coordinate array if necessary */
    if (coord != NULL)
        H5MM_xfree(coord);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_deserialize() */

/*--------------------------------------------------------------------------
 NAME
    H5S__get_select_elem_pointlist
 PURPOSE
    Get the list of element points currently selected
 USAGE
    herr_t H5S__get_select_elem_pointlist(space, hsize_t *buf)
        const H5S_t *space;     IN: Dataspace pointer of selection to query
        hsize_t startpoint;     IN: Element point to start with
        hsize_t numpoints;      IN: Number of element points to get
        hsize_t *buf;           OUT: List of element points selected
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
        Puts a list of the element points into the user's buffer.  The points
    start with the 'startpoint'th block in the list of points and put
    'numpoints' number of points into the user's buffer (or until the end of
    the list of points, whichever happen first)
        The point coordinates have the same dimensionality (rank) as the
    dataspace they are located within.  The list of points is formatted as
    follows: <coordinate> followed by the next coordinate, etc. until all the
    point information in the selection have been put into the user's buffer.
        The points are returned in the order they will be iterated through
    when a selection is read/written from/to disk.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__get_select_elem_pointlist(const H5S_t *space, hsize_t startpoint, hsize_t numpoints, hsize_t *buf)
{
    const hsize_t   endpoint = startpoint + numpoints; /* Index of last point in iteration */
    H5S_pnt_node_t *node;                              /* Point node */
    unsigned        rank;                              /* Dataspace rank */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);
    assert(buf);

    /* Get the dataspace extent rank */
    rank = space->extent.rank;

    /* Check for cached point at the correct index */
    if (space->select.sel_info.pnt_lst->last_idx_pnt &&
        startpoint == space->select.sel_info.pnt_lst->last_idx)
        node = space->select.sel_info.pnt_lst->last_idx_pnt;
    else {
        /* Get the head of the point list */
        node = space->select.sel_info.pnt_lst->head;

        /* Iterate to the first point to return */
        while (node != NULL && startpoint > 0) {
            startpoint--;
            node = node->next;
        } /* end while */
    }     /* end else */

    /* Iterate through the node, copying each point's information */
    while (node != NULL && numpoints > 0) {
        H5MM_memcpy(buf, node->pnt, sizeof(hsize_t) * rank);
        buf += rank;
        numpoints--;
        node = node->next;
    } /* end while */

    /* Cached next point in iteration */
    space->select.sel_info.pnt_lst->last_idx     = endpoint;
    space->select.sel_info.pnt_lst->last_idx_pnt = node;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__get_select_elem_pointlist() */

/*--------------------------------------------------------------------------
 NAME
    H5Sget_select_elem_pointlist
 PURPOSE
    Get the list of element points currently selected
 USAGE
    herr_t H5Sget_select_elem_pointlist(dsid, hsize_t *buf)
        hid_t dsid;             IN: Dataspace ID of selection to query
        hsize_t startpoint;     IN: Element point to start with
        hsize_t numpoints;      IN: Number of element points to get
        hsize_t buf[];          OUT: List of element points selected
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
        Puts a list of the element points into the user's buffer.  The points
    start with the 'startpoint'th block in the list of points and put
    'numpoints' number of points into the user's buffer (or until the end of
    the list of points, whichever happen first)
        The point coordinates have the same dimensionality (rank) as the
    dataspace they are located within.  The list of points is formatted as
    follows: <coordinate> followed by the next coordinate, etc. until all the
    point information in the selection have been put into the user's buffer.
        The points are returned in the order they will be iterated through
    when a selection is read/written from/to disk.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sget_select_elem_pointlist(hid_t spaceid, hsize_t startpoint, hsize_t numpoints,
                             hsize_t buf[/*numpoints*/] /*out*/)
{
    H5S_t *space;     /* Dataspace to modify selection of */
    herr_t ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ihhx", spaceid, startpoint, numpoints, buf);

    /* Check args */
    if (NULL == buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pointer");
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_GET_SELECT_TYPE(space) != H5S_SEL_POINTS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a point selection");

    ret_value = H5S__get_select_elem_pointlist(space, startpoint, numpoints, buf);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sget_select_elem_pointlist() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_bounds
 PURPOSE
    Gets the bounding box containing the selection.
 USAGE
    herr_t H5S__point_bounds(space, start, end)
        H5S_t *space;           IN: Dataspace pointer of selection to query
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
    with be (4, 5), (10, 8).
        The bounding box calculations _does_ include the current offset of the
    selection within the dataspace extent.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_bounds(const H5S_t *space, hsize_t *start, hsize_t *end)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(space);
    assert(start);
    assert(end);

    /* Loop over dimensions */
    for (u = 0; u < space->extent.rank; u++) {
        /* Sanity check */
        assert(space->select.sel_info.pnt_lst->low_bounds[u] <=
               space->select.sel_info.pnt_lst->high_bounds[u]);

        /* Check for offset moving selection negative */
        if (((hssize_t)space->select.sel_info.pnt_lst->low_bounds[u] + space->select.offset[u]) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "offset moves selection out of bounds");

        /* Set the low & high bounds in this dimension */
        start[u] =
            (hsize_t)((hssize_t)space->select.sel_info.pnt_lst->low_bounds[u] + space->select.offset[u]);
        end[u] =
            (hsize_t)((hssize_t)space->select.sel_info.pnt_lst->high_bounds[u] + space->select.offset[u]);
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_bounds() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_offset
 PURPOSE
    Gets the linear offset of the first element for the selection.
 USAGE
    herr_t H5S__point_offset(space, offset)
        const H5S_t *space;     IN: Dataspace pointer of selection to query
        hsize_t *offset;        OUT: Linear offset of first element in selection
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the linear offset (in "units" of elements) of the first element
    selected within the dataspace.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Calling this function on a "none" selection returns fail.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_offset(const H5S_t *space, hsize_t *offset)
{
    const hsize_t  *pnt;                 /* Pointer to a selected point's coordinates */
    const hssize_t *sel_offset;          /* Pointer to the selection's offset */
    const hsize_t  *dim_size;            /* Pointer to a dataspace's extent */
    hsize_t         accum;               /* Accumulator for dimension sizes */
    int             i;                   /* index variable */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(space);
    assert(offset);

    /* Start at linear offset 0 */
    *offset = 0;

    /* Set up pointers to arrays of values */
    pnt        = space->select.sel_info.pnt_lst->head->pnt;
    sel_offset = space->select.offset;
    dim_size   = space->extent.size;

    /* Loop through coordinates, calculating the linear offset */
    accum = 1;
    for (i = (int)space->extent.rank - 1; i >= 0; i--) {
        hssize_t pnt_offset = (hssize_t)pnt[i] + sel_offset[i]; /* Point's offset in this dimension */

        /* Check for offset moving selection out of the dataspace */
        if (pnt_offset < 0 || (hsize_t)pnt_offset >= dim_size[i])
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "offset moves selection out of bounds");

        /* Add the point's offset in this dimension to the total linear offset */
        *offset += (hsize_t)pnt_offset * accum;

        /* Increase the accumulator */
        accum *= dim_size[i];
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_offset() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_unlim_dim
 PURPOSE
    Return unlimited dimension of selection, or -1 if none
 USAGE
    int H5S__point_unlim_dim(space)
        H5S_t *space;           IN: Dataspace pointer to check
 RETURNS
    Unlimited dimension of selection, or -1 if none (never fails).
 DESCRIPTION
    Returns the index of the unlimited dimension in this selection, or -1
    if the selection has no unlimited dimension.  Currently point
    selections cannot have an unlimited dimension, so this function always
    returns -1.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5S__point_unlim_dim(const H5S_t H5_ATTR_UNUSED *space)
{
    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(-1)
} /* end H5S__point_unlim_dim() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_is_contiguous
 PURPOSE
    Check if a point selection is contiguous within the dataspace extent.
 USAGE
    htri_t H5S__point_is_contiguous(space)
        H5S_t *space;           IN: Dataspace pointer to check
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspace is contiguous.
    This is primarily used for reading the entire selection in one swoop.
    This code currently doesn't properly check for contiguousness when there is
    more than one point, as that would take a lot of extra coding that we
    don't need now.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__point_is_contiguous(const H5S_t *space)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);

    /* One point is definitely contiguous */
    if (space->select.num_elem == 1)
        ret_value = true;
    else /* More than one point might be contiguous, but it's complex to check and we don't need it right now
          */
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_is_contiguous() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_is_single
 PURPOSE
    Check if a point selection is single within the dataspace extent.
 USAGE
    htri_t H5S__point_is_single(space)
        H5S_t *space;           IN: Dataspace pointer to check
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in the dataspace is a single block.
    This is primarily used for reading the entire selection in one swoop.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__point_is_single(const H5S_t *space)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);

    /* One point is definitely 'single' :-) */
    if (space->select.num_elem == 1)
        ret_value = true;
    else
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_is_single() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_is_regular
 PURPOSE
    Check if a point selection is "regular"
 USAGE
    htri_t H5S__point_is_regular(space)
        H5S_t *space;     IN: Dataspace pointer to check
 RETURNS
    true/false/FAIL
 DESCRIPTION
    Checks to see if the current selection in a dataspace is the a regular
    pattern.
    This is primarily used for reading the entire selection in one swoop.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Doesn't check for points selected to be next to one another in a regular
    pattern yet.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__point_is_regular(H5S_t *space)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(space);

    /* Only simple check for regular points for now... */
    if (space->select.num_elem == 1)
        ret_value = true;
    else
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_is_regular() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_shape_same
 PURPOSE
    Check if a two "point" selections are the same shape
 USAGE
    htri_t H5S__point_shape_same(space1, space2)
        H5S_t *space1;           IN: First dataspace to check
        H5S_t *space2;           IN: Second dataspace to check
 RETURNS
    true / false / FAIL
 DESCRIPTION
    Checks to see if the current selection in each dataspace are the same
    shape.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static htri_t
H5S__point_shape_same(H5S_t *space1, H5S_t *space2)
{
    H5S_pnt_node_t *pnt1, *pnt2;          /* Point information nodes */
    hssize_t        offset[H5S_MAX_RANK]; /* Offset between the selections */
    unsigned        space1_rank;          /* Number of dimensions of first dataspace */
    unsigned        space2_rank;          /* Number of dimensions of second dataspace */
    int             space1_dim;           /* Current dimension in first dataspace */
    int             space2_dim;           /* Current dimension in second dataspace */
    htri_t          ret_value = true;     /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(space1);
    assert(space2);

    /* Get dataspace ranks */
    space1_rank = space1->extent.rank;
    space2_rank = space2->extent.rank;

    /* Sanity check */
    assert(space1_rank >= space2_rank);
    assert(space2_rank > 0);

    /* Initialize dimensions */
    space1_dim = (int)space1_rank - 1;
    space2_dim = (int)space2_rank - 1;

    /* Look at first point in each selection to compute the offset for common
     *  dimensions.
     */
    pnt1 = space1->select.sel_info.pnt_lst->head;
    pnt2 = space2->select.sel_info.pnt_lst->head;
    while (space2_dim >= 0) {
        /* Set the relative locations of the selections */
        offset[space1_dim] = (hssize_t)pnt2->pnt[space2_dim] - (hssize_t)pnt1->pnt[space1_dim];

        space1_dim--;
        space2_dim--;
    } /* end while */

    /* For dimensions that appear only in space1: */
    while (space1_dim >= 0) {
        /* Set the absolute offset of the remaining dimensions */
        offset[space1_dim] = (hssize_t)pnt1->pnt[space1_dim];

        space1_dim--;
    } /* end while */

    /* Advance to next point */
    pnt1 = pnt1->next;
    pnt2 = pnt2->next;

    /* Loop over remaining points */
    while (pnt1 && pnt2) {
        /* Initialize dimensions */
        space1_dim = (int)space1_rank - 1;
        space2_dim = (int)space2_rank - 1;

        /* Compare locations in common dimensions, including relative offset */
        while (space2_dim >= 0) {
            if ((hsize_t)((hssize_t)pnt1->pnt[space1_dim] + offset[space1_dim]) != pnt2->pnt[space2_dim])
                HGOTO_DONE(false);

            space1_dim--;
            space2_dim--;
        } /* end while */

        /* For dimensions that appear only in space1: */
        while (space1_dim >= 0) {
            /* Compare the absolute offset in the remaining dimensions */
            if ((hssize_t)pnt1->pnt[space1_dim] != offset[space1_dim])
                HGOTO_DONE(false);

            space1_dim--;
        } /* end while */

        /* Advance to next point */
        pnt1 = pnt1->next;
        pnt2 = pnt2->next;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_shape_same() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_intersect_block
 PURPOSE
    Detect intersections of selection with block
 USAGE
    htri_t H5S__point_intersect_block(space, start, end)
        H5S_t *space;           IN: Dataspace with selection to use
        const hsize_t *start;   IN: Starting coordinate for block
        const hsize_t *end;     IN: Ending coordinate for block
 RETURNS
    Non-negative true / false on success, negative on failure
 DESCRIPTION
    Quickly detect intersections with a block
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5S__point_intersect_block(H5S_t *space, const hsize_t *start, const hsize_t *end)
{
    H5S_pnt_node_t *pnt;               /* Point information node */
    htri_t          ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(space);
    assert(H5S_SEL_POINTS == H5S_GET_SELECT_TYPE(space));
    assert(start);
    assert(end);

    /* Loop over points */
    pnt = space->select.sel_info.pnt_lst->head;
    while (pnt) {
        unsigned u; /* Local index variable */

        /* Verify that the point is within the block */
        for (u = 0; u < space->extent.rank; u++)
            if (pnt->pnt[u] < start[u] || pnt->pnt[u] > end[u])
                break;

        /* Check if point was within block for all dimensions */
        if (u == space->extent.rank)
            HGOTO_DONE(true);

        /* Advance to next point */
        pnt = pnt->next;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_intersect_block() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_adjust_u
 PURPOSE
    Adjust a "point" selection by subtracting an offset
 USAGE
    herr_t H5S__point_adjust_u(space, offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to adjust
        const hsize_t *offset; IN: Offset to subtract
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves a point selection by subtracting an offset from it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_adjust_u(H5S_t *space, const hsize_t *offset)
{
    bool            non_zero_offset = false; /* Whether any offset is non-zero */
    H5S_pnt_node_t *node;                    /* Point node */
    unsigned        rank;                    /* Dataspace rank */
    unsigned        u;                       /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);
    assert(offset);

    /* Check for an all-zero offset vector */
    for (u = 0; u < space->extent.rank; u++)
        if (0 != offset[u]) {
            non_zero_offset = true;
            break;
        }

    /* Only perform operation if the offset is non-zero */
    if (non_zero_offset) {
        /* Iterate through the nodes, checking the bounds on each element */
        node = space->select.sel_info.pnt_lst->head;
        rank = space->extent.rank;
        while (node) {
            /* Adjust each coordinate for point node */
            for (u = 0; u < rank; u++) {
                /* Check for offset moving selection negative */
                assert(node->pnt[u] >= offset[u]);

                /* Adjust node's coordinate location */
                node->pnt[u] -= offset[u];
            } /* end for */

            /* Advance to next point node in selection */
            node = node->next;
        } /* end while */

        /* update the bound box of the selection */
        for (u = 0; u < rank; u++) {
            space->select.sel_info.pnt_lst->low_bounds[u] -= offset[u];
            space->select.sel_info.pnt_lst->high_bounds[u] -= offset[u];
        } /* end for */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_adjust_u() */

/*--------------------------------------------------------------------------
 NAME
    H5S__point_adjust_s
 PURPOSE
    Adjust a "point" selection by subtracting an offset
 USAGE
    herr_t H5S__point_adjust_u(space, offset)
        H5S_t *space;           IN/OUT: Pointer to dataspace to adjust
        const hssize_t *offset; IN: Offset to subtract
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves a point selection by subtracting an offset from it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5S__point_adjust_s(H5S_t *space, const hssize_t *offset)
{
    bool            non_zero_offset = false; /* Whether any offset is non-zero */
    H5S_pnt_node_t *node;                    /* Point node */
    unsigned        rank;                    /* Dataspace rank */
    unsigned        u;                       /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    assert(space);
    assert(offset);

    /* Check for an all-zero offset vector */
    for (u = 0; u < space->extent.rank; u++)
        if (0 != offset[u]) {
            non_zero_offset = true;
            break;
        } /* end if */

    /* Only perform operation if the offset is non-zero */
    if (non_zero_offset) {
        /* Iterate through the nodes, checking the bounds on each element */
        node = space->select.sel_info.pnt_lst->head;
        rank = space->extent.rank;
        while (node) {
            /* Adjust each coordinate for point node */
            for (u = 0; u < rank; u++) {
                /* Check for offset moving selection negative */
                assert((hssize_t)node->pnt[u] >= offset[u]);

                /* Adjust node's coordinate location */
                node->pnt[u] = (hsize_t)((hssize_t)node->pnt[u] - offset[u]);
            } /* end for */

            /* Advance to next point node in selection */
            node = node->next;
        } /* end while */

        /* update the bound box of the selection */
        for (u = 0; u < rank; u++) {
            assert((hssize_t)space->select.sel_info.pnt_lst->low_bounds[u] >= offset[u]);
            space->select.sel_info.pnt_lst->low_bounds[u] =
                (hsize_t)((hssize_t)space->select.sel_info.pnt_lst->low_bounds[u] - offset[u]);
            space->select.sel_info.pnt_lst->high_bounds[u] =
                (hsize_t)((hssize_t)space->select.sel_info.pnt_lst->high_bounds[u] - offset[u]);
        } /* end for */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5S__point_adjust_s() */

/*-------------------------------------------------------------------------
 * Function:    H5S__point_project_scalar
 *
 * Purpose:     Projects a single element point selection into a scalar
 *              dataspace
 *
 * Return:      Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__point_project_scalar(const H5S_t *space, hsize_t *offset)
{
    const H5S_pnt_node_t *node;                /* Point node */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space && H5S_SEL_POINTS == H5S_GET_SELECT_TYPE(space));
    assert(offset);

    /* Get the head of the point list */
    node = space->select.sel_info.pnt_lst->head;

    /* Check for more than one point selected */
    if (node->next)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL,
                    "point selection of one element has more than one node!");

    /* Calculate offset of selection in projected buffer */
    *offset = H5VM_array_offset(space->extent.rank, space->extent.size, node->pnt);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_project_scalar() */

/*-------------------------------------------------------------------------
 * Function:    H5S__point_project_simple
 *
 * Purpose:     Projects a point selection onto/into a simple dataspace
 *              of a different rank
 *
 * Return:      Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__point_project_simple(const H5S_t *base_space, H5S_t *new_space, hsize_t *offset)
{
    const H5S_pnt_node_t *base_node;           /* Point node in base space */
    H5S_pnt_node_t       *new_node;            /* Point node in new space */
    H5S_pnt_node_t       *prev_node;           /* Previous point node in new space */
    unsigned              rank_diff;           /* Difference in ranks between spaces */
    unsigned              u;                   /* Local index variable */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(base_space && H5S_SEL_POINTS == H5S_GET_SELECT_TYPE(base_space));
    assert(new_space);
    assert(offset);

    /* We are setting a new selection, remove any current selection in new dataspace */
    if (H5S_SELECT_RELEASE(new_space) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't release selection");

    /* Allocate room for the head of the point list */
    if (NULL == (new_space->select.sel_info.pnt_lst = H5FL_MALLOC(H5S_pnt_list_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate point list node");

    /* Check if the new space's rank is < or > base space's rank */
    if (new_space->extent.rank < base_space->extent.rank) {
        hsize_t block[H5S_MAX_RANK]; /* Block selected in base dataspace */

        /* Compute the difference in ranks */
        rank_diff = base_space->extent.rank - new_space->extent.rank;

        /* Calculate offset of selection in projected buffer */
        memset(block, 0, sizeof(block));
        H5MM_memcpy(block, base_space->select.sel_info.pnt_lst->head->pnt, sizeof(hsize_t) * rank_diff);
        *offset = H5VM_array_offset(base_space->extent.rank, base_space->extent.size, block);

        /* Iterate through base space's point nodes, copying the point information */
        base_node = base_space->select.sel_info.pnt_lst->head;
        prev_node = NULL;
        while (base_node) {
            /* Create new point */
            if (NULL == (new_node = (H5S_pnt_node_t *)H5FL_ARR_MALLOC(hcoords_t, new_space->extent.rank)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate point node");
            new_node->next = NULL;

            /* Copy over the point's coordinates */
            H5MM_memcpy(new_node->pnt, &base_node->pnt[rank_diff],
                        (new_space->extent.rank * sizeof(hsize_t)));

            /* Keep the order the same when copying */
            if (NULL == prev_node)
                prev_node = new_space->select.sel_info.pnt_lst->head = new_node;
            else {
                prev_node->next = new_node;
                prev_node       = new_node;
            } /* end else */

            /* Advance to next node */
            base_node = base_node->next;
        } /* end while */

        /* Update the bounding box */
        for (u = 0; u < new_space->extent.rank; u++) {
            new_space->select.sel_info.pnt_lst->low_bounds[u] =
                base_space->select.sel_info.pnt_lst->low_bounds[u + rank_diff];
            new_space->select.sel_info.pnt_lst->high_bounds[u] =
                base_space->select.sel_info.pnt_lst->high_bounds[u + rank_diff];
        } /* end for */
    }     /* end if */
    else {
        assert(new_space->extent.rank > base_space->extent.rank);

        /* Compute the difference in ranks */
        rank_diff = new_space->extent.rank - base_space->extent.rank;

        /* The offset is zero when projected into higher dimensions */
        *offset = 0;

        /* Iterate through base space's point nodes, copying the point information */
        base_node = base_space->select.sel_info.pnt_lst->head;
        prev_node = NULL;
        while (base_node) {
            /* Create new point */
            if (NULL == (new_node = (H5S_pnt_node_t *)H5FL_ARR_MALLOC(hcoords_t, new_space->extent.rank)))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate point node");
            new_node->next = NULL;

            /* Copy over the point's coordinates */
            memset(new_node->pnt, 0, sizeof(hsize_t) * rank_diff);
            H5MM_memcpy(&new_node->pnt[rank_diff], base_node->pnt,
                        (base_space->extent.rank * sizeof(hsize_t)));

            /* Keep the order the same when copying */
            if (NULL == prev_node)
                prev_node = new_space->select.sel_info.pnt_lst->head = new_node;
            else {
                prev_node->next = new_node;
                prev_node       = new_node;
            } /* end else */

            /* Advance to next node */
            base_node = base_node->next;
        } /* end while */

        /* Update the bounding box */
        for (u = 0; u < rank_diff; u++) {
            new_space->select.sel_info.pnt_lst->low_bounds[u]  = 0;
            new_space->select.sel_info.pnt_lst->high_bounds[u] = 0;
        } /* end for */
        for (; u < new_space->extent.rank; u++) {
            new_space->select.sel_info.pnt_lst->low_bounds[u] =
                base_space->select.sel_info.pnt_lst->low_bounds[u - rank_diff];
            new_space->select.sel_info.pnt_lst->high_bounds[u] =
                base_space->select.sel_info.pnt_lst->high_bounds[u - rank_diff];
        } /* end for */
    }     /* end else */

    /* Clear cached iteration point */
    new_space->select.sel_info.pnt_lst->last_idx     = 0;
    new_space->select.sel_info.pnt_lst->last_idx_pnt = NULL;

    /* Number of elements selected will be the same */
    new_space->select.num_elem = base_space->select.num_elem;

    /* Set selection type */
    new_space->select.type = H5S_sel_point;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__point_project_simple() */

/*--------------------------------------------------------------------------
 NAME
    H5Sselect_elements
 PURPOSE
    Specify a series of elements in the dataspace to select
 USAGE
    herr_t H5Sselect_elements(dsid, op, num_elem, coord)
        hid_t dsid;             IN: Dataspace ID of selection to modify
        H5S_seloper_t op;       IN: Operation to perform on current selection
        size_t num_elem;        IN: Number of elements in COORD array.
        const hsize_t *coord;   IN: The location of each element selected
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    This function selects array elements to be included in the selection for
    the dataspace.  The COORD array is a 2-D array of size <dataspace rank>
    by NUM_ELEM (ie. a list of coordinates in the dataspace).  The order of
    the element coordinates in the COORD array specifies the order that the
    array elements are iterated through when I/O is performed.  Duplicate
    coordinates are not checked for.  The selection operator, OP, determines
    how the new selection is to be combined with the existing selection for
    the dataspace.  Currently, only H5S_SELECT_SET is supported, which replaces
    the existing selection with the one defined in this call.  When operators
    other than H5S_SELECT_SET are used to combine a new selection with an
    existing selection, the selection ordering is reset to 'C' array ordering.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Sselect_elements(hid_t spaceid, H5S_seloper_t op, size_t num_elem, const hsize_t *coord)
{
    H5S_t *space;     /* Dataspace to modify selection of */
    herr_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iSsz*h", spaceid, op, num_elem, coord);

    /* Check args */
    if (NULL == (space = (H5S_t *)H5I_object_verify(spaceid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (H5S_SCALAR == H5S_GET_EXTENT_TYPE(space))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "point doesn't support H5S_SCALAR space");
    if (H5S_NULL == H5S_GET_EXTENT_TYPE(space))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "point doesn't support H5S_NULL space");
    if (coord == NULL || num_elem == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "elements not specified");
    if (!(op == H5S_SELECT_SET || op == H5S_SELECT_APPEND || op == H5S_SELECT_PREPEND))
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, FAIL, "unsupported operation attempted");

    /* Call the real element selection routine */
    if ((ret_value = H5S_select_elements(space, op, num_elem, coord)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTDELETE, FAIL, "can't select elements");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Sselect_elements() */
