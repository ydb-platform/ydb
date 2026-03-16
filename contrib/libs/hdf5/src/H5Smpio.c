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
 * Purpose:     Create MPI data types for HDF5 selections.
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
#include "H5FLprivate.h" /* Free Lists				*/
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Spkg.h"      /* Dataspaces 				*/
#include "H5VMprivate.h" /* Vector and array functions		*/

#ifdef H5_HAVE_PARALLEL

/****************/
/* Local Macros */
/****************/
#define H5S_MPIO_INITIAL_ALLOC_COUNT 256

/*******************/
/* Local Variables */
/*******************/

/******************/
/* Local Typedefs */
/******************/

/* Node in linked list of MPI data types created during traversal of irregular hyperslab selection */
typedef struct H5S_mpio_mpitype_node_t {
    MPI_Datatype                    type; /* MPI Datatype */
    struct H5S_mpio_mpitype_node_t *next; /* Pointer to next node in list */
} H5S_mpio_mpitype_node_t;

/* List to track MPI data types generated during traversal of irregular hyperslab selection */
typedef struct H5S_mpio_mpitype_list_t {
    H5S_mpio_mpitype_node_t *head; /* Pointer to head of list */
    H5S_mpio_mpitype_node_t *tail; /* Pointer to tail of list */
} H5S_mpio_mpitype_list_t;

/********************/
/* Local Prototypes */
/********************/
static herr_t H5S__mpio_all_type(const H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count,
                                 bool *is_derived_type);
static herr_t H5S__mpio_none_type(MPI_Datatype *new_type, int *count, bool *is_derived_type);
static herr_t H5S__mpio_create_point_datatype(size_t elmt_size, hsize_t num_points, MPI_Aint *disp,
                                              MPI_Datatype *new_type);
static herr_t H5S__mpio_point_type(const H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count,
                                   bool *is_derived_type, bool do_permute, hsize_t **permute_map,
                                   bool *is_permuted);
static herr_t H5S__mpio_permute_type(H5S_t *space, size_t elmt_size, hsize_t **permute_map,
                                     MPI_Datatype *new_type, int *count, bool *is_derived_type);
static herr_t H5S__mpio_reg_hyper_type(H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count,
                                       bool *is_derived_type);
static herr_t H5S__mpio_span_hyper_type(const H5S_t *space, size_t elmt_size, MPI_Datatype *new_type,
                                        int *count, bool *is_derived_type);
static herr_t H5S__release_datatype(H5S_mpio_mpitype_list_t *type_list);
static herr_t H5S__obtain_datatype(H5S_hyper_span_info_t *spans, const hsize_t *down, size_t elmt_size,
                                   const MPI_Datatype *elmt_type, MPI_Datatype *span_type,
                                   H5S_mpio_mpitype_list_t *type_list, unsigned op_info_i, uint64_t op_gen);

/*****************************/
/* Library Private Variables */
/*****************************/

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5S_mpio_mpitype_node_t struct */
H5FL_DEFINE_STATIC(H5S_mpio_mpitype_node_t);

/* Declare a free list to manage dataspace selection iterators */
H5FL_EXTERN(H5S_sel_iter_t);

/*-------------------------------------------------------------------------
 * Function:	H5S__mpio_all_type
 *
 * Purpose:	Translate an HDF5 "all" selection into an MPI type.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *		*count		  how many objects of the new_type in selection
 *				  (useful if this is the buffer type for xfer)
 *		*is_derived_type  0 if MPI primitive type, 1 if derived
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__mpio_all_type(const H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count,
                   bool *is_derived_type)
{
    hsize_t  total_bytes;
    hssize_t snelmts;             /* Total number of elmts	(signed) */
    hsize_t  nelmts;              /* Total number of elmts	*/
    hsize_t  bigio_count;         /* Transition point to create derived type */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);

    /* Just treat the entire extent as a block of bytes */
    if ((snelmts = (hssize_t)H5S_GET_EXTENT_NPOINTS(space)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "src dataspace has invalid selection");
    H5_CHECKED_ASSIGN(nelmts, hsize_t, snelmts, hssize_t);

    total_bytes = (hsize_t)elmt_size * nelmts;
    bigio_count = H5_mpi_get_bigio_count();

    /* Verify that the size can be expressed as a 32 bit integer */
    if (bigio_count >= total_bytes) {
        /* fill in the return values */
        *new_type = MPI_BYTE;
        H5_CHECKED_ASSIGN(*count, int, total_bytes, hsize_t);
        *is_derived_type = false;
    }
    else {
        /* Create a LARGE derived datatype for this transfer */
        if (H5_mpio_create_large_type(total_bytes, 0, MPI_BYTE, new_type) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                        "couldn't create a large datatype from the all selection");
        *count           = 1;
        *is_derived_type = true;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__mpio_all_type() */

/*-------------------------------------------------------------------------
 * Function:	H5S__mpio_none_type
 *
 * Purpose:	Translate an HDF5 "none" selection into an MPI type.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *		*count		  how many objects of the new_type in selection
 *				  (useful if this is the buffer type for xfer)
 *		*is_derived_type  0 if MPI primitive type, 1 if derived
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__mpio_none_type(MPI_Datatype *new_type, int *count, bool *is_derived_type)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* fill in the return values */
    *new_type        = MPI_BYTE;
    *count           = 0;
    *is_derived_type = false;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5S__mpio_none_type() */

/*-------------------------------------------------------------------------
 * Function:	H5S__mpio_create_point_datatype
 *
 * Purpose:	Create a derived datatype for point selections.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__mpio_create_point_datatype(size_t elmt_size, hsize_t num_points, MPI_Aint *disp, MPI_Datatype *new_type)
{
    MPI_Datatype  elmt_type;                 /* MPI datatype for individual element */
    bool          elmt_type_created = false; /* Whether the element MPI datatype was created */
    int          *inner_blocks      = NULL;  /* Arrays for MPI datatypes when "large" datatype needed */
    MPI_Aint     *inner_disps       = NULL;
    MPI_Datatype *inner_types       = NULL;
#if MPI_VERSION < 3
    int    *blocks = NULL; /* Array of block sizes for MPI hindexed create call */
    hsize_t u;             /* Local index variable */
#endif
    hsize_t bigio_count;         /* Transition point to create derived type */
    int     mpi_code;            /* MPI error code */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Create an MPI datatype for an element */
    if (MPI_SUCCESS != (mpi_code = MPI_Type_contiguous((int)elmt_size, MPI_BYTE, &elmt_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)
    elmt_type_created = true;

    bigio_count = H5_mpi_get_bigio_count();

    /* Check whether standard or BIGIO processing will be employeed */
    if (bigio_count >= num_points) {
#if H5_CHECK_MPI_VERSION(3, 0)
        /* Create an MPI datatype for the whole point selection */
        if (MPI_SUCCESS !=
            (mpi_code = MPI_Type_create_hindexed_block((int)num_points, 1, disp, elmt_type, new_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_indexed_block failed", mpi_code)
#else
        /* Allocate block sizes for MPI datatype call */
        if (NULL == (blocks = (int *)H5MM_malloc(sizeof(int) * num_points)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of blocks");

        for (u = 0; u < num_points; u++)
            blocks[u] = 1;

        /* Create an MPI datatype for the whole point selection */
        if (MPI_SUCCESS !=
            (mpi_code = MPI_Type_create_hindexed((int)num_points, blocks, disp, elmt_type, new_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed failed", mpi_code)
#endif

        /* Commit MPI datatype for later use */
        if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(new_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)
    }
    else {
        /* use LARGE_DATATYPE::
         * We'll create an hindexed_block type for every 2G point count and then combine
         * those and any remaining points into a single large datatype.
         */
        int     total_types, i;
        int     remaining_points;
        int     num_big_types;
        hsize_t leftover;

        /* Calculate how many Big MPI datatypes are needed to represent the buffer */
        num_big_types = (int)(num_points / bigio_count);

        leftover = (hsize_t)num_points - (hsize_t)num_big_types * (hsize_t)bigio_count;
        H5_CHECKED_ASSIGN(remaining_points, int, leftover, hsize_t);

        total_types = (int)(remaining_points) ? (num_big_types + 1) : num_big_types;

        /* Allocate array if MPI derived types needed */
        if (NULL == (inner_types = (MPI_Datatype *)H5MM_malloc((sizeof(MPI_Datatype) * (size_t)total_types))))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of blocks");

        if (NULL == (inner_blocks = (int *)H5MM_malloc(sizeof(int) * (size_t)total_types)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of blocks");

        if (NULL == (inner_disps = (MPI_Aint *)H5MM_malloc(sizeof(MPI_Aint) * (size_t)total_types)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of blocks");

#if MPI_VERSION < 3
        /* Allocate block sizes for MPI datatype call */
        if (NULL == (blocks = (int *)H5MM_malloc(sizeof(int) * bigio_count)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of blocks");

        for (u = 0; u < bigio_count; u++)
            blocks[u] = 1;
#endif

        for (i = 0; i < num_big_types; i++) {
#if H5_CHECK_MPI_VERSION(3, 0)
            if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hindexed_block((int)bigio_count, 1,
                                                                          &disp[(hsize_t)i * bigio_count],
                                                                          elmt_type, &inner_types[i])))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed_block failed", mpi_code);
#else
            if (MPI_SUCCESS !=
                (mpi_code = MPI_Type_create_hindexed((int)bigio_count, blocks, &disp[i * bigio_count],
                                                     elmt_type, &inner_types[i])))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed failed", mpi_code)
#endif
            inner_blocks[i] = 1;
            inner_disps[i]  = 0;
        } /* end for*/

        if (remaining_points) {
#if H5_CHECK_MPI_VERSION(3, 0)
            if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hindexed_block(
                                    remaining_points, 1, &disp[(hsize_t)num_big_types * bigio_count],
                                    elmt_type, &inner_types[num_big_types])))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed_block failed", mpi_code);
#else
            if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hindexed((int)remaining_points, blocks,
                                                                    &disp[num_big_types * bigio_count],
                                                                    elmt_type, &inner_types[num_big_types])))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed failed", mpi_code)
#endif
            inner_blocks[num_big_types] = 1;
            inner_disps[num_big_types]  = 0;
        }

        if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct(total_types, inner_blocks, inner_disps,
                                                              inner_types, new_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct", mpi_code);

        for (i = 0; i < total_types; i++)
            MPI_Type_free(&inner_types[i]);

        /* Commit MPI datatype for later use */
        if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(new_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)
    } /* end else */

done:
    if (elmt_type_created)
        MPI_Type_free(&elmt_type);
#if MPI_VERSION < 3
    if (blocks)
        H5MM_free(blocks);
#endif
    if (inner_types)
        H5MM_free(inner_types);
    if (inner_blocks)
        H5MM_free(inner_blocks);
    if (inner_disps)
        H5MM_free(inner_disps);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__mpio_create_point_datatype() */

/*-------------------------------------------------------------------------
 * Function:	H5S__mpio_point_type
 *
 * Purpose:	Translate an HDF5 "point" selection into an MPI type.
 *              Create a permutation array to handle out-of-order point selections.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *		*count		  how many objects of the new_type in selection
 *				  (useful if this is the buffer type for xfer)
 *		*is_derived_type  0 if MPI primitive type, 1 if derived
 *              *permute_map      the permutation of the displacements to create
 *                                the MPI_Datatype
 *              *is_permuted      0 if the displacements are permuted, 1 if not
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__mpio_point_type(const H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count,
                     bool *is_derived_type, bool do_permute, hsize_t **permute, bool *is_permuted)
{
    MPI_Aint       *disp = NULL;         /* Datatype displacement for each point*/
    H5S_pnt_node_t *curr = NULL;         /* Current point being operated on in from the selection */
    hssize_t        snum_points;         /* Signed number of elements in selection */
    hsize_t         num_points;          /* Sumber of points in the selection */
    hsize_t         u;                   /* Local index variable */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);

    /* Get the total number of points selected */
    if ((snum_points = (hssize_t)H5S_GET_SELECT_NPOINTS(space)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL, "can't get number of elements selected");
    num_points = (hsize_t)snum_points;

    /* Allocate array for element displacements */
    if (NULL == (disp = (MPI_Aint *)H5MM_malloc(sizeof(MPI_Aint) * num_points)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of displacements");

    /* Allocate array for element permutation - returned to caller */
    if (do_permute)
        if (NULL == (*permute = (hsize_t *)H5MM_malloc(sizeof(hsize_t) * num_points)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate permutation array");

    /* Iterate through list of elements */
    curr = space->select.sel_info.pnt_lst->head;
    for (u = 0; u < num_points; u++) {
        /* Calculate the displacement of the current point */
        hsize_t disp_tmp = H5VM_array_offset(space->extent.rank, space->extent.size, curr->pnt);
        if (disp_tmp > LONG_MAX) /* Maximum value of type long */
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "disp overflow");
        disp[u] = (MPI_Aint)disp_tmp;
        disp[u] *= (MPI_Aint)elmt_size;

        /* This is a File Space used to set the file view, so adjust the displacements
         * to have them monotonically non-decreasing.
         * Generate the permutation array by indicating at each point being selected,
         * the position it will shifted in the new displacement. Example:
         * Suppose 4 points with corresponding are selected
         * Pt 1: disp=6 ; Pt 2: disp=3 ; Pt 3: disp=0 ; Pt 4: disp=4
         * The permute map to sort the displacements in order will be:
         * point 1: map[0] = L, indicating that this point is not moved (1st point selected)
         * point 2: map[1] = 0, indicating that this point is moved to the first position,
         *                      since disp_pt1(6) > disp_pt2(3)
         * point 3: map[2] = 0, move to position 0, bec it has the lowest disp between
         *                      the points selected so far.
         * point 4: map[3] = 2, move the 2nd position since point 1 has a higher disp,
         *                      but points 2 and 3 have lower displacements.
         */
        if (do_permute) {
            if (u > 0 && disp[u] < disp[u - 1]) {
                hsize_t s = 0, l = u, m = u / 2;

                *is_permuted = true;
                do {
                    if (disp[u] > disp[m])
                        s = m + 1;
                    else if (disp[u] < disp[m])
                        l = m;
                    else
                        break;
                    m = s + ((l - s) / 2);
                } while (s < l);

                if (m < u) {
                    MPI_Aint temp;

                    temp = disp[u];
                    memmove(disp + m + 1, disp + m, (u - m) * sizeof(MPI_Aint));
                    disp[m] = temp;
                } /* end if */
                (*permute)[u] = m;
            } /* end if */
            else
                (*permute)[u] = num_points;
        } /* end if */
        /* this is a memory space, and no permutation is necessary to create
           the derived datatype */
        else {
            ; /* do nothing */
        }     /* end else */

        /* get the next point */
        curr = curr->next;
    } /* end for */

    /* Create the MPI datatype for the set of element displacements */
    if (H5S__mpio_create_point_datatype(elmt_size, num_points, disp, new_type) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create an MPI Datatype from point selection");

    /* Set values about MPI datatype created */
    *count           = 1;
    *is_derived_type = true;

done:
    if (NULL != disp)
        H5MM_free(disp);

    /* Release the permutation buffer, if it wasn't used */
    if (!(*is_permuted) && (*permute)) {
        H5MM_free(*permute);
        *permute = NULL;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__mpio_point_type() */

/*-------------------------------------------------------------------------
 * Function:	H5S__mpio_permute_type
 *
 * Purpose:	Translate an HDF5 "all/hyper/point" selection into an MPI type,
 *              while applying the permutation map. This function is called if
 *              the file space selection is permuted due to out-of-order point
 *              selection and so the memory datatype has to be permuted using the
 *              permutation map created by the file selection.
 *
 * Note:	This routine is called from H5S_mpio_space_type(), which is
 *              called first for the file dataspace and creates
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *		*count		  how many objects of the new_type in selection
 *				  (useful if this is the buffer type for xfer)
 *		*is_derived_type  0 if MPI primitive type, 1 if derived
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__mpio_permute_type(H5S_t *space, size_t elmt_size, hsize_t **permute, MPI_Datatype *new_type, int *count,
                       bool *is_derived_type)
{
    MPI_Aint       *disp          = NULL;  /* Datatype displacement for each point*/
    H5S_sel_iter_t *sel_iter      = NULL;  /* Selection iteration info */
    bool            sel_iter_init = false; /* Selection iteration info has been initialized */
    hssize_t        snum_points;           /* Signed number of elements in selection */
    hsize_t         num_points;            /* Number of points in the selection */
    hsize_t        *off = NULL;
    size_t         *len = NULL;
    size_t          max_elem;            /* Maximum number of elements allowed in sequences */
    hsize_t         u;                   /* Local index variable */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);

    /* Get the total number of points selected */
    if ((snum_points = (hssize_t)H5S_GET_SELECT_NPOINTS(space)) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCOUNT, FAIL, "can't get number of elements selected");
    num_points = (hsize_t)snum_points;

    /* Allocate array to store point displacements */
    if (NULL == (disp = (MPI_Aint *)H5MM_malloc(sizeof(MPI_Aint) * num_points)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of displacements");

    /* Allocate arrays to hold sequence offsets and lengths */
    if (NULL == (off = H5MM_malloc(H5D_IO_VECTOR_SIZE * sizeof(*off))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate sequence offsets array");
    if (NULL == (len = H5MM_malloc(H5D_IO_VECTOR_SIZE * sizeof(*len))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't allocate sequence lengths array");

    /* Allocate a selection iterator for iterating over the dataspace */
    if (NULL == (sel_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "couldn't allocate dataspace selection iterator");

    /* Initialize selection iterator */
    if (H5S_select_iter_init(sel_iter, space, elmt_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
    sel_iter_init = true; /* Selection iteration info has been initialized */

    /* Set the number of elements to iterate over */
    H5_CHECKED_ASSIGN(max_elem, size_t, num_points, hsize_t);

    /* Loop, while elements left in selection */
    u = 0;
    while (max_elem > 0) {
        size_t nelem;    /* Number of elements used in sequences */
        size_t nseq;     /* Number of sequences generated */
        size_t curr_seq; /* Current sequence being worked on */

        /* Get the sequences of bytes */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(sel_iter, (size_t)H5D_IO_VECTOR_SIZE, max_elem, &nseq, &nelem, off,
                                         len) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_UNSUPPORTED, FAIL, "sequence length generation failed");

        /* Loop, while sequences left to process */
        for (curr_seq = 0; curr_seq < nseq; curr_seq++) {
            hsize_t curr_off; /* Current offset within sequence */
            size_t  curr_len; /* Length of bytes left to process in sequence */

            /* Get the current offset */
            curr_off = off[curr_seq];

            /* Get the number of bytes in sequence */
            curr_len = len[curr_seq];

            /* Loop, while bytes left in sequence */
            while (curr_len > 0) {
                /* Set the displacement of the current point */
                if (curr_off > LONG_MAX)
                    HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "curr_off overflow");
                disp[u] = (MPI_Aint)curr_off;

                /* This is a memory displacement, so for each point selected,
                 * apply the map that was generated by the file selection */
                if ((*permute)[u] != num_points) {
                    MPI_Aint temp = disp[u];

                    memmove(disp + (*permute)[u] + 1, disp + (*permute)[u],
                            (u - (*permute)[u]) * sizeof(MPI_Aint));
                    disp[(*permute)[u]] = temp;
                } /* end if */

                /* Advance to next element */
                u++;

                /* Increment offset in dataspace */
                curr_off += elmt_size;

                /* Decrement number of bytes left in sequence */
                curr_len -= elmt_size;
            } /* end while */
        }     /* end for */

        /* Decrement number of elements left to process */
        max_elem -= nelem;
    } /* end while */

    /* Create the MPI datatype for the set of element displacements */
    if (H5S__mpio_create_point_datatype(elmt_size, num_points, disp, new_type) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create an MPI Datatype from point selection");

    /* Set values about MPI datatype created */
    *count           = 1;
    *is_derived_type = true;

done:
    /* Release selection iterator */
    if (sel_iter) {
        if (sel_iter_init && H5S_SELECT_ITER_RELEASE(sel_iter) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
        sel_iter = H5FL_FREE(H5S_sel_iter_t, sel_iter);
    }

    H5MM_free(len);
    H5MM_free(off);

    /* Free memory */
    if (disp)
        H5MM_free(disp);
    if (*permute) {
        H5MM_free(*permute);
        *permute = NULL;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5S__mpio_permute_type() */

/*-------------------------------------------------------------------------
 * Function:	H5S__mpio_reg_hyper_type
 *
 * Purpose:	Translate a regular HDF5 hyperslab selection into an MPI type.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *		*count		  how many objects of the new_type in selection
 *				  (useful if this is the buffer type for xfer)
 *		*is_derived_type  0 if MPI primitive type, 1 if derived
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__mpio_reg_hyper_type(H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count,
                         bool *is_derived_type)
{
    H5S_sel_iter_t *sel_iter      = NULL;  /* Selection iteration info */
    bool            sel_iter_init = false; /* Selection iteration info has been initialized */

    struct dim { /* less hassle than malloc/free & ilk */
        hssize_t start;
        hsize_t  strid;
        hsize_t  block;
        hsize_t  xtent;
        hsize_t  count;
    } d[H5S_MAX_RANK];

    hsize_t          bigio_count; /* Transition point to create derived type */
    hsize_t          offset[H5S_MAX_RANK];
    hsize_t          max_xtent[H5S_MAX_RANK];
    H5S_hyper_dim_t *diminfo; /* [rank] */
    unsigned         rank;
    MPI_Datatype     inner_type, outer_type;
    MPI_Aint         extent_len, start_disp, new_extent;
    MPI_Aint         lb;       /* Needed as an argument for MPI_Type_get_extent */
    unsigned         u;        /* Local index variable */
    int              i;        /* Local index variable */
    int              mpi_code; /* MPI return code */
    herr_t           ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);
    assert(sizeof(MPI_Aint) >= sizeof(elmt_size));

    bigio_count = H5_mpi_get_bigio_count();

    /* Allocate a selection iterator for iterating over the dataspace */
    if (NULL == (sel_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "couldn't allocate dataspace selection iterator");

    /* Initialize selection iterator */
    if (H5S_select_iter_init(sel_iter, space, elmt_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
    sel_iter_init = true; /* Selection iteration info has been initialized */

    /* Abbreviate args */
    diminfo = sel_iter->u.hyp.diminfo;
    assert(diminfo);

    /* Make a local copy of the dimension info so we can operate with them */

    /* Check if this is a "flattened" regular hyperslab selection */
    if (sel_iter->u.hyp.iter_rank != 0 && sel_iter->u.hyp.iter_rank < space->extent.rank) {
        /* Flattened selection */
        rank = sel_iter->u.hyp.iter_rank;
#ifdef H5S_DEBUG
        if (H5DEBUG(S))
            fprintf(H5DEBUG(S), "%s: Flattened selection\n", __func__);
#endif
        for (u = 0; u < rank; ++u) {
            H5_CHECK_OVERFLOW(diminfo[u].start, hsize_t, hssize_t);
            d[u].start = (hssize_t)diminfo[u].start + sel_iter->u.hyp.sel_off[u];
            d[u].strid = diminfo[u].stride;
            d[u].block = diminfo[u].block;
            d[u].count = diminfo[u].count;
            d[u].xtent = sel_iter->u.hyp.size[u];

#ifdef H5S_DEBUG
            if (H5DEBUG(S)) {
                fprintf(H5DEBUG(S),
                        "%s: start=%" PRIdHSIZE "  stride=%" PRIuHSIZE "  count=%" PRIuHSIZE
                        "  block=%" PRIuHSIZE "  xtent=%" PRIuHSIZE,
                        __func__, d[u].start, d[u].strid, d[u].count, d[u].block, d[u].xtent);
                if (u == 0)
                    fprintf(H5DEBUG(S), "  rank=%u\n", rank);
                else
                    fprintf(H5DEBUG(S), "\n");
            }
#endif

            /* Sanity check */
            assert(d[u].block > 0);
            assert(d[u].count > 0);
            assert(d[u].xtent > 0);
        } /* end for */
    }     /* end if */
    else {
        /* Non-flattened selection */
        rank = space->extent.rank;
#ifdef H5S_DEBUG
        if (H5DEBUG(S))
            fprintf(H5DEBUG(S), "%s: Non-flattened selection\n", __func__);
#endif
        for (u = 0; u < rank; ++u) {
            H5_CHECK_OVERFLOW(diminfo[u].start, hsize_t, hssize_t);
            d[u].start = (hssize_t)diminfo[u].start + space->select.offset[u];
            d[u].strid = diminfo[u].stride;
            d[u].block = diminfo[u].block;
            d[u].count = diminfo[u].count;
            d[u].xtent = space->extent.size[u];

#ifdef H5S_DEBUG
            if (H5DEBUG(S)) {
                fprintf(H5DEBUG(S),
                        "%s: start=%" PRIdHSIZE "  stride=%" PRIuHSIZE "  count=%" PRIuHSIZE
                        "  block=%" PRIuHSIZE "  xtent=%" PRIuHSIZE,
                        __func__, d[u].start, d[u].strid, d[u].count, d[u].block, d[u].xtent);
                if (u == 0)
                    fprintf(H5DEBUG(S), "  rank=%u\n", rank);
                else
                    fprintf(H5DEBUG(S), "\n");
            }
#endif

            /* Sanity check */
            assert(d[u].block > 0);
            assert(d[u].count > 0);
            assert(d[u].xtent > 0);
        } /* end for */
    }     /* end else */

    /**********************************************************************
        Compute array "offset[rank]" which gives the offsets for a multi-
        dimensional array with dimensions "d[i].xtent" (i=0,1,...,rank-1).
    **********************************************************************/
    offset[rank - 1]    = 1;
    max_xtent[rank - 1] = d[rank - 1].xtent;
#ifdef H5S_DEBUG
    if (H5DEBUG(S)) {
        i = ((int)rank) - 1;
        fprintf(H5DEBUG(S), " offset[%2d]=%" PRIuHSIZE "; max_xtent[%2d]=%" PRIuHSIZE "\n", i, offset[i], i,
                max_xtent[i]);
    }
#endif
    for (i = ((int)rank) - 2; i >= 0; --i) {
        offset[i]    = offset[i + 1] * d[i + 1].xtent;
        max_xtent[i] = max_xtent[i + 1] * d[i].xtent;
#ifdef H5S_DEBUG
        if (H5DEBUG(S))
            fprintf(H5DEBUG(S), " offset[%2d]=%" PRIuHSIZE "; max_xtent[%2d]=%" PRIuHSIZE "\n", i, offset[i],
                    i, max_xtent[i]);
#endif
    } /* end for */

    /*  Create a type covering the selected hyperslab.
     *  Multidimensional dataspaces are stored in row-major order.
     *  The type is built from the inside out, going from the
     *  fastest-changing (i.e., inner) dimension * to the slowest (outer).
     */

/*******************************************************
 *  Construct contig type for inner contig dims:
 *******************************************************/
#ifdef H5S_DEBUG
    if (H5DEBUG(S)) {
        fprintf(H5DEBUG(S), "%s: Making contig type %zu MPI_BYTEs\n", __func__, elmt_size);
        for (i = ((int)rank) - 1; i >= 0; --i)
            fprintf(H5DEBUG(S), "d[%d].xtent=%" PRIuHSIZE "\n", i, d[i].xtent);
    }
#endif

    /* LARGE_DATATYPE::
     * Check if the number of elements to form the inner type fits into a 32 bit integer.
     * If yes then just create the innertype with MPI_Type_contiguous.
     * Otherwise create a compound datatype by iterating as many times as needed
     * for the innertype to be created.
     */
    if (bigio_count >= elmt_size) {
        /* Use a single MPI datatype that has a 32 bit size */
        if (MPI_SUCCESS != (mpi_code = MPI_Type_contiguous((int)elmt_size, MPI_BYTE, &inner_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)
    }
    else
        /* Create the compound datatype for this operation (> 2GB) */
        if (H5_mpio_create_large_type(elmt_size, 0, MPI_BYTE, &inner_type) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                        "couldn't create a large inner datatype in hyper selection");

    /*******************************************************
     *  Construct the type by walking the hyperslab dims
     *  from the inside out:
     *******************************************************/
    for (i = ((int)rank) - 1; i >= 0; --i) {
#ifdef H5S_DEBUG
        if (H5DEBUG(S))
            fprintf(H5DEBUG(S),
                    "%s: Dimension i=%d \n"
                    "start=%" PRIdHSIZE " count=%" PRIuHSIZE " block=%" PRIuHSIZE " stride=%" PRIuHSIZE
                    ", xtent=%" PRIuHSIZE " max_xtent=%" PRIuHSIZE "\n",
                    __func__, i, d[i].start, d[i].count, d[i].block, d[i].strid, d[i].xtent, max_xtent[i]);
#endif

#ifdef H5S_DEBUG
        if (H5DEBUG(S))
            fprintf(H5DEBUG(S), "%s: i=%d  Making vector-type \n", __func__, i);
#endif
        /****************************************
         * Build vector type of the selection.
         ****************************************/
        if (bigio_count >= d[i].count && bigio_count >= d[i].block && bigio_count >= d[i].strid) {
            /* All the parameters fit into 32 bit integers so create the vector type normally */
            mpi_code = MPI_Type_vector((int)(d[i].count), /* count */
                                       (int)(d[i].block), /* blocklength */
                                       (int)(d[i].strid), /* stride */
                                       inner_type,        /* old type */
                                       &outer_type);      /* new type */

            MPI_Type_free(&inner_type);
            if (mpi_code != MPI_SUCCESS)
                HMPI_GOTO_ERROR(FAIL, "couldn't create MPI vector type", mpi_code)
        }
        else {
            /* Things get a bit more complicated and require LARGE_DATATYPE processing
             * There are two MPI datatypes that need to be created:
             *   1) an internal contiguous block; and
             *   2) a collection of elements where an element is a contiguous block(1).
             * Remember that the input arguments to the MPI-IO functions use integer
             * values to represent element counts.  We ARE allowed however, in the
             * more recent MPI implementations to use constructed datatypes whereby
             * the total number of bytes in a transfer could be :
             *   (2GB-1)number_of_blocks * the_datatype_extent.
             */

            MPI_Aint     stride_in_bytes, inner_extent;
            MPI_Datatype block_type;

            /* Create a contiguous datatype inner_type x number of BLOCKS.
             * Again we need to check that the number of BLOCKS can fit into
             * a 32 bit integer */
            if (bigio_count < d[i].block) {
                if (H5_mpio_create_large_type(d[i].block, 0, inner_type, &block_type) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                "couldn't create a large block datatype in hyper selection");
            }
            else if (MPI_SUCCESS !=
                     (mpi_code = MPI_Type_contiguous((int)d[i].block, inner_type, &block_type)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)

            /* As of version 4.0, OpenMPI now turns off MPI-1 API calls by default,
             * so we're using the MPI-2 version even though we don't need the lb
             * value.
             */
            {
                MPI_Aint unused_lb_arg;
                MPI_Type_get_extent(inner_type, &unused_lb_arg, &inner_extent);
            }
            stride_in_bytes = inner_extent * (MPI_Aint)d[i].strid;

            /* If the element count is larger than what a 32 bit integer can hold,
             * we call the large type creation function to handle that
             */
            if (bigio_count < d[i].count) {
                if (H5_mpio_create_large_type(d[i].count, stride_in_bytes, block_type, &outer_type) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                "couldn't create a large outer datatype in hyper selection");
            }
            /* otherwise a regular create_hvector will do */
            else if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hvector((int)d[i].count, /* count */
                                                                        1,               /* blocklength */
                                                                        stride_in_bytes, /* stride in bytes*/
                                                                        block_type,      /* old type */
                                                                        &outer_type)))   /* new type */
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hvector failed", mpi_code)

            MPI_Type_free(&block_type);
            MPI_Type_free(&inner_type);
        } /* end else */

        /****************************************
         *  Then build the dimension type as (start, vector type, xtent).
         ****************************************/

        /* Calculate start and extent values of this dimension */
        /* Check if value overflow to cast to type MPI_Aint */
        if (d[i].start > LONG_MAX || offset[i] > LONG_MAX || elmt_size > LONG_MAX)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "result overflow");
        start_disp = (MPI_Aint)d[i].start * (MPI_Aint)offset[i] * (MPI_Aint)elmt_size;

        if (max_xtent[i] > LONG_MAX)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "max_xtent overflow");
        new_extent = (MPI_Aint)elmt_size * (MPI_Aint)max_xtent[i];
        if (MPI_SUCCESS != (mpi_code = MPI_Type_get_extent(outer_type, &lb, &extent_len)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_get_extent failed", mpi_code)

        /*************************************************
         *  Restructure this datatype ("outer_type")
         *  so that it still starts at 0, but its extent
         *  is the full extent in this dimension.
         *************************************************/
        if (start_disp > 0 || extent_len < new_extent) {
            MPI_Datatype interm_type;
            int          block_len = 1;

            assert(0 == lb);

            mpi_code = MPI_Type_create_hindexed(1, &block_len, &start_disp, outer_type, &interm_type);
            MPI_Type_free(&outer_type);
            if (mpi_code != MPI_SUCCESS)
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed failed", mpi_code)

            mpi_code = MPI_Type_create_resized(interm_type, lb, new_extent, &inner_type);
            MPI_Type_free(&interm_type);
            if (mpi_code != MPI_SUCCESS)
                HMPI_GOTO_ERROR(FAIL, "couldn't resize MPI vector type", mpi_code)
        } /* end if */
        else
            inner_type = outer_type;
    } /* end for */
      /******************************************
       *  End of loop, walking through dimensions.
       *******************************************/

    /* At this point inner_type is actually the outermost type, even for 0-trip loop */
    *new_type = inner_type;
    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(new_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

    /* fill in the remaining return values */
    *count           = 1; /* only have to move one of these suckers! */
    *is_derived_type = true;

done:
    /* Release selection iterator */
    if (sel_iter) {
        if (sel_iter_init && H5S_SELECT_ITER_RELEASE(sel_iter) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
        sel_iter = H5FL_FREE(H5S_sel_iter_t, sel_iter);
    }

#ifdef H5S_DEBUG
    if (H5DEBUG(S))
        fprintf(H5DEBUG(S), "Leave %s, count=%d  is_derived_type=%s\n", __func__, *count,
                (*is_derived_type) ? "TRUE" : "FALSE");
#endif
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__mpio_reg_hyper_type() */

/*-------------------------------------------------------------------------
 * Function:	H5S__mpio_span_hyper_type
 *
 * Purpose:	Translate an HDF5 irregular hyperslab selection into an
                MPI type.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *		*count		  how many objects of the new_type in selection
 *				  (useful if this is the buffer type for xfer)
 *		*is_derived_type  0 if MPI primitive type, 1 if derived
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__mpio_span_hyper_type(const H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count,
                          bool *is_derived_type)
{
    H5S_mpio_mpitype_list_t type_list;                    /* List to track MPI data types created */
    MPI_Datatype            elmt_type;                    /* MPI datatype for an element */
    bool                    elmt_type_is_derived = false; /* Whether the element type has been created */
    MPI_Datatype            span_type;                    /* MPI datatype for overall span tree */
    hsize_t                 bigio_count;                  /* Transition point to create derived type */
    hsize_t                 down[H5S_MAX_RANK];           /* 'down' sizes for each dimension */
    uint64_t                op_gen;                       /* Operation generation value */
    int                     mpi_code;                     /* MPI return code */
    herr_t                  ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(space);
    assert(space->extent.size);
    assert(space->select.sel_info.hslab->span_lst);
    assert(space->select.sel_info.hslab->span_lst->head);

    bigio_count = H5_mpi_get_bigio_count();
    /* Create the base type for an element */
    if (bigio_count >= elmt_size) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_contiguous((int)elmt_size, MPI_BYTE, &elmt_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)
    }
    else if (H5_mpio_create_large_type(elmt_size, 0, MPI_BYTE, &elmt_type) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                    "couldn't create a large element datatype in span_hyper selection");
    elmt_type_is_derived = true;

    /* Compute 'down' sizes for each dimension */
    H5VM_array_down(space->extent.rank, space->extent.size, down);

    /* Acquire an operation generation value for creating MPI datatypes */
    op_gen = H5S__hyper_get_op_gen();

    /* Obtain derived MPI data type */
    /* Always use op_info[0] since we own this op_info, so there can be no
     * simultaneous operations */
    type_list.head = type_list.tail = NULL;
    if (H5S__obtain_datatype(space->select.sel_info.hslab->span_lst, down, elmt_size, &elmt_type, &span_type,
                             &type_list, 0, op_gen) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't obtain MPI derived data type");
    if (MPI_SUCCESS != (mpi_code = MPI_Type_dup(span_type, new_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)
    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(new_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

    /* Release MPI data types generated during span tree traversal */
    if (H5S__release_datatype(&type_list) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "couldn't release MPI derived data type");

    /* fill in the remaining return values */
    *count           = 1;
    *is_derived_type = true;

done:
    /* Release resources */
    if (elmt_type_is_derived)
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&elmt_type)))
            HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__mpio_span_hyper_type() */

/*-------------------------------------------------------------------------
 * Function:	H5S__release_datatype
 *
 * Purpose:	Release the MPI derived datatypes for span-tree hyperslab selection
 *
 * Return:	Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__release_datatype(H5S_mpio_mpitype_list_t *type_list)
{
    H5S_mpio_mpitype_node_t *curr;                /* Pointer to head of list */
    herr_t                   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(type_list);

    /* Iterate over the list, freeing the MPI data types */
    curr = type_list->head;
    while (curr) {
        H5S_mpio_mpitype_node_t *next;     /* Pointer to next node in list */
        int                      mpi_code; /* MPI return status code */

        /* Release the MPI data type for this span tree */
        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&curr->type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_free failed", mpi_code)

        /* Get pointer to next node in list */
        next = curr->next;

        /* Free the current node */
        curr = H5FL_FREE(H5S_mpio_mpitype_node_t, curr);

        /* Advance to next node */
        curr = next;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__release_datatype() */

/*-------------------------------------------------------------------------
 * Function:	H5S__obtain_datatype
 *
 * Purpose:	Obtain an MPI derived datatype for span-tree hyperslab selection
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*span_type	 the MPI type corresponding to the selection
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5S__obtain_datatype(H5S_hyper_span_info_t *spans, const hsize_t *down, size_t elmt_size,
                     const MPI_Datatype *elmt_type, MPI_Datatype *span_type,
                     H5S_mpio_mpitype_list_t *type_list, unsigned op_info_i, uint64_t op_gen)
{
    H5S_hyper_span_t *span;                  /* Hyperslab span to iterate with */
    hsize_t           bigio_count;           /* Transition point to create derived type */
    size_t            alloc_count       = 0; /* Number of span tree nodes allocated at this level */
    size_t            outercount        = 0; /* Number of span tree nodes at this level */
    MPI_Datatype     *inner_type        = NULL;
    bool              inner_types_freed = false; /* Whether the inner_type MPI datatypes have been freed */
    int              *blocklen          = NULL;
    MPI_Aint         *disp              = NULL;
    size_t            u;                   /* Local index variable */
    int               mpi_code;            /* MPI return status code */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(spans);
    assert(type_list);

    bigio_count = H5_mpi_get_bigio_count();
    /* Check if we've visited this span tree before */
    if (spans->op_info[op_info_i].op_gen != op_gen) {
        H5S_mpio_mpitype_node_t *type_node; /* Pointer to new node in MPI data type list */

        /* Allocate the initial displacement & block length buffers */
        alloc_count = H5S_MPIO_INITIAL_ALLOC_COUNT;
        if (NULL == (disp = (MPI_Aint *)H5MM_malloc(alloc_count * sizeof(MPI_Aint))))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of displacements");
        if (NULL == (blocklen = (int *)H5MM_malloc(alloc_count * sizeof(int))))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate array of block lengths");

        /* If this is the fastest changing dimension, it is the base case for derived datatype. */
        span = spans->head;
        if (NULL == span->down) {
            bool large_block = false; /* Whether the block length is larger than 32 bit integer */

            outercount = 0;
            while (span) {
                hsize_t nelmts; /* # of elements covered by current span */

                /* Check if we need to increase the size of the buffers */
                if (outercount >= alloc_count) {
                    MPI_Aint *tmp_disp;     /* Temporary pointer to new displacement buffer */
                    int      *tmp_blocklen; /* Temporary pointer to new block length buffer */

                    /* Double the allocation count */
                    alloc_count *= 2;

                    /* Re-allocate the buffers */
                    if (NULL == (tmp_disp = (MPI_Aint *)H5MM_realloc(disp, alloc_count * sizeof(MPI_Aint))))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL,
                                    "can't allocate array of displacements");
                    disp = tmp_disp;
                    if (NULL == (tmp_blocklen = (int *)H5MM_realloc(blocklen, alloc_count * sizeof(int))))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL,
                                    "can't allocate array of block lengths");
                    blocklen = tmp_blocklen;
                } /* end if */

                /* Compute the number of elements to attempt in this span */
                nelmts = (span->high - span->low) + 1;

                /* Store displacement & block length */
                disp[outercount] = (MPI_Aint)elmt_size * (MPI_Aint)span->low;
                H5_CHECK_OVERFLOW(nelmts, hsize_t, int);
                blocklen[outercount] = (int)nelmts;

                if (bigio_count < (hsize_t)blocklen[outercount])
                    large_block = true; /* at least one block type is large, so set this flag to true */

                span = span->next;
                outercount++;
            } /* end while */

            /* Everything fits into integers, so cast them and use hindexed */
            if (bigio_count >= outercount && large_block == false) {
                if (MPI_SUCCESS !=
                    (mpi_code = MPI_Type_create_hindexed((int)outercount, blocklen, disp, *elmt_type,
                                                         &spans->op_info[op_info_i].u.down_type)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed failed", mpi_code)
            }      /* end if */
            else { /* LARGE_DATATYPE:: Something doesn't fit into a 32 bit integer */
                for (u = 0; u < outercount; u++) {
                    MPI_Datatype temp_type = MPI_DATATYPE_NULL;

                    /* create the block type from elmt_type while checking the 32 bit int limit */
                    if ((hsize_t)(blocklen[u]) > bigio_count) {
                        if (H5_mpio_create_large_type((hsize_t)blocklen[u], 0, *elmt_type, &temp_type) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                        "couldn't create a large element datatype in span_hyper selection");
                    } /* end if */
                    else if (MPI_SUCCESS !=
                             (mpi_code = MPI_Type_contiguous((int)blocklen[u], *elmt_type, &temp_type)))
                        HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)

                    /* Combine the current datatype that is created with this current block type */
                    if (0 == u) /* first iteration, there is no combined datatype yet */
                        spans->op_info[op_info_i].u.down_type = temp_type;
                    else {
                        int          bl[2] = {1, 1};
                        MPI_Aint     ds[2] = {disp[u - 1], disp[u]};
                        MPI_Datatype dt[2] = {spans->op_info[op_info_i].u.down_type, temp_type};

                        if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct(
                                                2,                                        /* count */
                                                bl,                                       /* blocklength */
                                                ds,                                       /* stride in bytes*/
                                                dt,                                       /* old type */
                                                &spans->op_info[op_info_i].u.down_type))) /* new type */
                            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)

                        /* Release previous temporary datatype */
                        if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&temp_type)))
                            HMPI_GOTO_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
                    } /* end else */
                }     /* end for */
            }         /* end else (LARGE_DATATYPE::) */
        }             /* end if */
        else {
            MPI_Aint stride; /* Distance between inner MPI datatypes */

            if (NULL == (inner_type = (MPI_Datatype *)H5MM_malloc(alloc_count * sizeof(MPI_Datatype))))
                HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL,
                            "can't allocate array of inner MPI datatypes");

            /* Calculate the total bytes of the lower dimension */
            stride = (MPI_Aint)(*down) * (MPI_Aint)elmt_size;

            /* Loop over span nodes */
            outercount = 0;
            while (span) {
                MPI_Datatype down_type; /* Temporary MPI datatype for a span tree node's children */
                hsize_t      nelmts;    /* # of elements covered by current span */

                /* Check if we need to increase the size of the buffers */
                if (outercount >= alloc_count) {
                    MPI_Aint     *tmp_disp;       /* Temporary pointer to new displacement buffer */
                    int          *tmp_blocklen;   /* Temporary pointer to new block length buffer */
                    MPI_Datatype *tmp_inner_type; /* Temporary pointer to inner MPI datatype buffer */

                    /* Double the allocation count */
                    alloc_count *= 2;

                    /* Re-allocate the buffers */
                    if (NULL == (tmp_disp = (MPI_Aint *)H5MM_realloc(disp, alloc_count * sizeof(MPI_Aint))))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL,
                                    "can't allocate array of displacements");
                    disp = tmp_disp;
                    if (NULL == (tmp_blocklen = (int *)H5MM_realloc(blocklen, alloc_count * sizeof(int))))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL,
                                    "can't allocate array of block lengths");
                    blocklen = tmp_blocklen;
                    if (NULL == (tmp_inner_type = (MPI_Datatype *)H5MM_realloc(
                                     inner_type, alloc_count * sizeof(MPI_Datatype))))
                        HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL,
                                    "can't allocate array of inner MPI datatypes");
                    inner_type = tmp_inner_type;
                } /* end if */

                /* Displacement should be in byte and should have dimension information */
                /* First using MPI Type vector to build derived data type for this span only */
                /* Need to calculate the disp in byte for this dimension. */
                disp[outercount]     = (MPI_Aint)span->low * stride;
                blocklen[outercount] = 1;

                /* Generate MPI datatype for next dimension down */
                if (H5S__obtain_datatype(span->down, down + 1, elmt_size, elmt_type, &down_type, type_list,
                                         op_info_i, op_gen) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't obtain MPI derived data type");

                /* Compute the number of elements to attempt in this span */
                nelmts = (span->high - span->low) + 1;

                /* Build the MPI datatype for this node */
                H5_CHECK_OVERFLOW(nelmts, hsize_t, int);
                if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hvector((int)nelmts, 1, stride, down_type,
                                                                       &inner_type[outercount])))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hvector failed", mpi_code)

                span = span->next;
                outercount++;
            } /* end while */

            /* Building the whole vector datatype */
            H5_CHECK_OVERFLOW(outercount, size_t, int);
            if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct((int)outercount, blocklen, disp, inner_type,
                                                                  &spans->op_info[op_info_i].u.down_type)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)

            /* Release inner node types */
            for (u = 0; u < outercount; u++)
                if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&inner_type[u])))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
            inner_types_freed = true;
        } /* end else */

        /* Allocate space for the MPI data type list node */
        if (NULL == (type_node = H5FL_MALLOC(H5S_mpio_mpitype_node_t)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate MPI data type list node");

        /* Set up MPI type node */
        type_node->type = spans->op_info[op_info_i].u.down_type;
        type_node->next = NULL;

        /* Add MPI type node to list */
        if (type_list->head == NULL)
            type_list->head = type_list->tail = type_node;
        else {
            type_list->tail->next = type_node;
            type_list->tail       = type_node;
        } /* end else */

        /* Remember that we've visited this span tree */
        spans->op_info[op_info_i].op_gen = op_gen;
    } /* end else */

    /* Return MPI data type for span tree */
    *span_type = spans->op_info[op_info_i].u.down_type;

done:
    /* General cleanup */
    if (inner_type != NULL) {
        if (!inner_types_freed)
            for (u = 0; u < outercount; u++)
                if (MPI_SUCCESS != (mpi_code = MPI_Type_free(&inner_type[u])))
                    HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code)
        H5MM_free(inner_type);
    } /* end if */
    if (blocklen != NULL)
        H5MM_free(blocklen);
    if (disp != NULL)
        H5MM_free(disp);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S__obtain_datatype() */

/*-------------------------------------------------------------------------
 * Function:	H5S_mpio_space_type
 *
 * Purpose:	Translate an HDF5 dataspace selection into an MPI type.
 *		Currently handle only hyperslab and "all" selections.
 *
 * Return:	Non-negative on success, negative on failure.
 *
 * Outputs:	*new_type	  the MPI type corresponding to the selection
 *		*count		  how many objects of the new_type in selection
 *				  (useful if this is the buffer type for xfer)
 *		*is_derived_type  0 if MPI primitive type, 1 if derived
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5S_mpio_space_type(H5S_t *space, size_t elmt_size, MPI_Datatype *new_type, int *count, bool *is_derived_type,
                    bool do_permute, hsize_t **permute_map, bool *is_permuted)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(space);
    assert(elmt_size);

    /* Create MPI type based on the kind of selection */
    switch (H5S_GET_EXTENT_TYPE(space)) {
        case H5S_NULL:
        case H5S_SCALAR:
        case H5S_SIMPLE:
            /* If the file space has been permuted previously due to
             * out-of-order point selection, then permute this selection which
             * should be a memory selection to match the file space permutation.
             */
            if (true == *is_permuted) {
                switch (H5S_GET_SELECT_TYPE(space)) {
                    case H5S_SEL_NONE:
                        if (H5S__mpio_none_type(new_type, count, is_derived_type) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                        "couldn't convert 'none' selection to MPI type");
                        break;

                    case H5S_SEL_ALL:
                    case H5S_SEL_POINTS:
                    case H5S_SEL_HYPERSLABS:
                        /* Sanity check */
                        assert(!do_permute);

                        if (H5S__mpio_permute_type(space, elmt_size, permute_map, new_type, count,
                                                   is_derived_type) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                        "couldn't convert 'all' selection to MPI type");
                        break;

                    case H5S_SEL_ERROR:
                    case H5S_SEL_N:
                    default:
                        assert("unknown selection type" && 0);
                        break;
                } /* end switch */
            }     /* end if */
            /* the file space is not permuted, so do a regular selection */
            else {
                switch (H5S_GET_SELECT_TYPE(space)) {
                    case H5S_SEL_NONE:
                        if (H5S__mpio_none_type(new_type, count, is_derived_type) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                        "couldn't convert 'none' selection to MPI type");
                        break;

                    case H5S_SEL_ALL:
                        if (H5S__mpio_all_type(space, elmt_size, new_type, count, is_derived_type) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                        "couldn't convert 'all' selection to MPI type");
                        break;

                    case H5S_SEL_POINTS:
                        if (H5S__mpio_point_type(space, elmt_size, new_type, count, is_derived_type,
                                                 do_permute, permute_map, is_permuted) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                        "couldn't convert 'point' selection to MPI type");
                        break;

                    case H5S_SEL_HYPERSLABS:
                        if ((H5S_SELECT_IS_REGULAR(space) == true)) {
                            if (H5S__mpio_reg_hyper_type(space, elmt_size, new_type, count, is_derived_type) <
                                0)
                                HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                            "couldn't convert regular 'hyperslab' selection to MPI type");
                        } /* end if */
                        else if (H5S__mpio_span_hyper_type(space, elmt_size, new_type, count,
                                                           is_derived_type) < 0)
                            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL,
                                        "couldn't convert irregular 'hyperslab' selection to MPI type");
                        break;

                    case H5S_SEL_ERROR:
                    case H5S_SEL_N:
                    default:
                        assert("unknown selection type" && 0);
                        break;
                } /* end switch */
            }     /* end else */
            break;

        case H5S_NO_CLASS:
        default:
            assert("unknown dataspace type" && 0);
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5S_mpio_space_type() */

#endif /* H5_HAVE_PARALLEL */
