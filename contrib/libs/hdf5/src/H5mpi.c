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
 * Purpose:     Common MPI routines
 *
 */

#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5MMprivate.h" /* Memory Management                        */

#ifdef H5_HAVE_PARALLEL

/****************/
/* Local Macros */
/****************/
#define TWO_GIG_LIMIT INT32_MAX
#ifndef H5_MAX_MPI_COUNT
#define H5_MAX_MPI_COUNT (1 << 30)
#endif

/*******************/
/* Local Variables */
/*******************/
static hsize_t bigio_count_g = H5_MAX_MPI_COUNT;

/*-------------------------------------------------------------------------
 * Function:  H5_mpi_set_bigio_count
 *
 * Purpose:   Allow us to programmatically change the switch point
 *            when we utilize derived datatypes.  This is of
 *            particular interest for allowing nightly testing
 *
 * Return:    The current/previous value of bigio_count_g.
 *
 *-------------------------------------------------------------------------
 */
hsize_t
H5_mpi_set_bigio_count(hsize_t new_count)
{
    hsize_t orig_count = bigio_count_g;

    if ((new_count > 0) && (new_count < (hsize_t)TWO_GIG_LIMIT)) {
        bigio_count_g = new_count;
    }
    return orig_count;
} /* end H5_mpi_set_bigio_count() */

/*-------------------------------------------------------------------------
 * Function:  H5_mpi_get_bigio_count
 *
 * Purpose:   Allow other HDF5 library functions to access
 *            the current value for bigio_count_g.
 *
 * Return:    The current/previous value of bigio_count_g.
 *
 *-------------------------------------------------------------------------
 */
H5_ATTR_PURE hsize_t
H5_mpi_get_bigio_count(void)
{
    return bigio_count_g;
}

/*-------------------------------------------------------------------------
 * Function:    H5_mpi_comm_dup
 *
 * Purpose:     Duplicate an MPI communicator.
 *
 *              Does not duplicate MPI_COMM_NULL. Instead, comm_new will
 *              be set to MPI_COMM_NULL directly.
 *
 *              The new communicator is returned via the comm_new pointer.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpi_comm_dup(MPI_Comm comm, MPI_Comm *comm_new)
{
    herr_t   ret_value = SUCCEED;
    MPI_Comm comm_dup  = MPI_COMM_NULL;
    int      mpi_code;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    if (!comm_new)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADVALUE, FAIL, "comm_new cannot be NULL");

    /* Handle MPI_COMM_NULL separately */
    if (MPI_COMM_NULL == comm) {
        /* Don't duplicate MPI_COMM_NULL since that's an error in MPI */
        comm_dup = MPI_COMM_NULL;
    }
    else {

        /* Duplicate the MPI communicator */
        if (MPI_SUCCESS != (mpi_code = MPI_Comm_dup(comm, &comm_dup)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Comm_dup failed", mpi_code)

        /* Set MPI_ERRORS_RETURN on comm_dup so that MPI failures are not fatal,
         * and return codes can be checked and handled.
         */
        if (MPI_SUCCESS != (mpi_code = MPI_Comm_set_errhandler(comm_dup, MPI_ERRORS_RETURN)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Errhandler_set failed", mpi_code)
    }

    /* Copy the new communicator to the return argument */
    *comm_new = comm_dup;

done:
    if (FAIL == ret_value) {
        /* need to free anything created here */
        if (MPI_COMM_NULL != comm_dup)
            MPI_Comm_free(&comm_dup);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpi_comm_dup() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpi_info_dup
 *
 * Purpose:     Duplicate an MPI info.
 *
 *              If the info object is MPI_INFO_NULL, no duplicate
 *              is made but the same value assigned to the new info object
 *              handle.
 *
 *              The new info is returned via the info_new pointer.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpi_info_dup(MPI_Info info, MPI_Info *info_new)
{
    herr_t   ret_value = SUCCEED;
    MPI_Info info_dup  = MPI_INFO_NULL;
    int      mpi_code;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    if (!info_new)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADVALUE, FAIL, "info_new cannot be NULL");

    /* Duplicate the MPI info */
    if (info == MPI_INFO_NULL) {
        /* Don't duplicate MPI_INFO_NULL. Just copy it. */
        info_dup = MPI_INFO_NULL;
    }
    else {
        /* Duplicate the info */
        if (MPI_SUCCESS != (mpi_code = MPI_Info_dup(info, &info_dup)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Info_dup failed", mpi_code)
    }

    /* Copy the new info to the return argument */
    *info_new = info_dup;

done:
    if (FAIL == ret_value) {
        /* need to free anything created here */
        if (MPI_INFO_NULL != info_dup)
            MPI_Info_free(&info_dup);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpi_info_dup() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpi_comm_free
 *
 * Purpose:     Free an MPI communicator.
 *
 *              If comm is MPI_COMM_NULL this call does nothing.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpi_comm_free(MPI_Comm *comm)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    if (!comm)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADVALUE, FAIL, "comm pointer cannot be NULL");

    /* Free the communicator */
    if (MPI_COMM_WORLD != *comm && MPI_COMM_NULL != *comm)
        MPI_Comm_free(comm);

    *comm = MPI_COMM_NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* End H5_mpi_comm_free() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpi_info_free
 *
 * Purpose:     Free the MPI info.
 *
 *              If info is MPI_INFO_NULL this call does nothing.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpi_info_free(MPI_Info *info)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    if (!info)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADVALUE, FAIL, "info pointer cannot be NULL");

    /* Free the info */
    if (MPI_INFO_NULL != *info)
        MPI_Info_free(info);

    *info = MPI_INFO_NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* End H5_mpi_info_free() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpi_comm_cmp
 *
 * Purpose:     Compares two MPI communicators.
 *
 *              Note that passing MPI_COMM_NULL to this function will not
 *              throw errors, unlike MPI_Comm_compare().
 *
 *              We consider MPI communicators to be the "same" when the
 *              groups are identical. We don't care about the context
 *              since that will always be different as we call MPI_Comm_dup
 *              when we store the communicator in the fapl.
 *
 *              The out parameter is a value like strcmp. The value is
 *              undefined when the return value is FAIL.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpi_comm_cmp(MPI_Comm comm1, MPI_Comm comm2, int *result)
{
    int    mpi_code;
    int    mpi_result = MPI_IDENT;
    herr_t ret_value  = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    if (!result)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADVALUE, FAIL, "result cannot be NULL");

    /* Set out parameter to something reasonable in case something goes wrong */
    *result = 0;

    /* Can't pass MPI_COMM_NULL to MPI_Comm_compare() so we have to handle
     * it in special cases.
     *
     * MPI_Comm can either be an integer type or a pointer. We cast them
     * to intptr_t so we can compare them with < and > when needed.
     */
    if (MPI_COMM_NULL == comm1 && MPI_COMM_NULL == comm2) {
        /* Special case of both communicators being MPI_COMM_NULL */
        *result = 0;
    }
    else if (MPI_COMM_NULL == comm1 || MPI_COMM_NULL == comm2) {

        /* Special case of one communicator being MPI_COMM_NULL */
        *result = (intptr_t)comm1 < (intptr_t)comm2 ? -1 : 1;
    }
    else {

        /* Normal communicator compare */

        /* Compare the MPI communicators */
        if (MPI_SUCCESS != (mpi_code = MPI_Comm_compare(comm1, comm2, &mpi_result)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Comm_compare failed", mpi_code)

        /* Set the result */
        if (MPI_IDENT == mpi_result || MPI_CONGRUENT == mpi_result)
            *result = 0;
        else
            *result = (intptr_t)comm1 < (intptr_t)comm2 ? -1 : 1;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpi_comm_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpi_info_cmp
 *
 * Purpose:     Compares two MPI info objects.
 *
 *              For our purposes, two mpi info objects are the "same" if
 *              they contain the same key-value pairs or are both
 *              MPI_INFO_NULL.
 *
 *              The out parameter is a value like strcmp. The value is
 *              undefined when the return value is FAIL.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpi_info_cmp(MPI_Info info1, MPI_Info info2, int *result)
{
    bool   same      = false;
    char  *key       = NULL;
    char  *value1    = NULL;
    char  *value2    = NULL;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    if (!result)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADVALUE, FAIL, "result cannot be NULL");

    /* Check for MPI_INFO_NULL */
    if (MPI_INFO_NULL == info1 && MPI_INFO_NULL == info2) {
        /* Special case of both info objects being MPI_INFO_NULL */
        same = true;
    }
    else if (MPI_INFO_NULL == info1 || MPI_INFO_NULL == info2) {

        /* Special case of one info object being MPI_INFO_NULL */
        same = false;
    }
    else {
        int mpi_code;
        int nkeys_1;
        int nkeys_2;

        /* Check if the number of keys is the same */
        if (MPI_SUCCESS != (mpi_code = MPI_Info_get_nkeys(info1, &nkeys_1)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Info_get_nkeys failed", mpi_code)
        if (MPI_SUCCESS != (mpi_code = MPI_Info_get_nkeys(info2, &nkeys_2)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Info_get_nkeys failed", mpi_code)

        if (nkeys_1 != nkeys_2)
            same = false;
        else if (0 == nkeys_1 && 0 == nkeys_2)
            same = true;
        else {
            int i;
            int flag1 = -1;
            int flag2 = -1;

            /* Allocate buffers for iteration */
            if (NULL == (key = (char *)H5MM_malloc(MPI_MAX_INFO_KEY * sizeof(char))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
            if (NULL == (value1 = (char *)H5MM_malloc(MPI_MAX_INFO_VAL * sizeof(char))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
            if (NULL == (value2 = (char *)H5MM_malloc(MPI_MAX_INFO_VAL * sizeof(char))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

            /* Iterate over the keys, comparing them */
            for (i = 0; i < nkeys_1; i++) {

                same = true;

                /* Memset the buffers to zero */
                memset(key, 0, MPI_MAX_INFO_KEY);
                memset(value1, 0, MPI_MAX_INFO_VAL);
                memset(value2, 0, MPI_MAX_INFO_VAL);

                /* Get the nth key */
                if (MPI_SUCCESS != (mpi_code = MPI_Info_get_nthkey(info1, i, key)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Info_get_nthkey failed", mpi_code)

                /* Get the values */
                if (MPI_SUCCESS != (mpi_code = MPI_Info_get(info1, key, MPI_MAX_INFO_VAL, value1, &flag1)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Info_get failed", mpi_code)
                if (MPI_SUCCESS != (mpi_code = MPI_Info_get(info2, key, MPI_MAX_INFO_VAL, value2, &flag2)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Info_get failed", mpi_code)

                /* Compare values and flags */
                if (!flag1 || !flag2 || memcmp(value1, value2, MPI_MAX_INFO_VAL)) {
                    same = false;
                    break;
                }

            } /* end for */
        }     /* end else */
    }         /* end else */

    /* Set the output value
     *
     * MPI_Info can either be an integer type or a pointer. We cast them
     * to intptr_t so we can compare them with < and > when needed.
     */
    if (same)
        *result = 0;
    else
        *result = (intptr_t)info1 < (intptr_t)info2 ? -1 : 1;

done:
    if (key)
        H5MM_xfree(key);
    if (value1)
        H5MM_xfree(value1);
    if (value2)
        H5MM_xfree(value2);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpi_info_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpio_create_large_type
 *
 * Purpose:     Create a large datatype of size larger than what a 32 bit integer
 *              can hold.
 *
 * Return:      Non-negative on success, negative on failure.
 *
 *              *new_type    the new datatype created
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpio_create_large_type(hsize_t num_elements, MPI_Aint stride_bytes, MPI_Datatype old_type,
                          MPI_Datatype *new_type)
{
    int          num_big_types;   /* num times the 2G datatype will be repeated */
    int          remaining_bytes; /* the number of bytes left that can be held in an int value */
    hsize_t      leftover;
    int          block_len[2];
    int          mpi_code; /* MPI return code */
    MPI_Datatype inner_type, outer_type, leftover_type, type[2];
    MPI_Aint     disp[2], old_extent;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Calculate how many Big MPI datatypes are needed to represent the buffer */
    num_big_types = (int)(num_elements / bigio_count_g);
    leftover      = (hsize_t)num_elements - (hsize_t)num_big_types * bigio_count_g;
    H5_CHECKED_ASSIGN(remaining_bytes, int, leftover, hsize_t);

    /* Create a contiguous datatype of size equal to the largest
     * number that a 32 bit integer can hold x size of old type.
     * If the displacement is 0, then the type is contiguous, otherwise
     * use type_hvector to create the type with the displacement provided
     */
    if (0 == stride_bytes) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_contiguous((int)bigio_count_g, old_type, &inner_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)
    } /* end if */
    else if (MPI_SUCCESS !=
             (mpi_code = MPI_Type_create_hvector((int)bigio_count_g, 1, stride_bytes, old_type, &inner_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hvector failed", mpi_code)

    /* Create a contiguous datatype of the buffer (minus the remaining < 2GB part)
     * If a stride is present, use hvector type
     */
    if (0 == stride_bytes) {
        if (MPI_SUCCESS != (mpi_code = MPI_Type_contiguous(num_big_types, inner_type, &outer_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)
    } /* end if */
    else if (MPI_SUCCESS !=
             (mpi_code = MPI_Type_create_hvector(num_big_types, 1, stride_bytes, inner_type, &outer_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hvector failed", mpi_code)

    MPI_Type_free(&inner_type);

    /* If there is a remaining part create a contiguous/vector datatype and then
     * use a struct datatype to encapsulate everything.
     */
    if (remaining_bytes) {
        if (stride_bytes == 0) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_contiguous(remaining_bytes, old_type, &leftover_type)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_contiguous failed", mpi_code)
        } /* end if */
        else if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hvector(
                                     (int)(num_elements - (hsize_t)num_big_types * bigio_count_g), 1,
                                     stride_bytes, old_type, &leftover_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hvector failed", mpi_code)

        /* As of version 4.0, OpenMPI now turns off MPI-1 API calls by default,
         * so we're using the MPI-2 version even though we don't need the lb
         * value.
         */
        {
            MPI_Aint unused_lb_arg;
            MPI_Type_get_extent(old_type, &unused_lb_arg, &old_extent);
        }

        /* Set up the arguments for MPI_Type_create_struct() */
        type[0]      = outer_type;
        type[1]      = leftover_type;
        block_len[0] = 1;
        block_len[1] = 1;
        disp[0]      = 0;
        disp[1]      = (old_extent + stride_bytes) * num_big_types * (MPI_Aint)bigio_count_g;

        if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct(2, block_len, disp, type, new_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code)

        MPI_Type_free(&outer_type);
        MPI_Type_free(&leftover_type);
    } /* end if */
    else
        /* There are no remaining bytes so just set the new type to
         * the outer type created */
        *new_type = outer_type;

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(new_type)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code)

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpio_create_large_type() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpio_gatherv_alloc
 *
 * Purpose:     A wrapper around MPI_(All)gatherv that performs allocation
 *              of the receive buffer on the caller's behalf. This
 *              routine's parameters are as follows:
 *
 *              `send_buf` - The buffer that data will be sent from for
 *                           the calling MPI rank. Analogous to
 *                           MPI_(All)gatherv's `sendbuf` parameter.
 *
 *              `send_count` - The number of `send_type` elements in the
 *                             send buffer. Analogous to MPI_(All)gatherv's
 *                             `sendcount` parameter.
 *
 *              `send_type` - The MPI Datatype of the elements in the send
 *                            buffer. Analogous to MPI_(All)gatherv's
 *                            `sendtype` parameter.
 *
 *              `recv_counts` - An array containing the number of elements
 *                              to be received from each MPI rank.
 *                              Analogous to MPI_(All)gatherv's `recvcount`
 *                              parameter.
 *
 *              `displacements` - An array containing the displacements
 *                                in the receive buffer where data from
 *                                each MPI rank should be placed. Analogous
 *                                to MPI_(All)gatherv's `displs` parameter.
 *
 *              `recv_type` - The MPI Datatype of the elements in the
 *                            receive buffer. Analogous to
 *                            MPI_(All)gatherv's `recvtype` parameter.
 *
 *              `allgather` - Specifies whether the gather operation to be
 *                            performed should be MPI_Allgatherv (true) or
 *                            MPI_Gatherv (false).
 *
 *              `root` - For MPI_Gatherv operations, specifies the rank
 *                       that will receive the data sent by other ranks.
 *                       Analogous to MPI_Gatherv's `root` parameter. For
 *                       MPI_Allgatherv operations, this parameter is
 *                       ignored.
 *
 *              `comm` - Specifies the MPI Communicator for the operation.
 *                       Analogous to MPI_(All)gatherv's `comm` parameter.
 *
 *              `mpi_rank` - Specifies the calling rank's rank value, as
 *                           obtained by calling MPI_Comm_rank on the
 *                           MPI Communicator `comm`.
 *
 *              `mpi_size` - Specifies the MPI Communicator size, as
 *                           obtained by calling MPI_Comm_size on the
 *                           MPI Communicator `comm`.
 *
 *              `out_buf` - Resulting buffer that is allocated and
 *                          returned to the caller after data has been
 *                          gathered into it. Returned only to the rank
 *                          specified by `root` for MPI_Gatherv
 *                          operations, or to all ranks for
 *                          MPI_Allgatherv operations.
 *
 *              `out_buf_num_entries` - The number of elements in the
 *                                      resulting buffer, in terms of
 *                                      the MPI Datatype provided for
 *                                      `recv_type`.
 *
 * Notes:       This routine is collective across `comm`.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpio_gatherv_alloc(void *send_buf, int send_count, MPI_Datatype send_type, const int recv_counts[],
                      const int displacements[], MPI_Datatype recv_type, bool allgather, int root,
                      MPI_Comm comm, int mpi_rank, int mpi_size, void **out_buf, size_t *out_buf_num_entries)
{
    size_t recv_buf_num_entries = 0;
    void  *recv_buf             = NULL;
#if H5_CHECK_MPI_VERSION(3, 0)
    MPI_Count type_lb;
    MPI_Count type_extent;
#else
    MPI_Aint type_lb;
    MPI_Aint type_extent;
#endif
    int    mpi_code;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(send_buf || send_count == 0);
    if (allgather || (mpi_rank == root))
        assert(out_buf && out_buf_num_entries);

        /* Retrieve the extent of the MPI Datatype being used */
#if H5_CHECK_MPI_VERSION(3, 0)
    if (MPI_SUCCESS != (mpi_code = MPI_Type_get_extent_x(recv_type, &type_lb, &type_extent)))
#else
    if (MPI_SUCCESS != (mpi_code = MPI_Type_get_extent(recv_type, &type_lb, &type_extent)))
#endif
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_get_extent(_x) failed", mpi_code)

    if (type_extent < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "MPI recv_type had a negative extent");

    /*
     * Calculate the total size of the buffer being
     * returned and allocate it
     */
    if (allgather || (mpi_rank == root)) {
        size_t i;
        size_t buf_size;

        for (i = 0, recv_buf_num_entries = 0; i < (size_t)mpi_size; i++)
            recv_buf_num_entries += (size_t)recv_counts[i];
        buf_size = recv_buf_num_entries * (size_t)type_extent;

        /* If our buffer size is 0, there's nothing to do */
        if (buf_size == 0)
            HGOTO_DONE(SUCCEED);

        if (NULL == (recv_buf = H5MM_malloc(buf_size)))
            /* Push an error, but still participate in collective gather operation */
            HDONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "couldn't allocate receive buffer");
    }

    /* Perform gather operation */
    if (allgather) {
        if (MPI_SUCCESS != (mpi_code = MPI_Allgatherv(send_buf, send_count, send_type, recv_buf, recv_counts,
                                                      displacements, recv_type, comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Allgatherv failed", mpi_code)
    }
    else {
        if (MPI_SUCCESS != (mpi_code = MPI_Gatherv(send_buf, send_count, send_type, recv_buf, recv_counts,
                                                   displacements, recv_type, root, comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Gatherv failed", mpi_code)
    }

    if (allgather || (mpi_rank == root)) {
        *out_buf             = recv_buf;
        *out_buf_num_entries = recv_buf_num_entries;
    }

done:
    if (ret_value < 0) {
        if (recv_buf)
            H5MM_free(recv_buf);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpio_gatherv_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpio_gatherv_alloc_simple
 *
 * Purpose:     A slightly simplified interface to H5_mpio_gatherv_alloc
 *              which calculates the receive counts and receive buffer
 *              displacements for the caller.
 *
 * Notes:       This routine is collective across `comm`.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpio_gatherv_alloc_simple(void *send_buf, int send_count, MPI_Datatype send_type, MPI_Datatype recv_type,
                             bool allgather, int root, MPI_Comm comm, int mpi_rank, int mpi_size,
                             void **out_buf, size_t *out_buf_num_entries)
{
    int   *recv_counts_disps_array = NULL;
    int    mpi_code;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(send_buf || send_count == 0);
    if (allgather || (mpi_rank == root))
        assert(out_buf && out_buf_num_entries);

    /*
     * Allocate array to store the receive counts of each rank, as well as
     * the displacements into the final array where each rank will place
     * their data. The first half of the array contains the receive counts
     * (in rank order), while the latter half contains the displacements
     * (also in rank order).
     */
    if (allgather || (mpi_rank == root)) {
        if (NULL ==
            (recv_counts_disps_array = H5MM_malloc(2 * (size_t)mpi_size * sizeof(*recv_counts_disps_array))))
            /* Push an error, but still participate in collective gather operation */
            HDONE_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                        "couldn't allocate receive counts and displacements array");
    }

    /* Collect each rank's send count to interested ranks */
    if (allgather) {
        if (MPI_SUCCESS !=
            (mpi_code = MPI_Allgather(&send_count, 1, MPI_INT, recv_counts_disps_array, 1, MPI_INT, comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Allgather failed", mpi_code)
    }
    else {
        if (MPI_SUCCESS !=
            (mpi_code = MPI_Gather(&send_count, 1, MPI_INT, recv_counts_disps_array, 1, MPI_INT, root, comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Gather failed", mpi_code)
    }

    /* Set the displacements into the receive buffer for the gather operation */
    if (allgather || (mpi_rank == root)) {
        size_t i;
        int   *displacements_ptr;

        displacements_ptr = &recv_counts_disps_array[mpi_size];

        *displacements_ptr = 0;
        for (i = 1; i < (size_t)mpi_size; i++)
            displacements_ptr[i] = displacements_ptr[i - 1] + recv_counts_disps_array[i - 1];
    }

    /* Perform gather operation */
    if (H5_mpio_gatherv_alloc(send_buf, send_count, send_type, recv_counts_disps_array,
                              &recv_counts_disps_array[mpi_size], recv_type, allgather, root, comm, mpi_rank,
                              mpi_size, out_buf, out_buf_num_entries) < 0)
        HGOTO_ERROR(H5E_LIB, H5E_CANTGATHER, FAIL, "can't gather data");

done:
    if (recv_counts_disps_array)
        H5MM_free(recv_counts_disps_array);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpio_gatherv_alloc_simple() */

/*-------------------------------------------------------------------------
 * Function:    H5_mpio_get_file_sync_required
 *
 * Purpose:     Retrieve the MPI hint indicating whether the data written
 *              by the MPI ROMIO driver is immediately visible to all MPI
 *              ranks.
 *
 * Notes:       This routine is designed for supporting UnifyFS that needs
 *              MPI_File_sync in order to make the written data available
 *              to all ranks.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5_mpio_get_file_sync_required(MPI_File fh, bool *file_sync_required)
{
    MPI_Info info_used;
    int      flag;
    char    *sync_env_var;
    char     value[MPI_MAX_INFO_VAL];
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(file_sync_required);

    *file_sync_required = false;

    if (MPI_SUCCESS != MPI_File_get_info(fh, &info_used))
        HGOTO_ERROR(H5E_LIB, H5E_CANTGET, FAIL, "can't get MPI info");

    if (MPI_SUCCESS !=
        MPI_Info_get(info_used, "romio_visibility_immediate", MPI_MAX_INFO_VAL - 1, value, &flag))
        HGOTO_ERROR(H5E_LIB, H5E_CANTGET, FAIL, "can't get MPI info");

    if (flag && !strcmp(value, "false"))
        *file_sync_required = true;

    if (MPI_SUCCESS != MPI_Info_free(&info_used))
        HGOTO_ERROR(H5E_LIB, H5E_CANTFREE, FAIL, "can't free MPI info");

    /* Force setting the flag via env variable (temp solution before the flag is implemented in MPI) */
    sync_env_var = getenv("HDF5_DO_MPI_FILE_SYNC");
    if (sync_env_var && (!strcmp(sync_env_var, "TRUE") || !strcmp(sync_env_var, "1")))
        *file_sync_required = true;
    if (sync_env_var && (!strcmp(sync_env_var, "FALSE") || !strcmp(sync_env_var, "0")))
        *file_sync_required = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5_mpio_get_file_sync_required() */
#endif /* H5_HAVE_PARALLEL */
