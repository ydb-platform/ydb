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
 * Purpose:	Common routines for all MPI-based VFL drivers.
 */

#include "H5private.h"   /* Generic Functions			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5FDprivate.h" /* File drivers				*/
#include "H5FDmpi.h"     /* Common MPI file driver		*/
#include "H5Pprivate.h"  /* Property lists			*/

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:	H5FD_mpi_get_rank
 *
 * Purpose:	Retrieves the rank of an MPI process.
 *
 * Return:	Success:	The rank (non-negative)
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5FD_mpi_get_rank(H5FD_t *file)
{
    const H5FD_class_t *cls;
    uint64_t            flags    = H5FD_CTL_FAIL_IF_UNKNOWN_FLAG | H5FD_CTL_ROUTE_TO_TERMINAL_VFD_FLAG;
    int                 rank     = -1;
    void               *rank_ptr = (void *)(&rank);
    int                 ret_value;

    FUNC_ENTER_NOAPI(FAIL)

    assert(file);
    cls = (const H5FD_class_t *)(file->cls);
    assert(cls);
    assert(cls->ctl); /* All MPI drivers must implement this */

    /* Dispatch to driver */
    if ((cls->ctl)(file, H5FD_CTL_GET_MPI_RANK_OPCODE, flags, NULL, &rank_ptr) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "driver get_rank request failed");

    assert(rank >= 0);

    ret_value = rank;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_mpi_get_rank() */

/*-------------------------------------------------------------------------
 * Function:	H5FD_mpi_get_size
 *
 * Purpose:	Retrieves the size of the communicator used for the file
 *
 * Return:	Success:	The communicator size (non-negative)
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5FD_mpi_get_size(H5FD_t *file)
{
    const H5FD_class_t *cls;
    uint64_t            flags    = H5FD_CTL_FAIL_IF_UNKNOWN_FLAG | H5FD_CTL_ROUTE_TO_TERMINAL_VFD_FLAG;
    int                 size     = 0;
    void               *size_ptr = (void *)(&size);
    int                 ret_value;

    FUNC_ENTER_NOAPI(FAIL)

    assert(file);
    cls = (const H5FD_class_t *)(file->cls);
    assert(cls);
    assert(cls->ctl); /* All MPI drivers must implement this */

    /* Dispatch to driver */
    if ((cls->ctl)(file, H5FD_CTL_GET_MPI_SIZE_OPCODE, flags, NULL, &size_ptr) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "driver get_size request failed");

    if (0 >= size)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "driver get_size request returned bad value");

    ret_value = size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_mpi_get_size() */

/*-------------------------------------------------------------------------
 * Function:	H5FD_mpi_get_comm
 *
 * Purpose:	Retrieves the file's communicator
 *
 * Return:	Success:	The communicator (non-negative)
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
MPI_Comm
H5FD_mpi_get_comm(H5FD_t *file)
{
    const H5FD_class_t *cls;
    uint64_t            flags    = H5FD_CTL_FAIL_IF_UNKNOWN_FLAG | H5FD_CTL_ROUTE_TO_TERMINAL_VFD_FLAG;
    MPI_Comm            comm     = MPI_COMM_NULL;
    void               *comm_ptr = (void *)(&comm);
    MPI_Comm            ret_value;

    FUNC_ENTER_NOAPI(MPI_COMM_NULL)

    assert(file);
    cls = (const H5FD_class_t *)(file->cls);
    assert(cls);
    assert(cls->ctl); /* All MPI drivers must implement this */

    /* Dispatch to driver */
    if ((cls->ctl)(file, H5FD_CTL_GET_MPI_COMMUNICATOR_OPCODE, flags, NULL, &comm_ptr) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, MPI_COMM_NULL, "driver get_comm request failed");

    if (comm == MPI_COMM_NULL)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, MPI_COMM_NULL, "driver get_comm request failed -- bad comm");

    ret_value = comm;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_mpi_get_comm() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_mpi_MPIOff_to_haddr
 *
 * Purpose:     Convert an MPI_Offset value to haddr_t.
 *
 * Return:      Success:	The haddr_t equivalent of the MPI_OFF
 *				argument.
 *
 *              Failure:	HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5FD_mpi_MPIOff_to_haddr(MPI_Offset mpi_off)
{
    haddr_t ret_value = HADDR_UNDEF;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (mpi_off != (MPI_Offset)(haddr_t)mpi_off)
        ret_value = HADDR_UNDEF;
    else
        ret_value = (haddr_t)mpi_off;

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_mpi_haddr_to_MPIOff
 *
 * Purpose:     Convert an haddr_t value to MPI_Offset.
 *
 * Return:      Success:	Non-negative, the MPI_OFF argument contains
 *				the converted value.
 *
 * 		Failure:	Negative, MPI_OFF is undefined.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FD_mpi_haddr_to_MPIOff(haddr_t addr, MPI_Offset *mpi_off /*out*/)
{
    herr_t ret_value = FAIL;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(mpi_off);

    /* Convert the HDF5 address into an MPI offset */
    *mpi_off = (MPI_Offset)addr;

    if (addr != (haddr_t)((MPI_Offset)addr))
        ret_value = FAIL;
    else
        ret_value = SUCCEED;

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5FD_mpi_get_file_sync_required
 *
 * Purpose:	Retrieves the mpi_file_sync_required used for the file
 *
 * Return:	Success:	Non-negative
 *
 *              Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FD_mpi_get_file_sync_required(H5FD_t *file, bool *file_sync_required)
{
    const H5FD_class_t *cls;
    uint64_t            flags                  = H5FD_CTL_ROUTE_TO_TERMINAL_VFD_FLAG;
    void               *file_sync_required_ptr = (void *)(&file_sync_required);
    herr_t              ret_value              = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(file);
    cls = (const H5FD_class_t *)(file->cls);
    assert(cls);
    assert(cls->ctl); /* All MPI drivers must implement this */

    /* Dispatch to driver */
    if ((cls->ctl)(file, H5FD_CTL_GET_MPI_FILE_SYNC_OPCODE, flags, NULL, file_sync_required_ptr) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "driver get_mpi_file_synce request failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_mpi_get_file_sync_required() */

#ifdef NOT_YET

/*-------------------------------------------------------------------------
 * Function:	H5FD_mpio_wait_for_left_neighbor
 *
 * Purpose:	Blocks until (empty) msg is received from immediately
 *		lower-rank neighbor. In conjunction with
 *		H5FD_mpio_signal_right_neighbor, useful for enforcing
 *		1-process-at-at-time access to critical regions to avoid race
 *		conditions (though it is overkill to require that the
 *		processes be allowed to proceed strictly in order of their
 *		rank).
 *
 * Note:	This routine doesn't read or write any file, just performs
 *		interprocess coordination. It really should reside in a
 *		separate package of such routines.
 *
 * Return:	Success:	0
 *		Failure:	-1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FD_mpio_wait_for_left_neighbor(H5FD_t *_file)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
    char         msgbuf[1];
    MPI_Status   rcvstat;
    int          mpi_code;            /* mpi return code */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    /* Portably initialize MPI status variable */
    memset(&rcvstat, 0, sizeof(MPI_Status));

    /* p0 has no left neighbor; all other procs wait for msg */
    if (file->mpi_rank != 0) {
        if (MPI_SUCCESS != (mpi_code = MPI_Recv(&msgbuf, 1, MPI_CHAR, file->mpi_rank - 1, MPI_ANY_TAG,
                                                file->comm, &rcvstat)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Recv failed", mpi_code)
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:	H5FD_mpio_signal_right_neighbor
 *
 * Purpose:	Blocks until (empty) msg is received from immediately
 *		lower-rank neighbor. In conjunction with
 *		H5FD_mpio_wait_for_left_neighbor, useful for enforcing
 *		1-process-at-at-time access to critical regions to avoid race
 *		conditions (though it is overkill to require that the
 *		processes be allowed to proceed strictly in order of their
 *		rank).
 *
 * Note: 	This routine doesn't read or write any file, just performs
 *		interprocess coordination. It really should reside in a
 *		separate package of such routines.
 *
 * Return:	Success:	0
 *		Failure:	-1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FD_mpio_signal_right_neighbor(H5FD_t *_file)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
    char         msgbuf[1];
    int          mpi_code;            /* mpi return code */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    if (file->mpi_rank != (file->mpi_size - 1))
        if (MPI_SUCCESS !=
            (mpi_code = MPI_Send(&msgbuf, 0 /*empty msg*/, MPI_CHAR, file->mpi_rank + 1, 0, file->comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Send failed", mpi_code)

done:
    FUNC_LEAVE_NOAPI(ret_value)
}
#endif /* NOT_YET */
#endif /* H5_HAVE_PARALLEL */
