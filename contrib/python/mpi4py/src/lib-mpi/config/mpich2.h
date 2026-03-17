#ifndef PyMPI_CONFIG_MPICH2_H
#define PyMPI_CONFIG_MPICH2_H

#include "mpi-11.h"
#include "mpi-12.h"
#include "mpi-20.h"
#include "mpi-22.h"
#include "mpi-30.h"
#include "mpi-31.h"
#include "mpi-40.h"

/* These types are difficult to implement portably */
#undef PyMPI_HAVE_MPI_REAL2
#undef PyMPI_HAVE_MPI_COMPLEX4

#if defined(MPI_UNWEIGHTED) && (MPICH2_NUMVERSION < 10300000)
#undef  MPI_UNWEIGHTED
#define MPI_UNWEIGHTED ((int *)0)
#endif /* MPICH2 < 1.3.0 */

#if !defined(MPICH2_NUMVERSION) || (MPICH2_NUMVERSION < 10100000)
#undef PyMPI_HAVE_MPI_Type_create_f90_integer
#undef PyMPI_HAVE_MPI_Type_create_f90_real
#undef PyMPI_HAVE_MPI_Type_create_f90_complex
#endif /* MPICH2 < 1.1.0 */

#ifndef ROMIO_VERSION
#include "mpi-io.h"
#endif

#if MPI_VERSION < 3 && defined(MPICH2_NUMVERSION)
#if MPICH2_NUMVERSION >= 10500000 && \
    MPICH2_NUMVERSION <  20000000

/*
#define PyMPI_HAVE_MPI_Count 1
#define PyMPI_HAVE_MPI_COUNT 1
#define PyMPI_HAVE_MPI_Type_size_x 1
#define PyMPI_HAVE_MPI_Type_get_extent_x 1
#define PyMPI_HAVE_MPI_Type_get_true_extent_x 1
#define PyMPI_HAVE_MPI_Get_elements_x 1
#define PyMPI_HAVE_MPI_Status_set_elements_x 1
#define MPI_Count                  MPIX_Count
#define MPI_COUNT                  MPIX_COUNT
#define MPI_Type_size_x            MPIX_Type_size_x
#define MPI_Type_get_extent_x      MPIX_Type_get_extent_x
#define MPI_Type_get_true_extent_x MPIX_Type_get_true_extent_x
#define MPI_Get_elements_x         MPIX_Get_elements_x
#define MPI_Status_set_elements_x  MPIX_Status_set_elements_x
*/

#define PyMPI_HAVE_MPI_COMBINER_HINDEXED_BLOCK 1
#define PyMPI_HAVE_MPI_Type_create_hindexed_block 1
#define MPI_COMBINER_HINDEXED_BLOCK    MPIX_COMBINER_HINDEXED_BLOCK
#define MPI_Type_create_hindexed_block MPIX_Type_create_hindexed_block

#define PyMPI_HAVE_MPI_NO_OP 1
#define MPI_NO_OP MPIX_NO_OP

#define PyMPI_HAVE_MPI_Message 1
#define PyMPI_HAVE_MPI_MESSAGE_NULL 1
#define PyMPI_HAVE_MPI_MESSAGE_NO_PROC 1
#define PyMPI_HAVE_MPI_Message_c2f 1
#define PyMPI_HAVE_MPI_Message_f2c 1
#define PyMPI_HAVE_MPI_Mprobe 1
#define PyMPI_HAVE_MPI_Improbe 1
#define PyMPI_HAVE_MPI_Mrecv 1
#define PyMPI_HAVE_MPI_Imrecv 1
#define MPI_Message         MPIX_Message
#define MPI_MESSAGE_NULL    MPIX_MESSAGE_NULL
#define MPI_MESSAGE_NO_PROC MPIX_MESSAGE_NO_PROC
#define MPI_Message_c2f     MPIX_Message_c2f
#define MPI_Message_f2c     MPIX_Message_f2c
#define MPI_Mprobe          MPIX_Mprobe
#define MPI_Improbe         MPIX_Improbe
#define MPI_Mrecv           MPIX_Mrecv
#define MPI_Imrecv          MPIX_Imrecv

#define PyMPI_HAVE_MPI_Ibarrier 1
#define PyMPI_HAVE_MPI_Ibcast 1
#define PyMPI_HAVE_MPI_Igather 1
#define PyMPI_HAVE_MPI_Igatherv 1
#define PyMPI_HAVE_MPI_Iscatter 1
#define PyMPI_HAVE_MPI_Iscatterv 1
#define PyMPI_HAVE_MPI_Iallgather 1
#define PyMPI_HAVE_MPI_Iallgatherv 1
#define PyMPI_HAVE_MPI_Ialltoall 1
#define PyMPI_HAVE_MPI_Ialltoallv 1
#define PyMPI_HAVE_MPI_Ialltoallw 1
#define PyMPI_HAVE_MPI_Ireduce 1
#define PyMPI_HAVE_MPI_Iallreduce 1
#define PyMPI_HAVE_MPI_Ireduce_scatter_block 1
#define PyMPI_HAVE_MPI_Ireduce_scatter 1
#define PyMPI_HAVE_MPI_Iscan 1
#define PyMPI_HAVE_MPI_Iexscan 1
#define MPI_Ibarrier              MPIX_Ibarrier
#define MPI_Ibcast                MPIX_Ibcast
#define MPI_Igather               MPIX_Igather
#define MPI_Igatherv              MPIX_Igatherv
#define MPI_Iscatter              MPIX_Iscatter
#define MPI_Iscatterv             MPIX_Iscatterv
#define MPI_Iallgather            MPIX_Iallgather
#define MPI_Iallgatherv           MPIX_Iallgatherv
#define MPI_Ialltoall             MPIX_Ialltoall
#define MPI_Ialltoallv            MPIX_Ialltoallv
#define MPI_Ialltoallw            MPIX_Ialltoallw
#define MPI_Ireduce               MPIX_Ireduce
#define MPI_Iallreduce            MPIX_Iallreduce
#define MPI_Ireduce_scatter_block MPIX_Ireduce_scatter_block
#define MPI_Ireduce_scatter       MPIX_Ireduce_scatter
#define MPI_Iscan                 MPIX_Iscan
#define MPI_Iexscan               MPIX_Iexscan

#define PyMPI_HAVE_MPI_Neighbor_allgather 1
#define PyMPI_HAVE_MPI_Neighbor_allgatherv 1
#define PyMPI_HAVE_MPI_Neighbor_alltoall 1
#define PyMPI_HAVE_MPI_Neighbor_alltoallv 1
#define PyMPI_HAVE_MPI_Neighbor_alltoallw 1
#define MPI_Neighbor_allgather  MPIX_Neighbor_allgather
#define MPI_Neighbor_allgatherv MPIX_Neighbor_allgatherv
#define MPI_Neighbor_alltoall   MPIX_Neighbor_alltoall
#define MPI_Neighbor_alltoallv  MPIX_Neighbor_alltoallv
#define MPI_Neighbor_alltoallw  MPIX_Neighbor_alltoallw
#define PyMPI_HAVE_MPI_Ineighbor_allgather 1
#define PyMPI_HAVE_MPI_Ineighbor_allgatherv 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoall 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoallv 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoallw 1
#define MPI_Ineighbor_allgather  MPIX_Ineighbor_allgather
#define MPI_Ineighbor_allgatherv MPIX_Ineighbor_allgatherv
#define MPI_Ineighbor_alltoall   MPIX_Ineighbor_alltoall
#define MPI_Ineighbor_alltoallv  MPIX_Ineighbor_alltoallv
#define MPI_Ineighbor_alltoallw  MPIX_Ineighbor_alltoallw

#define PyMPI_HAVE_MPI_Comm_idup 1
#define PyMPI_HAVE_MPI_Comm_create_group 1
#define PyMPI_HAVE_MPI_COMM_TYPE_SHARED 1
#define PyMPI_HAVE_MPI_Comm_split_type 1
#define MPI_Comm_idup             MPIX_Comm_idup
#define MPI_Comm_create_group     MPIX_Comm_create_group
#define MPI_COMM_TYPE_SHARED      MPIX_COMM_TYPE_SHARED
#define MPI_Comm_split_type       MPIX_Comm_split_type
/*
#define PyMPI_HAVE_MPI_Comm_dup_with_info 1
#define PyMPI_HAVE_MPI_Comm_set_info 1
#define PyMPI_HAVE_MPI_Comm_get_info 1
#define MPI_Comm_dup_with_info    MPIX_Comm_dup_with_info
#define MPI_Comm_set_info         MPIX_Comm_set_info
#define MPI_Comm_get_info         MPIX_Comm_get_info
*/

#define PyMPI_HAVE_MPI_WIN_CREATE_FLAVOR 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_CREATE 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_ALLOCATE 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_DYNAMIC 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_SHARED 1
#define MPI_WIN_CREATE_FLAVOR   MPIX_WIN_CREATE_FLAVOR
#define MPI_WIN_FLAVOR_CREATE   MPIX_WIN_FLAVOR_CREATE
#define MPI_WIN_FLAVOR_ALLOCATE MPIX_WIN_FLAVOR_ALLOCATE
#define MPI_WIN_FLAVOR_DYNAMIC  MPIX_WIN_FLAVOR_DYNAMIC
#define MPI_WIN_FLAVOR_SHARED   MPIX_WIN_FLAVOR_SHARED
#define PyMPI_HAVE_MPI_WIN_MODEL 1
#define PyMPI_HAVE_MPI_WIN_SEPARATE 1
#define PyMPI_HAVE_MPI_WIN_UNIFIED 1
#define MPI_WIN_MODEL    MPIX_WIN_MODEL
#define MPI_WIN_SEPARATE MPIX_WIN_SEPARATE
#define MPI_WIN_UNIFIED  MPIX_WIN_UNIFIED
#define PyMPI_HAVE_MPI_Win_allocate 1
#define MPI_Win_allocate MPIX_Win_allocate
#define PyMPI_HAVE_MPI_Win_allocate_shared 1
#define PyMPI_HAVE_MPI_Win_shared_query 1
#define MPI_Win_allocate_shared MPIX_Win_allocate_shared
#define MPI_Win_shared_query    MPIX_Win_shared_query
#define PyMPI_HAVE_MPI_Win_create_dynamic 1
#define PyMPI_HAVE_MPI_Win_attach 1
#define PyMPI_HAVE_MPI_Win_detach 1
#define MPI_Win_create_dynamic  MPIX_Win_create_dynamic
#define MPI_Win_attach          MPIX_Win_attach
#define MPI_Win_detach          MPIX_Win_detach
/*
#define PyMPI_HAVE_MPI_Win_set_info 1
#define PyMPI_HAVE_MPI_Win_get_info 1
#define MPI_Win_set_info MPIX_Win_set_info
#define MPI_Win_get_info MPIX_Win_get_info
*/
#define PyMPI_HAVE_MPI_Get_accumulate 1
#define PyMPI_HAVE_MPI_Fetch_and_op 1
#define PyMPI_HAVE_MPI_Compare_and_swap 1
#define MPI_Get_accumulate   MPIX_Get_accumulate
#define MPI_Fetch_and_op     MPIX_Fetch_and_op
#define MPI_Compare_and_swap MPIX_Compare_and_swap
#define PyMPI_HAVE_MPI_Rget 1
#define PyMPI_HAVE_MPI_Rput 1
#define PyMPI_HAVE_MPI_Raccumulate 1
#define PyMPI_HAVE_MPI_Rget_accumulate 1
#define MPI_Rget            MPIX_Rget
#define MPI_Rput            MPIX_Rput
#define MPI_Raccumulate     MPIX_Raccumulate
#define MPI_Rget_accumulate MPIX_Rget_accumulate
#define PyMPI_HAVE_MPI_Win_lock_all 1
#define PyMPI_HAVE_MPI_Win_unlock_all 1
#define PyMPI_HAVE_MPI_Win_flush 1
#define PyMPI_HAVE_MPI_Win_flush_all 1
#define PyMPI_HAVE_MPI_Win_flush_local 1
#define PyMPI_HAVE_MPI_Win_flush_local_all 1
#define PyMPI_HAVE_MPI_Win_sync
#define MPI_Win_lock_all        MPIX_Win_lock_all
#define MPI_Win_unlock_all      MPIX_Win_unlock_all
#define MPI_Win_flush           MPIX_Win_flush
#define MPI_Win_flush_all       MPIX_Win_flush_all
#define MPI_Win_flush_local     MPIX_Win_flush_local
#define MPI_Win_flush_local_all MPIX_Win_flush_local_all
#define MPI_Win_sync            MPIX_Win_sync
#define PyMPI_HAVE_MPI_ERR_RMA_RANGE 1
#define PyMPI_HAVE_MPI_ERR_RMA_ATTACH 1
#define PyMPI_HAVE_MPI_ERR_RMA_SHARED 1
#define PyMPI_HAVE_MPI_ERR_RMA_FLAVOR 1
#define MPI_ERR_RMA_RANGE  MPIX_ERR_RMA_RANGE
#define MPI_ERR_RMA_ATTACH MPIX_ERR_RMA_ATTACH
#define MPI_ERR_RMA_SHARED MPIX_ERR_RMA_SHARED
#define MPI_ERR_RMA_FLAVOR MPIX_ERR_RMA_WRONG_FLAVOR

/*
#define PyMPI_HAVE_MPI_MAX_LIBRARY_VERSION_STRING 1
#define PyMPI_HAVE_MPI_Get_library_version 1
#define PyMPI_HAVE_MPI_INFO_ENV 1
#define MPI_MAX_LIBRARY_VERSION_STRING MPIX_MAX_LIBRARY_VERSION_STRING
#define MPI_Get_library_version        MPIX_Get_library_version
#define MPI_INFO_ENV                   MPIX_INFO_ENV
*/

#endif /* MPICH2 < 1.5*/
#endif /* MPI    < 3.0*/

#endif /* !PyMPI_CONFIG_MPICH2_H */
