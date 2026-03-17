#ifndef PyMPI_CONFIG_MSMPI_H
#define PyMPI_CONFIG_MSMPI_H

#include "mpi-11.h"
#include "mpi-12.h"
#include "mpi-20.h"
#include "mpi-22.h"
#include "mpi-30.h"
#include "mpi-31.h"
#include "mpi-40.h"

#if MSMPI_VER >= 0x402
#define PyMPI_HAVE_MPI_AINT 1
#define PyMPI_HAVE_MPI_OFFSET 1
#define PyMPI_HAVE_MPI_C_BOOL 1
#define PyMPI_HAVE_MPI_INT8_T 1
#define PyMPI_HAVE_MPI_INT16_T 1
#define PyMPI_HAVE_MPI_INT32_T 1
#define PyMPI_HAVE_MPI_INT64_T 1
#define PyMPI_HAVE_MPI_UINT8_T 1
#define PyMPI_HAVE_MPI_UINT16_T 1
#define PyMPI_HAVE_MPI_UINT32_T 1
#define PyMPI_HAVE_MPI_UINT64_T 1
#define PyMPI_HAVE_MPI_C_COMPLEX 1
#define PyMPI_HAVE_MPI_C_FLOAT_COMPLEX 1
#define PyMPI_HAVE_MPI_C_DOUBLE_COMPLEX 1
#define PyMPI_HAVE_MPI_C_LONG_DOUBLE_COMPLEX 1
#define PyMPI_HAVE_MPI_REAL2 1
#define PyMPI_HAVE_MPI_COMPLEX4 1
#define PyMPI_HAVE_MPI_Reduce_local 1
#endif

#if MSMPI_VER >= 0x500
#define PyMPI_HAVE_MPI_COMM_TYPE_SHARED 1
#define PyMPI_HAVE_MPI_Comm_split_type 1
#define PyMPI_HAVE_MPI_Win_allocate_shared 1
#define PyMPI_HAVE_MPI_Win_shared_query 1
#define PyMPI_HAVE_MPI_MAX_LIBRARY_VERSION_STRING 1
#define PyMPI_HAVE_MPI_Get_library_version 1
#endif

#if MSMPI_VER >= 0x600
#define PyMPI_HAVE_MPI_Count 1
#define PyMPI_HAVE_MPI_COUNT 1
#define PyMPI_HAVE_MPI_Type_create_hindexed_block 1
#define PyMPI_HAVE_MPI_COMBINER_HINDEXED_BLOCK 1
#define PyMPI_HAVE_MPI_Type_size_x 1
#define PyMPI_HAVE_MPI_Type_get_extent_x 1
#define PyMPI_HAVE_MPI_Type_get_true_extent_x 1
#define PyMPI_HAVE_MPI_Get_elements_x 1
#define PyMPI_HAVE_MPI_Status_set_elements_x 1
#define PyMPI_HAVE_MPI_Message 1
#define PyMPI_HAVE_MPI_MESSAGE_NULL 1
#define PyMPI_HAVE_MPI_MESSAGE_NO_PROC 1
#define PyMPI_HAVE_MPI_Mprobe 1
#define PyMPI_HAVE_MPI_Improbe 1
#define PyMPI_HAVE_MPI_Mrecv 1
#define PyMPI_HAVE_MPI_Imrecv 1
#define PyMPI_HAVE_MPI_Message_c2f 1
#define PyMPI_HAVE_MPI_Message_f2c 1
#define PyMPI_HAVE_MPI_Op_commutative 1
#define PyMPI_HAVE_MPI_DIST_GRAPH 1
#define PyMPI_HAVE_MPI_UNWEIGHTED 1
#define PyMPI_HAVE_MPI_WEIGHTS_EMPTY 1
#define PyMPI_HAVE_MPI_Dist_graph_create_adjacent 1
#define PyMPI_HAVE_MPI_Dist_graph_create 1
#define PyMPI_HAVE_MPI_Dist_graph_neighbors_count 1
#define PyMPI_HAVE_MPI_Dist_graph_neighbors 1
#define PyMPI_HAVE_MPI_Ibarrier 1
#define PyMPI_HAVE_MPI_Ibcast 1
#define PyMPI_HAVE_MPI_Igather 1
#define PyMPI_HAVE_MPI_Ireduce 1
#endif

#if MSMPI_VER >= 0x700
#define PyMPI_HAVE_MPI_Iallgather 1
#define PyMPI_HAVE_MPI_Iallreduce 1
#define PyMPI_HAVE_MPI_Igatherv 1
#define PyMPI_HAVE_MPI_Iscatter 1
#define PyMPI_HAVE_MPI_Iscatterv 1
#endif

#if MSMPI_VER >= 0x800
#define PyMPI_HAVE_MPI_Reduce_scatter_block 1
#define PyMPI_HAVE_MPI_Iallgatherv 1
#define PyMPI_HAVE_MPI_Ialltoall 1
#define PyMPI_HAVE_MPI_Ialltoallv 1
#define PyMPI_HAVE_MPI_Ialltoallw 1
#define PyMPI_HAVE_MPI_Iallreduce 1
#define PyMPI_HAVE_MPI_Ireduce_scatter 1
#define PyMPI_HAVE_MPI_Ireduce_scatter_block 1
#define PyMPI_HAVE_MPI_Iscan 1
#define PyMPI_HAVE_MPI_Iexscan 1
#endif

#if MSMPI_VER >= 0x900
#define PyMPI_HAVE_MPI_NO_OP 1
#define PyMPI_HAVE_MPI_Win_allocate 1
#define PyMPI_HAVE_MPI_Win_create_dynamic 1
#define PyMPI_HAVE_MPI_Win_attach 1
#define PyMPI_HAVE_MPI_Win_detach 1
#define PyMPI_HAVE_MPI_Rget 1
#define PyMPI_HAVE_MPI_Rput 1
#define PyMPI_HAVE_MPI_Raccumulate 1
#define PyMPI_HAVE_MPI_Win_flush 1
#define PyMPI_HAVE_MPI_WIN_CREATE_FLAVOR 1
#define PyMPI_HAVE_MPI_WIN_MODEL 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_CREATE 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_ALLOCATE 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_DYNAMIC 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_SHARED 1
#define PyMPI_HAVE_MPI_WIN_SEPARATE 1
#define PyMPI_HAVE_MPI_WIN_UNIFIED 1
#endif

#if MSMPI_VER >= 0xA00
#define PyMPI_HAVE_MPI_Get_accumulate
#define PyMPI_HAVE_MPI_Rget_accumulate
#define PyMPI_HAVE_MPI_Fetch_and_op
#define PyMPI_HAVE_MPI_Compare_and_swap
#define PyMPI_HAVE_MPI_Win_lock_all
#define PyMPI_HAVE_MPI_Win_unlock_all
#define PyMPI_HAVE_MPI_Win_flush_all
#define PyMPI_HAVE_MPI_Win_flush_local
#define PyMPI_HAVE_MPI_Win_flush_local_all
#define PyMPI_HAVE_MPI_Win_sync
#endif

#endif /* !PyMPI_CONFIG_MSMPI_H */
