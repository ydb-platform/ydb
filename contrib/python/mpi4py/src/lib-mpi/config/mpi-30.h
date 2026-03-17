#if defined(MPI_VERSION)
#if (MPI_VERSION >= 3)

#define PyMPI_HAVE_MPI_Count 1
#define PyMPI_HAVE_MPI_COUNT 1
#define PyMPI_HAVE_MPI_CXX_BOOL 1
#define PyMPI_HAVE_MPI_CXX_FLOAT_COMPLEX 1
#define PyMPI_HAVE_MPI_CXX_DOUBLE_COMPLEX 1
#define PyMPI_HAVE_MPI_CXX_LONG_DOUBLE_COMPLEX 1
#define PyMPI_HAVE_MPI_Type_size_x 1
#define PyMPI_HAVE_MPI_Type_get_extent_x 1
#define PyMPI_HAVE_MPI_Type_get_true_extent_x 1
#define PyMPI_HAVE_MPI_Get_elements_x 1
#define PyMPI_HAVE_MPI_Status_set_elements_x 1
#define PyMPI_HAVE_MPI_COMBINER_HINDEXED_BLOCK
#define PyMPI_HAVE_MPI_Type_create_hindexed_block 1

#define PyMPI_HAVE_MPI_NO_OP 1

#define PyMPI_HAVE_MPI_Message 1
#define PyMPI_HAVE_MPI_MESSAGE_NULL 1
#define PyMPI_HAVE_MPI_MESSAGE_NO_PROC 1
#define PyMPI_HAVE_MPI_Message_c2f 1
#define PyMPI_HAVE_MPI_Message_f2c 1
#define PyMPI_HAVE_MPI_Mprobe 1
#define PyMPI_HAVE_MPI_Improbe 1
#define PyMPI_HAVE_MPI_Mrecv 1
#define PyMPI_HAVE_MPI_Imrecv 1

#define PyMPI_HAVE_MPI_Neighbor_allgather 1
#define PyMPI_HAVE_MPI_Neighbor_allgatherv 1
#define PyMPI_HAVE_MPI_Neighbor_alltoall 1
#define PyMPI_HAVE_MPI_Neighbor_alltoallv 1
#define PyMPI_HAVE_MPI_Neighbor_alltoallw 1

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

#define PyMPI_HAVE_MPI_Ineighbor_allgather 1
#define PyMPI_HAVE_MPI_Ineighbor_allgatherv 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoall 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoallv 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoallw 1

#define PyMPI_HAVE_MPI_WEIGHTS_EMPTY 1

#define PyMPI_HAVE_MPI_Comm_dup_with_info 1
#define PyMPI_HAVE_MPI_Comm_idup 1
#define PyMPI_HAVE_MPI_Comm_create_group 1
#define PyMPI_HAVE_MPI_COMM_TYPE_SHARED 1
#define PyMPI_HAVE_MPI_Comm_split_type 1
#define PyMPI_HAVE_MPI_Comm_set_info 1
#define PyMPI_HAVE_MPI_Comm_get_info 1

#define PyMPI_HAVE_MPI_WIN_CREATE_FLAVOR 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_CREATE 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_ALLOCATE 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_DYNAMIC 1
#define PyMPI_HAVE_MPI_WIN_FLAVOR_SHARED 1

#define PyMPI_HAVE_MPI_WIN_MODEL 1
#define PyMPI_HAVE_MPI_WIN_SEPARATE 1
#define PyMPI_HAVE_MPI_WIN_UNIFIED 1

#define PyMPI_HAVE_MPI_Win_allocate 1
#define PyMPI_HAVE_MPI_Win_allocate_shared 1
#define PyMPI_HAVE_MPI_Win_shared_query 1

#define PyMPI_HAVE_MPI_Win_create_dynamic 1
#define PyMPI_HAVE_MPI_Win_attach 1
#define PyMPI_HAVE_MPI_Win_detach 1

#define PyMPI_HAVE_MPI_Win_set_info 1
#define PyMPI_HAVE_MPI_Win_get_info 1

#define PyMPI_HAVE_MPI_Get_accumulate 1
#define PyMPI_HAVE_MPI_Fetch_and_op 1
#define PyMPI_HAVE_MPI_Compare_and_swap 1

#define PyMPI_HAVE_MPI_Rget 1
#define PyMPI_HAVE_MPI_Rput 1
#define PyMPI_HAVE_MPI_Raccumulate 1
#define PyMPI_HAVE_MPI_Rget_accumulate 1

#define PyMPI_HAVE_MPI_Win_lock_all 1
#define PyMPI_HAVE_MPI_Win_unlock_all 1
#define PyMPI_HAVE_MPI_Win_flush 1
#define PyMPI_HAVE_MPI_Win_flush_all 1
#define PyMPI_HAVE_MPI_Win_flush_local 1
#define PyMPI_HAVE_MPI_Win_flush_local_all 1
#define PyMPI_HAVE_MPI_Win_sync 1

#define PyMPI_HAVE_MPI_ERR_RMA_RANGE  1
#define PyMPI_HAVE_MPI_ERR_RMA_ATTACH 1
#define PyMPI_HAVE_MPI_ERR_RMA_SHARED 1
#define PyMPI_HAVE_MPI_ERR_RMA_FLAVOR 1

#define PyMPI_HAVE_MPI_MAX_LIBRARY_VERSION_STRING 1
#define PyMPI_HAVE_MPI_Get_library_version 1
#define PyMPI_HAVE_MPI_INFO_ENV 1

/*
#define PyMPI_HAVE_MPI_F08_status 1
#define PyMPI_HAVE_MPI_F08_STATUS_IGNORE 1
#define PyMPI_HAVE_MPI_F08_STATUSES_IGNORE 1
#define PyMPI_HAVE_MPI_Status_c2f08 1
#define PyMPI_HAVE_MPI_Status_f082c 1
#define PyMPI_HAVE_MPI_Status_f2f08 1
#define PyMPI_HAVE_MPI_Status_f082f 1
*/

#endif
#endif
