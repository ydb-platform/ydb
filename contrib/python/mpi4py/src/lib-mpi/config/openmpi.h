#ifndef PyMPI_CONFIG_OPENMPI_H
#define PyMPI_CONFIG_OPENMPI_H

#include "mpi-11.h"
#include "mpi-12.h"
#include "mpi-20.h"
#include "mpi-22.h"
#include "mpi-30.h"
#include "mpi-31.h"
#include "mpi-40.h"

#ifndef OMPI_HAVE_FORTRAN_LOGICAL1
#define OMPI_HAVE_FORTRAN_LOGICAL1 0
#endif
#ifndef OMPI_HAVE_FORTRAN_LOGICAL2
#define OMPI_HAVE_FORTRAN_LOGICAL2 0
#endif
#ifndef OMPI_HAVE_FORTRAN_LOGICAL4
#define OMPI_HAVE_FORTRAN_LOGICAL4 0
#endif
#ifndef OMPI_HAVE_FORTRAN_LOGICAL8
#define OMPI_HAVE_FORTRAN_LOGICAL8 0
#endif

#if OMPI_HAVE_FORTRAN_LOGICAL1
#define PyMPI_HAVE_MPI_LOGICAL1 1
#endif
#if OMPI_HAVE_FORTRAN_LOGICAL2
#define PyMPI_HAVE_MPI_LOGICAL2 1
#endif
#if OMPI_HAVE_FORTRAN_LOGICAL4
#define PyMPI_HAVE_MPI_LOGICAL4 1
#endif
#if OMPI_HAVE_FORTRAN_LOGICAL8
#define PyMPI_HAVE_MPI_LOGICAL8 1
#endif

#if !OMPI_HAVE_FORTRAN_INTEGER1
#undef PyMPI_HAVE_MPI_INTEGER1
#endif
#if !OMPI_HAVE_FORTRAN_INTEGER2
#undef PyMPI_HAVE_MPI_INTEGER2
#endif
#if !OMPI_HAVE_FORTRAN_INTEGER4
#undef PyMPI_HAVE_MPI_INTEGER4
#endif
#if !OMPI_HAVE_FORTRAN_INTEGER8
#undef PyMPI_HAVE_MPI_INTEGER8
#endif
#if !OMPI_HAVE_FORTRAN_INTEGER16
#undef PyMPI_HAVE_MPI_INTEGER16
#endif
#if !OMPI_HAVE_FORTRAN_REAL2
#undef PyMPI_HAVE_MPI_REAL2
#undef PyMPI_HAVE_MPI_COMPLEX4
#endif
#if !OMPI_HAVE_FORTRAN_REAL4
#undef PyMPI_HAVE_MPI_REAL4
#undef PyMPI_HAVE_MPI_COMPLEX8
#endif
#if !OMPI_HAVE_FORTRAN_REAL8
#undef PyMPI_HAVE_MPI_REAL8
#undef PyMPI_HAVE_MPI_COMPLEX16
#endif
#if !OMPI_HAVE_FORTRAN_REAL16
#undef PyMPI_HAVE_MPI_REAL16
#undef PyMPI_HAVE_MPI_COMPLEX32
#endif

#ifdef OMPI_PROVIDE_MPI_FILE_INTERFACE
#if OMPI_PROVIDE_MPI_FILE_INTERFACE == 0
#include "mpi-io.h"
#endif
#endif

#if (defined(OMPI_MAJOR_VERSION) && \
     defined(OMPI_MINOR_VERSION) && \
     defined(OMPI_RELEASE_VERSION))
#define OMPI_NUMVERSION (OMPI_MAJOR_VERSION*10000 + \
                         OMPI_MINOR_VERSION*100 + \
                         OMPI_RELEASE_VERSION)
#else
#define OMPI_NUMVERSION (10100)
#endif

#if MPI_VERSION < 3

#if OMPI_NUMVERSION >= 10700
#define PyMPI_HAVE_MPI_Message 1
#define PyMPI_HAVE_MPI_MESSAGE_NULL 1
#define PyMPI_HAVE_MPI_MESSAGE_NO_PROC 1
#define PyMPI_HAVE_MPI_Message_c2f 1
#define PyMPI_HAVE_MPI_Message_f2c 1
#define PyMPI_HAVE_MPI_Mprobe 1
#define PyMPI_HAVE_MPI_Improbe 1
#define PyMPI_HAVE_MPI_Mrecv 1
#define PyMPI_HAVE_MPI_Imrecv 1
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
#define PyMPI_HAVE_MPI_MAX_LIBRARY_VERSION_STRING 1
#define PyMPI_HAVE_MPI_Get_library_version 1
#endif /* OMPI >= 1.7.0 */

#if OMPI_NUMVERSION >= 10704
#define PyMPI_HAVE_MPI_Neighbor_allgather 1
#define PyMPI_HAVE_MPI_Neighbor_allgatherv 1
#define PyMPI_HAVE_MPI_Neighbor_alltoall 1
#define PyMPI_HAVE_MPI_Neighbor_alltoallv 1
#define PyMPI_HAVE_MPI_Neighbor_alltoallw 1
#define PyMPI_HAVE_MPI_Ineighbor_allgather 1
#define PyMPI_HAVE_MPI_Ineighbor_allgatherv 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoall 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoallv 1
#define PyMPI_HAVE_MPI_Ineighbor_alltoallw 1
#endif /* OMPI >= 1.7.4 */

#endif

#if MPI_VERSION == 3

#if OMPI_NUMVERSION <= 10705
#undef PyMPI_HAVE_MPI_Comm_set_info
#undef PyMPI_HAVE_MPI_Comm_get_info
#undef PyMPI_HAVE_MPI_WEIGHTS_EMPTY
#undef PyMPI_HAVE_MPI_ERR_RMA_SHARED
#endif /* OMPI <= 1.7.5 */

#endif

#if OMPI_NUMVERSION >= 40000
#undef PyMPI_HAVE_MPI_LB
#undef PyMPI_HAVE_MPI_UB
#endif /* OMPI >= 4.0.0 */

#endif /* !PyMPI_CONFIG_OPENMPI_H */
