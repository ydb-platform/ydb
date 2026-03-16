/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Bcast = PMPI_Bcast
#endif
#define MPI_Bcast PMPI_Bcast
#endif

static const char FUNC_NAME[] = "MPI_Bcast";


int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype,
              int root, MPI_Comm comm)
{
    int err;

    SPC_RECORD(OMPI_SPC_BCAST, 1);

    MEMCHECKER(
        memchecker_datatype(datatype);
        memchecker_comm(comm);
        if (OMPI_COMM_IS_INTRA(comm)) {
            if (ompi_comm_rank(comm) == root) {
                /* check whether root's send buffer is defined. */
                memchecker_call(&opal_memchecker_base_isdefined, buffer, count, datatype);
            }
            /* check whether receive buffer is addressable. */
            memchecker_call(&opal_memchecker_base_isaddressable, buffer, count, datatype);
        } else {
            if (MPI_ROOT == root) {
                /* check whether root's send buffer is defined. */
                memchecker_call(&opal_memchecker_base_isdefined, buffer, count, datatype);
            } else if (MPI_PROC_NULL != root) {
                /* check whether receive buffer is addressable. */
                memchecker_call(&opal_memchecker_base_isaddressable, buffer, count, datatype);
            }
        }
    );

    if (MPI_PARAM_CHECK) {
      err = MPI_SUCCESS;
      OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
      if (ompi_comm_invalid(comm)) {
          return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                     FUNC_NAME);
      }

      /* Errors for all ranks */

      OMPI_CHECK_DATATYPE_FOR_SEND(err, datatype, count);
      OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
      if (MPI_IN_PLACE == buffer) {
          return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
      }

      /* Errors for intracommunicators */

      if (OMPI_COMM_IS_INTRA(comm)) {
        if ((root >= ompi_comm_size(comm)) || (root < 0)) {
          return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ROOT, FUNC_NAME);
        }
      }

      /* Errors for intercommunicators */

      else {
        if (! ((root >= 0 && root < ompi_comm_remote_size(comm)) ||
               MPI_ROOT == root || MPI_PROC_NULL == root)) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ROOT, FUNC_NAME);
        }
      }
    }

    /* If there's only one node, or if the count is 0, we're done */

    if ((OMPI_COMM_IS_INTRA(comm) && ompi_comm_size(comm) <= 1) ||
        0 == count) {
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */

    err = comm->c_coll->coll_bcast(buffer, count, datatype, root, comm,
                                  comm->c_coll->coll_bcast_module);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}
