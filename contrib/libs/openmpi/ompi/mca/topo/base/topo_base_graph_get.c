/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/communicator/communicator.h"

/*
 * function - retrieves graph topology information associated with a
 *            communicator
 *
 * @param comm communicator with graph structure (handle)
 * @param maxindex length of vector 'index' in the calling program  (integer)
 * @param maxedges length of vector 'edges' in the calling program  (integer)
 * @param nodes array of integers containing the graph structure (for details see
 * @param edges array of integers containing the graph structure
 *
 * @retval MPI_SUCCESS
 */

int mca_topo_base_graph_get(ompi_communicator_t* comm,
                            int maxindex,
                            int maxedges,
                            int *index,
                            int *edges)
{
    int i, *p;
    int nprocs = ompi_comm_size(comm);

    /*
     * Fill the nodes and edges arrays.
     */
     p = comm->c_topo->mtc.graph->index;
     for (i = 0; (i < nprocs) && (i < maxindex); ++i, ++p) {
         *index++ = *p;
      }

      p = comm->c_topo->mtc.graph->edges;

      for (i = 0;
          (i < comm->c_topo->mtc.graph->index[nprocs-1]) && (i < maxedges);
          ++i, ++p) {

         *edges++ = *p;

      }

      return MPI_SUCCESS;
}
