/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "coll_sync.h"


/*
 *	reduce_scatter
 *
 *	Function:	- reduce then scatter
 *	Accepts:	- same as MPI_Reduce_scatter()
 *	Returns:	- MPI_SUCCESS or error code
 */
int mca_coll_sync_reduce_scatter(const void *sbuf, void *rbuf, const int *rcounts,
                                 struct ompi_datatype_t *dtype,
                                 struct ompi_op_t *op,
                                 struct ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module)
{
    mca_coll_sync_module_t *s = (mca_coll_sync_module_t*) module;

    if (s->in_operation) {
        return s->c_coll.coll_reduce_scatter(sbuf, rbuf, rcounts,
                                             dtype, op, comm,
                                             s->c_coll.coll_reduce_scatter_module);
    }
    COLL_SYNC(s, s->c_coll.coll_reduce_scatter(sbuf, rbuf, rcounts,
                                               dtype, op, comm,
                                               s->c_coll.coll_reduce_scatter_module));
}
