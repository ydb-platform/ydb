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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "coll_sync.h"


/*
 *	reduce
 *
 *	Function:	- reduce
 *	Accepts:	- same as MPI_Reduce()
 *	Returns:	- MPI_SUCCESS or error code
 */
int mca_coll_sync_reduce(const void *sbuf, void *rbuf, int count,
                         struct ompi_datatype_t *dtype,
                         struct ompi_op_t *op,
                         int root, struct ompi_communicator_t *comm,
                         mca_coll_base_module_t *module)
{
    mca_coll_sync_module_t *s = (mca_coll_sync_module_t*) module;

    if (s->in_operation) {
        return s->c_coll.coll_reduce(sbuf, rbuf, count, dtype, op, root, comm,
                                     s->c_coll.coll_reduce_module);
    }
    COLL_SYNC(s, s->c_coll.coll_reduce(sbuf, rbuf, count, dtype,
                                       op, root, comm,
                                       s->c_coll.coll_reduce_module));
}
