/*
 * Copyright (c) 2014-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2014-2015 NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_cuda.h"

#include <stdio.h>

#include "ompi/op/op.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype_cuda.h"

/*
 *	reduce_scatter_block
 *
 *	Function:	- reduce then scatter
 *	Accepts:	- same as MPI_Reduce_scatter_block()
 *	Returns:	- MPI_SUCCESS or error code
 *
 * Algorithm:
 *     reduce and scatter (needs to be cleaned
 *     up at some point)
 */
int
mca_coll_cuda_reduce_scatter_block(const void *sbuf, void *rbuf, int rcount,
                                   struct ompi_datatype_t *dtype,
                                   struct ompi_op_t *op,
                                   struct ompi_communicator_t *comm,
                                   mca_coll_base_module_t *module)
{
    mca_coll_cuda_module_t *s = (mca_coll_cuda_module_t*) module;
    ptrdiff_t gap;
    char *rbuf1 = NULL, *sbuf1 = NULL, *rbuf2 = NULL;
    size_t sbufsize, rbufsize;
    int rc;

    rbufsize = opal_datatype_span(&dtype->super, rcount, &gap);

    sbufsize = rbufsize * ompi_comm_size(comm);

    if ((MPI_IN_PLACE != sbuf) && (opal_cuda_check_bufs((char *)sbuf, NULL))) {
        sbuf1 = (char*)malloc(sbufsize);
        if (NULL == sbuf1) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        opal_cuda_memcpy_sync(sbuf1, sbuf, sbufsize);
        sbuf = sbuf1 - gap;
    }

    if (opal_cuda_check_bufs(rbuf, NULL)) {
        rbuf1 = (char*)malloc(rbufsize);
        if (NULL == rbuf1) {
            if (NULL != sbuf1) free(sbuf1);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        opal_cuda_memcpy_sync(rbuf1, rbuf, rbufsize);
        rbuf2 = rbuf; /* save away original buffer */
        rbuf = rbuf1 - gap;
    }
    rc = s->c_coll.coll_reduce_scatter_block(sbuf, rbuf, rcount, dtype, op, comm,
                                             s->c_coll.coll_reduce_scatter_block_module);
    if (NULL != sbuf1) {
        free(sbuf1);
    }
    if (NULL != rbuf1) {
        rbuf = rbuf2;
        opal_cuda_memcpy_sync(rbuf, rbuf1, rbufsize);
        free(rbuf1);
    }
    return rc;
}

