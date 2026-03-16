/*
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef COMM_COLL_OP_TYPES_H
#define COMM_COLL_OP_TYPES_H

#include "ompi_config.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/proc/proc.h"

BEGIN_C_DECLS

#define OMPI_COMMON_TAG_ALLREDUCE 99
#define OMPI_COMMON_TAG_BCAST     98




OMPI_DECLSPEC int ompi_comm_allgather_pml(void *src_buf, void *dest_buf, int count,
        ompi_datatype_t *dtype, int my_rank_in_group, int n_peers,
        int *ranks_in_comm,ompi_communicator_t *comm);
OMPI_DECLSPEC int ompi_comm_allreduce_pml(void *sbuf, void *rbuf, int count,
        ompi_datatype_t *dtype, int my_rank_in_group,
        struct ompi_op_t *op, int n_peers,int *ranks_in_comm,
        ompi_communicator_t *comm);
OMPI_DECLSPEC int ompi_comm_bcast_pml(void *buffer, int root, int count,
        ompi_datatype_t *dtype, int my_rank_in_group,
        int n_peers, int *ranks_in_comm,ompi_communicator_t
        *comm);

/* reduction operations supported */
#define OP_SUM 1
#define OP_MAX 2
#define OP_MIN 3

#define TYPE_INT4 1


END_C_DECLS

#endif /* COMM_COLL_OP_TYPES_H */
