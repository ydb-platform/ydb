/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All right
 *                         reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/constants.h"

#include "ompi/mca/topo/base/base.h"
#include "ompi/mca/topo/topo.h"

/*
 * function - makes a new communicator to which topology information
 *            has been attached
 *
 * @param comm input communicator (handle)
 * @param ndims number of dimensions of cartesian grid (integer)
 * @param dims integer array of size ndims specifying the number of processes in
 *             each dimension
 * @param periods logical array of size ndims specifying whether the grid is
 *                periodic (true) or not (false) in each dimension
 * @param reorder ranking may be reordered (true) or not (false) (logical)
 * @param comm_cart communicator with new cartesian topology (handle)
 *
 * Open MPI currently ignores the 'reorder' flag.
 *
 * @retval OMPI_SUCCESS
 */

int mca_topo_base_cart_create(mca_topo_base_module_t *topo,
                              ompi_communicator_t* old_comm,
                              int ndims,
                              const int *dims,
                              const int *periods,
                              bool reorder,
                              ompi_communicator_t** comm_topo)
{
    int nprocs = 1, i, new_rank, num_procs, ret;
    ompi_communicator_t *new_comm;
    ompi_proc_t **topo_procs = NULL;
    mca_topo_base_comm_cart_2_2_0_t* cart;

    num_procs = old_comm->c_local_group->grp_proc_count;
    new_rank = old_comm->c_local_group->grp_my_rank;
    assert(topo->type == OMPI_COMM_CART);

    /* Calculate the number of processes in this grid */
    for (i = 0; i < ndims; ++i) {
        if(dims[i] <= 0) {
            return OMPI_ERROR;
        }
        nprocs *= dims[i];
    }

    /* check for the error condition */
    if (num_procs < nprocs) {
        return MPI_ERR_DIMS;
    }

    /* check if we have to trim the list of processes */
    if (nprocs < num_procs) {
        num_procs = nprocs;
    }

    if (new_rank > (nprocs-1)) {
        ndims = 0;
        new_rank = MPI_UNDEFINED;
        num_procs = 0;
    }

    cart = OBJ_NEW(mca_topo_base_comm_cart_2_2_0_t);
    if( NULL == cart ) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    cart->ndims = ndims;

    /* MPI-2.1 allows 0-dimension cartesian communicators, so prevent
       a 0-byte malloc -- leave dims as NULL */
    if( ndims > 0 ) {
        cart->dims = (int*)malloc(sizeof(int) * ndims);
        if (NULL == cart->dims) {
            OBJ_RELEASE(cart);
            return OMPI_ERROR;
        }
        memcpy(cart->dims, dims, ndims * sizeof(int));

        /* Cartesian communicator; copy the right data to the common information */
        cart->periods = (int*)malloc(sizeof(int) * ndims);
        if (NULL == cart->periods) {
            OBJ_RELEASE(cart);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        memcpy(cart->periods, periods, ndims * sizeof(int));

        cart->coords = (int*)malloc(sizeof(int) * ndims);
        if (NULL == cart->coords) {
            OBJ_RELEASE(cart);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        {  /* setup the cartesian topology */
            int nprocs = num_procs, rank = new_rank;

            for (i = 0; i < ndims; ++i) {
                nprocs /= cart->dims[i];
                cart->coords[i] = rank / nprocs;
                rank %= nprocs;
            }
        }
    }

    /* JMS: This should really be refactored to use
       comm_create_group(), because ompi_comm_allocate() still
       complains about 0-byte mallocs in debug builds for 0-member
       groups. */
    if (num_procs > 0) {
        /* Copy the proc structure from the previous communicator over to
           the new one.  The topology module is then able to work on this
           copy and rearrange it as it deems fit. */
        topo_procs = (ompi_proc_t**)malloc(num_procs * sizeof(ompi_proc_t *));
        if (NULL == topo_procs) {
            OBJ_RELEASE(cart);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        if(OMPI_GROUP_IS_DENSE(old_comm->c_local_group)) {
            memcpy(topo_procs,
                   old_comm->c_local_group->grp_proc_pointers,
                   num_procs * sizeof(ompi_proc_t *));
        } else {
            for(i = 0 ; i < num_procs; i++) {
                topo_procs[i] = ompi_group_peer_lookup(old_comm->c_local_group,i);
            }
        }
    }

    /* allocate a new communicator */
    new_comm = ompi_comm_allocate(num_procs, 0);
    if (NULL == new_comm) {
        free(topo_procs);
        OBJ_RELEASE(cart);
        return MPI_ERR_INTERN;
    }

    ret = ompi_comm_enable(old_comm, new_comm,
                           new_rank, num_procs, topo_procs);
    if (OMPI_SUCCESS != ret) {
        /* something wrong happened during setting the communicator */
        free(topo_procs);
        OBJ_RELEASE(cart);
        if (MPI_COMM_NULL != new_comm) {
            new_comm->c_topo = NULL;
            new_comm->c_flags &= ~OMPI_COMM_CART;
            ompi_comm_free (&new_comm);
        }
        return ret;
    }

    new_comm->c_topo           = topo;
    new_comm->c_topo->mtc.cart = cart;
    new_comm->c_topo->reorder  = reorder;
    new_comm->c_flags         |= OMPI_COMM_CART;
    *comm_topo = new_comm;

    if( MPI_UNDEFINED == new_rank ) {
        ompi_comm_free(&new_comm);
        *comm_topo = MPI_COMM_NULL;
    }

    /* end here */
    return OMPI_SUCCESS;
}

static void mca_topo_base_comm_cart_2_2_0_construct(mca_topo_base_comm_cart_2_2_0_t * cart) {
    cart->ndims = 0;
    cart->dims = NULL;
    cart->periods = NULL;
    cart->coords = NULL;
}

static void mca_topo_base_comm_cart_2_2_0_destruct(mca_topo_base_comm_cart_2_2_0_t * cart) {
    if (NULL != cart->dims) {
        free(cart->dims);
    }
    if (NULL != cart->periods) {
        free(cart->periods);
    }
    if (NULL != cart->coords) {
        free(cart->coords);
    }
}

OBJ_CLASS_INSTANCE(mca_topo_base_comm_cart_2_2_0_t, opal_object_t,
                   mca_topo_base_comm_cart_2_2_0_construct,
                   mca_topo_base_comm_cart_2_2_0_destruct);
