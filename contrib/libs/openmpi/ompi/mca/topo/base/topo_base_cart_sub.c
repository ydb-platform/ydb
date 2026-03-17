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
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
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
#include "ompi/mca/topo/base/base.h"
#include "ompi/communicator/communicator.h"

/*
 * function - partitions a communicator into subgroups which
 *            form lower-dimensional cartesian subgrids
 *
 * @param comm communicator with cartesian structure (handle)
 * @param remain_dims the 'i'th entry of 'remain_dims' specifies whether
 *                the 'i'th dimension is kept in the subgrid (true)
 *                or is dropped (false) (logical vector)
 * @param new_comm communicator containing the subgrid that includes the
 *                 calling process (handle)
 *
 * @retval MPI_SUCCESS
 * @retval MPI_ERR_TOPOLOGY
 * @retval MPI_ERR_COMM
 */
int mca_topo_base_cart_sub (ompi_communicator_t* comm,
                            const int *remain_dims,
                            ompi_communicator_t** new_comm)
{
    struct ompi_communicator_t *temp_comm;
    mca_topo_base_comm_cart_2_2_0_t *old_cart;
    int errcode, colour, key, colfactor, keyfactor;
    int ndim, dim, i;
    int *d, *dorig = NULL, *dold, *c, *p, *porig = NULL, *pold;
    mca_topo_base_module_t* topo;
    mca_topo_base_comm_cart_2_2_0_t* cart;

    *new_comm = MPI_COMM_NULL;
    old_cart = comm->c_topo->mtc.cart;

    /*
     * Compute colour and key used in splitting the communicator.
     */
    colour = key = 0;
    colfactor = keyfactor = 1;
    ndim = 0;

    i = old_cart->ndims - 1;
    d = old_cart->dims + i;
    c = comm->c_topo->mtc.cart->coords + i;

    for (; i >= 0; --i, --d, --c) {
        dim = *d;
        if (remain_dims[i] == 0) {
            colour += colfactor * (*c);
            colfactor *= dim;
        } else {
            ++ndim;
            key += keyfactor * (*c);
            keyfactor *= dim;
        }
    }
    /* Special case: if all of remain_dims were false, we need to make
       a 0-dimension cartesian communicator with just ourselves in it
       (you can't have a communicator unless you're in it). */
    if (0 == ndim) {
        colour = ompi_comm_rank (comm);
    }
    /* Split the communicator. */
    errcode = ompi_comm_split(comm, colour, key, &temp_comm, false);
    if (errcode != OMPI_SUCCESS) {
        return errcode;
    }

    /* Fill the communicator with topology information. */
    if (temp_comm != MPI_COMM_NULL) {

        assert( NULL == temp_comm->c_topo );
        if (OMPI_SUCCESS != (errcode = mca_topo_base_comm_select(temp_comm,
                                                                 comm->c_topo,
                                                                 &topo,
                                                                 OMPI_COMM_CART))) {
            ompi_comm_free(&temp_comm);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        if (ndim >= 1) {
            /* Copy the dimensions */
            dorig = d = (int*)malloc(ndim * sizeof(int));
            dold = old_cart->dims;
            /* Copy the periods */
            porig = p = (int*)malloc(ndim * sizeof(int));
            pold = old_cart->periods;
            for (i = 0; i < old_cart->ndims; ++i, ++dold, ++pold) {
                if (remain_dims[i]) {
                    *d++ = *dold;
                    *p++ = *pold;
                }
            }
        }
        cart = OBJ_NEW(mca_topo_base_comm_cart_2_2_0_t);
        if( NULL == cart ) {
            ompi_comm_free(&temp_comm);
            if (NULL != dorig) {
                free(dorig);
            }
            if (NULL != porig) {
                free(porig);
            }
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        cart->ndims = ndim;
        cart->dims = dorig;
        cart->periods = porig;

        /* NTH: protect against a 0-byte alloc in the ndims = 0 case */
        if (ndim > 0) {
            cart->coords = (int*)malloc(sizeof(int) * ndim);
            if (NULL == cart->coords) {
                free(cart->periods);
                if(NULL != cart->dims) free(cart->dims);
                OBJ_RELEASE(cart);
                return OMPI_ERR_OUT_OF_RESOURCE;
            }
            {  /* setup the cartesian topology */
                int nprocs = temp_comm->c_local_group->grp_proc_count,
                    rank   = temp_comm->c_local_group->grp_my_rank;

                for (i = 0; i < ndim; ++i) {
                    nprocs /= cart->dims[i];
                    cart->coords[i] = rank / nprocs;
                    rank %= nprocs;
                }
            }
        }

        temp_comm->c_topo           = topo;
        temp_comm->c_topo->mtc.cart = cart;
        temp_comm->c_topo->reorder  = false;
        temp_comm->c_flags         |= OMPI_COMM_CART;
    }

    *new_comm = temp_comm;

    return MPI_SUCCESS;
}
