/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

#include "ompi_config.h"
#include "ompi/constants.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/bml/bml.h"
#include "opal/mca/mpool/mpool.h"
#include "opal/runtime/opal_params.h"
#include "pml_ob1.h"
#include "pml_ob1_rdma.h"

/*
 * Check to see if memory is registered or can be registered. Build a
 * set of registrations on the request.
 */

size_t mca_pml_ob1_rdma_btls(
    mca_bml_base_endpoint_t* bml_endpoint,
    unsigned char* base,
    size_t size,
    mca_pml_ob1_com_btl_t* rdma_btls)
{
    int num_btls = mca_bml_base_btl_array_get_size(&bml_endpoint->btl_rdma);
    int num_eager_btls = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_eager);
    double weight_total = 0;
    int num_btls_used = 0;

    /* shortcut when there are no rdma capable btls */
    if(num_btls == 0) {
        return 0;
    }

    /* check to see if memory is registered */
    for (int n = 0; n < num_btls && num_btls_used < mca_pml_ob1.max_rdma_per_request; n++) {
        mca_bml_base_btl_t* bml_btl =
            mca_bml_base_btl_array_get_index(&bml_endpoint->btl_rdma,
                    (bml_endpoint->btl_rdma_index + n) % num_btls);
        mca_btl_base_registration_handle_t *reg_handle = NULL;
        mca_btl_base_module_t *btl = bml_btl->btl;
        /* NTH: go ahead and use an rdma btl if is the only one */
        bool ignore = !mca_pml_ob1.use_all_rdma;

        /* do not use rdma btls that are not in the eager list. this is necessary to avoid using
         * btls that exist on the endpoint only to support RMA. */
        for (int i = 0 ; i < num_eager_btls && ignore ; ++i) {
            mca_bml_base_btl_t *eager_btl = mca_bml_base_btl_array_get_index (&bml_endpoint->btl_eager, i);
            if (eager_btl->btl_endpoint == bml_btl->btl_endpoint) {
                ignore = false;
                break;
            }
        }

        if (ignore) {
            continue;
        }

        if (btl->btl_register_mem) {
            /* do not use the RDMA protocol with this btl if 1) leave pinned is disabled,
             * 2) the btl supports put, and 3) the fragment is larger than the minimum
             * pipeline size specified by the BTL */
            if (!opal_leave_pinned && (btl->btl_flags & MCA_BTL_FLAGS_PUT) &&
                  size > btl->btl_min_rdma_pipeline_size) {
                continue;
            }

            /* try to register the memory region with the btl */
            reg_handle = btl->btl_register_mem (btl, bml_btl->btl_endpoint, base,
                                                size, MCA_BTL_REG_FLAG_REMOTE_READ);
            if (NULL == reg_handle) {
                /* btl requires registration but the registration failed */
                continue;
            }
        } /* else no registration is needed with this btl */

        rdma_btls[num_btls_used].bml_btl = bml_btl;
        rdma_btls[num_btls_used].btl_reg = reg_handle;
        weight_total += bml_btl->btl_weight;
        num_btls_used++;
    }

    /* if we don't use leave_pinned and all BTLs that already have this memory
     * registered amount to less then half of available bandwidth - fall back to
     * pipeline protocol */
    if (0 == num_btls_used || (!opal_leave_pinned && weight_total < 0.5))
        return 0;

    mca_pml_ob1_calc_weighted_length(rdma_btls, num_btls_used, size,
                                     weight_total);

    bml_endpoint->btl_rdma_index = (bml_endpoint->btl_rdma_index + 1) % num_btls;
    return num_btls_used;
}

size_t mca_pml_ob1_rdma_pipeline_btls_count (mca_bml_base_endpoint_t* bml_endpoint)
{
    int num_btls = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_rdma);
    int num_eager_btls = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_eager);
    int rdma_count = 0;

    for(int i = 0; i < num_btls && i < mca_pml_ob1.max_rdma_per_request; ++i) {
        mca_bml_base_btl_t *bml_btl = mca_bml_base_btl_array_get_next(&bml_endpoint->btl_rdma);
        /* NTH: go ahead and use an rdma btl if is the only one */
        bool ignore = !mca_pml_ob1.use_all_rdma;

        for (int i = 0 ; i < num_eager_btls && ignore ; ++i) {
            mca_bml_base_btl_t *eager_btl = mca_bml_base_btl_array_get_index (&bml_endpoint->btl_eager, i);
            if (eager_btl->btl_endpoint == bml_btl->btl_endpoint) {
                ignore = false;
                break;
            }
        }

        if (!ignore) {
            ++rdma_count;
        }
    }

    return rdma_count;
}

size_t mca_pml_ob1_rdma_pipeline_btls( mca_bml_base_endpoint_t* bml_endpoint,
                                       size_t size,
                                       mca_pml_ob1_com_btl_t* rdma_btls )
{
    int num_btls = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_rdma);
    int num_eager_btls = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_eager);
    double weight_total = 0;
    int rdma_count = 0;

    for(int i = 0; i < num_btls && i < mca_pml_ob1.max_rdma_per_request; i++) {
        mca_bml_base_btl_t *bml_btl = mca_bml_base_btl_array_get_next(&bml_endpoint->btl_rdma);
        /* NTH: go ahead and use an rdma btl if is the only one */
        bool ignore = !mca_pml_ob1.use_all_rdma;

        for (int i = 0 ; i < num_eager_btls && ignore ; ++i) {
            mca_bml_base_btl_t *eager_btl = mca_bml_base_btl_array_get_index (&bml_endpoint->btl_eager, i);
            if (eager_btl->btl_endpoint == bml_btl->btl_endpoint) {
                ignore = false;
                break;
            }
        }

        if (ignore) {
            continue;
        }

        rdma_btls[rdma_count].bml_btl = bml_btl;
        rdma_btls[rdma_count++].btl_reg = NULL;

        weight_total += bml_btl->btl_weight;
    }

    mca_pml_ob1_calc_weighted_length (rdma_btls, rdma_count, size, weight_total);

    return rdma_count;
}
