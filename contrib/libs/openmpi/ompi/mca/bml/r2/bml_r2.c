/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2008-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Intel, Inc. All rights reserved
 * Copyright (c) 2014      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdlib.h>
#include <string.h>

#include "opal/class/opal_bitmap.h"
#include "opal/util/argv.h"
#include "opal/util/show_help.h"
#include "opal/util/output.h"
#include "ompi/mca/bml/bml.h"
#include "ompi/mca/bml/base/base.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/base.h"
#include "ompi/mca/bml/base/bml_base_btl.h"
#include "bml_r2.h"
#include "ompi/proc/proc.h"

extern mca_bml_base_component_t mca_bml_r2_component;

/* Names of all the BTL components that this BML is aware of */
static char *btl_names = NULL;

static int btl_exclusivity_compare(const void* arg1, const void* arg2)
{
    mca_btl_base_module_t* btl1 = *(struct mca_btl_base_module_t**)arg1;
    mca_btl_base_module_t* btl2 = *(struct mca_btl_base_module_t**)arg2;
    if( btl1->btl_exclusivity > btl2->btl_exclusivity ) {
        return -1;
    } else if (btl1->btl_exclusivity == btl2->btl_exclusivity ) {
        return 0;
    } else {
        return 1;
    }
}

static int mca_bml_r2_add_btls( void )
{
    int i;
    opal_list_t *btls = NULL;
    mca_btl_base_selected_module_t* selected_btl;
    size_t num_btls = 0;
    char **btl_names_argv = NULL;

    if(true == mca_bml_r2.btls_added) {
        return OMPI_SUCCESS;
    }

    /* build an array of r2s and r2 modules */
    btls = &mca_btl_base_modules_initialized;
    num_btls = opal_list_get_size(btls);

    mca_bml_r2.num_btl_modules = 0;
    mca_bml_r2.num_btl_progress = 0;

    mca_bml_r2.btl_modules = (mca_btl_base_module_t **)malloc(sizeof(mca_btl_base_module_t*) * num_btls);
    mca_bml_r2.btl_progress = (mca_btl_base_component_progress_fn_t*)malloc(sizeof(mca_btl_base_component_progress_fn_t) * num_btls);

    if (NULL == mca_bml_r2.btl_modules ||
        NULL == mca_bml_r2.btl_progress) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    OPAL_LIST_FOREACH(selected_btl, btls, mca_btl_base_selected_module_t) {
        mca_btl_base_module_t *btl = selected_btl->btl_module;
        mca_bml_r2.btl_modules[mca_bml_r2.num_btl_modules++] = btl;
        for (i = 0; NULL != btl_names_argv && NULL != btl_names_argv[i]; ++i) {
            if (0 ==
                strcmp(btl_names_argv[i],
                       btl->btl_component->btl_version.mca_component_name)) {
                break;
            }
        }
        if (NULL == btl_names_argv || NULL == btl_names_argv[i]) {
            opal_argv_append_nosize(&btl_names_argv,
                                    btl->btl_component->btl_version.mca_component_name);
        }
    }
    if (NULL != btl_names_argv) {
        btl_names = opal_argv_join(btl_names_argv, ' ');
        opal_argv_free(btl_names_argv);
    } else {
        btl_names = strdup("no devices available");
    }

    /* sort r2 list by exclusivity */
    qsort(mca_bml_r2.btl_modules,
          mca_bml_r2.num_btl_modules,
          sizeof(struct mca_btl_base_module_t*),
          btl_exclusivity_compare);
    mca_bml_r2.btls_added = true;
    return OMPI_SUCCESS;
}

static int btl_bandwidth_compare(const void *v1, const void *v2)
{
    mca_bml_base_btl_t *b1 = (mca_bml_base_btl_t*)v1,
                       *b2 = (mca_bml_base_btl_t*)v2;

    return b2->btl->btl_bandwidth - b1->btl->btl_bandwidth;
}

static void mca_bml_r2_calculate_bandwidth_latency (mca_bml_base_btl_array_t *btl_array, double *total_bandwidth, uint32_t *latency)
{
    const size_t array_length = mca_bml_base_btl_array_get_size (btl_array);

    *latency = UINT_MAX;
    *total_bandwidth = 0.;

    for (size_t i = 0 ; i < array_length ; ++i) {
        mca_bml_base_btl_t *bml_btl = mca_bml_base_btl_array_get_index (btl_array, i);
        mca_btl_base_module_t *btl = bml_btl->btl;
        *total_bandwidth += btl->btl_bandwidth;
        if (btl->btl_latency < *latency) {
            *latency = btl->btl_latency;
        }
    }
}

static mca_bml_base_endpoint_t *mca_bml_r2_allocate_endpoint (ompi_proc_t *proc) {
    mca_bml_base_endpoint_t *bml_endpoint;

    /* allocate bml specific proc data */
    bml_endpoint = OBJ_NEW(mca_bml_base_endpoint_t);
    if (NULL == bml_endpoint) {
        opal_output(0, "%s: unable to allocate resources", __func__);
        return NULL;
    }

    /* preallocate space in array for max number of r2s */
    mca_bml_base_btl_array_reserve(&bml_endpoint->btl_eager, mca_bml_r2.num_btl_modules);
    mca_bml_base_btl_array_reserve(&bml_endpoint->btl_send,  mca_bml_r2.num_btl_modules);
    mca_bml_base_btl_array_reserve(&bml_endpoint->btl_rdma,  mca_bml_r2.num_btl_modules);
    bml_endpoint->btl_max_send_size = -1;
    bml_endpoint->btl_proc = proc;

    bml_endpoint->btl_flags_or = 0;
    return bml_endpoint;
}

static void mca_bml_r2_register_progress (mca_btl_base_module_t *btl, bool hp)
{
    if (NULL != btl->btl_component->btl_progress) {
        bool found = false;
        size_t p;

        for (p = 0 ; p < mca_bml_r2.num_btl_progress ; ++p) {
            if(mca_bml_r2.btl_progress[p] == btl->btl_component->btl_progress) {
                found = true;
                break;
            }
        }

        if (found == false || hp) {
            if (found == false) {
                mca_bml_r2.btl_progress[mca_bml_r2.num_btl_progress++] =
                    btl->btl_component->btl_progress;
            }

            if (hp) {
                opal_progress_register (btl->btl_component->btl_progress);
            } else {
                opal_progress_register_lp (btl->btl_component->btl_progress);
            }
        }
    }
}

static int mca_bml_r2_endpoint_add_btl (struct ompi_proc_t *proc, mca_bml_base_endpoint_t *bml_endpoint,
                                        mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *btl_endpoint)
{
    mca_bml_base_btl_t* bml_btl = NULL;
    int btl_flags = btl->btl_flags;
    bool btl_in_use = false;
    size_t size;

    /* NTH: these flags should have been sanitized by the btl. Once that is verified these
     * checks can be safely removed. */
    if ((btl_flags & MCA_BTL_FLAGS_PUT) && (NULL == btl->btl_put)) {
        opal_output(0, "%s: The PUT flag is specified for"
                    " the %s BTL without any PUT function attached. Discard the flag !",
                    __func__,
                    btl->btl_component->btl_version.mca_component_name);
        btl_flags ^= MCA_BTL_FLAGS_PUT;
    }
    if ((btl_flags & MCA_BTL_FLAGS_GET) && (NULL == btl->btl_get)) {
        opal_output(0, "%s: The GET flag is specified for"
                    " the %s BTL without any GET function attached. Discard the flag !",
                    __func__, btl->btl_component->btl_version.mca_component_name);
        btl_flags ^= MCA_BTL_FLAGS_GET;
    }

    if ((btl_flags & (MCA_BTL_FLAGS_PUT | MCA_BTL_FLAGS_GET | MCA_BTL_FLAGS_SEND)) == 0) {
        /* If no protocol specified, we have 2 choices: we ignore the BTL
         * as we don't know which protocl to use, or we suppose that all
         * BTLs support the send protocol. This is really a btl error as
         * these flags should have been sanitized by the btl. */
        btl_flags |= MCA_BTL_FLAGS_SEND;
    }

    if (btl_flags & MCA_BTL_FLAGS_SEND) {
        /* dont allow an additional BTL with a lower exclusivity ranking */
        size = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_send);
        bml_btl = mca_bml_base_btl_array_get_index (&bml_endpoint->btl_send, size - 1);

        if (!bml_btl || bml_btl->btl->btl_exclusivity <= btl->btl_exclusivity) {
            /* this btl has higher exclusivity than an existing btl or none exists */

            opal_output_verbose(1, opal_btl_base_framework.framework_output,
                                "mca: bml: Using %s btl for send to %s on node %s",
                                btl->btl_component->btl_version.mca_component_name,
                                OMPI_NAME_PRINT(&proc->super.proc_name),
                                proc->super.proc_hostname);

            /* cache the endpoint on the proc */
            if (NULL == bml_btl || (bml_btl->btl->btl_exclusivity <= btl->btl_exclusivity)) {
                bml_btl = mca_bml_base_btl_array_insert (&bml_endpoint->btl_send);
                bml_btl->btl = btl;
                bml_btl->btl_endpoint = btl_endpoint;
                bml_btl->btl_weight = 0;
                bml_btl->btl_flags = btl_flags;

                /**
                 * calculate the bitwise OR of the btl flags
                 */
                bml_endpoint->btl_flags_or |= bml_btl->btl_flags;
            } else {
                opal_output_verbose(20, opal_btl_base_framework.framework_output,
                                    "mca: bml: Not using %s btl for send to %s on node %s "
                                    "because %s btl has higher exclusivity (%d > %d)",
                                    btl->btl_component->btl_version.mca_component_name,
                                    OMPI_NAME_PRINT(&proc->super.proc_name), proc->super.proc_hostname,
                                    bml_btl->btl->btl_component->btl_version.mca_component_name,
                                    bml_btl->btl->btl_exclusivity,
                                    btl->btl_exclusivity);
            }

            btl_in_use = true;
        }
    }

    /* always add rdma endpoints if they support full rdma */
    if (((btl_in_use && (btl_flags & MCA_BTL_FLAGS_RDMA)) ||
         (btl_flags & (MCA_BTL_FLAGS_RDMA | MCA_BTL_FLAGS_ATOMIC_FOPS)) == (MCA_BTL_FLAGS_RDMA | MCA_BTL_FLAGS_ATOMIC_FOPS)) &&
        !((proc->super.proc_arch != ompi_proc_local_proc->super.proc_arch) &&
          (0 == (btl->btl_flags & MCA_BTL_FLAGS_HETEROGENEOUS_RDMA)))) {
        mca_bml_base_btl_t *bml_btl_rdma = mca_bml_base_btl_array_insert(&bml_endpoint->btl_rdma);

        bml_btl_rdma->btl = btl;
        bml_btl_rdma->btl_endpoint = btl_endpoint;
        bml_btl_rdma->btl_weight = 0;
        bml_btl_rdma->btl_flags = btl_flags;

        if (bml_endpoint->btl_pipeline_send_length < btl->btl_rdma_pipeline_send_length) {
            bml_endpoint->btl_pipeline_send_length = btl->btl_rdma_pipeline_send_length;
        }

        if (bml_endpoint->btl_send_limit < btl->btl_min_rdma_pipeline_size) {
            bml_endpoint->btl_send_limit = btl->btl_min_rdma_pipeline_size;
        }

        btl_in_use = true;
    }

    return btl_in_use ? OMPI_SUCCESS : OMPI_ERR_NOT_AVAILABLE;
}

static void mca_bml_r2_compute_endpoint_metrics (mca_bml_base_endpoint_t *bml_endpoint)
{
    double total_bandwidth = 0;
    uint32_t latency;
    size_t n_send, n_rdma;

    /* (1) determine the total bandwidth available across all btls
     *     note that we need to do this here, as we may already have btls configured
     * (2) determine the highest priority ranking for latency
     * (3) compute the maximum amount of bytes that can be send without any
     *     weighting. Once the left over is smaller than this number we will
     *     start using the weight to compute the correct amount.
     */
    n_send = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_send);
    n_rdma = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_rdma);

    /* sort BTLs in descending order according to bandwidth value */
    qsort (bml_endpoint->btl_send.bml_btls, n_send,
           sizeof(mca_bml_base_btl_t), btl_bandwidth_compare);

    bml_endpoint->btl_rdma_index = 0;

    mca_bml_r2_calculate_bandwidth_latency (&bml_endpoint->btl_send, &total_bandwidth, &latency);

    /* (1) set the weight of each btl as a percentage of overall bandwidth
     * (2) copy all btl instances at the highest priority ranking into the
     *     list of btls used for first fragments
     */
    for (size_t n_index = 0 ; n_index < n_send ; ++n_index) {
        mca_bml_base_btl_t *bml_btl =
            mca_bml_base_btl_array_get_index(&bml_endpoint->btl_send, n_index);
        mca_btl_base_module_t *btl = bml_btl->btl;

        /* compute weighting factor for this r2 */
        if(btl->btl_bandwidth > 0) {
            bml_btl->btl_weight = (float)(btl->btl_bandwidth / total_bandwidth);
        } else {
            bml_btl->btl_weight = (float)(1.0 / n_send);
        }

        /* check to see if this r2 is already in the array of r2s
         * used for first fragments - if not add it.
         */
        if(btl->btl_latency == latency) {
            mca_bml_base_btl_t* bml_btl_new =
                mca_bml_base_btl_array_insert(&bml_endpoint->btl_eager);
            *bml_btl_new = *bml_btl;
        }

        /* set endpoint max send size as min of available btls */
        if (bml_endpoint->btl_max_send_size > btl->btl_max_send_size)
            bml_endpoint->btl_max_send_size = btl->btl_max_send_size;
    }

    /* sort BTLs in descending order according to bandwidth value */
    qsort(bml_endpoint->btl_rdma.bml_btls, n_rdma,
          sizeof(mca_bml_base_btl_t), btl_bandwidth_compare);

    mca_bml_r2_calculate_bandwidth_latency (&bml_endpoint->btl_rdma, &total_bandwidth, &latency);

    /* set rdma btl weights */
    for (size_t n_index = 0 ; n_index < n_rdma ; ++n_index) {
        mca_bml_base_btl_t *bml_btl =
            mca_bml_base_btl_array_get_index(&bml_endpoint->btl_rdma, n_index);

        /* compute weighting factor for this r2 */
        if (bml_btl->btl->btl_bandwidth > 0.0) {
            bml_btl->btl_weight = (float)(bml_btl->btl->btl_bandwidth / total_bandwidth);
        } else {
            bml_btl->btl_weight = (float)(1.0 / n_rdma);
        }
    }
}

static int mca_bml_r2_add_proc (struct ompi_proc_t *proc)
{
    mca_bml_base_endpoint_t *bml_endpoint;
    /* at least one btl is in use */
    bool btl_in_use = false;
    int rc;

    if (OPAL_UNLIKELY(NULL == proc)) {
        return OMPI_ERR_BAD_PARAM;
    }

    /* check if this endpoint is already set up */
    if (NULL != proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]) {
        OBJ_RETAIN(proc);
        return OMPI_SUCCESS;
    }

    /* add btls if not already done */
    if (OMPI_SUCCESS != (rc = mca_bml_r2_add_btls())) {
        return rc;
    }

    bml_endpoint = mca_bml_r2_allocate_endpoint (proc);
    if (OPAL_UNLIKELY(NULL == bml_endpoint)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (size_t p_index = 0 ; p_index < mca_bml_r2.num_btl_modules ; ++p_index) {
        mca_btl_base_module_t *btl = mca_bml_r2.btl_modules[p_index];
        struct mca_btl_base_endpoint_t *btl_endpoint = NULL;

        /* if the r2 can reach the destination proc it sets the
         * corresponding bit (proc index) in the reachable bitmap
         * and can return addressing information for each proc
         * that is passed back to the r2 on data transfer calls
         */
        rc = btl->btl_add_procs (btl, 1, (opal_proc_t **) &proc, &btl_endpoint, NULL);
        if (OMPI_SUCCESS != rc || NULL == btl_endpoint) {
            /* This BTL has troubles adding the nodes. Let's continue maybe some other BTL
             * can take care of this task. */
            continue;
        }

        rc = mca_bml_r2_endpoint_add_btl (proc, bml_endpoint, btl, btl_endpoint);
        if (OMPI_SUCCESS != rc) {
            btl->btl_del_procs (btl, 1, (opal_proc_t **) &proc, &btl_endpoint);
        } else {
            mca_bml_r2_register_progress (btl, true);
            btl_in_use = true;
        }
    }

    if (!btl_in_use) {
        proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML] = NULL;
        OBJ_RELEASE(bml_endpoint);
        /* no btl is available for this proc */
        if (mca_bml_r2.show_unreach_errors) {
            opal_show_help ("help-mca-bml-r2.txt", "unreachable proc", true,
                            OMPI_NAME_PRINT(&(ompi_proc_local_proc->super.proc_name)),
                            (NULL != ompi_proc_local_proc->super.proc_hostname ?
                             ompi_proc_local_proc->super.proc_hostname : "unknown!"),
                            OMPI_NAME_PRINT(&(proc->super.proc_name)),
                            (NULL != proc->super.proc_hostname ?
                             proc->super.proc_hostname : "unknown!"),
                            btl_names);
        }

        return OMPI_ERR_UNREACH;
    }

    /* compute metrics for registered btls */
    mca_bml_r2_compute_endpoint_metrics (bml_endpoint);

    /* do it last, for the lazy initialization check in bml_base_get* */
    opal_atomic_wmb();
    proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML] = bml_endpoint;

    return OMPI_SUCCESS;
}

/*
 *   For each proc setup a datastructure that indicates the BTLs
 *   that can be used to reach the destination.
 *
 */

static int mca_bml_r2_add_procs( size_t nprocs,
                                 struct ompi_proc_t** procs,
                                 struct opal_bitmap_t* reachable )
{
    size_t n_new_procs = 0;
    struct mca_btl_base_endpoint_t ** btl_endpoints = NULL;
    struct ompi_proc_t** new_procs = NULL;
    int rc, ret = OMPI_SUCCESS;

    if(0 == nprocs) {
        return OMPI_SUCCESS;
    }

    if(OMPI_SUCCESS != (rc = mca_bml_r2_add_btls()) ) {
        return rc;
    }

    /* Select only the procs that don't yet have the BML proc struct. This prevent
     * us from calling btl->add_procs several times on the same destination proc.
     */
    for (size_t p_index = 0 ; p_index < nprocs ; ++p_index) {
        struct ompi_proc_t* proc = procs[p_index];

        if(NULL !=  proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]) {
            continue;  /* go to the next proc */
        }
        /* Allocate the new_procs on demand */
        if( NULL == new_procs ) {
            new_procs = (struct ompi_proc_t **)malloc(nprocs * sizeof(struct ompi_proc_t *));
            if( NULL == new_procs ) {
                return OMPI_ERR_OUT_OF_RESOURCE;
            }
        }
        OBJ_RETAIN(proc);
        new_procs[n_new_procs++] = proc;
    }

    if ( 0 == n_new_procs ) {
        return OMPI_SUCCESS;
    }

    /* Starting from here we only work on the unregistered procs */
    procs = new_procs;
    nprocs = n_new_procs;

    /* attempt to add all procs to each r2 */
    btl_endpoints = (struct mca_btl_base_endpoint_t **)
        malloc(nprocs * sizeof(struct mca_btl_base_endpoint_t*));
    if (NULL == btl_endpoints) {
        free(new_procs);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (size_t p_index = 0 ; p_index < mca_bml_r2.num_btl_modules ; ++p_index) {
        mca_btl_base_module_t *btl = mca_bml_r2.btl_modules[p_index];
        int btl_inuse = 0;

        /* if the r2 can reach the destination proc it sets the
         * corresponding bit (proc index) in the reachable bitmap
         * and can return addressing information for each proc
         * that is passed back to the r2 on data transfer calls
         */
        opal_bitmap_clear_all_bits(reachable);
        memset(btl_endpoints, 0, nprocs *sizeof(struct mca_btl_base_endpoint_t*));

        rc = btl->btl_add_procs(btl, n_new_procs, (opal_proc_t**)new_procs, btl_endpoints, reachable);
        if (OMPI_SUCCESS != rc) {
            /* This BTL encountered an error while adding procs. Continue in case some other
             * BTL(s) can be used. */
            continue;
        }

        /* for each proc that is reachable */
        for (size_t p = 0 ; p < n_new_procs ; ++p) {
            if (!opal_bitmap_is_set_bit(reachable, p)) {
                continue;
            }

            ompi_proc_t *proc = new_procs[p];
            mca_bml_base_endpoint_t *bml_endpoint =
                (mca_bml_base_endpoint_t *) proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML];

            if (NULL == bml_endpoint) {
                bml_endpoint = mca_bml_r2_allocate_endpoint (proc);
                proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML] = bml_endpoint;
                if (NULL == bml_endpoint) {
                    free(btl_endpoints);
                    free(new_procs);
                    return OPAL_ERR_OUT_OF_RESOURCE;
                }
            }

            rc = mca_bml_r2_endpoint_add_btl (proc, bml_endpoint, btl, btl_endpoints[p]);
            if (OMPI_SUCCESS != rc) {
                btl->btl_del_procs(btl, 1, (opal_proc_t**)&proc, &btl_endpoints[p]);
                continue;
            }

            /* This BTL is in use, allow the progress registration */
            btl_inuse++;
        }

        mca_bml_r2_register_progress (btl, !!(btl_inuse));
    }

    free(btl_endpoints);

    /* iterate back through procs and compute metrics for registered r2s */
    for (size_t p = 0; p < n_new_procs ; ++p) {
        mca_bml_base_endpoint_t *bml_endpoint =
            (mca_bml_base_endpoint_t *) new_procs[p]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML];

        /* skip over procs w/ no btl's registered */
        if (NULL != bml_endpoint) {
            mca_bml_r2_compute_endpoint_metrics (bml_endpoint);
        }
    }

    /* see if we have a connection to everyone else */
    for(size_t p = 0; p < n_new_procs ; ++p) {
        ompi_proc_t *proc = new_procs[p];

        if (NULL == proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]) {
            ret = OMPI_ERR_UNREACH;
            if (mca_bml_r2.show_unreach_errors) {
                opal_show_help("help-mca-bml-r2.txt", "unreachable proc", true,
                               OMPI_NAME_PRINT(&(ompi_proc_local_proc->super.proc_name)),
                               (NULL != ompi_proc_local_proc->super.proc_hostname ?
                                ompi_proc_local_proc->super.proc_hostname : "unknown!"),
                               OMPI_NAME_PRINT(&(proc->super.proc_name)),
                               (NULL != proc->super.proc_hostname ?
                                proc->super.proc_hostname : "unknown!"),
                               btl_names);
            }

            break;
        }
    }

    free(new_procs);

    return ret;
}

/*
 * iterate through each proc and notify any BTLs associated
 * with the proc that it is/has gone away
 */

static int mca_bml_r2_del_procs(size_t nprocs,
                                struct ompi_proc_t** procs)
{
    for (size_t p = 0 ; p < nprocs ; ++p) {
        ompi_proc_t *proc = procs[p];
        mca_bml_base_endpoint_t *bml_endpoint =
            (mca_bml_base_endpoint_t*) proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML];

        if (!bml_endpoint) {
            /* NTH: I would think this is a developer bug and should not be ignored. */
            continue;
        }

        /* notify each btl that the proc is going away */
        size_t f_size = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_send);
        for (size_t f_index = 0 ; f_index < f_size ; ++f_index) {
            mca_bml_base_btl_t* bml_btl = mca_bml_base_btl_array_get_index(&bml_endpoint->btl_send, f_index);
            mca_btl_base_module_t *btl = bml_btl->btl;

            int rc = btl->btl_del_procs (btl, 1, (opal_proc_t **) &proc, &bml_btl->btl_endpoint);
            if (OPAL_SUCCESS != rc) {
                return rc;
            }

            /* The reference stored in btl_eager and btl_rdma will automatically
             * dissapear once the btl_array destructor is called. Thus, there is
             * no need for extra cleaning here.
             */
        }

        /* some btl endpoints may only be in the btl_rdma array. call del_procs on those as well */
        size_t r_size = mca_bml_base_btl_array_get_size (&bml_endpoint->btl_rdma);
        for (size_t r_index = 0 ; r_index < r_size ; ++r_index) {
            mca_bml_base_btl_t *rdma_btl = mca_bml_base_btl_array_get_index (&bml_endpoint->btl_rdma, r_index);
            mca_btl_base_module_t *btl = rdma_btl->btl;
            bool needs_del = true;

            for (size_t f_index = 0 ; f_index < f_size ; ++f_index) {
                mca_bml_base_btl_t *bml_btl = mca_bml_base_btl_array_get_index (&bml_endpoint->btl_send, f_index);
                if (bml_btl->btl_endpoint == rdma_btl->btl_endpoint) {
                    needs_del = false;
                    break;
                }
            }

            if (needs_del) {
                int rc = btl->btl_del_procs (btl, 1, (opal_proc_t **) &proc, &rdma_btl->btl_endpoint);
                if (OPAL_SUCCESS != rc) {
                    return rc;
                }
            }
        }

        proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML] = NULL;

        /* release the bml endpoint's reference to the proc */
        OBJ_RELEASE(proc);

        /* do any required cleanup */
        OBJ_RELEASE(bml_endpoint);
    }

    return OMPI_SUCCESS;
}

static inline int bml_r2_remove_btl_progress(mca_btl_base_module_t* btl)
{
    unsigned int p;

    if(NULL == btl->btl_component->btl_progress) {
        return OMPI_SUCCESS;
    }
    for(p = 0; p < mca_bml_r2.num_btl_progress; p++) {
        if(btl->btl_component->btl_progress != mca_bml_r2.btl_progress[p])
            continue;
        opal_progress_unregister( btl->btl_component->btl_progress );
        if( p < (mca_bml_r2.num_btl_progress-1) ) {
            mca_bml_r2.btl_progress[p] = mca_bml_r2.btl_progress[mca_bml_r2.num_btl_progress-1];
        }
        mca_bml_r2.num_btl_progress--;
        return OMPI_SUCCESS;
    }
    return OMPI_ERR_NOT_FOUND;
}

static int mca_bml_r2_del_proc_btl(ompi_proc_t* proc, mca_btl_base_module_t* btl)
{
    mca_bml_base_endpoint_t* ep = (mca_bml_base_endpoint_t*)proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML];
    mca_bml_base_btl_t* bml_btl;
    mca_btl_base_module_t* ep_btl;
    double total_bandwidth = 0;
    size_t b;

    if(NULL == ep)
        return OMPI_SUCCESS;

    /* remove btl from eager list */
    mca_bml_base_btl_array_remove(&ep->btl_eager, btl);

    /* remove btl from send list */
    if(mca_bml_base_btl_array_remove(&ep->btl_send, btl)) {

        /* compute total_bandwidth and
           reset max_send_size to the min of all btl's */
        total_bandwidth = 0;
        ep->btl_max_send_size = -1;
        for(b=0; b< mca_bml_base_btl_array_get_size(&ep->btl_send); b++) {
            bml_btl = mca_bml_base_btl_array_get_index(&ep->btl_send, b);
            ep_btl = bml_btl->btl;

            total_bandwidth += ep_btl->btl_bandwidth;
            if (ep->btl_max_send_size > ep_btl->btl_max_send_size) {
                ep->btl_max_send_size = ep_btl->btl_max_send_size;
            }
        }

        /* compute weighting factor for this btl */
        for(b=0; b< mca_bml_base_btl_array_get_size(&ep->btl_send); b++) {
            bml_btl = mca_bml_base_btl_array_get_index(&ep->btl_send, b);
            ep_btl = bml_btl->btl;

            if(ep_btl->btl_bandwidth > 0) {
                bml_btl->btl_weight = (float)(ep_btl->btl_bandwidth / total_bandwidth);
            } else {
                bml_btl->btl_weight = (float)(1.0 / mca_bml_base_btl_array_get_size(&ep->btl_send));
            }
        }
    }

    /* remove btl from RDMA list */
    if(mca_bml_base_btl_array_remove(&ep->btl_rdma, btl)) {

        /* compute total bandwidth */
        total_bandwidth = 0;
        ep->btl_pipeline_send_length = 0;
        ep->btl_send_limit = 0;
        for(b=0; b< mca_bml_base_btl_array_get_size(&ep->btl_rdma); b++) {
            bml_btl = mca_bml_base_btl_array_get_index(&ep->btl_rdma, b);
            ep_btl = bml_btl->btl;

            /* update aggregate endpoint info */
            total_bandwidth += ep_btl->btl_bandwidth;
            if (ep->btl_pipeline_send_length < ep_btl->btl_rdma_pipeline_send_length) {
                ep->btl_pipeline_send_length = ep_btl->btl_rdma_pipeline_send_length;
            }
            if (ep->btl_send_limit < ep_btl->btl_min_rdma_pipeline_size) {
                ep->btl_send_limit = ep_btl->btl_min_rdma_pipeline_size;
            }
        }

        /* compute weighting factor for this btl */
        for(b=0; b< mca_bml_base_btl_array_get_size(&ep->btl_rdma); b++) {
            bml_btl = mca_bml_base_btl_array_get_index(&ep->btl_rdma, b);
            ep_btl = bml_btl->btl;

            if(ep_btl->btl_bandwidth > 0) {
                bml_btl->btl_weight = (float)(ep_btl->btl_bandwidth / total_bandwidth);
            } else {
                bml_btl->btl_weight = (float)(1.0 / mca_bml_base_btl_array_get_size(&ep->btl_rdma));
            }
        }
    }

    return OMPI_SUCCESS;
}

int mca_bml_r2_finalize( void )
{
    ompi_proc_t** procs;
    size_t p, num_procs;
    opal_list_item_t* w_item;

    if (NULL != btl_names) {
        free(btl_names);
        btl_names = NULL;
    }

    /* Similar to mca_bml_r2_del_btl ... */
    procs = ompi_proc_all(&num_procs);
    if(NULL == procs)
        goto CLEANUP;

    for (w_item =  opal_list_get_first(&mca_btl_base_modules_initialized);
         w_item != opal_list_get_end(&mca_btl_base_modules_initialized);
         w_item =  opal_list_get_next(w_item)) {
        mca_btl_base_selected_module_t *sm = (mca_btl_base_selected_module_t *) w_item;
        mca_btl_base_module_t* btl = sm->btl_module;

        /* unregister the BTL progress function if any */
        bml_r2_remove_btl_progress(btl);

        /* dont use this btl for any peers */
        for( p = 0; p < num_procs; p++ ) {
            ompi_proc_t* proc = procs[p];
            mca_bml_r2_del_proc_btl(proc, sm->btl_module);
        }
    }
    /* Release the procs as the ompi_proc_all increase their ref_count */
    for( p = 0; p < num_procs; p++ ) {
        OBJ_RELEASE(procs[p]);
    }
    free(procs);

 CLEANUP:
    mca_bml_r2.num_btl_modules = 0;
    mca_bml_r2.num_btl_progress = 0;

    if( NULL != mca_bml_r2.btl_modules) {
        free(mca_bml_r2.btl_modules);
        mca_bml_r2.btl_modules = NULL;
    }
    if( NULL != mca_bml_r2.btl_progress ) {
        free(mca_bml_r2.btl_progress);
        mca_bml_r2.btl_progress = NULL;
    }

    /* Do not close the BTL base here; the BML upper layer will take
       care of that. */

    return OMPI_SUCCESS;
}


/*
 *  (1) Remove btl from each bml endpoint
 *  (2) Remove btl from the global list
 */

static int mca_bml_r2_del_btl(mca_btl_base_module_t* btl)
{
    ompi_proc_t** procs;
    size_t i, m, p, num_procs;
    opal_list_item_t* item;
    mca_btl_base_module_t** modules;
    bool found = false;

    if(opal_list_get_size(&mca_btl_base_modules_initialized) == 2) {
        opal_output(0, "only one BTL left, can't failover");
        return OMPI_SUCCESS;
    }

    procs = ompi_proc_all(&num_procs);
    if(NULL == procs)
        return OMPI_SUCCESS;

    /* Get rid of the associated progress function */
    bml_r2_remove_btl_progress(btl);

    /* dont use this btl for any peers */
    for( p = 0; p < num_procs; p++ ) {
        ompi_proc_t* proc = procs[p];
        mca_bml_r2_del_proc_btl(proc, btl);
    }

    /* remove from the btl list */
    for (item =  opal_list_get_first(&mca_btl_base_modules_initialized);
         item != opal_list_get_end(&mca_btl_base_modules_initialized);
         item =  opal_list_get_next(item)) {
        mca_btl_base_selected_module_t *sm = (mca_btl_base_selected_module_t *) item;
        if(sm->btl_module == btl) {
            opal_list_remove_item(&mca_btl_base_modules_initialized, item);
            free(sm);
            found = true;
            break;
        }
    }
    if(!found) {
        /* doesn't even exist */
        goto CLEANUP;
    }
    /* remove from bml list */
    modules = (mca_btl_base_module_t**)malloc(sizeof(mca_btl_base_module_t*) * (mca_bml_r2.num_btl_modules-1));
    for(i=0,m=0; i<mca_bml_r2.num_btl_modules; i++) {
        if(mca_bml_r2.btl_modules[i] != btl) {
            modules[m++] = mca_bml_r2.btl_modules[i];
        }
    }
    free(mca_bml_r2.btl_modules);
    mca_bml_r2.btl_modules = modules;
    mca_bml_r2.num_btl_modules = m;

    /* cleanup */
    btl->btl_finalize(btl);
CLEANUP:
    /* Decrease the ref_count increased by the call to ompi_proc_all */
    for( p = 0; p < num_procs; p++ ) {
        OBJ_RELEASE(procs[p]);
    }
    free(procs);
    return OMPI_SUCCESS;
}

static int mca_bml_r2_add_btl(mca_btl_base_module_t* btl)
{
    return OMPI_ERR_NOT_IMPLEMENTED;
}


/*
 *  Register callback w/ all active btls
 */
static int mca_bml_r2_register( mca_btl_base_tag_t tag,
                                mca_btl_base_module_recv_cb_fn_t cbfunc,
                                void* data )
{
    mca_btl_base_active_message_trigger[tag].cbfunc = cbfunc;
    mca_btl_base_active_message_trigger[tag].cbdata = data;
    /* Give an oportunity to the BTLs to do something special
     * for each registration.
     */
    {
        int i, rc;
        mca_btl_base_module_t *btl;

        for(i = 0; i < (int)mca_bml_r2.num_btl_modules; i++) {
            btl = mca_bml_r2.btl_modules[i];
            if( NULL == btl->btl_register )
                continue;
            rc = btl->btl_register(btl, tag, cbfunc, data);
            if(OMPI_SUCCESS != rc) {
                return rc;
            }
        }
    }

    return OMPI_SUCCESS;
}


/*
 *  Register an error handler with/ all active btls
 *   if they support error handlers..
 */

static int mca_bml_r2_register_error( mca_btl_base_module_error_cb_fn_t  cbfunc)
{
    uint32_t  i;
    int rc;
    mca_btl_base_module_t *btl;
    uint32_t ver;

    for(i = 0; i < mca_bml_r2.num_btl_modules; i++) {
        btl = mca_bml_r2.btl_modules[i];
        /* this wont work for version numbers greater than 256... seems
           reasonable.. */
        ver = btl->btl_component->btl_version.mca_type_major_version << 16 |
            btl->btl_component->btl_version.mca_type_minor_version << 8 |
            btl->btl_component->btl_version.mca_type_release_version;
        /* is version number greater than or equal to 1.0.1? */
        if(ver >= ((1 << 16) |  (0 << 8) | 1) &&
           NULL != btl->btl_register_error) {
            rc = btl->btl_register_error(btl, cbfunc);
            if(OMPI_SUCCESS != rc) {
                return rc;
            }
        }
    }
    return OMPI_SUCCESS;
}


int mca_bml_r2_component_fini(void)
{
    return OMPI_SUCCESS;
}

mca_bml_r2_module_t mca_bml_r2 = {
    .super = {
        .bml_component = &mca_bml_r2_component,
        .bml_add_proc = mca_bml_r2_add_proc,
        .bml_add_procs = mca_bml_r2_add_procs,
        .bml_del_procs = mca_bml_r2_del_procs,
        .bml_add_btl = mca_bml_r2_add_btl,
        .bml_del_btl = mca_bml_r2_del_btl,
        .bml_del_proc_btl = mca_bml_r2_del_proc_btl,
        .bml_register = mca_bml_r2_register,
        .bml_register_error = mca_bml_r2_register_error,
        .bml_finalize = mca_bml_r2_finalize,
        .bml_ft_event = mca_bml_r2_ft_event,
    },
};

