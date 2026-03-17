/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      UT-Battelle, LLC. All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012-2015 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "opal/prefetch.h"
#include "opal/runtime/opal_params.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/mpool/mpool.h"
#include "ompi/constants.h"
#include "ompi/mca/pml/pml.h"
#include "pml_ob1.h"
#include "pml_ob1_hdr.h"
#include "pml_ob1_rdmafrag.h"
#include "pml_ob1_recvreq.h"
#include "pml_ob1_sendreq.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/memchecker.h"

size_t mca_pml_ob1_rdma_cuda_btls(
    mca_bml_base_endpoint_t* bml_endpoint,
    unsigned char* base,
    size_t size,
    mca_pml_ob1_com_btl_t* rdma_btls);

int mca_pml_ob1_cuda_need_buffers(void * rreq,
                                  mca_btl_base_module_t* btl);

void mca_pml_ob1_cuda_add_ipc_support(struct mca_btl_base_module_t* btl, int32_t flags,
                                      ompi_proc_t* errproc, char* btlinfo);

/**
 * Handle the CUDA buffer.
 */
int mca_pml_ob1_send_request_start_cuda(mca_pml_ob1_send_request_t* sendreq,
                                        mca_bml_base_btl_t* bml_btl,
                                        size_t size) {
    int rc;
#if OPAL_CUDA_GDR_SUPPORT
    /* With some BTLs, switch to RNDV from RGET at large messages */
    if ((sendreq->req_send.req_base.req_convertor.flags & CONVERTOR_CUDA) &&
        (sendreq->req_send.req_bytes_packed > (bml_btl->btl->btl_cuda_rdma_limit - sizeof(mca_pml_ob1_hdr_t)))) {
        return mca_pml_ob1_send_request_start_rndv(sendreq, bml_btl, 0, 0);
    }
#endif /* OPAL_CUDA_GDR_SUPPORT */

    sendreq->req_send.req_base.req_convertor.flags &= ~CONVERTOR_CUDA;
    if (opal_convertor_need_buffers(&sendreq->req_send.req_base.req_convertor) == false) {
        unsigned char *base;
        opal_convertor_get_current_pointer( &sendreq->req_send.req_base.req_convertor, (void**)&base );
        /* Set flag back */
        sendreq->req_send.req_base.req_convertor.flags |= CONVERTOR_CUDA;
        if( 0 != (sendreq->req_rdma_cnt = (uint32_t)mca_pml_ob1_rdma_cuda_btls(
                                                                           sendreq->req_endpoint,
                                                                           base,
                                                                           sendreq->req_send.req_bytes_packed,
                                                                           sendreq->req_rdma))) {
            rc = mca_pml_ob1_send_request_start_rdma(sendreq, bml_btl,
                                                     sendreq->req_send.req_bytes_packed);
            if( OPAL_UNLIKELY(OMPI_SUCCESS != rc) ) {
                mca_pml_ob1_free_rdma_resources(sendreq);
            }
        } else {
            if (bml_btl->btl_flags & MCA_BTL_FLAGS_CUDA_PUT) {
                rc = mca_pml_ob1_send_request_start_rndv(sendreq, bml_btl, size,
                                                         MCA_PML_OB1_HDR_FLAGS_CONTIG);
            } else {
                rc = mca_pml_ob1_send_request_start_rndv(sendreq, bml_btl, 0, 0);
            }
        }
    } else {
        /* Do not send anything with first rendezvous message as copying GPU
         * memory into RNDV message is expensive. */
        sendreq->req_send.req_base.req_convertor.flags |= CONVERTOR_CUDA;
        rc = mca_pml_ob1_send_request_start_rndv(sendreq, bml_btl, 0, 0);
    }
    return rc;
}



size_t mca_pml_ob1_rdma_cuda_btls(
    mca_bml_base_endpoint_t* bml_endpoint,
    unsigned char* base,
    size_t size,
    mca_pml_ob1_com_btl_t* rdma_btls)
{
    int num_btls = mca_bml_base_btl_array_get_size(&bml_endpoint->btl_send);
    double weight_total = 0;
    int num_btls_used = 0, n;

    /* shortcut when there are no rdma capable btls */
    if(num_btls == 0) {
        return 0;
    }

    /* check to see if memory is registered */
    for(n = 0; n < num_btls && num_btls_used < mca_pml_ob1.max_rdma_per_request;
            n++) {
        mca_bml_base_btl_t* bml_btl =
            mca_bml_base_btl_array_get_index(&bml_endpoint->btl_send, n);

        if (bml_btl->btl_flags & MCA_BTL_FLAGS_CUDA_GET) {
            mca_btl_base_registration_handle_t *handle = NULL;

            if( NULL != bml_btl->btl->btl_register_mem ) {
                /* register the memory */
                handle = bml_btl->btl->btl_register_mem (bml_btl->btl, bml_btl->btl_endpoint,
                                                         base, size, MCA_BTL_REG_FLAG_CUDA_GPU_MEM |
                                                         MCA_BTL_REG_FLAG_REMOTE_READ);
            }

            if(NULL == handle)
                continue;

            rdma_btls[num_btls_used].bml_btl = bml_btl;
            rdma_btls[num_btls_used].btl_reg = handle;
            weight_total += bml_btl->btl_weight;
            num_btls_used++;
        }
    }

    /* if we don't use leave_pinned and all BTLs that already have this memory
     * registered amount to less then half of available bandwidth - fall back to
     * pipeline protocol */
    if(0 == num_btls_used || (!opal_leave_pinned && weight_total < 0.5))
        return 0;

    mca_pml_ob1_calc_weighted_length(rdma_btls, num_btls_used, size,
                                     weight_total);

    return num_btls_used;
}

int mca_pml_ob1_cuda_need_buffers(void * rreq,
                                  mca_btl_base_module_t* btl)
{
    mca_pml_ob1_recv_request_t* recvreq = (mca_pml_ob1_recv_request_t*)rreq;
    mca_bml_base_endpoint_t* bml_endpoint = mca_bml_base_get_endpoint (recvreq->req_recv.req_base.req_proc);
    mca_bml_base_btl_t *bml_btl = mca_bml_base_btl_array_find(&bml_endpoint->btl_send, btl);

    /* A btl could be in the rdma list but not in the send list so check there also */
    if (NULL == bml_btl) {
        bml_btl = mca_bml_base_btl_array_find(&bml_endpoint->btl_rdma, btl);
    }
    /* We should always be able to find back the bml_btl based on the btl */
    assert(NULL != bml_btl);

    if ((recvreq->req_recv.req_base.req_convertor.flags & CONVERTOR_CUDA) &&
        (bml_btl->btl_flags & MCA_BTL_FLAGS_CUDA_GET)) {
        recvreq->req_recv.req_base.req_convertor.flags &= ~CONVERTOR_CUDA;
        if(opal_convertor_need_buffers(&recvreq->req_recv.req_base.req_convertor) == true) {
            recvreq->req_recv.req_base.req_convertor.flags |= CONVERTOR_CUDA;
            return true;
        } else {
            recvreq->req_recv.req_base.req_convertor.flags |= CONVERTOR_CUDA;
            return false;
        }
    }
    return true;
}

/*
 * This function enables us to start using RDMA get protocol with GPU buffers.
 * We do this by adjusting the flags in the BML structure.  This is not the
 * best thing, but this may go away if CUDA IPC is supported everywhere in the
 * future. */
void mca_pml_ob1_cuda_add_ipc_support(struct mca_btl_base_module_t* btl, int32_t flags,
                                      ompi_proc_t* errproc, char* btlinfo)
{
    mca_bml_base_endpoint_t* ep;
    int btl_verbose_stream = 0;
    int i;

    assert(NULL != errproc);
    assert(NULL != errproc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML]);
    if (NULL != btlinfo) {
        btl_verbose_stream = *(int *)btlinfo;
    }
    ep = (mca_bml_base_endpoint_t*)errproc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_BML];

    /* Find the corresponding bml and adjust the flag to support CUDA get */
    for( i = 0; i < (int)ep->btl_send.arr_size; i++ ) {
        if( ep->btl_send.bml_btls[i].btl == btl ) {
            ep->btl_send.bml_btls[i].btl_flags |= MCA_BTL_FLAGS_CUDA_GET;
            opal_output_verbose(5, btl_verbose_stream,
                        "BTL %s: rank=%d enabling CUDA IPC "
                        "to rank=%d on node=%s \n",
                        btl->btl_component->btl_version.mca_component_name,
                        OMPI_PROC_MY_NAME->vpid,
                        ((ompi_process_name_t*)&errproc->super.proc_name)->vpid,
                        errproc->super.proc_hostname);
        }
    }
}
