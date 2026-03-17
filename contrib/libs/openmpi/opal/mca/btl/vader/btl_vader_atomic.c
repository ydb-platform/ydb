/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2010-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "btl_vader.h"
#include "btl_vader_frag.h"
#include "btl_vader_endpoint.h"
#include "btl_vader_xpmem.h"

static void mca_btl_vader_sc_emu_aop_complete (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint,
                                                mca_btl_base_descriptor_t *desc, int status)
{
    mca_btl_vader_frag_t *frag = (mca_btl_vader_frag_t *) desc;
    void *local_address = frag->rdma.local_address;
    void *context = frag->rdma.context;
    void *cbdata = frag->rdma.cbdata;
    mca_btl_base_rdma_completion_fn_t cbfunc = frag->rdma.cbfunc;

    /* return the fragment first since the callback may call put/get/amo and could use this fragment */
    MCA_BTL_VADER_FRAG_RETURN(frag);

    cbfunc (btl, endpoint, local_address, NULL, context, cbdata, status);
}

int mca_btl_vader_emu_aop (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint,
                           uint64_t remote_address, mca_btl_base_registration_handle_t *remote_handle,
                           mca_btl_base_atomic_op_t op, uint64_t operand, int flags, int order,
                           mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata)
{
    mca_btl_vader_frag_t *frag;

    frag = mca_btl_vader_rdma_frag_alloc (btl, endpoint, MCA_BTL_VADER_OP_ATOMIC, operand, 0, op, 0, order, flags, NULL,
                                          remote_address, cbfunc, cbcontext, cbdata, mca_btl_vader_sc_emu_aop_complete);
    if (OPAL_UNLIKELY(NULL == frag)) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* send is always successful */
    (void) mca_btl_vader_send (btl, endpoint, &frag->base, MCA_BTL_TAG_VADER);

    return OPAL_SUCCESS;
}

static void mca_btl_vader_sc_emu_afop_complete (mca_btl_base_module_t *btl, mca_btl_base_endpoint_t *endpoint,
                                                mca_btl_base_descriptor_t *desc, int status)
{
    mca_btl_vader_frag_t *frag = (mca_btl_vader_frag_t *) desc;
    mca_btl_vader_sc_emu_hdr_t *hdr;
    void *local_address = frag->rdma.local_address;
    void *context = frag->rdma.context;
    void *cbdata = frag->rdma.cbdata;
    mca_btl_base_rdma_completion_fn_t cbfunc = frag->rdma.cbfunc;

    hdr = (mca_btl_vader_sc_emu_hdr_t *) frag->segments[0].seg_addr.pval;

    *((int64_t *) frag->rdma.local_address) = hdr->operand[0];

    /* return the fragment first since the callback may call put/get/amo and could use this fragment */
    MCA_BTL_VADER_FRAG_RETURN(frag);

    cbfunc (btl, endpoint, local_address, NULL, context, cbdata, status);
}

int mca_btl_vader_emu_afop (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint,
                            void *local_address, uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                            mca_btl_base_registration_handle_t *remote_handle, mca_btl_base_atomic_op_t op,
                            uint64_t operand, int flags, int order, mca_btl_base_rdma_completion_fn_t cbfunc,
                            void *cbcontext, void *cbdata)
{
    mca_btl_vader_frag_t *frag;

    frag = mca_btl_vader_rdma_frag_alloc (btl, endpoint, MCA_BTL_VADER_OP_ATOMIC, operand, 0, op, 0, order, flags,
                                          local_address, remote_address, cbfunc, cbcontext, cbdata,
                                          mca_btl_vader_sc_emu_afop_complete);
    if (OPAL_UNLIKELY(NULL == frag)) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* send is always successful */
    (void) mca_btl_vader_send (btl, endpoint, &frag->base, MCA_BTL_TAG_VADER);

    return OPAL_SUCCESS;
}

int mca_btl_vader_emu_acswap (struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *endpoint,
                              void *local_address, uint64_t remote_address, mca_btl_base_registration_handle_t *local_handle,
                              mca_btl_base_registration_handle_t *remote_handle, uint64_t compare, uint64_t value, int flags,
                              int order, mca_btl_base_rdma_completion_fn_t cbfunc, void *cbcontext, void *cbdata)
{
    mca_btl_vader_frag_t *frag;

    frag = mca_btl_vader_rdma_frag_alloc (btl, endpoint, MCA_BTL_VADER_OP_CSWAP, compare, value, 0, 0, order,
                                          flags, local_address, remote_address, cbfunc, cbcontext, cbdata,
                                          mca_btl_vader_sc_emu_afop_complete);
    if (OPAL_UNLIKELY(NULL == frag)) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* send is always successful */
    (void) mca_btl_vader_send (btl, endpoint, &frag->base, MCA_BTL_TAG_VADER);

    return OPAL_SUCCESS;
}
