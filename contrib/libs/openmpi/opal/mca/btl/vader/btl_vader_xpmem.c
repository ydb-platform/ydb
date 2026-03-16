/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "btl_vader.h"

#include "opal/include/opal/align.h"
#include "opal/mca/memchecker/base/base.h"

#if OPAL_BTL_VADER_HAVE_XPMEM

int mca_btl_vader_xpmem_init (void)
{
    mca_btl_vader_component.my_seg_id = xpmem_make (0, VADER_MAX_ADDRESS, XPMEM_PERMIT_MODE, (void *)0666);
    if (-1 == mca_btl_vader_component.my_seg_id) {
        return OPAL_ERR_NOT_AVAILABLE;
    }

    mca_btl_vader.super.btl_get = mca_btl_vader_get_xpmem;
    mca_btl_vader.super.btl_put = mca_btl_vader_put_xpmem;

    return OPAL_SUCCESS;
}

struct vader_check_reg_ctx_t {
    mca_btl_base_endpoint_t *ep;
    mca_rcache_base_registration_t **reg;
    uintptr_t base;
    uintptr_t bound;
};
typedef struct vader_check_reg_ctx_t vader_check_reg_ctx_t;

static int vader_check_reg (mca_rcache_base_registration_t *reg, void *ctx)
{
    vader_check_reg_ctx_t *vader_ctx = (vader_check_reg_ctx_t *) ctx;

    if ((intptr_t) reg->alloc_base != vader_ctx->ep->peer_smp_rank ||
        (reg->flags & MCA_RCACHE_FLAGS_PERSIST)) {
        /* ignore this registration */
        return OPAL_SUCCESS;
    }

    vader_ctx->reg[0] = reg;

    if (vader_ctx->bound <= (uintptr_t) reg->bound && vader_ctx->base >= (uintptr_t) reg->base) {
        opal_atomic_add (&reg->ref_count, 1);
        return 1;
    }

    return 2;
}

void vader_return_registration (mca_rcache_base_registration_t *reg, struct mca_btl_base_endpoint_t *ep)
{
    mca_rcache_base_vma_module_t *vma_module =  mca_btl_vader_component.vma_module;
    int32_t ref_count;

    ref_count = opal_atomic_add_fetch_32 (&reg->ref_count, -1);
    if (OPAL_UNLIKELY(0 == ref_count && !(reg->flags & MCA_RCACHE_FLAGS_PERSIST))) {
        mca_rcache_base_vma_delete (vma_module, reg);

        opal_memchecker_base_mem_noaccess (reg->rcache_context, (uintptr_t)(reg->bound - reg->base));
        (void)xpmem_detach (reg->rcache_context);
        OBJ_RELEASE (reg);
    }
}

/* look up the remote pointer in the peer rcache and attach if
 * necessary */
mca_rcache_base_registration_t *vader_get_registation (struct mca_btl_base_endpoint_t *ep, void *rem_ptr,
                                                       size_t size, int flags, void **local_ptr)
{
    mca_rcache_base_vma_module_t *vma_module = mca_btl_vader_component.vma_module;
    uint64_t attach_align = 1 << mca_btl_vader_component.log_attach_align;
    mca_rcache_base_registration_t *reg = NULL;
    vader_check_reg_ctx_t check_ctx = {.ep = ep, .reg = &reg};
    xpmem_addr_t xpmem_addr;
    uintptr_t base, bound;
    int rc;

    base = OPAL_DOWN_ALIGN((uintptr_t) rem_ptr, attach_align, uintptr_t);
    bound = OPAL_ALIGN((uintptr_t) rem_ptr + size - 1, attach_align, uintptr_t) + 1;
    if (OPAL_UNLIKELY(bound > VADER_MAX_ADDRESS)) {
        bound = VADER_MAX_ADDRESS;
    }

    check_ctx.base = base;
    check_ctx.bound = bound;

    /* several segments may match the base pointer */
    rc = mca_rcache_base_vma_iterate (vma_module, (void *) base, bound - base, true, vader_check_reg, &check_ctx);
    if (2 == rc) {
        /* remove this pointer from the rcache and decrement its reference count
           (so it is detached later) */
        mca_rcache_base_vma_delete (vma_module, reg);

        /* start the new segment from the lower of the two bases */
        base = (uintptr_t) reg->base < base ? (uintptr_t) reg->base : base;

        /* remove the last reference to this registration */
        vader_return_registration (reg, ep);

        reg = NULL;
    }

    if (NULL == reg) {
        reg = OBJ_NEW(mca_rcache_base_registration_t);
        if (OPAL_LIKELY(NULL != reg)) {
            /* stick around for awhile */
            reg->ref_count = 2;
            reg->base  = (unsigned char *) base;
            reg->bound = (unsigned char *) bound;
            reg->flags = flags;
            reg->alloc_base = (void *) (intptr_t) ep->peer_smp_rank;

#if defined(HAVE_SN_XPMEM_H)
            xpmem_addr.id     = ep->segment_data.xpmem.apid;
#else
            xpmem_addr.apid   = ep->segment_data.xpmem.apid;
#endif
            xpmem_addr.offset = base;

            reg->rcache_context = xpmem_attach (xpmem_addr, bound - base, NULL);
            if (OPAL_UNLIKELY((void *)-1 == reg->rcache_context)) {
                OBJ_RELEASE(reg);
                return NULL;
            }

            opal_memchecker_base_mem_defined (reg->rcache_context, bound - base);

            if (!(flags & MCA_RCACHE_FLAGS_PERSIST)) {
                mca_rcache_base_vma_insert (vma_module, reg, 0);
            }
        }
    }

    opal_atomic_wmb ();
    *local_ptr = (void *) ((uintptr_t) reg->rcache_context +
                           (ptrdiff_t)((uintptr_t) rem_ptr - (uintptr_t) reg->base));

    return reg;
}

static int mca_btl_vader_endpoint_xpmem_rcache_cleanup (mca_rcache_base_registration_t *reg, void *ctx)
{
    mca_btl_vader_endpoint_t *ep = (mca_btl_vader_endpoint_t *) ctx;
    if ((intptr_t) reg->alloc_base == ep->peer_smp_rank) {
        /* otherwise dereg will fail on assert */
        reg->ref_count = 0;
        OBJ_RELEASE(reg);
    }

    return OPAL_SUCCESS;
}

void mca_btl_vader_xpmem_cleanup_endpoint (struct mca_btl_base_endpoint_t *ep)
{
    /* clean out the registration cache */
    (void) mca_rcache_base_vma_iterate (mca_btl_vader_component.vma_module,
                                        NULL, (size_t) -1, true,
                                        mca_btl_vader_endpoint_xpmem_rcache_cleanup,
                                        (void *) ep);
    if (ep->segment_base) {
        xpmem_release (ep->segment_data.xpmem.apid);
        ep->segment_data.xpmem.apid = 0;
    }
}

#endif /* OPAL_BTL_VADER_HAVE_XPMEM */
