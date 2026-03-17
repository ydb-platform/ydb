/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "btl_vader.h"
#include "btl_vader_frag.h"

#if OPAL_HAVE_ATOMIC_MATH_64
static void mca_btl_vader_sc_emu_atomic_64 (int64_t *operand, volatile int64_t *addr, mca_btl_base_atomic_op_t op)
{
    int64_t result = 0;

    switch (op) {
    case MCA_BTL_ATOMIC_ADD:
        result = opal_atomic_fetch_add_64 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_AND:
        result = opal_atomic_fetch_and_64 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_OR:
        result = opal_atomic_fetch_or_64 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_XOR:
        result = opal_atomic_fetch_xor_64 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_SWAP:
        result = opal_atomic_swap_64 (addr, *operand);
        break;
#if OPAL_HAVE_ATOMIC_MIN_64
    case MCA_BTL_ATOMIC_MIN:
        result = opal_atomic_fetch_min_64 (addr, *operand);
        break;
#endif
#if OPAL_HAVE_ATOMIC_MAX_64
    case MCA_BTL_ATOMIC_MAX:
        result = opal_atomic_fetch_max_64 (addr, *operand);
        break;
#endif
    default:
        assert (0);
    }

    *operand = result;
}
#endif

#if OPAL_HAVE_ATOMIC_MATH_32
static void mca_btl_vader_sc_emu_atomic_32 (int32_t *operand, volatile int32_t *addr, mca_btl_base_atomic_op_t op)
{
    int32_t result = 0;

    switch (op) {
    case MCA_BTL_ATOMIC_ADD:
        result = opal_atomic_fetch_add_32 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_AND:
        result = opal_atomic_fetch_and_32 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_OR:
        result = opal_atomic_fetch_or_32 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_XOR:
        result = opal_atomic_fetch_xor_32 (addr, *operand);
        break;
    case MCA_BTL_ATOMIC_SWAP:
        result = opal_atomic_swap_32 (addr, *operand);
        break;
#if OPAL_HAVE_ATOMIC_MIN_32
    case MCA_BTL_ATOMIC_MIN:
        result = opal_atomic_fetch_min_32 (addr, *operand);
        break;
#endif
#if OPAL_HAVE_ATOMIC_MAX_32
    case MCA_BTL_ATOMIC_MAX:
        result = opal_atomic_fetch_max_32 (addr, *operand);
        break;
#endif
    default:
        assert (0);
    }

    *operand = result;
}
#endif

static void mca_btl_vader_sc_emu_rdma (mca_btl_base_module_t *btl, mca_btl_base_tag_t tag, mca_btl_base_descriptor_t *desc, void *ctx)
{
    mca_btl_vader_sc_emu_hdr_t *hdr = (mca_btl_vader_sc_emu_hdr_t *) desc->des_segments[0].seg_addr.pval;
    size_t size = desc->des_segments[0].seg_len - sizeof (*hdr);
    void *data = (void *)(hdr + 1);

    switch (hdr->type) {
    case MCA_BTL_VADER_OP_PUT:
        memcpy ((void *) hdr->addr, data, size);
        break;
    case MCA_BTL_VADER_OP_GET:
        memcpy (data, (void *) hdr->addr, size);
        break;
#if OPAL_HAVE_ATOMIC_MATH_64
    case MCA_BTL_VADER_OP_ATOMIC:
        if (!(hdr->flags & MCA_BTL_ATOMIC_FLAG_32BIT)) {
            mca_btl_vader_sc_emu_atomic_64 (hdr->operand, (void *) hdr->addr, hdr->op);
#if OPAL_HAVE_ATOMIC_MATH_32
        } else {
            int32_t tmp = (int32_t) hdr->operand[0];
            mca_btl_vader_sc_emu_atomic_32 (&tmp, (void *) hdr->addr, hdr->op);
            hdr->operand[0] = tmp;
#else
        } else {
            /* developer error. should not happen */
            assert (0);
#endif /* OPAL_HAVE_ATOMIC_MATH_32 */
        }
        break;
#endif /* OPAL_HAVE_ATOMIC_MATH_64 */
#if OPAL_HAVE_ATOMIC_MATH_64
    case MCA_BTL_VADER_OP_CSWAP:
        if (!(hdr->flags & MCA_BTL_ATOMIC_FLAG_32BIT)) {
            opal_atomic_compare_exchange_strong_64 ((volatile int64_t *) hdr->addr, &hdr->operand[0], hdr->operand[1]);
#if OPAL_HAVE_ATOMIC_MATH_32
        } else {
            opal_atomic_compare_exchange_strong_32 ((volatile int32_t *) hdr->addr, (int32_t *) &hdr->operand[0],
                                                    (int32_t) hdr->operand[1]);
#else
        } else {
            /* developer error. should not happen */
            assert (0);
#endif /* OPAL_HAVE_ATOMIC_MATH_32 */
        }
        break;
#endif /* OPAL_HAVE_ATOMIC_MATH_64 */
    }
}

void mca_btl_vader_sc_emu_init (void)
{
    mca_btl_base_active_message_trigger[MCA_BTL_TAG_VADER].cbfunc = mca_btl_vader_sc_emu_rdma;
    mca_btl_base_active_message_trigger[MCA_BTL_TAG_VADER].cbdata = NULL;
}
