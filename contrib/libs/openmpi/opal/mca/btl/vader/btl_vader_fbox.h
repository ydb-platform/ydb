/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2011-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(MCA_BTL_VADER_FBOX_H)
#define MCA_BTL_VADER_FBOX_H

#include "btl_vader.h"

#define MCA_BTL_VADER_POLL_COUNT 31

typedef union mca_btl_vader_fbox_hdr_t {
    struct {
        /* NTH: on 32-bit platforms loading/unloading the header may be completed
         * in multiple instructions. To ensure that seq is never loaded before tag
         * and the tag is never read before seq put them in the same 32-bits of the
         * header. */
        /** message size */
        uint32_t  size;
        /** message tag */
        uint16_t  tag;
        /** sequence number */
        uint16_t  seq;
    } data;
    struct {
        uint32_t value0;
        uint32_t value1;
    } data_i32;
    uint64_t ival;
} mca_btl_vader_fbox_hdr_t;

#define MCA_BTL_VADER_FBOX_HDR(x) ((mca_btl_vader_fbox_hdr_t *) (x))

#define MCA_BTL_VADER_FBOX_OFFSET_MASK 0x7fffffff
#define MCA_BTL_VADER_FBOX_HB_MASK     0x80000000

/* if the two offsets are equal and the high bit matches the buffer is empty else the buffer is full.
 * note that start will never be end - 1 so this simplified conditional will always produce the correct
 * result */
#define BUFFER_FREE(s,e,hbm,size) (((s + !hbm) > (e)) ? (s) - (e) : (size - (e)))

/** macro for checking if the high bit is set */
#define MCA_BTL_VADER_FBOX_OFFSET_HBS(v) (!!((v) & MCA_BTL_VADER_FBOX_HB_MASK))

void mca_btl_vader_poll_handle_frag (mca_btl_vader_hdr_t *hdr, mca_btl_base_endpoint_t *ep);

static inline void mca_btl_vader_fbox_set_header (mca_btl_vader_fbox_hdr_t *hdr, uint16_t tag,
                                                  uint16_t seq, uint32_t size)
{
    mca_btl_vader_fbox_hdr_t tmp = {.data = {.tag = tag, .seq = seq, .size = size}};
    /* clear out existing tag/seq */
    hdr->data_i32.value1 = 0;
    opal_atomic_wmb ();
    hdr->data_i32.value0 = size;
    opal_atomic_wmb ();
    hdr->data_i32.value1 = tmp.data_i32.value1;
}

static inline mca_btl_vader_fbox_hdr_t mca_btl_vader_fbox_read_header (mca_btl_vader_fbox_hdr_t *hdr)
{
    mca_btl_vader_fbox_hdr_t tmp = {.data_i32 = {.value1 = hdr->data_i32.value1}};;
    opal_atomic_rmb ();
    tmp.data_i32.value0 = hdr->data_i32.value0;
    return tmp;
}

/* attempt to reserve a contiguous segment from the remote ep */
static inline bool mca_btl_vader_fbox_sendi (mca_btl_base_endpoint_t *ep, unsigned char tag,
                                             void * restrict header, const size_t header_size,
                                             void * restrict payload, const size_t payload_size)
{
    const unsigned int fbox_size = mca_btl_vader_component.fbox_size;
    size_t size = header_size + payload_size;
    unsigned int start, end, buffer_free;
    size_t data_size = size;
    unsigned char *dst, *data;
    bool hbs, hbm;

    /* don't try to use the per-peer buffer for messages that will fill up more than 25% of the buffer */
    if (OPAL_UNLIKELY(NULL == ep->fbox_out.buffer || size > (fbox_size >> 2))) {
        return false;
    }

    OPAL_THREAD_LOCK(&ep->lock);

    /* the high bit helps determine if the buffer is empty or full */
    hbs = MCA_BTL_VADER_FBOX_OFFSET_HBS(ep->fbox_out.end);
    hbm = MCA_BTL_VADER_FBOX_OFFSET_HBS(ep->fbox_out.start) == hbs;

    /* read current start and end offsets and check for free space */
    start = ep->fbox_out.start & MCA_BTL_VADER_FBOX_OFFSET_MASK;
    end = ep->fbox_out.end & MCA_BTL_VADER_FBOX_OFFSET_MASK;
    buffer_free = BUFFER_FREE(start, end, hbm, fbox_size);

    /* need space for the fragment + the header */
    size = (size + sizeof (mca_btl_vader_fbox_hdr_t) + MCA_BTL_VADER_FBOX_ALIGNMENT_MASK) & ~MCA_BTL_VADER_FBOX_ALIGNMENT_MASK;

    dst = ep->fbox_out.buffer + end;

    if (OPAL_UNLIKELY(buffer_free < size)) {
        /* check if we need to free up space for this fragment */
        BTL_VERBOSE(("not enough room for a fragment of size %u. in use buffer segment: {start: %x, end: %x, high bit matches: %d}",
                     (unsigned) size, start, end, (int) hbm));

        /* read the current start pointer from the remote peer and recalculate the available buffer space */
        start = ep->fbox_out.start = ep->fbox_out.startp[0];

        /* recalculate how much buffer space is available */
        start &= MCA_BTL_VADER_FBOX_OFFSET_MASK;
        hbm = MCA_BTL_VADER_FBOX_OFFSET_HBS(ep->fbox_out.start) == hbs;
        buffer_free = BUFFER_FREE(start, end, hbm, fbox_size);

        opal_atomic_rmb ();

        /* if this is the end of the buffer and the fragment doesn't fit then mark the remaining buffer space to
         * be skipped and check if the fragment can be written at the beginning of the buffer. */
        if (OPAL_UNLIKELY(buffer_free > 0 && buffer_free < size && start <= end)) {
            BTL_VERBOSE(("message will not fit in remaining buffer space. skipping to beginning"));

            mca_btl_vader_fbox_set_header (MCA_BTL_VADER_FBOX_HDR(dst), 0xff, ep->fbox_out.seq++,
                                           buffer_free - sizeof (mca_btl_vader_fbox_hdr_t));

            end = MCA_BTL_VADER_FBOX_ALIGNMENT;
            /* toggle the high bit */
            hbs = !hbs;
            /* toggle the high bit match */
            buffer_free = BUFFER_FREE(start, end, !hbm, fbox_size);
            dst = ep->fbox_out.buffer + end;
        }

        if (OPAL_UNLIKELY(buffer_free < size)) {
            ep->fbox_out.end = (hbs << 31) | end;
            opal_atomic_wmb ();
            OPAL_THREAD_UNLOCK(&ep->lock);
            return false;
        }
    }

    BTL_VERBOSE(("writing fragment of size %u to offset %u {start: 0x%x, end: 0x%x (hbs: %d)} of peer's buffer. free = %u",
                 (unsigned int) size, end, start, end, hbs, buffer_free));

    data = dst + sizeof (mca_btl_vader_fbox_hdr_t);

    memcpy (data, header, header_size);
    if (payload) {
        /* inline sends are typically just pml headers (due to MCA_BTL_FLAGS_SEND_INPLACE) */
        memcpy (data + header_size, payload, payload_size);
    }

    end += size;

    if (OPAL_UNLIKELY(fbox_size == end)) {
        /* toggle the high bit */
        hbs = !hbs;
        /* reset the end pointer to the beginning of the buffer */
        end = MCA_BTL_VADER_FBOX_ALIGNMENT;
    } else if (buffer_free > size) {
        MCA_BTL_VADER_FBOX_HDR(ep->fbox_out.buffer + end)->ival = 0;
    }

    /* write out part of the header now. the tag will be written when the data is available */
    mca_btl_vader_fbox_set_header (MCA_BTL_VADER_FBOX_HDR(dst), tag, ep->fbox_out.seq++, data_size);

    /* align the buffer */
    ep->fbox_out.end = ((uint32_t) hbs << 31) | end;
    opal_atomic_wmb ();
    OPAL_THREAD_UNLOCK(&ep->lock);

    return true;
}

static inline bool mca_btl_vader_check_fboxes (void)
{
    const unsigned int fbox_size = mca_btl_vader_component.fbox_size;
    bool processed = false;

    for (unsigned int i = 0 ; i < mca_btl_vader_component.num_fbox_in_endpoints ; ++i) {
        mca_btl_base_endpoint_t *ep = mca_btl_vader_component.fbox_in_endpoints[i];
        unsigned int start = ep->fbox_in.start & MCA_BTL_VADER_FBOX_OFFSET_MASK;

        /* save the current high bit state */
        bool hbs = MCA_BTL_VADER_FBOX_OFFSET_HBS(ep->fbox_in.start);
        int poll_count;

        for (poll_count = 0 ; poll_count <= MCA_BTL_VADER_POLL_COUNT ; ++poll_count) {
            const mca_btl_vader_fbox_hdr_t hdr = mca_btl_vader_fbox_read_header (MCA_BTL_VADER_FBOX_HDR(ep->fbox_in.buffer + start));

            /* check for a valid tag a sequence number */
            if (0 == hdr.data.tag || hdr.data.seq != ep->fbox_in.seq) {
                break;
            }

            ++ep->fbox_in.seq;

            /* force all prior reads to complete before continuing */
            opal_atomic_rmb ();

            BTL_VERBOSE(("got frag from %d with header {.tag = %d, .size = %d, .seq = %u} from offset %u",
                         ep->peer_smp_rank, hdr.data.tag, hdr.data.size, hdr.data.seq, start));

            /* the 0xff tag indicates we should skip the rest of the buffer */
            if (OPAL_LIKELY((0xfe & hdr.data.tag) != 0xfe)) {
                mca_btl_base_segment_t segment;
                mca_btl_base_descriptor_t desc = {.des_segments = &segment, .des_segment_count = 1};
                const mca_btl_active_message_callback_t *reg =
                    mca_btl_base_active_message_trigger + hdr.data.tag;

                /* fragment fits entirely in the remaining buffer space. some
                 * btl users do not handle fragmented data so we can't split
                 * the fragment without introducing another copy here. this
                 * limitation has not appeared to cause any performance
                 * degradation. */
                segment.seg_len = hdr.data.size;
                segment.seg_addr.pval = (void *) (ep->fbox_in.buffer + start + sizeof (hdr));

                /* call the registered callback function */
                reg->cbfunc(&mca_btl_vader.super, hdr.data.tag, &desc, reg->cbdata);
            } else if (OPAL_LIKELY(0xfe == hdr.data.tag)) {
                /* process fragment header */
                fifo_value_t *value = (fifo_value_t *)(ep->fbox_in.buffer + start + sizeof (hdr));
                mca_btl_vader_hdr_t *hdr = relative2virtual(*value);
                mca_btl_vader_poll_handle_frag (hdr, ep);
            }

            start = (start + hdr.data.size + sizeof (hdr) + MCA_BTL_VADER_FBOX_ALIGNMENT_MASK) & ~MCA_BTL_VADER_FBOX_ALIGNMENT_MASK;
            if (OPAL_UNLIKELY(fbox_size == start)) {
                /* jump to the beginning of the buffer */
                start = MCA_BTL_VADER_FBOX_ALIGNMENT;
                /* toggle the high bit */
                hbs = !hbs;
            }
        }

        if (poll_count) {
            BTL_VERBOSE(("left off at offset %u (hbs: %d)", start, hbs));

            /* save where we left off */
            /* let the sender know where we stopped */
            opal_atomic_mb ();
            ep->fbox_in.start = ep->fbox_in.startp[0] = ((uint32_t) hbs << 31) | start;
            processed = true;
        }
    }

    /* return the number of fragments processed */
    return processed;
}

static inline void mca_btl_vader_try_fbox_setup (mca_btl_base_endpoint_t *ep, mca_btl_vader_hdr_t *hdr)
{
    if (OPAL_UNLIKELY(NULL == ep->fbox_out.buffer && mca_btl_vader_component.fbox_threshold == OPAL_THREAD_ADD_FETCH_SIZE_T (&ep->send_count, 1))) {
        /* protect access to mca_btl_vader_component.segment_offset */
        OPAL_THREAD_LOCK(&mca_btl_vader_component.lock);

        /* verify the remote side will accept another fbox */
        if (0 <= opal_atomic_add_fetch_32 (&ep->fifo->fbox_available, -1)) {
            opal_free_list_item_t *fbox = opal_free_list_get (&mca_btl_vader_component.vader_fboxes);

            if (NULL != fbox) {
                /* zero out the fast box */
                memset (fbox->ptr, 0, mca_btl_vader_component.fbox_size);
                mca_btl_vader_endpoint_setup_fbox_send (ep, fbox);

                hdr->flags |= MCA_BTL_VADER_FLAG_SETUP_FBOX;
                hdr->fbox_base = virtual2relative((char *) ep->fbox_out.buffer);
            } else {
                opal_atomic_add_fetch_32 (&ep->fifo->fbox_available, 1);
            }

            opal_atomic_wmb ();
        }

        OPAL_THREAD_UNLOCK(&mca_btl_vader_component.lock);
    }
}

#endif /* !defined(MCA_BTL_VADER_FBOX_H) */
