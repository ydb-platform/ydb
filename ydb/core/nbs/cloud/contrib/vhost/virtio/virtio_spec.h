/**
 * Definitions from virtio spec version 1.0
 * http://docs.oasis-open.org/virtio/virtio/v1.0/virtio-v1.0.html.
 *
 * Type naming and style is preserved verbatim from virtio spec.
 */

#pragma once

#include "platform.h"
#include "virtio_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define VIRTQ_SIZE_MAX  32768u

struct virtq_desc {
    /* Address (guest-physical). */
    le64 addr;
    /* Length. */
    le32 len;

    /* This marks a buffer as continuing via the next field. */
#define VIRTQ_DESC_F_NEXT       1
    /* This marks a buffer as device write-only (otherwise device read-only). */
#define VIRTQ_DESC_F_WRITE      2
    /* This means the buffer contains a list of buffer descriptors. */
#define VIRTQ_DESC_F_INDIRECT   4
    /* The flags as indicated above. */
    le16 flags;
    /* Next field if flags & NEXT */
    le16 next;
};
VHD_STATIC_ASSERT(sizeof(struct virtq_desc) == 16);

struct virtq_avail {
#define VIRTQ_AVAIL_F_NO_INTERRUPT      1
    le16 flags;
    le16 idx;
    le16 ring[]; /* Queue Size */
    /* le16 used_event; Only if VIRTIO_F_EVENT_IDX */
};
VHD_STATIC_ASSERT(sizeof(struct virtq_avail) == 4);

/* le32 is used here for ids for padding reasons. */
struct virtq_used_elem {
    /* Index of start of used descriptor chain. */
    le32 id;
    /*
     * The number of bytes written into the device writable portion of
     * the buffer described by the descriptor chain.
     */
    le32 len;
};
VHD_STATIC_ASSERT(sizeof(struct virtq_used_elem) == 8);

struct virtq_used {
#define VIRTQ_USED_F_NO_NOTIFY  1
    le16 flags;
    le16 idx;
    struct virtq_used_elem ring[]; /* Queue Size */
    /* le16 avail_event; Only if VIRTIO_F_EVENT_IDX */
};
VHD_STATIC_ASSERT(sizeof(struct virtq_used) == 4);

/*
 * Virtqueue layout cannot be represented by a C struct,
 * definition below is intentionally a comment.
 * struct virtq {
 *     // The actual descriptors (16 bytes each)
 *     struct virtq_desc desc[ Queue Size ];
 *
 *     // A ring of available descriptor heads with free-running index.
 *     struct virtq_avail avail;
 *     le16 used_event; // Only if VIRTIO_F_EVENT_IDX
 *
 *     // Padding to the next PAGE_SIZE boundary.
 *     u8 pad[ Padding ];
 *
 *     // A ring of used descriptor heads with free-running index.
 *     struct virtq_used used;
 *     le16 avail_event; // Only if VIRTIO_F_EVENT_IDX
 * };
*/

static inline size_t virtq_align(size_t size)
{
    return (size + platform_page_size) & ~platform_page_size;
}

static inline unsigned virtq_size(unsigned int qsz)
{
    return virtq_align(sizeof(struct virtq_desc) * qsz +
                       sizeof(le16) * (3 + qsz))
         + virtq_align(sizeof(le16) * 3 +
                       sizeof(struct virtq_used_elem) * qsz);
}

#ifdef __cplusplus
}
#endif
