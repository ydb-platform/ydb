/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_MCA_OSC_PT2PT_HDR_H
#define OMPI_MCA_OSC_PT2PT_HDR_H

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#include "opal/types.h"
#include "opal/util/arch.h"

enum ompi_osc_pt2pt_hdr_type_t {
    OMPI_OSC_PT2PT_HDR_TYPE_PUT          = 0x01,
    OMPI_OSC_PT2PT_HDR_TYPE_PUT_LONG     = 0x02,
    OMPI_OSC_PT2PT_HDR_TYPE_ACC          = 0x03,
    OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG     = 0x04,
    OMPI_OSC_PT2PT_HDR_TYPE_GET          = 0x05,
    OMPI_OSC_PT2PT_HDR_TYPE_CSWAP        = 0x06,
    OMPI_OSC_PT2PT_HDR_TYPE_CSWAP_LONG   = 0x07,
    OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC      = 0x08,
    OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG = 0x09,
    OMPI_OSC_PT2PT_HDR_TYPE_COMPLETE     = 0x10,
    OMPI_OSC_PT2PT_HDR_TYPE_POST         = 0x11,
    OMPI_OSC_PT2PT_HDR_TYPE_LOCK_REQ     = 0x12,
    OMPI_OSC_PT2PT_HDR_TYPE_LOCK_ACK     = 0x13,
    OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_REQ   = 0x14,
    OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_ACK   = 0x15,
    OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_REQ    = 0x16,
    OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_ACK    = 0x17,
    OMPI_OSC_PT2PT_HDR_TYPE_FRAG         = 0x20,
};
typedef enum ompi_osc_pt2pt_hdr_type_t ompi_osc_pt2pt_hdr_type_t;

#define OMPI_OSC_PT2PT_HDR_FLAG_NBO            0x01
#define OMPI_OSC_PT2PT_HDR_FLAG_VALID          0x02
#define OMPI_OSC_PT2PT_HDR_FLAG_PASSIVE_TARGET 0x04
#define OMPI_OSC_PT2PT_HDR_FLAG_LARGE_DATATYPE 0x08

struct ompi_osc_pt2pt_header_base_t {
    /** fragment type. 8 bits */
    uint8_t type;
    /** fragment flags. 8 bits */
    uint8_t flags;
};
typedef struct ompi_osc_pt2pt_header_base_t ompi_osc_pt2pt_header_base_t;

struct ompi_osc_pt2pt_header_put_t {
    ompi_osc_pt2pt_header_base_t base;

    uint16_t tag;
    uint32_t count;
    uint64_t len;
    uint64_t displacement;
};
typedef struct ompi_osc_pt2pt_header_put_t ompi_osc_pt2pt_header_put_t;

struct ompi_osc_pt2pt_header_acc_t {
    ompi_osc_pt2pt_header_base_t base;

    uint16_t tag;
    uint32_t count;
    uint64_t len;
    uint64_t displacement;
    uint32_t op;
};
typedef struct ompi_osc_pt2pt_header_acc_t ompi_osc_pt2pt_header_acc_t;

struct ompi_osc_pt2pt_header_get_t {
    ompi_osc_pt2pt_header_base_t base;

    uint16_t tag;
    uint32_t count;
    uint64_t len;
    uint64_t displacement;
};
typedef struct ompi_osc_pt2pt_header_get_t ompi_osc_pt2pt_header_get_t;

struct ompi_osc_pt2pt_header_complete_t {
    ompi_osc_pt2pt_header_base_t base;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    uint8_t padding[2];
#endif
    int frag_count;
};
typedef struct ompi_osc_pt2pt_header_complete_t ompi_osc_pt2pt_header_complete_t;

struct ompi_osc_pt2pt_header_cswap_t {
    ompi_osc_pt2pt_header_base_t base;

    uint16_t tag;
    uint32_t len;
    uint64_t displacement;
};
typedef struct ompi_osc_pt2pt_header_cswap_t ompi_osc_pt2pt_header_cswap_t;

struct ompi_osc_pt2pt_header_post_t {
    ompi_osc_pt2pt_header_base_t base;
};
typedef struct ompi_osc_pt2pt_header_post_t ompi_osc_pt2pt_header_post_t;

struct ompi_osc_pt2pt_header_lock_t {
    ompi_osc_pt2pt_header_base_t base;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    uint8_t padding[2];
#endif
    int32_t lock_type;
    uint64_t lock_ptr;
};
typedef struct ompi_osc_pt2pt_header_lock_t ompi_osc_pt2pt_header_lock_t;

struct ompi_osc_pt2pt_header_lock_ack_t {
    ompi_osc_pt2pt_header_base_t base;
    uint32_t source;
    uint64_t lock_ptr;
};
typedef struct ompi_osc_pt2pt_header_lock_ack_t ompi_osc_pt2pt_header_lock_ack_t;

struct ompi_osc_pt2pt_header_unlock_t {
    ompi_osc_pt2pt_header_base_t base;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    uint8_t padding[2];
#endif
    int32_t lock_type;
    uint64_t lock_ptr;
    uint32_t frag_count;
};
typedef struct ompi_osc_pt2pt_header_unlock_t ompi_osc_pt2pt_header_unlock_t;

struct ompi_osc_pt2pt_header_unlock_ack_t {
    ompi_osc_pt2pt_header_base_t base;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    uint8_t padding[6];
#endif
    uint64_t lock_ptr;
};
typedef struct ompi_osc_pt2pt_header_unlock_ack_t ompi_osc_pt2pt_header_unlock_ack_t;

struct ompi_osc_pt2pt_header_flush_t {
    ompi_osc_pt2pt_header_base_t base;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    uint8_t padding[2];
#endif
    uint32_t frag_count;
    uint64_t lock_ptr;
};
typedef struct ompi_osc_pt2pt_header_flush_t ompi_osc_pt2pt_header_flush_t;

struct ompi_osc_pt2pt_header_flush_ack_t {
    ompi_osc_pt2pt_header_base_t base;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    uint8_t padding[6];
#endif
    uint64_t lock_ptr;
};
typedef struct ompi_osc_pt2pt_header_flush_ack_t ompi_osc_pt2pt_header_flush_ack_t;

struct ompi_osc_pt2pt_frag_header_t {
    ompi_osc_pt2pt_header_base_t base;
    uint32_t source; /* rank in window of source process */
    int32_t num_ops; /* number of operations in this buffer */
    uint32_t pad; /* ensure the fragment header is a multiple of 8 bytes */
};
typedef struct ompi_osc_pt2pt_frag_header_t ompi_osc_pt2pt_frag_header_t;

union ompi_osc_pt2pt_header_t {
    ompi_osc_pt2pt_header_base_t       base;
    ompi_osc_pt2pt_header_put_t        put;
    ompi_osc_pt2pt_header_acc_t        acc;
    ompi_osc_pt2pt_header_get_t        get;
    ompi_osc_pt2pt_header_complete_t   complete;
    ompi_osc_pt2pt_header_cswap_t      cswap;
    ompi_osc_pt2pt_header_post_t       post;
    ompi_osc_pt2pt_header_lock_t       lock;
    ompi_osc_pt2pt_header_lock_ack_t   lock_ack;
    ompi_osc_pt2pt_header_unlock_t     unlock;
    ompi_osc_pt2pt_header_unlock_ack_t unlock_ack;
    ompi_osc_pt2pt_header_flush_t      flush;
    ompi_osc_pt2pt_header_flush_ack_t  flush_ack;
    ompi_osc_pt2pt_frag_header_t       frag;
};
typedef union ompi_osc_pt2pt_header_t ompi_osc_pt2pt_header_t;

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
#define MCA_OSC_PT2PT_FRAG_HDR_NTOH(h)       \
    (h).source = ntohl((h).source);             \
    (h).num_ops = ntohl((h).num_ops);           \
    (h).pad = ntohl((h).pad);
#define MCA_OSC_PT2PT_FRAG_HDR_HTON(h)       \
    (h).source = htonl((h).source);             \
    (h).num_ops = htonl((h).num_ops);           \
    (h).pad = htonl((h).pad);

#define MCA_OSC_PT2PT_PUT_HDR_NTOH(h)        \
    (h).tag = ntohs((h).tag);                   \
    (h).count = ntohl((h).count);               \
    (h).len = ntoh64((h).len);                  \
    (h).displacement = ntoh64((h).displacement);
#define MCA_OSC_PT2PT_PUT_HDR_HTON(h)        \
    (h).tag = htons((h).tag);                   \
    (h).count = htonl((h).count);               \
    (h).len = hton64((h).len);                  \
    (h).displacement = hton64((h).displacement);

#define MCA_OSC_PT2PT_GET_HDR_NTOH(h)        \
    (h).tag = ntohs((h).tag);                   \
    (h).count = ntohl((h).count);               \
    (h).len = ntoh64((h).len);                  \
    (h).displacement = ntoh64((h).displacement);
#define MCA_OSC_PT2PT_GET_HDR_HTON(h)        \
    (h).tag = htons((h).tag);                   \
    (h).count = htonl((h).count);               \
    (h).len = hton64((h).len);                  \
    (h).displacement = hton64((h).displacement);

#define MCA_OSC_PT2PT_ACC_HDR_NTOH(h)        \
    (h).tag = ntohs((h).tag);                   \
    (h).count = ntohl((h).count);               \
    (h).len = ntoh64((h).len);                  \
    (h).displacement = ntoh64((h).displacement);\
    (h).op = ntohl((h).op);
#define MCA_OSC_PT2PT_ACC_HDR_HTON(h)        \
    (h).tag = htons((h).tag);                   \
    (h).count = htonl((h).count);               \
    (h).len = hton64((h).len);                  \
    (h).displacement = hton64((h).displacement);\
    (h).op = htonl((h).op);

#define MCA_OSC_PT2PT_LOCK_HDR_NTOH(h)       \
    (h).lock_type = ntohl((h).lock_type)
#define MCA_OSC_PT2PT_LOCK_HDR_HTON(h)       \
    (h).lock_type = htonl((h).lock_type)

#define MCA_OSC_PT2PT_UNLOCK_HDR_NTOH(h)     \
    (h).lock_type = ntohl((h).lock_type);       \
    (h).frag_count = ntohl((h).frag_count)
#define MCA_OSC_PT2PT_UNLOCK_HDR_HTON(h)     \
    (h).lock_type = htonl((h).lock_type);       \
    (h).frag_count = htonl((h).frag_count)

#define MCA_OSC_PT2PT_LOCK_ACK_HDR_NTOH(h)   \
    (h).source = ntohl((h).source)
#define MCA_OSC_PT2PT_LOCK_ACK_HDR_HTON(h)   \
    (h).source= htonl((h).source)

#define MCA_OSC_PT2PT_UNLOCK_ACK_HDR_NTOH(h)
#define MCA_OSC_PT2PT_UNLOCK_ACK_HDR_HTON(h)

#define MCA_OSC_PT2PT_COMPLETE_HDR_NTOH(h)   \
    (h).frag_count = ntohl((h).frag_count)
#define MCA_OSC_PT2PT_COMPLETE_HDR_HTON(h)   \
    (h).frag_count = htonl((h).frag_count)

#define MCA_OSC_PT2PT_FLUSH_HDR_NTOH(h)      \
    (h).frag_count = ntohl((h).frag_count)
#define MCA_OSC_PT2PT_FLUSH_HDR_HTON(h)      \
    (h).frag_count = htonl((h).frag_count)

#define MCA_OSC_PT2PT_FLUSH_ACK_HDR_NTOH(h)
#define MCA_OSC_PT2PT_FLUSH_ACK_HDR_HTON(h)

#define MCA_OSC_PT2PT_POST_HDR_NTOH(h)
#define MCA_OSC_PT2PT_POST_HDR_HTON(h)

#define MCA_OSC_PT2PT_CSWAP_HDR_NTOH(h)      \
    (h).tag = ntohs((h).tag);                   \
    (h).len = ntohl((h).len);                   \
    (h).displacement = ntoh64((h).displacement)
#define MCA_OSC_PT2PT_CSWAP_HDR_HTON(h)      \
    (h).tag = htons((h).tag);                   \
    (h).len = htonl((h).len);                   \
    (h).displacement = hton64((h).displacement)
#endif /* OPAL_ENABLE_HETEROGENEOUS_SUPPORT */

#if !defined(WORDS_BIGENDIAN) && OPAL_ENABLE_HETEROGENEOUS_SUPPORT
static inline __opal_attribute_always_inline__ void
osc_pt2pt_ntoh(ompi_osc_pt2pt_header_t *hdr)
{
    if(!(hdr->base.flags & OMPI_OSC_PT2PT_HDR_FLAG_NBO))
        return;

    switch(hdr->base.type) {
        case OMPI_OSC_PT2PT_HDR_TYPE_PUT:
        case OMPI_OSC_PT2PT_HDR_TYPE_PUT_LONG:
            MCA_OSC_PT2PT_PUT_HDR_NTOH(hdr->put);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_ACC:
        case OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG:
        case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC:
        case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG:
            MCA_OSC_PT2PT_ACC_HDR_NTOH(hdr->acc);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_GET:
            MCA_OSC_PT2PT_GET_HDR_NTOH(hdr->get);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_CSWAP:
        case OMPI_OSC_PT2PT_HDR_TYPE_CSWAP_LONG:
            MCA_OSC_PT2PT_CSWAP_HDR_NTOH(hdr->cswap);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_COMPLETE:
            MCA_OSC_PT2PT_COMPLETE_HDR_NTOH(hdr->complete);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_POST:
            MCA_OSC_PT2PT_POST_HDR_NTOH(hdr->post);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_LOCK_REQ:
            MCA_OSC_PT2PT_LOCK_HDR_NTOH(hdr->lock);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_LOCK_ACK:
            MCA_OSC_PT2PT_LOCK_ACK_HDR_NTOH(hdr->lock_ack);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_REQ:
            MCA_OSC_PT2PT_UNLOCK_HDR_NTOH(hdr->unlock);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_ACK:
            MCA_OSC_PT2PT_UNLOCK_ACK_HDR_NTOH(hdr->unlock_ack);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_REQ:
            MCA_OSC_PT2PT_FLUSH_HDR_NTOH(hdr->flush);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_ACK:
            MCA_OSC_PT2PT_FLUSH_ACK_HDR_NTOH(hdr->flush_ack);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_FRAG:
            MCA_OSC_PT2PT_FRAG_HDR_NTOH(hdr->frag);
            break;
        default:
            assert(0);
            break;
    }
}
#else
#define osc_pt2pt_ntoh(h)    \
    do { } while (0)
#endif /* !defined(WORDS_BIGENDIAN) && OPAL_ENABLE_HETEROGENEOUS_SUPPORT */

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
#define osc_pt2pt_hton(h, p) \
    osc_pt2pt_hton_intr((ompi_osc_pt2pt_header_t *)(h), (p));
static inline __opal_attribute_always_inline__ void
osc_pt2pt_hton_intr(ompi_osc_pt2pt_header_t *hdr, const ompi_proc_t *proc)
{
#ifdef WORDS_BIGENDIAN
    hdr->base.flags |= OMPI_OSC_PT2PT_HDR_FLAG_NBO;
#else
    if(!(proc->super.proc_arch & OPAL_ARCH_ISBIGENDIAN))
        return;

    hdr->base.flags |= OMPI_OSC_PT2PT_HDR_FLAG_NBO;
    switch(hdr->base.type) {
        case OMPI_OSC_PT2PT_HDR_TYPE_PUT:
        case OMPI_OSC_PT2PT_HDR_TYPE_PUT_LONG:
            MCA_OSC_PT2PT_PUT_HDR_HTON(hdr->put);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_ACC:
        case OMPI_OSC_PT2PT_HDR_TYPE_ACC_LONG:
        case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC:
        case OMPI_OSC_PT2PT_HDR_TYPE_GET_ACC_LONG:
            MCA_OSC_PT2PT_ACC_HDR_HTON(hdr->acc);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_GET:
            MCA_OSC_PT2PT_GET_HDR_HTON(hdr->get);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_CSWAP:
        case OMPI_OSC_PT2PT_HDR_TYPE_CSWAP_LONG:
            MCA_OSC_PT2PT_CSWAP_HDR_HTON(hdr->cswap);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_COMPLETE:
            MCA_OSC_PT2PT_COMPLETE_HDR_HTON(hdr->complete);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_POST:
            MCA_OSC_PT2PT_POST_HDR_HTON(hdr->post);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_LOCK_REQ:
            MCA_OSC_PT2PT_LOCK_HDR_HTON(hdr->lock);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_LOCK_ACK:
            MCA_OSC_PT2PT_LOCK_ACK_HDR_HTON(hdr->lock_ack);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_REQ:
            MCA_OSC_PT2PT_UNLOCK_HDR_HTON(hdr->unlock);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_ACK:
            MCA_OSC_PT2PT_UNLOCK_ACK_HDR_HTON(hdr->unlock_ack);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_REQ:
            MCA_OSC_PT2PT_FLUSH_HDR_HTON(hdr->flush);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_ACK:
            MCA_OSC_PT2PT_FLUSH_ACK_HDR_HTON(hdr->flush_ack);
            break;
        case OMPI_OSC_PT2PT_HDR_TYPE_FRAG:
            MCA_OSC_PT2PT_FRAG_HDR_HTON(hdr->frag);
            break;
        default:
            assert(0);
            break;
    }
#endif /* WORDS_BIGENDIAN */
}
#define OSC_PT2PT_HTON(h, m, r) \
        osc_pt2pt_hton_intr((ompi_osc_pt2pt_header_t *)(h), ompi_comm_peer_lookup((m)->comm, (r)));
#else
#define osc_pt2pt_hton(h, p) \
    do { } while (0)
#define OSC_PT2PT_HTON(h, m, r) \
    do { } while (0)
#endif /* OPAL_ENABLE_HETEROGENEOUS_SUPPORT */

#endif /* OMPI_MCA_OSC_PT2PT_HDR_H */
