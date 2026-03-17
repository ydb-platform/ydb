/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2012-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 *  @file
 */

#ifndef MCA_PML_OB1_H
#define MCA_PML_OB1_H

#include "ompi_config.h"
#include "opal/class/opal_free_list.h"
#include "ompi/request/request.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/pml_base_request.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/datatype/ompi_datatype.h"
#include "pml_ob1_hdr.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/proc/proc.h"
#include "opal/mca/allocator/base/base.h"

BEGIN_C_DECLS

/**
 * OB1 PML module
 */

struct mca_pml_ob1_t {
    mca_pml_base_module_t super;

    int priority;
    int free_list_num;      /* initial size of free list */
    int free_list_max;      /* maximum size of free list */
    int free_list_inc;      /* number of elements to grow free list */
    int32_t send_pipeline_depth;
    int32_t recv_pipeline_depth;
    size_t rdma_retries_limit;
    int max_rdma_per_request;
    int max_send_per_range;
    bool use_all_rdma;

    /* lock queue access */
    opal_mutex_t lock;

    /* free lists */
    opal_free_list_t rdma_frags;
    opal_free_list_t recv_frags;
    opal_free_list_t pending_pckts;
    opal_free_list_t buffers;
    opal_free_list_t send_ranges;

    /* list of pending operations */
    opal_list_t pckt_pending;
    opal_list_t send_pending;
    opal_list_t recv_pending;
    opal_list_t rdma_pending;
    /* List of pending fragments without a matching communicator */
    opal_list_t non_existing_communicator_pending;
    bool enabled;
    char* allocator_name;
    mca_allocator_base_module_t* allocator;
    unsigned int unexpected_limit;
};
typedef struct mca_pml_ob1_t mca_pml_ob1_t;

extern mca_pml_ob1_t mca_pml_ob1;
extern int mca_pml_ob1_output;
extern bool mca_pml_ob1_matching_protection;
/*
 * PML interface functions.
 */

extern int mca_pml_ob1_add_comm(
    struct ompi_communicator_t* comm
);

extern int mca_pml_ob1_del_comm(
    struct ompi_communicator_t* comm
);

extern int mca_pml_ob1_add_procs(
    struct ompi_proc_t **procs,
    size_t nprocs
);

extern int mca_pml_ob1_del_procs(
    struct ompi_proc_t **procs,
    size_t nprocs
);

extern int mca_pml_ob1_enable( bool enable );

extern int mca_pml_ob1_progress(void);

extern int mca_pml_ob1_iprobe( int dst,
                               int tag,
                               struct ompi_communicator_t* comm,
                               int *matched,
                               ompi_status_public_t* status );

extern int mca_pml_ob1_probe( int dst,
                              int tag,
                              struct ompi_communicator_t* comm,
                              ompi_status_public_t* status );

extern int mca_pml_ob1_improbe( int dst,
                               int tag,
                               struct ompi_communicator_t* comm,
                               int *matched,
                               struct ompi_message_t **message,
                               ompi_status_public_t* status );

extern int mca_pml_ob1_mprobe( int dst,
                              int tag,
                              struct ompi_communicator_t* comm,
                              struct ompi_message_t **message,
                              ompi_status_public_t* status );

extern int mca_pml_ob1_isend_init( const void *buf,
                                   size_t count,
                                   ompi_datatype_t *datatype,
                                   int dst,
                                   int tag,
                                   mca_pml_base_send_mode_t mode,
                                   struct ompi_communicator_t* comm,
                                   struct ompi_request_t **request );

extern int mca_pml_ob1_isend( const void *buf,
                              size_t count,
                              ompi_datatype_t *datatype,
                              int dst,
                              int tag,
                              mca_pml_base_send_mode_t mode,
                              struct ompi_communicator_t* comm,
                              struct ompi_request_t **request );

extern int mca_pml_ob1_send( const void *buf,
                             size_t count,
                             ompi_datatype_t *datatype,
                             int dst,
                             int tag,
                             mca_pml_base_send_mode_t mode,
                             struct ompi_communicator_t* comm );

extern int mca_pml_ob1_irecv_init( void *buf,
                                   size_t count,
                                   ompi_datatype_t *datatype,
                                   int src,
                                   int tag,
                                   struct ompi_communicator_t* comm,
                                   struct ompi_request_t **request );

extern int mca_pml_ob1_irecv( void *buf,
                              size_t count,
                              ompi_datatype_t *datatype,
                              int src,
                              int tag,
                              struct ompi_communicator_t* comm,
                              struct ompi_request_t **request );

extern int mca_pml_ob1_recv( void *buf,
                             size_t count,
                             ompi_datatype_t *datatype,
                             int src,
                             int tag,
                             struct ompi_communicator_t* comm,
                             ompi_status_public_t* status );

extern int mca_pml_ob1_imrecv( void *buf,
                               size_t count,
                               ompi_datatype_t *datatype,
                               struct ompi_message_t **message,
                               struct ompi_request_t **request );

extern int mca_pml_ob1_mrecv( void *buf,
                              size_t count,
                              ompi_datatype_t *datatype,
                              struct ompi_message_t **message,
                              ompi_status_public_t* status );

extern int mca_pml_ob1_dump( struct ompi_communicator_t* comm,
                             int verbose );

extern int mca_pml_ob1_start( size_t count,
                              ompi_request_t** requests );

extern int mca_pml_ob1_ft_event( int state );

/**
 * We will use these requests to hold on a traditionally allocated
 * requests in order to allow the parallel debugger full access to the
 * message queues (instead of allocating the requests on the stack).
 */
extern struct mca_pml_ob1_recv_request_t *mca_pml_ob1_recvreq;
extern struct mca_pml_ob1_send_request_t *mca_pml_ob1_sendreq;

END_C_DECLS

struct mca_pml_ob1_pckt_pending_t {
    opal_free_list_item_t super;
    ompi_proc_t* proc;
    mca_pml_ob1_hdr_t hdr;
    struct mca_bml_base_btl_t *bml_btl;
    uint8_t order;
    int status;
};
typedef struct mca_pml_ob1_pckt_pending_t mca_pml_ob1_pckt_pending_t;
OBJ_CLASS_DECLARATION(mca_pml_ob1_pckt_pending_t);

#define MCA_PML_OB1_PCKT_PENDING_ALLOC(pckt)                    \
do {                                                            \
    pckt = (mca_pml_ob1_pckt_pending_t *)                       \
        opal_free_list_get (&mca_pml_ob1.pending_pckts);        \
} while (0)

#define MCA_PML_OB1_PCKT_PENDING_RETURN(pckt)                   \
do {                                                            \
    /* return packet */                                         \
    opal_free_list_return (&mca_pml_ob1.pending_pckts,          \
        (opal_free_list_item_t*)pckt);                          \
} while(0)

#define MCA_PML_OB1_ADD_FIN_TO_PENDING(P, D, Sz, B, O, S)           \
    do {                                                            \
        mca_pml_ob1_pckt_pending_t *_pckt;                          \
                                                                    \
        MCA_PML_OB1_PCKT_PENDING_ALLOC(_pckt);                      \
        mca_pml_ob1_fin_hdr_prepare (&_pckt->hdr.hdr_fin, 0,        \
                                     (D).lval, (Sz));               \
        _pckt->proc = (P);                                          \
        _pckt->bml_btl = (B);                                       \
        _pckt->order = (O);                                         \
        _pckt->status = (S);                                        \
        OPAL_THREAD_LOCK(&mca_pml_ob1.lock);                        \
        opal_list_append(&mca_pml_ob1.pckt_pending,                 \
                (opal_list_item_t*)_pckt);                          \
        OPAL_THREAD_UNLOCK(&mca_pml_ob1.lock);                      \
    } while(0)

#define OB1_MATCHING_LOCK(lock)                                  \
    do {                                                         \
        if( mca_pml_ob1_matching_protection ) {                  \
            opal_mutex_lock(lock);                               \
        }                                                        \
        else { OPAL_THREAD_LOCK(lock); }                         \
    } while(0)


#define OB1_MATCHING_UNLOCK(lock)                                \
    do {                                                         \
        if( mca_pml_ob1_matching_protection ) {                  \
            opal_mutex_unlock(lock);                             \
        }                                                        \
        else { OPAL_THREAD_UNLOCK(lock); }                       \
    } while(0)



int mca_pml_ob1_send_fin(ompi_proc_t* proc, mca_bml_base_btl_t* bml_btl,
        opal_ptr_t hdr_frag, uint64_t size, uint8_t order, int status);

/* This function tries to resend FIN/ACK packets from pckt_pending queue.
 * Packets are added to the queue when sending of FIN or ACK is failed due to
 * resource unavailability. bml_btl passed to the function doesn't represents
 * packet's destination, it represents BTL on which resource was freed, so only
 * this BTL should be considered for resending packets */
void mca_pml_ob1_process_pending_packets(mca_bml_base_btl_t* bml_btl);

/* This function retries failed PUT/GET operations on frag. When RDMA operation
 * cannot be accomplished for some reason, frag is put on the rdma_pending list.
 * Later the operation is retried. The destination of RDMA operation is stored
 * inside the frag structure */
void mca_pml_ob1_process_pending_rdma(void);

#define MCA_PML_OB1_PROGRESS_PENDING(bml_btl)                   \
    do {                                                        \
        if(opal_list_get_size(&mca_pml_ob1.pckt_pending))       \
            mca_pml_ob1_process_pending_packets(bml_btl);       \
        if(opal_list_get_size(&mca_pml_ob1.recv_pending))       \
            mca_pml_ob1_recv_request_process_pending();         \
        if(opal_list_get_size(&mca_pml_ob1.send_pending))       \
            mca_pml_ob1_send_request_process_pending(bml_btl);  \
        if(opal_list_get_size(&mca_pml_ob1.rdma_pending))       \
            mca_pml_ob1_process_pending_rdma();                 \
    } while (0)

/*
 * Compute the total number of bytes on supplied descriptor
 */
static inline size_t
mca_pml_ob1_compute_segment_length_base(mca_btl_base_segment_t *segments,
                                        size_t count, size_t hdrlen)
{
    size_t i, length = 0;

    for (i = 0; i < count ; ++i) {
        length += segments[i].seg_len;
    }
    return (length - hdrlen);
}

static inline size_t
mca_pml_ob1_compute_segment_length_remote (size_t seg_size, void *segments,
                                           size_t count, ompi_proc_t *rem_proc)
{
    mca_btl_base_segment_t *segment = (mca_btl_base_segment_t *) segments;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t *local_proc = ompi_proc_local();
#endif
    size_t i, length = 0;

    for (i = 0 ; i < count ; ++i) {
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        if ((rem_proc->super.proc_arch & OPAL_ARCH_ISBIGENDIAN) !=
            (local_proc->super.proc_arch & OPAL_ARCH_ISBIGENDIAN))
            /* NTH: seg_len is always 64-bit so use swap_bytes8 */
            length += opal_swap_bytes8(segment->seg_len);
        else
#endif
            length += segment->seg_len;

        segment = (mca_btl_base_segment_t *)((char *)segment + seg_size);
    }

    return length;
}

/* represent BTL chosen for sending request */
struct mca_pml_ob1_com_btl_t {
    mca_bml_base_btl_t *bml_btl;
    struct mca_btl_base_registration_handle_t *btl_reg;
    size_t length;
};
typedef struct mca_pml_ob1_com_btl_t mca_pml_ob1_com_btl_t;

int mca_pml_ob1_com_btl_comp(const void *v1, const void *v2);

/* Calculate what percentage of a message to send through each BTL according to
 * relative weight */
static inline void
mca_pml_ob1_calc_weighted_length( mca_pml_ob1_com_btl_t *btls, int num_btls, size_t size,
                                  double weight_total )
{
    int i;
    size_t length_left;

    /* shortcut for common case for only one BTL */
    if( OPAL_LIKELY(1 == num_btls) ) {
        btls[0].length = size;
        return;
    }

    /* sort BTLs according of their weights so BTLs with smaller weight will
     * not hijack all of the traffic */
    qsort( btls, num_btls, sizeof(mca_pml_ob1_com_btl_t),
           mca_pml_ob1_com_btl_comp );

    for(length_left = size, i = 0; i < num_btls; i++) {
        mca_bml_base_btl_t* bml_btl = btls[i].bml_btl;
        size_t length = 0;
        if( OPAL_UNLIKELY(0 != length_left) ) {
            length = (length_left > bml_btl->btl->btl_eager_limit)?
                ((size_t)(size * (bml_btl->btl_weight / weight_total))) :
                length_left;

            if(length > length_left)
                length = length_left;
            length_left -= length;
        }
        btls[i].length = length;
    }

    /* account for rounding errors */
    btls[0].length += length_left;
}

/**
 * A thread-safe function that should be called every time we need the OB1
 * progress to be turned (or kept) on.
 */
int mca_pml_ob1_enable_progress(int32_t count);

#endif
