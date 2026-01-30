#pragma once

#include <pthread.h>

#include "vhost/types.h"
#include "vhost_spec.h"

#include "virtio_spec.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Describes parsed buffer chain to be handled by virtio device type
 */
struct virtio_iov {
    uint16_t niov_out;
    uint16_t niov_in;
    struct vhd_buffer *iov_out;
    struct vhd_buffer *iov_in;
    struct vhd_buffer buffers[/* niov_out + niov_in */];
};

struct vhd_memory_map;
struct vhd_memory_log;

struct virtio_virtq {
    const char *log_tag;

    uint32_t flags;
    struct virtq_desc *desc;
    struct virtq_avail *avail;
    struct virtq_used *used;
    uint64_t used_gpa_base;

    /* Size of queue in number of descriptors it can hold */
    uint16_t qsz;

    /* Max chain length (for bug compatibility with non-compliant drivers) */
    uint16_t max_chain_len;

    /* Shadow avail ring index */
    uint16_t  last_avail;

    /*
     * 2.4.5.3.1: A driver MUST NOT create a descriptor chain longer than
     * the Queue Size of the device
     * Thus we can preallocate a scratch area of a known size to accumulate
     * scatter-gather segments before handing them over to the device.
     */
    uint16_t niov_out;
    uint16_t niov_in;
    struct vhd_buffer *buffers;

    /*
     * Virtqueue is broken, probably because there is an invalid descriptor
     * chain in it.
     * Broken status is sticky and so far cannot be repared.
     */
    bool broken;


    /*
     * If set, VIRTIO_F_RING_EVENT_IDX is negotiated for this queue and
     * avail/used_event fields must be used for notification.
     */
    bool has_event_idx;

    /*
     * eventfd for used buffers notification.
     * can be reset after virtq is started.
     */
    int notify_fd;

    /*
     * Whether the processing of this virtq is enabled.
     * Can be toggled after virtq is started.
     */
    bool enabled;

    /* inflight information */
    uint64_t req_cnt;
    struct inflight_split_region *inflight_region;
    bool inflight_check;

    /*
     * these objects are per-device but storing a link on virtqueue facilitates
     * bookkeeping
     */
    struct vhd_memory_map *mm;
    struct vhd_memory_log *log;

    /* Usage statistics */
    struct vq_stat {
        /* Metrics provided to users */
        struct vhd_vq_metrics metrics;

        /* Metrics service info fields. Not provided to uses */
        /* timestamps for periodic metrics */
        time_t period_start_ts;
    } stat;
};

void virtio_virtq_init(struct virtio_virtq *vq);

void virtio_virtq_release(struct virtio_virtq *vq);

bool virtq_is_broken(struct virtio_virtq *vq);

void mark_broken(struct virtio_virtq *vq);

typedef void(*virtq_handle_buffers_cb)(void *arg,
                                       struct virtio_virtq *vq,
                                       struct virtio_iov *iov);
int virtq_dequeue_many(struct virtio_virtq *vq,
                       virtq_handle_buffers_cb handle_buffers_cb,
                       void *arg);

void virtq_push(struct virtio_virtq *vq, struct virtio_iov *iov, uint32_t len);

void virtq_set_notify_fd(struct virtio_virtq *vq, int fd);

void virtio_free_iov(struct virtio_iov *iov);
uint16_t virtio_iov_get_head(struct virtio_iov *iov);

void virtio_virtq_get_stat(struct virtio_virtq *vq,
                           struct vhd_vq_metrics *metrics);

void abort_request(struct virtio_virtq *vq, struct virtio_iov *iov);
#ifdef __cplusplus
}
#endif
