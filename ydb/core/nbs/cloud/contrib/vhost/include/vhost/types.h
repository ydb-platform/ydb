/**
 * Common types' definitions
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

struct vhd_buffer {
    void *base;
    size_t len;

    /* Buffer is write-only if true and read-only if false */
    bool write_only;
};

struct vhd_sglist {
    uint32_t nbuffers;
    struct vhd_buffer *buffers;
};

/**
 * virtqueue usage statistics
 */
struct vhd_vq_metrics {
    /* Dispatch counters */
    /* number of times vring was processed */
    uint64_t dispatch_total;
    /* number of times vring was empty on processing */
    uint64_t dispatch_empty;

    /* Request counters */
    /* total amount of requests processed */
    uint64_t request_total;
    /* total amount of requests completed */
    uint64_t request_completed;

    /* Other counters*/
    /* number of requests was dispatched from vring last time*/
    uint16_t queue_len_last;
    /* max queue len was processed during 60s period */
    uint16_t queue_len_max_60s;
};

/**
 * request queue usage statistics
 */
struct vhd_rq_metrics {
    /* number of requests read from guest and put to internal queue */
    uint64_t enqueued;
    /* number of requests dispatched for handling */
    uint64_t dequeued;
    /* number of requests completed externally and scheduled for completion in rq */
    uint64_t completions_received;
    /* number of requests completed and reported to guest */
    uint64_t completed;
    /* number of requests canceled from internal queue before dispatch */
    uint64_t cancelled;

    /* timestamp of oldest infight request */
    time_t oldest_inflight_ts;
};

#ifdef __cplusplus
}
#endif
