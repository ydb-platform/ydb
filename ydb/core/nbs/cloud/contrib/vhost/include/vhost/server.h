#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "vhost/types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define VHD_MAX_REQUEST_QUEUES 256

struct vhd_vdev;
struct vhd_io;

/**
 * Logging support
 */

enum LogLevel {
    LOG_ERROR = 0,
    LOG_WARNING = 1,
    LOG_INFO = 2,
    LOG_DEBUG = 3
};

typedef void (*log_function)(enum LogLevel level, const char *format, ...);

/**
 * Start vhost server
 *
 * Server will spawn one native thread to wait for incoming vhost handshakes.
 * This thread will only handle global vhost protocol communication.
 * Device I/O events are handled separately by plugging into request queues.
 *
 * Return 0 on success or negative error code.
 */
int vhd_start_vhost_server(log_function log_fn);

/**
 * Stop vhost server
 *
 * Stop vhost event thread which means no new vhost connections are possible
 */
void vhd_stop_vhost_server(void);

/**
 * Request instance stored in request queue
 */
struct vhd_request {
    /* Device that generated this request */
    struct vhd_vdev *vdev;

    /* Device type-specific request data */
    struct vhd_io *io;
};

/**
 * Server request queue
 *
 * Request queues are created by client and attached to vhost device(s).
 * Each device will then send its events to its attched queue.
 * This way request queues serve as a unit of load balancing.
 */
struct vhd_request_queue;

/**
 * Create new request queue
 */
struct vhd_request_queue *vhd_create_request_queue(void);

/**
 * Destroy request queue.
 * Don't call this until there are devices attached to this queue.
 */
void vhd_release_request_queue(struct vhd_request_queue *rq);

/**
 * Run queue in calling thread.
 * Will block until any of the devices enqueue requests.
 * Returns:
 *    0         - when the request queue shouldn't be running any more
 *   -EAGAIN    - when the request should be running further
 *   <0         - on other errors
 */
int vhd_run_queue(struct vhd_request_queue *rq);

/**
 * Unblock running request queue.
 * After calling this vhd_run_queue will eventually return and can the be
 * reeintered.
 */
void vhd_stop_queue(struct vhd_request_queue *rq);

/**
 * Dequeue next request.
 */
bool vhd_dequeue_request(struct vhd_request_queue *rq,
                         struct vhd_request *out_req);

/**
 * Get request queue metrics.
 */
void vhd_get_rq_stat(struct vhd_request_queue *rq,
                     struct vhd_rq_metrics *metrics);

/**
 * Block io request result
 */
enum vhd_bdev_io_result {
    VHD_BDEV_SUCCESS = 0,
    VHD_BDEV_IOERR,
    VHD_BDEV_CANCELED,
};

/*
 * Complete the processing of the request.  The backend calls this to indicate
 * that it's done with the request and the library may signal completion to the
 * guest driver and dispose of the request.
 */
void vhd_complete_bio(struct vhd_io *io, enum vhd_bdev_io_result status);

/**
 * Get private data associated with vdev.
 */
void *vhd_vdev_get_priv(struct vhd_vdev *vdev);

/**
 * Get statistics for device's virtio queue.
 */
int vhd_vdev_get_queue_stat(struct vhd_vdev *vdev, uint32_t queue_num,
                            struct vhd_vq_metrics *metrics);

#ifdef __cplusplus
}
#endif
