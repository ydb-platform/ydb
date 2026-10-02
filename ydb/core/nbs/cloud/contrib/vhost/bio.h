/*
 * Internally used represenation of a block io request passed to and returned
 * from the block backend.
 */

#pragma once

#include "queue.h"
#include "vhost/server.h"

#ifdef __cplusplus
extern "C" {
#endif

struct vhd_vring;

struct vhd_io {
    enum vhd_bdev_io_result status;
    struct vhd_vring *vring;

    void (*completion_handler)(struct vhd_io *io);

    TAILQ_ENTRY(vhd_io) submission_link;
    TAILQ_ENTRY(vhd_io) inflight_link;
    SLIST_ENTRY(vhd_io) completion_link;

    time_t ts;
};

#ifdef __cplusplus
}
#endif
