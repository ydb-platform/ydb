#pragma once

#include "vhost/server.h"

struct vhd_io_handler;
/* Add io handler to vhost control event loop */
struct vhd_io_handler *vhd_add_vhost_io_handler(int fd, int (*read)(void *),
                                                void *opaque);

struct vhd_request_queue;
/* Add io handler to request queue event loop */
struct vhd_io_handler *vhd_add_rq_io_handler(struct vhd_request_queue *rq,
                                             int fd, int (*read)(void *),
                                             void *opaque);

struct vhd_vdev;
struct vhd_io;
struct vhd_vring;

/**
 * Enqueue IO request
 */
int vhd_enqueue_request(struct vhd_request_queue *rq,
                        struct vhd_io *io);

void vhd_cancel_queued_requests(struct vhd_request_queue *rq,
                                const struct vhd_vring *vring);

/**
 * Run callback in request queue
 */
void vhd_run_in_rq(struct vhd_request_queue *rq, void (*cb)(void *),
                   void *opaque);

/*
 * Run callback in vhost control event loop
 */
void vhd_run_in_ctl(void (*cb)(void *), void *opaque);

/*
 * Submit a work item onto vhost control event loop and wait till it's
 * finished.
 */
struct vhd_work;
int vhd_submit_ctl_work_and_wait(void (*func)(struct vhd_work *, void *),
                                 void *opaque);
