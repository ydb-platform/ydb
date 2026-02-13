#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VHD_EVENT_LOOP_DEFAULT_MAX_EVENTS 32

/**
 * Event loop instance
 *
 * Each event loop will run in a thread which calls vhd_run_event_loop.
 * Events detected in given event loop iteration will also be handled in this
 * thread.
 *
 * Event loop management operations (add/remove events) are thread-safe,
 * although changes to list of events may not be visible until next
 * vhd_run_event_loop.
 */
struct vhd_event_loop;

/**
 * Create new event loop.
 * @max_events      How many events we can handle in one iteration.
 *                  Events are reported in FIFO order to avoid starvation.
 */
struct vhd_event_loop *vhd_create_event_loop(size_t max_events);

/**
 * Free event loop.
 */
void vhd_free_event_loop(struct vhd_event_loop *evloop);

/**
 * Run a single iteration of the event loop
 *
 * @timeout     0 to return immediately, -1 to block indefinitely, milliseconds
 *              value otherwise.
 *
 * @return      0 if the event loop is terminated upon request
 *              -EAGAIN if the event loop should keep going
 *              another negative code on error
 */
int vhd_run_event_loop(struct vhd_event_loop *evloop, int timeout_ms);

/**
 * Request event loop termination
 */
void vhd_terminate_event_loop(struct vhd_event_loop *evloop);

/* I/O handling to be associated with a file descriptor */
struct vhd_io_handler;

/*
 * Add io handler @read for @fd and attach it to @evloop.
 * For safe data access must be called in @evloop only.
 */
struct vhd_io_handler *vhd_add_io_handler(struct vhd_event_loop *evloop,
                                          int fd, int (*read)(void *),
                                          void *opaque);

/*
 * Stop monitoring io handler @handler's file descriptor and calling its
 * handler functions.
 * For safe data access must be called in @handler's event loop only.
 */

int vhd_detach_io_handler(struct vhd_io_handler *handler);
/*
 * Resume monitoring io handler @handler's file descriptor and calling its
 * handler functions.
 * For safe data access must be called in @handler's event loop only.
 */
int vhd_attach_io_handler(struct vhd_io_handler *handler);

/*
 * Detach io handler @handler from its event loop and delete it.
 * For safe data access must be called in @handler's event loop only.
 */
int vhd_del_io_handler(struct vhd_io_handler *handler);

/**
 * Clear eventfd after handling it
 */
void vhd_clear_eventfd(int fd);

/**
 * Trigger eventfd
 */
void vhd_set_eventfd(int fd);

struct vhd_bh;
typedef void vhd_bh_cb(void *opaque);

struct vhd_bh *vhd_bh_new(struct vhd_event_loop *ctx,
                          vhd_bh_cb *cb, void *opaque);
void vhd_bh_schedule_oneshot(struct vhd_event_loop *ctx,
                             vhd_bh_cb *cb, void *opaque);
void vhd_bh_schedule(struct vhd_bh *bh);
void vhd_bh_cancel(struct vhd_bh *bh);
void vhd_bh_delete(struct vhd_bh *bh);

/*
 * Submit a work item onto @evloop and wait till it's finished.
 * Must not be called in the target event loop.
 *
 * Returns exactly the value which user sets by vhd_complete_work(), no other
 * errors possible.
 */
struct vhd_work;
int vhd_submit_work_and_wait(struct vhd_event_loop *evloop,
                             void (*func)(struct vhd_work *, void *),
                             void *opaque);
/*
 * Signal work completion to the submitter
 */
void vhd_complete_work(struct vhd_work *work, int ret);

#ifdef __cplusplus
}
#endif
