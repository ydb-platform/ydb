/*
 * Based on QEMU's util/async.c
 *
 * Copyright (c) 2003-2008 Fabrice Bellard
 * Copyright (c) 2009-2017 QEMU contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <semaphore.h>

#include "catomic.h"
#include "queue.h"
#include "platform.h"
#include "event.h"
#include "logging.h"

enum {
    /* Already enqueued and waiting for bh_poll() */
    BH_PENDING   = (1 << 0),

    /* Invoke the callback */
    BH_SCHEDULED = (1 << 1),

    /* Delete without invoking callback */
    BH_DELETED   = (1 << 2),

    /* Delete after invoking callback */
    BH_ONESHOT   = (1 << 3),
};

struct vhd_bh {
    struct vhd_event_loop *ctx;
    vhd_bh_cb *cb;
    void *opaque;
    SLIST_ENTRY(vhd_bh) next;
    unsigned flags;
};

typedef SLIST_HEAD(, vhd_bh) vhd_bh_list;

struct vhd_event_loop {
    int epollfd;

    /* eventfd we use to cancel epoll_wait if needed */
    int notifyfd;

    /* number of currently attached events (for consistency checks) */
    uint32_t num_events_attached;

    bool notified;

    /* vhd_terminate_event_loop has been completed */
    bool is_terminated;

    bool has_home_thread;

    /* preallocated events buffer */
    struct epoll_event *events;
    uint64_t max_events;

    vhd_bh_list bh_list;

    SLIST_HEAD(, vhd_io_handler) deleted_handlers;
};

static void evloop_notify(struct vhd_event_loop *evloop)
{
    if (!catomic_xchg(&evloop->notified, true)) {
        vhd_set_eventfd(evloop->notifyfd);
    }
}

static void notify_accept(struct vhd_event_loop *evloop)
{
    if (catomic_read(&evloop->notified)) {
        vhd_clear_eventfd(evloop->notifyfd);
        catomic_xchg(&evloop->notified, false);
    }
}

/* called concurrently from any thread */
static void bh_enqueue(struct vhd_bh *bh, unsigned new_flags)
{
    struct vhd_event_loop *ctx = bh->ctx;
    unsigned old_flags;

    /*
     * The memory barrier implicit in catomic_fetch_or makes sure that:
     * 1. any writes needed by the callback are done before the locations are
     *    read in the bh_poll.
     * 2. ctx is loaded before the callback has a chance to execute and bh
     *    could be freed.
     * Paired with bh_dequeue().
     */
    old_flags = catomic_fetch_or(&bh->flags, BH_PENDING | new_flags);
    if (!(old_flags & BH_PENDING)) {
        SLIST_INSERT_HEAD_ATOMIC(&ctx->bh_list, bh, next);
    }

    evloop_notify(ctx);
}

/* only called from bh_poll() and bh_cleanup() */
static struct vhd_bh *bh_dequeue(vhd_bh_list *head, unsigned *flags)
{
    struct vhd_bh *bh = SLIST_FIRST_RCU(head);

    if (!bh) {
        return NULL;
    }

    SLIST_REMOVE_HEAD(head, next);

    /*
     * The catomic_and is paired with bh_enqueue().  The implicit memory barrier
     * ensures that the callback sees all writes done by the scheduling thread.
     * It also ensures that the scheduling thread sees the cleared flag before
     * bh->cb has run, and thus will call evloop_notify again if necessary.
     */
    *flags = catomic_fetch_and(&bh->flags, ~(BH_PENDING | BH_SCHEDULED));
    return bh;
}

struct vhd_bh *vhd_bh_new(struct vhd_event_loop *ctx,
                          vhd_bh_cb *cb, void *opaque)
{
    struct vhd_bh *bh = vhd_alloc(sizeof(*bh));
    *bh = (struct vhd_bh){
        .ctx = ctx,
        .cb = cb,
        .opaque = opaque,
    };
    return bh;
}

void vhd_bh_schedule_oneshot(struct vhd_event_loop *ctx,
                             vhd_bh_cb *cb, void *opaque)
{
    struct vhd_bh *bh = vhd_bh_new(ctx, cb, opaque);
    bh_enqueue(bh, BH_SCHEDULED | BH_ONESHOT);
}

void vhd_bh_schedule(struct vhd_bh *bh)
{
    bh_enqueue(bh, BH_SCHEDULED);
}

/* this is async and doesn't interfere with already running bh */
void vhd_bh_cancel(struct vhd_bh *bh)
{
    catomic_and(&bh->flags, ~BH_SCHEDULED);
}

/* this is async; deletion only happens in bh_poll, so need to enqueue first */
void vhd_bh_delete(struct vhd_bh *bh)
{
    bh_enqueue(bh, BH_DELETED);
}


static void bh_call(struct vhd_bh *bh)
{
    bh->cb(bh->opaque);
}

/*
 * Execute bottom halves scheduled so far.  Return true if any progress has
 * been made (i.e. any bh was executed).
 * Multiple occurrences of bh_poll cannot be called concurrently.
 */
static bool bh_poll(struct vhd_event_loop *ctx)
{
    vhd_bh_list bh_list;
    struct vhd_bh *bh;
    unsigned flags;
    bool ret = false;

    SLIST_INIT(&bh_list);
    /* swap bh list from ctx for a fresh one */
    SLIST_MOVE_ATOMIC(&bh_list, &ctx->bh_list);

    for (;;) {
        bh = bh_dequeue(&bh_list, &flags);
        if (!bh) {
            break;
        }

        if ((flags & (BH_SCHEDULED | BH_DELETED)) == BH_SCHEDULED) {
            ret = true;
            bh_call(bh);
        }

        if (flags & (BH_DELETED | BH_ONESHOT)) {
            vhd_free(bh);
        }
    }

    return ret;
}

static void bh_cleanup(struct vhd_event_loop *ctx)
{
    struct vhd_bh *bh;
    unsigned flags;

    for (;;) {
        bh = bh_dequeue(&ctx->bh_list, &flags);
        if (!bh) {
            break;
        }

        /* only deleted bhs may remain */
        assert(flags & BH_DELETED);
        vhd_free(bh);
    }
}

struct vhd_io_handler {
    struct vhd_event_loop *evloop;
    int (*read)(void *opaque);
    /* FIXME: must really include write handler as well */
    void *opaque;
    int fd;

    bool attached;
    SLIST_ENTRY(vhd_io_handler) deleted_entry;
};

static int handle_one_event(struct vhd_io_handler *handler, int event_code)
{
    if ((event_code & (EPOLLIN | EPOLLERR | EPOLLRDHUP)) && handler->read) {
        return handler->read(handler->opaque);
    }

    return 0;
}

static int handle_events(struct vhd_event_loop *evloop, int nevents)
{
    int nerr = 0;
    struct epoll_event *events = evloop->events;

    for (int i = 0; i < nevents; i++) {
        struct vhd_io_handler *handler = events[i].data.ptr;
        /* event loop notifer doesn't use a handler */
        if (!handler) {
            continue;
        }
        /* don't call into detached handler even if it's on the ready list */
        if (!handler->attached) {
            continue;
        }
        if (handle_one_event(handler, events[i].events)) {
            nerr++;
        }
    }

    /*
     * The deleted handlers are detached and won't appear on the ready list any
     * more, so it's now safe to actually delete them.
     */
    while (!SLIST_EMPTY(&evloop->deleted_handlers)) {
        struct vhd_io_handler *handler =
            SLIST_FIRST(&evloop->deleted_handlers);
        SLIST_REMOVE_HEAD(&evloop->deleted_handlers, deleted_entry);
        vhd_free(handler);
    }

    return nerr;
}

struct vhd_event_loop *vhd_create_event_loop(size_t max_events)
{
    int notifyfd;
    int epollfd;

    epollfd = epoll_create1(EPOLL_CLOEXEC);
    if (epollfd < 0) {
        VHD_LOG_ERROR("epoll_create1: %s", strerror(errno));
        return NULL;
    }

    notifyfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notifyfd < 0) {
        VHD_LOG_ERROR("eventfd() failed: %s", strerror(errno));
        goto close_epoll;
    }

    /* Register notify eventfd, make sure it is level-triggered */
    struct epoll_event ev = {
        .events = EPOLLIN,
    };
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, notifyfd, &ev) == -1) {
        VHD_LOG_ERROR("epoll_ctl(EPOLL_CTL_ADD, notifyfd): %s",
                      strerror(errno));
        goto error_out;
    }

    struct vhd_event_loop *evloop = vhd_alloc(sizeof(*evloop));
    max_events++; /* +1 for notify eventfd */
    *evloop = (struct vhd_event_loop) {
        .epollfd = epollfd,
        .notifyfd = notifyfd,
        .max_events = max_events,
        .events = vhd_calloc(sizeof(evloop->events[0]), max_events),
    };
    SLIST_INIT(&evloop->bh_list);
    SLIST_INIT(&evloop->deleted_handlers);

    return evloop;

error_out:
    close(notifyfd);
close_epoll:
    close(epollfd);
    return NULL;
}

static __thread struct vhd_event_loop *home_evloop;

int vhd_run_event_loop(struct vhd_event_loop *evloop, int timeout_ms)
{
    if (!home_evloop) {
        bool had_home_thread = catomic_xchg(&evloop->has_home_thread, true);
        VHD_VERIFY(!had_home_thread);
        home_evloop = evloop;
    }
    VHD_ASSERT(evloop == home_evloop);

    if (evloop->is_terminated) {
        return 0;
    }

    int nev = epoll_wait(evloop->epollfd, evloop->events, evloop->max_events,
                         timeout_ms);
    if (!nev) {
        return -EAGAIN;
    } else if (nev < 0) {
        int ret = -errno;
        if (ret == -EINTR) {
            return -EAGAIN;
        }

        VHD_LOG_ERROR("epoll_wait internal error: %s", strerror(-ret));
        return ret;
    }

    notify_accept(evloop);
    bh_poll(evloop);

    int nerr = handle_events(evloop, nev);
    if (nerr) {
        VHD_LOG_WARN("Got %d events, can't handle %d events", nev, nerr);
        return -EIO;
    }

    return -EAGAIN;
}

static void evloop_stop_bh(void *opaque)
{
    struct vhd_event_loop *evloop = opaque;
    evloop->is_terminated = true;
}

void vhd_terminate_event_loop(struct vhd_event_loop *evloop)
{
    vhd_bh_schedule_oneshot(evloop, evloop_stop_bh, evloop);
}

/*
 * Only free the event loop when there's no concurrent access to it.  One way
 * to do it is to do free at the end of the thread running the event loop.
 * Another is to wait for the thread running the event loop to terminate (to
 * join it) and only do free afterwards.
 */
void vhd_free_event_loop(struct vhd_event_loop *evloop)
{
    VHD_ASSERT(evloop->is_terminated);
    VHD_ASSERT(evloop->num_events_attached == 0);
    bh_cleanup(evloop);
    close(evloop->epollfd);
    close(evloop->notifyfd);
    vhd_free(evloop->events);
    vhd_free(evloop);
}

static void event_loop_inc_events(struct vhd_event_loop *evloop)
{
    evloop->num_events_attached++;
}

static void event_loop_dec_events(struct vhd_event_loop *evloop)
{
    VHD_ASSERT(evloop->num_events_attached > 0);
    evloop->num_events_attached--;
}

int vhd_attach_io_handler(struct vhd_io_handler *handler)
{
    struct vhd_event_loop *evloop = handler->evloop;
    int fd = handler->fd;

    struct epoll_event ev = {
        .events = EPOLLIN | EPOLLHUP | EPOLLRDHUP,
        .data.ptr = handler
    };

    /* to maintain fields consistency only do this in the home event loop */
    VHD_ASSERT(evloop == home_evloop);

    /* unlike detach, multiple attachment is a logic error */
    VHD_ASSERT(!handler->attached);

    if (epoll_ctl(evloop->epollfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        int ret = -errno;
        VHD_LOG_ERROR("Can't add event: %s", strerror(-ret));
        return ret;
    }

    handler->attached = true;

    return 0;
}

struct vhd_io_handler *vhd_add_io_handler(struct vhd_event_loop *evloop,
                                          int fd, int (*read)(void *opaque),
                                          void *opaque)
{
    struct vhd_io_handler *handler;

    handler = vhd_alloc(sizeof(*handler));
    *handler = (struct vhd_io_handler) {
            .evloop = evloop,
            .fd = fd,
            .read = read,
            .opaque = opaque
    };

    if (vhd_attach_io_handler(handler) < 0) {
        goto fail;
    }

    event_loop_inc_events(evloop);
    return handler;
fail:
    vhd_free(handler);
    return NULL;
}

int vhd_detach_io_handler(struct vhd_io_handler *handler)
{
    struct vhd_event_loop *evloop = handler->evloop;

    /* to maintain fields consistency only do this in the home event loop */
    VHD_ASSERT(evloop == home_evloop);

    if (!handler->attached) {
        return 0;
    }

    if (epoll_ctl(evloop->epollfd, EPOLL_CTL_DEL, handler->fd, NULL) < 0) {
        int ret = -errno;
        VHD_LOG_ERROR("Can't delete event: %s", strerror(-ret));
        return ret;
    }

    /*
     * The file descriptor being detached may still be sitting on the ready
     * list returned by epoll_wait.
     * Make sure the handler for it isn't called.
     */
    handler->attached = false;

    return 0;
}

int vhd_del_io_handler(struct vhd_io_handler *handler)
{
    int ret;
    struct vhd_event_loop *evloop = handler->evloop;

    ret = vhd_detach_io_handler(handler);
    if (ret < 0) {
        return ret;
    }

    /*
     * The file descriptor being deleted may still be sitting on the ready list
     * returned by epoll_wait.
     * Schedule it for deallocation at the end of the iteration after the ready
     * event list processing is through.
     */
    SLIST_INSERT_HEAD(&evloop->deleted_handlers, handler, deleted_entry);

    event_loop_dec_events(evloop);
    return 0;
}

void vhd_clear_eventfd(int fd)
{
    eventfd_t unused;
    while (eventfd_read(fd, &unused) && errno == EINTR) {
        ;
    }
}

void vhd_set_eventfd(int fd)
{
    while (eventfd_write(fd, 1) && errno == EINTR) {
        ;
    }
}

struct vhd_work {
    void (*func)(struct vhd_work *, void *);
    void *opaque;
    int ret;
    sem_t wait;
};

void vhd_complete_work(struct vhd_work *work, int ret)
{
    work->ret = ret;
    /*
     * sem_post is a full memory barrier so the vhd_submit_work_and_wait will
     * see ->ret set above
     */
    if (sem_post(&work->wait) < 0) {
        /* log an error and continue as there's no better strategy */
        VHD_LOG_ERROR("sem_post: %s", strerror(errno));
    }
}

static void work_bh(void *opaque)
{
    struct vhd_work *work = opaque;
    work->func(work, work->opaque);
}

int vhd_submit_work_and_wait(struct vhd_event_loop *evloop,
                             void (*func)(struct vhd_work *, void *),
                             void *opaque)
{
    int ret;
    struct vhd_work work = {
        .func = func,
        .opaque = opaque,
    };

    /* waiting for completion in the same event loop would deadlock */
    VHD_ASSERT(evloop != home_evloop);

    /* sem_init can't fail when both arguments are zero */
    ret = sem_init(&work.wait, 0, 0);
    VHD_ASSERT(ret == 0);

    vhd_bh_schedule_oneshot(evloop, work_bh, &work);

    /*
     * sem_wait may fail with either EINTR (we handle it) or EINVAL when
     * called on invalid pointer, which is impossible here.
     */
    do {
        ret = sem_wait(&work.wait);
    } while (ret < 0 && errno == EINTR);
    VHD_ASSERT(ret == 0);

    /*
     * sem_destroy may fail only with EINVAL when called on invalid pointer,
     * which is impossible here.
     */
    ret = sem_destroy(&work.wait);
    VHD_ASSERT(ret == 0);

    /*
     * sem_wait is a full memory barrier so this is the ->ret set in
     * vhd_complete_work
     */
    return work.ret;
}
