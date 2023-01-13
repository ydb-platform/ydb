/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/event_loop.h>

#include <aws/io/logging.h>

#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/mutex.h>
#include <aws/common/task_scheduler.h>
#include <aws/common/thread.h>

#if defined(__FreeBSD__) || defined(__NetBSD__)
#    define __BSD_VISIBLE 1
#    include <sys/types.h>
#endif

#include <sys/event.h>

#include <aws/io/io.h>
#include <limits.h>
#include <unistd.h>

static void s_destroy(struct aws_event_loop *event_loop);
static int s_run(struct aws_event_loop *event_loop);
static int s_stop(struct aws_event_loop *event_loop);
static int s_wait_for_stop_completion(struct aws_event_loop *event_loop);
static void s_schedule_task_now(struct aws_event_loop *event_loop, struct aws_task *task);
static void s_schedule_task_future(struct aws_event_loop *event_loop, struct aws_task *task, uint64_t run_at_nanos);
static void s_cancel_task(struct aws_event_loop *event_loop, struct aws_task *task);
static int s_subscribe_to_io_events(
    struct aws_event_loop *event_loop,
    struct aws_io_handle *handle,
    int events,
    aws_event_loop_on_event_fn *on_event,
    void *user_data);
static int s_unsubscribe_from_io_events(struct aws_event_loop *event_loop, struct aws_io_handle *handle);
static void s_free_io_event_resources(void *user_data);
static bool s_is_event_thread(struct aws_event_loop *event_loop);

static void aws_event_loop_thread(void *user_data);

int aws_open_nonblocking_posix_pipe(int pipe_fds[2]);

enum event_thread_state {
    EVENT_THREAD_STATE_READY_TO_RUN,
    EVENT_THREAD_STATE_RUNNING,
    EVENT_THREAD_STATE_STOPPING,
};

enum pipe_fd_index {
    READ_FD,
    WRITE_FD,
};

struct kqueue_loop {
    /* thread_created_on is the handle to the event loop thread. */
    struct aws_thread thread_created_on;
    /* thread_joined_to is used by the thread destroying the event loop. */
    aws_thread_id_t thread_joined_to;
    /* running_thread_id is NULL if the event loop thread is stopped or points-to the thread_id of the thread running
     * the event loop (either thread_created_on or thread_joined_to). Atomic because of concurrent writes (e.g.,
     * run/stop) and reads (e.g., is_event_loop_thread).
     * An aws_thread_id_t variable itself cannot be atomic because it is an opaque type that is platform-dependent. */
    struct aws_atomic_var running_thread_id;
    int kq_fd; /* kqueue file descriptor */

    /* Pipe for signaling to event-thread that cross_thread_data has changed. */
    int cross_thread_signal_pipe[2];

    /* cross_thread_data holds things that must be communicated across threads.
     * When the event-thread is running, the mutex must be locked while anyone touches anything in cross_thread_data.
     * If this data is modified outside the thread, the thread is signaled via activity on a pipe. */
    struct {
        struct aws_mutex mutex;
        bool thread_signaled; /* whether thread has been signaled about changes to cross_thread_data */
        struct aws_linked_list tasks_to_schedule;
        enum event_thread_state state;
    } cross_thread_data;

    /* thread_data holds things which, when the event-thread is running, may only be touched by the thread */
    struct {
        struct aws_task_scheduler scheduler;

        int connected_handle_count;

        /* These variables duplicate ones in cross_thread_data. We move values out while holding the mutex and operate
         * on them later */
        enum event_thread_state state;
    } thread_data;

    struct aws_thread_options thread_options;
};

/* Data attached to aws_io_handle while the handle is subscribed to io events */
struct handle_data {
    struct aws_io_handle *owner;
    struct aws_event_loop *event_loop;
    aws_event_loop_on_event_fn *on_event;
    void *on_event_user_data;

    int events_subscribed; /* aws_io_event_types this handle should be subscribed to */
    int events_this_loop;  /* aws_io_event_types received during current loop of the event-thread */

    enum { HANDLE_STATE_SUBSCRIBING, HANDLE_STATE_SUBSCRIBED, HANDLE_STATE_UNSUBSCRIBED } state;

    struct aws_task subscribe_task;
    struct aws_task cleanup_task;
};

enum {
    DEFAULT_TIMEOUT_SEC = 100, /* Max kevent() timeout per loop of the event-thread */
    MAX_EVENTS = 100,          /* Max kevents to process per loop of the event-thread */
};

struct aws_event_loop_vtable s_kqueue_vtable = {
    .destroy = s_destroy,
    .run = s_run,
    .stop = s_stop,
    .wait_for_stop_completion = s_wait_for_stop_completion,
    .schedule_task_now = s_schedule_task_now,
    .schedule_task_future = s_schedule_task_future,
    .subscribe_to_io_events = s_subscribe_to_io_events,
    .cancel_task = s_cancel_task,
    .unsubscribe_from_io_events = s_unsubscribe_from_io_events,
    .free_io_event_resources = s_free_io_event_resources,
    .is_on_callers_thread = s_is_event_thread,
};

struct aws_event_loop *aws_event_loop_new_default_with_options(
    struct aws_allocator *alloc,
    const struct aws_event_loop_options *options) {
    AWS_ASSERT(alloc);
    AWS_ASSERT(clock);
    AWS_ASSERT(options);
    AWS_ASSERT(options->clock);

    bool clean_up_event_loop_mem = false;
    bool clean_up_event_loop_base = false;
    bool clean_up_impl_mem = false;
    bool clean_up_thread = false;
    bool clean_up_kqueue = false;
    bool clean_up_signal_pipe = false;
    bool clean_up_signal_kevent = false;
    bool clean_up_mutex = false;

    struct aws_event_loop *event_loop = aws_mem_acquire(alloc, sizeof(struct aws_event_loop));
    if (!event_loop) {
        return NULL;
    }

    AWS_LOGF_INFO(AWS_LS_IO_EVENT_LOOP, "id=%p: Initializing edge-triggered kqueue", (void *)event_loop);
    clean_up_event_loop_mem = true;

    int err = aws_event_loop_init_base(event_loop, alloc, options->clock);
    if (err) {
        goto clean_up;
    }
    clean_up_event_loop_base = true;

    struct kqueue_loop *impl = aws_mem_calloc(alloc, 1, sizeof(struct kqueue_loop));
    if (!impl) {
        goto clean_up;
    }

    if (options->thread_options) {
        impl->thread_options = *options->thread_options;
    } else {
        impl->thread_options = *aws_default_thread_options();
    }

    /* intialize thread id to NULL. It will be set when the event loop thread starts. */
    aws_atomic_init_ptr(&impl->running_thread_id, NULL);
    clean_up_impl_mem = true;

    err = aws_thread_init(&impl->thread_created_on, alloc);
    if (err) {
        goto clean_up;
    }
    clean_up_thread = true;

    impl->kq_fd = kqueue();
    if (impl->kq_fd == -1) {
        AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "id=%p: Failed to open kqueue handle.", (void *)event_loop);
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
        goto clean_up;
    }
    clean_up_kqueue = true;

    err = aws_open_nonblocking_posix_pipe(impl->cross_thread_signal_pipe);
    if (err) {
        AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "id=%p: failed to open pipe handle.", (void *)event_loop);
        goto clean_up;
    }
    AWS_LOGF_TRACE(
        AWS_LS_IO_EVENT_LOOP,
        "id=%p: pipe descriptors read %d, write %d.",
        (void *)event_loop,
        impl->cross_thread_signal_pipe[READ_FD],
        impl->cross_thread_signal_pipe[WRITE_FD]);
    clean_up_signal_pipe = true;

    /* Set up kevent to handle activity on the cross_thread_signal_pipe */
    struct kevent thread_signal_kevent;
    EV_SET(
        &thread_signal_kevent,
        impl->cross_thread_signal_pipe[READ_FD],
        EVFILT_READ /*filter*/,
        EV_ADD | EV_CLEAR /*flags*/,
        0 /*fflags*/,
        0 /*data*/,
        NULL /*udata*/);

    int res = kevent(
        impl->kq_fd,
        &thread_signal_kevent /*changelist*/,
        1 /*nchanges*/,
        NULL /*eventlist*/,
        0 /*nevents*/,
        NULL /*timeout*/);

    if (res == -1) {
        AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "id=%p: failed to create cross-thread signal kevent.", (void *)event_loop);
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
        goto clean_up;
    }
    clean_up_signal_kevent = true;

    err = aws_mutex_init(&impl->cross_thread_data.mutex);
    if (err) {
        goto clean_up;
    }
    clean_up_mutex = true;

    impl->cross_thread_data.thread_signaled = false;

    aws_linked_list_init(&impl->cross_thread_data.tasks_to_schedule);

    impl->cross_thread_data.state = EVENT_THREAD_STATE_READY_TO_RUN;

    err = aws_task_scheduler_init(&impl->thread_data.scheduler, alloc);
    if (err) {
        goto clean_up;
    }

    impl->thread_data.state = EVENT_THREAD_STATE_READY_TO_RUN;

    event_loop->impl_data = impl;

    event_loop->vtable = &s_kqueue_vtable;

    /* success */
    return event_loop;

clean_up:
    if (clean_up_mutex) {
        aws_mutex_clean_up(&impl->cross_thread_data.mutex);
    }
    if (clean_up_signal_kevent) {
        thread_signal_kevent.flags = EV_DELETE;
        kevent(
            impl->kq_fd,
            &thread_signal_kevent /*changelist*/,
            1 /*nchanges*/,
            NULL /*eventlist*/,
            0 /*nevents*/,
            NULL /*timeout*/);
    }
    if (clean_up_signal_pipe) {
        close(impl->cross_thread_signal_pipe[READ_FD]);
        close(impl->cross_thread_signal_pipe[WRITE_FD]);
    }
    if (clean_up_kqueue) {
        close(impl->kq_fd);
    }
    if (clean_up_thread) {
        aws_thread_clean_up(&impl->thread_created_on);
    }
    if (clean_up_impl_mem) {
        aws_mem_release(alloc, impl);
    }
    if (clean_up_event_loop_base) {
        aws_event_loop_clean_up_base(event_loop);
    }
    if (clean_up_event_loop_mem) {
        aws_mem_release(alloc, event_loop);
    }
    return NULL;
}

static void s_destroy(struct aws_event_loop *event_loop) {
    AWS_LOGF_INFO(AWS_LS_IO_EVENT_LOOP, "id=%p: destroying event_loop", (void *)event_loop);
    struct kqueue_loop *impl = event_loop->impl_data;

    /* Stop the event-thread. This might have already happened. It's safe to call multiple times. */
    s_stop(event_loop);
    int err = s_wait_for_stop_completion(event_loop);
    if (err) {
        AWS_LOGF_WARN(
            AWS_LS_IO_EVENT_LOOP,
            "id=%p: failed to destroy event-thread, resources have been leaked",
            (void *)event_loop);
        AWS_ASSERT("Failed to destroy event-thread, resources have been leaked." == NULL);
        return;
    }
    /* setting this so that canceled tasks don't blow up when asking if they're on the event-loop thread. */
    impl->thread_joined_to = aws_thread_current_thread_id();
    aws_atomic_store_ptr(&impl->running_thread_id, &impl->thread_joined_to);

    /* Clean up task-related stuff first. It's possible the a cancelled task adds further tasks to this event_loop.
     * Tasks added in this way will be in cross_thread_data.tasks_to_schedule, so we clean that up last */

    aws_task_scheduler_clean_up(&impl->thread_data.scheduler); /* Tasks in scheduler get cancelled*/

    while (!aws_linked_list_empty(&impl->cross_thread_data.tasks_to_schedule)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&impl->cross_thread_data.tasks_to_schedule);
        struct aws_task *task = AWS_CONTAINER_OF(node, struct aws_task, node);
        task->fn(task, task->arg, AWS_TASK_STATUS_CANCELED);
    }

    /* Warn user if aws_io_handle was subscribed, but never unsubscribed. This would cause memory leaks. */
    AWS_ASSERT(impl->thread_data.connected_handle_count == 0);

    /* Clean up everything else */
    aws_mutex_clean_up(&impl->cross_thread_data.mutex);

    struct kevent thread_signal_kevent;
    EV_SET(
        &thread_signal_kevent,
        impl->cross_thread_signal_pipe[READ_FD],
        EVFILT_READ /*filter*/,
        EV_DELETE /*flags*/,
        0 /*fflags*/,
        0 /*data*/,
        NULL /*udata*/);

    kevent(
        impl->kq_fd,
        &thread_signal_kevent /*changelist*/,
        1 /*nchanges*/,
        NULL /*eventlist*/,
        0 /*nevents*/,
        NULL /*timeout*/);

    close(impl->cross_thread_signal_pipe[READ_FD]);
    close(impl->cross_thread_signal_pipe[WRITE_FD]);
    close(impl->kq_fd);
    aws_thread_clean_up(&impl->thread_created_on);
    aws_mem_release(event_loop->alloc, impl);
    aws_event_loop_clean_up_base(event_loop);
    aws_mem_release(event_loop->alloc, event_loop);
}

static int s_run(struct aws_event_loop *event_loop) {
    struct kqueue_loop *impl = event_loop->impl_data;

    AWS_LOGF_INFO(AWS_LS_IO_EVENT_LOOP, "id=%p: starting event-loop thread.", (void *)event_loop);
    /* to re-run, call stop() and wait_for_stop_completion() */
    AWS_ASSERT(impl->cross_thread_data.state == EVENT_THREAD_STATE_READY_TO_RUN);
    AWS_ASSERT(impl->thread_data.state == EVENT_THREAD_STATE_READY_TO_RUN);

    /* Since thread isn't running it's ok to touch thread_data,
     * and it's ok to touch cross_thread_data without locking the mutex */
    impl->cross_thread_data.state = EVENT_THREAD_STATE_RUNNING;

    aws_thread_increment_unjoined_count();
    int err =
        aws_thread_launch(&impl->thread_created_on, aws_event_loop_thread, (void *)event_loop, &impl->thread_options);

    if (err) {
        aws_thread_decrement_unjoined_count();
        AWS_LOGF_FATAL(AWS_LS_IO_EVENT_LOOP, "id=%p: thread creation failed.", (void *)event_loop);
        goto clean_up;
    }

    return AWS_OP_SUCCESS;

clean_up:
    impl->cross_thread_data.state = EVENT_THREAD_STATE_READY_TO_RUN;
    return AWS_OP_ERR;
}

/* This function can't fail, we're relying on the thread responding to critical messages (ex: stop thread) */
void signal_cross_thread_data_changed(struct aws_event_loop *event_loop) {
    struct kqueue_loop *impl = event_loop->impl_data;

    AWS_LOGF_TRACE(
        AWS_LS_IO_EVENT_LOOP,
        "id=%p: signaling event-loop that cross-thread tasks need to be scheduled.",
        (void *)event_loop);
    /* Doesn't actually matter what we write, any activity on pipe signals that cross_thread_data has changed,
     * If the pipe is full and the write fails, that's fine, the event-thread will get the signal from some previous
     * write */
    uint32_t write_whatever = 0xC0FFEE;
    write(impl->cross_thread_signal_pipe[WRITE_FD], &write_whatever, sizeof(write_whatever));
}

static int s_stop(struct aws_event_loop *event_loop) {
    struct kqueue_loop *impl = event_loop->impl_data;

    bool signal_thread = false;

    { /* Begin critical section */
        aws_mutex_lock(&impl->cross_thread_data.mutex);
        if (impl->cross_thread_data.state == EVENT_THREAD_STATE_RUNNING) {
            impl->cross_thread_data.state = EVENT_THREAD_STATE_STOPPING;
            signal_thread = !impl->cross_thread_data.thread_signaled;
            impl->cross_thread_data.thread_signaled = true;
        }
        aws_mutex_unlock(&impl->cross_thread_data.mutex);
    } /* End critical section */

    if (signal_thread) {
        signal_cross_thread_data_changed(event_loop);
    }

    return AWS_OP_SUCCESS;
}

static int s_wait_for_stop_completion(struct aws_event_loop *event_loop) {
    struct kqueue_loop *impl = event_loop->impl_data;

#ifdef DEBUG_BUILD
    aws_mutex_lock(&impl->cross_thread_data.mutex);
    /* call stop() before wait_for_stop_completion() or you'll wait forever */
    AWS_ASSERT(impl->cross_thread_data.state != EVENT_THREAD_STATE_RUNNING);
    aws_mutex_unlock(&impl->cross_thread_data.mutex);
#endif

    int err = aws_thread_join(&impl->thread_created_on);
    aws_thread_decrement_unjoined_count();
    if (err) {
        return AWS_OP_ERR;
    }

    /* Since thread is no longer running it's ok to touch thread_data,
     * and it's ok to touch cross_thread_data without locking the mutex */
    impl->cross_thread_data.state = EVENT_THREAD_STATE_READY_TO_RUN;
    impl->thread_data.state = EVENT_THREAD_STATE_READY_TO_RUN;

    return AWS_OP_SUCCESS;
}

/* Common functionality for "now" and "future" task scheduling.
 * If `run_at_nanos` is zero then the task is scheduled as a "now" task. */
static void s_schedule_task_common(struct aws_event_loop *event_loop, struct aws_task *task, uint64_t run_at_nanos) {
    AWS_ASSERT(task);
    struct kqueue_loop *impl = event_loop->impl_data;

    /* If we're on the event-thread, just schedule it directly */
    if (s_is_event_thread(event_loop)) {
        AWS_LOGF_TRACE(
            AWS_LS_IO_EVENT_LOOP,
            "id=%p: scheduling task %p in-thread for timestamp %llu",
            (void *)event_loop,
            (void *)task,
            (unsigned long long)run_at_nanos);
        if (run_at_nanos == 0) {
            aws_task_scheduler_schedule_now(&impl->thread_data.scheduler, task);
        } else {
            aws_task_scheduler_schedule_future(&impl->thread_data.scheduler, task, run_at_nanos);
        }
        return;
    }

    /* Otherwise, add it to cross_thread_data.tasks_to_schedule and signal the event-thread to process it */
    AWS_LOGF_TRACE(
        AWS_LS_IO_EVENT_LOOP,
        "id=%p: scheduling task %p cross-thread for timestamp %llu",
        (void *)event_loop,
        (void *)task,
        (unsigned long long)run_at_nanos);
    task->timestamp = run_at_nanos;
    bool should_signal_thread = false;

    /* Begin critical section */
    aws_mutex_lock(&impl->cross_thread_data.mutex);
    aws_linked_list_push_back(&impl->cross_thread_data.tasks_to_schedule, &task->node);

    /* Signal thread that cross_thread_data has changed (unless it's been signaled already) */
    if (!impl->cross_thread_data.thread_signaled) {
        should_signal_thread = true;
        impl->cross_thread_data.thread_signaled = true;
    }

    aws_mutex_unlock(&impl->cross_thread_data.mutex);
    /* End critical section */

    if (should_signal_thread) {
        signal_cross_thread_data_changed(event_loop);
    }
}

static void s_schedule_task_now(struct aws_event_loop *event_loop, struct aws_task *task) {
    s_schedule_task_common(event_loop, task, 0); /* Zero is used to denote "now" tasks */
}

static void s_schedule_task_future(struct aws_event_loop *event_loop, struct aws_task *task, uint64_t run_at_nanos) {
    s_schedule_task_common(event_loop, task, run_at_nanos);
}

static void s_cancel_task(struct aws_event_loop *event_loop, struct aws_task *task) {
    struct kqueue_loop *kqueue_loop = event_loop->impl_data;
    AWS_LOGF_TRACE(AWS_LS_IO_EVENT_LOOP, "id=%p: cancelling task %p", (void *)event_loop, (void *)task);
    aws_task_scheduler_cancel_task(&kqueue_loop->thread_data.scheduler, task);
}

/* Scheduled task that connects aws_io_handle with the kqueue */
static void s_subscribe_task(struct aws_task *task, void *user_data, enum aws_task_status status) {
    (void)task;
    struct handle_data *handle_data = user_data;
    struct aws_event_loop *event_loop = handle_data->event_loop;
    struct kqueue_loop *impl = handle_data->event_loop->impl_data;

    impl->thread_data.connected_handle_count++;

    /* if task was cancelled, nothing to do */
    if (status == AWS_TASK_STATUS_CANCELED) {
        return;
    }

    /* If handle was unsubscribed before this task could execute, nothing to do */
    if (handle_data->state == HANDLE_STATE_UNSUBSCRIBED) {
        return;
    }

    AWS_ASSERT(handle_data->state == HANDLE_STATE_SUBSCRIBING);
    AWS_LOGF_TRACE(
        AWS_LS_IO_EVENT_LOOP, "id=%p: subscribing to events on fd %d", (void *)event_loop, handle_data->owner->data.fd);

    /* In order to monitor both reads and writes, kqueue requires you to add two separate kevents.
     * If we're adding two separate kevents, but one of those fails, we need to remove the other kevent.
     * Therefore we use the EV_RECEIPT flag. This causes kevent() to tell whether each EV_ADD succeeded,
     * rather than the usual behavior of telling us about recent events. */
    struct kevent changelist[2];
    AWS_ZERO_ARRAY(changelist);

    int changelist_size = 0;

    if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_READABLE) {
        EV_SET(
            &changelist[changelist_size++],
            handle_data->owner->data.fd,
            EVFILT_READ /*filter*/,
            EV_ADD | EV_RECEIPT | EV_CLEAR /*flags*/,
            0 /*fflags*/,
            0 /*data*/,
            handle_data /*udata*/);
    }
    if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_WRITABLE) {
        EV_SET(
            &changelist[changelist_size++],
            handle_data->owner->data.fd,
            EVFILT_WRITE /*filter*/,
            EV_ADD | EV_RECEIPT | EV_CLEAR /*flags*/,
            0 /*fflags*/,
            0 /*data*/,
            handle_data /*udata*/);
    }

    int num_events = kevent(
        impl->kq_fd,
        changelist /*changelist*/,
        changelist_size /*nchanges*/,
        changelist /*eventlist. It's OK to re-use the same memory for changelist input and eventlist output*/,
        changelist_size /*nevents*/,
        NULL /*timeout*/);
    if (num_events == -1) {
        goto subscribe_failed;
    }

    /* Look through results to see if any failed */
    for (int i = 0; i < num_events; ++i) {
        /* Every result should be flagged as error, that's just how EV_RECEIPT works */
        AWS_ASSERT(changelist[i].flags & EV_ERROR);

        /* If a real error occurred, .data contains the error code */
        if (changelist[i].data != 0) {
            goto subscribe_failed;
        }
    }

    /* Success */
    handle_data->state = HANDLE_STATE_SUBSCRIBED;
    return;

subscribe_failed:
    AWS_LOGF_ERROR(
        AWS_LS_IO_EVENT_LOOP,
        "id=%p: failed to subscribe to events on fd %d",
        (void *)event_loop,
        handle_data->owner->data.fd);
    /* Remove any related kevents that succeeded */
    for (int i = 0; i < num_events; ++i) {
        if (changelist[i].data == 0) {
            changelist[i].flags = EV_DELETE;
            kevent(
                impl->kq_fd,
                &changelist[i] /*changelist*/,
                1 /*nchanges*/,
                NULL /*eventlist*/,
                0 /*nevents*/,
                NULL /*timeout*/);
        }
    }

    /* We can't return an error code because this was a scheduled task.
     * Notify the user of the failed subscription by passing AWS_IO_EVENT_TYPE_ERROR to the callback. */
    handle_data->on_event(event_loop, handle_data->owner, AWS_IO_EVENT_TYPE_ERROR, handle_data->on_event_user_data);
}

static int s_subscribe_to_io_events(
    struct aws_event_loop *event_loop,
    struct aws_io_handle *handle,
    int events,
    aws_event_loop_on_event_fn *on_event,
    void *user_data) {

    AWS_ASSERT(event_loop);
    AWS_ASSERT(handle->data.fd != -1);
    AWS_ASSERT(handle->additional_data == NULL);
    AWS_ASSERT(on_event);
    /* Must subscribe for read, write, or both */
    AWS_ASSERT(events & (AWS_IO_EVENT_TYPE_READABLE | AWS_IO_EVENT_TYPE_WRITABLE));

    struct handle_data *handle_data = aws_mem_calloc(event_loop->alloc, 1, sizeof(struct handle_data));
    if (!handle_data) {
        return AWS_OP_ERR;
    }

    handle_data->owner = handle;
    handle_data->event_loop = event_loop;
    handle_data->on_event = on_event;
    handle_data->on_event_user_data = user_data;
    handle_data->events_subscribed = events;
    handle_data->state = HANDLE_STATE_SUBSCRIBING;

    handle->additional_data = handle_data;

    /* We schedule a task to perform the actual changes to the kqueue, read on for an explanation why...
     *
     * kqueue requires separate registrations for read and write events.
     * If the user wants to know about both read and write, we need register once for read and once for write.
     * If the first registration succeeds, but the second registration fails, we need to delete the first registration.
     * If this all happened outside the event-thread, the successful registration's events could begin processing
     * in the brief window of time before the registration is deleted. */

    aws_task_init(&handle_data->subscribe_task, s_subscribe_task, handle_data, "kqueue_event_loop_subscribe");
    s_schedule_task_now(event_loop, &handle_data->subscribe_task);

    return AWS_OP_SUCCESS;
}

static void s_free_io_event_resources(void *user_data) {
    struct handle_data *handle_data = user_data;
    struct kqueue_loop *impl = handle_data->event_loop->impl_data;

    impl->thread_data.connected_handle_count--;

    aws_mem_release(handle_data->event_loop->alloc, handle_data);
}

static void s_clean_up_handle_data_task(struct aws_task *task, void *user_data, enum aws_task_status status) {
    (void)task;
    (void)status;

    struct handle_data *handle_data = user_data;
    s_free_io_event_resources(handle_data);
}

static int s_unsubscribe_from_io_events(struct aws_event_loop *event_loop, struct aws_io_handle *handle) {
    AWS_LOGF_TRACE(
        AWS_LS_IO_EVENT_LOOP, "id=%p: un-subscribing from events on fd %d", (void *)event_loop, handle->data.fd);
    AWS_ASSERT(handle->additional_data);
    struct handle_data *handle_data = handle->additional_data;
    struct kqueue_loop *impl = event_loop->impl_data;

    AWS_ASSERT(event_loop == handle_data->event_loop);

    /* If the handle was successfully subscribed to kqueue, then remove it. */
    if (handle_data->state == HANDLE_STATE_SUBSCRIBED) {
        struct kevent changelist[2];
        int changelist_size = 0;

        if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_READABLE) {
            EV_SET(
                &changelist[changelist_size++],
                handle_data->owner->data.fd,
                EVFILT_READ /*filter*/,
                EV_DELETE /*flags*/,
                0 /*fflags*/,
                0 /*data*/,
                handle_data /*udata*/);
        }
        if (handle_data->events_subscribed & AWS_IO_EVENT_TYPE_WRITABLE) {
            EV_SET(
                &changelist[changelist_size++],
                handle_data->owner->data.fd,
                EVFILT_WRITE /*filter*/,
                EV_DELETE /*flags*/,
                0 /*fflags*/,
                0 /*data*/,
                handle_data /*udata*/);
        }

        kevent(impl->kq_fd, changelist, changelist_size, NULL /*eventlist*/, 0 /*nevents*/, NULL /*timeout*/);
    }

    /* Schedule a task to clean up the memory. This is done in a task to prevent the following scenario:
     * - While processing a batch of events, some callback unsubscribes another aws_io_handle.
     * - One of the other events in this batch belongs to that other aws_io_handle.
     * - If the handle_data were already deleted, there would be an access invalid memory. */

    aws_task_init(
        &handle_data->cleanup_task, s_clean_up_handle_data_task, handle_data, "kqueue_event_loop_clean_up_handle_data");
    aws_event_loop_schedule_task_now(event_loop, &handle_data->cleanup_task);

    handle_data->state = HANDLE_STATE_UNSUBSCRIBED;
    handle->additional_data = NULL;

    return AWS_OP_SUCCESS;
}

static bool s_is_event_thread(struct aws_event_loop *event_loop) {
    struct kqueue_loop *impl = event_loop->impl_data;

    aws_thread_id_t *thread_id = aws_atomic_load_ptr(&impl->running_thread_id);
    return thread_id && aws_thread_thread_id_equal(*thread_id, aws_thread_current_thread_id());
}

/* Called from thread.
 * Takes tasks from tasks_to_schedule and adds them to the scheduler. */
static void s_process_tasks_to_schedule(struct aws_event_loop *event_loop, struct aws_linked_list *tasks_to_schedule) {
    struct kqueue_loop *impl = event_loop->impl_data;
    AWS_LOGF_TRACE(AWS_LS_IO_EVENT_LOOP, "id=%p: processing cross-thread tasks", (void *)event_loop);

    while (!aws_linked_list_empty(tasks_to_schedule)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(tasks_to_schedule);
        struct aws_task *task = AWS_CONTAINER_OF(node, struct aws_task, node);

        AWS_LOGF_TRACE(
            AWS_LS_IO_EVENT_LOOP,
            "id=%p: task %p pulled to event-loop, scheduling now.",
            (void *)event_loop,
            (void *)task);
        /* Timestamp 0 is used to denote "now" tasks */
        if (task->timestamp == 0) {
            aws_task_scheduler_schedule_now(&impl->thread_data.scheduler, task);
        } else {
            aws_task_scheduler_schedule_future(&impl->thread_data.scheduler, task, task->timestamp);
        }
    }
}

static void s_process_cross_thread_data(struct aws_event_loop *event_loop) {
    struct kqueue_loop *impl = event_loop->impl_data;

    AWS_LOGF_TRACE(AWS_LS_IO_EVENT_LOOP, "id=%p: notified of cross-thread data to process", (void *)event_loop);
    /* If there are tasks to schedule, grab them all out of synced_data.tasks_to_schedule.
     * We'll process them later, so that we minimize time spent holding the mutex. */
    struct aws_linked_list tasks_to_schedule;
    aws_linked_list_init(&tasks_to_schedule);

    { /* Begin critical section */
        aws_mutex_lock(&impl->cross_thread_data.mutex);
        impl->cross_thread_data.thread_signaled = false;

        bool initiate_stop = (impl->cross_thread_data.state == EVENT_THREAD_STATE_STOPPING) &&
                             (impl->thread_data.state == EVENT_THREAD_STATE_RUNNING);
        if (AWS_UNLIKELY(initiate_stop)) {
            impl->thread_data.state = EVENT_THREAD_STATE_STOPPING;
        }

        aws_linked_list_swap_contents(&impl->cross_thread_data.tasks_to_schedule, &tasks_to_schedule);

        aws_mutex_unlock(&impl->cross_thread_data.mutex);
    } /* End critical section */

    s_process_tasks_to_schedule(event_loop, &tasks_to_schedule);
}

static int s_aws_event_flags_from_kevent(struct kevent *kevent) {
    int event_flags = 0;

    if (kevent->flags & EV_ERROR) {
        event_flags |= AWS_IO_EVENT_TYPE_ERROR;
    } else if (kevent->filter == EVFILT_READ) {
        if (kevent->data != 0) {
            event_flags |= AWS_IO_EVENT_TYPE_READABLE;
        }

        if (kevent->flags & EV_EOF) {
            event_flags |= AWS_IO_EVENT_TYPE_CLOSED;
        }
    } else if (kevent->filter == EVFILT_WRITE) {
        if (kevent->data != 0) {
            event_flags |= AWS_IO_EVENT_TYPE_WRITABLE;
        }

        if (kevent->flags & EV_EOF) {
            event_flags |= AWS_IO_EVENT_TYPE_CLOSED;
        }
    }

    return event_flags;
}

/**
 * This just calls kevent()
 *
 * We broke this out into its own function so that the stacktrace clearly shows
 * what this thread is doing. We've had a lot of cases where users think this
 * thread is deadlocked because it's stuck here. We want it to be clear
 * that it's doing nothing on purpose. It's waiting for events to happen...
 */
AWS_NO_INLINE
static int aws_event_loop_listen_for_io_events(int kq_fd, struct kevent kevents[MAX_EVENTS], struct timespec *timeout) {
    return kevent(kq_fd, NULL /*changelist*/, 0 /*nchanges*/, kevents /*eventlist*/, MAX_EVENTS /*nevents*/, timeout);
}

static void aws_event_loop_thread(void *user_data) {
    struct aws_event_loop *event_loop = user_data;
    AWS_LOGF_INFO(AWS_LS_IO_EVENT_LOOP, "id=%p: main loop started", (void *)event_loop);
    struct kqueue_loop *impl = event_loop->impl_data;

    /* set thread id to the event-loop's thread. */
    aws_atomic_store_ptr(&impl->running_thread_id, &impl->thread_created_on.thread_id);

    AWS_ASSERT(impl->thread_data.state == EVENT_THREAD_STATE_READY_TO_RUN);
    impl->thread_data.state = EVENT_THREAD_STATE_RUNNING;

    struct kevent kevents[MAX_EVENTS];

    /* A single aws_io_handle could have two separate kevents if subscribed for both read and write.
     * If both the read and write kevents fire in the same loop of the event-thread,
     * combine the event-flags and deliver them in a single callback.
     * This makes the kqueue_event_loop behave more like the other platform implementations. */
    struct handle_data *io_handle_events[MAX_EVENTS];

    struct timespec timeout = {
        .tv_sec = DEFAULT_TIMEOUT_SEC,
        .tv_nsec = 0,
    };

    AWS_LOGF_INFO(
        AWS_LS_IO_EVENT_LOOP,
        "id=%p: default timeout %ds, and max events to process per tick %d",
        (void *)event_loop,
        DEFAULT_TIMEOUT_SEC,
        MAX_EVENTS);

    while (impl->thread_data.state == EVENT_THREAD_STATE_RUNNING) {
        int num_io_handle_events = 0;
        bool should_process_cross_thread_data = false;

        AWS_LOGF_TRACE(
            AWS_LS_IO_EVENT_LOOP,
            "id=%p: waiting for a maximum of %ds %lluns",
            (void *)event_loop,
            (int)timeout.tv_sec,
            (unsigned long long)timeout.tv_nsec);

        /* Process kqueue events */
        int num_kevents = aws_event_loop_listen_for_io_events(impl->kq_fd, kevents, &timeout);

        aws_event_loop_register_tick_start(event_loop);
        AWS_LOGF_TRACE(
            AWS_LS_IO_EVENT_LOOP, "id=%p: wake up with %d events to process.", (void *)event_loop, num_kevents);
        if (num_kevents == -1) {
            /* Raise an error, in case this is interesting to anyone monitoring,
             * and continue on with this loop. We can't process events,
             * but we can still process scheduled tasks */
            aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);

            /* Force the cross_thread_data to be processed.
             * There might be valuable info in there, like the message to stop the thread.
             * It's fine to do this even if nothing has changed, it just costs a mutex lock/unlock. */
            should_process_cross_thread_data = true;
        }

        for (int i = 0; i < num_kevents; ++i) {
            struct kevent *kevent = &kevents[i];

            /* Was this event to signal that cross_thread_data has changed? */
            if ((int)kevent->ident == impl->cross_thread_signal_pipe[READ_FD]) {
                should_process_cross_thread_data = true;

                /* Drain whatever data was written to the signaling pipe */
                uint32_t read_whatever;
                while (read((int)kevent->ident, &read_whatever, sizeof(read_whatever)) > 0) {
                }

                continue;
            }

            /* Otherwise this was a normal event on a subscribed handle. Figure out which flags to report. */
            int event_flags = s_aws_event_flags_from_kevent(kevent);
            if (event_flags == 0) {
                continue;
            }

            /* Combine flags, in case multiple kevents correspond to one handle. (see notes at top of function) */
            struct handle_data *handle_data = kevent->udata;
            if (handle_data->events_this_loop == 0) {
                io_handle_events[num_io_handle_events++] = handle_data;
            }
            handle_data->events_this_loop |= event_flags;
        }

        /* Invoke each handle's event callback (unless the handle has been unsubscribed) */
        for (int i = 0; i < num_io_handle_events; ++i) {
            struct handle_data *handle_data = io_handle_events[i];

            if (handle_data->state == HANDLE_STATE_SUBSCRIBED) {
                AWS_LOGF_TRACE(
                    AWS_LS_IO_EVENT_LOOP,
                    "id=%p: activity on fd %d, invoking handler.",
                    (void *)event_loop,
                    handle_data->owner->data.fd);
                handle_data->on_event(
                    event_loop, handle_data->owner, handle_data->events_this_loop, handle_data->on_event_user_data);
            }

            handle_data->events_this_loop = 0;
        }

        /* Process cross_thread_data */
        if (should_process_cross_thread_data) {
            s_process_cross_thread_data(event_loop);
        }

        /* Run scheduled tasks */
        uint64_t now_ns = 0;
        event_loop->clock(&now_ns); /* If clock fails, now_ns will be 0 and tasks scheduled for a specific time
                                       will not be run. That's ok, we'll handle them next time around. */
        AWS_LOGF_TRACE(AWS_LS_IO_EVENT_LOOP, "id=%p: running scheduled tasks.", (void *)event_loop);
        aws_task_scheduler_run_all(&impl->thread_data.scheduler, now_ns);

        /* Set timeout for next kevent() call.
         * If clock fails, or scheduler has no tasks, use default timeout */
        bool use_default_timeout = false;

        int err = event_loop->clock(&now_ns);
        if (err) {
            use_default_timeout = true;
        }

        uint64_t next_run_time_ns;
        if (!aws_task_scheduler_has_tasks(&impl->thread_data.scheduler, &next_run_time_ns)) {

            use_default_timeout = true;
        }

        if (use_default_timeout) {
            AWS_LOGF_TRACE(
                AWS_LS_IO_EVENT_LOOP, "id=%p: no more scheduled tasks using default timeout.", (void *)event_loop);
            timeout.tv_sec = DEFAULT_TIMEOUT_SEC;
            timeout.tv_nsec = 0;
        } else {
            /* Convert from timestamp in nanoseconds, to timeout in seconds with nanosecond remainder */
            uint64_t timeout_ns = next_run_time_ns > now_ns ? next_run_time_ns - now_ns : 0;

            uint64_t timeout_remainder_ns = 0;
            uint64_t timeout_sec =
                aws_timestamp_convert(timeout_ns, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_SECS, &timeout_remainder_ns);

            if (timeout_sec > LONG_MAX) { /* Check for overflow. On Darwin, these values are stored as longs */
                timeout_sec = LONG_MAX;
                timeout_remainder_ns = 0;
            }

            AWS_LOGF_TRACE(
                AWS_LS_IO_EVENT_LOOP,
                "id=%p: detected more scheduled tasks with the next occurring at "
                "%llu using timeout of %ds %lluns.",
                (void *)event_loop,
                (unsigned long long)timeout_ns,
                (int)timeout_sec,
                (unsigned long long)timeout_remainder_ns);
            timeout.tv_sec = (time_t)(timeout_sec);
            timeout.tv_nsec = (long)(timeout_remainder_ns);
        }

        aws_event_loop_register_tick_end(event_loop);
    }

    AWS_LOGF_INFO(AWS_LS_IO_EVENT_LOOP, "id=%p: exiting main loop", (void *)event_loop);
    /* reset to NULL. This should be updated again during destroy before tasks are canceled. */
    aws_atomic_store_ptr(&impl->running_thread_id, NULL);
}
