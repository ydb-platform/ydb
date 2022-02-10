/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#if !defined(__MACH__)
#    define _GNU_SOURCE
#endif

#include <aws/common/clock.h>
#include <aws/common/logging.h>
#include <aws/common/private/dlloads.h>
#include <aws/common/thread.h>

#include <dlfcn.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <sched.h>
#include <time.h>
#include <unistd.h>

#if defined(__FreeBSD__) || defined(__NETBSD__)
#    include <pthread_np.h>
typedef cpuset_t cpu_set_t;
#endif

static struct aws_thread_options s_default_options = {
    /* this will make sure platform default stack size is used. */
    .stack_size = 0,
    .cpu_id = -1,
};

struct thread_atexit_callback {
    aws_thread_atexit_fn *callback;
    void *user_data;
    struct thread_atexit_callback *next;
};

struct thread_wrapper {
    struct aws_allocator *allocator;
    void (*func)(void *arg);
    void *arg;
    struct thread_atexit_callback *atexit;
    void (*call_once)(void *);
    void *once_arg;
    struct aws_thread *thread;
    bool membind;
};

static AWS_THREAD_LOCAL struct thread_wrapper *tl_wrapper = NULL;

static void *thread_fn(void *arg) {
    struct thread_wrapper wrapper = *(struct thread_wrapper *)arg;
    struct aws_allocator *allocator = wrapper.allocator;
    tl_wrapper = &wrapper;
    if (wrapper.membind && g_set_mempolicy_ptr) {
        AWS_LOGF_INFO(
            AWS_LS_COMMON_THREAD,
            "id=%p: a cpu affinity was specified when launching this thread and set_mempolicy() is available on this "
            "system. Setting the memory policy to MPOL_PREFERRED",
            (void *)tl_wrapper->thread);
        /* if a user set a cpu id in their thread options, we're going to make sure the numa policy honors that
         * and makes sure the numa node of the cpu we launched this thread on is where memory gets allocated. However,
         * we don't want to fail the application if this fails, so make the call, and ignore the result. */
        long resp = g_set_mempolicy_ptr(AWS_MPOL_PREFERRED_ALIAS, NULL, 0);
        if (resp) {
            AWS_LOGF_WARN(
                AWS_LS_COMMON_THREAD,
                "id=%p: call to set_mempolicy() failed with errno %d",
                (void *)wrapper.thread,
                errno);
        }
    }
    wrapper.func(wrapper.arg);

    struct thread_atexit_callback *exit_callback_data = wrapper.atexit;
    aws_mem_release(allocator, arg);

    while (exit_callback_data) {
        aws_thread_atexit_fn *exit_callback = exit_callback_data->callback;
        void *exit_callback_user_data = exit_callback_data->user_data;
        struct thread_atexit_callback *next_exit_callback_data = exit_callback_data->next;

        aws_mem_release(allocator, exit_callback_data);

        exit_callback(exit_callback_user_data);
        exit_callback_data = next_exit_callback_data;
    }
    tl_wrapper = NULL;

    return NULL;
}

const struct aws_thread_options *aws_default_thread_options(void) {
    return &s_default_options;
}

void aws_thread_clean_up(struct aws_thread *thread) {
    if (thread->detach_state == AWS_THREAD_JOINABLE) {
        pthread_detach(thread->thread_id);
    }
}

static void s_call_once(void) {
    tl_wrapper->call_once(tl_wrapper->once_arg);
}

void aws_thread_call_once(aws_thread_once *flag, void (*call_once)(void *), void *user_data) {
    // If this is a non-aws_thread, then gin up a temp thread wrapper
    struct thread_wrapper temp_wrapper;
    if (!tl_wrapper) {
        tl_wrapper = &temp_wrapper;
    }

    tl_wrapper->call_once = call_once;
    tl_wrapper->once_arg = user_data;
    pthread_once(flag, s_call_once);

    if (tl_wrapper == &temp_wrapper) {
        tl_wrapper = NULL;
    }
}

int aws_thread_init(struct aws_thread *thread, struct aws_allocator *allocator) {
    *thread = (struct aws_thread){.allocator = allocator, .detach_state = AWS_THREAD_NOT_CREATED};

    return AWS_OP_SUCCESS;
}

int aws_thread_launch(
    struct aws_thread *thread,
    void (*func)(void *arg),
    void *arg,
    const struct aws_thread_options *options) {

    pthread_attr_t attributes;
    pthread_attr_t *attributes_ptr = NULL;
    int attr_return = 0;
    int allocation_failed = 0;

    if (options) {
        attr_return = pthread_attr_init(&attributes);

        if (attr_return) {
            goto cleanup;
        }

        attributes_ptr = &attributes;

        if (options->stack_size > PTHREAD_STACK_MIN) {
            attr_return = pthread_attr_setstacksize(attributes_ptr, options->stack_size);

            if (attr_return) {
                goto cleanup;
            }
        }

/* AFAIK you can't set thread affinity on apple platforms, and it doesn't really matter since all memory
 * NUMA or not is setup in interleave mode.
 * Thread afinity is also not supported on Android systems, and honestly, if you're running android on a NUMA
 * configuration, you've got bigger problems. */
#if !defined(__MACH__) && !defined(__ANDROID__) && !defined(_musl_)
        if (options->cpu_id >= 0) {
            AWS_LOGF_INFO(
                AWS_LS_COMMON_THREAD,
                "id=%p: cpu affinity of cpu_id %d was specified, attempting to honor the value.",
                (void *)thread,
                options->cpu_id);

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET((uint32_t)options->cpu_id, &cpuset);

            attr_return = pthread_attr_setaffinity_np(attributes_ptr, sizeof(cpuset), &cpuset);

            if (attr_return) {
                AWS_LOGF_ERROR(
                    AWS_LS_COMMON_THREAD,
                    "id=%p: pthread_attr_setaffinity_np() failed with %d.",
                    (void *)thread,
                    errno);
                goto cleanup;
            }
        }
#endif /* !defined(__MACH__) && !defined(__ANDROID__) */
    }

    struct thread_wrapper *wrapper =
        (struct thread_wrapper *)aws_mem_calloc(thread->allocator, 1, sizeof(struct thread_wrapper));

    if (!wrapper) {
        allocation_failed = 1;
        goto cleanup;
    }

    if (options && options->cpu_id >= 0) {
        wrapper->membind = true;
    }

    wrapper->thread = thread;
    wrapper->allocator = thread->allocator;
    wrapper->func = func;
    wrapper->arg = arg;
    attr_return = pthread_create(&thread->thread_id, attributes_ptr, thread_fn, (void *)wrapper);

    if (attr_return) {
        goto cleanup;
    }

    thread->detach_state = AWS_THREAD_JOINABLE;

cleanup:
    if (attributes_ptr) {
        pthread_attr_destroy(attributes_ptr);
    }

    if (attr_return == EINVAL) {
        return aws_raise_error(AWS_ERROR_THREAD_INVALID_SETTINGS);
    }

    if (attr_return == EAGAIN) {
        return aws_raise_error(AWS_ERROR_THREAD_INSUFFICIENT_RESOURCE);
    }

    if (attr_return == EPERM) {
        return aws_raise_error(AWS_ERROR_THREAD_NO_PERMISSIONS);
    }

    if (allocation_failed || attr_return == ENOMEM) {
        return aws_raise_error(AWS_ERROR_OOM);
    }

    return AWS_OP_SUCCESS;
}

aws_thread_id_t aws_thread_get_id(struct aws_thread *thread) {
    return thread->thread_id;
}

enum aws_thread_detach_state aws_thread_get_detach_state(struct aws_thread *thread) {
    return thread->detach_state;
}

int aws_thread_join(struct aws_thread *thread) {
    if (thread->detach_state == AWS_THREAD_JOINABLE) {
        int err_no = pthread_join(thread->thread_id, 0);

        if (err_no) {
            if (err_no == EINVAL) {
                return aws_raise_error(AWS_ERROR_THREAD_NOT_JOINABLE);
            }
            if (err_no == ESRCH) {
                return aws_raise_error(AWS_ERROR_THREAD_NO_SUCH_THREAD_ID);
            }
            if (err_no == EDEADLK) {
                return aws_raise_error(AWS_ERROR_THREAD_DEADLOCK_DETECTED);
            }
        }

        thread->detach_state = AWS_THREAD_JOIN_COMPLETED;
    }

    return AWS_OP_SUCCESS;
}

aws_thread_id_t aws_thread_current_thread_id(void) {
    return pthread_self();
}

bool aws_thread_thread_id_equal(aws_thread_id_t t1, aws_thread_id_t t2) {
    return pthread_equal(t1, t2) != 0;
}

void aws_thread_current_sleep(uint64_t nanos) {
    uint64_t nano = 0;
    time_t seconds = (time_t)aws_timestamp_convert(nanos, AWS_TIMESTAMP_NANOS, AWS_TIMESTAMP_SECS, &nano);

    struct timespec tm = {
        .tv_sec = seconds,
        .tv_nsec = (long)nano,
    };
    struct timespec output;

    nanosleep(&tm, &output);
}

int aws_thread_current_at_exit(aws_thread_atexit_fn *callback, void *user_data) {
    if (!tl_wrapper) {
        return aws_raise_error(AWS_ERROR_THREAD_NOT_JOINABLE);
    }

    struct thread_atexit_callback *cb = aws_mem_calloc(tl_wrapper->allocator, 1, sizeof(struct thread_atexit_callback));
    if (!cb) {
        return AWS_OP_ERR;
    }
    cb->callback = callback;
    cb->user_data = user_data;
    cb->next = tl_wrapper->atexit;
    tl_wrapper->atexit = cb;
    return AWS_OP_SUCCESS;
}
