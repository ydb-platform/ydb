/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/promise.h>
#include <aws/common/ref_count.h>

struct aws_promise {
    struct aws_allocator *allocator;
    struct aws_mutex mutex;
    struct aws_condition_variable cv;
    struct aws_ref_count rc;
    bool complete;
    int error_code;
    void *value;

    /* destructor for value, will be invoked if the value is not taken */
    void (*dtor)(void *);
};

static void s_aws_promise_dtor(void *ptr) {
    struct aws_promise *promise = ptr;
    aws_condition_variable_clean_up(&promise->cv);
    aws_mutex_clean_up(&promise->mutex);
    if (promise->value && promise->dtor) {
        promise->dtor(promise->value);
    }
    aws_mem_release(promise->allocator, promise);
}

struct aws_promise *aws_promise_new(struct aws_allocator *allocator) {
    struct aws_promise *promise = aws_mem_calloc(allocator, 1, sizeof(struct aws_promise));
    promise->allocator = allocator;
    aws_ref_count_init(&promise->rc, promise, s_aws_promise_dtor);
    aws_mutex_init(&promise->mutex);
    aws_condition_variable_init(&promise->cv);
    return promise;
}

struct aws_promise *aws_promise_acquire(struct aws_promise *promise) {
    aws_ref_count_acquire(&promise->rc);
    return promise;
}

void aws_promise_release(struct aws_promise *promise) {
    aws_ref_count_release(&promise->rc);
}

static bool s_promise_completed(void *user_data) {
    struct aws_promise *promise = user_data;
    return promise->complete;
}

void aws_promise_wait(struct aws_promise *promise) {
    aws_mutex_lock(&promise->mutex);
    aws_condition_variable_wait_pred(&promise->cv, &promise->mutex, s_promise_completed, promise);
    aws_mutex_unlock(&promise->mutex);
}

bool aws_promise_wait_for(struct aws_promise *promise, size_t nanoseconds) {
    aws_mutex_lock(&promise->mutex);
    aws_condition_variable_wait_for_pred(
        &promise->cv, &promise->mutex, (int64_t)nanoseconds, s_promise_completed, promise);
    const bool complete = promise->complete;
    aws_mutex_unlock(&promise->mutex);
    return complete;
}

bool aws_promise_is_complete(struct aws_promise *promise) {
    aws_mutex_lock(&promise->mutex);
    const bool complete = promise->complete;
    aws_mutex_unlock(&promise->mutex);
    return complete;
}

void aws_promise_complete(struct aws_promise *promise, void *value, void (*dtor)(void *)) {
    aws_mutex_lock(&promise->mutex);
    AWS_FATAL_ASSERT(!promise->complete && "aws_promise_complete: cannot complete a promise more than once");
    promise->value = value;
    promise->dtor = dtor;
    promise->complete = true;
    aws_mutex_unlock(&promise->mutex);
    aws_condition_variable_notify_all(&promise->cv);
}

void aws_promise_fail(struct aws_promise *promise, int error_code) {
    AWS_FATAL_ASSERT(error_code != 0 && "aws_promise_fail: cannot fail a promise with a 0 error_code");
    aws_mutex_lock(&promise->mutex);
    AWS_FATAL_ASSERT(!promise->complete && "aws_promise_fail: cannot complete a promise more than once");
    promise->error_code = error_code;
    promise->complete = true;
    aws_mutex_unlock(&promise->mutex);
    aws_condition_variable_notify_all(&promise->cv);
}

int aws_promise_error_code(struct aws_promise *promise) {
    AWS_FATAL_ASSERT(aws_promise_is_complete(promise));
    return promise->error_code;
}

void *aws_promise_value(struct aws_promise *promise) {
    AWS_FATAL_ASSERT(aws_promise_is_complete(promise));
    return promise->value;
}

void *aws_promise_take_value(struct aws_promise *promise) {
    AWS_FATAL_ASSERT(aws_promise_is_complete(promise));
    void *value = promise->value;
    promise->value = NULL;
    promise->dtor = NULL;
    return value;
}
