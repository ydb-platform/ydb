/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_COMMON_PROMISE_H
#define AWS_COMMON_PROMISE_H

#include <aws/common/common.h>

AWS_PUSH_SANE_WARNING_LEVEL

/*
 * Standard promise interface. Promise can be waited on by multiple threads, and as long as it is
 * ref-counted correctly, will provide the resultant value/error code to all waiters.
 * All promise API calls are internally thread-safe.
 */
struct aws_promise;

AWS_EXTERN_C_BEGIN

/*
 * Creates a new promise
 */
AWS_COMMON_API
struct aws_promise *aws_promise_new(struct aws_allocator *allocator);

/*
 * Indicate a new reference to a promise. At minimum, each new thread making use of the promise should
 * acquire it.
 */
AWS_COMMON_API
struct aws_promise *aws_promise_acquire(struct aws_promise *promise);

/*
 * Releases a reference on the promise. When the refcount hits 0, the promise is cleaned up and freed.
 */
AWS_COMMON_API
void aws_promise_release(struct aws_promise *promise);

/*
 * Waits infinitely for the promise to be completed
 */
AWS_COMMON_API
void aws_promise_wait(struct aws_promise *promise);
/*
 * Waits for the requested time in nanoseconds. Returns true if the promise was completed.
 */
AWS_COMMON_API
bool aws_promise_wait_for(struct aws_promise *promise, size_t nanoseconds);

/*
 * Completes the promise and stores the result along with an optional destructor. If the value
 * is not taken via `aws_promise_take_value`, it will be destroyed when the promise's reference
 * count reaches zero.
 * NOTE: Promise cannot be completed twice
 */
AWS_COMMON_API
void aws_promise_complete(struct aws_promise *promise, void *value, void (*dtor)(void *));

/*
 * Completes the promise and stores the error code
 * NOTE: Promise cannot be completed twice
 */
AWS_COMMON_API
void aws_promise_fail(struct aws_promise *promise, int error_code);

/*
 * Returns whether or not the promise has completed (regardless of success or failure)
 */
AWS_COMMON_API
bool aws_promise_is_complete(struct aws_promise *promise);

/*
 * Returns the error code recorded if the promise failed, or 0 if it succeeded
 * NOTE: It is fatal to attempt to retrieve the error code before the promise is completed
 */
AWS_COMMON_API
int aws_promise_error_code(struct aws_promise *promise);

/*
 * Returns the value provided to the promise if it succeeded, or NULL if none was provided
 * or the promise failed. Check `aws_promise_error_code` to be sure.
 * NOTE: The ownership of the value is retained by the promise.
 * NOTE: It is fatal to attempt to retrieve the value before the promise is completed
 */
AWS_COMMON_API
void *aws_promise_value(struct aws_promise *promise);

/*
 * Returns the value provided to the promise if it succeeded, or NULL if none was provided
 * or the promise failed. Check `aws_promise_error_code` to be sure.
 * NOTE: The promise relinquishes ownership of the value, the caller is now responsible for
 * freeing any resources associated with the value
 * NOTE: It is fatal to attempt to take the value before the promise is completed
 */
AWS_COMMON_API
void *aws_promise_take_value(struct aws_promise *promise);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif // AWS_COMMON_PROMISE_H
