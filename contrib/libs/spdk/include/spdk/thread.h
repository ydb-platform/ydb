/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/** \file
 * Thread
 */

#ifndef SPDK_THREAD_H_
#define SPDK_THREAD_H_

#include "spdk/stdinc.h"

#include "spdk/cpuset.h"
#include "spdk/queue.h"

#ifdef __cplusplus
extern "C" {
#endif

enum spdk_thread_poller_rc {
	SPDK_POLLER_IDLE,
	SPDK_POLLER_BUSY,
};

/**
 * A stackless, lightweight thread.
 */
struct spdk_thread;

/**
 * A function repeatedly called on the same spdk_thread.
 */
struct spdk_poller;

struct spdk_io_channel_iter;

/**
 * A function that is called each time a new thread is created.
 * The implementor of this function should frequently call
 * spdk_thread_poll() on the thread provided.
 *
 * \param thread The new spdk_thread.
 */
typedef int (*spdk_new_thread_fn)(struct spdk_thread *thread);

/**
 * SPDK thread operation type.
 */
enum spdk_thread_op {
	/* Called each time a new thread is created. The implementor of this operation
	 * should frequently call spdk_thread_poll() on the thread provided.
	 */
	SPDK_THREAD_OP_NEW,

	/* Called when SPDK thread needs to be rescheduled. (e.g., when cpumask of the
	 * SPDK thread is updated.
	 */
	SPDK_THREAD_OP_RESCHED,
};

/**
 * Function to be called for SPDK thread operation.
 */
typedef int (*spdk_thread_op_fn)(struct spdk_thread *thread, enum spdk_thread_op op);

/**
 * Function to check whether the SPDK thread operation is supported.
 */
typedef bool (*spdk_thread_op_supported_fn)(enum spdk_thread_op op);

/**
 * A function that will be called on the target thread.
 *
 * \param ctx Context passed as arg to spdk_thread_pass_msg().
 */
typedef void (*spdk_msg_fn)(void *ctx);

/**
 * Function to be called to pass a message to a thread.
 *
 * \param fn Callback function for a thread.
 * \param ctx Context passed to fn.
 * \param thread_ctx Context for the thread.
 */
typedef void (*spdk_thread_pass_msg)(spdk_msg_fn fn, void *ctx,
				     void *thread_ctx);

/**
 * Callback function for a poller.
 *
 * \param ctx Context passed as arg to spdk_poller_register().
 * \return 0 to indicate that polling took place but no events were found;
 * positive to indicate that polling took place and some events were processed;
 * negative if the poller does not provide spin-wait information.
 */
typedef int (*spdk_poller_fn)(void *ctx);

/**
 * Function to be called to start a poller for the thread.
 *
 * \param thread_ctx Context for the thread.
 * \param fn Callback function for a poller.
 * \param arg Argument passed to callback.
 * \param period_microseconds Polling period in microseconds.
 *
 * \return a pointer to the poller on success, or NULL on failure.
 */
typedef struct spdk_poller *(*spdk_start_poller)(void *thread_ctx,
		spdk_poller_fn fn,
		void *arg,
		uint64_t period_microseconds);

/**
 * Function to be called to stop a poller.
 *
 * \param poller Poller to stop.
 * \param thread_ctx Context for the thread.
 */
typedef void (*spdk_stop_poller)(struct spdk_poller *poller, void *thread_ctx);

/**
 * Callback function to set poller into interrupt mode or back to poll mode.
 *
 * \param poller Poller to set interrupt or poll mode.
 * \param cb_arg Argument passed to the callback function.
 * \param interrupt_mode Set interrupt mode for true, or poll mode for false
 */
typedef void (*spdk_poller_set_interrupt_mode_cb)(struct spdk_poller *poller, void *cb_arg,
		bool interrupt_mode);

/**
 * Mark that the poller is capable of entering interrupt mode.
 *
 * When registering the poller set interrupt callback, the callback will get
 * executed immediately if its spdk_thread is in the interrupt mode.
 *
 * \param poller The poller to register callback function.
 * \param cb_fn Callback function called when the poller must transition into or out of interrupt mode
 * \param cb_arg Argument passed to the callback function.
 */
void spdk_poller_register_interrupt(struct spdk_poller *poller,
				    spdk_poller_set_interrupt_mode_cb cb_fn,
				    void *cb_arg);

/**
 * I/O channel creation callback.
 *
 * \param io_device I/O device associated with this channel.
 * \param ctx_buf Context for the I/O device.
 */
typedef int (*spdk_io_channel_create_cb)(void *io_device, void *ctx_buf);

/**
 * I/O channel destruction callback.
 *
 * \param io_device I/O device associated with this channel.
 * \param ctx_buf Context for the I/O device.
 */
typedef void (*spdk_io_channel_destroy_cb)(void *io_device, void *ctx_buf);

/**
 * I/O device unregister callback.
 *
 * \param io_device Unregistered I/O device.
 */
typedef void (*spdk_io_device_unregister_cb)(void *io_device);

/**
 * Called on the appropriate thread for each channel associated with io_device.
 *
 * \param i I/O channel iterator.
 */
typedef void (*spdk_channel_msg)(struct spdk_io_channel_iter *i);

/**
 * spdk_for_each_channel() callback.
 *
 * \param i I/O channel iterator.
 * \param status 0 if it completed successfully, or negative errno if it failed.
 */
typedef void (*spdk_channel_for_each_cpl)(struct spdk_io_channel_iter *i, int status);

/**
 * \brief Represents a per-thread channel for accessing an I/O device.
 *
 * An I/O device may be a physical entity (i.e. NVMe controller) or a software
 *  entity (i.e. a blobstore).
 *
 * This structure is not part of the API - all accesses should be done through
 *  spdk_io_channel function calls.
 */
struct spdk_io_channel {
	struct spdk_thread		*thread;
	struct io_device		*dev;
	uint32_t			ref;
	uint32_t			destroy_ref;
	TAILQ_ENTRY(spdk_io_channel)	tailq;
	spdk_io_channel_destroy_cb	destroy_cb;

	/*
	 * Modules will allocate extra memory off the end of this structure
	 *  to store references to hardware-specific references (i.e. NVMe queue
	 *  pairs, or references to child device spdk_io_channels (i.e.
	 *  virtual bdevs).
	 */
};

/**
 * Initialize the threading library. Must be called once prior to allocating any threads.
 *
 * \param new_thread_fn Called each time a new SPDK thread is created. The implementor
 * is expected to frequently call spdk_thread_poll() on the provided thread.
 * \param ctx_sz For each thread allocated, an additional region of memory of
 * size ctx_size will also be allocated, for use by the thread scheduler. A pointer
 * to this region may be obtained by calling spdk_thread_get_ctx().
 *
 * \return 0 on success. Negated errno on failure.
 */
int spdk_thread_lib_init(spdk_new_thread_fn new_thread_fn, size_t ctx_sz);

/**
 * Initialize the threading library. Must be called once prior to allocating any threads
 *
 * Both thread_op_fn and thread_op_type_supported_fn have to be specified or not
 * specified together.
 *
 * \param thread_op_fn Called for SPDK thread operation.
 * \param thread_op_supported_fn Called to check whether the SPDK thread operation is supported.
 * \param ctx_sz For each thread allocated, for use by the thread scheduler. A pointer
 * to this region may be obtained by calling spdk_thread_get_ctx().
 *
 * \return 0 on success. Negated errno on failure.
 */
int spdk_thread_lib_init_ext(spdk_thread_op_fn thread_op_fn,
			     spdk_thread_op_supported_fn thread_op_supported_fn,
			     size_t ctx_sz);

/**
 * Release all resources associated with this library.
 */
void spdk_thread_lib_fini(void);

/**
 * Creates a new SPDK thread object.
 *
 * \param name Human-readable name for the thread; can be retrieved with spdk_thread_get_name().
 * The string is copied, so the pointed-to data only needs to be valid during the
 * spdk_thread_create() call. May be NULL to specify no name.
 * \param cpumask Optional mask of CPU cores on which to schedule this thread. This is only
 * a suggestion to the scheduler. The value is copied, so cpumask may be released when
 * this function returns. May be NULL if no mask is required.
 *
 * \return a pointer to the allocated thread on success or NULL on failure..
 */
struct spdk_thread *spdk_thread_create(const char *name, struct spdk_cpuset *cpumask);

/**
 * Force the current system thread to act as if executing the given SPDK thread.
 *
 * \param thread The thread to set.
 */
void spdk_set_thread(struct spdk_thread *thread);

/**
 * Mark the thread as exited, failing all future spdk_thread_send_msg(),
 * spdk_poller_register(), and spdk_get_io_channel() calls. May only be called
 * within an spdk poller or message.
 *
 * All I/O channel references associated with the thread must be released
 * using spdk_put_io_channel(), and all active pollers associated with the thread
 * should be unregistered using spdk_poller_unregister(), prior to calling
 * this function. This function will complete these processing. The completion can
 * be queried by spdk_thread_is_exited().
 *
 * \param thread The thread to destroy.
 *
 * \return always 0. (return value was deprecated but keep it for ABI compatibility.)
 */
int spdk_thread_exit(struct spdk_thread *thread);

/**
 * Returns whether the thread is marked as exited.
 *
 * \param thread The thread to query.
 *
 * \return true if marked as exited, false otherwise.
 */
bool spdk_thread_is_exited(struct spdk_thread *thread);

/**
 * Destroy a thread, releasing all of its resources. May only be called
 * on a thread previously marked as exited.
 *
 * \param thread The thread to destroy.
 *
 */
void spdk_thread_destroy(struct spdk_thread *thread);

/**
 * Return a pointer to this thread's context.
 *
 * \param thread The thread on which to get the context.
 *
 * \return a pointer to the per-thread context, or NULL if there is
 * no per-thread context.
 */
void *spdk_thread_get_ctx(struct spdk_thread *thread);

/**
 * Get the thread's cpumask.
 *
 * \param thread The thread to get the cpumask for.
 *
 * \return cpuset pointer
 */
struct spdk_cpuset *spdk_thread_get_cpumask(struct spdk_thread *thread);

/**
 * Set the current thread's cpumask to the specified value. The thread may be
 * rescheduled to one of the CPUs specified in the cpumask.
 *
 * This API requires SPDK thread operation supports SPDK_THREAD_OP_RESCHED.
 *
 * \param cpumask The new cpumask for the thread.
 *
 * \return 0 on success, negated errno otherwise.
 */
int spdk_thread_set_cpumask(struct spdk_cpuset *cpumask);

/**
 * Return the thread object associated with the context handle previously
 * obtained by calling spdk_thread_get_ctx().
 *
 * \param ctx A context previously obtained by calling spdk_thread_get_ctx()
 *
 * \return The associated thread.
 */
struct spdk_thread *spdk_thread_get_from_ctx(void *ctx);

/**
 * Perform one iteration worth of processing on the thread. This includes
 * both expired and continuous pollers as well as messages. If the thread
 * has exited, return immediately.
 *
 * \param thread The thread to process
 * \param max_msgs The maximum number of messages that will be processed.
 *                 Use 0 to process the default number of messages (8).
 * \param now The current time, in ticks. Optional. If 0 is passed, this
 *            function will call spdk_get_ticks() to get the current time.
 *            The current time is used as start time and this function
 *            will call spdk_get_ticks() at its end to know end time to
 *            measure run time of this function.
 *
 * \return 1 if work was done. 0 if no work was done.
 */
int spdk_thread_poll(struct spdk_thread *thread, uint32_t max_msgs, uint64_t now);

/**
 * Return the number of ticks until the next timed poller
 * would expire. Timed pollers are pollers for which
 * period_microseconds is greater than 0.
 *
 * \param thread The thread to check poller expiration times on
 *
 * \return Number of ticks. If no timed pollers, return 0.
 */
uint64_t spdk_thread_next_poller_expiration(struct spdk_thread *thread);

/**
 * Returns whether there are any active pollers (pollers for which
 * period_microseconds equals 0) registered to be run on the thread.
 *
 * \param thread The thread to check.
 *
 * \return 1 if there is at least one active poller, 0 otherwise.
 */
int spdk_thread_has_active_pollers(struct spdk_thread *thread);

/**
 * Returns whether there are any pollers registered to be run
 * on the thread.
 *
 * \param thread The thread to check.
 *
 * \return true if there is any active poller, false otherwise.
 */
bool spdk_thread_has_pollers(struct spdk_thread *thread);

/**
 * Returns whether there are scheduled operations to be run on the thread.
 *
 * \param thread The thread to check.
 *
 * \return true if there are no scheduled operations, false otherwise.
 */
bool spdk_thread_is_idle(struct spdk_thread *thread);

/**
 * Get count of allocated threads.
 */
uint32_t spdk_thread_get_count(void);

/**
 * Get a handle to the current thread.
 *
 * This handle may be passed to other threads and used as the target of
 * spdk_thread_send_msg().
 *
 * \sa spdk_io_channel_get_thread()
 *
 * \return a pointer to the current thread on success or NULL on failure.
 */
struct spdk_thread *spdk_get_thread(void);

/**
 * Get a thread's name.
 *
 * \param thread Thread to query.
 *
 * \return the name of the thread.
 */
const char *spdk_thread_get_name(const struct spdk_thread *thread);

/**
 * Get a thread's ID.
 *
 * \param thread Thread to query.
 *
 * \return the ID of the thread..
 */
uint64_t spdk_thread_get_id(const struct spdk_thread *thread);

/**
 * Get the thread by the ID.
 *
 * \param id ID of the thread.
 * \return Thread whose ID matches or NULL otherwise.
 */
struct spdk_thread *spdk_thread_get_by_id(uint64_t id);

struct spdk_thread_stats {
	uint64_t busy_tsc;
	uint64_t idle_tsc;
};

/**
 * Get statistics about the current thread.
 *
 * Copy cumulative thread stats values to the provided thread stats structure.
 *
 * \param stats User's thread_stats structure.
 */
int spdk_thread_get_stats(struct spdk_thread_stats *stats);

/**
 * Return the TSC value from the end of the last time this thread was polled.
 *
 * \param thread Thread to query.
 *
 * \return TSC value from the end of the last time this thread was polled.
 */
uint64_t spdk_thread_get_last_tsc(struct spdk_thread *thread);

/**
 * Send a message to the given thread.
 *
 * The message will be sent asynchronously - i.e. spdk_thread_send_msg will always return
 * prior to `fn` being called.
 *
 * \param thread The target thread.
 * \param fn This function will be called on the given thread.
 * \param ctx This context will be passed to fn when called.
 *
 * \return 0 on success
 * \return -ENOMEM if the message could not be allocated
 * \return -EIO if the message could not be sent to the destination thread
 */
int spdk_thread_send_msg(const struct spdk_thread *thread, spdk_msg_fn fn, void *ctx);

/**
 * Send a message to the given thread. Only one critical message can be outstanding at the same
 * time. It's intended to use this function in any cases that might interrupt the execution of the
 * application, such as signal handlers.
 *
 * The message will be sent asynchronously - i.e. spdk_thread_send_critical_msg will always return
 * prior to `fn` being called.
 *
 * \param thread The target thread.
 * \param fn This function will be called on the given thread.
 *
 * \return 0 on success
 * \return -EIO if the message could not be sent to the destination thread, due to an already
 * outstanding critical message
 */
int spdk_thread_send_critical_msg(struct spdk_thread *thread, spdk_msg_fn fn);

/**
 * Send a message to each thread, serially.
 *
 * The message is sent asynchronously - i.e. spdk_for_each_thread will return
 * prior to `fn` being called on each thread.
 *
 * \param fn This is the function that will be called on each thread.
 * \param ctx This context will be passed to fn when called.
 * \param cpl This will be called on the originating thread after `fn` has been
 * called on each thread.
 */
void spdk_for_each_thread(spdk_msg_fn fn, void *ctx, spdk_msg_fn cpl);

/**
 * Set current spdk_thread into interrupt mode or back to poll mode.
 *
 * Only valid when thread interrupt facility is enabled by
 * spdk_interrupt_mode_enable().
 *
 * \param enable_interrupt Set interrupt mode for true, or poll mode for false
 */
void spdk_thread_set_interrupt_mode(bool enable_interrupt);

/**
 * Register a poller on the current thread.
 *
 * The poller can be unregistered by calling spdk_poller_unregister().
 *
 * \param fn This function will be called every `period_microseconds`.
 * \param arg Argument passed to fn.
 * \param period_microseconds How often to call `fn`. If 0, call `fn` as often
 *  as possible.
 *
 * \return a pointer to the poller registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_poller *spdk_poller_register(spdk_poller_fn fn,
		void *arg,
		uint64_t period_microseconds);

/**
 * Register a poller on the current thread with arbitrary name.
 *
 * The poller can be unregistered by calling spdk_poller_unregister().
 *
 * \param fn This function will be called every `period_microseconds`.
 * \param arg Argument passed to fn.
 * \param period_microseconds How often to call `fn`. If 0, call `fn` as often
 *  as possible.
 * \param name Human readable name for the poller. Pointer of the poller function
 * name is set if NULL.
 *
 * \return a pointer to the poller registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_poller *spdk_poller_register_named(spdk_poller_fn fn,
		void *arg,
		uint64_t period_microseconds,
		const char *name);

/*
 * \brief Register a poller on the current thread with setting its name
 * to the string of the poller function name.
 */
#define SPDK_POLLER_REGISTER(fn, arg, period_microseconds)	\
	spdk_poller_register_named(fn, arg, period_microseconds, #fn)

/**
 * Unregister a poller on the current thread.
 *
 * \param ppoller The poller to unregister.
 */
void spdk_poller_unregister(struct spdk_poller **ppoller);

/**
 * Pause a poller on the current thread.
 *
 * The poller is not run until it is resumed with spdk_poller_resume().  It is
 * perfectly fine to pause an already paused poller.
 *
 * \param poller The poller to pause.
 */
void spdk_poller_pause(struct spdk_poller *poller);

/**
 * Resume a poller on the current thread.
 *
 * Resumes a poller paused with spdk_poller_pause().  It is perfectly fine to
 * resume an unpaused poller.
 *
 * \param poller The poller to resume.
 */
void spdk_poller_resume(struct spdk_poller *poller);

/**
 * Register the opaque io_device context as an I/O device.
 *
 * After an I/O device is registered, it can return I/O channels using the
 * spdk_get_io_channel() function.
 *
 * \param io_device The pointer to io_device context.
 * \param create_cb Callback function invoked to allocate any resources required
 * for a new I/O channel.
 * \param destroy_cb Callback function invoked to release the resources for an
 * I/O channel.
 * \param ctx_size The size of the context buffer allocated to store references
 * to allocated I/O channel resources.
 * \param name A string name for the device used only for debugging. Optional -
 * may be NULL.
 */
void spdk_io_device_register(void *io_device, spdk_io_channel_create_cb create_cb,
			     spdk_io_channel_destroy_cb destroy_cb, uint32_t ctx_size,
			     const char *name);

/**
 * Unregister the opaque io_device context as an I/O device.
 *
 * The actual unregistration might be deferred until all active I/O channels are
 * destroyed.
 *
 * \param io_device The pointer to io_device context.
 * \param unregister_cb An optional callback function invoked to release any
 * references to this I/O device.
 */
void spdk_io_device_unregister(void *io_device, spdk_io_device_unregister_cb unregister_cb);

/**
 * Get an I/O channel for the specified io_device to be used by the calling thread.
 *
 * The io_device context pointer specified must have previously been registered
 * using spdk_io_device_register(). If an existing I/O channel does not exist
 * yet for the given io_device on the calling thread, it will allocate an I/O
 * channel and invoke the create_cb function pointer specified in spdk_io_device_register().
 * If an I/O channel already exists for the given io_device on the calling thread,
 * its reference is returned rather than creating a new I/O channel.
 *
 * \param io_device The pointer to io_device context.
 *
 * \return a pointer to the I/O channel for this device on success or NULL on failure.
 */
struct spdk_io_channel *spdk_get_io_channel(void *io_device);

/**
 * Release a reference to an I/O channel. This happens asynchronously.
 *
 * This must be called on the same thread that called spdk_get_io_channel()
 * for the specified I/O channel. If this releases the last reference to the
 * I/O channel, The destroy_cb function specified in spdk_io_device_register()
 * will be invoked to release any associated resources.
 *
 * \param ch I/O channel to release a reference.
 */
void spdk_put_io_channel(struct spdk_io_channel *ch);

/**
 * Get the context buffer associated with an I/O channel.
 *
 * \param ch I/O channel.
 *
 * \return a pointer to the context buffer.
 */
static inline void *
spdk_io_channel_get_ctx(struct spdk_io_channel *ch)
{
	return (uint8_t *)ch + sizeof(*ch);
}

/**
 * Get I/O channel from the context buffer. This is the inverse of
 * spdk_io_channel_get_ctx().
 *
 * \param ctx The pointer to the context buffer.
 *
 * \return a pointer to the I/O channel associated with the context buffer.
 */
struct spdk_io_channel *spdk_io_channel_from_ctx(void *ctx);

/**
 * Get the thread associated with an I/O channel.
 *
 * \param ch I/O channel.
 *
 * \return a pointer to the thread associated with the I/O channel
 */
struct spdk_thread *spdk_io_channel_get_thread(struct spdk_io_channel *ch);

/**
 * Call 'fn' on each channel associated with io_device.
 *
 * This happens asynchronously, so fn may be called after spdk_for_each_channel
 * returns. 'fn' will be called for each channel serially, such that two calls
 * to 'fn' will not overlap in time. After 'fn' has been called, call
 * spdk_for_each_channel_continue() to continue iterating.
 *
 * \param io_device 'fn' will be called on each channel associated with this io_device.
 * \param fn Called on the appropriate thread for each channel associated with io_device.
 * \param ctx Context buffer registered to spdk_io_channel_iter that can be obatined
 * form the function spdk_io_channel_iter_get_ctx().
 * \param cpl Called on the thread that spdk_for_each_channel was initially called
 * from when 'fn' has been called on each channel.
 */
void spdk_for_each_channel(void *io_device, spdk_channel_msg fn, void *ctx,
			   spdk_channel_for_each_cpl cpl);

/**
 * Get io_device from the I/O channel iterator.
 *
 * \param i I/O channel iterator.
 *
 * \return a pointer to the io_device.
 */
void *spdk_io_channel_iter_get_io_device(struct spdk_io_channel_iter *i);

/**
 * Get I/O channel from the I/O channel iterator.
 *
 * \param i I/O channel iterator.
 *
 * \return a pointer to the I/O channel.
 */
struct spdk_io_channel *spdk_io_channel_iter_get_channel(struct spdk_io_channel_iter *i);

/**
 * Get context buffer from the I/O channel iterator.
 *
 * \param i I/O channel iterator.
 *
 * \return a pointer to the context buffer.
 */
void *spdk_io_channel_iter_get_ctx(struct spdk_io_channel_iter *i);

/**
 * Get the io_device for the specified I/O channel.
 *
 * \param ch I/O channel.
 *
 * \return a pointer to the io_device for the I/O channel
 */
void *spdk_io_channel_get_io_device(struct spdk_io_channel *ch);

/**
 * Helper function to iterate all channels for spdk_for_each_channel().
 *
 * \param i I/O channel iterator.
 * \param status Status for the I/O channel iterator.
 */
void spdk_for_each_channel_continue(struct spdk_io_channel_iter *i, int status);

/**
 * A representative for registered interrupt file descriptor.
 */
struct spdk_interrupt;

/**
 * Callback function registered for interrupt file descriptor.
 *
 * \param ctx Context passed as arg to spdk_interrupt_register().
 *
 * \return 0 to indicate that interrupt took place but no events were found;
 * positive to indicate that interrupt took place and some events were processed;
 * negative if no event information is provided.
 */
typedef int (*spdk_interrupt_fn)(void *ctx);

/**
 * Register an spdk_interrupt on the current thread. The provided function
 * will be called any time the associated file descriptor is written to.
 *
 * \param efd File descriptor of the spdk_interrupt.
 * \param fn Called each time there are events in spdk_interrupt.
 * \param arg Function argument for fn.
 * \param name Human readable name for the spdk_interrupt. Pointer of the spdk_interrupt
 * name is set if NULL.
 *
 * \return a pointer to the spdk_interrupt registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_interrupt *spdk_interrupt_register(int efd, spdk_interrupt_fn fn,
		void *arg, const char *name);

/*
 * \brief Register an spdk_interrupt on the current thread with setting its name
 * to the string of the spdk_interrupt function name.
 */
#define SPDK_INTERRUPT_REGISTER(efd, fn, arg)	\
	spdk_interrupt_register(efd, fn, arg, #fn)

/**
 * Unregister an spdk_interrupt on the current thread.
 *
 * \param pintr The spdk_interrupt to unregister.
 */
void spdk_interrupt_unregister(struct spdk_interrupt **pintr);

enum spdk_interrupt_event_types {
	SPDK_INTERRUPT_EVENT_IN = 0x001,
	SPDK_INTERRUPT_EVENT_OUT = 0x004,
	SPDK_INTERRUPT_EVENT_ET = 1u << 31
};

/**
 * Change the event_types associated with the spdk_interrupt on the current thread.
 *
 * \param intr The pointer to the spdk_interrupt registered on the current thread.
 * \param event_types New event_types for the spdk_interrupt.
 *
 * \return 0 if success or -errno if failed.
 */
int spdk_interrupt_set_event_types(struct spdk_interrupt *intr,
				   enum spdk_interrupt_event_types event_types);

/**
 * Return a file descriptor that becomes ready whenever any of the registered
 * interrupt file descriptors are ready
 *
 * \param thread The thread to get.
 *
 * \return The spdk_interrupt fd of thread itself.
 */
int spdk_thread_get_interrupt_fd(struct spdk_thread *thread);

/**
 * Set SPDK run as event driven mode
 *
 * \return 0 on success or -errno on failure
 */
int spdk_interrupt_mode_enable(void);

/**
 * Reports whether interrupt mode is set.
 *
 * \return True if interrupt mode is set, false otherwise.
 */
bool spdk_interrupt_mode_is_enabled(void);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_THREAD_H_ */
