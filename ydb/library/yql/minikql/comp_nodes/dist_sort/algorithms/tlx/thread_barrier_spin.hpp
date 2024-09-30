/*******************************************************************************
 * tlx/thread_barrier_spin.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_THREAD_BARRIER_SPIN_HEADER
#define TLX_THREAD_BARRIER_SPIN_HEADER

#include <tlx/meta/no_operation.hpp>

#include <atomic>
#include <thread>

namespace tlx {

/*!
 * Implements a thread barrier using atomics and a spin lock that can be used to
 * synchronize threads.
 *
 * This ThreadBarrier implementation was a lot faster in tests than
 * ThreadBarrierMutex, but ThreadSanitizer shows data races (probably due to the
 * generation counter).
 */
class ThreadBarrierSpin
{
public:
    /*!
     * Creates a new barrier that waits for n threads.
     */
    explicit ThreadBarrierSpin(size_t thread_count)
        : thread_count_(thread_count - 1) { }

    /*!
     * Waits for n threads to arrive. When they have arrive, execute lambda on
     * the one thread, which arrived last. After lambda, step the generation
     * counter.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    template <typename Lambda = NoOperation<void> >
    void wait(Lambda lambda = Lambda()) {
        // get synchronization generation step counter.
        size_t this_step = step_.load(std::memory_order_acquire);

        if (waiting_.fetch_add(1, std::memory_order_acq_rel) == thread_count_) {
            // we are the last thread to wait() -> reset and increment step.
            waiting_.store(0, std::memory_order_release);
            // step other generation counters.
            lambda();
            // the following statement releases all threads from busy waiting.
            step_.fetch_add(1, std::memory_order_acq_rel);
        }
        else {
            // spin lock awaiting the last thread to increment the step counter.
            while (step_.load(std::memory_order_acquire) == this_step) {
                // busy spinning loop
            }
        }
    }

    /*!
     * Waits for n threads to arrive, yield thread while spinning. When they
     * have arrive, execute lambda on the one thread, which arrived last. After
     * lambda, step the generation counter.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    template <typename Lambda = NoOperation<void> >
    void wait_yield(Lambda lambda = Lambda()) {
        // get synchronization generation step counter.
        size_t this_step = step_.load(std::memory_order_acquire);

        if (waiting_.fetch_add(1, std::memory_order_acq_rel) == thread_count_) {
            // we are the last thread to wait() -> reset and increment step.
            waiting_.store(0, std::memory_order_release);
            // step other generation counters.
            lambda();
            // the following statement releases all threads from busy waiting.
            step_.fetch_add(1, std::memory_order_acq_rel);
        }
        else {
            // spin lock awaiting the last thread to increment the step counter.
            while (step_.load(std::memory_order_acquire) == this_step) {
                std::this_thread::yield();
            }
        }
    }

    //! Return generation step counter
    size_t step() const {
        return step_.load(std::memory_order_acquire);
    }

protected:
    //! number of threads, minus one due to comparison needed in loop
    const size_t thread_count_;

    //! number of threads in spin lock
    std::atomic<size_t> waiting_ { 0 };

    //! barrier synchronization generation
    std::atomic<size_t> step_ { 0 };
};

} // namespace tlx

#endif // !TLX_THREAD_BARRIER_SPIN_HEADER

/******************************************************************************/
