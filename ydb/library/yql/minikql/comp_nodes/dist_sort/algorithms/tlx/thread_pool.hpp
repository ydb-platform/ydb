/*******************************************************************************
 * tlx/thread_pool.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_THREAD_POOL_HEADER
#define TLX_THREAD_POOL_HEADER

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

#include <tlx/container/simple_vector.hpp>
#include <tlx/delegate.hpp>

namespace tlx {

/*!
 * ThreadPool starts a fixed number p of std::threads which process Jobs that
 * are \ref enqueue "enqueued" into a concurrent job queue. The jobs
 * themselves can enqueue more jobs that will be processed when a thread is
 * ready.
 *
 * The ThreadPool can either run until
 *
 * 1. all jobs are done AND all threads are idle, when called with
 * loop_until_empty(), or
 *
 * 2. until Terminate() is called when run with loop_until_terminate().
 *
 * Jobs are plain tlx::Delegate<void()> objects, hence the pool user must pass
 * in ALL CONTEXT himself. The best method to pass parameters to Jobs is to use
 * lambda captures. Alternatively, old-school objects implementing operator(),
 * or std::binds can be used.
 *
 * The ThreadPool uses a condition variable to wait for new jobs and does not
 * remain busy waiting.
 *
 * Note that the threads in the pool start **before** the two loop functions are
 * called. In case of loop_until_empty() the threads continue to be idle
 * afterwards, and can be reused, until the ThreadPool is destroyed.

\code
ThreadPool pool(4); // pool with 4 threads

int value = 0;
pool.enqueue([&value]() {
  // increment value in another thread.
  ++value;
});

pool.loop_until_empty();
\endcode

 */
class ThreadPool
{
public:
    using Job = Delegate<void ()>;
    using InitThread = Delegate<void (size_t)>;

private:
    //! Deque of scheduled jobs.
    std::deque<Job> jobs_;

    //! Mutex used to access the queue of scheduled jobs.
    std::mutex mutex_;

    //! threads in pool
    simple_vector<std::thread> threads_;

    //! Condition variable used to notify that a new job has been inserted in
    //! the queue.
    std::condition_variable cv_jobs_;
    //! Condition variable to signal when a jobs finishes.
    std::condition_variable cv_finished_;

    //! Counter for number of threads busy.
    std::atomic<size_t> busy_ = { 0 };
    //! Counter for number of idle threads waiting for a job.
    std::atomic<size_t> idle_ = { 0 };
    //! Counter for total number of jobs executed
    std::atomic<size_t> done_ = { 0 };

    //! Flag whether to terminate
    std::atomic<bool> terminate_ = { false };

    //! Run once per worker thread
    InitThread init_thread_;

public:
    //! Construct running thread pool of num_threads
    ThreadPool(
        size_t num_threads = std::thread::hardware_concurrency(),
        InitThread&& init_thread = InitThread());

    //! non-copyable: delete copy-constructor
    ThreadPool(const ThreadPool&) = delete;
    //! non-copyable: delete assignment operator
    ThreadPool& operator = (const ThreadPool&) = delete;

    //! Stop processing jobs, terminate threads.
    ~ThreadPool();

    //! enqueue a Job, the caller must pass in all context using captures.
    void enqueue(Job&& job);

    //! Loop until no more jobs are in the queue AND all threads are idle. When
    //! this occurs, this method exits, however, the threads remain active.
    void loop_until_empty();

    //! Loop until terminate flag was set.
    void loop_until_terminate();

    //! Terminate thread pool gracefully, wait until currently running jobs
    //! finish and then exit. This should be called from within one of the
    //! enqueue jobs or from an outside thread.
    void terminate();

    //! Return number of jobs currently completed.
    size_t done() const;

    //! Return number of threads in pool
    size_t size() const;

    //! return number of idle threads in pool
    size_t idle() const;

    //! true if any thread is idle (= waiting for jobs)
    bool has_idle() const;

    //! Return thread handle to thread i
    std::thread& thread(size_t i);

private:
    //! Worker function, one per thread is started.
    void worker(size_t p);
};

} // namespace tlx

#endif // !TLX_THREAD_POOL_HEADER

/******************************************************************************/
