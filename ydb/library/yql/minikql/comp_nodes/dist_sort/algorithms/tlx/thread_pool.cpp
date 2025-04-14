/*******************************************************************************
 * tlx/thread_pool.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/thread_pool.hpp>

#include <iostream>

namespace tlx {

ThreadPool::ThreadPool(size_t num_threads, InitThread&& init_thread)
    : threads_(num_threads),
      init_thread_(std::move(init_thread)) {
    // immediately construct worker threads
    for (size_t i = 0; i < num_threads; ++i)
        threads_[i] = std::thread(&ThreadPool::worker, this, i);
}

ThreadPool::~ThreadPool() {
    std::unique_lock<std::mutex> lock(mutex_);
    // set stop-condition
    terminate_ = true;
    cv_jobs_.notify_all();
    lock.unlock();

    // all threads terminate, then we're done
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i].join();
}

void ThreadPool::enqueue(Job&& job) {
    std::unique_lock<std::mutex> lock(mutex_);
    jobs_.emplace_back(std::move(job));
    cv_jobs_.notify_one();
}

void ThreadPool::loop_until_empty() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_finished_.wait(lock, [this]() { return jobs_.empty() && (busy_ == 0); });
    std::atomic_thread_fence(std::memory_order_seq_cst);
}

void ThreadPool::loop_until_terminate() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_finished_.wait(lock, [this]() { return terminate_ && (busy_ == 0); });
    std::atomic_thread_fence(std::memory_order_seq_cst);
}

void ThreadPool::terminate() {
    std::unique_lock<std::mutex> lock(mutex_);
    // flag termination
    terminate_ = true;
    // wake up all worker threads and let them terminate.
    cv_jobs_.notify_all();
    // notify LoopUntilTerminate in case all threads are idle.
    cv_finished_.notify_one();
}

size_t ThreadPool::done() const {
    return done_;
}

size_t ThreadPool::size() const {
    return threads_.size();
}

size_t ThreadPool::idle() const {
    return idle_;
}

bool ThreadPool::has_idle() const {
    return (idle_.load(std::memory_order_relaxed) != 0);
}

std::thread& ThreadPool::thread(size_t i) {
    assert(i < threads_.size());
    return threads_[i];
}

void ThreadPool::worker(size_t p) {
    if (init_thread_)
        init_thread_(p);

    // lock mutex, it is released during condition waits
    std::unique_lock<std::mutex> lock(mutex_);

    while (true) {
        // wait on condition variable until job arrives, frees lock
        if (!terminate_ && jobs_.empty()) {
            ++idle_;
            cv_jobs_.wait(
                lock, [this]() { return terminate_ || !jobs_.empty(); });
            --idle_;
        }

        if (terminate_)
            break;

        if (!jobs_.empty()) {
            // got work. set busy.
            ++busy_;

            {
                // pull job.
                Job job = std::move(jobs_.front());
                jobs_.pop_front();

                // release lock.
                lock.unlock();

                // execute job.
                try {
                    job();
                }
                catch (std::exception& e) {
                    std::cerr << "EXCEPTION: " << e.what() << std::endl;
                }
                // destroy job by closing scope
            }

            // release memory the Job changed
            std::atomic_thread_fence(std::memory_order_seq_cst);

            ++done_;
            --busy_;

            // relock mutex before signaling condition.
            lock.lock();
            cv_finished_.notify_one();
        }
    }
}

} // namespace tlx

/******************************************************************************/
