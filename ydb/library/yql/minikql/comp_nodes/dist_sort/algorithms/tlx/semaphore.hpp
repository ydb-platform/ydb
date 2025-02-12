/*******************************************************************************
 * tlx/semaphore.hpp
 *
 * A simple semaphore implementation using C++11 synchronization methods.
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2013-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SEMAPHORE_HEADER
#define TLX_SEMAPHORE_HEADER

#include <condition_variable>
#include <mutex>

namespace tlx {

//! A simple semaphore implementation using C++11 synchronization methods.
class Semaphore
{
public:
    //! construct semaphore
    explicit Semaphore(size_t initial_value = 0)
        : value_(initial_value) { }

    //! non-copyable: delete copy-constructor
    Semaphore(const Semaphore&) = delete;
    //! non-copyable: delete assignment operator
    Semaphore& operator = (const Semaphore&) = delete;
    //! move-constructor: just move the value
    Semaphore(Semaphore&& s) : value_(s.value_) { }
    //! move-assignment: just move the value
    Semaphore& operator = (Semaphore&& s) { value_ = s.value_; return *this; }

    //! function increments the semaphore and signals any threads that are
    //! blocked waiting a change in the semaphore
    size_t signal() {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t res = ++value_;
        cv_.notify_one();
        return res;
    }
    //! function increments the semaphore and signals any threads that are
    //! blocked waiting a change in the semaphore
    size_t signal(size_t delta) {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t res = (value_ += delta);
        cv_.notify_all();
        return res;
    }
    //! function decrements the semaphore by delta and blocks if the semaphore
    //! is < (delta + slack) until another thread signals a change
    size_t wait(size_t delta = 1, size_t slack = 0) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (value_ < delta + slack)
            cv_.wait(lock);
        value_ -= delta;
        return value_;
    }
    //! function decrements the semaphore by delta if (delta + slack) tokens are
    //! available as a batch. the function will not block and returns true if
    //! delta was acquired otherwise false.
    bool try_acquire(size_t delta = 1, size_t slack = 0) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (value_ < delta + slack)
            return false;
        value_ -= delta;
        return true;
    }

    //! return the current value -- should only be used for debugging.
    size_t value() const { return value_; }

private:
    //! value of the semaphore
    size_t value_;

    //! mutex for condition variable
    std::mutex mutex_;

    //! condition variable
    std::condition_variable cv_;
};

//! alias for STL-like code style
using semaphore = Semaphore;

} // namespace tlx

#endif // !TLX_SEMAPHORE_HEADER

/******************************************************************************/
