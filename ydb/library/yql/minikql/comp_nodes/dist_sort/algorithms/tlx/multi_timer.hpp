/*******************************************************************************
 * tlx/multi_timer.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MULTI_TIMER_HEADER
#define TLX_MULTI_TIMER_HEADER

#include <chrono>
#include <cstdint>
#include <ostream>
#include <vector>

namespace tlx {

/*!
 * MultiTimer can be used to measure time usage of different phases in a program
 * or algorithm. It contains multiple named "timers", which can be activated
 * without prior definition. At most one timer is start at any time, which
 * means `start()` will stop any current timer and start a new one.
 *
 * Timers are identified by strings, which are passed as const char*, which MUST
 * remain valid for the lifetime of the MultiTimer. Dynamic strings will not
 * work, the standard way is to use plain string literals. The strings are hash
 * for faster searches.
 *
 * MultiTimer can also be used for multi-threading parallel programs. Each
 * thread must create and keep its own MultiTimer instance, which can then be
 * added together into a global MultiTimer object. The add() method of the
 * global object is internally thread-safe using a global mutex.
 */
class MultiTimer
{
public:
    //! constructor
    MultiTimer();

    //! default copy-constructor
    MultiTimer(const MultiTimer&);
    //! default assignment operator
    MultiTimer& operator = (const MultiTimer&);
    //! move-constructor: default
    MultiTimer(MultiTimer&&);
    //! move-assignment operator: default
    MultiTimer& operator = (MultiTimer&&);

    //! destructor
    ~MultiTimer();

    //! start new timer phase, stop the currently running one.
    void start(const char* timer);

    //! stop the currently running timer.
    void stop();

    //! zero timers.
    void reset();

    //! return name of currently running timer.
    const char * running() const;

    //! return timer duration in seconds of timer.
    double get(const char* timer);
    //! return total duration of all timers.
    double total() const;

    //! print all timers as a TIMER line to os
    void print(const char* info, std::ostream& os) const;
    //! print all timers as a TIMER line to stderr
    void print(const char* info) const;

    //! add all timers from another, internally holds a global mutex lock,
    //! because this is used to add thread values
    MultiTimer& add(const MultiTimer& b);

    //! add all timers from another, internally holds a global mutex lock,
    //! because this is used to add thread values
    MultiTimer& operator += (const MultiTimer& b);

private:
    //! timer entry
    struct Entry;

    //! array of timers
    std::vector<Entry> timers_;

    //! total duration
    std::chrono::duration<double> total_duration_;

    //! currently running timer name
    const char* running_;
    //! hash of running_
    std::uint32_t running_hash_;
    //! start of currently running timer name
    std::chrono::time_point<std::chrono::high_resolution_clock> time_point_;

    //! internal methods to find or create new timer entries
    Entry& find_or_create(const char* name);
};

//! RAII Scoped MultiTimer switcher: switches the timer of a MultiTimer on
//! construction and back to old one on destruction.
class ScopedMultiTimerSwitch
{
public:
    //! construct and timer to switch to
    ScopedMultiTimerSwitch(MultiTimer& timer, const char* new_timer);

    //! change back timer to previous timer.
    ~ScopedMultiTimerSwitch();

protected:
    //! reference to MultiTimer
    MultiTimer& timer_;

    //! previous timer, used to switch back to on destruction
    const char* previous_;
};

//! Independent RAII Scoped MultiTimer: contains a MultiTimer which is started
//! with the given timer, and added to the base MultiTimer on destruction.
class ScopedMultiTimer
{
public:
    //! construct and change timer to tm
    ScopedMultiTimer(MultiTimer& base, const char* timer);

    //! change back timer to previous timer.
    ~ScopedMultiTimer();

protected:
    //! reference to base timer
    MultiTimer& base_;

    //! contained independent timer
    MultiTimer timer_;
};

} // namespace tlx

#endif // !TLX_MULTI_TIMER_HEADER

/******************************************************************************/
