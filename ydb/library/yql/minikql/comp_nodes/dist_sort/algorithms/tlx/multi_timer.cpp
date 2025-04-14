/*******************************************************************************
 * tlx/multi_timer.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/multi_timer.hpp>

#include <iostream>
#include <mutex>

#include <tlx/die/core.hpp>
#include <tlx/logger/core.hpp>
#include <tlx/string/hash_djb2.hpp>

namespace tlx {

static std::mutex s_timer_add_mutex;

/******************************************************************************/
// MultiTimer::Entry

struct MultiTimer::Entry {
    //! hash of name for faster search
    std::uint32_t hash;
    //! reference to original string for comparison
    const char* name;
    //! duration of this timer
    std::chrono::duration<double> duration;
};

/******************************************************************************/
// MultiTimer

MultiTimer::MultiTimer()
    : total_duration_(std::chrono::duration<double>::zero()),
      running_(nullptr),
      running_hash_(0) { }

MultiTimer::MultiTimer(const MultiTimer&) = default;
MultiTimer& MultiTimer::operator = (const MultiTimer&) = default;
MultiTimer::MultiTimer(MultiTimer&&) = default;
MultiTimer& MultiTimer::operator = (MultiTimer&&) = default;

MultiTimer::~MultiTimer() = default;

MultiTimer::Entry& MultiTimer::find_or_create(const char* name) {
    std::uint32_t hash = hash_djb2(name);
    for (size_t i = 0; i < timers_.size(); ++i) {
        if (timers_[i].hash == hash && strcmp(timers_[i].name, name) == 0)
            return timers_[i];
    }
    Entry new_entry;
    new_entry.hash = hash;
    new_entry.name = name;
    new_entry.duration = std::chrono::duration<double>::zero();
    timers_.emplace_back(new_entry);
    return timers_.back();
}

void MultiTimer::start(const char* timer) {
    tlx_die_unless(timer);
    std::uint32_t hash = hash_djb2(timer);
    if (running_ && hash == running_hash_ && strcmp(running_, timer) == 0) {
        static bool warning_shown = false;
        if (!warning_shown) {
            TLX_LOG1 << "MultiTimer: trying to start timer "
                     << timer << " twice!";
            TLX_LOG1 << "MultiTimer: multi-threading is not supported, "
                     << "use .add()";
            warning_shown = true;
        }
    }
    stop();
    running_ = timer;
    running_hash_ = hash;
}

void MultiTimer::stop() {
    auto new_time_point = std::chrono::high_resolution_clock::now();
    if (running_) {
        Entry& e = find_or_create(running_);
        e.duration += new_time_point - time_point_;
        total_duration_ += new_time_point - time_point_;
    }
    time_point_ = new_time_point;
    running_ = nullptr;
    running_hash_ = 0;
}

void MultiTimer::reset() {
    timers_.clear();
    total_duration_ = std::chrono::duration<double>::zero();
}

const char* MultiTimer::running() const {
    return running_;
}

double MultiTimer::get(const char* name) {
    return find_or_create(name).duration.count();
}

double MultiTimer::total() const {
    return total_duration_.count();
}

void MultiTimer::print(const char* info, std::ostream& os) const {
    tlx_die_unless(!running_);

    os << "TIMER info=" << info;
    for (const Entry& timer : timers_) {
        os << ' ' << timer.name << '=' << timer.duration.count();
    }
    os << " total=" << total_duration_.count() << std::endl;
}

void MultiTimer::print(const char* info) const {
    return print(info, std::cerr);
}

MultiTimer& MultiTimer::add(const MultiTimer& b) {
    std::unique_lock<std::mutex> lock(s_timer_add_mutex);
    if (b.running_) {
        TLX_LOG1 << "MultiTimer: trying to add running timer";
    }
    for (const Entry& t : b.timers_) {
        Entry& e = find_or_create(t.name);
        e.duration += t.duration;
    }
    total_duration_ += b.total_duration_;
    return *this;
}

MultiTimer& MultiTimer::operator += (const MultiTimer& b) {
    return add(b);
}

/******************************************************************************/
// ScopedMultiTimerSwitch

ScopedMultiTimerSwitch::ScopedMultiTimerSwitch(
    MultiTimer& timer, const char* new_timer)
    : timer_(timer), previous_(timer.running()) {
    timer_.start(new_timer);
}

ScopedMultiTimerSwitch::~ScopedMultiTimerSwitch() {
    timer_.start(previous_);
}

/******************************************************************************/
// ScopedMultiTimer

ScopedMultiTimer::ScopedMultiTimer(MultiTimer& base, const char* timer)
    : base_(base) {
    timer_.start(timer);
}

ScopedMultiTimer::~ScopedMultiTimer() {
    timer_.stop();
    base_.add(timer_);
}

} // namespace tlx

/******************************************************************************/
