#pragma once
#if defined(OS_LINUX)
#include <CHDBPoco/Timespan.h>

namespace DB_CHDB
{

/// Wrapper over timerfd.
class TimerDescriptor
{
private:
    int timer_fd;

public:
    TimerDescriptor();
    ~TimerDescriptor();

    TimerDescriptor(const TimerDescriptor &) = delete;
    TimerDescriptor & operator=(const TimerDescriptor &) = delete;
    TimerDescriptor(TimerDescriptor && other) noexcept;
    TimerDescriptor & operator=(TimerDescriptor &&) noexcept;

    int getDescriptor() const { return timer_fd; }

    void reset() const;
    void drain() const;
    void setRelative(uint64_t usec) const;
    void setRelative(CHDBPoco::Timespan timespan) const;
};

}
#endif
