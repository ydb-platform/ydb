#ifndef THREAD_INL_H_
#error "Direct inclusion of this file is not allowed, include thread.h"
// For the sake of sane code completion.
#include "thread.h"
#endif
#undef THREAD_INL_H_

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

inline bool TThread::Start()
{
    if (Y_LIKELY(Started_.load(std::memory_order::relaxed))) {
        if (Y_UNLIKELY(Stopping_.load(std::memory_order::relaxed))) {
            return false;
        }
        return true;
    }

    return StartSlow();
}

inline bool TThread::IsStarted() const
{
    return Started_.load(std::memory_order::relaxed);
}

inline bool TThread::IsStopping() const
{
    return Stopping_.load(std::memory_order::acquire);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
