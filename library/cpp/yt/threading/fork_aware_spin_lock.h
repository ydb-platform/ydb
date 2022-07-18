#pragma once

#include <util/system/spinlock.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

//! Wraps TSpinLock and additionally acquires a global fork lock (in read mode)
//! preventing concurrent forks from happening.
class TForkAwareSpinLock
{
public:
    TForkAwareSpinLock() = default;
    TForkAwareSpinLock(const TForkAwareSpinLock&) = delete;
    TForkAwareSpinLock& operator =(const TForkAwareSpinLock&) = delete;

    void Acquire() noexcept;
    void Release() noexcept;

    bool IsLocked() const noexcept;

private:
    TSpinLock SpinLock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
