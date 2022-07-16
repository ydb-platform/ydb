#include "fork_aware_spin_lock.h"

#include "at_fork.h"

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

void TForkAwareSpinLock::Acquire() noexcept
{
    GetForkLock()->AcquireReaderForkFriendly();
    SpinLock_.Acquire();
}

void TForkAwareSpinLock::Release() noexcept
{
    SpinLock_.Release();
    GetForkLock()->ReleaseReader();
}

bool TForkAwareSpinLock::IsLocked() noexcept
{
    return SpinLock_.IsLocked();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading

