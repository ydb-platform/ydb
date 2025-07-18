#include "context_switch_aware_periodic_yielder.h"


namespace NYT::NConcurrency {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TContextSwitchAwarePeriodicYielder::TContextSwitchAwarePeriodicYielder(TDuration period)
    : TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Restart(); })
    , YieldThreshold_(period)
{ }

void TContextSwitchAwarePeriodicYielder::Checkpoint(const TLogger& Logger)
{
    if (GetElapsedTime() > YieldThreshold_) {
        YT_LOG_DEBUG("Yielding fiber (SyncTime: %v)",
            GetElapsedTime());

        Yield();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
