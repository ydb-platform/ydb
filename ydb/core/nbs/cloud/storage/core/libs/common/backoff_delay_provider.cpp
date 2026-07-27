#include "backoff_delay_provider.h"

namespace NYdb::NBS {

namespace {

///////////////////////////////////////////////////////////////////////////////

constexpr auto DefaultInitialDelay = TDuration::Seconds(1);

///////////////////////////////////////////////////////////////////////////////

TDuration CreateFirstStepDelay(TDuration initialDelay, TDuration maxDelay)
{
    return initialDelay ? initialDelay : Min(maxDelay, DefaultInitialDelay);
}

///////////////////////////////////////////////////////////////////////////////

}   // namespace

TBackoffDelayProvider::TBackoffDelayProvider(
    TDuration initialDelay,
    TDuration maxDelay)
    : InitialDelay(initialDelay)
    , MaxDelay(Max(maxDelay, initialDelay))
    , FirstStepDelay(CreateFirstStepDelay(initialDelay, maxDelay))
    , CurrentDelay(initialDelay)
{}

TBackoffDelayProvider::TBackoffDelayProvider(
    TDuration initialDelay,
    TDuration firstStepDelay,
    TDuration maxDelay)
    : InitialDelay(initialDelay)
    , MaxDelay(Max(maxDelay, initialDelay, firstStepDelay))
    , FirstStepDelay(firstStepDelay)
    , CurrentDelay(initialDelay)
{}

TDuration TBackoffDelayProvider::GetDelay() const
{
    return CurrentDelay;
}

TDuration TBackoffDelayProvider::GetDelayAndIncrease()
{
    auto result = GetDelay();
    IncreaseDelay();
    return result;
}

void TBackoffDelayProvider::IncreaseDelay()
{
    if (!CurrentDelay) {
        CurrentDelay = FirstStepDelay;
    } else {
        CurrentDelay = Min(CurrentDelay * 2, MaxDelay);
    }
}

void TBackoffDelayProvider::Reset()
{
    CurrentDelay = InitialDelay;
}

}   // namespace NYdb::NBS
