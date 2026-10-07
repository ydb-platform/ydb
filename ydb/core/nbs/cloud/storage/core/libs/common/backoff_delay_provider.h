#pragma once

#include <util/datetime/base.h>

namespace NYdb::NBS {

///////////////////////////////////////////////////////////////////////////////

class TBackoffDelayProvider
{
private:
    const TDuration InitialDelay;
    const TDuration MaxDelay;
    const TDuration FirstStepDelay;
    TDuration CurrentDelay;

public:
    TBackoffDelayProvider(TDuration initialDelay, TDuration maxDelay);

    TBackoffDelayProvider(
        TDuration initialDelay,
        TDuration firstStepDelay,
        TDuration maxDelay);

    [[nodiscard]] TDuration GetDelay() const;
    [[nodiscard]] TDuration GetDelayAndIncrease();

    void IncreaseDelay();
    void Reset();
};

///////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS
