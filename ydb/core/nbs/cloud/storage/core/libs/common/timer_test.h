#pragma once

#include "timer.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/vector.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TTestTimer final: public ITimer
{
private:
    TAtomic Timestamp = 0;
    TVector<TDuration> SleepDurations;

public:
    TInstant Now() override;
    void Sleep(TDuration duration) override;

    void AdvanceTime(TDuration delay);
    [[nodiscard]] const TVector<TDuration>& GetSleepDurations() const;
};

}   // namespace NYdb::NBS
