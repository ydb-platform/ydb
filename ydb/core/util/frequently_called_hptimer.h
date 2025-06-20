#pragma once

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/system/yassert.h>

namespace NKikimr {
class TFrequentlyCalledHPTimer {
    public:
        TFrequentlyCalledHPTimer(TDuration threshold, ui32 callsBetweenMeasures = 1024)
            : Threshold(threshold)
            , CallsBetweenMeasures(callsBetweenMeasures)
            , CallCounter(0)
            , Expired(false)
        {
            Y_ABORT_UNLESS(CallsBetweenMeasures > 0);
        }

        bool Check() {
            if (++CallCounter == CallsBetweenMeasures) {
                CallCounter = 0;
                if (!Expired && TDuration::Seconds(Timer.Passed()) > Threshold) {
                    Expired = true;
                }
                return Expired;
            }
            return false;
        }

    private:
        THPTimer Timer;
        const TDuration Threshold;
        const ui32 CallsBetweenMeasures = 1024;
        ui32 CallCounter;
        bool Expired;
    };
}
