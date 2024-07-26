#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/core/monotonic.h>

namespace NKikimr::NColumnShard {

class TDurationGuard;

class TDurationController {
private:
    ::NMonitoring::THistogramPtr Count;
    ::NMonitoring::THistogramPtr SumDuration;

    TDurationController(const ::NMonitoring::THistogramPtr hCount, const ::NMonitoring::THistogramPtr hSumDuration)
        : Count(hCount)
        , SumDuration(hSumDuration)
    {

    }

public:
    class TGuard: TNonCopyable {
    private:
        TDurationController& Controller;
        const TMonotonic Start = TMonotonic::Now();
    public:
        TGuard(TDurationController& controller)
            : Controller(controller) {

        }

        ~TGuard() {
            const auto d = TMonotonic::Now() - Start;
            Controller.Count->Collect(d.MilliSeconds());
            Controller.SumDuration->Collect((i64)d.MilliSeconds(), d.MilliSeconds());
        }
    };

    static TDurationController CreateController(const TString& name);
};

}
