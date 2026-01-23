#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/lwtrace/shuttle.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

enum class EProcessingStage
{
    Postponed,
    Backoff,
    Last
};

struct TRequestTime
{
    TDuration TotalTime;
    TDuration ExecutionTime;

    explicit operator bool() const
    {
        return TotalTime || ExecutionTime;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCallContextBase
    : public TThrRefBase
{
private:
    TAtomic Stage2Time[static_cast<int>(EProcessingStage::Last)] = {};
    TAtomic RequestStartedCycles = 0;
    TAtomic ResponseSentCycles = 0;
    TAtomic PossiblePostponeMicroSeconds = 0;
    TAtomic PostponeTsCycles = 0;

    // Used only in tablet throttler.
    TInstant PostponeTs = TInstant::Zero();

public:
    ui64 RequestId;
    NLWTrace::TOrbit LWOrbit;

    TCallContextBase(ui64 requestId);

    TDuration GetPossiblePostponeDuration() const;
    void SetPossiblePostponeDuration(TDuration d);

    ui64 GetRequestStartedCycles() const;
    void SetRequestStartedCycles(ui64 cycles);

    TInstant GetPostponeTs() const;
    void SetPostponeTs(TInstant ts);

    ui64 GetResponseSentCycles() const;
    void SetResponseSentCycles(ui64 cycles);

    void Postpone(ui64 nowCycles);
    TDuration Advance(ui64 nowCycles);

    TDuration Time(EProcessingStage stage) const;
    void AddTime(EProcessingStage stage, TDuration d);

    TRequestTime CalcRequestTime(ui64 nowCycles) const;
};

}   // namespace NYdb::NBS
