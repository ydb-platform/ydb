#include "context.h"

#include <util/datetime/cputimer.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TCallContextBase::TCallContextBase(ui64 requestId)
    : RequestId(requestId)
{}

TRequestTime TCallContextBase::CalcRequestTime(ui64 nowCycles) const
{
    const ui64 startCycles = GetRequestStartedCycles();
    if (!startCycles || startCycles >= nowCycles) {
        return TRequestTime {
            .TotalTime = TDuration::Zero(),
            .ExecutionTime = TDuration::Zero(),
        };
    }

    TRequestTime requestTime;
    requestTime.TotalTime = CyclesToDurationSafe(nowCycles - startCycles);

    const ui64 postponeStart = AtomicGet(PostponeTsCycles);
    if (postponeStart && startCycles < postponeStart && postponeStart < nowCycles) {
        nowCycles = postponeStart;
    }

    const auto postponeDuration = Time(EProcessingStage::Postponed);
    const auto backoffTime = Time(EProcessingStage::Backoff);

    auto responseSentCycles = GetResponseSentCycles();
    auto responseDuration = CyclesToDurationSafe(
        (responseSentCycles ? responseSentCycles : nowCycles) - startCycles);

    requestTime.ExecutionTime = responseDuration - postponeDuration -
        backoffTime - GetPossiblePostponeDuration();

    return requestTime;
}

TDuration TCallContextBase::GetPossiblePostponeDuration() const
{
    return TDuration::MicroSeconds(AtomicGet(PossiblePostponeMicroSeconds));
}

void TCallContextBase::SetPossiblePostponeDuration(TDuration d)
{
    AtomicSet(PossiblePostponeMicroSeconds, d.MicroSeconds());
}

ui64 TCallContextBase::GetRequestStartedCycles() const
{
    return AtomicGet(RequestStartedCycles);
}

void TCallContextBase::SetRequestStartedCycles(ui64 cycles)
{
    AtomicSet(RequestStartedCycles, cycles);
}

TInstant TCallContextBase::GetPostponeTs() const
{
    return PostponeTs;
}

void TCallContextBase::SetPostponeTs(TInstant ts)
{
    PostponeTs = ts;
}

ui64 TCallContextBase::GetResponseSentCycles() const
{
    return AtomicGet(ResponseSentCycles);
}

void TCallContextBase::SetResponseSentCycles(ui64 cycles)
{
    AtomicSet(ResponseSentCycles, cycles);
}

void TCallContextBase::Postpone(ui64 nowCycles)
{
    Y_DEBUG_ABORT_UNLESS(
        AtomicGet(PostponeTsCycles) == 0,
        "Request was not advanced.");
    Y_DEBUG_ABORT_UNLESS(nowCycles > 0);
    AtomicSet(PostponeTsCycles, nowCycles);
}

TDuration TCallContextBase::Advance(ui64 nowCycles)
{
    const auto start = AtomicGet(PostponeTsCycles);
    Y_DEBUG_ABORT_UNLESS(start != 0, "Request was not postponed.");

    const auto delay = CyclesToDurationSafe(nowCycles - start);
    AddTime(EProcessingStage::Postponed, delay);
    AtomicSet(PostponeTsCycles, 0);

    return delay;
}

TDuration TCallContextBase::Time(EProcessingStage stage) const
{
    return TDuration::MicroSeconds(AtomicGet(Stage2Time[static_cast<int>(stage)]));
}

void TCallContextBase::AddTime(EProcessingStage stage, TDuration d)
{
    AtomicAdd(Stage2Time[static_cast<int>(stage)], d.MicroSeconds());
}

}   // namespace NYdb::NBS
