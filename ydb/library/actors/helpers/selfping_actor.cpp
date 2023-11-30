#include "selfping_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/sliding_window/sliding_window.h>

namespace NActors {

ui64 MeasureTaskDurationNs() {
    // Prepare worm test data
    // 11 * 11 * 3 * 8 = 2904 bytes, fits in L1 cache
    constexpr ui64 Size = 11;
    // Align the data to reduce random alignment effects
    alignas(64) TStackVec<ui64, Size * Size * 3> data;
    ui64 s = 0;
    NHPTimer::STime beginTime;
    NHPTimer::STime endTime;
    // Prepare the data
    data.resize(Size * Size * 3);
    for (ui64 matrixIdx = 0; matrixIdx < 3; ++matrixIdx) {
        for (ui64 y = 0; y < Size; ++y) {
            for (ui64 x = 0; x < Size; ++x) {
                data[matrixIdx * (Size * Size) + y * Size + x] = y * Size + x;
            }
        }
    }
    // Warm-up the cache
    NHPTimer::GetTime(&beginTime);
    for (ui64 idx = 0; idx < data.size(); ++idx) {
        s += data[idx];
    }
    NHPTimer::GetTime(&endTime);
    s += (ui64)(1000000.0 * NHPTimer::GetSeconds(endTime - beginTime));

    // Measure the CPU performance
    // C = A * B  with injected dependency to s
    NHPTimer::GetTime(&beginTime);
    for (ui64 y = 0; y < Size; ++y) {
        for (ui64 x = 0; x < Size; ++x) {
            for (ui64 i = 0; i < Size; ++i) {
                s += data[y * Size + i] * data[Size * Size + i * Size + x];
            }
            data[2 * Size * Size + y * Size + x] = s;
            s = 0;
        }
    }
    for (ui64 idx = 0; idx < data.size(); ++idx) {
        s += data[idx];
    }
    NHPTimer::GetTime(&endTime);
    // Prepare the result
    double d = 1000000000.0 * (NHPTimer::GetSeconds(endTime - beginTime) + 0.000000001 * (s & 1));
    return (ui64)d;
}

namespace {

struct TEvPing: public TEventLocal<TEvPing, TEvents::THelloWorld::Ping> {
    TEvPing(double timeStart)
        : TimeStart(timeStart)
    {}

    const double TimeStart;
};

template <class TValueType_>
struct TAvgOperation {
    struct TValueType {
        ui64 Count = 0;
        TValueType_ Sum = TValueType_();
    };
    using TValueVector = TVector<TValueType>;

    static constexpr TValueType InitialValue() {
        return TValueType(); // zero
    }

    // Updates value in current bucket and returns window value
    static TValueType UpdateBucket(TValueType windowValue, TValueVector& buckets, size_t index, TValueType newVal) {
        Y_ASSERT(index < buckets.size());
        buckets[index].Sum += newVal.Sum;
        buckets[index].Count += newVal.Count;
        windowValue.Sum += newVal.Sum;
        windowValue.Count += newVal.Count;
        return windowValue;
    }

    static TValueType ClearBuckets(TValueType windowValue, TValueVector& buckets, size_t firstElemIndex, size_t bucketsToClear) {
        Y_ASSERT(!buckets.empty());
        Y_ASSERT(firstElemIndex < buckets.size());
        Y_ASSERT(bucketsToClear <= buckets.size());

        const size_t arraySize = buckets.size();
        for (size_t i = 0; i < bucketsToClear; ++i) {
            TValueType& curVal = buckets[firstElemIndex];
            windowValue.Sum -= curVal.Sum;
            windowValue.Count -= curVal.Count;
            curVal = InitialValue();
            firstElemIndex = (firstElemIndex + 1) % arraySize;
        }
        return windowValue;
    }

};

class TSelfPingActor : public TActorBootstrapped<TSelfPingActor> {
private:
    const TDuration SendInterval;
    const NMonitoring::TDynamicCounters::TCounterPtr MaxPingCounter;
    const NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounter;
    const NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounterWithSmallWindow;
    const NMonitoring::TDynamicCounters::TCounterPtr CalculationTimeCounter;

    NSlidingWindow::TSlidingWindow<NSlidingWindow::TMaxOperation<ui64>> MaxPingSlidingWindow;
    NSlidingWindow::TSlidingWindow<TAvgOperation<ui64>> AvgPingSlidingWindow;
    NSlidingWindow::TSlidingWindow<TAvgOperation<ui64>> AvgPingSmallSlidingWindow;
    NSlidingWindow::TSlidingWindow<TAvgOperation<ui64>> CalculationSlidingWindow;

    THPTimer Timer;

public:
    static constexpr auto ActorActivityType() {
        return EActivityType::SELF_PING_ACTOR;
    }

    TSelfPingActor(TDuration sendInterval,
            const NMonitoring::TDynamicCounters::TCounterPtr& maxPingCounter,
            const NMonitoring::TDynamicCounters::TCounterPtr& avgPingCounter,
            const NMonitoring::TDynamicCounters::TCounterPtr& avgPingSmallWindowCounter,
            const NMonitoring::TDynamicCounters::TCounterPtr& calculationTimeCounter)
        : SendInterval(sendInterval)
        , MaxPingCounter(maxPingCounter)
        , AvgPingCounter(avgPingCounter)
        , AvgPingCounterWithSmallWindow(avgPingSmallWindowCounter)
        , CalculationTimeCounter(calculationTimeCounter)
        , MaxPingSlidingWindow(TDuration::Seconds(15), 100)
        , AvgPingSlidingWindow(TDuration::Seconds(15), 100)
        , AvgPingSmallSlidingWindow(TDuration::Seconds(1), 100)
        , CalculationSlidingWindow(TDuration::Seconds(15), 100)
    {
    }

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TSelfPingActor::RunningState);
        SchedulePing(ctx, Timer.Passed());
    }

    STFUNC(RunningState)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPing, HandlePing);
        default:
            Y_ABORT("TSelfPingActor::RunningState: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    void HandlePing(TEvPing::TPtr &ev, const TActorContext &ctx)
    {
        const auto now = ctx.Now();
        const double hpNow = Timer.Passed();
        const auto& e = *ev->Get();
        const double passedTime = hpNow - e.TimeStart;
        const ui64 delayUs = passedTime > 0.0 ? static_cast<ui64>(passedTime * 1e6) : 0;

        if (MaxPingCounter) {
            *MaxPingCounter = MaxPingSlidingWindow.Update(delayUs, now);
        }
        if (AvgPingCounter) {
            auto res = AvgPingSlidingWindow.Update({1, delayUs}, now);
            *AvgPingCounter = double(res.Sum) / double(res.Count + 1);
        }
        if (AvgPingCounterWithSmallWindow) {
            auto res = AvgPingSmallSlidingWindow.Update({1, delayUs}, now);
            *AvgPingCounterWithSmallWindow = double(res.Sum) / double(res.Count + 1);
        }

        if (CalculationTimeCounter) {
            ui64 d = MeasureTaskDurationNs();
            auto res = CalculationSlidingWindow.Update({1, d}, now);
            *CalculationTimeCounter = double(res.Sum) / double(res.Count + 1);
        }

        SchedulePing(ctx, hpNow);
    }

private:
    void SchedulePing(const TActorContext &ctx, double hpNow) const
    {
        ctx.Schedule(SendInterval, new TEvPing(hpNow));
    }
};

} // namespace

IActor* CreateSelfPingActor(
    TDuration sendInterval,
    const NMonitoring::TDynamicCounters::TCounterPtr& maxPingCounter,
    const NMonitoring::TDynamicCounters::TCounterPtr& avgPingCounter,
    const NMonitoring::TDynamicCounters::TCounterPtr& avgPingSmallWindowCounter,
    const NMonitoring::TDynamicCounters::TCounterPtr& calculationTimeCounter)
{
    return new TSelfPingActor(sendInterval, maxPingCounter, avgPingCounter, avgPingSmallWindowCounter, calculationTimeCounter);
}

} // NActors
