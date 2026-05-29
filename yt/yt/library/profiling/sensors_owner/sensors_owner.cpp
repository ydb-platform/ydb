#include "sensors_owner.h"

#include <util/digest/sequence.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

namespace NSensorsOwnerPrivate {

////////////////////////////////////////////////////////////////////////////////

TTagSetKey::operator ui64() const
{
    return TRangeHash<>{}(Tags.Tags());
}

bool TTagSetKey::operator==(const TTagSetKey& key) const
{
    return Tags.Tags() == key.Tags.Tags();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSensorsOwnerPrivate

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TSensor, typename... TArgs>
struct TSensorWrapper
{
    template <TSensor (TProfiler::*Getter)(TStringBuf, TArgs...) const>
    struct TImpl
    {
        using TKey = std::string;

        TImpl(const TProfiler& profiler, TStringBuf key, TArgs... args)
            : Sensor((profiler.*Getter)(key, std::move(args)...))
        { }

        TSensor Sensor;
    };
};

using TCounterWrapper = TSensorWrapper<TCounter>::template TImpl<&TProfiler::Counter>;
using TGaugeWrapper = TSensorWrapper<TGauge>::template TImpl<&TProfiler::Gauge>;
using TTimeGaugeWrapper = TSensorWrapper<TTimeGauge>::template TImpl<&TProfiler::TimeGauge>;
using TTimerWrapper = TSensorWrapper<TEventTimer>::template TImpl<&TProfiler::Timer>;

template <typename... Args>
using TTimeHistogramWrapper = typename TSensorWrapper<TEventTimer, Args...>::template TImpl<&TProfiler::TimeHistogram>;
template <typename... Args>
using TGaugeHistogramWrapper = typename TSensorWrapper<TGaugeHistogram, Args...>::template TImpl<&TProfiler::GaugeHistogram>;
template <typename... Args>
using TRateHistogramWrapper = typename TSensorWrapper<TRateHistogram, Args...>::template TImpl<&TProfiler::RateHistogram>;
template <typename... Args>
using TSummaryWrapper = typename TSensorWrapper<TSummary, Args...>::template TImpl<&TProfiler::Summary>;

////////////////////////////////////////////////////////////////////////////////

} // namespace

TSensorsOwner::TSensorsOwner()
    : State_(GetDefaultState())
{ }

TSensorsOwner::TSensorsOwner(const TProfiler& profiler)
    : State_(New<TState>(profiler))
{ }

const TProfiler& TSensorsOwner::GetProfiler() const
{
    return State_->Profiler;
}

TSensorsOwner::TState::TState(const TProfiler& profiler)
    : Profiler(profiler)
{ }

const TSensorsOwner& TSensorsOwner::WithTags(const TTagSet& tags) const
{
    struct TChild
    {
        TSensorsOwner SensorsOwner;

        TChild(const TProfiler& profiler)
            : SensorsOwner(profiler)
        { }
    };

    return GetWithTags<TChild>(tags).SensorsOwner;
}

const TSensorsOwner& TSensorsOwner::WithTag(TStringBuf name, TStringBuf value) const
{
    return WithTags(TTagSet().WithTag({std::string(name), std::string(value)}));
}

const TSensorsOwner& TSensorsOwner::WithRequiredTag(TStringBuf name, TStringBuf value) const
{
    return WithTags(TTagSet().WithRequiredTag({std::string(name), std::string(value)}));
}

const TSensorsOwner& TSensorsOwner::WithExcludedTag(TStringBuf name, TStringBuf value) const
{
    return WithTags(TTagSet().WithExcludedTag({std::string(name), std::string(value)}));
}

const TSensorsOwner& TSensorsOwner::WithAlternativeTag(TStringBuf name, TStringBuf value, int alternativeTo) const
{
    return WithTags(TTagSet().WithAlternativeTag({std::string(name), std::string(value)}, alternativeTo));
}

const TSensorsOwner& TSensorsOwner::WithPrefix(TStringBuf prefix) const
{
    struct TChild
    {
        using TKey = std::string;

        TSensorsOwner SensorsOwner;

        TChild(const TProfiler& profiler, TStringBuf prefix)
            : SensorsOwner(profiler.WithPrefix(prefix))
        { }
    };

    return Get<TChild>(prefix).SensorsOwner;
}

const TSensorsOwner& TSensorsOwner::WithGlobal() const
{
    struct TChild
    {
        TSensorsOwner SensorsOwner;

        TChild(const TProfiler& profiler)
            : SensorsOwner(profiler.WithGlobal())
        { }
    };

    return Get<TChild>().SensorsOwner;
}

const TCounter& TSensorsOwner::GetCounter(TStringBuf name) const
{
    return Get<TCounterWrapper>(name).Sensor;
}

const TGauge& TSensorsOwner::GetGauge(TStringBuf name) const
{
    return Get<TGaugeWrapper>(name).Sensor;
}

const TTimeGauge& TSensorsOwner::GetTimeGauge(TStringBuf name) const
{
    return Get<TTimeGaugeWrapper>(name).Sensor;
}

const TEventTimer& TSensorsOwner::GetTimer(TStringBuf name) const
{
    return Get<TTimerWrapper>(name).Sensor;
}

const TEventTimer& TSensorsOwner::GetTimeHistogram(TStringBuf name, std::vector<TDuration> bounds) const
{
    return Get<TTimeHistogramWrapper<std::vector<TDuration>>>(name, std::move(bounds)).Sensor;
}

const TEventTimer& TSensorsOwner::GetTimeHistogram(TStringBuf name, TDuration min, TDuration max) const
{
    return Get<TTimeHistogramWrapper<TDuration, TDuration>>(name, min, max).Sensor;
}

const TGaugeHistogram& TSensorsOwner::GetGaugeHistogram(TStringBuf name, std::vector<double> buckets) const
{
    return Get<TGaugeHistogramWrapper<std::vector<double>>>(name, std::move(buckets)).Sensor;
}

const TRateHistogram& TSensorsOwner::GetRateHistogram(TStringBuf name, std::vector<double> buckets) const
{
    return Get<TRateHistogramWrapper<std::vector<double>>>(name, std::move(buckets)).Sensor;
}

const TSummary& TSensorsOwner::GetSummary(TStringBuf name, ESummaryPolicy policy) const
{
    return Get<TSummaryWrapper<ESummaryPolicy>>(name, policy).Sensor;
}

void TSensorsOwner::Increment(TStringBuf name, i64 delta) const
{
    GetCounter(name).Increment(delta);
}

TIntrusivePtr<TSensorsOwner::TState> TSensorsOwner::GetDefaultState()
{
    static auto state = New<TState>(TProfiler());
    return state;
}

const TSensorsOwner& TSensorsOwner::WithHot(bool value) const
{
    struct TChild
    {
        using TKey = bool;

        TSensorsOwner SensorsOwner;

        TChild(const TProfiler& profiler, bool value)
            : SensorsOwner(profiler.WithHot(value))
        { }
    };
    return Get<TChild>(value).SensorsOwner;
}

const TSensorsOwner& GetRootSensorsOwner()
{
    struct TLocalType
    {
        TSensorsOwner SensorsOwner{TProfiler("", "")};
    };

    return Singleton<TLocalType>()->SensorsOwner;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
