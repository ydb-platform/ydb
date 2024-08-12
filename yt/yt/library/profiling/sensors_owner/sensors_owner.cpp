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
    template <TSensor (TProfiler::*Getter)(const TString&, TArgs...) const>
    struct TImpl
    {
        using TKey = TString;

        TImpl(const TProfiler& profiler, const TString& key, TArgs... args)
            : Sensor((profiler.*Getter)(key, std::move(args)...))
        { }

        TSensor Sensor;
    };
};

using TCounterWrapper = TSensorWrapper<TCounter>::template TImpl<&TProfiler::Counter>;
using TGaugeWrapper = TSensorWrapper<TGauge>::template TImpl<&TProfiler::Gauge>;

template <typename... Args>
using TTimeHistogramWrapper = typename TSensorWrapper<TEventTimer, Args...>::template TImpl<&TProfiler::TimeHistogram>;
template <typename... Args>
using TGaugeHistogramWrapper = typename TSensorWrapper<TGaugeHistogram, Args...>::template TImpl<&TProfiler::GaugeHistogram>;
template <typename... Args>
using TRateHistogramWrapper = typename TSensorWrapper<TRateHistogram, Args...>::template TImpl<&TProfiler::RateHistogram>;

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

const TSensorsOwner& TSensorsOwner::WithTag(const TString& name, const TString& value) const
{
    return WithTags(TTagSet().WithTag({name, value}));
}

const TSensorsOwner& TSensorsOwner::WithRequiredTag(const TString& name, const TString& value) const
{
    return WithTags(TTagSet().WithRequiredTag({name, value}));
}

const TSensorsOwner& TSensorsOwner::WithExcludedTag(const TString& name, const TString& value) const
{
    return WithTags(TTagSet().WithExcludedTag({name, value}));
}

const TSensorsOwner& TSensorsOwner::WithAlternativeTag(const TString& name, const TString& value, int alternativeTo) const
{
    return WithTags(TTagSet().WithAlternativeTag({name, value}, alternativeTo));
}

const TSensorsOwner& TSensorsOwner::WithPrefix(const TString& prefix) const
{
    struct TChild
    {
        using TKey = TString;

        TSensorsOwner SensorsOwner;

        TChild(const TProfiler& profiler, const TString& prefix)
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

void TSensorsOwner::Inc(TStringBuf name, i64 delta) const
{
    GetCounter(name).Increment(delta);
}

TIntrusivePtr<TSensorsOwner::TState> TSensorsOwner::GetDefaultState()
{
    static auto state = New<TState>(TProfiler());
    return state;
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
