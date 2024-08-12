#pragma once

#include "sensors_owner_traits.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tag.h>

#include <yt/yt/library/syncmap/map.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Class that can own metrics of different types.
/*!
 *  What does 'own' means?
 *  YT profiler metric is reported only while the corresponding metric object is alive.
 *  So if you increment YT counter and destroy its object, you lost this increment.
 *  This class helps with storing metrics in long-living storage.
 *
 *  You can find examples in unittests.
 */
class TSensorsOwner
{
public:
    //! Returns no-op sensors owner. Note that is still holds the owned structs created with Get* methods.
    TSensorsOwner();
    explicit TSensorsOwner(const TProfiler& profiler);

    //! Gets owned struct of type TChild
    /*!
     *  If std::is_same<TFindKey, std::monostate>
     *      TChild is constructed as TChild{Profiler, extraArgs...]}.
     *      Result of &Get<TChild>() is always the same with fixed *this, and TChild.
     *      TChild must not contain nested type TKey or member Key.
     *  Else
     *      TChild is constructed as TChild{Profiler, TChildKey{key}, extraArgs...]}.
     *      Result of &Get<TChild>(key) is always the same with fixed *this, key and TChild.
     *      TChild must contain nested type TKey or member Key, TChildKey is determined by them.
     */
    template <typename TChild, typename TFindKey = std::monostate, typename... TExtraConstructionArgs>
    const TChild& Get(const TFindKey& key = {}, const TExtraConstructionArgs&... extraArgs) const;

    //! Gets owned struct of type TChild.
    /*!
     *  Result of &GetWithTags<TChild>(tags) is always the same with fixed *this, TChild and tags.Tags().
     *  TChild is constructed as TChild{Profiler.WithTags(tags)}.
     */
    template <typename TChild>
        requires(!NSensorsOwnerPrivate::TChildTraits<TChild>::HasKey)
    const TChild& GetWithTags(const TTagSet& tags) const;

    //! Gets owned TSensorsOwner with profiler=Profiler.WithTags(...).
    //! Result of WithTags(tags) is always the same with fixed *this and tags.Tags().
    const TSensorsOwner& WithTags(const TTagSet& tags) const;
    const TSensorsOwner& WithTag(const TString& name, const TString& value) const;
    const TSensorsOwner& WithRequiredTag(const TString& name, const TString& value) const;
    const TSensorsOwner& WithExcludedTag(const TString& name, const TString& value) const;
    const TSensorsOwner& WithAlternativeTag(const TString& name, const TString& value, int alternativeTo) const;

    //! Gets owned TSensorsOwner with profiler=Profiler.WithPrefix(...).
    //! Result of WithPrefix(prefix) is always the same with fixed *this and prefix.
    const TSensorsOwner& WithPrefix(const TString& prefix) const;

    const TSensorsOwner& WithGlobal() const;

    const TProfiler& GetProfiler() const;

    /*!
     *  Note that it is generally better to have a structure storing all the sensors
     *  you need and access it through the Get method. Avoid using methods below
     *  unless you only need a single sensor and lookup by your key is not
     *  cheaper than by TString, or you don't care about performance.
     */

    //! Gets owned counter with given metric suffix.
    const TCounter& GetCounter(TStringBuf name) const;

    //! ~ .Counter(str).Increment(delta)
    void Inc(TStringBuf name, i64 delta) const;

    //! Gets owned gauge with given metric suffix.
    const TGauge& GetGauge(TStringBuf name) const;

    //! Gets owned TimeHistogram with given metric suffix using bounds as a constructor argument.
    const TEventTimer& GetTimeHistogram(TStringBuf name, std::vector<TDuration> bounds) const;

    //! Gets owned TimeHistogram with given metric suffix using min/max as a constructor arguments.
    const TEventTimer& GetTimeHistogram(TStringBuf name, TDuration min, TDuration max) const;

    //! Gets owned GaugeHistogram with given metric suffix using buckets as a constructor.
    const TGaugeHistogram& GetGaugeHistogram(TStringBuf name, std::vector<double> buckets) const;

    //! Gets owned RateHistogram with given metric suffix using buckets as a constructor.
    const TRateHistogram& GetRateHistogram(TStringBuf name, std::vector<double> buckets) const;

private:
    struct TState final
    {
        TProfiler Profiler;
        NConcurrency::TSyncMap<std::type_index, TRefCountedPtr> Children;

        explicit TState(const TProfiler& profiler);
    };

    TIntrusivePtr<TState> State_;

    static TIntrusivePtr<TState> GetDefaultState();
};

// Root sensors owner to create others from. Has empty prefix.
const TSensorsOwner& GetRootSensorsOwner();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

#define ALLOW_INCLUDE_SENSORS_OWNER_INL_H
#include "sensors_owner-inl.h"
#undef ALLOW_INCLUDE_SENSORS_OWNER_INL_H
