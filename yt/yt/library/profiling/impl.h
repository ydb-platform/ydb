#pragma once

#include "public.h"
#include "sensor.h"
#include "summary.h"

#include <library/cpp/yt/memory/weak_ptr.h>
#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct IRegistry
    : public TRefCounted
{
    virtual ICounterPtr RegisterCounter(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimeCounterPtr RegisterTimeCounter(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IGaugePtr RegisterGauge(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimeGaugePtr RegisterTimeGauge(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ISummaryPtr RegisterSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IGaugePtr RegisterGaugeSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimeGaugePtr RegisterTimeGaugeSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimerPtr RegisterTimerSummary(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimerPtr RegisterTimeHistogram(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IHistogramPtr RegisterGaugeHistogram(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IHistogramPtr RegisterRateHistogram(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual void RegisterFuncCounter(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) = 0;

    virtual void RegisterFuncGauge(
        const std::string& name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<double()> reader) = 0;

    virtual void RegisterProducer(
        const std::string& prefix,
        const TTagSet& tags,
        TSensorOptions options,
        const ISensorProducerPtr& owner) = 0;

    virtual void RenameDynamicTag(
        const TDynamicTagPtr& tag,
        const std::string& name,
        const std::string& value) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRegistry)

IRegistryPtr GetGlobalRegistry();

////////////////////////////////////////////////////////////////////////////////

struct ICounter
    : public TRefCounted
{
    virtual void Increment(i64 delta) = 0;
    virtual i64 GetValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICounter)

////////////////////////////////////////////////////////////////////////////////

struct ITimeCounter
    : public TRefCounted
{
    virtual void Add(TDuration delta) = 0;

    virtual TDuration GetValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimeCounter)

////////////////////////////////////////////////////////////////////////////////

struct IGauge
    : public virtual TRefCounted
{
    virtual void Update(double value) = 0;
    virtual double GetValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(IGauge)

////////////////////////////////////////////////////////////////////////////////

struct ITimeGauge
    : public virtual TRefCounted
{
    virtual void Update(TDuration value) = 0;
    virtual TDuration GetValue() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimeGauge)

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct ISummaryBase
    : public virtual TRefCounted
{
    virtual void Record(T value) = 0;

    virtual TSummarySnapshot<T> GetSummary() = 0;
    virtual TSummarySnapshot<T> GetSummaryAndReset() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISummary)
DEFINE_REFCOUNTED_TYPE(ITimer)

////////////////////////////////////////////////////////////////////////////////

struct IHistogram
    : public virtual TRefCounted
{
    virtual void Add(double value, int count) = 0;
    virtual void Remove(double value, int count) = 0;
    virtual void Reset() = 0;

    virtual THistogramSnapshot GetSnapshot(bool reset) = 0;
    virtual void LoadSnapshot(THistogramSnapshot snapshot) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHistogram)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
