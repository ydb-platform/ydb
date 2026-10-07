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
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimeCounterPtr RegisterTimeCounter(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IGaugePtr RegisterGauge(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimeGaugePtr RegisterTimeGauge(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ISummaryPtr RegisterSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IGaugePtr RegisterGaugeSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimeGaugePtr RegisterTimeGaugeSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimerPtr RegisterTimerSummary(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual ITimerPtr RegisterTimeHistogram(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IHistogramPtr RegisterGaugeHistogram(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual IHistogramPtr RegisterRateHistogram(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options) = 0;

    virtual void RegisterFuncCounter(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) = 0;

    virtual void RegisterFuncGauge(
        TStringBuf name,
        const TTagSet& tags,
        TSensorOptions options,
        const TRefCountedPtr& owner,
        std::function<double()> reader) = 0;

    virtual void RegisterProducer(
        TStringBuf prefix,
        const TTagSet& tags,
        TSensorOptions options,
        const ISensorProducerPtr& owner) = 0;

    virtual void RenameDynamicTag(
        const TDynamicTagPtr& tag,
        TStringBuf name,
        TStringBuf value) = 0;
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
