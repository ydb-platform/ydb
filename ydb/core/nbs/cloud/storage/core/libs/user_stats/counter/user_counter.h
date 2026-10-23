#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram_counter_options.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NYdb::NBS::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

class IUserCounter
{
public:
    virtual ~IUserCounter() = default;

    virtual void GetType(NMonitoring::IMetricConsumer* consumer) const = 0;
    virtual void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const = 0;
};

class TUserCounter
{
private:
    std::shared_ptr<IUserCounter> Counter;

public:
    TUserCounter(std::shared_ptr<IUserCounter> counter);

    void Accept(
        const NMonitoring::TLabels& baseLabels,
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const;
};

////////////////////////////////////////////////////////////////////////////////

class IUserCounterSupplier: public NMonitoring::IMetricSupplier
{
public:
    ~IUserCounterSupplier() override = default;

    virtual void AddUserMetric(
        NMonitoring::TLabels labels,
        TStringBuf name,
        TUserCounter metric) = 0;
    virtual void RemoveUserMetric(
        NMonitoring::TLabels labels,
        TStringBuf name) = 0;
};

using IUserCounterSupplierPtr = std::shared_ptr<IUserCounterSupplier>;

IUserCounterSupplierPtr CreateUserCounterSupplier();
IUserCounterSupplierPtr CreateUserCounterSupplierStub();

////////////////////////////////////////////////////////////////////////////////

struct TBucket
{
    NMonitoring::TBucketBound Bound;
    TString Name;
};

static constexpr size_t BUCKETS_COUNT = 25;

using TBuckets = std::array<TBucket, BUCKETS_COUNT>;
using TBucketsWithUnits = std::pair<TBuckets, TString>;

TBucketsWithUnits GetMsBuckets();
TBucketsWithUnits GetUsBuckets();
TBucketsWithUnits GetTimeBuckets(EHistogramCounterOptions options);

////////////////////////////////////////////////////////////////////////////////

using TBaseDynamicCounters =
    std::pair<NMonitoring::TDynamicCounterPtr, TString>;

void AddUserMetric(
    IUserCounterSupplier& dsc,
    const NMonitoring::TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName);

void AddHistogramUserMetric(
    const TBucketsWithUnits& buckets,
    IUserCounterSupplier& dsc,
    const NMonitoring::TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName,
    EHistogramCounterOptions histogramCounterOptions);

}   // namespace NYdb::NBS::NStorage::NUserStats
