#include "user_counter.h"

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram_types.h>

namespace NYdb::NBS::NStorage::NUserStats {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUserCounterSupplier: public IUserCounterSupplier
{
private:
    TRWMutex Lock;
    THashMap<TLabels, TUserCounter> Metrics;

public:
    // NMonitoring::IMetricSupplier
    void Accept(TInstant time, IMetricConsumer* consumer) const override
    {
        if (!consumer) {
            return;
        }

        consumer->OnStreamBegin();
        {
            TReadGuard g{Lock};
            for (const auto& it: Metrics) {
                it.second.Accept(it.first, time, consumer);
            }
        }
        consumer->OnStreamEnd();
    }

    void Append(TInstant time, IMetricConsumer* consumer) const override
    {
        TReadGuard g{Lock};
        for (const auto& it: Metrics) {
            it.second.Accept(it.first, time, consumer);
        }
    }

    // IUserCounterSupplier
    void
    AddUserMetric(TLabels labels, TStringBuf name, TUserCounter metric) override
    {
        labels.Add("name", name);

        TWriteGuard g{Lock};
        Metrics.emplace(std::move(labels), std::move(metric));
    }

    void RemoveUserMetric(TLabels labels, TStringBuf name) override
    {
        labels.Add("name", name);

        TWriteGuard g{Lock};
        Metrics.erase(labels);
    }
};

class TUserCounterSupplierStub: public IUserCounterSupplier
{
public:
    // NMonitoring::IMetricSupplier
    void Accept(TInstant /*time*/, IMetricConsumer* /*consumer*/) const override
    {}

    void Append(TInstant /*time*/, IMetricConsumer* /*consumer*/) const override
    {}

    // IUserCounterSupplier
    void AddUserMetric(
        TLabels /*labels*/,
        TStringBuf /*name*/,
        TUserCounter /*metric*/) override
    {}

    void RemoveUserMetric(TLabels /*labels*/, TStringBuf /*name*/) override
    {}
};

template <typename THistogramType>
TBuckets MakeBuckets(auto convertBound)
{
    static_assert(BUCKETS_COUNT == THistogramType::BUCKETS_COUNT, "");

    TBuckets result;
    const auto names = THistogramType::MakeNames();
    for (size_t i = 0; i < names.size(); ++i) {
        result[i].Bound = convertBound(THistogramType::Buckets[i]);
        result[i].Name = names[i];
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TUserCounter::TUserCounter(std::shared_ptr<IUserCounter> counter)
    : Counter(std::move(counter))
{}

void TUserCounter::Accept(
    const TLabels& baseLabels,
    TInstant time,
    IMetricConsumer* consumer) const
{
    if (!Counter || !consumer) {
        return;
    }

    Counter->GetType(consumer);

    consumer->OnLabelsBegin();

    for (const auto& label: baseLabels) {
        consumer->OnLabel(label.Name(), label.Value());
    }

    consumer->OnLabelsEnd();

    Counter->GetValue(time, consumer);

    consumer->OnMetricEnd();
}

///////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IUserCounterSupplier> CreateUserCounterSupplier()
{
    return std::make_shared<TUserCounterSupplier>();
}

std::shared_ptr<IUserCounterSupplier> CreateUserCounterSupplierStub()
{
    return std::make_shared<TUserCounterSupplierStub>();
}

///////////////////////////////////////////////////////////////////////////////

TBucketsWithUnits GetMsBuckets()
{
    constexpr auto Identity = [](double data)
    {
        return data;
    };
    static const auto Buckets = MakeBuckets<TRequestMsTimeBuckets>(Identity);
    return {Buckets, "msec"};
}

TBucketsWithUnits GetUsBuckets()
{
    constexpr auto UsToMs = [](double data)
    {
        return data == std::numeric_limits<double>::max() ? data : data / 1000.;
    };
    static const auto Buckets = MakeBuckets<TRequestUsTimeBuckets>(UsToMs);
    return {Buckets, "usec"};
}

TBucketsWithUnits GetTimeBuckets(EHistogramCounterOptions options)
{
    if (options & EHistogramCounterOption::UseMsUnitsForTimeHistogram) {
        return GetMsBuckets();
    }
    return GetUsBuckets();
}

///////////////////////////////////////////////////////////////////////////////

class TUserSumCounterWrapper: public IUserCounter
{
private:
    TVector<TIntrusivePtr<NMonitoring::TCounterForPtr>> Counters;
    EMetricType Type = EMetricType::UNKNOWN;

public:
    explicit TUserSumCounterWrapper(
        const TVector<TBaseDynamicCounters>& baseCounters)
    {
        for (const auto& [baseCounter, name]: baseCounters) {
            if (baseCounter) {
                if (auto countSub = baseCounter->FindCounter(name)) {
                    Counters.push_back(countSub);
                    Type = countSub->ForDerivative() ? EMetricType::RATE
                                                     : EMetricType::GAUGE;
                }
            }
        }
    }

    EMetricType GetType() const
    {
        return Type;
    }

    void GetType(NMonitoring::IMetricConsumer* consumer) const override
    {
        consumer->OnMetricBegin(Type);
    }

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override
    {
        int64_t sum = 0;

        for (const auto& counter: Counters) {
            sum += counter->Val();
        }

        consumer->OnInt64(time, sum);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TUserSumHistogramWrapper: public IUserCounter
{
    using TExplicitHistogramSnapshot = NMonitoring::TExplicitHistogramSnapshot;

private:
    static constexpr size_t MergeFirstBucketsCount = 10;

    const TBuckets Buckets;
    const TString Units;
    // Original inputs. Kept for lazy re-resolution if base subgroups are
    // not yet registered.
    const TVector<TBaseDynamicCounters> OriginalInputs;
    // Empty if source histograms are lazily registered (not yet created at
    // construction time).
    const TVector<TBaseDynamicCounters> BaseCounters;
    // True if some of the subgroups were not yet registered at construction.
    // GetValue re-resolves from OriginalInputs on every call in this case.
    const bool IsLazy;
    const size_t HistogramSize;
    TIntrusivePtr<TExplicitHistogramSnapshot> Histogram;
    const EMetricType Type = EMetricType::HIST_RATE;
    const EHistogramCounterOptions HistogramCounterOptions;

    TVector<TBaseDynamicCounters> ResolveBaseCounters() const
    {
        TVector<TBaseDynamicCounters> result;
        for (const auto& [baseCounter, name]: OriginalInputs) {
            if (!baseCounter) {
                continue;
            }
            auto subgroup = baseCounter->FindSubgroup("histogram", name);
            if (!subgroup) {
                continue;
            }
            if (Units) {
                if (auto unitsSubgroup = subgroup->FindSubgroup("units", Units))
                {
                    subgroup = unitsSubgroup;
                }
            }
            if (subgroup) {
                result.emplace_back(subgroup, name);
            }
        }
        return result;
    }

public:
    TUserSumHistogramWrapper(
        const TBucketsWithUnits& buckets,
        const TVector<TBaseDynamicCounters>& baseCounters,
        EHistogramCounterOptions histogramCounterOptions)
        : Buckets(buckets.first)
        , Units(buckets.second)
        , OriginalInputs(baseCounters)
        , BaseCounters(ResolveBaseCounters())
        , IsLazy(BaseCounters.size() < OriginalInputs.size())
        , HistogramSize(Buckets.size() - MergeFirstBucketsCount)
        , Histogram(TExplicitHistogramSnapshot::New(HistogramSize))
        , HistogramCounterOptions(histogramCounterOptions)
    {
        for (size_t i = 0; i < HistogramSize; ++i) {
            (*Histogram)[i].first = Buckets[i + MergeFirstBucketsCount].Bound;
        }
    }

    void Clear() const
    {
        for (size_t i = 0; i < HistogramSize; ++i) {
            (*Histogram)[i].second = 0;
        }
    }

    void GetType(NMonitoring::IMetricConsumer* consumer) const override
    {
        consumer->OnMetricBegin(Type);
    }

    void IncrementHistogram(ui64 value, size_t baseBucketId) const
    {
        // Base histogram in usec vs user histogram in msec, merge first buckets
        size_t id = baseBucketId < MergeFirstBucketsCount
                        ? 0
                        : baseBucketId - MergeFirstBucketsCount;
        (*Histogram)[id].second += value;
    }

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override
    {
        // Re-resolve on every call when lazy: histogram subgroups may not have
        // existed at construction but are created once the first request fires.
        const auto lazyCounters =
            IsLazy ? ResolveBaseCounters() : TVector<TBaseDynamicCounters>{};
        const auto& counters = IsLazy ? lazyCounters : BaseCounters;
        Clear();

        for (const auto& [baseCounter, name]: counters) {
            if (!baseCounter) {
                continue;
            }

            if (HistogramCounterOptions &
                EHistogramCounterOption::ReportSingleCounter)
            {
                const auto histogram = baseCounter->FindHistogram(name);
                if (histogram) {
                    const auto snapshot = histogram->Snapshot();
                    const size_t count =
                        Min<size_t>(Buckets.size(), snapshot->Count());
                    for (size_t i = 0; i < count; ++i) {
                        IncrementHistogram(snapshot->Value(i), i);
                    }
                    continue;
                }
            }

            if (HistogramCounterOptions &
                EHistogramCounterOption::ReportMultipleCounters)
            {
                for (size_t i = 0; i < Buckets.size(); ++i) {
                    const auto counter =
                        baseCounter->FindCounter(Buckets[i].Name);
                    if (counter) {
                        IncrementHistogram(counter->Val(), i);
                    }
                }
            }
        }
        consumer->OnHistogram(time, Histogram);
    }
};

////////////////////////////////////////////////////////////////////////////////

void AddUserMetric(
    IUserCounterSupplier& dsc,
    const NMonitoring::TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName)
{
    auto wrapper = std::make_shared<TUserSumCounterWrapper>(baseCounters);

    if (wrapper->GetType() != EMetricType::UNKNOWN) {
        dsc.AddUserMetric(commonLabels, newName, TUserCounter(wrapper));
    }
}

void AddHistogramUserMetric(
    const TBucketsWithUnits& buckets,
    IUserCounterSupplier& dsc,
    const TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName,
    EHistogramCounterOptions histogramCounterOptions)
{
    auto wrapper = std::make_shared<TUserSumHistogramWrapper>(
        buckets,
        baseCounters,
        histogramCounterOptions);

    dsc.AddUserMetric(commonLabels, newName, TUserCounter(wrapper));
}

}   // namespace NYdb::NBS::NStorage::NUserStats
