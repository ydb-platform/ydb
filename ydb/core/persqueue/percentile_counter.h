#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/persqueue/topic_parser/type_definitions.h>

namespace NKikimr {
namespace NPQ {

class TMultiCounter {
public:
    TMultiCounter() = default;

    TMultiCounter(::NMonitoring::TDynamicCounterPtr counters,
                  const TVector<NPersQueue::TPQLabelsInfo>& labels,
                  const TVector<std::pair<TString, TString>>& subgroups,
                  const TVector<TString>& counter_names,
                  bool deriv,
                  const TString& name = "sensor",
                  bool expiring = true);

    void Inc(ui64 val = 1);
    void Dec(ui64 val = 1);
    void Set(ui64 value);

    operator bool();

private:
    ui64 Value = 0;
    TVector<::NMonitoring::TDynamicCounters::TCounterPtr> Counters;
};

class TPercentileCounter {
friend class TPartitionHistogramWrapper;

public:
    TPercentileCounter() = default;
    TPercentileCounter(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                       const TVector<NPersQueue::TPQLabelsInfo>& labels,
                       const TVector<std::pair<TString, TString>>& subgroups,
                       const TString& sensor,
                       const TVector<std::pair<ui64, TString>>& intervals,
                       const bool deriv,
                       bool expiring = true);

    void IncFor(ui64 key, ui64 value = 1);
    void DecFor(ui64 key, ui64 value = 1);

    operator bool();

private:
    TVector<TMultiCounter> Counters;
    TVector<ui64> Ranges;
};

NKikimr::NPQ::TPercentileCounter CreateSLIDurationCounter(
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TVector<NPersQueue::TPQLabelsInfo> aggr,
        const TString name, ui32 border, TVector<ui32> durations);


class TPartitionCounterWrapper {
private:
    bool DoSave;
    bool DoReport;
    TMaybe<NKikimr::NPQ::TMultiCounter> Counter;
    ui64 CounterValue = 0;
    bool Inited = false;

public:
    TPartitionCounterWrapper() = default;
    explicit TPartitionCounterWrapper(NKikimr::NPQ::TMultiCounter&& counter, bool isSupportivePartition, bool doReport = true);
    void Setup(bool isSupportivePartition, bool doReport, NKikimr::NPQ::TMultiCounter&& counter);
    void Inc(ui64 value);
    ui64 Value() const;
    void SetSavedValue(ui64 value);
    operator bool() const;
};

class TPartitionHistogramWrapper {
private:
    bool IsSupportivePartition;
    std::unique_ptr<NKikimr::NPQ::TPercentileCounter> Histogram;
    TMap<ui32, ui64> Values;
    bool Inited = false;

public:
    TPartitionHistogramWrapper() = default;
    void Setup(bool isSupportivePartition, std::unique_ptr<NKikimr::NPQ::TPercentileCounter>&& histogram);
    void IncFor(ui64 key, ui64 value = 1);
    TVector<ui64> GetValues() const;
    template<class TIterable>
    void SetValues(const TIterable& inputVector) {
        auto iter = Values.begin();
        for (auto inputVal: inputVector) {
            if (iter == Values.end())
                break;
            iter->second = inputVal;
            iter++;
        }
    }
    const TVector<ui64>& GetRanges() const;
    operator bool() const;
};


class TMultiBucketCounter {
private:
    struct TBucket {
        ui64 Range;
        double AvgValue = 0;
        ui64 ValuesCount = 0;
        TBucket() = default;
        explicit TBucket(ui64 range)
            : Range(range)
            , AvgValue(0)
            , ValuesCount(0)
        {}
    };
    TVector<TBucket> Buckets;
    ui64 TimeReference;

    ui64 InsertWithHint(double value, ui64 count, ui64 hint) noexcept;

public:
    TMultiBucketCounter(const TVector<ui64>& buckets, ui64 multiplier, ui64 timeRef);
    void UpdateTimestamp(ui64 newTimeReference);
    void Insert(i64 value, ui64 count) noexcept;
    TVector<std::pair<double, ui64>> GetValues(bool allowZeroes = false) const noexcept;

};

}// NPQ
}// NKikimr
