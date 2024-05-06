#include "percentile_counter.h"

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/base/counters.h>

namespace NKikimr {

namespace NPQ {


TMultiCounter::TMultiCounter(::NMonitoring::TDynamicCounterPtr counters,
                             const TVector<NPersQueue::TPQLabelsInfo>& labels,
                             const TVector<std::pair<TString, TString>>& subgroups,
                             const TVector<TString>& counter_names,
                             bool deriv,
                             const TString& name,
                             bool expiring)
    : Value(0)
{
    Y_ABORT_UNLESS(counters);

    for (const auto& counter : counter_names) {
        for (ui32 i = 0; i <= labels.size(); ++i) {
            auto cc = counters;
            for (ui32 j = 0; j < labels.size(); ++j) {
                Y_ABORT_UNLESS(!labels[j].Labels.empty());
                for (ui32 k = 0; k < labels[j].Labels.size(); ++k) {
                    Y_ABORT_UNLESS(labels[j].Labels.size() == labels[j].AggrNames.size());
                    const TString& res = (j < i) ? labels[j].Labels[k].second : labels[j].AggrNames[k];
                    cc = cc->GetSubgroup(labels[j].Labels[k].first, res);
                }
            }
            for (const auto& g: subgroups) {
                cc = cc->GetSubgroup(g.first, g.second);
            }
            if (expiring) {
                Counters.push_back(cc->GetExpiringNamedCounter(name, counter, deriv));
            } else {
                Counters.push_back(cc->GetNamedCounter(name, counter, deriv));
            }
        }
    }
}

void TMultiCounter::Inc(ui64 val)
{
    for (auto& c : Counters) (*c) += val;
    Value += val;
}

void TMultiCounter::Dec(ui64 val) {
    for (auto& c : Counters) (*c) -= val;
    Value -= val;
}

void TMultiCounter::Set(ui64 value) {
    auto diff = value - Value;
    Inc(diff);
}

TMultiCounter::operator bool() {
    return !Counters.empty();
}


TPercentileCounter::TPercentileCounter(
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const TVector<NPersQueue::TPQLabelsInfo>& labels,
        const TVector<std::pair<TString, TString>>& subgroups, const TString& sensor,
        const TVector<std::pair<ui64, TString>>& intervals, const bool deriv, bool expiring
) {
    Y_ABORT_UNLESS(!intervals.empty());
    Counters.reserve(intervals.size());
    Ranges.reserve(intervals.size());
    for (auto& interval : intervals) {
        Ranges.push_back(interval.first);
        Counters.push_back(TMultiCounter(counters, labels, subgroups, {interval.second}, deriv, sensor, expiring));
    }
    Ranges.back() = Max<ui64>();
}

void TPercentileCounter::IncFor(ui64 key, ui64 value) {
    if (!Ranges.empty()) {
        ui32 i = 0;
        // The last range value is Max<ui64>().
        while (Ranges[i] < key) {
            ++i;
        }
        Y_ASSERT(i < Ranges.size());
        Counters[i].Inc(value);
    }
}

void TPercentileCounter::DecFor(ui64 key, ui64 value) {
    if (!Ranges.empty()) {
        ui32 i = 0;
        // The last range value is Max<ui64>().
        while (Ranges[i] < key) {
            ++i;
        }
        Y_ASSERT(i < Ranges.size());
        Counters[i].Dec(value);
    }
}

TPercentileCounter::operator bool() {
    return !Counters.empty();
}

NKikimr::NPQ::TPercentileCounter CreateSLIDurationCounter(
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TVector<NPersQueue::TPQLabelsInfo> aggr, const TString name, ui32 border,
        TVector<ui32> durations
) {
    bool found = false;
    for (auto it = durations.begin(); it != durations.end(); ++it) {
        if (*it == border) {
            found = true;
            break;
        }
        if (*it > border) {
            found = true;
            durations.insert(it, border);
            break;
        }
    }
    if (!found)
        durations.push_back(border);
    TVector<std::pair<ui64, TString>> buckets;
    for (auto& dur : durations) {
        buckets.emplace_back(dur, TStringBuilder() << dur << "ms");
    }
    return NKikimr::NPQ::TPercentileCounter(counters->GetSubgroup("sensor", name), aggr, {}, "Duration", buckets, true, false);
}

TPartitionCounterWrapper::TPartitionCounterWrapper(NKikimr::NPQ::TMultiCounter&& counter, bool isSupportivePartition, bool doReport) {
    Setup(isSupportivePartition, doReport, std::move(counter));
}

void TPartitionCounterWrapper::Setup(bool isSupportivePartition, bool doReport, NKikimr::NPQ::TMultiCounter&& counter) {
    Inited = true;
    DoSave = isSupportivePartition;
    DoReport = !isSupportivePartition || doReport;
    if (DoReport) {
        Counter = std::move(counter);
    }
}

void TPartitionCounterWrapper::Inc(ui64 value) {
    if (DoSave) {
        CounterValue += value;
    }
    if (DoReport) {
        Counter->Inc(value);
    }
}

ui64 TPartitionCounterWrapper::Value() const {
    return CounterValue; //Getting counter value for non-supportive partition is unexpected and returns 0
}

void TPartitionCounterWrapper::SetSavedValue(ui64 value) {
    CounterValue = value;
}

TPartitionCounterWrapper::operator bool() const {
    return Inited && (!DoReport || Counter);
}

void TPartitionHistogramWrapper::Setup(bool isSupportivePartition, std::unique_ptr<NKikimr::NPQ::TPercentileCounter>&& histogram) {
    IsSupportivePartition = isSupportivePartition;
    Inited = true;
    if (!IsSupportivePartition) {
        Histogram = std::move(histogram);
    } else {
        for (const auto& bucket : histogram->Ranges) {
            Values.insert(std::make_pair(bucket, 0));
        }
    }
}
void TPartitionHistogramWrapper::IncFor(ui64 key, ui64 value) {
    if (!IsSupportivePartition) {
        return Histogram->IncFor(key, value);
    }
    auto bucket = Values.lower_bound(key);
    if (bucket != Values.end()) {
        bucket->second += value;
    }
}
TVector<ui64> TPartitionHistogramWrapper::GetValues() const {
    TVector<ui64> res;
    res.reserve(Values.size());
    for (auto iter : Values) {
        res.push_back(iter.second);
    }
    return res;
}

TPartitionHistogramWrapper::operator bool() const {
    return Inited && (IsSupportivePartition || Histogram);
}

} // NPQ
} // NKikimr
