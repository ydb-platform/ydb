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
const TVector<ui64>& TPartitionHistogramWrapper::GetRanges() const {
    Y_ABORT_UNLESS(!IsSupportivePartition);
    return Histogram->Ranges;
}

TPartitionHistogramWrapper::operator bool() const {
    return Inited && (IsSupportivePartition || Histogram);
}


ui64 TMultiBucketCounter::InsertWithHint(double value, ui64 count, ui64 hint) noexcept {
    while (hint < Buckets.size()) {
        if (Buckets[hint] > value) {
            break;
        }
        ++hint;
    }
    auto newAvg = (AvgValues[hint] * ValuesCount[hint] + count * value) / (ValuesCount[hint] + count);

    ValuesCount[hint] += count;
    AvgValues[hint] = newAvg;
    return hint;
}

TMultiBucketCounter::TMultiBucketCounter(TMultiBucketCounter&& other, ui64 newTimeRef)
    : TimeReference(newTimeRef)
{
    Buckets = std::move(other.Buckets);
    ValuesCount.resize(Buckets.size(), 0);
    AvgValues.resize(Buckets.size(), 0.0);

    ui64 timeDiff = 0;
    if (newTimeRef > other.TimeReference) {
        timeDiff = newTimeRef - other.TimeReference;
    }
    ui64 hint = 0;
    for (ui64 i = 0; i < Buckets.size(); ++i) {
        if (other.ValuesCount[i])
            hint = InsertWithHint(other.AvgValues[i] + timeDiff, other.ValuesCount[i], hint);
    }

}

TMultiBucketCounter::TMultiBucketCounter(const TVector<ui64>& buckets, ui64 multiplier, ui64 timeRef)
    : TimeReference(timeRef)
{

    Buckets.resize(buckets.size() * multiplier + 1);
    auto prev = 0u;
    ui64 i = 0;
    for (auto b : buckets) {
        ui64 step = (b - prev) / multiplier;
        for (auto j = 1u; j < multiplier; ++j) {
            Buckets[i++] = prev + j * step;
        }
        Buckets[i++] = b;
        prev = b;
    }
    Buckets[i++] = std::numeric_limits<ui64>::max();
    AvgValues.resize(Buckets.size(), 0.0);
    ValuesCount.resize(Buckets.size(), 0);

}

void TMultiBucketCounter::Insert(i64 value, ui64 count) noexcept {
    if (value < 0) {
        InsertWithHint(0, count, 0);
        return;
    }

    ui64 begin = 0, end = Buckets.size() - 1;
    while (end - begin > 10) {
        ui64 median = begin + (end - begin) / 2;
        if (Buckets[median] >= (ui64)value) {
            end = median;
        } else {
            begin = median;
        }
    }
    InsertWithHint((ui64)value, count, begin);
}

TVector<std::pair<double, ui64>> TMultiBucketCounter::GetValues() const noexcept {
    TVector<std::pair<double, ui64>> result;
    for (auto i = 0u; i < ValuesCount.size(); ++i) {
        if (ValuesCount[i] != 0)
            result.push_back(std::make_pair(AvgValues[i], ValuesCount[i]));
    }
    return result;
}

} // NPQ
} // NKikimr
