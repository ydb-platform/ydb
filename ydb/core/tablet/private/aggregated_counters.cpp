#include "aggregated_counters.h"
#include <ydb/core/protos/labeled_counters.pb.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>


namespace NKikimr::NPrivate {

////////////////////////////////////////////
using TCountersVector = TVector<::NMonitoring::TDynamicCounters::TCounterPtr>;

/*
** struct THistogramCounter
*/
THistogramCounter::THistogramCounter(
    const TVector<TTabletPercentileCounter::TRangeDef>& ranges,
    TCountersVector&& values,
    NMonitoring::THistogramPtr histogram)
        : Ranges(ranges)
        , Values(std::move(values))
        , Histogram(histogram) {
    Y_VERIFY(!Ranges.empty() && Ranges.size() == Values.size());
}

void THistogramCounter::Clear() {
    for (const ::NMonitoring::TDynamicCounters::TCounterPtr& cnt : Values) {
        *cnt = 0;
    }

    Histogram->Reset();
}

void THistogramCounter::IncrementFor(ui64 value) {
    const size_t i = std::lower_bound(Ranges.begin(), Ranges.end(), value) - Ranges.begin();
    Values[i]->Inc();

    Histogram->Collect(value);
}

using THistogramVector = TVector<THolder<THistogramCounter>>;

/*
** class TAggregatedSimpleCounters
*/
TAggregatedSimpleCounters::TAggregatedSimpleCounters(::NMonitoring::TDynamicCounterPtr counterGroup)
    : CounterGroup(counterGroup)
{}

void TAggregatedSimpleCounters::Reserve(size_t hint) {
    CountersByTabletID.reserve(hint);
    ChangedCounters.reserve(hint);
    MaxSimpleCounters.reserve(hint);
}

void TAggregatedSimpleCounters::AddSimpleCounter(
    const char* name,
    THolder<THistogramCounter> percentileAggregate) {
    auto fnAddCounter = [this](const char* name, TCountersVector& container) {
        auto counter = CounterGroup->GetCounter(name, false);
        container.push_back(counter);
    };

    CountersByTabletID.push_back(TCountersByTabletIDMap());
    ChangedCounters.push_back(true);
    TString maxName = Sprintf("MAX(%s)", name);
    TString sumName = Sprintf("SUM(%s)", name);

    fnAddCounter(maxName.data(), MaxSimpleCounters);
    fnAddCounter(sumName.data(), SumSimpleCounters);

    HistSimpleCounters.emplace_back(std::move(percentileAggregate));
}

ui64 TAggregatedSimpleCounters::GetSum(ui32 counterIndex) const {
    Y_VERIFY(counterIndex < SumSimpleCounters.size(),
             "inconsistent sum simple counters, %u >= %lu", counterIndex, SumSimpleCounters.size());
    return *SumSimpleCounters[counterIndex];
}

void TAggregatedSimpleCounters::SetSum(ui32 counterIndex, ui64 value) {
    Y_VERIFY(counterIndex < SumSimpleCounters.size(),
             "inconsistent sum simple counters, %u >= %lu", counterIndex, SumSimpleCounters.size());
    *SumSimpleCounters[counterIndex] = value;
}

ui64 TAggregatedSimpleCounters::GetMax(ui32 counterIndex) const {
    Y_VERIFY(counterIndex < MaxSimpleCounters.size(),
             "inconsistent max simple counters, %u >= %lu", counterIndex, MaxSimpleCounters.size());
    return *MaxSimpleCounters[counterIndex];
}

void TAggregatedSimpleCounters::SetMax(ui32 counterIndex, ui64 value) {
    Y_VERIFY(counterIndex < MaxSimpleCounters.size(),
             "inconsistent max simple counters, %u >= %lu", counterIndex, MaxSimpleCounters.size());
    *MaxSimpleCounters[counterIndex] = value;
}

void TAggregatedSimpleCounters::SetValue(ui64 tabletID, ui32 counterIndex, ui64 value, NKikimrTabletBase::TTabletTypes::EType tabletType) {
    Y_VERIFY(counterIndex < CountersByTabletID.size(),
             "inconsistent counters for tablet type %s", TTabletTypes::TypeToStr(tabletType));
    TCountersByTabletIDMap::insert_ctx insertCtx;
    auto it = CountersByTabletID[counterIndex].find(tabletID, insertCtx);
    if (it != CountersByTabletID[counterIndex].end()) {
        if (it->second != value) {
            ChangedCounters[counterIndex] = true;
            it->second = value;
        }
    } else {
        CountersByTabletID[counterIndex].insert_direct(std::make_pair(tabletID, value), insertCtx);
        ChangedCounters[counterIndex] = true;
    }
}

void TAggregatedSimpleCounters::ForgetTablet(ui64 tabletId) {
    for (ui32 idx : xrange(CountersByTabletID.size())) {
        auto &counters = CountersByTabletID[idx];
        if (counters.erase(tabletId) != 0)
            ChangedCounters[idx] = true;
    }
}

void TAggregatedSimpleCounters::RecalcAll() {
    for (ui32 idx : xrange(CountersByTabletID.size())) {
        if (ChangedCounters[idx])
            Recalc(idx);
        ChangedCounters[idx] = false;
    }
}

void TAggregatedSimpleCounters::Recalc(ui32 idx) {
    auto &counters = CountersByTabletID[idx];
    THistogramCounter* histCounter = HistSimpleCounters[idx].Get();

    ui64 maxVal = 0;
    ui64 sumVal = 0;

    if (histCounter) {
        histCounter->Clear();
    }

    for (auto&& t : counters) {
        ui64 tValue = t.second;
        maxVal = Max(tValue, maxVal);
        sumVal += tValue;
        if (histCounter) {
            histCounter->IncrementFor(tValue);
        }
    }

    *MaxSimpleCounters[idx].Get() = maxVal;
    *SumSimpleCounters[idx].Get() = sumVal;
}

/*
** class TAggregatedCumulativeCounters
 */
TAggregatedCumulativeCounters::TAggregatedCumulativeCounters(::NMonitoring::TDynamicCounterPtr counterGroup)
    : CounterGroup(counterGroup)
{}

void TAggregatedCumulativeCounters::Reserve(size_t hint) {
    CountersByTabletID.reserve(hint);
    ChangedCounters.reserve(hint);
    MaxCumulativeCounters.reserve(hint);
}

void TAggregatedCumulativeCounters::AddCumulativeCounter(
    const char* name, THolder<THistogramCounter> percentileAggregate) {
    auto fnAddCounter = [this](const char* name, TCountersVector& container) {
        auto counter = CounterGroup->GetCounter(name, false);
        container.push_back(counter);
    };

    CountersByTabletID.push_back(TCountersByTabletIDMap());
    ChangedCounters.push_back(true);
    TString maxName = Sprintf("MAX(%s)", name);

    fnAddCounter(maxName.data(), MaxCumulativeCounters);

    HistCumulativeCounters.emplace_back(std::move(percentileAggregate));
}

ui64 TAggregatedCumulativeCounters::GetMax(ui32 counterIndex) const {
    Y_VERIFY(counterIndex < MaxCumulativeCounters.size(),
             "inconsistent max cumulative counters, %u >= %lu", counterIndex, MaxCumulativeCounters.size());
    return *MaxCumulativeCounters[counterIndex];
}

void TAggregatedCumulativeCounters::SetMax(ui32 counterIndex, ui64 value) {
    Y_VERIFY(counterIndex < MaxCumulativeCounters.size(),
             "inconsistent max cumulative counters, %u >= %lu", counterIndex, MaxCumulativeCounters.size());
    *MaxCumulativeCounters[counterIndex] = value;
}

void TAggregatedCumulativeCounters::SetValue(ui64 tabletID, ui32 counterIndex, ui64 value,
                                             NKikimrTabletBase::TTabletTypes::EType tabletType) {
    Y_VERIFY(counterIndex < CountersByTabletID.size(), "inconsistent counters for tablet type %s", TTabletTypes::TypeToStr(tabletType));
    TCountersByTabletIDMap::insert_ctx insertCtx;
    auto it = CountersByTabletID[counterIndex].find(tabletID, insertCtx);
    if (it != CountersByTabletID[counterIndex].end()) {
        if (it->second != value) {
            ChangedCounters[counterIndex] = true;
            it->second = value;
        }
    } else {
        CountersByTabletID[counterIndex].insert_direct(std::make_pair(tabletID, value), insertCtx);
        ChangedCounters[counterIndex] = true;
    }
}

void TAggregatedCumulativeCounters::ForgetTablet(ui64 tabletId) {
    for (ui32 idx : xrange(CountersByTabletID.size())) {
        auto &counters = CountersByTabletID[idx];
        if (counters.erase(tabletId) != 0)
            ChangedCounters[idx] = true;
    }
}

void TAggregatedCumulativeCounters::RecalcAll() {
    for (ui32 idx : xrange(CountersByTabletID.size())) {
        if (ChangedCounters[idx])
            Recalc(idx);
        ChangedCounters[idx] = false;
    }
}

void TAggregatedCumulativeCounters::Recalc(ui32 idx) {
    auto &counters = CountersByTabletID[idx];
    THistogramCounter* histCounter = HistCumulativeCounters[idx].Get();

    ui64 maxVal = 0;

    if (histCounter) {
        histCounter->Clear();
    }

    for (auto&& t : counters) {
        ui64 tValue = t.second;
        maxVal = Max(tValue, maxVal);
        if (histCounter) {
            histCounter->IncrementFor(tValue);
        }
    }

    *MaxCumulativeCounters[idx].Get() = maxVal;
}

/*
** class TAggregatedHistogramCounters
 */

TAggregatedHistogramCounters::TAggregatedHistogramCounters(::NMonitoring::TDynamicCounterPtr counterGroup)
    : CounterGroup(counterGroup)
{}

void TAggregatedHistogramCounters::Reserve(size_t hint) {
    PercentileCounters.reserve(hint);
    Histograms.reserve(hint);
    IsDerivative.reserve(hint);
    BucketBounds.reserve(hint);
    CountersByTabletID.reserve(hint);
}

void TAggregatedHistogramCounters::AddCounter(
    const char* name,
    const NKikimr::TTabletPercentileCounter& percentileCounter,
    THashMap<TString, THolder<THistogramCounter>>& histogramAggregates) {
    // old style
    PercentileCounters.push_back(TCountersVector());
    auto& rangeCounters = PercentileCounters.back();

    TStringBuf counterName(name);
    TStringBuf simpleCounterName = GetHistogramAggregateSimpleName(counterName);
    bool histogramAggregate = !simpleCounterName.empty();
    bool isDerivative = !histogramAggregate && !percentileCounter.GetIntegral();
    IsDerivative.push_back(isDerivative);

    auto rangeCount = percentileCounter.GetRangeCount();
    Y_VERIFY_DEBUG(rangeCount > 0);

    for (ui32 r = 0; r < rangeCount; ++r) {
        const char* rangeName = percentileCounter.GetRangeName(r);
        auto subgroup = CounterGroup->GetSubgroup("range", rangeName);
        auto counter = subgroup->GetCounter(name, isDerivative);
        rangeCounters.push_back(counter);
    }

    // new style
    // note that inf bucket in histogram description is implicit
    NMonitoring::TBucketBounds bucketBounds;
    bucketBounds.reserve(rangeCount);
    for (auto i: xrange(rangeCount - 1)) {
        bucketBounds.push_back(percentileCounter.GetRangeBound(i));
    }

    auto histogram = CounterGroup->GetHistogram(
        name, NMonitoring::ExplicitHistogram(bucketBounds), isDerivative);

    if (histogramAggregate) {
        // either simple or cumulative aggregate will handle this histogram,
        // it is a special case for hists name HIST(name), which have corresponding
        // simple or cumulative counter updated by tablet (tablet doesn't update hist itself,
        // hist is updated here by aggregated values)
        histogramAggregates.emplace(simpleCounterName, new THistogramCounter(
            percentileCounter.GetRanges(), std::move(rangeCounters), histogram));

        // we need this hack to access PercentileCounters by index easily skipping
        // hists we moved to simple/cumulative aggregates
        TCountersVector().swap(rangeCounters);
        BucketBounds.emplace_back();
    } else {
        // now save inf bound (note that in Percentile it is ui64, in Hist - double)
        bucketBounds.push_back(Max<NMonitoring::TBucketBound>());
        BucketBounds.emplace_back(std::move(bucketBounds));
    }

    // note that in case of histogramAggregate it will contain reference
    // on the histogram updated outside
    Histograms.push_back(histogram);

    CountersByTabletID.emplace_back(TCountersByTabletIDMap());
}

void TAggregatedHistogramCounters::SetValue(
    ui64 tabletID,
    ui32 counterIndex,
    const NKikimr::TTabletPercentileCounter& percentileCounter,
    const char* name,
    NKikimrTabletBase::TTabletTypes::EType tabletType) {
    Y_VERIFY(counterIndex < CountersByTabletID.size(),
             "inconsistent counters for tablet type %s, counter %s",
             TTabletTypes::TypeToStr(tabletType),
             name);

    Y_VERIFY(counterIndex < PercentileCounters.size(),
             "inconsistent counters for tablet type %s, counter %s",
             TTabletTypes::TypeToStr(tabletType),
             name);

    auto& percentileRanges = PercentileCounters[counterIndex];

    // see comment in AddCounter() related to histogramAggregate
    if (percentileRanges.empty())
        return;

    // just sanity check, normally should not happen
    const auto rangeCount = percentileCounter.GetRangeCount();
    if (rangeCount == 0)
        return;

    Y_VERIFY(rangeCount <= percentileRanges.size(),
             "inconsistent counters for tablet type %s, counter %s",
             TTabletTypes::TypeToStr(tabletType),
             name);

    if (IsDerivative[counterIndex]) {
        AddValues(counterIndex, percentileCounter);
        return;
    }

    // integral histogram

    TValuesVec newValues;
    newValues.reserve(rangeCount);
    for (auto i: xrange(rangeCount))
        newValues.push_back(percentileCounter.GetRangeValue(i));

    TCountersByTabletIDMap::insert_ctx insertCtx;
    auto it = CountersByTabletID[counterIndex].find(tabletID, insertCtx);
    if (it != CountersByTabletID[counterIndex].end()) {
        auto& oldValues = it->second;
        if (newValues != oldValues) {
            SubValues(counterIndex, oldValues);
            AddValues(counterIndex, newValues);
        }
        oldValues.swap(newValues);
    } else {
        AddValues(counterIndex, newValues);
        CountersByTabletID[counterIndex].insert_direct(std::make_pair(tabletID, std::move(newValues)), insertCtx);
    }
}

void TAggregatedHistogramCounters::ForgetTablet(ui64 tabletId) {
    for (auto idx : xrange(CountersByTabletID.size())) {
        auto &tabletToCounters = CountersByTabletID[idx];
        auto it = tabletToCounters.find(tabletId);
        if (it == tabletToCounters.end())
            continue;

        auto values = std::move(it->second);
        tabletToCounters.erase(it);

        if (IsDerivative[idx])
            continue;

        SubValues(idx, values);
    }
}

NMonitoring::THistogramPtr TAggregatedHistogramCounters::GetHistogram(size_t i) {
    Y_VERIFY(i < Histograms.size());
    return Histograms[i];
}

void TAggregatedHistogramCounters::SubValues(
    size_t counterIndex, const TAggregatedHistogramCounters::TValuesVec& values) {
    auto& percentileRanges = PercentileCounters[counterIndex];
    auto& histogram = Histograms[counterIndex];
    auto snapshot = histogram->Snapshot();
    histogram->Reset();
    for (auto i: xrange(values.size())) {
        Y_VERIFY_DEBUG(static_cast<ui64>(*percentileRanges[i]) >= values[i]);
        *percentileRanges[i] -= values[i];

        ui64 oldValue = snapshot->Value(i);
        ui64 negValue = 0UL - values[i];
        ui64 newValue = oldValue + negValue;
        histogram->Collect(BucketBounds[counterIndex][i], newValue);
    }
}

void TAggregatedHistogramCounters::AddValues(
    size_t counterIndex, const TAggregatedHistogramCounters::TValuesVec& values) {
    auto& percentileRanges = PercentileCounters[counterIndex];
    auto& histogram = Histograms[counterIndex];
    for (auto i: xrange(values.size())) {
        *percentileRanges[i] += values[i];
        histogram->Collect(BucketBounds[counterIndex][i], values[i]);
    }
}

void TAggregatedHistogramCounters::AddValues(
    size_t counterIndex, const NKikimr::TTabletPercentileCounter& percentileCounter) {
    auto& percentileRanges = PercentileCounters[counterIndex];
    auto& histogram = Histograms[counterIndex];
    for (auto i: xrange(percentileCounter.GetRangeCount())) {
        auto value = percentileCounter.GetRangeValue(i);
        *percentileRanges[i] += value;
        histogram->Collect(BucketBounds[counterIndex][i], value);
    }
}

/*
** class TAggregatedLabeledCounters
 */

TAggregatedLabeledCounters::TAggregatedLabeledCounters(
    ui32 count,
    const ui8* aggrFunc,
    const char * const * names,
    const ui8* types,
    const TString& groupNames)
    : AggrFunc(aggrFunc)
    , Names(names)
    , GroupNames(groupNames)
    , Types(types)
    , AggrCounters(count, 0)
    , Ids(count, 0)
    , Changed(false)
    , CountersByTabletID(count)
{
}

void TAggregatedLabeledCounters::SetValue(ui64 tabletID, ui32 counterIndex, ui64 value, ui64 id) {
    CountersByTabletID[counterIndex][tabletID] = std::make_pair(value, id);
    Changed = true;
}

bool TAggregatedLabeledCounters::ForgetTablet(ui64 tabletId) {
    for (ui32 idx : xrange(CountersByTabletID.size())) {
        auto &counters = CountersByTabletID[idx];
        counters.erase(tabletId);
    }
    Changed = true;
    return CountersByTabletID.size() == 0 || CountersByTabletID[0].size() == 0;
}

ui32 TAggregatedLabeledCounters::Size() const {
    return AggrCounters.size();
}

ui64 TAggregatedLabeledCounters::GetValue(ui32 index) const {
    return AggrCounters[index];
}

ui64 TAggregatedLabeledCounters::GetId(ui32 index) const {
    return Ids[index];
}

void TAggregatedLabeledCounters::FillGetRequestV1(
    NKikimrLabeledCounters::TTabletLabeledCounters* labeledCounters,
    const TString& group, ui32 start, ui32 end) const {
    if (Changed) {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            Recalc(idx);
        }
        Changed = false;
    }
    Y_VERIFY(end >= start);
    Y_VERIFY(end <= Size());
    labeledCounters->SetGroupNames(GroupNames);
    labeledCounters->SetGroup(group);
    labeledCounters->SetDelimiter("/"); //TODO: change here to "|"
    for (ui32 i = start; i < end; ++i) {
        auto& labeledCounter = *labeledCounters->AddLabeledCounter();
        labeledCounter.SetValue(GetValue(i));
        labeledCounter.SetId(GetId(i));
        labeledCounter.SetName(Names[i]);
        labeledCounter.SetAggregateFunc(NKikimr::TLabeledCounterOptions::EAggregateFunc(AggrFunc[i]));
        labeledCounter.SetType(NKikimr::TLabeledCounterOptions::ECounterType(Types[i]));
    }
}

void TAggregatedLabeledCounters::FillGetRequestV2(
    NKikimr::TTabletLabeledCountersResponseContext* context, const TString& group) const {
    if (Changed) {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            Recalc(idx);
        }
        Changed = false;
    }
    auto& labeledCounters = *context->Response.AddLabeledCountersByGroup();
    labeledCounters.SetGroup(group);
    labeledCounters.SetDelimiter("/"); // TODO: change here to "|"
    for (ui32 i = 0; i < Size(); ++i) {
        auto& labeledCounter = *labeledCounters.AddLabeledCounter();
        labeledCounter.SetValue(GetValue(i));
        labeledCounter.SetNameId(context->GetNameId(Names[i]));
        labeledCounter.SetAggregateFunc(NKikimr::TLabeledCounterOptions::EAggregateFunc(AggrFunc[i]));
        labeledCounter.SetType(NKikimr::TLabeledCounterOptions::ECounterType(Types[i]));
    }
}

void TAggregatedLabeledCounters::ToProto(NKikimrLabeledCounters::TTabletLabeledCounters& labeledCounters) const {
    if (Changed) {
        for (ui32 idx : xrange(CountersByTabletID.size())) {
            Recalc(idx);
        }
        Changed = false;
    }
    ui32 updatedCount{0};
    for (ui32 i = 0; i < Size(); ++i) {
        if (strlen(Names[i]) != 0) {
            if (labeledCounters.LabeledCounterSize() <= updatedCount) {
                labeledCounters.AddLabeledCounter();
            }
            auto& labeledCounter = *labeledCounters.MutableLabeledCounter(updatedCount);
            labeledCounter.SetValue(GetValue(i));
            labeledCounter.SetNameId(i);
            labeledCounter.SetAggregateFunc(NKikimr::TLabeledCounterOptions::EAggregateFunc(AggrFunc[i]));
            labeledCounter.SetType(NKikimr::TLabeledCounterOptions::ECounterType(Types[i]));
            ++updatedCount;
        }
    }
}

void TAggregatedLabeledCounters::FromProto(
    NMonitoring::TDynamicCounterPtr group,
    const NKikimrLabeledCounters::TTabletLabeledCounters& labeledCounters) const {
    for (const auto& counter : labeledCounters.GetLabeledCounter()) {
        const ui32 nameId{counter.GetNameId()};
        if (strlen(Names[nameId]) != 0) {
            // TODO: ASDFGS if CT_TIMELAG -> ctx.Now() - counters.GetValue
            const bool derived = counter.GetType() == TLabeledCounterOptions::CT_DERIV;
            auto namedCounter = group->GetNamedCounter("name", Names[nameId], derived);
            *namedCounter = counter.GetValue();
        }
    }
}

void TAggregatedLabeledCounters::Recalc(ui32 idx) const {
    Y_VERIFY(idx < Ids.size());
    auto &counters = CountersByTabletID[idx];
    TTabletLabeledCountersBase::EAggregateFunc aggrFunc{AggrFunc[idx]};
    std::pair<ui64, ui64> aggrVal{0,0};
    ui64 cntCount = counters.size();

    // Y_VERIFY(cntCount > 0);
    if (cntCount == 0) {
        return;
    }

    if (aggrFunc == TTabletLabeledCountersBase::EAggregateFunc::EAF_MIN)
        aggrVal = counters.begin()->second;

    for (auto&& t : counters) {
        const std::pair<ui64, ui64>& tValue = t.second;
        switch (aggrFunc) {
            case TTabletLabeledCountersBase::EAggregateFunc::EAF_MIN:
                aggrVal = Min(tValue, aggrVal);
                break;
            case TTabletLabeledCountersBase::EAggregateFunc::EAF_MAX:
                aggrVal = Max(tValue, aggrVal);
                break;
            case TTabletLabeledCountersBase::EAggregateFunc::EAF_SUM:
                aggrVal.first += tValue.first;
                break;
            default:
                Y_FAIL("bad aggrFunc value");
        };
    }

    AggrCounters[idx] = aggrVal.first;
    Ids[idx] = aggrVal.second;
}


} // namespace NKikimr::NPrivate
