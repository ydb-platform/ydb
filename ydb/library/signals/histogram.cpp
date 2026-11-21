#include "histogram.h"

namespace NKikimr::NColumnShard {

TIncrementalHistogram::TBuckets::TBuckets(const TString& moduleId, const TString& metricId, const TString& category,
    const std::vector<std::pair<i64, TString>>& borders, TIntrusivePtr<NMonitoring::TDynamicCounters> baseSignals)
    : TBase(moduleId, baseSignals)
{
    DeepSubGroup("metric", metricId);
    if (category) {
        DeepSubGroup("category", category);
    }
    std::optional<TString> predName;
    for (auto&& [border, name] : borders) {
        Borders.emplace_back(border);
        if (!predName) {
            Names.emplace_back("(-Inf," + name + "]");
        } else {
            Names.emplace_back("(" + *predName + "," + name + "]");
        }
        predName = name;
    }
    Y_ABORT_UNLESS(predName);
    Names.emplace_back("(" + *predName + ",+Inf)");
}

TIncrementalHistogram::TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::set<i64>& values,
    TIntrusivePtr<NMonitoring::TDynamicCounters> baseSignals)
{
    std::vector<std::pair<i64, TString>> borders;
    for (auto&& i : values) {
        borders.emplace_back(i, ::ToString(i));
    }
    Buckets = std::make_shared<TBuckets>(moduleId, metricId, category, borders, baseSignals);
}

TIncrementalHistogram::TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::map<i64, TString>& values)
{
    std::vector<std::pair<i64, TString>> borders(values.begin(), values.end());
    Buckets = std::make_shared<TBuckets>(moduleId, metricId, category, borders, nullptr);
}

TDeriviativeHistogram::TDeriviativeHistogram(const TString& moduleId, const TString& signalName, const TString& category, const std::set<i64>& values)
    : TBase(moduleId)
{
    if (category) {
        DeepSubGroup("category", category);
    }
    std::optional<TString> predName;
    for (auto&& i : values) {
        if (!predName) {
            Counters.emplace(i, CreateSubGroup("bin", "(-Inf," + ::ToString(i) + "]").GetDeriviative(signalName));
        } else {
            Counters.emplace(i, CreateSubGroup("bin", "(" + *predName + "," + ::ToString(i) + "]").GetDeriviative(signalName));
        }
        predName = ::ToString(i);
    }
    Y_ABORT_UNLESS(predName);
    PlusInf = CreateSubGroup("bin", "(" + *predName + ",+Inf)").GetDeriviative(signalName);
}

TDeriviativeHistogram::TDeriviativeHistogram(const TString& moduleId, const TString& signalName, const TString& category, const std::map<i64, TString>& values)
    : TBase(moduleId) 
{
    if (category) {
        DeepSubGroup("category", category);
    }
    std::optional<TString> predName;
    for (auto&& i : values) {
        if (!predName) {
            Counters.emplace(i.first, CreateSubGroup("bin", "(-Inf," + i.second + "]").GetDeriviative(signalName));
        } else {
            Counters.emplace(i.first, CreateSubGroup("bin", "(" + *predName + "," + i.second + "]").GetDeriviative(signalName));
        }
        predName = i.second;
    }
    Y_ABORT_UNLESS(predName);
    PlusInf = CreateSubGroup("bin", "(" + *predName + ",+Inf)").GetDeriviative(signalName);
}

}