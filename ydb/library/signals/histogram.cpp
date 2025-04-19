#include "histogram.h"

namespace NKikimr::NColumnShard {

TIncrementalHistogram::TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::set<i64>& values)
    : TBase(moduleId)
{
    DeepSubGroup("metric", metricId);
    if (category) {
        DeepSubGroup("category", category);
    }
    std::optional<TString> predName;
    for (auto&& i : values) {
        if (!predName) {
            Counters.emplace(i, TBase::GetValue("(-Inf," + ::ToString(i) + "]"));
        } else {
            Counters.emplace(i, TBase::GetValue("(" + *predName + "," + ::ToString(i) + "]"));
        }
        predName = ::ToString(i);
    }
    Y_ABORT_UNLESS(predName);
    PlusInf = TBase::GetValue("(" + *predName + ",+Inf)");
}

TIncrementalHistogram::TIncrementalHistogram(const TString& moduleId, const TString& metricId, const TString& category, const std::map<i64, TString>& values)
    : TBase(moduleId)
{
    DeepSubGroup("metric", metricId);
    if (category) {
        DeepSubGroup("category", category);
    }
    std::optional<TString> predName;
    for (auto&& i : values) {
        if (!predName) {
            Counters.emplace(i.first, TBase::GetValue("(-Inf," + i.second + "]"));
        } else {
            Counters.emplace(i.first, TBase::GetValue("(" + *predName + "," + i.second + "]"));
        }
        predName = i.second;
    }
    Y_ABORT_UNLESS(predName);
    PlusInf = TBase::GetValue("(" + *predName + ",+Inf)");
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