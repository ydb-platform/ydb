#include "name_set.h"

#include "name_index.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

TVector<TString> Pruned(TVector<TString> names, const THashMap<TString, size_t>& frequency) {
    THashMap<TString, TVector<std::tuple<TString, size_t>>> groups;

    for (auto& [normalized, original] : BuildNameIndex(std::move(names), NormalizeName)) {
        size_t freq = 0;
        if (const size_t* it = frequency.FindPtr(original)) {
            freq = *it;
        }
        groups[normalized].emplace_back(std::move(original), freq);
    }

    for (auto& [_, group] : groups) {
        Sort(group, [](const auto& lhs, const auto& rhs) {
            return std::get<1>(lhs) < std::get<1>(rhs);
        });
    }

    names = TVector<TString>();
    names.reserve(groups.size());
    for (auto& [_, group] : groups) {
        Y_ASSERT(!group.empty());
        names.emplace_back(std::move(std::get<0>(group.back())));
    }
    return names;
}

TNameSet Pruned(TNameSet names, const TFrequencyData& frequency) {
    names.Pragmas = Pruned(std::move(names.Pragmas), frequency.Pragmas);
    names.Types = Pruned(std::move(names.Types), frequency.Types);
    names.Functions = Pruned(std::move(names.Functions), frequency.Functions);
    for (auto& [k, h] : names.Hints) {
        h = Pruned(h, frequency.Hints);
    }
    return names;
}

std::function<bool(TStringBuf)> DefaultNameFilter() {
    return [](TStringBuf name) {
        const bool isUDAF =
            (name.StartsWith("AdaptiveDistanceHistogram::") ||
             name.StartsWith("AdaptiveWardHistogram::") ||
             name.StartsWith("AdaptiveWeightHistogram::") ||
             name.StartsWith("BlockWardHistogram::") ||
             name.StartsWith("BlockWeightHistogram::") ||
             name.StartsWith("LinearHistogram::") ||
             name.StartsWith("LogarithmicHistogram::") ||
             name.StartsWith("TDigest::") ||
             name.StartsWith("TopFreq::")) &&
            (name.EndsWith("_AddValue") ||
             name.EndsWith("_Create") ||
             name.EndsWith("_Deserialize") ||
             name.EndsWith("_GetPercentile") ||
             name.EndsWith("_GetResult") ||
             name.EndsWith("_Get") ||
             name.EndsWith("_Merge") ||
             name.EndsWith("_Serialize"));

        return !isUDAF;
    };
}

TVector<TString> Filtered(TVector<TString> names, std::function<bool(TStringBuf)> predicate) {
    EraseIf(names, std::not_fn(predicate));
    return names;
}

TNameSet Filtered(TNameSet names, std::function<bool(TStringBuf)> predicate) {
    names.Pragmas = Filtered(std::move(names.Pragmas), predicate);
    names.Types = Filtered(std::move(names.Types), predicate);
    names.Functions = Filtered(std::move(names.Functions), predicate);
    for (auto& [k, h] : names.Hints) {
        h = Filtered(std::move(h), predicate);
    }
    return names;
}

} // namespace NSQLComplete
