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

} // namespace NSQLComplete
