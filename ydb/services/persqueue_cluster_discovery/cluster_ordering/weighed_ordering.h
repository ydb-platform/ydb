#pragma once

#include <algorithm>
#include <type_traits>
#include <vector>

#include <util/system/types.h>
#include <util/system/yassert.h>

namespace NKikimr::NPQ::NClusterDiscovery::NClusterOrdering {

template <typename Iterator, typename GetWeightFunc>
Iterator SelectByHashAndWeight(Iterator begin, Iterator end, const ui64 hashValue, const GetWeightFunc getWeight) {
    if (std::distance(begin, end) < 2) {
        return begin;
    }

    static_assert(std::is_integral<decltype(getWeight(*begin))>::value);

    std::vector<std::pair<const ui64, const Iterator>> borders;
    borders.reserve(std::distance(begin, end));

    ui64 total = 0;
    for (auto it = begin; it != end; ++it) {
        total += getWeight(*it);
        borders.emplace_back(total, it);
    }

    Y_ABORT_UNLESS(total);
    const ui64 borderToFind = hashValue % total + 1;

    const auto bordersIt = lower_bound(borders.begin(), borders.end(), borderToFind,
                                       [](const auto& borderInfo, const ui64& toFind) { return borderInfo.first < toFind; });

    return bordersIt->second;
}

template <typename Iterator, typename GetWeightFunc>
void OrderByHashAndWeight(Iterator begin, Iterator end, const ui64 hashValue, const GetWeightFunc getWeight) {
    auto currentBegin = begin;
    while (std::distance(currentBegin, end) > 1) {
        auto selectedIt = SelectByHashAndWeight(currentBegin, end, hashValue, getWeight);
        if (currentBegin != selectedIt) {
            std::swap(*currentBegin, *selectedIt);
        }
        ++currentBegin;
    }
}

} // namespace NKikimr::NPQ::NClusterDiscovery::NClusterOrdering
