#pragma once

#include <yql/essentials/sql/v1/complete/text/case.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/algorithm.h>

namespace NSQLComplete {

    struct TNameIndexEntry {
        TString Normalized;
        TString Original;
    };

    using TNameIndex = TVector<TNameIndexEntry>;

    inline bool NameIndexCompare(const TNameIndexEntry& lhs, const TNameIndexEntry& rhs) {
        return NoCaseCompare(lhs.Normalized, rhs.Normalized);
    }

    inline auto NameIndexCompareLimit(size_t limit) {
        return [cmp = NoCaseCompareLimit(limit)](const TNameIndexEntry& lhs, const TNameIndexEntry& rhs) {
            return cmp(lhs.Normalized, rhs.Normalized);
        };
    }

    TNameIndex BuildNameIndex(TVector<TString> originals, auto normalize) {
        TNameIndex index;
        for (auto& original : originals) {
            TNameIndexEntry entry = {
                .Normalized = normalize(original),
                .Original = std::move(original),
            };
            index.emplace_back(std::move(entry));
        }

        Sort(index, NameIndexCompare);
        return index;
    }

} // namespace NSQLComplete
