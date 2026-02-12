#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include "split.h"

namespace NKikimr::NSplitMerge {

// Converts a sample with unsorted, repeated keys with unit (in practice) weights into
// a histogram: sorted and deduplicated array with accumulated weights.
//
//NOTE: This is an idempotent operation that maintains backward compatibility. Code that
// previously worked with raw samples will continue working with samples converted by this function.
// This wouldn't be true if MakeKeyAccessHistogram produced cumulative histograms.
void MakeKeyAccessHistogram(TKeyAccessHistogram &keysHist, const TKeyTypes& keyColumnTypes) {
    // compare histogram entries by keys
    auto fnCmp = [&keyColumnTypes] (const auto& entry1, const auto& entry2) {
        const auto& key1cells = entry1.first.GetCells();
        const auto& key2cells = entry2.first.GetCells();
        const auto minKeySize = std::min(key1cells.size(), key2cells.size());
        int cmp = CompareTypedCellVectors(key1cells.data(), key2cells.data(), keyColumnTypes.data(), minKeySize);
        if (cmp == 0 && key1cells.size() != key2cells.size()) {
            // smaller key is filled with +inf => always bigger
            cmp = (key1cells.size() < key2cells.size()) ? +1 : -1;
        }
        return cmp;
    };

    Sort(keysHist, [&fnCmp] (const auto& key1, const auto& key2) { return fnCmp(key1, key2) < 0; });

    // The keys are now sorted. Next we convert the stats into a histogram by accumulating
    // stats for all previous keys at each key.
    // (But not building a cumulative histogram.)
    size_t last = 0;
    for (size_t i = 1; i < keysHist.size(); ++i) {
        if (fnCmp(keysHist[i], keysHist[last]) == 0) {
            // Merge equal keys
            keysHist[last].second += keysHist[i].second;
        } else {
            ++last;
            if (last != i) {
                keysHist[last] = keysHist[i];
            }
        }
    }
    keysHist.resize(std::min(keysHist.size(), last + 1));
}

// Converts a histogram (made by MakeKeyAccessHistogram) into a cumulative histogram.
void ConvertToCumulativeHistogram(TKeyAccessHistogram &keysHist) {
    for (size_t i = 1; i < keysHist.size(); ++i) {
        keysHist[i].second += keysHist[i-1].second;
    }
}

TSerializedCellVec FindSplitKeyPrefix(const TKeyAccessHistogram& keysHist, const TKeyTypes& keyColumnTypes, const size_t prefixSize) {
    ui64 total = keysHist.back().second;

    // Compares bucket value
    auto fnValueLess = [] (ui64 val, const auto& bucket) {
        return val < bucket.second;
    };

    // Find the position of total/2
    auto halfIt = std::upper_bound(keysHist.begin(), keysHist.end(), total*0.5, fnValueLess);
    auto loIt = std::upper_bound(keysHist.begin(), keysHist.end(), total*0.1, fnValueLess);
    auto hiIt = std::upper_bound(keysHist.begin(), keysHist.end(), total*0.9, fnValueLess);

    // compare histogram entries by key prefixes
    auto comparePrefix = [&keyColumnTypes] (const auto& entry1, const auto& entry2, const size_t prefixSize) {
        const auto& key1cells = entry1.first.GetCells();
        const auto clampedSize1 = std::min(key1cells.size(), prefixSize);

        const auto& key2cells = entry2.first.GetCells();
        const auto clampedSize2 = std::min(key2cells.size(), prefixSize);

        int cmp = CompareTypedCellVectors(key1cells.data(), key2cells.data(), keyColumnTypes.data(), std::min(clampedSize1, clampedSize2));
        if (cmp == 0 && clampedSize1 != clampedSize2) {
            // smaller key prefix is filled with +inf => always bigger
            cmp = (clampedSize1 < clampedSize2) ? +1 : -1;
        }
        return cmp;
    };

    // Check if half key is no equal to low and high keys
    if (comparePrefix(*halfIt, *loIt, prefixSize) == 0) {
        return TSerializedCellVec();
    }
    if (comparePrefix(*halfIt, *hiIt, prefixSize) == 0) {
        return TSerializedCellVec();
    }

    // Build split key by leaving the prefix and extending it with NULLs
    TVector<TCell> splitKey(halfIt->first.GetCells().begin(), halfIt->first.GetCells().end());
    splitKey.resize(prefixSize);
    splitKey.resize(keyColumnTypes.size());

    return TSerializedCellVec(splitKey);
}

//NOTE: requires histogram built by ConvertToCumulativeHistogram
TSerializedCellVec SelectShortestMedianKeyPrefix(const TKeyAccessHistogram& keysHist, const TKeyTypes& keyColumnTypes) {
    if (keysHist.size() < 2) {
        return TSerializedCellVec();
    }

    // Find the median key with the shortest prefix
    size_t minPrefix = 0;
    size_t maxPrefix = keyColumnTypes.size();

    // Binary search for shortest prefix that can be used to split the load
    TSerializedCellVec splitKey;
    while (minPrefix + 1 < maxPrefix) {
        size_t prefixSize = (minPrefix + maxPrefix + 1) / 2;
        splitKey = FindSplitKeyPrefix(keysHist, keyColumnTypes, prefixSize);
        if (splitKey.GetCells().empty()) {
            minPrefix = prefixSize;
        } else {
            maxPrefix = prefixSize;
        }
    }
    splitKey = FindSplitKeyPrefix(keysHist, keyColumnTypes, maxPrefix);

    return splitKey;
}

}  // namespace NKikimr::NSplitMerge
