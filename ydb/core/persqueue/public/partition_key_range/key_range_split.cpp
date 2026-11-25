#include "key_range_split.h"

#include <ydb/core/scheme/scheme_borders.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/kll_median/sketch.h>

#include <optional>

namespace NKikimr::NPQ {

namespace {

bool HasValidCells(const TSerializedCellVec& key, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    const auto cells = key.GetCells();
    if (cells.size() > keyTypes.size()) {
        return false;
    }
    for (size_t i = 0; i < cells.size(); ++i) {
        if (NScheme::HasUnexpectedValueSize(cells[i], keyTypes[i])) {
            return false;
        }
    }
    return true;
}

bool IsFullKey(const TSerializedCellVec& key, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return key.GetCells().size() == keyTypes.size() && HasValidCells(key, keyTypes);
}

bool IsValidKeyRange(const TPartitionKeyRange& range, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return (!range.FromBound || HasValidCells(*range.FromBound, keyTypes))
        && (!range.ToBound || HasValidCells(*range.ToBound, keyTypes));
}

int CompareBounds(const TSerializedCellVec& lhs, const TSerializedCellVec& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return ComparePrefixBorders(keyTypes,
        lhs.GetCells(), PrefixModeLeftBorderInclusive,
        rhs.GetCells(), PrefixModeLeftBorderInclusive);
}

bool IsInside(const TSerializedCellVec& key, const TPartitionKeyRange& range, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return (!range.FromBound || CompareBounds(key, *range.FromBound, keyTypes) >= 0)
        && (!range.ToBound || CompareBounds(key, *range.ToBound, keyTypes) < 0);
}

bool IsStrictlyInside(const TSerializedCellVec& boundary, const TPartitionKeyRange& range, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return (!range.FromBound || CompareBounds(boundary, *range.FromBound, keyTypes) > 0)
        && (!range.ToBound || CompareBounds(boundary, *range.ToBound, keyTypes) < 0);
}

TSerializedCellVec MakeFullKey(const TSerializedCellVec& bound, size_t keySize) {
    TVector<TCell> cells(bound.GetCells().begin(), bound.GetCells().end());
    cells.resize(keySize);
    return TSerializedCellVec(TSerializedCellVec::Serialize(cells));
}

struct TTypedKey {
    TSerializedCellVec Key;
    TConstArrayRef<NScheme::TTypeInfo> KeyTypes;

    friend bool operator<(const TTypedKey& lhs, const TTypedKey& rhs) {
        return CompareBounds(lhs.Key, rhs.Key, lhs.KeyTypes) < 0;
    }
};

using TWeightedKeys = TVector<std::pair<TTypedKey, ui64>>;

struct TCandidate {
    TSerializedCellVec Boundary;
    bool SourceBoundary = false;
};

TWeightedKeys GetPartitionKeys(const TPartitionKeyRange& parent, TConstArrayRef<NScheme::TTypeInfo> keyTypes,
    TConstArrayRef<TKeyRangeKeyStats> keys)
{
    TWeightedKeys result;
    for (const auto& key : keys) {
        if (key.WrittenBytes > 0 && IsFullKey(key.Key, keyTypes) && IsInside(key.Key, parent, keyTypes)) {
            result.emplace_back(TTypedKey{key.Key, keyTypes}, key.WrittenBytes);
        }
    }
    return result;
}

ui64 GetBytesBefore(const TWeightedKeys& keys, const TSerializedCellVec& boundary, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return NKll::GetRank(keys, TTypedKey{boundary, keyTypes});
}

TVector<TCandidate> GetCandidates(TWeightedKeys& keys, TConstArrayRef<TKeyRangeSource> sources, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    TVector<TCandidate> candidates;

    const auto median = NKll::GetQuantile(keys, 0.5);
    candidates.push_back({median.Key, false});
    for (const auto& [key, _] : keys) {
        if (median < key) {
            candidates.push_back({key.Key, false});
            break;
        }
    }

    for (const auto& source : sources) {
        if (!IsValidKeyRange(source.KeyRange, keyTypes)) {
            continue;
        }
        for (const auto* bound : {source.KeyRange.FromBound.Get(), source.KeyRange.ToBound.Get()}) {
            if (bound) {
                candidates.push_back({MakeFullKey(*bound, keyTypes.size()), true});
            }
        }
    }

    return candidates;
}

std::optional<TSerializedCellVec> ChooseBoundary(const TPartitionKeyRange& parent, TConstArrayRef<NScheme::TTypeInfo> keyTypes,
    TConstArrayRef<TKeyRangeSource> sources, TWeightedKeys& keys, ui64 totalBytes)
{
    std::optional<TCandidate> best;
    ui64 bestHeavierPart = 0;
    for (auto& candidate : GetCandidates(keys, sources, keyTypes)) {
        if (!IsStrictlyInside(candidate.Boundary, parent, keyTypes)) {
            continue;
        }
        const ui64 leftBytes = GetBytesBefore(keys, candidate.Boundary, keyTypes);
        if (!leftBytes || leftBytes == totalBytes) {
            continue;
        }
        const ui64 heavierPart = Max(leftBytes, totalBytes - leftBytes);
        if (!best || heavierPart < bestHeavierPart
            || (heavierPart == bestHeavierPart && candidate.SourceBoundary && !best->SourceBoundary))
        {
            bestHeavierPart = heavierPart;
            best = std::move(candidate);
        }
    }
    if (!best) {
        return std::nullopt;
    }
    return std::move(best->Boundary);
}

} // anonymous

TVector<TPlannedPartition> PlanKeyRangeSplit(
    const TPartitionKeyRange& parent,
    TConstArrayRef<NScheme::TTypeInfo> keyTypes,
    const TKeyRangeWriteStats& stats)
{
    if (!IsValidKeyRange(parent, keyTypes)) {
        return {};
    }

    auto keys = GetPartitionKeys(parent, keyTypes, stats.Keys);
    if (keys.empty()) {
        return {};
    }

    ui64 totalBytes = 0;
    for (const auto& [_, bytes] : keys) {
        totalBytes += bytes;
    }
    const auto boundary = ChooseBoundary(parent, keyTypes, stats.Sources, keys, totalBytes);
    if (!boundary) {
        return {};
    }

    TVector<TPlannedPartition> plan(2);
    plan[0].Range.FromBound = parent.FromBound;
    plan[0].Range.ToBound = *boundary;
    plan[0].EstimatedBytes = GetBytesBefore(keys, *boundary, keyTypes);
    plan[1].Range.FromBound = *boundary;
    plan[1].Range.ToBound = parent.ToBound;
    plan[1].EstimatedBytes = totalBytes - plan[0].EstimatedBytes;

    for (auto& partition : plan) {
        for (const auto& source : stats.Sources) {
            if (Intersects(source.KeyRange, partition.Range, keyTypes)) {
                partition.Sources.push_back(source);
            }
        }
    }

    return plan;
}

TKeyRangeKeysCollector::TKeyRangeKeysCollector(TVector<NScheme::TTypeInfo> keyTypes, size_t numSketches, TDuration windowSize)
    : KeyTypes(std::move(keyTypes))
    , WindowSize(windowSize)
    , Rng(std::random_device{}())
{
    AFL_ENSURE(numSketches > 0)("reason", "numSketches must be greater than 0");
    SketchWindowSize = Max(Min(TDuration::Seconds(1), windowSize), windowSize / numSketches);
}

bool TKeyRangeKeysCollector::Add(const TString& key, ui64 bytes, TInstant now) {
    if (key.empty() || key.size() > MaxKeySize || !bytes) {
        return false;
    }

    TSerializedCellVec cells;
    if (!TSerializedCellVec::TryParse(key, cells) || cells.GetCells().size() != KeyTypes.size()) {
        return false;
    }
    for (size_t i = 0; i < KeyTypes.size(); ++i) {
        if (NScheme::HasUnexpectedValueSize(cells.GetCells()[i], KeyTypes[i])) {
            return false;
        }
    }

    RemoveOldSketches(now);
    EnsureSketch(now);
    Sketches.back().Sketch.Add(TKey{std::move(cells), KeyTypes}, bytes);
    return true;
}

TVector<TKeyRangeKeyStats> TKeyRangeKeysCollector::GetKeys(TInstant now) {
    RemoveOldSketches(now);

    TVector<TKeyRangeKeyStats> keys;
    for (const auto& sketch : Sketches) {
        for (const auto& level : sketch.Sketch.GetLevels()) {
            for (const auto& item : level.Items) {
                keys.push_back({.Key = item.Cells, .WrittenBytes = level.Weight});
            }
        }
    }
    return keys;
}

void TKeyRangeKeysCollector::RemoveOldSketches(TInstant now) {
    while (!Sketches.empty() && Sketches.front().StartTime + WindowSize < now) {
        Sketches.pop_front();
    }
}

void TKeyRangeKeysCollector::EnsureSketch(TInstant now) {
    if (Sketches.empty() || Sketches.back().StartTime + SketchWindowSize <= now) {
        Sketches.push_back(TSketch{
            .Sketch = NKll::TDynamicKllSketch<TKey>(SketchLevelSize, Rng(), MinWeight),
            .StartTime = now,
        });
    }
}

} // namespace NKikimr::NPQ
