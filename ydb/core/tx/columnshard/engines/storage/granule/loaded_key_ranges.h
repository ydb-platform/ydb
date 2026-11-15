#pragma once
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>

#include <optional>

namespace NKikimr::NOlap {

class TPKRangesFilter;

class TLoadedKeyRanges {
private:
    enum class ECoverage {
        None,
        FromKey,
        All,
    };

    ECoverage Coverage;
    // ECoverage::FromKey: every portion whose last key is >= this key is in memory
    std::optional<NArrow::TSimpleRow> FromKey;
    // FromKey, in the form the ranges of a scan compare with
    std::optional<NArrow::NMerger::TSortableBatchPosition> FromKeyPosition;

    explicit TLoadedKeyRanges(const ECoverage coverage)
        : Coverage(coverage)
    {
    }

    explicit TLoadedKeyRanges(const NArrow::TSimpleRow& from)
        : Coverage(ECoverage::FromKey)
        , FromKey(from)
        , FromKeyPosition(from.BuildSortablePosition())
    {
    }

public:
    static TLoadedKeyRanges BuildNothing() {
        return TLoadedKeyRanges(ECoverage::None);
    }

    static TLoadedKeyRanges BuildFrom(const NArrow::TSimpleRow& from) {
        return TLoadedKeyRanges(from);
    }

    static TLoadedKeyRanges BuildEverything() {
        return TLoadedKeyRanges(ECoverage::All);
    }

    bool CoversAll() const {
        return Coverage == ECoverage::All;
    }

    // Whether every row the ranges select lies in a portion that is in memory.
    bool Covers(const TPKRangesFilter& ranges) const;

    // Whether every row >= key lies in a portion that is in memory.
    bool CoversFrom(const NArrow::TSimpleRow& key) const;
};

}   // namespace NKikimr::NOlap
