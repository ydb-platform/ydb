#pragma once
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>

#include <optional>

namespace NKikimr::NOlap {

class TPKRangesFilter;

class TLoadedKeyRanges {
private:
    enum class EState {
        Nothing,
        From,
        Everything,
    };

    EState State;
    // EState::From: every portion whose last key is >= this key is in memory
    std::optional<NArrow::TSimpleRow> From;
    // From, in the form the ranges of a scan compare with
    std::optional<NArrow::NMerger::TSortableBatchPosition> FromPosition;

    explicit TLoadedKeyRanges(const EState state)
        : State(state)
    {
    }

    explicit TLoadedKeyRanges(const NArrow::TSimpleRow& from)
        : State(EState::From)
        , From(from)
        , FromPosition(from.BuildSortablePosition())
    {
    }

public:
    static TLoadedKeyRanges BuildNothing() {
        return TLoadedKeyRanges(EState::Nothing);
    }

    static TLoadedKeyRanges BuildFrom(const NArrow::TSimpleRow& from) {
        return TLoadedKeyRanges(from);
    }

    static TLoadedKeyRanges BuildEverything() {
        return TLoadedKeyRanges(EState::Everything);
    }

    bool IsEverythingLoaded() const {
        return State == EState::Everything;
    }

    // Whether every row the ranges select lies in a portion that is in memory.
    bool Covers(const TPKRangesFilter& ranges) const;

    // Whether every row >= key lies in a portion that is in memory.
    bool CoversFrom(const NArrow::TSimpleRow& key) const;
};

}   // namespace NKikimr::NOlap
