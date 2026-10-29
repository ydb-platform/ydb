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
    std::optional<NArrow::NMerger::TSortableBatchPosition> From;

    TLoadedKeyRanges(const EState state, std::optional<NArrow::NMerger::TSortableBatchPosition>&& from)
        : State(state)
        , From(std::move(from))
    {
    }

public:
    static TLoadedKeyRanges BuildNothing() {
        return TLoadedKeyRanges(EState::Nothing, std::nullopt);
    }

    static TLoadedKeyRanges BuildFrom(const NArrow::TSimpleRow& from) {
        return TLoadedKeyRanges(EState::From, from.BuildSortablePosition());
    }

    static TLoadedKeyRanges BuildEverything() {
        return TLoadedKeyRanges(EState::Everything, std::nullopt);
    }

    bool IsEverythingLoaded() const {
        return State == EState::Everything;
    }

    // Whether every row the ranges select lies in a portion that is in memory.
    bool Covers(const TPKRangesFilter& ranges) const;
};

}   // namespace NKikimr::NOlap
