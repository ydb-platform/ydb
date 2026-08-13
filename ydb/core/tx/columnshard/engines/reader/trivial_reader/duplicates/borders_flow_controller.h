#pragma once

#include "common.h"
#include "merge.h"
#include "private_events.h"

#include <ydb/core/formats/arrow/reader/batch_iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/comparable.h>

#include <optional>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

class TBordersFlowController {
private:
    struct TBorderInfo {
        std::vector<ui64> Start;
        std::vector<ui64> Finish;
    };

    std::shared_ptr<TMergeContext> MergeContext;
    // Present when no merge task owns the progressive Merger/FiltersBuilder.
    std::optional<TMergeRuntimeState> IdleMergeState;
    std::map<NCommon::TReplaceKeyAdapter, TBorderInfo> Borders;
    std::set<NCommon::TReplaceKeyAdapter> WaitingBorders;
    std::set<NCommon::TReplaceKeyAdapter> ReadyBorders;
    std::unordered_set<ui64> ExclusivePortions;
    std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    TReadMetadataBase::TConstPtr ReadMetadata;
    std::deque<TEvBordersConstructionResult::TPtr> BordersQueue;
    bool IsInflight = false;

public:
    TBordersFlowController(const std::shared_ptr<TMergeContext>& mergeContext, TMergeRuntimeState&& mergeState,
        const std::deque<std::shared_ptr<TPortionInfo>>& portions, const TReadMetadataBase::TConstPtr& readMetadata,
        const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters);

    bool ExtractExclusiveInterval(const ui64 portionId);

    TBordersIterator Next(const std::shared_ptr<const TPortionInfo>& portion);

    TString DebugString() const;

    std::optional<NArrow::TSimpleRow> NextReadyBorder();

    bool IsReversed() const;

    void Enqueue(const TEvBordersConstructionResult::TPtr& event);

    // Drop queued merges only. Keep IsInflight if a merge task is already on the conveyor
    // so AbortAndPassAway cannot start another merge against the progressive merge state.
    void AbortPendingMerges();

    void OnReadyMergeBorders(const bool allowDrain = true);

    void ReturnMergeState(TMergeRuntimeState&& state);

    bool IsMergeInflight() const {
        return IsInflight;
    }

    ~TBordersFlowController();

private:
    void AddBatch(const TBordersBatch& batch);

    void DrainQueue();

    void BuildExclusivePortions();
};

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
