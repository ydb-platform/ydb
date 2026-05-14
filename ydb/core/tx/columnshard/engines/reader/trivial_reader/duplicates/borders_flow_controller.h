#pragma once

#include "common.h"
#include "private_events.h"
#include "merge.h"

#include <ydb/core/formats/arrow/reader/batch_iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/comparable.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

class TBordersFlowController {
private:
    struct TBorderInfo {
        std::vector<ui64> Start;
        std::vector<ui64> Finish;
    };
    std::shared_ptr<TMergeContext> MergeContext;
    std::map<NCommon::TReplaceKeyAdapter, TBorderInfo> Borders;
    std::set<NCommon::TReplaceKeyAdapter> WaitingBorders;
    std::set<NCommon::TReplaceKeyAdapter> ReadyBorders;
    std::unordered_set<ui64> ExclusivePortions;
    std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    TReadMetadataBase::TConstPtr ReadMetadata;
    std::deque<TEvBordersConstructionResult::TPtr> BordersQueue;
    bool IsInflight = false;

public:
    TBordersFlowController(const std::shared_ptr<TMergeContext>& mergeContext, const std::deque<std::shared_ptr<TPortionInfo>>& portions, const TReadMetadataBase::TConstPtr& readMetadata, const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters);

    bool ExtractExclusiveInterval(const ui64 portionId);

    TBordersIterator Next(const std::shared_ptr<const TPortionInfo>& portion);

    TString DebugString() const;

    std::optional<NArrow::TSimpleRow> NextReadyBorder();

    bool IsReversed() const;

    void OnReadyMergeBorders();

    void Enqueue(const TEvBordersConstructionResult::TPtr& event);

    ~TBordersFlowController();

private:
    void AddBatch(const TBordersBatch& batch);

    void DrainQueue();

    void BuildExclusivePortions();
};

}