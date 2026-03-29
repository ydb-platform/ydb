#pragma once

#include "common.h"
#include "context.h"

#include <ydb/core/formats/arrow/reader/batch_iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/comparable.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TBordersFlowController {
private:
    struct TBorderInfo {
        std::vector<ui64> Start;
        std::vector<ui64> Finish;
    };
    std::map<NCommon::TReplaceKeyAdapter, TBorderInfo> Borders;
    std::set<NCommon::TReplaceKeyAdapter> WaitingBorders;
    std::set<NCommon::TReplaceKeyAdapter> ReadyBorders;
    std::unordered_set<ui64> ExclusivePortions;
    std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    TReadMetadataBase::TConstPtr ReadMetadata;

public:
    TBordersFlowController(const std::deque<std::shared_ptr<TPortionInfo>>& portions, const TReadMetadataBase::TConstPtr& readMetadata, const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters);
    
    bool IsExclusiveInterval(const ui64 portionId) const;

    TBordersIterator Next(const std::shared_ptr<const TPortionInfo>& portion);
    
    TString DebugString();
    
    std::optional<NArrow::TSimpleRow> NextReadyBorder();
    
    void AddBatch(const TBordersBatch& batch);
    
    bool IsReversed() const;
    
    ~TBordersFlowController();

private:
    void BuildExclusivePortions();
};

}