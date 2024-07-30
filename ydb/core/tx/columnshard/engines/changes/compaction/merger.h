#pragma once
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/splitter/stats.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>

namespace NKikimr::NOlap::NCompaction {
class TMerger {
private:
    YDB_ACCESSOR(bool, OptimizationWritingPackMode, false);
    std::vector<std::shared_ptr<NArrow::TGeneralContainer>> Batches;
    std::vector<std::shared_ptr<NArrow::TColumnFilter>> Filters;
    const TConstructionContext& Context;
    const TSaverContext& SaverContext;

public:
    void AddBatch(const std::shared_ptr<NArrow::TGeneralContainer>& batch, const std::shared_ptr<NArrow::TColumnFilter>& filter) {
        AFL_VERIFY(batch);
        Batches.emplace_back(batch);
        Filters.emplace_back(filter);
    }

    TMerger(const TConstructionContext& context, const TSaverContext& saverContext)
        : Context(context)
        , SaverContext(saverContext)
    {
    
    }

    TMerger(const TConstructionContext& context, const TSaverContext& saverContext,
        std::vector<std::shared_ptr<NArrow::TGeneralContainer>>&& batches,
        std::vector<std::shared_ptr<NArrow::TColumnFilter>>&& filters)
        : Batches(std::move(batches))
        , Filters(std::move(filters))
        , Context(context)
        , SaverContext(saverContext) {
        AFL_VERIFY(Batches.size() == Filters.size());
    }

    std::vector<NKikimr::NOlap::TWritePortionInfoWithBlobsResult> Execute(
        const std::shared_ptr<TSerializationStats>& stats,
        const NArrow::NMerger::TIntervalPositions& checkPoints,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const ui64 pathId, const std::optional<ui64> shardingActualVersion);
};
}
