#pragma once

#include "common.h"
#include "context.h"
#include "executor.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <ydb/library/actors/interconnect/types.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TBuildFilterTaskContext {
private:
    TBuildFilterContext Context;
    YDB_READONLY_DEF(std::shared_ptr<TBuildFilterTaskExecutor>, Executor);
    YDB_READONLY_DEF(std::vector<TIntervalInfo>, Intervals);
    YDB_READONLY_DEF(THashSet<ui64>, RequiredPortions);

public:
    TBuildFilterTaskContext(
        TBuildFilterContext&& context, const std::shared_ptr<TBuildFilterTaskExecutor>& executor, std::vector<TIntervalInfo>&& intervals, THashSet<ui64>&& portions)
        : Context(std::move(context))
        , Executor(executor)
        , Intervals(std::move(intervals))
        , RequiredPortions(std::move(portions))
    {
    }

    const TBuildFilterContext& GetGlobalContext() const {
        return Context;
    }

    TBuildFilterContext&& ExtractGlobalContext() {
        return std::move(Context);
    }
};

class TBuildDuplicateFilters: public NConveyor::ITask {
private:
    class TDuplicateSourceCacheResult {
    private:
        using TColumnData = THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
        TColumnData DataByAddress;

    public:
        TDuplicateSourceCacheResult(TColumnData&& data)
            : DataByAddress(std::move(data))
        {
        }

        THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> ExtractDataByPortion(
            const std::map<ui32, std::shared_ptr<arrow::Field>>& fieldByColumn) {
            THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>> dataByPortion;
            std::vector<std::shared_ptr<arrow::Field>> fields;
            for (const auto& [_, field] : fieldByColumn) {
                fields.emplace_back(field);
            }

            THashMap<ui64, THashMap<ui32, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>> columnsByPortion;
            for (auto&& [address, data] : DataByAddress) {
                AFL_VERIFY(columnsByPortion[address.GetPortionId()].emplace(address.GetColumnId(), data).second);
            }

            for (auto& [portion, columns] : columnsByPortion) {
                std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> sortedColumns;
                for (const auto& [columnId, field] : fieldByColumn) {
                    auto column = columns.FindPtr(columnId);
                    AFL_VERIFY(column);
                    sortedColumns.emplace_back(*column);
                }
                std::shared_ptr<NArrow::TGeneralContainer> container =
                    std::make_shared<NArrow::TGeneralContainer>(fields, std::move(sortedColumns));
                AFL_VERIFY(dataByPortion.emplace(portion, std::move(container)).second);
            }

            return dataByPortion;
        }
    };

private:
    TBuildFilterTaskContext Context;
    TDuplicateSourceCacheResult ColumnData;
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AllocationGuard;
    NArrow::NMerger::TCursor ScanSnapshotBatch;
    NArrow::NMerger::TCursor MinUncommittedSnapshotBatch;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

    THashMap<ui64, NArrow::TColumnFilter> BuildFiltersOnInterval(const TIntervalInfo& interval, NArrow::NMerger::TMergePartialStream& merger,
        const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& columnData);
    std::vector<std::string> GetVersionColumnNames() const {
        return IIndexInfo::GetSnapshotColumnNames();
    }

    NArrow::NMerger::TCursor GetVersionBatch(const TSnapshot& snapshot, const ui64 writeId) {
        NArrow::TGeneralContainer batch(1);
        IIndexInfo::AddSnapshotColumns(batch, snapshot, writeId);
        return NArrow::NMerger::TCursor(batch.BuildTableVerified(), 0, IIndexInfo::GetSnapshotColumnNames());
    };

public:
    TBuildDuplicateFilters(TBuildFilterTaskContext&& context,
        THashMap<NGeneralCache::TGlobalColumnAddress, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>&& columns,
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& allocationGuard)
        : Context(std::move(context))
        , ColumnData(std::move(columns))
        , AllocationGuard(allocationGuard)
        , ScanSnapshotBatch(GetVersionBatch(Context.GetGlobalContext().GetMaxVersion(), std::numeric_limits<ui64>::max()))
        , MinUncommittedSnapshotBatch(GetVersionBatch(TSnapshot::Max(), 0))
    {
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << '{';
        sb << "context=" << Context.GetGlobalContext().DebugString();
        sb << '}';
        return sb;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
