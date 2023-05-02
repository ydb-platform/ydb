#pragma once
#include "conveyor_task.h"

#include <ydb/core/formats/arrow_filter.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {
struct TReadMetadata;
}

namespace NKikimr::NOlap::NIndexedReader {

class TGranule;

class TBatch {
private:
    YDB_READONLY(ui64, BatchNo, 0);
    YDB_READONLY(ui64, Portion, 0);
    YDB_READONLY(ui64, Granule, 0);
    YDB_READONLY(ui64, WaitingBytes, 0);
    YDB_READONLY(ui64, FetchedBytes, 0);

    THashSet<TBlobRange> WaitIndexed;
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, FilteredBatch);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, FilterBatch);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, FutureFilter);
    
    ui32 OriginalRecordsCount = 0;

    YDB_FLAG_ACCESSOR(DuplicationsAvailable, false);
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> Data;
    TGranule* Owner;
    const TPortionInfo* PortionInfo = nullptr;

    YDB_READONLY_DEF(std::optional<std::set<ui32>>, CurrentColumnIds);
    std::set<ui32> AskedColumnIds;
    void ResetCommon(const std::set<ui32>& columnIds);
    ui64 GetUsefulBytes(const ui64 bytes) const;

public:
    ui64 GetUsefulWaitingBytes() const {
        return GetUsefulBytes(WaitingBytes);
    }

    ui64 GetUsefulFetchedBytes() const {
        return GetUsefulBytes(FetchedBytes);
    }

    bool NeedAdditionalData() const;
    bool IsSortableInGranule() const {
        return PortionInfo->IsSortableInGranule();
    }
    TBatch(const ui32 batchNo, TGranule& owner, const TPortionInfo& portionInfo);
    bool AddIndexedReady(const TBlobRange& bRange, const TString& blobData);
    bool AskedColumnsAlready(const std::set<ui32>& columnIds) const;

    void ResetNoFilter(const std::set<ui32>& columnIds);
    void ResetWithFilter(const std::set<ui32>& columnIds);
    ui64 GetFetchBytes(const std::set<ui32>& columnIds);

    bool IsFiltered() const {
        return !!Filter;
    }
    ui32 GetFilteredRecordsCount() const {
        Y_VERIFY(IsFiltered());
        if (!FilterBatch) {
            return 0;
        } else {
            return FilterBatch->num_rows();
        }
    }
    bool InitFilter(std::shared_ptr<NArrow::TColumnFilter> filter, std::shared_ptr<arrow::RecordBatch> filterBatch,
        const ui32 originalRecordsCount, std::shared_ptr<NArrow::TColumnFilter> futureFilter);
    void InitBatch(std::shared_ptr<arrow::RecordBatch> batch);

    NColumnShard::IDataTasksProcessor::ITask::TPtr AssembleTask(NColumnShard::IDataTasksProcessor::TPtr processor, std::shared_ptr<const NOlap::TReadMetadata> readMetadata);

    const THashSet<TBlobRange>& GetWaitingBlobs() const {
        return WaitIndexed;
    }

    const TGranule& GetOwner() const {
        return *Owner;
    }

    bool IsFetchingReady() const {
        return WaitIndexed.empty();
    }

    const TPortionInfo& GetPortionInfo() const {
        return *PortionInfo;
    }
};
}
