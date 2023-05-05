#pragma once
#include "common.h"
#include "conveyor_task.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {
struct TReadMetadata;
}

namespace NKikimr::NOlap::NIndexedReader {

class TGranule;
class TBatch;

class TBatchFetchedInfo {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, FilteredBatch);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, FilterBatch);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, NotAppliedEarlyFilter);
    ui32 OriginalRecordsCount = 0;
public:
    void InitFilter(std::shared_ptr<NArrow::TColumnFilter> filter, std::shared_ptr<arrow::RecordBatch> filterBatch,
        const ui32 originalRecordsCount, std::shared_ptr<NArrow::TColumnFilter> notAppliedEarlyFilter) {
        Y_VERIFY(filter);
        Y_VERIFY(!Filter);
        Y_VERIFY(!FilterBatch);
        Filter = filter;
        FilterBatch = filterBatch;
        OriginalRecordsCount = originalRecordsCount;
        NotAppliedEarlyFilter = notAppliedEarlyFilter;
    }

    double GetUsefulDataKff() const {
        if (!FilterBatch || !FilterBatch->num_rows()) {
            return 0;
        }
        Y_VERIFY_DEBUG(OriginalRecordsCount);
        if (!OriginalRecordsCount) {
            return 0;
        }
        return 1.0 * FilterBatch->num_rows() / OriginalRecordsCount;
    }

    bool IsFiltered() const {
        return !!Filter;
    }

    void InitBatch(std::shared_ptr<arrow::RecordBatch> fullBatch) {
        Y_VERIFY(!FilteredBatch);
        FilteredBatch = fullBatch;
    }

    ui32 GetFilteredRecordsCount() const {
        Y_VERIFY(IsFiltered());
        if (!FilterBatch) {
            return 0;
        } else {
            return FilterBatch->num_rows();
        }
    }
};

class TBatch {
private:
    const TBatchAddress BatchAddress;
    YDB_READONLY(ui64, Portion, 0);
    YDB_READONLY(ui64, Granule, 0);
    YDB_READONLY(ui64, WaitingBytes, 0);
    YDB_READONLY(ui64, FetchedBytes, 0);

    THashSet<TBlobRange> WaitIndexed;
    
    YDB_READONLY_FLAG(DuplicationsAvailable, false);
    YDB_READONLY_DEF(TBatchFetchedInfo, FetchedInfo);
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> Data;
    TGranule* Owner;
    const TPortionInfo* PortionInfo = nullptr;

    YDB_READONLY_DEF(std::optional<std::set<ui32>>, CurrentColumnIds);
    std::set<ui32> AskedColumnIds;
    void ResetCommon(const std::set<ui32>& columnIds);
    ui64 GetUsefulBytes(const ui64 bytes) const;

public:
    bool AllowEarlyFilter() const {
        return PortionInfo->AllowEarlyFilter();
    }
    const TBatchAddress& GetBatchAddress() const {
        return BatchAddress;
    }
    std::optional<ui32> GetMergePoolId() const;

    ui64 GetUsefulWaitingBytes() const {
        return GetUsefulBytes(WaitingBytes);
    }

    ui64 GetUsefulFetchedBytes() const {
        return GetUsefulBytes(FetchedBytes);
    }

    bool IsSortableInGranule() const {
        return PortionInfo->IsSortableInGranule();
    }
    TBatch(const TBatchAddress& address, TGranule& owner, const TPortionInfo& portionInfo);
    bool AddIndexedReady(const TBlobRange& bRange, const TString& blobData);
    bool AskedColumnsAlready(const std::set<ui32>& columnIds) const;

    void ResetNoFilter(const std::set<ui32>& columnIds);
    void ResetWithFilter(const std::set<ui32>& columnIds);
    ui64 GetFetchBytes(const std::set<ui32>& columnIds);

    ui32 GetFilteredRecordsCount() const;
    bool InitFilter(std::shared_ptr<NArrow::TColumnFilter> filter, std::shared_ptr<arrow::RecordBatch> filterBatch,
        const ui32 originalRecordsCount, std::shared_ptr<NArrow::TColumnFilter> notAppliedEarlyFilter);
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
