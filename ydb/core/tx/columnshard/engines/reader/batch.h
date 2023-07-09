#pragma once
#include "common.h"
#include "conveyor_task.h"
#include "read_filter_merger.h"

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
    YDB_READONLY_DEF(std::optional<ui64>, BatchSize);
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
        BatchSize = NArrow::GetBatchMemorySize(FilteredBatch);
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

class TBatch: TNonCopyable {
private:
    const TBatchAddress BatchAddress;
    YDB_READONLY(ui64, Portion, 0);
    YDB_READONLY(ui64, Granule, 0);
    YDB_READONLY(ui64, WaitingBytes, 0);
    YDB_READONLY(ui64, FetchedBytes, 0);
    const ui64 PredictedBatchSize;

    THashSet<TBlobRange> WaitIndexed;
    mutable std::optional<std::shared_ptr<TSortableBatchPosition>> FirstPK;
    mutable std::optional<std::shared_ptr<TSortableBatchPosition>> LastPK;
    mutable std::optional<std::shared_ptr<TSortableBatchPosition>> ReverseFirstPK;
    mutable std::optional<std::shared_ptr<TSortableBatchPosition>> ReverseLastPK;

    YDB_READONLY_FLAG(DuplicationsAvailable, false);
    YDB_READONLY_DEF(TBatchFetchedInfo, FetchedInfo);
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> Data;
    TGranule* Owner;
    const TPortionInfo* PortionInfo = nullptr;

    YDB_READONLY_DEF(std::optional<std::set<ui32>>, CurrentColumnIds);
    std::set<ui32> AskedColumnIds;
    void ResetCommon(const std::set<ui32>& columnIds);
    ui64 GetUsefulBytes(const ui64 bytes) const;
    bool CheckReadyForAssemble();
    bool IsFetchingReady() const {
        return WaitIndexed.empty();
    }

public:
    std::shared_ptr<TSortableBatchPosition> GetFirstPK(const bool reverse, const TIndexInfo& indexInfo) const;
    void GetPKBorders(const bool reverse, const TIndexInfo& indexInfo, std::shared_ptr<TSortableBatchPosition>& from, std::shared_ptr<TSortableBatchPosition>& to) const;

    ui64 GetPredictedBatchSize() const {
        return PredictedBatchSize;
    }

    ui64 GetRealBatchSizeVerified() const {
        auto result = FetchedInfo.GetBatchSize();
        Y_VERIFY(result);
        return *result;
    }

    bool AllowEarlyFilter() const {
        return PortionInfo->AllowEarlyFilter();
    }
    const TBatchAddress& GetBatchAddress() const {
        return BatchAddress;
    }

    ui64 GetUsefulWaitingBytes() const {
        return GetUsefulBytes(WaitingBytes);
    }

    ui64 GetUsefulFetchedBytes() const {
        return GetUsefulBytes(FetchedBytes);
    }

    TBatch(const TBatchAddress& address, TGranule& owner, const TPortionInfo& portionInfo, const ui64 predictedBatchSize);
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

    const TPortionInfo& GetPortionInfo() const {
        return *PortionInfo;
    }
};
}
