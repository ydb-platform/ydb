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
    YDB_FLAG_ACCESSOR(DuplicationsAvailable, false);
    THashMap<TBlobRange, TString> Data;
    TGranule* Owner = nullptr;
    const TPortionInfo* PortionInfo = nullptr;

    friend class TGranule;
    TBatch(const ui32 batchNo, TGranule& owner, const TPortionInfo& portionInfo);

    YDB_READONLY_DEF(std::optional<std::set<ui32>>, CurrentColumnIds);
    std::set<ui32> AskedColumnIds;
public:
    bool AddIndexedReady(const TBlobRange& bRange, const TString& blobData);
    bool AskedColumnsAlready(const std::set<ui32>& columnIds) const;

    void Reset(const std::set<ui32>* columnIds);

    void InitFilter(std::shared_ptr<NArrow::TColumnFilter> filter, std::shared_ptr<arrow::RecordBatch> filterBatch);
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
