#include "batch.h"
#include "granule.h"
#include "filter_assembler.h"
#include "postfilter_assembler.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NOlap::NIndexedReader {

TBatch::TBatch(const ui32 batchNo, TGranule& owner, const TPortionInfo& portionInfo)
    : BatchNo(batchNo)
    , Portion(portionInfo.Records[0].Portion)
    , Granule(owner.GetGranuleId())
    , Owner(&owner)
    , PortionInfo(&portionInfo) {
    Y_VERIFY(portionInfo.Records.size());

    if (portionInfo.CanIntersectOthers()) {
        Owner->SetDuplicationsAvailable(true);
        if (portionInfo.CanHaveDups()) {
            SetDuplicationsAvailable(true);
        }
    }
}

NColumnShard::IDataTasksProcessor::ITask::TPtr TBatch::AssembleTask(NColumnShard::IDataTasksProcessor::TPtr processor, NOlap::TReadMetadata::TConstPtr readMetadata) {
    Y_VERIFY(WaitIndexed.empty());
    Y_VERIFY(PortionInfo->Produced());
    Y_VERIFY(!FilteredBatch);
    auto batchConstructor = PortionInfo->PrepareForAssemble(readMetadata->IndexInfo, readMetadata->LoadSchema, Data, CurrentColumnIds);
    Data.clear();
    if (!Filter) {
        return std::make_shared<TAssembleFilter>(std::move(batchConstructor), readMetadata, *this, PortionInfo->AllowEarlyFilter(), Owner->GetEarlyFilterColumns(), processor);
    } else {
        Y_VERIFY(FilterBatch);
        return std::make_shared<TAssembleBatch>(std::move(batchConstructor), *this, readMetadata->GetColumnsOrder(), processor);
    }
}

bool TBatch::AskedColumnsAlready(const std::set<ui32>& columnIds) const {
    if (!CurrentColumnIds) {
        return true;
    }
    if (AskedColumnIds.size() < columnIds.size()) {
        return false;
    }
    for (auto&& i : columnIds) {
        if (!AskedColumnIds.contains(i)) {
            return false;
        }
    }
    return true;
}

ui64 TBatch::GetFetchBytes(const std::set<ui32>& columnIds) {
    ui64 result = 0;
    for (const NOlap::TColumnRecord& rec : PortionInfo->Records) {
        if (!columnIds.contains(rec.ColumnId)) {
            continue;
        }
        Y_VERIFY(rec.Portion == Portion);
        Y_VERIFY(rec.Valid());
        Y_VERIFY(Granule == rec.Granule);
        result += rec.BlobRange.Size;
    }
    return result;
}

void TBatch::ResetCommon(const std::set<ui32>& columnIds) {
    CurrentColumnIds = columnIds;
    Y_VERIFY(CurrentColumnIds->size());
    for (auto&& i : *CurrentColumnIds) {
        Y_VERIFY(AskedColumnIds.emplace(i).second);
    }

    Y_VERIFY(WaitIndexed.empty());
    Y_VERIFY(Data.empty());
    WaitingBytes = 0;
    FetchedBytes = 0;
}

void TBatch::ResetNoFilter(const std::set<ui32>& columnIds) {
    Y_VERIFY(!Filter);
    ResetCommon(columnIds);
    for (const NOlap::TColumnRecord& rec : PortionInfo->Records) {
        if (CurrentColumnIds && !CurrentColumnIds->contains(rec.ColumnId)) {
            continue;
        }
        Y_VERIFY(WaitIndexed.emplace(rec.BlobRange).second);
        Owner->AddBlobForFetch(rec.BlobRange, *this);
        Y_VERIFY(rec.Portion == Portion);
        Y_VERIFY(rec.Valid());
        Y_VERIFY(Granule == rec.Granule);
        WaitingBytes += rec.BlobRange.Size;
    }
}

void TBatch::ResetWithFilter(const std::set<ui32>& columnIds) {
    Y_VERIFY(Filter);
    ResetCommon(columnIds);
    std::map<ui32, std::map<ui16, const TColumnRecord*>> orderedObjects;
    for (const NOlap::TColumnRecord& rec : PortionInfo->Records) {
        if (CurrentColumnIds && !CurrentColumnIds->contains(rec.ColumnId)) {
            continue;
        }
        orderedObjects[rec.ColumnId][rec.Chunk] = &rec;
        Y_VERIFY(rec.Valid());
        Y_VERIFY(Portion == rec.Portion);
        Y_VERIFY(Granule == rec.Granule);
    }

    for (auto&& columnInfo : orderedObjects) {
        ui32 expected = 0;
        auto it = Filter->GetIterator();
        bool undefinedShift = false;
        bool itFinished = false;
        for (auto&& [chunk, rec] : columnInfo.second) {
            Y_VERIFY(!itFinished);
            Y_VERIFY(expected++ == chunk);
            if (!rec->GetChunkRowsCount()) {
                undefinedShift = true;
            }
            if (!undefinedShift && it.IsBatchForSkip(*rec->GetChunkRowsCount())) {
                Data.emplace(rec->BlobRange, TPortionInfo::TAssembleBlobInfo(*rec->GetChunkRowsCount()));
            } else {
                Y_VERIFY(WaitIndexed.emplace(rec->BlobRange).second);
                Owner->AddBlobForFetch(rec->BlobRange, *this);
                WaitingBytes += rec->BlobRange.Size;
            }
            if (!undefinedShift) {
                itFinished = !it.Next(*rec->GetChunkRowsCount());
            }
        }
    }
}

bool TBatch::InitFilter(std::shared_ptr<NArrow::TColumnFilter> filter, std::shared_ptr<arrow::RecordBatch> filterBatch,
    const ui32 originalRecordsCount, std::shared_ptr<NArrow::TColumnFilter> futureFilter) {
    Y_VERIFY(filter);
    Y_VERIFY(!Filter);
    Y_VERIFY(!FilterBatch);
    Filter = filter;
    FilterBatch = filterBatch;
    OriginalRecordsCount = originalRecordsCount;
    FutureFilter = futureFilter;
    return Owner->OnFilterReady(*this);
}

void TBatch::InitBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    Y_VERIFY(!FilteredBatch);
    FilteredBatch = batch;
    Owner->OnBatchReady(*this, batch);
}

bool TBatch::AddIndexedReady(const TBlobRange& bRange, const TString& blobData) {
    if (!WaitIndexed.erase(bRange)) {
        Y_ASSERT(false);
        return false;
    }
    WaitingBytes -= bRange.Size;
    FetchedBytes += bRange.Size;
    Data.emplace(bRange, TPortionInfo::TAssembleBlobInfo(blobData));
    return true;
}

bool TBatch::NeedAdditionalData() const {
    if (!Filter) {
        return true;
    }
    if (!FilteredBatch || !FilteredBatch->num_rows()) {
        return false;
    }
    if (AskedColumnsAlready(Owner->GetOwner().GetPostFilterColumns())) {
        return false;
    }
    return true;
}

ui64 TBatch::GetUsefulBytes(const ui64 bytes) const {
    if (!FilteredBatch || !FilteredBatch->num_rows()) {
        return 0;
    }
    Y_VERIFY_DEBUG(OriginalRecordsCount);
    if (!OriginalRecordsCount) {
        return 0;
    }
    return bytes * FilteredBatch->num_rows() / OriginalRecordsCount;
}

}
