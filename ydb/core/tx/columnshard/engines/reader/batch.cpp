#include "batch.h"
#include "granule.h"
#include "filter_assembler.h"
#include "postfilter_assembler.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>

namespace NKikimr::NOlap::NIndexedReader {

TBatch::TBatch(const TBatchAddress& address, TGranule& owner, const TPortionInfo& portionInfo, const ui64 predictedBatchSize)
    : BatchAddress(address)
    , Portion(portionInfo.GetPortion())
    , Granule(owner.GetGranuleId())
    , PredictedBatchSize(predictedBatchSize)
    , Owner(&owner)
    , PortionInfo(&portionInfo)
{
    Y_VERIFY(Granule == PortionInfo->GetGranule());
    Y_VERIFY(portionInfo.Records.size());

    if (portionInfo.CanIntersectOthers()) {
        ACFL_TRACE("event", "intersect_portion");
        Owner->SetDuplicationsAvailable(true);
        if (portionInfo.CanHaveDups()) {
            ACFL_TRACE("event", "dup_portion");
            DuplicationsAvailableFlag = true;
        }
    }
}

NColumnShard::IDataTasksProcessor::ITask::TPtr TBatch::AssembleTask(NColumnShard::IDataTasksProcessor::TPtr processor, NOlap::TReadMetadata::TConstPtr readMetadata) {
    Y_VERIFY(WaitIndexed.empty());
    Y_VERIFY(PortionInfo->Produced());
    Y_VERIFY(!FetchedInfo.GetFilteredBatch());

    auto blobSchema = readMetadata->GetLoadSchema(PortionInfo->GetMinSnapshot());
    auto readSchema = readMetadata->GetLoadSchema(readMetadata->GetSnapshot());
    ISnapshotSchema::TPtr resultSchema;
    if (CurrentColumnIds) {
        resultSchema = std::make_shared<TFilteredSnapshotSchema>(readSchema, *CurrentColumnIds);
    } else {
        resultSchema = readSchema;
    }
    auto batchConstructor = PortionInfo->PrepareForAssemble(*blobSchema, *resultSchema, Data);
    Data.clear();
    if (!FetchedInfo.GetFilter()) {
        return std::make_shared<TAssembleFilter>(std::move(batchConstructor), readMetadata,
            *this, Owner->GetEarlyFilterColumns(), processor, Owner->GetOwner().GetSortingPolicy());
    } else {
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
        Y_VERIFY(rec.Valid());
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
    Y_VERIFY(!FetchedInfo.GetFilter());
    ResetCommon(columnIds);
    for (const NOlap::TColumnRecord& rec : PortionInfo->Records) {
        if (CurrentColumnIds && !CurrentColumnIds->contains(rec.ColumnId)) {
            continue;
        }
        Y_VERIFY(WaitIndexed.emplace(rec.BlobRange).second);
        Owner->AddBlobForFetch(rec.BlobRange, *this);
        Y_VERIFY(rec.Valid());
        WaitingBytes += rec.BlobRange.Size;
    }
}

void TBatch::ResetWithFilter(const std::set<ui32>& columnIds) {
    Y_VERIFY(FetchedInfo.GetFilter());
    ResetCommon(columnIds);
    std::map<ui32, std::map<ui16, const TColumnRecord*>> orderedObjects;
    for (const NOlap::TColumnRecord& rec : PortionInfo->Records) {
        if (CurrentColumnIds && !CurrentColumnIds->contains(rec.ColumnId)) {
            continue;
        }
        orderedObjects[rec.ColumnId][rec.Chunk] = &rec;
        Y_VERIFY(rec.Valid());
    }

    for (auto&& columnInfo : orderedObjects) {
        ui32 expected = 0;
        auto it = FetchedInfo.GetFilter()->GetIterator(false);
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
    CheckReadyForAssemble();
}

bool TBatch::InitFilter(std::shared_ptr<NArrow::TColumnFilter> filter, std::shared_ptr<arrow::RecordBatch> filterBatch,
    const ui32 originalRecordsCount, std::shared_ptr<NArrow::TColumnFilter> notAppliedEarlyFilter)
{
    FetchedInfo.InitFilter(filter, filterBatch, originalRecordsCount, notAppliedEarlyFilter);
    return Owner->OnFilterReady(*this);
}

void TBatch::InitBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    FetchedInfo.InitBatch(batch);
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
    Owner->OnBlobReady(bRange);
    CheckReadyForAssemble();
    return true;
}

ui64 TBatch::GetUsefulBytes(const ui64 bytes) const {
    return bytes * FetchedInfo.GetUsefulDataKff();
}

std::shared_ptr<TSortableBatchPosition> TBatch::GetFirstPK(const bool reverse, const TIndexInfo& indexInfo) const {
    if (!FirstPK || !ReverseFirstPK) {
        std::shared_ptr<TSortableBatchPosition> from;
        std::shared_ptr<TSortableBatchPosition> to;
        GetPKBorders(reverse, indexInfo, from, to);
    }
    if (reverse) {
        return *ReverseFirstPK;
    } else {
        return *FirstPK;
    }
}

void TBatch::GetPKBorders(const bool reverse, const TIndexInfo& indexInfo, std::shared_ptr<TSortableBatchPosition>& from, std::shared_ptr<TSortableBatchPosition>& to) const {
    auto indexKey = indexInfo.GetIndexKey();
    Y_VERIFY(PortionInfo->Valid());
    if (!FirstPK) {
        const NArrow::TReplaceKey& minRecord = PortionInfo->IndexKeyStart();
        auto batch = minRecord.ToBatch(indexKey);
        Y_VERIFY(batch);
        FirstPK = std::make_shared<TSortableBatchPosition>(batch, 0, indexKey->field_names(), false);
        ReverseLastPK = std::make_shared<TSortableBatchPosition>(batch, 0, indexKey->field_names(), true);
    }
    if (!LastPK) {
        const NArrow::TReplaceKey& maxRecord = PortionInfo->IndexKeyEnd();
        auto batch = maxRecord.ToBatch(indexKey);
        Y_VERIFY(batch);
        LastPK = std::make_shared<TSortableBatchPosition>(batch, 0, indexKey->field_names(), false);
        ReverseFirstPK = std::make_shared<TSortableBatchPosition>(batch, 0, indexKey->field_names(), true);
    }
    if (reverse) {
        from = *ReverseFirstPK;
        to = *ReverseLastPK;
    } else {
        from = *FirstPK;
        to = *LastPK;
    }
}

bool TBatch::CheckReadyForAssemble() {
    if (IsFetchingReady()) {
        auto& context = Owner->GetOwner();
        auto processor = context.GetTasksProcessor();
        if (auto assembleBatchTask = AssembleTask(processor.GetObject(), context.GetReadMetadata())) {
            processor.Add(context, assembleBatchTask);
        }
        return true;
    }
    return false;
}

}
