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
        return std::make_shared<TAssembleFilter>(std::move(batchConstructor), readMetadata, *this, PortionInfo->AllowEarlyFilter(), processor);
    } else {
        Y_VERIFY(FilterBatch);
        return std::make_shared<TAssembleBatch>(std::move(batchConstructor), *this, readMetadata->GetColumnsOrder(), processor);
    }
}

bool TBatch::AskedColumnsAlready(const std::set<ui32>& columnIds) const {
    if (AskedColumnIds < columnIds) {
        return false;
    }
    for (auto&& i : columnIds) {
        if (!AskedColumnIds.contains(i)) {
            return false;
        }
    }
    return true;
}

void TBatch::Reset(const std::set<ui32>* columnIds) {
    if (!columnIds) {
        CurrentColumnIds.reset();
    } else {
        CurrentColumnIds = *columnIds;
        Y_VERIFY(CurrentColumnIds->size());
    }
    if (CurrentColumnIds) {
        for (auto&& i : *CurrentColumnIds) {
            AskedColumnIds.emplace(i);
        }
    }
    Y_VERIFY(WaitIndexed.empty());
    Y_VERIFY(Data.empty());
    WaitingBytes = 0;
    for (const NOlap::TColumnRecord& rec : PortionInfo->Records) {
        if (CurrentColumnIds && !CurrentColumnIds->contains(rec.ColumnId)) {
            continue;
        }
        Y_VERIFY(WaitIndexed.emplace(rec.BlobRange).second);
        Owner->Owner->AddBlobForFetch(rec.BlobRange, *this);
        Y_VERIFY(rec.Portion == Portion);
        Y_VERIFY(rec.Valid());
        Y_VERIFY(Granule == rec.Granule);
        WaitingBytes += rec.BlobRange.Size;
    }
}

void TBatch::InitFilter(std::shared_ptr<NArrow::TColumnFilter> filter, std::shared_ptr<arrow::RecordBatch> filterBatch) {
    Y_VERIFY(filter);
    Y_VERIFY(!Filter);
    Y_VERIFY(!FilterBatch);
    Filter = filter;
    FilterBatch = filterBatch;
}

void TBatch::InitBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    Y_VERIFY(!FilteredBatch);
    FilteredBatch = batch;
    Owner->OnBatchReady(*this, batch);
}

bool TBatch::AddIndexedReady(const TBlobRange& bRange, const TString& blobData) {
    if (!WaitIndexed.erase(bRange)) {
        return false;
    }
    WaitingBytes -= bRange.Size;
    Data.emplace(bRange, blobData);
    return true;
}

}
