#include "granule.h"
#include "filling_context.h"
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>
#include <ydb/core/tx/columnshard/engines/filter.h>

namespace NKikimr::NOlap::NIndexedReader {

void TGranule::OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch) {
    if (Owner->GetSortingPolicy()->CanInterrupt()) {
        if (ReadyFlag) {
            return;
        }
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_batch")("granule_id", GranuleId)("batch_no", batchInfo.GetBatchNo())("count", WaitBatches.size());
    Y_VERIFY(!ReadyFlag);
    Y_VERIFY(WaitBatches.erase(batchInfo.GetBatchNo()));
    if (batch && batch->num_rows()) {
        if (batchInfo.IsSortableInGranule()) {
            SortableBatches.emplace_back(batch);
        } else {
            NonSortableBatches.emplace_back(batch);
        }

        if (!batchInfo.IsDuplicationsAvailable()) {
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, Owner->GetReadMetadata()->IndexInfo.GetReplaceKey(), false));
        } else {
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "dup_portion_on_ready");
            Y_VERIFY_DEBUG(NArrow::IsSorted(batch, Owner->GetReadMetadata()->IndexInfo.GetReplaceKey(), false));
            Y_VERIFY(IsDuplicationsAvailable());
            BatchesToDedup.insert(batch.get());
        }
    }
    Owner->OnBatchReady(batchInfo, batch);
    CheckReady();
}

NKikimr::NOlap::NIndexedReader::TBatch& TGranule::AddBatch(const ui32 batchNo, const TPortionInfo& portionInfo) {
    Y_VERIFY(!ReadyFlag);
    WaitBatches.emplace(batchNo);
    Batches.emplace_back(TBatch(batchNo, *this, portionInfo));
    Y_VERIFY(GranuleBatchNumbers.emplace(batchNo).second);
    Owner->OnNewBatch(Batches.back());
    return Batches.back();
}

void TGranule::AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch) const {
    Owner->AddBlobForFetch(range, batch);
}

const std::set<ui32>& TGranule::GetEarlyFilterColumns() const {
    return Owner->GetEarlyFilterColumns();
}

bool TGranule::OnFilterReady(TBatch& batchInfo) {
    if (ReadyFlag) {
        return false;
    }
    return Owner->GetSortingPolicy()->OnFilterReady(batchInfo, *this, *Owner);
}

std::deque<TBatch*> TGranule::SortBatchesByPK(const bool reverse, TReadMetadata::TConstPtr readMetadata) {
    std::deque<TBatch*> batches;
    for (auto&& i : Batches) {
        batches.emplace_back(&i);
    }
    const int reverseKff = reverse ? -1 : 0;
    const auto pred = [reverseKff, readMetadata](const TBatch* l, const TBatch* r) {
        if (l->IsSortableInGranule() && r->IsSortableInGranule()) {
            return l->GetPortionInfo().CompareMinByPk(r->GetPortionInfo(), readMetadata->IndexInfo) * reverseKff < 0;
        } else if (l->IsSortableInGranule()) {
            return false;
        } else if (r->IsSortableInGranule()) {
            return true;
        } else {
            return false;
        }
    };
    std::sort(batches.begin(), batches.end(), pred);
    bool nonCompactedSerial = true;
    for (ui32 i = 0; i + 1 < batches.size(); ++i) {
        if (batches[i]->IsSortableInGranule()) {
            auto& l = *batches[i];
            auto& r = *batches[i + 1];
            Y_VERIFY(r.IsSortableInGranule());
            Y_VERIFY(l.GetPortionInfo().CompareSelfMaxItemMinByPk(r.GetPortionInfo(), readMetadata->IndexInfo) * reverseKff <= 0);
            nonCompactedSerial = false;
        } else {
            Y_VERIFY(nonCompactedSerial);
        }
    }
    return batches;
}

void TGranule::AddNotIndexedBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    Y_VERIFY(!NotIndexedBatchReadyFlag || !batch);
    if (!NotIndexedBatchReadyFlag) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_batch")("granule_id", GranuleId)("batch_no", "add_not_indexed_batch")("count", WaitBatches.size());
    } else {
        return;
    }
    NotIndexedBatchReadyFlag = true;
    if (batch && batch->num_rows()) {
        Y_VERIFY(!NotIndexedBatch);
        NotIndexedBatch = batch;
        if (Owner->GetReadMetadata()->Program) {
            NotIndexedBatchFutureFilter = std::make_shared<NArrow::TColumnFilter>(NOlap::EarlyFilter(batch, Owner->GetReadMetadata()->Program));
        }
        DuplicationsAvailableFlag = true;
    }
    CheckReady();
    Owner->Wakeup(*this);
}

void TGranule::CheckReady() {
    if (WaitBatches.empty() && NotIndexedBatchReadyFlag) {
        ReadyFlag = true;
        Owner->OnGranuleReady(*this);
    }
}

}
