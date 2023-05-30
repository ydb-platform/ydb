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
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_batch")("granule_id", GranuleId)
        ("batch_address", batchInfo.GetBatchAddress().ToString())("count", WaitBatches.size());
    Y_VERIFY(!ReadyFlag);
    Y_VERIFY(WaitBatches.erase(batchInfo.GetBatchAddress().GetBatchGranuleIdx()));
    if (batch && batch->num_rows()) {
        RecordBatches.emplace_back(batch);

        auto& indexInfo = Owner->GetReadMetadata()->GetIndexInfo();
        if (!batchInfo.IsDuplicationsAvailable()) {
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, indexInfo.GetReplaceKey(), false));
        } else {
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "dup_portion_on_ready");
            Y_VERIFY_DEBUG(NArrow::IsSorted(batch, indexInfo.GetReplaceKey(), false));
            Y_VERIFY(IsDuplicationsAvailable());
            BatchesToDedup.insert(batch.get());
        }
    }
    Owner->OnBatchReady(batchInfo, batch);
    CheckReady();
}

NKikimr::NOlap::NIndexedReader::TBatch& TGranule::AddBatch(const TPortionInfo& portionInfo) {
    Y_VERIFY(!ReadyFlag);
    ui32 batchGranuleIdx = Batches.size();
    WaitBatches.emplace(batchGranuleIdx);
    Batches.emplace_back(TBatch(TBatchAddress(GranuleIdx, batchGranuleIdx), *this, portionInfo));
    Y_VERIFY(GranuleBatchNumbers.emplace(batchGranuleIdx).second);
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

std::deque<TGranule::TBatchForMerge> TGranule::SortBatchesByPK(const bool reverse, TReadMetadata::TConstPtr readMetadata) {
    std::deque<TBatchForMerge> batches;
    for (auto&& i : Batches) {
        std::shared_ptr<TSortableBatchPosition> from;
        std::shared_ptr<TSortableBatchPosition> to;
        i.GetPKBorders(reverse, readMetadata->GetIndexInfo(), from, to);
        batches.emplace_back(TBatchForMerge(&i, from, to));
    }
    std::sort(batches.begin(), batches.end());
    ui32 currentPoolId = 0;
    std::map<TSortableBatchPosition, ui32> poolIds;
    for (auto&& i : batches) {
        if (!i.GetFrom() && !i.GetTo()) {
            continue;
        }
        if (i.GetFrom()) {
            auto it = poolIds.rbegin();
            for (; it != poolIds.rend(); ++it) {
                if (it->first.Compare(*i.GetFrom()) < 0) {
                    break;
                }
            }
            if (it != poolIds.rend()) {
                i.SetPoolId(it->second);
                if (i.GetTo()) {
                    poolIds.erase(it->first);
                    poolIds.emplace(*i.GetTo(), *i.GetPoolId());
                } else {
                    poolIds.erase(it->first);
                }
            } else if (i.GetTo()) {
                i.SetPoolId(++currentPoolId);
                poolIds.emplace(*i.GetTo(), *i.GetPoolId());
            }
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
        if (NotIndexedBatch) {
            RecordBatches.emplace_back(NotIndexedBatch);
        }
        NotIndexedBatchFutureFilter = Owner->GetReadMetadata()->GetProgram().BuildEarlyFilter(batch);
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
