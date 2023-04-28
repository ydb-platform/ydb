#include "granule.h"
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NOlap::NIndexedReader {

void TGranule::OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch) {
    Y_VERIFY(!ReadyFlag);
    Y_VERIFY(WaitBatches.erase(batchInfo.GetBatchNo()));
    if (batch && batch->num_rows()) {
        ReadyBatches.emplace_back(batch);
    }
    Owner->OnBatchReady(batchInfo, batch);
    if (WaitBatches.empty()) {
        ReadyFlag = true;
        Owner->OnGranuleReady(*this);
    }
}

NKikimr::NOlap::NIndexedReader::TBatch& TGranule::AddBatch(const ui32 batchNo, const TPortionInfo& portionInfo) {
    Y_VERIFY(!ReadyFlag);
    WaitBatches.emplace(batchNo);
    auto infoEmplace = Batches.emplace(batchNo, TBatch(batchNo, *this, portionInfo));
    Y_VERIFY(infoEmplace.second);
    return infoEmplace.first->second;
}

void TGranule::AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch) const {
    Owner->AddBlobForFetch(range, batch);
}

const std::set<ui32>& TGranule::GetEarlyFilterColumns() const {
    return Owner->GetEarlyFilterColumns();
}

}
