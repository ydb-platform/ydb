#include "counters.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLBuckets {

void TPortionCategoryCounters::AddPortion(const std::shared_ptr<TPortionInfo>& p) {
    RecordsCount->Add(p->NumRows());
    Count->Add(1);
    Bytes->Add(p->GetTotalBlobBytes());
}

void TPortionCategoryCounters::RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
    RecordsCount->Remove(p->NumRows());
    Count->Remove(1);
    Bytes->Remove(p->GetTotalBlobBytes());
}

}
