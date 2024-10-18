#include "portions.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NColumnShard {

void TPortionCategoryCounters::AddPortion(const std::shared_ptr<NOlap::TPortionInfo>& p) {
    RecordsCount->Add(p->NumRows());
    Count->Add(1);
    BlobBytes->Add(p->GetTotalBlobBytes());
    RawBytes->Add(p->GetTotalRawBytes());
}

void TPortionCategoryCounters::RemovePortion(const std::shared_ptr<NOlap::TPortionInfo>& p) {
    RecordsCount->Remove(p->NumRows());
    Count->Remove(1);
    BlobBytes->Remove(p->GetTotalBlobBytes());
    RawBytes->Remove(p->GetTotalRawBytes());
}

}   // namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {

void TSimplePortionsGroupInfo::AddPortion(const std::shared_ptr<TPortionInfo>& p) {
    AFL_VERIFY(p);
    AddPortion(*p);
}
void TSimplePortionsGroupInfo::AddPortion(const TPortionInfo& p) {
    BlobBytes += p.GetTotalBlobBytes();
    RawBytes += p.GetTotalRawBytes();
    Count += 1;
    RecordsCount += p.NumRows();
}

void TSimplePortionsGroupInfo::RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
    AFL_VERIFY(p);
    RemovePortion(*p);
}
void TSimplePortionsGroupInfo::RemovePortion(const TPortionInfo& p) {
    BlobBytes -= p.GetTotalBlobBytes();
    RawBytes -= p.GetTotalRawBytes();
    Count -= 1;
    RecordsCount -= p.NumRows();
    AFL_VERIFY(RawBytes >= 0);
    AFL_VERIFY(BlobBytes >= 0);
    AFL_VERIFY(Count >= 0);
    AFL_VERIFY(RecordsCount >= 0);
}

}   // namespace NKikimr::NOlap
