#include "portions.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NColumnShard {

void TPortionCategoryCounters::AddPortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
    RecordsCount->Add(p->GetRecordsCount());
    Count->Add(1);
    BlobBytes->Add(p->GetTotalBlobBytes());
    RawBytes->Add(p->GetTotalRawBytes());
}

void TPortionCategoryCounters::RemovePortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
    RecordsCount->Remove(p->GetRecordsCount());
    Count->Remove(1);
    BlobBytes->Remove(p->GetTotalBlobBytes());
    RawBytes->Remove(p->GetTotalRawBytes());
}

}   // namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {

void TSimplePortionsGroupInfo::AddPortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
    AFL_VERIFY(p);
    AddPortion(*p);
}
void TSimplePortionsGroupInfo::AddPortion(const TPortionInfo& p) {
    Blobs += p.GetBlobIdsCount();
    BlobBytes += p.GetTotalBlobBytes();
    RawBytes += p.GetTotalRawBytes();
    Count += 1;
    RecordsCount += p.GetRecordsCount();
    for (const auto& blob : p.GetBlobIds()) {
        BytesByChannel[blob.Channel()] += blob.BlobSize();
    }
}

void TSimplePortionsGroupInfo::RemovePortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
    AFL_VERIFY(p);
    RemovePortion(*p);
}
void TSimplePortionsGroupInfo::RemovePortion(const TPortionInfo& p) {
    Blobs -= p.GetBlobIdsCount();
    BlobBytes -= p.GetTotalBlobBytes();
    RawBytes -= p.GetTotalRawBytes();
    Count -= 1;
    RecordsCount -= p.GetRecordsCount();
    AFL_VERIFY(Blobs >= 0);
    AFL_VERIFY(RawBytes >= 0);
    AFL_VERIFY(BlobBytes >= 0);
    AFL_VERIFY(Count >= 0);
    AFL_VERIFY(RecordsCount >= 0);
    for (const auto& blob : p.GetBlobIds()) {
        auto findChannel = BytesByChannel.find(blob.Channel());
        AFL_VERIFY(!findChannel.IsEnd())("blob", blob.ToStringLegacy());
        findChannel->second -= blob.BlobSize();
        AFL_VERIFY(findChannel->second >= 0);
        if (!findChannel->second) {
            BytesByChannel.erase(findChannel);
        }
    }
}

}   // namespace NKikimr::NOlap
