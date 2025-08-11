#include "portions.h"

#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
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

void TSimplePortionsGroupInfo::RemovePortion(const TPortionInfo& p) {
    BlobBytes.Sub(p.GetTotalBlobBytes());
    RawBytes.Sub(p.GetTotalRawBytes());
    Count.Sub(1);
    RecordsCount.Sub(p.GetRecordsCount());
}

void TSimplePortionsGroupInfo::AddPortion(const TPortionInfo& p) {
    BlobBytes.Add(p.GetTotalBlobBytes());
    RawBytes.Add(p.GetTotalRawBytes());
    Count.Inc();
    RecordsCount.Add(p.GetRecordsCount());
}

void TFullPortionsGroupInfo::AddPortion(const TPortionDataAccessor& p) {
    TBase::AddPortion(p.GetPortionInfo());
    Blobs.Add(p.GetBlobIdsCount());
    for (const auto& blob : p.GetBlobIds()) {
        BytesByChannel[blob.Channel()] += blob.BlobSize();
    }
}

void TFullPortionsGroupInfo::RemovePortion(const TPortionDataAccessor& p) {
    TBase::RemovePortion(p.GetPortionInfo());
    Blobs.Sub(p.GetBlobIdsCount());
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
