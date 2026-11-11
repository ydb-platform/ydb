#include "granule_data_stats.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NColumnShard {

namespace {

template <class TGetGroup>
NOlap::TSimplePortionsGroupInfo SumGroups(
    const THashMap<TString, TStorageStats>& storages, const std::optional<TString>& storageId, const TGetGroup& getGroup) {
    NOlap::TSimplePortionsGroupInfo result;
    for (const auto& [id, storage] : storages) {
        if (!storageId || id == *storageId) {
            result += getGroup(storage);
        }
    }
    return result;
}

}   // namespace

NOlap::TSimplePortionsGroupInfo& TStorageStats::GetGroup(const NOlap::TPortionInfo& portion) {
    if (portion.HasRemoveSnapshot()) {
        return Removed;
    }
    return portion.GetPortionType() == NOlap::EPortionType::Written ? Written : Compacted;
}

void TStorageStats::AddPortion(const NOlap::TPortionInfo& portion) {
    GetGroup(portion).AddPortion(portion);
}

void TStorageStats::RemovePortion(const NOlap::TPortionInfo& portion) {
    GetGroup(portion).RemovePortion(portion);
}

void TStorageStats::AddSmallBlob(const ui64 bytes) {
    SmallBlobsBytes += (i64)bytes;
    ++SmallBlobsCount;
}

void TStorageStats::RemoveSmallBlob(const ui64 bytes) {
    SmallBlobsBytes -= (i64)bytes;
    --SmallBlobsCount;
}

bool TStorageStats::IsEmpty() const {
    return Written.IsEmpty() && Compacted.IsEmpty() && Removed.IsEmpty() && !SmallBlobsBytes && !SmallBlobsCount;
}

void TGranuleDataStats::EraseStorageIfEmpty(const TString& storageId) {
    auto it = Storages.find(storageId);
    if (it != Storages.end() && it->second.IsEmpty()) {
        Storages.erase(it);
    }
}

NOlap::TSimplePortionsGroupInfo TGranuleDataStats::GetWritten(const std::optional<TString>& storageId) const {
    return SumGroups(Storages, storageId, [](const TStorageStats& storage) {
        return storage.GetWritten();
    });
}

NOlap::TSimplePortionsGroupInfo TGranuleDataStats::GetCompacted(const std::optional<TString>& storageId) const {
    return SumGroups(Storages, storageId, [](const TStorageStats& storage) {
        return storage.GetCompacted();
    });
}

NOlap::TSimplePortionsGroupInfo TGranuleDataStats::GetRemoved(const std::optional<TString>& storageId) const {
    return SumGroups(Storages, storageId, [](const TStorageStats& storage) {
        return storage.GetRemoved();
    });
}

NOlap::TSimplePortionsGroupInfo TGranuleDataStats::GetAll(const std::optional<TString>& storageId) const {
    return SumGroups(Storages, storageId, [](const TStorageStats& storage) {
        return storage.GetAll();
    });
}

NOlap::TSmallBlobsStat TGranuleDataStats::GetSmallBlobs(const std::optional<TString>& storageId) const {
    NOlap::TSmallBlobsStat result;
    for (const auto& [id, storage] : Storages) {
        if (!storageId || id == *storageId) {
            const NOlap::TSmallBlobsStat stat = storage.GetSmallBlobs();
            result.VolumeBytes += stat.VolumeBytes;
            result.Count += stat.Count;
        }
    }
    return result;
}

}   // namespace NKikimr::NColumnShard
