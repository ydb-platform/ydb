#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <optional>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NColumnShard {

class TStorageStats {
private:
    NOlap::TSimplePortionsGroupInfo Written;
    NOlap::TSimplePortionsGroupInfo Compacted;
    // waiting for cleanup
    NOlap::TSimplePortionsGroupInfo Removed;

    // signed, reads clamp at zero: a counting mistake must not fail the shard
    i64 SmallBlobsBytes = 0;
    i64 SmallBlobsCount = 0;

    NOlap::TSimplePortionsGroupInfo& GetGroup(const NOlap::TPortionInfo& portion);

public:
    void AddPortion(const NOlap::TPortionInfo& portion);
    void RemovePortion(const NOlap::TPortionInfo& portion);
    void AddSmallBlob(ui64 bytes);
    void RemoveSmallBlob(ui64 bytes);
    bool IsEmpty() const;

    const NOlap::TSimplePortionsGroupInfo& GetWritten() const {
        return Written;
    }

    const NOlap::TSimplePortionsGroupInfo& GetCompacted() const {
        return Compacted;
    }

    const NOlap::TSimplePortionsGroupInfo& GetRemoved() const {
        return Removed;
    }

    NOlap::TSimplePortionsGroupInfo GetAll() const {
        return Written + Compacted + Removed;
    }

    NOlap::TSmallBlobsStat GetSmallBlobs() const {
        return { .VolumeBytes = (ui64)std::max<i64>(0, SmallBlobsBytes), .Count = (ui64)std::max<i64>(0, SmallBlobsCount) };
    }
};

class TDataStatsByStorage {
private:
    THashMap<TString, TStorageStats> Storages;

    TStorageStats& GetStorageVerified(const TString& storageId);
    void EraseStorageIfEmpty(const TString& storageId);

public:
    // blobStorageSmallBlobBytes: the portion's bytes in the blob storage if they are a small blob, otherwise 0
    void AddPortion(const NOlap::TPortionInfo& portion, const ui64 blobStorageSmallBlobBytes);
    void RemovePortion(const NOlap::TPortionInfo& portion, const ui64 blobStorageSmallBlobBytes);

    bool IsEmpty() const {
        return Storages.empty();
    }

    const THashMap<TString, TStorageStats>& GetStorages() const {
        return Storages;
    }

    const TStorageStats& GetStorageStats(const TString& storageId) const;

    // without a storage id: summed over every storage
    NOlap::TSimplePortionsGroupInfo GetWritten(const std::optional<TString>& storageId = std::nullopt) const;
    NOlap::TSimplePortionsGroupInfo GetCompacted(const std::optional<TString>& storageId = std::nullopt) const;
    NOlap::TSimplePortionsGroupInfo GetRemoved(const std::optional<TString>& storageId = std::nullopt) const;
    NOlap::TSimplePortionsGroupInfo GetAll(const std::optional<TString>& storageId = std::nullopt) const;
    NOlap::TSmallBlobsStat GetSmallBlobs(const std::optional<TString>& storageId = std::nullopt) const;
};

}   // namespace NKikimr::NColumnShard
