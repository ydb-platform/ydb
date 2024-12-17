#pragma once
#include "storage.h"
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>

namespace NKikimr::NOlap {

class TPortionInfo;

class IStoragesManager {
private:
    mutable TRWMutex RWMutex;
    bool Initialized = false;
    bool Finished = false;
    std::optional<ui64> Generation;
protected:
    virtual std::shared_ptr<IBlobsStorageOperator> DoBuildOperator(const TString& storageId) = 0;
    THashMap<TString, std::shared_ptr<IBlobsStorageOperator>> Constructed;
    std::shared_ptr<IBlobsStorageOperator> BuildOperator(const TString& storageId);

    virtual void DoInitialize();
    virtual bool DoLoadIdempotency(NTable::TDatabase& database) = 0;
    virtual const std::shared_ptr<NDataSharing::TSharedBlobsManager>& DoGetSharedBlobsManager() const = 0;

public:
    static const inline TString DefaultStorageId = NBlobOperations::TGlobal::DefaultStorageId;
    static const inline TString MemoryStorageId = NBlobOperations::TGlobal::MemoryStorageId;
    static const inline TString LocalMetadataStorageId = NBlobOperations::TGlobal::LocalMetadataStorageId;
    virtual ~IStoragesManager() = default;

    void Initialize(const ui64 generation) {
        Generation = generation;
        AFL_VERIFY(!Initialized);
        Initialized = true;
        DoInitialize();
    }

    ui64 GetGeneration() const {
        AFL_VERIFY(Generation);
        return *Generation;
    }

    const std::shared_ptr<NDataSharing::TSharedBlobsManager>& GetSharedBlobsManager() const {
        AFL_VERIFY(Initialized);
        return DoGetSharedBlobsManager();
    }

    bool LoadIdempotency(NTable::TDatabase& database);
    bool HasBlobsToDelete() const;
    void Stop();

    std::shared_ptr<IBlobsStorageOperator> GetDefaultOperator() const {
        return GetOperatorVerified(DefaultStorageId);
    }

    std::shared_ptr<IBlobsStorageOperator> GetInsertOperator() const {
        return GetDefaultOperator();
    }

    const THashMap<TString, std::shared_ptr<IBlobsStorageOperator>>& GetStorages() const {
        AFL_VERIFY(Initialized);
        return Constructed;
    }

    void OnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers);

    std::shared_ptr<IBlobsStorageOperator> GetOperator(const TString& storageIdExt);
    std::shared_ptr<IBlobsStorageOperator> GetOperatorGuarantee(const TString& storageIdExt);
    std::shared_ptr<IBlobsStorageOperator> GetOperatorVerified(const TString& storageIdExt) const;
    std::shared_ptr<IBlobsStorageOperator> GetOperatorOptional(const TString& storageIdExt) const;
};


}
