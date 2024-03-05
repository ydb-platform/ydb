#pragma once
#include "storage.h"

namespace NKikimr::NOlap {

class TPortionInfo;

class IStoragesManager {
private:
    TRWMutex RWMutex;
    bool Initialized = false;
protected:
    virtual std::shared_ptr<IBlobsStorageOperator> DoBuildOperator(const TString& storageId) = 0;
    THashMap<TString, std::shared_ptr<IBlobsStorageOperator>> Constructed;
    std::shared_ptr<IBlobsStorageOperator> BuildOperator(const TString& storageId);

    virtual void DoInitialize();
    virtual bool DoLoadIdempotency(NTable::TDatabase& database) = 0;
    virtual const std::shared_ptr<NDataSharing::TSharedBlobsManager>& DoGetSharedBlobsManager() const = 0;

public:
    static const inline TString DefaultStorageId = "__DEFAULT";
    static const inline TString MemoryStorageId = "__MEMORY";
    virtual ~IStoragesManager() = default;

    void Initialize() {
        AFL_VERIFY(!Initialized);
        Initialized = true;
        DoInitialize();
    }

    IStoragesManager() = default;
    const std::shared_ptr<NDataSharing::TSharedBlobsManager>& GetSharedBlobsManager() const {
        AFL_VERIFY(Initialized);
        return DoGetSharedBlobsManager();
    }

    bool LoadIdempotency(NTable::TDatabase& database);

    bool HasBlobsToDelete() const;

    void Stop();

    std::shared_ptr<IBlobsStorageOperator> GetDefaultOperator() {
        return GetOperator(DefaultStorageId);
    }

    std::shared_ptr<IBlobsStorageOperator> GetInsertOperator() {
        return GetDefaultOperator();
    }

    const THashMap<TString, std::shared_ptr<IBlobsStorageOperator>>& GetStorages() {
        AFL_VERIFY(Initialized);
        return Constructed;
    }

    void OnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers);

    std::shared_ptr<IBlobsStorageOperator> GetOperator(const TString& storageIdExt);
    std::shared_ptr<IBlobsStorageOperator> GetOperatorGuarantee(const TString& storageIdExt);
    std::shared_ptr<IBlobsStorageOperator> GetOperatorVerified(const TString& storageIdExt);
};


}
