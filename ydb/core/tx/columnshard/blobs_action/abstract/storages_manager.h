#pragma once
#include "storage.h"

namespace NKikimr::NOlap {

struct TPortionInfo;

class IStoragesManager {
private:
    TRWMutex RWMutex;
protected:
    virtual std::shared_ptr<IBlobsStorageOperator> DoBuildOperator(const TString& storageId) = 0;
    THashMap<TString, std::shared_ptr<IBlobsStorageOperator>> Constructed;
    std::shared_ptr<IBlobsStorageOperator> BuildOperator(const TString& storageId) {
        auto result = DoBuildOperator(storageId);
        Y_VERIFY(result);
        return result;
    }

    virtual void InitializeNecessaryStorages();
public:
    static const inline TString DefaultStorageId = "__DEFAULT";
    virtual ~IStoragesManager() = default;

    IStoragesManager() = default;

    std::shared_ptr<IBlobsStorageOperator> GetDefaultOperator() {
        return GetOperator(DefaultStorageId);
    }

    std::shared_ptr<IBlobsStorageOperator> GetInsertOperator() {
        return GetDefaultOperator();
    }

    const THashMap<TString, std::shared_ptr<IBlobsStorageOperator>>& GetStorages() {
        InitializeNecessaryStorages();
        return Constructed;
    }

    void OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers);

    std::shared_ptr<IBlobsStorageOperator> GetOperator(const TString& storageIdExt);
    std::shared_ptr<IBlobsStorageOperator> InitializePortionOperator(const TPortionInfo& portionInfo);
};


}
