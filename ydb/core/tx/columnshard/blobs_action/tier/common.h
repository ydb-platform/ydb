#pragma once

#include <ydb/core/wrappers/abstract.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TExternalStorageOperatorHolder {
private:
    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr StorageOperator = nullptr;
    TAdaptiveLock Mutex;

public:
    void Emplace(const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr& storageOperator) {
        TGuard g(Mutex);
        StorageOperator = storageOperator;
    }

    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr Get() const {
        TGuard g(Mutex);
        return StorageOperator;
    }
};

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
