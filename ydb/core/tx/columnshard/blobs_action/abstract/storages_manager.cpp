#include "storages_manager.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/tiering/manager.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> IStoragesManager::GetOperatorOptional(const TString& storageId) const {
    AFL_VERIFY(Initialized);
    AFL_VERIFY(storageId);
    TReadGuard rg(RWMutex);
    auto it = Constructed.find(storageId);
    if (it != Constructed.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> IStoragesManager::GetOperatorVerified(const TString& storageId) const {
    auto result = GetOperatorOptional(storageId);
    AFL_VERIFY(result)("storage_id", storageId);
    return result;
}

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> IStoragesManager::GetOperatorGuarantee(const TString& storageId) {
    TReadGuard rg(RWMutex);
    auto it = Constructed.find(storageId);
    if (it == Constructed.end()) {
        rg.Release();
        TWriteGuard wg(RWMutex);
        it = Constructed.find(storageId);
        if (it == Constructed.end()) {
            it = Constructed.emplace(storageId, BuildOperator(storageId)).first;
        }
        return it->second;
    }
    return it->second;
}

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> IStoragesManager::GetOperator(const TString& storageId) {
    return GetOperatorGuarantee(storageId);
}

void IStoragesManager::OnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers) {
    AFL_VERIFY(tiers);
    for (auto&& i : tiers->GetManagers()) {
        GetOperatorGuarantee(i.first)->OnTieringModified(tiers);
    }
}

void IStoragesManager::DoInitialize() {
    GetOperator(DefaultStorageId);
    GetOperator(MemoryStorageId);
    GetOperator(LocalMetadataStorageId);
}

bool IStoragesManager::LoadIdempotency(NTable::TDatabase& database) {
    AFL_VERIFY(Initialized);
    if (!DoLoadIdempotency(database)) {
        return false;
    }
    TBlobManagerDb blobsDB(database);
    for (auto&& i : GetStorages()) {
        if (!i.second->Load(blobsDB)) {
            return false;
        }
    }
    GetOperatorVerified(DefaultStorageId);
    GetSharedBlobsManager()->GetStorageManagerVerified(DefaultStorageId);
    return true;
}

bool IStoragesManager::HasBlobsToDelete() const {
    for (auto&& i : Constructed) {
        if (!i.second->GetBlobsToDelete().IsEmpty()) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> IStoragesManager::BuildOperator(const TString& storageId) {
    auto result = DoBuildOperator(storageId);
    AFL_VERIFY(result)("storage_id", storageId);
    return result;
}

void IStoragesManager::Stop() {
    AFL_VERIFY(!Finished);
    if (Initialized && !Finished) {
        for (auto&& i : Constructed) {
            i.second->Stop();
        }
        Finished = true;
    }
}

}
