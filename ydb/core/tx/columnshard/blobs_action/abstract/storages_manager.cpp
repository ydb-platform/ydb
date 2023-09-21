#include "storages_manager.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/tiering/manager.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> IStoragesManager::GetOperator(const TString& storageId) {
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

std::shared_ptr<IBlobsStorageOperator> IStoragesManager::InitializePortionOperator(const TPortionInfo& portionInfo) {
    Y_VERIFY(!portionInfo.HasStorageOperator());
    if (portionInfo.GetMeta().GetTierName()) {
        return GetOperator(portionInfo.GetMeta().GetTierName());
    } else {
        return GetOperator(DefaultStorageId);
    }
}

void IStoragesManager::OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers) {
    for (auto&& i : tiers->GetManagers()) {
        GetOperator(i.second.GetTierName())->OnTieringModified(tiers);
    }
}

void IStoragesManager::InitializeNecessaryStorages() {
    GetOperator(DefaultStorageId);
}

}
