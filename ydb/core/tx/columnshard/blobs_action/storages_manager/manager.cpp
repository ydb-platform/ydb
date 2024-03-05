#include "manager.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#ifndef KIKIMR_DISABLE_S3_OPS
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#endif
#include <ydb/core/wrappers/fake_storage_config.h>
#include <ydb/core/wrappers/fake_storage.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> TStoragesManager::DoBuildOperator(const TString& storageId) {
    if (storageId == TBase::DefaultStorageId) {
        return std::make_shared<NOlap::NBlobOperations::NBlobStorage::TOperator>(storageId, Shard.SelfId(), Shard.Info(),
            Shard.Executor()->Generation(), SharedBlobsManager->GetStorageManagerGuarantee(storageId));
    } else if (storageId == TBase::MemoryStorageId) {
        {
            static TMutex mutexLocal;
            TGuard<TMutex> g(mutexLocal);
            Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");
        }
        return std::make_shared<NOlap::NBlobOperations::NTier::TOperator>(storageId, Shard.SelfId(), std::make_shared<NWrappers::NExternalStorage::TFakeExternalStorageConfig>("fakeBucket", "fakeSecret"),
            SharedBlobsManager->GetStorageManagerGuarantee(storageId));
    } else if (!Shard.Tiers) {
        return nullptr;
    } else {
#ifndef KIKIMR_DISABLE_S3_OPS
        return std::make_shared<NOlap::NBlobOperations::NTier::TOperator>(storageId, Shard, SharedBlobsManager->GetStorageManagerGuarantee(storageId));
#else
        return nullptr;
#endif
    }
}

bool TStoragesManager::DoLoadIdempotency(NTable::TDatabase& database) {
    return SharedBlobsManager->LoadIdempotency(database);
}

TStoragesManager::TStoragesManager(NColumnShard::TColumnShard& shard)
    : Shard(shard)
    , SharedBlobsManager(std::make_shared<NDataSharing::TSharedBlobsManager>((TTabletId)Shard.TabletID()))
{

}

}