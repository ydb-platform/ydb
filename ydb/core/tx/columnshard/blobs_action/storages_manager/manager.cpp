#include "manager.h"

#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/local/storage.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#ifndef KIKIMR_DISABLE_S3_OPS
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#endif
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/core/wrappers/fake_storage_config.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> TStoragesManager::DoBuildOperator(const TString& storageId) {
    if (storageId == TBase::DefaultStorageId) {
        return std::make_shared<NOlap::NBlobOperations::NBlobStorage::TOperator>(
            storageId, Shard.SelfId(), Shard.Info(), Shard.Executor()->Generation(), SharedBlobsManager->GetStorageManagerGuarantee(storageId));
    } else if (storageId == TBase::LocalMetadataStorageId) {
        return std::make_shared<NOlap::NBlobOperations::NLocal::TOperator>(storageId, SharedBlobsManager->GetStorageManagerGuarantee(storageId));
    } else if (storageId == TBase::MemoryStorageId) {
#ifndef KIKIMR_DISABLE_S3_OPS
        {
            static TMutex mutexLocal;
            TGuard<TMutex> g(mutexLocal);
            Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");
        }
        return std::make_shared<NOlap::NBlobOperations::NTier::TOperator>(storageId, Shard.SelfId(),
            std::make_shared<NWrappers::NExternalStorage::TFakeExternalStorageConfig>("fakeBucket", "fakeSecret"),
            SharedBlobsManager->GetStorageManagerGuarantee(storageId), Shard.Executor()->Generation());
#else
        return nullptr;
#endif
    } else if (!Shard.Tiers) {
        return nullptr;
    } else {
#ifndef KIKIMR_DISABLE_S3_OPS
        return std::make_shared<NOlap::NBlobOperations::NTier::TOperator>(
            storageId, Shard, SharedBlobsManager->GetStorageManagerGuarantee(storageId));
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
    , SharedBlobsManager(std::make_shared<NDataSharing::TSharedBlobsManager>((TTabletId)Shard.TabletID())) {
}

}   // namespace NKikimr::NOlap
