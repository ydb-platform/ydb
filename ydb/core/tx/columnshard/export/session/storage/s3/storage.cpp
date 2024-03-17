#include "storage.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<std::shared_ptr<IBlobsStorageOperator>> TS3StorageInitializer::DoInitializeOperator(const std::shared_ptr<IStoragesManager>& /*storages*/) const {
    auto extStorageConfig = NWrappers::NExternalStorage::IExternalStorageConfig::Construct(S3Settings);
    if (!extStorageConfig) {
        return TConclusionStatus::Fail("cannot build operator with this config: " + S3Settings.DebugString());
    }
    return std::shared_ptr<IBlobsStorageOperator>(new NBlobOperations::NTier::TOperator("__EXPORT:" + StorageName, NActors::TActorId(), extStorageConfig,
        std::make_shared<NDataSharing::TStorageSharedBlobsManager>("__EXPORT:" + StorageName, (TTabletId)0)));
}

}