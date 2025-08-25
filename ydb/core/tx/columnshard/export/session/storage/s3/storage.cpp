#include "storage.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#ifndef KIKIMR_DISABLE_S3_OPS
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#include <ydb/core/wrappers/abstract.h>
#endif
#include <ydb/core/tx/columnshard/counters/error_collector.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>

namespace {
static std::shared_ptr<NKikimr::NColumnShard::TErrorCollector> DummyCollector = std::make_shared<NKikimr::NColumnShard::TErrorCollector>();
}

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<std::shared_ptr<IBlobsStorageOperator>> TS3StorageInitializer::DoInitializeOperator(
    const std::shared_ptr<IStoragesManager>& storages) const {
#ifndef KIKIMR_DISABLE_S3_OPS
    auto extStorageConfig = NWrappers::NExternalStorage::IExternalStorageConfig::Construct(S3Settings);
    if (!extStorageConfig) {
        return TConclusionStatus::Fail("cannot build operator with this config: " + S3Settings.DebugString());
    }
    return std::shared_ptr<IBlobsStorageOperator>(
        new NBlobOperations::NTier::TOperator("__EXPORT:" + StorageName, NActors::TActorId(), extStorageConfig,
            std::make_shared<NDataSharing::TStorageSharedBlobsManager>(
                "__EXPORT:" + StorageName, storages->GetSharedBlobsManager()->GetSelfTabletId()),
            storages->GetGeneration(), DummyCollector));
#else
    Y_UNUSED(storages);
    return TConclusionStatus::Fail("s3 not supported");
#endif
}

}   // namespace NKikimr::NOlap::NExport
