#include "path_cleaner.h"
#ifndef KIKIMR_DISABLE_S3_OPS
#include "external_data.h"

#include <ydb/services/metadata/service.h>

namespace NKikimr::NColumnShard::NTiers {

void TPathCleaner::Handle(TEvTierCleared::TPtr& ev) {
    TiersWait.erase(ev->Get()->GetTierName());
    Truncated |= ev->Get()->GetTruncated();
    if (TiersWait.empty()) {
        if (Truncated) {
            Controller->TaskInterrupted(nullptr);
        } else {
            Controller->TaskFinished();
        }
    }
}

NMetadataProvider::ISnapshotParser::TPtr TPathCleaner::GetTieringSnapshotParser() const {
    if (!ExternalDataManipulation) {
        ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>();
        auto edmPtr = std::dynamic_pointer_cast<NTiers::TSnapshotConstructor>(ExternalDataManipulation);
        edmPtr->Start(edmPtr);
    }
    return ExternalDataManipulation;
}

void TPathCleaner::Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
    auto configsSnapshot = ev->Get()->GetSnapshotAs<TConfigsSnapshot>();
    Y_VERIFY(configsSnapshot);
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()), new NMetadataProvider::TEvUnsubscribeExternal(GetTieringSnapshotParser()));
    std::vector<TTierConfig> configs = configsSnapshot->GetTiersForPathId(PathId);
    for (auto&& i : configs) {
        auto config = NWrappers::IExternalStorageConfig::Construct(i.GetProtoConfig().GetObjectStorage());
        if (!config) {
            ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot construct storage config for " << i.GetTierName();
            continue;
        }
        Register(new TTierCleaner(i.GetTierName(), SelfId(), PathId, config));
        TiersWait.emplace(i.GetTierName());
    }
}

void TPathCleaner::Bootstrap() {
    Become(&TPathCleaner::StateMain);
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()), new NMetadataProvider::TEvSubscribeExternal(GetTieringSnapshotParser()));
}

TPathCleaner::TPathCleaner(const ui64 pathId, NBackgroundTasks::ITaskExecutorController::TPtr controller)
    : PathId(pathId)
    , Controller(controller) {
}

}
#endif
