#include "path_cleaner.h"
#ifndef KIKIMR_DISABLE_S3_OPS
#include "external_data.h"

#include <ydb/services/metadata/service.h>
#include <ydb/services/metadata/secret/fetcher.h>

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

NMetadataProvider::ISnapshotsFetcher::TPtr TPathCleaner::GetTieringSnapshotParser() const {
    return std::make_shared<NTiers::TSnapshotConstructor>();
}

NMetadataProvider::ISnapshotsFetcher::TPtr TPathCleaner::GetSecretsSnapshotParser() const {
    return std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>();
}

void TPathCleaner::Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto configs = ev->Get()->GetSnapshotPtrAs<TConfigsSnapshot>()) {
        Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()), new NMetadataProvider::TEvUnsubscribeExternal(GetTieringSnapshotParser()));
        Configs = configs;
    } else if (auto secrets = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()), new NMetadataProvider::TEvUnsubscribeExternal(GetSecretsSnapshotParser()));
        Secrets = secrets;
    } else {
        Y_VERIFY(false);
    }

    if (Configs && Secrets) {
        const TTieringRule* rule = Configs->GetTieringById(TieringId);
        if (!rule) {
            ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot detect tiering for " << TieringId;
            Controller->TaskFinished();
            return;
        }
        for (auto&& i : rule->GetIntervals()) {
            const auto tier = Configs->GetTierById(i.GetTierName());
            if (!tier) {
                ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot detect tiering for " << TieringId;
                continue;
            }
            auto config = NWrappers::IExternalStorageConfig::Construct(tier->GetPatchedConfig(Secrets));
            if (!config) {
                ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot construct storage config for " << i.GetTierName();
                continue;
            }
            Register(new TTierCleaner(i.GetTierName(), SelfId(), PathId, config));
            TiersWait.emplace(i.GetTierName());
        }
    }
}

void TPathCleaner::Bootstrap() {
    Become(&TPathCleaner::StateMain);
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()), new NMetadataProvider::TEvSubscribeExternal(GetTieringSnapshotParser()));
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()), new NMetadataProvider::TEvSubscribeExternal(GetSecretsSnapshotParser()));
}

TPathCleaner::TPathCleaner(const TString& tieringId, const ui64 pathId, NBackgroundTasks::ITaskExecutorController::TPtr controller)
    : PathId(pathId)
    , TieringId(tieringId)
    , Controller(controller) {
}

}
#endif
