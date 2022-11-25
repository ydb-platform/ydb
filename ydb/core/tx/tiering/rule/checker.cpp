#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TRulePreparationActor::StartChecker() {
    auto g = PassAwayGuard();

    for (auto&& tiering : Objects) {
        for (auto&& interval : tiering.GetIntervals()) {
            auto tier = Tierings->GetTierById(interval.GetTierName());
            if (!tier) {
                Controller->PreparationProblem("unknown tier usage: " + interval.GetTierName());
                return;
            } else if (!Secrets->CheckSecretAccess(tier->GetProtoConfig().GetObjectStorage().GetAccessKey(), Context.GetUserToken())) {
                Controller->PreparationProblem("no access for secret: " + tier->GetProtoConfig().GetObjectStorage().GetAccessKey());
                return;
            } else if (!Secrets->CheckSecretAccess(tier->GetProtoConfig().GetObjectStorage().GetSecretKey(), Context.GetUserToken())) {
                Controller->PreparationProblem("no access for secret: " + tier->GetProtoConfig().GetObjectStorage().GetSecretKey());
                return;
            }
        }
    }
    Controller->PreparationFinished(std::move(Objects));
}

void TRulePreparationActor::Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<TConfigsSnapshot>()) {
        Tierings = snapshot;
    } else if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else {
        Y_VERIFY(false);
    }
    if (Tierings && Secrets) {
        StartChecker();
    }
}

void TRulePreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadataProvider::TEvAskSnapshot(std::make_shared<TSnapshotConstructor>()));
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadataProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
}

TRulePreparationActor::TRulePreparationActor(std::vector<TTieringRule>&& objects,
    NMetadataManager::IAlterPreparationController<TTieringRule>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
