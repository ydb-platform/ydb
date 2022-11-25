#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TTierPreparationActor::StartChecker() {
    auto g = PassAwayGuard();
    for (auto&& tier : Objects) {
        if (Context.GetActivityType() == NMetadata::IOperationsManager::EActivityType::Drop) {
            for (auto&& i : Tierings->GetTableTierings()) {
                if (i.second.ContainsTier(tier.GetTierName())) {
                    Controller->PreparationProblem("tier in usage for tiering " + i.first);
                    return;
                }
            }
        }
        if (!Secrets->CheckSecretAccess(tier.GetProtoConfig().GetObjectStorage().GetAccessKey(), Context.GetUserToken())) {
            Controller->PreparationProblem("no access for secret: " + tier.GetProtoConfig().GetObjectStorage().GetAccessKey());
            return;
        } else if (!Secrets->CheckSecretAccess(tier.GetProtoConfig().GetObjectStorage().GetSecretKey(), Context.GetUserToken())) {
            Controller->PreparationProblem("no access for secret: " + tier.GetProtoConfig().GetObjectStorage().GetSecretKey());
            return;
        }
    }
    Controller->PreparationFinished(std::move(Objects));
}

void TTierPreparationActor::Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else if (auto snapshot = ev->Get()->GetSnapshotPtrAs<TConfigsSnapshot>()) {
        Tierings = snapshot;
    } else {
        Y_VERIFY(false);
    }
    if (Secrets && Tierings) {
        StartChecker();
    }
}

void TTierPreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadataProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadataProvider::TEvAskSnapshot(std::make_shared<TSnapshotConstructor>()));
}

TTierPreparationActor::TTierPreparationActor(std::vector<TTierConfig>&& objects,
    NMetadataManager::IAlterPreparationController<TTierConfig>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
