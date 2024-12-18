#include "checker.h"

#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

void TTierPreparationActor::StartChecker() {
    if (!Secrets) {
        return;
    }
    auto g = PassAwayGuard();
    if (const auto& userToken = Context.GetExternalData().GetUserToken()) {
        for (auto&& tier : Objects) {
            if (!Secrets->CheckSecretAccess(tier.GetAccessKey(), *userToken)) {
                Controller->OnPreparationProblem("no access for secret: " + tier.GetAccessKey().DebugString());
                return;
            } else if (!Secrets->CheckSecretAccess(tier.GetSecretKey(), *userToken)) {
                Controller->OnPreparationProblem("no access for secret: " + tier.GetSecretKey().DebugString());
                return;
            }
        }
    }
    Controller->OnPreparationFinished(std::move(Objects));
}

void TTierPreparationActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        Secrets = snapshot;
    } else {
        Y_ABORT_UNLESS(false);
    }
    StartChecker();
}

void TTierPreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
}

TTierPreparationActor::TTierPreparationActor(std::vector<TTierConfig>&& objects,
    NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
