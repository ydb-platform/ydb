#include "checker_access.h"

#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NMetadata::NSecret {

void TAccessPreparationActor::StartChecker() {
    Y_ABORT_UNLESS(Secrets);
    auto g = PassAwayGuard();
    for (auto&& i : Objects) {
        if (Context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Create) {
            bool foundSecret = false;
            for (auto&& [_, s] : Secrets->GetSecrets()) {
                if (s.GetOwnerUserId() == i.GetOwnerUserId() && s.GetSecretId() == i.GetSecretId()) {
                    foundSecret = true;
                    break;
                }
            }
            if (!foundSecret) {
                Controller->OnPreparationProblem("used in access secret " + i.GetSecretId() + " not found");
                return;
            }
        }
    }
    Controller->OnPreparationFinished(std::move(Objects));
}

void TAccessPreparationActor::Handle(NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    Secrets = ev->Get()->GetValidatedSnapshotAs<NMetadata::NSecret::TSnapshot>();
    StartChecker();
}

void TAccessPreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NProvider::MakeServiceId(SelfId().NodeId()),
        new NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
}

TAccessPreparationActor::TAccessPreparationActor(std::vector<TAccess>&& objects,
    NModifications::IAlterPreparationController<TAccess>::TPtr controller,
    const NModifications::IOperationsManager::TInternalModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
