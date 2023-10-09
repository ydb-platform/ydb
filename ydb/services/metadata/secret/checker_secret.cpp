#include "checker_secret.h"

#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NMetadata::NSecret {

void TSecretPreparationActor::StartChecker() {
    Y_ABORT_UNLESS(Secrets);
    auto g = PassAwayGuard();
    for (auto&& i : Objects) {
        if (Context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Alter) {
            if (!Secrets->GetSecrets().contains(i)) {
                Controller->OnPreparationProblem("secret " + i.GetSecretId() + " not found for alter");
                return;
            }
        }
        for (auto&& sa : Secrets->GetAccess()) {
            if (Context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Drop) {
                if (sa.GetOwnerUserId() == i.GetOwnerUserId() && sa.GetSecretId() == i.GetSecretId()) {
                    Controller->OnPreparationProblem("secret " + i.GetSecretId() + " using in access for " + sa.GetAccessSID());
                    return;
                }
            }
        }
    }
    Controller->OnPreparationFinished(std::move(Objects));
}

void TSecretPreparationActor::Handle(NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    Secrets = ev->Get()->GetValidatedSnapshotAs<TSnapshot>();
    StartChecker();
}

void TSecretPreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NProvider::MakeServiceId(SelfId().NodeId()),
        new NProvider::TEvAskSnapshot(std::make_shared<TSnapshotsFetcher>()));
}

TSecretPreparationActor::TSecretPreparationActor(std::vector<TSecret>&& objects,
    NModifications::IAlterPreparationController<TSecret>::TPtr controller,
    const NModifications::IOperationsManager::TInternalModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
