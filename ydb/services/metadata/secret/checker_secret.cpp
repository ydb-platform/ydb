#include "checker_secret.h"

#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NMetadata::NSecret {

void TSecretPreparationActor::StartChecker() {
    Y_VERIFY(Secrets);
    auto g = PassAwayGuard();
    for (auto&& i : Objects) {
        if (Context.GetActivityType() == IOperationsManager::EActivityType::Alter) {
            if (!Secrets->GetSecrets().contains(i)) {
                Controller->PreparationProblem("secret " + i.GetSecretId() + " not found for alter");
                return;
            }
        }
        for (auto&& sa : Secrets->GetAccess()) {
            if (Context.GetActivityType() == IOperationsManager::EActivityType::Drop) {
                if (sa.GetOwnerUserId() == i.GetOwnerUserId() && sa.GetSecretId() == i.GetSecretId()) {
                    Controller->PreparationProblem("secret " + i.GetSecretId() + " using in access for " + sa.GetAccessSID());
                    return;
                }
            }
        }
    }
    Controller->PreparationFinished(std::move(Objects));
}

void TSecretPreparationActor::Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
    Secrets = ev->Get()->GetValidatedSnapshotAs<NMetadata::NSecret::TSnapshot>();
    StartChecker();
}

void TSecretPreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadataProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
}

TSecretPreparationActor::TSecretPreparationActor(std::vector<TSecret>&& objects,
    NMetadataManager::IAlterPreparationController<TSecret>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
