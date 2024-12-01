#include "checker_secret.h"

#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NMetadata::NSecret {

void TSecretPreparationActor::StartChecker() {
    Y_ABORT_UNLESS(Secrets);
    auto g = PassAwayGuard();
    THashMap<TString, TString> secretNameToOwner;
    for (auto&& object : Objects) {
        if (Context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Create ||
            Context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Upsert) {
            const auto* findSecret = secretNameToOwner.FindPtr(object.GetSecretId());
            if (findSecret && *findSecret != object.GetOwnerUserId()) {
                Controller->OnPreparationProblem("cannot create multiple secrets with same id: " + object.GetSecretId());
                return;
            }
        }
        if (Context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Alter) {
            if (!Secrets->GetSecrets().contains(object)) {
                Controller->OnPreparationProblem("secret " + object.GetSecretId() + " not found for alter");
                return;
            }
        }
        for (auto&& sa : Secrets->GetAccess()) {
            if (Context.GetActivityType() == NModifications::IOperationsManager::EActivityType::Drop) {
                if (sa.GetOwnerUserId() == object.GetOwnerUserId() && sa.GetSecretId() == object.GetSecretId()) {
                    Controller->OnPreparationProblem("secret " + object.GetSecretId() + " using in access for " + sa.GetAccessSID());
                    return;
                }
            }
        }
        secretNameToOwner.emplace(object.GetSecretId(), object.GetOwnerUserId());
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
