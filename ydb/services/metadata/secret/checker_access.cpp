#include "checker_access.h"

#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NMetadata::NSecret {

void TAccessPreparationActor::StartChecker() {
    Y_VERIFY(Secrets);
    auto g = PassAwayGuard();
    for (auto&& i : Objects) {
        if (Context.GetActivityType() == IOperationsManager::EActivityType::Create) {
            bool foundSecret = false;
            for (auto&& [_, s] : Secrets->GetSecrets()) {
                if (s.GetOwnerUserId() == i.GetOwnerUserId() && s.GetSecretId() == i.GetSecretId()) {
                    foundSecret = true;
                    break;
                }
            }
            if (!foundSecret) {
                Controller->PreparationProblem("used in access secret " + i.GetSecretId() + " not found");
                return;
            }
        }
    }
    Controller->PreparationFinished(std::move(Objects));
}

void TAccessPreparationActor::Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
    Secrets = ev->Get()->GetValidatedSnapshotAs<NMetadata::NSecret::TSnapshot>();
    StartChecker();
}

void TAccessPreparationActor::Bootstrap() {
    Become(&TThis::StateMain);
    Send(NMetadataProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadataProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
}

TAccessPreparationActor::TAccessPreparationActor(std::vector<TAccess>&& objects,
    NMetadataManager::IAlterPreparationController<TAccess>::TPtr controller,
    const NMetadata::IOperationsManager::TModificationContext& context)
    : Objects(std::move(objects))
    , Controller(controller)
    , Context(context)
{

}

}
