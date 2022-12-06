#pragma once

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/secret.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NMetadata::NSecret {

class TSecretPreparationActor: public NActors::TActorBootstrapped<TSecretPreparationActor> {
private:
    std::vector<TSecret> Objects;
    NMetadataManager::IAlterPreparationController<TSecret>::TPtr Controller;
    NMetadata::IOperationsManager::TModificationContext Context;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    void StartChecker();
protected:
    void Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadataProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }
    void Bootstrap();

    TSecretPreparationActor(std::vector<TSecret>&& objects,
        NMetadataManager::IAlterPreparationController<TSecret>::TPtr controller,
        const NMetadata::IOperationsManager::TModificationContext& context);
};

}
