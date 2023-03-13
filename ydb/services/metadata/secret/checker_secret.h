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
    NModifications::IAlterPreparationController<TSecret>::TPtr Controller;
    NModifications::IOperationsManager::TInternalModificationContext Context;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    void StartChecker();
protected:
    void Handle(NProvider::TEvRefreshSubscriberData::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }
    void Bootstrap();

    TSecretPreparationActor(std::vector<TSecret>&& objects,
        NModifications::IAlterPreparationController<TSecret>::TPtr controller,
        const NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
