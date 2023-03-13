#pragma once
#include "access.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NMetadata::NSecret {

class TAccessPreparationActor: public NActors::TActorBootstrapped<TAccessPreparationActor> {
private:
    std::vector<TAccess> Objects;
    NModifications::IAlterPreparationController<TAccess>::TPtr Controller;
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

    TAccessPreparationActor(std::vector<TAccess>&& objects,
        NModifications::IAlterPreparationController<TAccess>::TPtr controller,
        const NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
