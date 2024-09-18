#pragma once
#include "object.h"

#include <ydb/core/tx/tiering/snapshot.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

class TRulePreparationActor: public NActors::TActorBootstrapped<TRulePreparationActor> {
private:
    std::vector<TTieringRule> Objects;
    NMetadata::NModifications::IAlterPreparationController<TTieringRule>::TPtr Controller;
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;
    std::shared_ptr<TConfigsSnapshot> Tierings;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    void StartChecker();
protected:
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }
    void Bootstrap();

    TRulePreparationActor(std::vector<TTieringRule>&& objects,
        NMetadata::NModifications::IAlterPreparationController<TTieringRule>::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
