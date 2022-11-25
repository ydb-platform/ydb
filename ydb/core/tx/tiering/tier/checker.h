#pragma once
#include "object.h"

#include <ydb/core/tx/tiering/snapshot.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

class TTierPreparationActor: public NActors::TActorBootstrapped<TTierPreparationActor> {
private:
    std::vector<TTierConfig> Objects;
    NMetadataManager::IAlterPreparationController<TTierConfig>::TPtr Controller;
    NMetadata::IOperationsManager::TModificationContext Context;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    std::shared_ptr<TConfigsSnapshot> Tierings;
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

    TTierPreparationActor(std::vector<TTierConfig>&& objects,
        NMetadataManager::IAlterPreparationController<TTierConfig>::TPtr controller,
        const NMetadata::IOperationsManager::TModificationContext& context);
};

}
