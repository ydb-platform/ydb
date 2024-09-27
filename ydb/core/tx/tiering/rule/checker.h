#pragma once
#include "object.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tiering/snapshot.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/scheme_manager.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

class TRulePreprocessingActor: public NActors::TActorBootstrapped<TRulePreprocessingActor> {
private:
    using IController = NMetadata::NModifications::IPreprocessingController;

    NYql::TObjectSettingsImpl Settings;
    IController::TPtr Controller;
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;
    std::shared_ptr<TConfigsSnapshot> Tierings;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;

private:
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

    TRulePreprocessingActor(NYql::TObjectSettingsImpl settings, IController::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context);
};
}
