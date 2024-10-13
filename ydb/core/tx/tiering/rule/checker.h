#pragma once
#include "object.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tiering/fetcher/list.h>
#include <ydb/core/tx/tiering/rule/ss_fetcher.h>
#include <ydb/core/tx/tiering/tier/snapshot.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/manager/scheme_manager.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRulePreparationActor: public NActors::TActorBootstrapped<TTieringRulePreparationActor> {
private:
    NKikimrSchemeOp::TTieringRuleProperties TieringRule;
    NMetadata::NModifications::IBuildRequestController::TPtr Controller;
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    std::shared_ptr<TTiersSnapshot> Tiers;
    void StartChecker();
    void AdvanceCheckerState();
    void ReplySuccess();
protected:
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
public:
    STATEFN(StateFetchTiering) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }
    void Bootstrap();

    TTieringRulePreparationActor(NKikimrSchemeOp::TTieringRuleProperties tieringRule,
        NMetadata::NModifications::IBuildRequestController::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
