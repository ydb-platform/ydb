#pragma once
#include "object.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tiering/fetcher/list.h>
#include <ydb/core/tx/tiering/rule/ss_fetcher.h>
#include <ydb/core/tx/tiering/tier/snapshot.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

class TTierPreparationActor: public NActors::TActorBootstrapped<TTierPreparationActor> {
private:
    enum EState {
        INITIAL = 0,
        FETCH_TIERING,
        CHECK_PERMISSIONS,
        MAKE_RESULT,
    };

    EState State = INITIAL;
    std::vector<TTierConfig> Objects;
    NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr Controller;
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;
    std::shared_ptr<TTiersSnapshot> Tiers;
    std::optional<THashMap<TString, TTieringRule>> TieringRules;
    std::shared_ptr<TFetcherCheckUserTieringPermissions> SSFetcher;
    std::optional<TFetcherCheckUserTieringPermissions::TResult> SSCheckResult;
    void StartChecker();
    void StartSSFetcher();
    void AdvanceCheckerState();
protected:
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(NSchemeShard::TEvSchemeShard::TEvProcessingResponse::TPtr& ev);
    void Handle(TEvListTieringRulesResult::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvProcessingResponse, Handle);
            hFunc(TEvListTieringRulesResult, Handle);
            default:
                break;
        }
    }
    void Bootstrap();

    TTierPreparationActor(std::vector<TTierConfig>&& objects,
        NMetadata::NModifications::IAlterPreparationController<TTierConfig>::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
