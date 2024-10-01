#pragma once
#include "object.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tiering/snapshot.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/scheme_manager.h>
#include <ydb/services/metadata/secret/snapshot.h>

namespace NKikimr::NColumnShard::NTiers {

class TTierPreprocessingActor: public NActors::TActorBootstrapped<TTierPreprocessingActor> {
private:
    using IController = NMetadata::NModifications::IPreprocessingController;

    NYql::TObjectSettingsImpl Settings;
    NMetadata::NModifications::IPreprocessingController::TPtr Controller;
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;
    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;

private:
    void StartChecker();
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);

    TConclusionStatus PreprocessObjectStorage(NKikimrSchemeOp::TS3Settings& config) const;
    TConclusionStatus CheckSecretAccess(const TString& secretOrValue) const;
    TConclusionStatus CheckSecretAccess(const NKikimrSchemeOp::TSecretableVariable& secretOrValue) const;

public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            default:
                break;
        }
    }
    void Bootstrap();

    TTierPreprocessingActor(NYql::TObjectSettingsImpl settings, IController::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
