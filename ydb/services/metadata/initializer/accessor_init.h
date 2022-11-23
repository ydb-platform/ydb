#pragma once
#include "common.h"
#include "controller.h"
#include "events.h"
#include "snapshot.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/ds_table/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadataInitializer {

class TDSAccessorInitialized: public NActors::TActorBootstrapped<TDSAccessorInitialized> {
private:
    TDeque<ITableModifier::TPtr> Modifiers;
    const NInternal::NRequest::TConfig Config;
    NMetadata::IInitializationBehaviour::TPtr InitializationBehaviour;
    IInitializerOutput::TPtr ExternalController;
    TInitializerInput::TPtr InternalController;
    std::shared_ptr<NMetadataInitializer::TSnapshot> InitializationSnapshot;
    const TString ComponentId;
    void Handle(TEvInitializerPreparationStart::TPtr& ev);
    void Handle(TEvInitializerPreparationFinished::TPtr& ev);
    void Handle(TEvInitializerPreparationProblem::TPtr& ev);
    void Handle(NInternal::NRequest::TEvRequestFinished::TPtr& ev);
    void Handle(NMetadataManager::TEvModificationFinished::TPtr& ev);
    void Handle(NMetadataManager::TEvModificationProblem::TPtr& ev);
    void DoNextModifier();
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::METADATA_INITIALIZER;
    }

    void Bootstrap();
    TDSAccessorInitialized(const NInternal::NRequest::TConfig& config,
        const TString& componentId,
        NMetadata::IInitializationBehaviour::TPtr initializationBehaviour,
        IInitializerOutput::TPtr controller, std::shared_ptr<NMetadataInitializer::TSnapshot> initializationSnapshot);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestFinished, Handle);
            hFunc(TEvInitializerPreparationStart, Handle);
            hFunc(TEvInitializerPreparationFinished, Handle);
            hFunc(TEvInitializerPreparationProblem, Handle);
            hFunc(NMetadataManager::TEvModificationFinished, Handle);
            hFunc(NMetadataManager::TEvModificationProblem, Handle);
            default:
                break;
        }
    }
};

}
