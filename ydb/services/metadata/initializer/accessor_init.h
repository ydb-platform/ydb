#pragma once
#include "common.h"
#include "controller.h"
#include "events.h"
#include "snapshot.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/ds_table/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadata::NInitializer {

class TDSAccessorInitialized: public NActors::TActorBootstrapped<TDSAccessorInitialized> {
private:
    TDeque<ITableModifier::TPtr> Modifiers;
    const NRequest::TConfig Config;
    IInitializationBehaviour::TPtr InitializationBehaviour;
    IInitializerOutput::TPtr ExternalController;
    TInitializerInput::TPtr InternalController;
    std::shared_ptr<TSnapshot> InitializationSnapshot;
    const TString ComponentId;
    void Handle(TEvInitializerPreparationStart::TPtr& ev);
    void Handle(TEvInitializerPreparationFinished::TPtr& ev);
    void Handle(TEvInitializerPreparationProblem::TPtr& ev);
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev);
    void Handle(NModifications::TEvModificationFinished::TPtr& ev);
    void Handle(NModifications::TEvModificationProblem::TPtr& ev);
    void Handle(TEvAlterFinished::TPtr& ev);
    void Handle(TEvAlterProblem::TPtr& ev);
    void DoNextModifier();
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::METADATA_INITIALIZER;
    }

    void Bootstrap();
    TDSAccessorInitialized(const NRequest::TConfig& config,
        const TString& componentId,
        IInitializationBehaviour::TPtr initializationBehaviour,
        IInitializerOutput::TPtr controller, std::shared_ptr<TSnapshot> initializationSnapshot);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, Handle);
            hFunc(TEvInitializerPreparationStart, Handle);
            hFunc(TEvInitializerPreparationFinished, Handle);
            hFunc(TEvInitializerPreparationProblem, Handle);
            hFunc(NModifications::TEvModificationFinished, Handle);
            hFunc(NModifications::TEvModificationProblem, Handle);
            hFunc(TEvAlterFinished, Handle);
            hFunc(TEvAlterProblem, Handle);
            default:
            {
                auto evType = ev->GetTypeName();
                Y_FAIL("unexpected event: %s", evType.data());
            }
        }
    }
};

}
