#pragma once
#include "common.h"
#include "snapshot.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/services/metadata/ds_table/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadata::NInitializer {

class TDSAccessorInitialized: public IInitializerInput,
    public NModifications::IAlterController,
    public NMetadata::NInitializer::IModifierExternalController
{
private:
    mutable TDeque<ITableModifier::TPtr> Modifiers;
    const NRequest::TConfig Config;
    IInitializationBehaviour::TPtr InitializationBehaviour;
    IInitializerOutput::TPtr ExternalController;
    std::shared_ptr<TSnapshot> InitializationSnapshot;
    const TString ComponentId;
    std::shared_ptr<TDSAccessorInitialized> SelfPtr;

    void DoNextModifier(const bool doPop);
    virtual void OnPreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) override;
    virtual void OnPreparationProblem(const TString& errorMessage) const override;
    virtual void OnAlteringProblem(const TString& errorMessage) override;
    virtual void OnAlteringFinished() override;

    virtual void OnModificationFinished(const TString& modificationId) override;
    virtual void OnModificationFailed(const TString& errorMessage, const TString& modificationId) override;

    TDSAccessorInitialized(const NRequest::TConfig& config,
        const TString& componentId,
        IInitializationBehaviour::TPtr initializationBehaviour,
        IInitializerOutput::TPtr controller, std::shared_ptr<TSnapshot> initializationSnapshot);
public:
    static void Execute(const NRequest::TConfig& config,
        const TString& componentId,
        IInitializationBehaviour::TPtr initializationBehaviour,
        IInitializerOutput::TPtr controller, std::shared_ptr<TSnapshot> initializationSnapshot);

};

}
