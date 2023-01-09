#pragma once
#include "common.h"

#include <ydb/services/metadata/manager/common.h>

namespace NKikimr::NMetadata::NInitializer {

class TInitializerInput: public IInitializerInput, public NModifications::IAlterController {
private:
    const TActorIdentity ActorId;
public:
    using TPtr = std::shared_ptr<TInitializerInput>;
    TInitializerInput(const TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void OnAlteringProblem(const TString& errorMessage) override;
    virtual void OnAlteringFinished() override;
    virtual void OnPreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) const override;
    virtual void OnPreparationProblem(const TString& errorMessage) const override;
};

class TInitializerOutput: public IInitializerOutput {
private:
    const TActorIdentity ActorId;
public:
    TInitializerOutput(const TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void OnInitializationFinished(const TString& id) const override;
};

}
