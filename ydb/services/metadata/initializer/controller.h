#pragma once
#include "common.h"

namespace NKikimr::NMetadataInitializer {

class TInitializerController: public IController {
private:
    const TActorIdentity ActorId;
public:
    TInitializerController(const TActorIdentity& actorId)
        : ActorId(actorId) {

    }

    virtual void PreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) const override;
    virtual void PreparationProblem(const TString& errorMessage) const override;
};

}
