#include "controller.h"
#include "events.h"

namespace NKikimr::NMetadataInitializer {

void TInitializerController::PreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) const {
    ActorId.Send(ActorId, new TEvInitializerPreparationFinished(modifiers));
}

void TInitializerController::PreparationProblem(const TString& errorMessage) const {
    ActorId.Send(ActorId, new TEvInitializerPreparationProblem(errorMessage));
}

}
