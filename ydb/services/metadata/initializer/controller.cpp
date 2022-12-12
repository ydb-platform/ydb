#include "controller.h"
#include "events.h"

#include <ydb/services/metadata/manager/modification_controller.h>

namespace NKikimr::NMetadata::NInitializer {

void TInitializerInput::PreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) const {
    ActorId.Send(ActorId, new TEvInitializerPreparationFinished(modifiers));
}

void TInitializerInput::PreparationProblem(const TString& errorMessage) const {
    ActorId.Send(ActorId, new TEvInitializerPreparationProblem(errorMessage));
}

void TInitializerInput::AlterProblem(const TString& errorMessage) {
    ActorId.Send(ActorId, new NModifications::TEvModificationProblem(errorMessage));
}

void TInitializerInput::AlterFinished() {
    ActorId.Send(ActorId, new NModifications::TEvModificationFinished());
}

void TInitializerOutput::InitializationFinished(const TString& id) const {
    ActorId.Send(ActorId, new TEvInitializationFinished(id));
}

}
