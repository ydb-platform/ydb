#include "controller.h"
#include "events.h"

#include <ydb/services/metadata/manager/modification_controller.h>

namespace NKikimr::NMetadata::NInitializer {

void TInitializerInput::OnPreparationFinished(const TVector<ITableModifier::TPtr>& modifiers) const {
    ActorId.Send(ActorId, new TEvInitializerPreparationFinished(modifiers));
}

void TInitializerInput::OnPreparationProblem(const TString& errorMessage) const {
    ActorId.Send(ActorId, new TEvInitializerPreparationProblem(errorMessage));
}

void TInitializerInput::OnAlteringProblem(const TString& errorMessage) {
    ActorId.Send(ActorId, new TEvAlterProblem(errorMessage));
}

void TInitializerInput::OnAlteringFinished() {
    ActorId.Send(ActorId, new TEvAlterFinished());
}

void TInitializerOutput::OnInitializationFinished(const TString& id) const {
    ActorId.Send(ActorId, new TEvInitializationFinished(id));
}

}
