#include "events.h"

#include <ydb/core/protos/base.pb.h>

namespace NKikimr {

TEvTxCoordinator::TEvCoordinatorStep::TEvCoordinatorStep(ui64 step, ui64 prevStep, ui64 mediatorId, ui64 coordinatorId, ui64 activeGeneration)
{
    Record.SetStep(step);
    Record.SetPrevStep(prevStep);
    Record.SetMediatorID(mediatorId);
    Record.SetCoordinatorID(coordinatorId);
    Record.SetActiveCoordinatorGeneration(activeGeneration);
}

TEvTxCoordinator::TEvCoordinatorStepResult::TEvCoordinatorStepResult(NKikimrTx::TEvCoordinatorStepResult::EStatus status, ui64 step, ui64 completeStep, ui64 latestKnown, ui64 subjectiveTime, ui64 mediator, ui64 coordinator)
{
    Record.SetStatus(status);
    Record.SetStep(step);
    Record.SetCompleteStep(completeStep);
    Record.SetLatestKnown(latestKnown);
    Record.SetSubjectiveTime(subjectiveTime);
    Record.SetMediatorID(mediator);
    Record.SetCoordinatorID(coordinator);
}

TEvTxCoordinator::TEvCoordinatorSync::TEvCoordinatorSync(ui64 cookie, ui64 mediator, ui64 coordinator) {
    Record.SetCookie(cookie);
    Record.SetMediatorID(mediator);
    Record.SetCoordinatorID(coordinator);
}

TEvTxCoordinator::TEvCoordinatorSyncResult::TEvCoordinatorSyncResult(NKikimrProto::EReplyStatus status, ui64 cookie)
{
    Record.SetStatus(status);
    Record.SetCookie(cookie);
}

TEvTxCoordinator::TEvCoordinatorSyncResult::TEvCoordinatorSyncResult(ui64 cookie, ui64 completeStep, ui64 latestKnown, ui64 subjectiveTime, ui64 mediator, ui64 coordinator)
{
    Record.SetStatus(NKikimrProto::OK);
    Record.SetCookie(cookie);
    Record.SetCompleteStep(completeStep);
    Record.SetLatestKnown(latestKnown);
    Record.SetSubjectiveTime(subjectiveTime);
    Record.SetMediatorID(mediator);
    Record.SetCoordinatorID(coordinator);
}

} // namespace NKikimr

Y_DECLARE_OUT_SPEC(, NKikimrTx::TEvCoordinatorStepResult::EStatus, o, x) {
    o << NKikimrTx::TEvCoordinatorStepResult::EStatus_Name(x);
}
