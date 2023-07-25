#include "coordinator.h"
#include "coordinator_impl.h"

NKikimr::TEvTxCoordinator::TEvCoordinatorStep::TEvCoordinatorStep(const NFlatTxCoordinator::TMediatorStep &mediatorStep, ui64 prevStep, ui64 mediatorId, ui64 coordinatorId, ui64 activeGeneration)
{
    Record.SetStep(mediatorStep.Step);
    Record.SetPrevStep(prevStep);
    Record.SetMediatorID(mediatorId);
    Record.SetCoordinatorID(coordinatorId);
    Record.SetActiveCoordinatorGeneration(activeGeneration);

    for (const NFlatTxCoordinator::TMediatorStep::TTx &tx : mediatorStep.Transactions) {
        NKikimrTx::TCoordinatorTransaction *x = Record.AddTransactions();
        if (tx.TxId)
            x->SetTxId(tx.TxId);
        for (ui64 affected : tx.PushToAffected)
            x->AddAffectedSet(affected);
    }
}

NKikimr::TEvTxCoordinator::TEvCoordinatorStepResult::TEvCoordinatorStepResult(NKikimrTx::TEvCoordinatorStepResult::EStatus status, ui64 step, ui64 completeStep, ui64 latestKnown, ui64 subjectiveTime, ui64 mediator, ui64 coordinator)
{
    Record.SetStatus(status);
    Record.SetStep(step);
    Record.SetCompleteStep(completeStep);
    Record.SetLatestKnown(latestKnown);
    Record.SetSubjectiveTime(subjectiveTime);
    Record.SetMediatorID(mediator);
    Record.SetCoordinatorID(coordinator);
}

NKikimr::TEvTxCoordinator::TEvCoordinatorSync::TEvCoordinatorSync(ui64 cookie, ui64 mediator, ui64 coordinator) {
    Record.SetCookie(cookie);
    Record.SetMediatorID(mediator);
    Record.SetCoordinatorID(coordinator);
}

NKikimr::TEvTxCoordinator::TEvCoordinatorSyncResult::TEvCoordinatorSyncResult(NKikimrProto::EReplyStatus status, ui64 cookie)
{
    Record.SetStatus(status);
    Record.SetCookie(cookie);
}

NKikimr::TEvTxCoordinator::TEvCoordinatorSyncResult::TEvCoordinatorSyncResult(ui64 cookie, ui64 completeStep, ui64 latestKnown, ui64 subjectiveTime, ui64 mediator, ui64 coordinator)
{
    Record.SetStatus(NKikimrProto::OK);
    Record.SetCookie(cookie);
    Record.SetCompleteStep(completeStep);
    Record.SetLatestKnown(latestKnown);
    Record.SetSubjectiveTime(subjectiveTime);
    Record.SetMediatorID(mediator);
    Record.SetCoordinatorID(coordinator);
}
