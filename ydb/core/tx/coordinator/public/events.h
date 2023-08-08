#pragma once
#include <ydb/core/base/events.h>
#include <ydb/core/protos/tx.pb.h>
#include <ydb/core/tx/coordinator/protos/events.pb.h>

namespace NKikimr {

struct TEvTxCoordinator {
    enum EEv {
        EvCoordinatorStep = EventSpaceBegin(TKikimrEvents::ES_TX_COORDINATOR),
        EvCoordinatorSync,

        EvCoordinatorStepResult = EvCoordinatorStep + 1 * 512,
        EvCoordinatorSyncResult,

        EvMediatorQueueStep = EvCoordinatorStep + 2 * 512,
        EvMediatorQueueRestart,
        EvMediatorQueueStop,
        EvMediatorQueueConfirmations,

        // deprecated: EvCoordinatorConfirmPlan = EvCoordinatorStep + 3 * 512,

        EvCoordinatorStateRequest = EvCoordinatorStep + 4 * 512,
        EvCoordinatorStateResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COORDINATOR), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COORDINATOR)");

    struct TEvCoordinatorStep : public TEventPB<TEvCoordinatorStep, NKikimrTx::TEvCoordinatorStep, EvCoordinatorStep> {
        TEvCoordinatorStep()
        {}

        TEvCoordinatorStep(ui64 step, ui64 prevStep, ui64 mediatorId, ui64 coordinatorId, ui64 activeGeneration);
    };

    struct TEvCoordinatorStepResult : public TEventPB<TEvCoordinatorStepResult, NKikimrTx::TEvCoordinatorStepResult, EvCoordinatorStepResult> {
        TEvCoordinatorStepResult()
        {}

        TEvCoordinatorStepResult(NKikimrTx::TEvCoordinatorStepResult::EStatus status, ui64 step, ui64 completeStep, ui64 latestKnown, ui64 subjectiveTime, ui64 mediator, ui64 coordinator);
    };

    struct TEvCoordinatorSync : public TEventPB<TEvCoordinatorSync, NKikimrTx::TEvCoordinatorSync, EvCoordinatorSync> {
        TEvCoordinatorSync()
        {}

        TEvCoordinatorSync(ui64 cookie, ui64 mediator, ui64 coordinator);
    };

    struct TEvCoordinatorSyncResult : public TEventPB<TEvCoordinatorSyncResult, NKikimrTx::TEvCoordinatorSyncResult, EvCoordinatorSyncResult> {

        TEvCoordinatorSyncResult()
        {}

        TEvCoordinatorSyncResult(NKikimrProto::EReplyStatus status, ui64 cookie);

        TEvCoordinatorSyncResult(ui64 cookie, ui64 completeStep, ui64 latestKnown, ui64 subjectiveTime, ui64 mediator, ui64 coordinator);

    };

    struct TEvCoordinatorStateRequest
        : public TEventPB<TEvCoordinatorStateRequest,
                          NKikimrTxCoordinator::TEvCoordinatorStateRequest,
                          EvCoordinatorStateRequest>
    {
        TEvCoordinatorStateRequest() = default;

        explicit TEvCoordinatorStateRequest(ui32 generation, const TString& continuationToken = {}) {
            Record.SetGeneration(generation);
            if (!continuationToken.empty()) {
                Record.SetContinuationToken(continuationToken);
            }
        }
    };

    struct TEvCoordinatorStateResponse
        : public TEventPB<TEvCoordinatorStateResponse,
                          NKikimrTxCoordinator::TEvCoordinatorStateResponse,
                          EvCoordinatorStateResponse>
    {
        TEvCoordinatorStateResponse() = default;
    };

};

} // namespace NKikimr
