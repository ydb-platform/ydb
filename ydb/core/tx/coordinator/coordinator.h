#pragma once
#include "defs.h"
#include <ydb/core/tx/tx.h>
#include <ydb/core/util/queue_oneone_inplace.h>
#include <util/generic/bitmap.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NFlatTxCoordinator {
    struct TMediatorStep;
    struct TMediatorConfirmations;
    struct TCoordinatorStepConfirmations;
}
}

namespace NKikimr {

IActor* CreateFlatTxCoordinator(const TActorId &tablet, TTabletStorageInfo *info);

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

        EvCoordinatorConfirmPlan = EvCoordinatorStep + 3 * 512,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COORDINATOR), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_COORDINATOR)");


    struct TEvCoordinatorStep : public TEventPB<TEvCoordinatorStep, NKikimrTx::TEvCoordinatorStep, EvCoordinatorStep> {
        TEvCoordinatorStep()
        {}

        TEvCoordinatorStep(const NFlatTxCoordinator::TMediatorStep &mediatorStep, ui64 prevStep, ui64 mediatorId, ui64 coordinatorId, ui64 activeGeneration);
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

    // must be explicit queue?
    struct TEvMediatorQueueStep : public TEventLocal<TEvMediatorQueueStep, EvMediatorQueueStep> {
        const ui64 GenCookie;
        TAutoPtr<NFlatTxCoordinator::TMediatorStep> Step;

        TEvMediatorQueueStep(ui64 genCookie, TAutoPtr<NFlatTxCoordinator::TMediatorStep> step);
    };

    struct TEvMediatorQueueRestart : public TEventLocal<TEvMediatorQueueRestart, EvMediatorQueueRestart> {
        const ui64 MediatorId;
        const ui64 StartFrom;
        const ui64 GenCookie;

        TEvMediatorQueueRestart(ui64 mediatorId, ui64 startFrom, ui64 genCookie);
    };

    struct TEvMediatorQueueStop : public TEventLocal<TEvMediatorQueueStop, EvMediatorQueueStop> {
        const ui64 MediatorId;

        TEvMediatorQueueStop(ui64 mediatorId);
    };

    struct TEvMediatorQueueConfirmations : public TEventLocal<TEvMediatorQueueConfirmations, EvMediatorQueueConfirmations> {
        TAutoPtr<NFlatTxCoordinator::TMediatorConfirmations> Confirmations;

        TEvMediatorQueueConfirmations(TAutoPtr<NFlatTxCoordinator::TMediatorConfirmations> &confirmations);
    };

    struct TEvCoordinatorConfirmPlan : public TEventLocal<TEvCoordinatorConfirmPlan, EvCoordinatorConfirmPlan> {
        TAutoPtr<NFlatTxCoordinator::TCoordinatorStepConfirmations> Confirmations;

        TEvCoordinatorConfirmPlan(TAutoPtr<NFlatTxCoordinator::TCoordinatorStepConfirmations> &confirmations);
    };

};

}

template<>
inline void Out<NKikimrTx::TEvCoordinatorStepResult::EStatus>(IOutputStream& o, NKikimrTx::TEvCoordinatorStepResult::EStatus x) {
    o << NKikimrTx::TEvCoordinatorStepResult::EStatus_Name(x).data();
    return;
}


