#pragma once
#include "defs.h"
#include "coordinator.h"
#include <ydb/core/tx/coordinator/public/events.h>

namespace NKikimr::NFlatTxCoordinator {

class TTxCoordinator;

class TCoordinatorStateActor : public TActor<TCoordinatorStateActor> {
public:
    TCoordinatorStateActor(TTxCoordinator* owner, const TActorId& prevStateActorId);
    ~TCoordinatorStateActor();

    /**
     * Confirm this state actor is persistent and active
     */
    void ConfirmPersistent();

    /**
     * Called from tablet destructor. Unlinks from owner and waits for
     * eventual destruction (this method is only called due to actor system
     * stopping).
     */
    void OnTabletDestroyed();

    /**
     * Called when tablet dies. Seals current state, unlinks from owner and
     * waits to transfer state to the next generation or timeout.
     */
    void OnTabletDead();

private:
    void PreserveState();

private:
    STFUNC(StateWork);

    void Handle(TEvTxCoordinator::TEvCoordinatorStateRequest::TPtr& ev);
    void HandlePoison();

private:
    TTxCoordinator* Owner;
    TActorId PrevStateActorId;
    ui64 LastSentStep = -1;
    ui64 LastAcquiredStep = -1;
    ui64 LastConfirmedStep = -1;
    ui64 LastBlockedStep = 0;
    TString SerializedState;
};

} // namespace NKikimr::NFlatTxCoordinator
