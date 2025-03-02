#pragma once
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/tx/conveyor/usage/events.h>

namespace NKikimr::NConveyor {

class TActorWorker: public NActors::TActorBootstrapped<TActorWorker> {
private:
    using TBase = NActors::TActorBootstrapped<TActorWorker>;
    const NActors::TActorId DistributorId;

    void HandleMain(TEvExecution::TEvRegisterActor::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecution::TEvRegisterActor, HandleMain);
        default:
                ALS_ERROR(NKikimrServices::TX_CONVEYOR) << "unexpected event for task executor: " << ev->GetTypeRewrite();
                break;
        }
    }

    void Bootstrap() {
        Become(&TActorWorker::StateMain);
    }

    TActorWorker(const TString& conveyorName, const NActors::TActorId& distributorId)
        : TBase("CONVEYOR::" + conveyorName + "::WORKER")
        , DistributorId(distributorId)
    {

    }
};

}
