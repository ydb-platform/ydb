#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>

namespace NFq {

NActors::TActorId RowDispatcherServiceActorId();

struct TEvRowDispatcher {
    // Event ids.
    enum EEv : ui32 {
        EvCreateResource = YqEventSubspaceBegin(TYqEventSubspace::RowDispatcher),
        EvCreateResourceResponse,
        EvDeleteResource,
        EvStartSession,
        EvEnd,
    };

    struct TEvCoordinatorChanged : NActors::TEventLocal<TEvCoordinatorChanged, EEv::EvCreateResource> {
        TEvCoordinatorChanged(NActors::TActorId leaderActorId)
            : LeaderActorId(leaderActorId) {
        }
        NActors::TActorId LeaderActorId;
    };



    struct TEvStartSession : public NActors::TEventPB<TEvStartSession,
        NFq::NRowDispatcherProto::TEvStartSession, EEv::EvStartSession> {

        TEvStartSession() = default;

        // TEvStartSession(ui64 id, ui64 generation, ui64 taskId) {
        //     Record.MutableCheckpoint()->SetId(id);
        //     Record.MutableCheckpoint()->SetGeneration(generation);
        //     Record.SetTaskId(taskId);
        // }
    };

};

} // namespace NFq
