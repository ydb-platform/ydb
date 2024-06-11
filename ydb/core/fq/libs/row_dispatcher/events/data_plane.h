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
        EvStartSession = YqEventSubspaceBegin(TYqEventSubspace::RowDispatcher),
        EvCoordinatorInfo,
        EvGetAddressRequest,
        EvGetAddressResponse,
        EvCreateResource,
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
    };

    struct TEvCoordinatorInfo : public NActors::TEventPB<TEvCoordinatorInfo,
        NFq::NRowDispatcherProto::TEvCoordinatorInfo, EEv::EvCoordinatorInfo> {
        TEvCoordinatorInfo() = default;
    };

    struct TEvGetAddressRequest : public NActors::TEventPB<TEvGetAddressRequest,
        NFq::NRowDispatcherProto::TEvGetAddressRequest, EEv::EvGetAddressRequest> {
        TEvGetAddressRequest() = default;
    };

    struct TEvGetAddressResponse : public NActors::TEventPB<TEvGetAddressResponse,
        NFq::NRowDispatcherProto::TEvGetAddressResponse, EEv::EvGetAddressResponse> {
        TEvGetAddressResponse() = default;
    };

};

} // namespace NFq
