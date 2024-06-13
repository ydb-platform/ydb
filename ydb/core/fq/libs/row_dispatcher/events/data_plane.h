#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

namespace NFq {

NActors::TActorId RowDispatcherServiceActorId();

struct TEvRowDispatcher {
    // Event ids.
    enum EEv : ui32 {
        EvCreateSemaphoreResult = YqEventSubspaceBegin(TYqEventSubspace::RowDispatcher),
        EvCoordinatorChanged,
        EvStartSession,
        EvCoordinatorInfo,

        EvRowDispatcherRequest,
        EvRowDispatcherResult,
        EvCoordinatorRequest,
        EvCoordinatorResult,

        EvEnd,
    };

    struct TEvCoordinatorChanged : NActors::TEventLocal<TEvCoordinatorChanged, EEv::EvCoordinatorChanged> {
        TEvCoordinatorChanged(NActors::TActorId leaderActorId)
            : LeaderActorId(leaderActorId) {
        }
        NActors::TActorId LeaderActorId;
    };

    // struct TEvStartSession : public NActors::TEventPB<TEvStartSession,
    //     NFq::NRowDispatcherProto::TEvStartSession, EEv::EvStartSession> {
    //     TEvStartSession() = default;
    // };

    struct TEvCoordinatorInfo : public NActors::TEventPB<TEvCoordinatorInfo,
        NFq::NRowDispatcherProto::TEvCoordinatorInfo, EEv::EvCoordinatorInfo> {
        TEvCoordinatorInfo() = default;
    };


    // Read actor <-> local row_dispatcher: get coordinator actor id.

    struct TEvRowDispatcherRequest : public NActors::TEventLocal<TEvRowDispatcherRequest, EEv::EvRowDispatcherRequest> {};

    struct TEvRowDispatcherResult : public NActors::TEventLocal<TEvRowDispatcherResult, EEv::EvRowDispatcherResult> {
        TEvRowDispatcherResult(TMaybe<NActors::TActorId> coordinatorActorId)
            : CoordinatorActorId(coordinatorActorId) {}

        TMaybe<NActors::TActorId> CoordinatorActorId;
    };

    // Read actor <-> coordinator : get row_dispatcher actorId (which is responsible for processing) by topic/partition.

    struct TEvCoordinatorRequest : public NActors::TEventPB<TEvCoordinatorRequest,
        NFq::NRowDispatcherProto::TEvGetAddressRequest, EEv::EvCoordinatorRequest> {
        TEvCoordinatorRequest() = default;
        TEvCoordinatorRequest(
            const NYql::NPq::NProto::TDqPqTopicSource& sourceParams, 
            const std::vector<ui64>& partitionIds) {
            Record.mutable_source()->CopyFrom(sourceParams);
            for (const auto& id : partitionIds) {
                Record.AddPartitionId(id);
            }
        }
    };

    struct TEvCoordinatorResult : public NActors::TEventPB<TEvCoordinatorResult,
        NFq::NRowDispatcherProto::TEvGetAddressResponse, EEv::EvCoordinatorResult> {
        TEvCoordinatorResult() = default;
    };


    //  Read actor <-> row_dispatcher : 

    struct TEvStartSession2 : public NActors::TEventPB<TEvStartSession2,
        NFq::NRowDispatcherProto::TEvStartSession2, EEv::EvCoordinatorResult> {
        TEvStartSession2() = default;
    };

};

} // namespace NFq
