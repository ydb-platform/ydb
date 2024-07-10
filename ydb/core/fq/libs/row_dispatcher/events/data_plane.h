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
        EvStopSession,
        EvSessionData,
        EvSessionError,

        EvRowDispatcherRequest,
        EvRowDispatcherResult,
        EvCoordinatorRequest,
        EvCoordinatorResult,

        EvSessionAddConsumer,
        EvSessionDeleteConsumer,
        EvEnd,
    };

    struct TEvCoordinatorChanged : NActors::TEventLocal<TEvCoordinatorChanged, EEv::EvCoordinatorChanged> {
        TEvCoordinatorChanged(NActors::TActorId coordinatorActorId)
            : CoordinatorActorId(coordinatorActorId) {
        }
        NActors::TActorId CoordinatorActorId;
    };

    // Read actor <-> local row_dispatcher: get coordinator actor id.

    struct TEvRowDispatcherRequest : public NActors::TEventLocal<TEvRowDispatcherRequest, EEv::EvRowDispatcherRequest> {};

    // Read actor <-> coordinator : get row_dispatcher actorId (which is responsible for processing) by topic/partition.

    struct TEvCoordinatorRequest : public NActors::TEventPB<TEvCoordinatorRequest,
        NFq::NRowDispatcherProto::TEvGetAddressRequest, EEv::EvCoordinatorRequest> {
        TEvCoordinatorRequest() = default;
        TEvCoordinatorRequest(
            const NYql::NPq::NProto::TDqPqTopicSource& sourceParams, 
            const std::vector<ui64>& partitionIds) {
            Record.MutableSource()->CopyFrom(sourceParams);
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

    struct TEvStartSession : public NActors::TEventPB<TEvStartSession,
        NFq::NRowDispatcherProto::TEvStartSession, EEv::EvStartSession> {
        TEvStartSession() = default;
    };

    struct TEvStopSession : public NActors::TEventPB<TEvStopSession,
        NFq::NRowDispatcherProto::TEvStopSession, EEv::EvStopSession> {
        TEvStopSession() = default;
    };

    struct TEvSessionData : public NActors::TEventPB<TEvSessionData,
        NFq::NRowDispatcherProto::TEvSessionData, EEv::EvSessionData> {
        TEvSessionData() = default;
    };

    struct TEvSessionError : public NActors::TEventPB<TEvSessionData,
        NFq::NRowDispatcherProto::TEvSessionError, EEv::EvSessionError> {
        TEvSessionError() = default;
    };


    //  Row_dispatcher  <->  session: 
    struct TEvSessionAddConsumer : public NActors::TEventLocal<TEvSessionAddConsumer, EEv::EvSessionAddConsumer> {
        NActors::TActorId ConsumerActorId;
        TMaybe<ui64> Offset;
        ui64 StartingMessageTimestampMs;
    };

    struct TEvSessionDeleteConsumer : public NActors::TEventLocal<TEvSessionDeleteConsumer, EEv::EvSessionDeleteConsumer> {
        NActors::TActorId ConsumerActorId;
    };
};

} // namespace NFq
