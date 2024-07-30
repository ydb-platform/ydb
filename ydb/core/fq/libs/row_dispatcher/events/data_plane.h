#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <ydb/core/fq/libs/row_dispatcher/consumer.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

namespace NFq {

NActors::TActorId RowDispatcherServiceActorId();

struct TEvRowDispatcher {
    // Event ids.
    enum EEv : ui32 {
        EvCreateSemaphoreResult = YqEventSubspaceBegin(TYqEventSubspace::RowDispatcher),
        EvCoordinatorChanged,
        EvAddConsumer,
        EvAck,
        EvNewDataArrived,
        EvGetNextBatch,
        EvMessageBatch,
        EvStopSession,
        EvSessionError,

        EvRowDispatcherRequest,
        EvRowDispatcherResult,
        EvCoordinatorRequest,
        EvCoordinatorResult,

        EvSessionAddConsumer,
        EvSessionDeleteConsumer,
        EvSessionConsumerDeleted,
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

    struct TEvAddConsumer : public NActors::TEventPB<TEvAddConsumer,
        NFq::NRowDispatcherProto::TEvAddConsumer, EEv::EvAddConsumer> {
            
        TEvAddConsumer() = default;
        TEvAddConsumer(
            const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
            ui64 partitionId,
            const TString token,
            bool addBearerToToken,
            TMaybe<ui64> readOffset,
            ui64 startingMessageTimestampMs) {
            Record.MutableSource()->CopyFrom(sourceParams);
            Record.SetPartitionId(partitionId);
            Record.SetToken(token);
            Record.SetAddBearerToToken(addBearerToToken);
            if (readOffset) {
                Record.SetOffset(*readOffset);
            }
            Record.SetStartingMessageTimestampMs(startingMessageTimestampMs);
        }
    };

    struct TEvAck : public NActors::TEventPB<TEvAck,
        NFq::NRowDispatcherProto::TEvAck, EEv::EvAck> {
        TEvAck() = default;
        explicit TEvAck(
            const NFq::NRowDispatcherProto::TEvAddConsumer& consumer) {
            Record.MutableConsumer()->CopyFrom(consumer);
        }
    };

    struct TEvNewDataArrived : public NActors::TEventPB<TEvNewDataArrived,
        NFq::NRowDispatcherProto::TEvNewDataArrived, EEv::EvNewDataArrived> {
        TEvNewDataArrived() = default;
        NActors::TActorId ReadActorId;
    };

    struct TEvGetNextBatch : public NActors::TEventPB<TEvGetNextBatch,
        NFq::NRowDispatcherProto::TEvGetNextBatch, EEv::EvGetNextBatch> {
        TEvGetNextBatch() = default;
    };


    struct TEvStopSession : public NActors::TEventPB<TEvStopSession,
        NFq::NRowDispatcherProto::TEvStopSession, EEv::EvStopSession> {
        TEvStopSession() = default;
    };

    struct TEvMessageBatch : public NActors::TEventPB<TEvMessageBatch,
        NFq::NRowDispatcherProto::TEvMessageBatch, EEv::EvMessageBatch> {
        TEvMessageBatch() = default;
        NActors::TActorId ReadActorId;
    };

    struct TEvSessionError : public NActors::TEventPB<TEvSessionError,
        NFq::NRowDispatcherProto::TEvSessionError, EEv::EvSessionError> {
        TEvSessionError() = default;
    };


    // //  Row_dispatcher  <->  session: 
    // struct TEvSessionAddConsumer : public NActors::TEventLocal<TEvSessionAddConsumer, EEv::EvSessionAddConsumer> {
    //     TEvSessionAddConsumer(THolder<NFq::Consumer>&& consumer) 
    //         : Consumer(consumer.Release()) // TODO 
    //         {}
    //     THolder<NFq::Consumer> Consumer;
   // };

    struct TEvSessionDeleteConsumer : public NActors::TEventLocal<TEvSessionDeleteConsumer, EEv::EvSessionDeleteConsumer> {
        NActors::TActorId ReadActorId;
    };



    struct TEvSessionConsumerDeleted : public NActors::TEventLocal<TEvSessionConsumerDeleted, EEv::EvSessionConsumerDeleted> {
        TEvSessionConsumerDeleted(THolder<NFq::Consumer>&& consumer) 
        : Consumer(std::move(consumer)) // TODO 
        {}
        THolder<NFq::Consumer> Consumer;
    };

};

} // namespace NFq
