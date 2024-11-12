#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/core/fq/libs/row_dispatcher/events/topic_session_stats.h>

namespace NFq {

NActors::TActorId RowDispatcherServiceActorId();

struct TEvRowDispatcher {
    // Event ids.
    enum EEv : ui32 {
        EvCoordinatorChanged = YqEventSubspaceBegin(TYqEventSubspace::RowDispatcher),
        EvStartSession,
        EvStartSessionAck,
        EvNewDataArrived,
        EvGetNextBatch,
        EvMessageBatch,
        EvStatus,
        EvStopSession,
        EvSessionError,
        EvCoordinatorChangesSubscribe,
        EvCoordinatorRequest,
        EvCoordinatorResult,
        EvSessionStatistic,
        EvHeartbeat,
        EvEnd,
    };

    struct TEvCoordinatorChanged : NActors::TEventLocal<TEvCoordinatorChanged, EEv::EvCoordinatorChanged> {
        TEvCoordinatorChanged(NActors::TActorId coordinatorActorId)
            : CoordinatorActorId(coordinatorActorId) {
        }
        NActors::TActorId CoordinatorActorId;
    };

    struct TEvCoordinatorChangesSubscribe : public NActors::TEventLocal<TEvCoordinatorChangesSubscribe, EEv::EvCoordinatorChangesSubscribe> {};

    struct TEvCoordinatorRequest : public NActors::TEventPB<TEvCoordinatorRequest,
        NFq::NRowDispatcherProto::TEvGetAddressRequest, EEv::EvCoordinatorRequest> {
        TEvCoordinatorRequest() = default;
        TEvCoordinatorRequest(
            const NYql::NPq::NProto::TDqPqTopicSource& sourceParams, 
            const std::vector<ui64>& partitionIds) {
            *Record.MutableSource() = sourceParams;
            for (const auto& id : partitionIds) {
                Record.AddPartitionId(id);
            }
        }
    };

    struct TEvCoordinatorResult : public NActors::TEventPB<TEvCoordinatorResult,
        NFq::NRowDispatcherProto::TEvGetAddressResponse, EEv::EvCoordinatorResult> {
        TEvCoordinatorResult() = default;
    };

    struct TEvStartSession : public NActors::TEventPB<TEvStartSession,
        NFq::NRowDispatcherProto::TEvStartSession, EEv::EvStartSession> {
            
        TEvStartSession() = default;
        TEvStartSession(
            const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
            ui64 partitionId,
            const TString token,
            TMaybe<ui64> readOffset,
            ui64 startingMessageTimestampMs,
            const TString& queryId) {
            *Record.MutableSource() = sourceParams;
            Record.SetPartitionId(partitionId);
            Record.SetToken(token);
            if (readOffset) {
                Record.SetOffset(*readOffset);
            }
            Record.SetStartingMessageTimestampMs(startingMessageTimestampMs);
            Record.SetQueryId(queryId);
        }
    };

    struct TEvStartSessionAck : public NActors::TEventPB<TEvStartSessionAck,
        NFq::NRowDispatcherProto::TEvStartSessionAck, EEv::EvStartSessionAck> {
        TEvStartSessionAck() = default;
        explicit TEvStartSessionAck(
            const NFq::NRowDispatcherProto::TEvStartSession& consumer) {
            *Record.MutableConsumer() = consumer;
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

    struct TEvStatus : public NActors::TEventPB<TEvStatus,
        NFq::NRowDispatcherProto::TEvStatus, EEv::EvStatus> {
        TEvStatus() = default;
        NActors::TActorId ReadActorId;
    };

    struct TEvSessionError : public NActors::TEventPB<TEvSessionError,
        NFq::NRowDispatcherProto::TEvSessionError, EEv::EvSessionError> {
        TEvSessionError() = default;
        NActors::TActorId ReadActorId;
    };

    struct TEvSessionStatistic : public NActors::TEventLocal<TEvSessionStatistic, EEv::EvSessionStatistic> {
        TEvSessionStatistic(const TopicSessionStatistic& stat)
        : Stat(stat) {}
        TopicSessionStatistic Stat;
    };

    struct TEvHeartbeat : public NActors::TEventPB<TEvHeartbeat, NFq::NRowDispatcherProto::TEvHeartbeat, EEv::EvHeartbeat> {
        TEvHeartbeat() = default;
        TEvHeartbeat(ui32 partitionId) {
            Record.SetPartitionId(partitionId);
        }
    };
};

} // namespace NFq
