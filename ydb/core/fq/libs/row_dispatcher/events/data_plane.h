#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/core/fq/libs/row_dispatcher/events/topic_session_stats.h>

#include <yql/essentials/public/purecalc/common/fwd.h>

namespace NFq {

NActors::TActorId RowDispatcherServiceActorId();

struct TPurecalcCompileSettings {
    bool EnabledLLVM = false;

    std::strong_ordering operator<=>(const TPurecalcCompileSettings& other) const = default;
};

class IProgramHolder : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IProgramHolder>;

public:
    // Perform program creation and saving
    virtual void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) = 0;
};

struct TEvRowDispatcher {
    // Event ids.
    enum EEv : ui32 {
        EvCoordinatorChanged = YqEventSubspaceBegin(TYqEventSubspace::RowDispatcher),
        EvStartSession,
        EvStartSessionAck,
        EvNewDataArrived,
        EvGetNextBatch,
        EvMessageBatch,
        EvStatistics,
        EvStopSession,
        EvSessionError,
        EvCoordinatorChangesSubscribe,
        EvCoordinatorRequest,
        EvCoordinatorResult,
        EvSessionStatistic,
        EvHeartbeat,
        EvGetInternalStateRequest,
        EvGetInternalStateResponse,
        EvPurecalcCompileRequest,
        EvPurecalcCompileResponse,
        EvEnd,
    };

    struct TEvCoordinatorChanged : NActors::TEventLocal<TEvCoordinatorChanged, EEv::EvCoordinatorChanged> {
        TEvCoordinatorChanged(NActors::TActorId coordinatorActorId, ui64 generation)
            : CoordinatorActorId(coordinatorActorId)
            , Generation(generation) {
        }
        NActors::TActorId CoordinatorActorId;
        ui64 Generation = 0;
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

    struct TEvStatistics : public NActors::TEventPB<TEvStatistics,
        NFq::NRowDispatcherProto::TEvStatistics, EEv::EvStatistics> {
        TEvStatistics() = default;
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

    struct TEvGetInternalStateRequest : public NActors::TEventPB<TEvGetInternalStateRequest,
        NFq::NRowDispatcherProto::TEvGetInternalStateRequest, EEv::EvGetInternalStateRequest> {
        TEvGetInternalStateRequest() = default;
    };

    struct TEvGetInternalStateResponse : public NActors::TEventPB<TEvGetInternalStateResponse,
        NFq::NRowDispatcherProto::TEvGetInternalStateResponse, EEv::EvGetInternalStateResponse> {
        TEvGetInternalStateResponse() = default;
    };

    // Compilation events
    struct TEvPurecalcCompileRequest : public NActors::TEventLocal<TEvPurecalcCompileRequest, EEv::EvPurecalcCompileRequest> {
        TEvPurecalcCompileRequest(IProgramHolder::TPtr programHolder, const TPurecalcCompileSettings& settings)
            : ProgramHolder(std::move(programHolder))
            , Settings(settings)
        {}

        IProgramHolder::TPtr ProgramHolder;
        TPurecalcCompileSettings Settings;
    };

    struct TEvPurecalcCompileResponse : public NActors::TEventLocal<TEvPurecalcCompileResponse, EEv::EvPurecalcCompileResponse> {
        explicit TEvPurecalcCompileResponse(const TString& error)
            : Error(error)
        {}

        explicit TEvPurecalcCompileResponse(IProgramHolder::TPtr programHolder)
            : ProgramHolder(std::move(programHolder))
        {}

        IProgramHolder::TPtr ProgramHolder;  // Same holder that passed into TEvPurecalcCompileRequest
        TString Error;
    };
};

} // namespace NFq
