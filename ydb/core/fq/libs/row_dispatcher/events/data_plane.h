#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/core/fq/libs/row_dispatcher/events/topic_session_stats.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/purecalc/common/fwd.h>

#include <util/generic/set.h>
#include <util/generic/map.h>

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
        EvNoSession,
        EvGetInternalStateRequest,
        EvGetInternalStateResponse,
        EvPurecalcCompileRequest,
        EvPurecalcCompileResponse,
        EvPurecalcCompileAbort,
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
                Record.AddPartitionIds(id);
            }
        }
    };

    struct TEvCoordinatorResult : public NActors::TEventPB<TEvCoordinatorResult,
        NFq::NRowDispatcherProto::TEvGetAddressResponse, EEv::EvCoordinatorResult> {
        TEvCoordinatorResult() = default;
    };

// Session events (with seqNo checks)

    struct TEvStartSession : public NActors::TEventPB<TEvStartSession,
        NFq::NRowDispatcherProto::TEvStartSession, EEv::EvStartSession> {
            
        TEvStartSession() = default;
        TEvStartSession(
            const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
            const std::set<ui32>& partitionIds,
            const TString token,
            const std::map<ui32, ui64>& readOffsets,
            ui64 startingMessageTimestampMs,
            const TString& queryId) {
            *Record.MutableSource() = sourceParams;
            for (auto partitionId : partitionIds) {
                Record.AddPartitionIds(partitionId);
            }
            Record.SetToken(token);
            for (const auto& [partitionId, offset] : readOffsets) {
                auto* partitionOffset = Record.AddOffsets();
                partitionOffset->SetPartitionId(partitionId);
                partitionOffset->SetOffset(offset);
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
    };

    struct TEvSessionError : public NActors::TEventPB<TEvSessionError,
        NFq::NRowDispatcherProto::TEvSessionError, EEv::EvSessionError> {
        TEvSessionError() = default;
        NActors::TActorId ReadActorId;
        bool IsFatalError = false;      // session is no longer valid if true (need to send TEvPoisonPill).
    };

    struct TEvSessionStatistic : public NActors::TEventLocal<TEvSessionStatistic, EEv::EvSessionStatistic> {
        TEvSessionStatistic(const TTopicSessionStatistic& stat)
        : Stat(stat) {}
        TTopicSessionStatistic Stat;
    };

    // two purposes: confirm seqNo and check the availability of the recipient actor (wait TEvUndelivered)
    struct TEvHeartbeat : public NActors::TEventPB<TEvHeartbeat, NFq::NRowDispatcherProto::TEvHeartbeat, EEv::EvHeartbeat> {
        TEvHeartbeat() = default;
    };

// Network events (without seqNo checks)

    struct TEvNoSession : public NActors::TEventPB<TEvNoSession, NFq::NRowDispatcherProto::TEvNoSession, EEv::EvNoSession> {
        TEvNoSession() = default;
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
        TEvPurecalcCompileResponse(NYql::NDqProto::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        explicit TEvPurecalcCompileResponse(IProgramHolder::TPtr programHolder)
            : ProgramHolder(std::move(programHolder))
            , Status(NYql::NDqProto::StatusIds::SUCCESS)
        {}

        IProgramHolder::TPtr ProgramHolder;  // Same holder that passed into TEvPurecalcCompileRequest
        NYql::NDqProto::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
    };

    struct TEvPurecalcCompileAbort : public NActors::TEventLocal<TEvPurecalcCompileAbort, EEv::EvPurecalcCompileAbort> {};
};

} // namespace NFq
