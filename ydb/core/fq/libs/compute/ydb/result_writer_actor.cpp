#include "base_compute_actor.h"

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/retry_actor.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/private_client/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TResultSetWriterActor : public TBaseComputeActor<TResultSetWriterActor> {
public:
    enum ERequestType {
        RT_FETCH_SCRIPT_RESULT,
        RT_WRITE_RESULT_SET,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("FetchScriptResult") },
            { MakeIntrusive<TComputeRequestCounters>("WriteResultSet") }
        });

        ::NMonitoring::TDynamicCounterPtr Counters;

    public:
        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: Requests) {
                request->Register(Counters);
            }
        }

        TComputeRequestCountersPtr GetCounters(ERequestType type) {
            return Requests[type];
        }
    };

    TResultSetWriterActor(const TRunActorParams& params, int64_t resultSetId, const TActorId& parent, const TActorId& connector, const NKikimr::NOperationId::TOperationId& operationId, const ::NMonitoring::TDynamicCounterPtr& counters)
        : TBaseComputeActor(counters, "ResultSetWriter")
        , Params(params)
        , ResultSetId(resultSetId)
        , Parent(parent)
        , Connector(connector)
        , OperationId(operationId)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_RESULT_SET_WRITER_ACTOR";

    void Start() {
        LOG_I("Start result set writer actor, id: " << ResultSetId);
        Become(&TResultSetWriterActor::StateFunc);
        SendFetchScriptResultRequest();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvFetchScriptResultResponse, Handle);
        hFunc(NFq::TEvInternalService::TEvWriteResultResponse, Handle);
    )

    void Handle(const TEvYdbCompute::TEvFetchScriptResultResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't fetch script result: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(ev->Get()->Issues, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
            return;
        }

        StartTime = TInstant::Now();
        FetchToken = response.NextFetchToken;
        auto emptyResultSet = response.ResultSet->RowsCount() == 0;
        const auto resultSetProto = NYdb::TProtoAccessor::GetProto(*response.ResultSet);

        if (!emptyResultSet) {
            auto chunk = CreateProtoRequestWithoutResultSet(Offset);
            Offset += response.ResultSet->RowsCount();
            *chunk.mutable_result_set() = resultSetProto;
            auto writeResultCounters = Counters.GetCounters(ERequestType::RT_WRITE_RESULT_SET);
            writeResultCounters->InFly->Inc();
            Send(NFq::MakeInternalServiceActorId(), new NFq::TEvInternalService::TEvWriteResultRequest(std::move(chunk)));
        }

        if (!FetchToken) {
            if (emptyResultSet) {
                SendReplyAndPassAway();
            }
        }
    }

    void Handle(const NFq::TEvInternalService::TEvWriteResultResponse::TPtr& ev) {
        auto writeResultCounters = Counters.GetCounters(ERequestType::RT_WRITE_RESULT_SET);
        writeResultCounters->InFly->Dec();
        writeResultCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Status.IsSuccess()) {
            writeResultCounters->Ok->Inc();
            LOG_I("Result successfully written for offset " << Offset);
            if (FetchToken) {
                SendFetchScriptResultRequest();
            } else {
                SendReplyAndPassAway();
            }
        } else {
            writeResultCounters->Error->Inc();
            LOG_E("Error writing result for offset " << Offset);
            Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Error writing result for offset " << Offset}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
        }
    }

    void SendFetchScriptResultRequest() {
        auto fetchScriptResultCounters = Counters.GetCounters(ERequestType::RT_FETCH_SCRIPT_RESULT);
        fetchScriptResultCounters->InFly->Inc();
        StartTime = TInstant::Now();
        Register(new TRetryActor<TEvYdbCompute::TEvFetchScriptResultRequest, TEvYdbCompute::TEvFetchScriptResultResponse, NKikimr::NOperationId::TOperationId, int64_t, TString>(Counters.GetCounters(ERequestType::RT_FETCH_SCRIPT_RESULT), SelfId(), Connector, OperationId, ResultSetId, FetchToken));
    }

    void SendReplyAndPassAway() {
        Send(Parent, new TEvYdbCompute::TEvResultSetWriterResponse(Offset));
        PassAway();
    }

    Fq::Private::WriteTaskResultRequest CreateProtoRequestWithoutResultSet(ui64 startRowIndex) {
        Fq::Private::WriteTaskResultRequest protoReq;
        protoReq.set_owner_id(Params.Owner);
        protoReq.mutable_result_id()->set_value(Params.ResultId);
        protoReq.set_result_set_id(ResultSetId);
        protoReq.set_offset(startRowIndex);
        *protoReq.mutable_deadline() = NProtoInterop::CastToProto(Params.Deadline);
        return protoReq;
    }

private:
    TRunActorParams Params;
    uint32_t ResultSetId = 0;
    TActorId Parent;
    TActorId Connector;
    NKikimr::NOperationId::TOperationId OperationId;
    TCounters Counters;
    TInstant StartTime;
    int64_t Offset = 0;
    TString FetchToken;
};

class TResultWriterActor : public TBaseComputeActor<TResultWriterActor> {
public:
    enum ERequestType {
        RT_GET_OPERATION,
        RT_PING,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("GetOperation") },
            { MakeIntrusive<TComputeRequestCounters>("Ping") }
        });

        ::NMonitoring::TDynamicCounterPtr Counters;

    public:
        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: Requests) {
                request->Register(Counters);
            }
        }

        TComputeRequestCountersPtr GetCounters(ERequestType type) {
            return Requests[type];
        }
    };

    TResultWriterActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const TActorId& pinger, const NKikimr::NOperationId::TOperationId& operationId, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "ResultWriter")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , Pinger(pinger)
        , OperationId(operationId)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_RESULT_WRITER_ACTOR";

    void Start() {
        LOG_I("Start result writer actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        Become(&TResultWriterActor::StateFunc);
        SendGetOperation();
    }

    void SendGetOperation() {
        Register(new TRetryActor<TEvYdbCompute::TEvGetOperationRequest, TEvYdbCompute::TEvGetOperationResponse, NYdb::TOperation::TOperationId>(Counters.GetCounters(ERequestType::RT_GET_OPERATION), SelfId(), Connector, OperationId));
    }

    void WriteNextResultSet() {
        if (CurrentResultSetId < (int64_t)PingTaskRequest.result_set_meta_size()) {
            Register(new TResultSetWriterActor(Params,
                                                    CurrentResultSetId++,
                                                    SelfId(),
                                                    Connector,
                                                    OperationId,
                                                    GetBaseCounters()));
            return;
        }

        SendFinalPingRequest();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvGetOperationResponse, Handle);
        hFunc(TEvYdbCompute::TEvResultSetWriterResponse, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

    void Handle(const TEvYdbCompute::TEvGetOperationResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't get operation: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(ev->Get()->Issues, ev->Get()->Status));
            FailedAndPassAway();
            return;
        }

        for (const auto& resultSetMeta: ev.Get()->Get()->ResultSetsMeta) {
            auto& meta = *PingTaskRequest.add_result_set_meta();
            for (const auto& column: resultSetMeta.columns()) {
                *meta.add_column() = column;
            }
            meta.set_truncated(resultSetMeta.truncated());
        }

        WriteNextResultSet();
    }

    void Handle(const TEvents::TEvForwardPingResponse::TPtr& ev) {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Dec();
        pingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Success) {
            pingCounters->Ok->Inc();
            LOG_I("The result has been moved");
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse({}, NYdb::EStatus::SUCCESS));
            CompleteAndPassAway();
        } else {
            pingCounters->Error->Inc();
            LOG_E("Move result error");
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Move result error. OperationId: " << ProtoToString(OperationId)}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
        }
    }

    void Handle(const TEvYdbCompute::TEvResultSetWriterResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't fetch script result: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(ev->Get()->Issues, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
            return;
        }
        PingTaskRequest.mutable_result_set_meta(CurrentResultSetId - 1)->set_rows_count(ev->Get()->RowsCount);
        WriteNextResultSet();
    }

    void SendFinalPingRequest() {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();
        PingTaskRequest.set_result_set_count(PingTaskRequest.result_set_meta_size());
        Send(Pinger, new TEvents::TEvForwardPingRequest(PingTaskRequest));
    }

private:
    TRunActorParams Params;
    int64_t CurrentResultSetId = 0;
    TActorId Parent;
    TActorId Connector;
    TActorId Pinger;
    NKikimr::NOperationId::TOperationId OperationId;
    TCounters Counters;
    TInstant StartTime;
    TString FetchToken;
    Fq::Private::PingTaskRequest PingTaskRequest;
};

std::unique_ptr<NActors::IActor> CreateResultWriterActor(const TRunActorParams& params,
                                                         const TActorId& parent,
                                                         const TActorId& connector,
                                                         const TActorId& pinger,
                                                         const NKikimr::NOperationId::TOperationId& operationId,
                                                         const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TResultWriterActor>(params, parent, connector, pinger, operationId, queryCounters);
}

}
