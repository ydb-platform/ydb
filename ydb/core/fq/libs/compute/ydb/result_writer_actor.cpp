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

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " ExecutionId: " << ExecutionId << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " ExecutionId: " << ExecutionId << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " ExecutionId: " << ExecutionId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " ExecutionId: " << ExecutionId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResultWriter] QueryId: " << Params.QueryId << " ExecutionId: " << ExecutionId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TResultWriterActor : public TBaseComputeActor<TResultWriterActor> {
public:
    enum ERequestType {
        RT_FETCH_SCRIPT_RESULT,
        RT_WRITE_RESULT,
        RT_PING,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("FetchScriptResult") },
            { MakeIntrusive<TComputeRequestCounters>("WriteResult") },
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

    TResultWriterActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const TActorId& pinger, const TString& executionId, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "ResultWriter")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , Pinger(pinger)
        , ExecutionId(executionId)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_RESULT_WRITER_ACTOR";

    void Start() {
        LOG_I("Start result writer actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        Become(&TResultWriterActor::StateFunc);
        SendFetchScriptResultRequest();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvFetchScriptResultResponse, Handle);
        hFunc(NFq::TEvInternalService::TEvWriteResultResponse, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

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
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Move result error. ExecutionId: " << ExecutionId}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
        }
    }

    void Handle(const TEvYdbCompute::TEvFetchScriptResultResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't fetch script result: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(ev->Get()->Issues, NYdb::EStatus::INTERNAL_ERROR));
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
            auto writeResultCounters = Counters.GetCounters(ERequestType::RT_WRITE_RESULT);
            writeResultCounters->InFly->Inc();
            Send(NFq::MakeInternalServiceActorId(), new NFq::TEvInternalService::TEvWriteResultRequest(std::move(chunk)));
        }

        if (!FetchToken) {
            PingTaskRequest.mutable_result_id()->set_value(Params.ResultId);
            PingTaskRequest.set_result_set_count(1);
            auto& resultSetMeta = *PingTaskRequest.add_result_set_meta();
            resultSetMeta.set_rows_count(Offset);
            for (const auto& column: resultSetProto.columns()) {
                *resultSetMeta.add_column() = column;
            }
            if (emptyResultSet) {
                SendFinalPingRequest();
            }
        }
    }

    void Handle(const NFq::TEvInternalService::TEvWriteResultResponse::TPtr& ev) {
        auto writeResultCounters = Counters.GetCounters(ERequestType::RT_WRITE_RESULT);
        writeResultCounters->InFly->Dec();
        writeResultCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Status.IsSuccess()) {
            writeResultCounters->Ok->Inc();
            LOG_I("Result successfully written for offset " << Offset);
            if (FetchToken) {
                SendFetchScriptResultRequest();
            } else {
                SendFinalPingRequest();
            }
        } else {
            writeResultCounters->Error->Inc();
            LOG_E("Error writing result for offset " << Offset);
            Send(Parent, new TEvYdbCompute::TEvResultWriterResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Error writing result for offset " << Offset}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
        }
    }

    void SendFetchScriptResultRequest() {
        auto fetchScriptResultCounters = Counters.GetCounters(ERequestType::RT_FETCH_SCRIPT_RESULT);
        fetchScriptResultCounters->InFly->Inc();
        StartTime = TInstant::Now();
        Register(new TRetryActor<TEvYdbCompute::TEvFetchScriptResultRequest, TEvYdbCompute::TEvFetchScriptResultResponse, TString, int64_t, TString>(Counters.GetCounters(ERequestType::RT_FETCH_SCRIPT_RESULT), SelfId(), Connector, ExecutionId, 0, FetchToken));
    }

    void SendFinalPingRequest() {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();
        Send(Pinger, new TEvents::TEvForwardPingRequest(PingTaskRequest));
    }

    Fq::Private::WriteTaskResultRequest CreateProtoRequestWithoutResultSet(ui64 startRowIndex) {
        Fq::Private::WriteTaskResultRequest protoReq;
        protoReq.set_owner_id(Params.Owner);
        protoReq.mutable_result_id()->set_value(Params.ResultId);
        protoReq.set_offset(startRowIndex);
        protoReq.set_result_set_id(0);
        protoReq.set_request_id(0);
        *protoReq.mutable_deadline() = NProtoInterop::CastToProto(Params.Deadline);
        return protoReq;
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Connector;
    TActorId Pinger;
    TString ExecutionId;
    TCounters Counters;
    TInstant StartTime;
    int64_t Offset = 0;
    TString FetchToken;
    Fq::Private::PingTaskRequest PingTaskRequest;
};

std::unique_ptr<NActors::IActor> CreateResultWriterActor(const TRunActorParams& params,
                                                         const TActorId& parent,
                                                         const TActorId& connector,
                                                         const TActorId& pinger,
                                                         const TString& executionId,
                                                         const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TResultWriterActor>(params, parent, connector, pinger, executionId, queryCounters);
}

}
