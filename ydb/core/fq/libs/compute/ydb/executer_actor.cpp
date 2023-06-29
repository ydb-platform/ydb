#include "base_compute_actor.h"

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/retry_actor.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/protos/services.pb.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>


#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ExecuterActor] QueryId: " << Params.QueryId << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ExecuterActor] QueryId: " << Params.QueryId << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ExecuterActor] QueryId: " << Params.QueryId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ExecuterActor] QueryId: " << Params.QueryId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ExecuterActor] QueryId: " << Params.QueryId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TExecuterActor : public TBaseComputeActor<TExecuterActor> {
public:
    enum ERequestType {
        RT_EXECUTE_SCRIPT,
        RT_PING,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("ExecuteScript") },
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

    TExecuterActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const TActorId& pinger, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "Executer")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , Pinger(pinger)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_EXECUTER_ACTOR";

    void Start() {
        LOG_I("Bootstrap");
        Become(&TExecuterActor::StateFunc);
        SendExecuteScript();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvExecuteScriptResponse, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

    void Handle(const TEvents::TEvForwardPingResponse::TPtr& ev) {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Dec();
        pingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Success) {
            pingCounters->Ok->Inc();
            LOG_I("Information about the operation id and execution id is stored. ExecutionId: " << ExecutionId << " OperationId: " << ProtoToString(OperationId));
            Send(Parent, new TEvYdbCompute::TEvExecuterResponse(OperationId, ExecutionId));
            CompleteAndPassAway();
        } else {
            pingCounters->Error->Inc();
            // Without the idempotency key, we lose the running operation here
            LOG_E("Error saving information about the operation id and execution id. ExecutionId: " << ExecutionId << " OperationId: " << ProtoToString(OperationId));
            Send(Parent, new TEvYdbCompute::TEvExecuterResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Error saving information about the operation id and execution id. ExecutionId: " << ExecutionId << " OperationId: " << ProtoToString(OperationId)}}));
            FailedAndPassAway();
        }
    }

    void Handle(const TEvYdbCompute::TEvExecuteScriptResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't execute script: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvExecuterResponse(ev->Get()->Issues));
            FailedAndPassAway();
            return;
        }
        ExecutionId = response.ExecutionId;
        OperationId = response.OperationId;
        LOG_I("Execution has been created. ExecutionId: " << ExecutionId << " OperationId: " << ProtoToString(OperationId));
        SendPingTask();
    }

    void SendExecuteScript() {
        Register(new TRetryActor<TEvYdbCompute::TEvExecuteScriptRequest, TEvYdbCompute::TEvExecuteScriptResponse, TString, TString>(Counters.GetCounters(ERequestType::RT_EXECUTE_SCRIPT), SelfId(), Connector, Params.Sql, Params.JobId));
    }

    void SendPingTask() {
        Fq::Private::PingTaskRequest pingTaskRequest;
        pingTaskRequest.set_execution_id(ExecutionId);
        pingTaskRequest.set_operation_id(ProtoToString(OperationId));
        pingTaskRequest.set_status(::FederatedQuery::QueryMeta::RUNNING);
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();
        StartTime = TInstant::Now();
        Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest));
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Connector;
    TActorId Pinger;
    NYdb::TOperation::TOperationId OperationId;
    TString ExecutionId;
    TCounters Counters;
    TInstant StartTime;
};

std::unique_ptr<NActors::IActor> CreateExecuterActor(const TRunActorParams& params,
                                                     const TActorId& parent,
                                                     const TActorId& connector,
                                                     const TActorId& pinger,
                                                     const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TExecuterActor>(params, parent, connector, pinger, queryCounters);
}

}
