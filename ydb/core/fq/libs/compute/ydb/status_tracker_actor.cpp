#include "base_compute_actor.h"

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/retry_actor.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/common/utils.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>


#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [StatusTracker] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [StatusTracker] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [StatusTracker] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [StatusTracker] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [StatusTracker] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TStatusTrackerActor : public TBaseComputeActor<TStatusTrackerActor> {
public:
    using IRetryPolicy = IRetryPolicy<const TEvYdbCompute::TEvGetOperationResponse::TPtr&>;

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

    TStatusTrackerActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const TActorId& pinger, const NYdb::TOperation::TOperationId& operationId, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "StatusTracker")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , Pinger(pinger)
        , OperationId(operationId)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_STATUS_TRACKER";

    void Start() {
        LOG_I("Become");
        Become(&TStatusTrackerActor::StateFunc);
        SendGetOperation();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvGetOperationResponse, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

    void Handle(const TEvents::TEvForwardPingResponse::TPtr& ev) {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Dec();
        pingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Success) {
            pingCounters->Ok->Inc();
            LOG_I("Information about the status of operation is stored");
            Send(Parent, new TEvYdbCompute::TEvStatusTrackerResponse(Issues, Status, ExecStatus));
            CompleteAndPassAway();
        } else {
            pingCounters->Error->Inc();
            LOG_E("Error saving information about the status of operation");
            Send(Parent, new TEvYdbCompute::TEvStatusTrackerResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Error saving information about the status of operation: " << ProtoToString(OperationId)}}, NYdb::EStatus::INTERNAL_ERROR, ExecStatus));
            FailedAndPassAway();
        }
    }

    void Handle(const TEvYdbCompute::TEvGetOperationResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't get operation: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvStatusTrackerResponse(ev->Get()->Issues, ev->Get()->Status, ExecStatus));
            FailedAndPassAway();
            return;
        }

        StartTime = TInstant::Now();
        LOG_D("Execution status: " << static_cast<int>(response.ExecStatus));
        switch (response.ExecStatus) {
            case NYdb::NQuery::EExecStatus::Unspecified:
            case NYdb::NQuery::EExecStatus::Starting:
                SendGetOperation(TDuration::Seconds(1));
                break;
            case NYdb::NQuery::EExecStatus::Aborted:
            case NYdb::NQuery::EExecStatus::Canceled:
            case NYdb::NQuery::EExecStatus::Failed:
                Issues = response.Issues;
                Status = response.Status;
                ExecStatus = response.ExecStatus;
                QueryStats = response.QueryStats;
                Failed();
                break;
            case NYdb::NQuery::EExecStatus::Completed:
                Issues = response.Issues;
                Status = response.Status;
                ExecStatus = response.ExecStatus;
                QueryStats = response.QueryStats;
                Complete();
                break;
        }
    }

    void SendGetOperation(const TDuration& delay = TDuration::Zero()) {
        Register(new TRetryActor<TEvYdbCompute::TEvGetOperationRequest, TEvYdbCompute::TEvGetOperationResponse, NYdb::TOperation::TOperationId>(Counters.GetCounters(ERequestType::RT_GET_OPERATION), delay, SelfId(), Connector, OperationId));
    }

    void Failed() {
        LOG_I("Execution status: Failed, " << Status);
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();
        Fq::Private::PingTaskRequest pingTaskRequest;
        NYql::IssuesToMessage(Issues, pingTaskRequest.mutable_issues());
        pingTaskRequest.set_status(::FederatedQuery::QueryMeta::FAILING);
        pingTaskRequest.set_ast(QueryStats.query_ast());
        pingTaskRequest.set_plan(QueryStats.query_plan());
        Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest));
    }

    void Complete() {
        LOG_I("Execution status: Complete" << Status);
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();
        Fq::Private::PingTaskRequest pingTaskRequest;
        NYql::IssuesToMessage(Issues, pingTaskRequest.mutable_issues());
        pingTaskRequest.set_status(::FederatedQuery::QueryMeta::COMPLETING);
        pingTaskRequest.set_ast(QueryStats.query_ast());
        pingTaskRequest.set_plan(QueryStats.query_plan());
        try {
            pingTaskRequest.set_statistics(GetV1StatFromV2Plan(QueryStats.query_plan()));
        } catch(const NJson::TJsonException& ex) {
            LOG_E("Error statistics conversion: " << ex.what());
        }
        Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest));
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Connector;
    TActorId Pinger;
    NYdb::TOperation::TOperationId OperationId;
    TCounters Counters;
    TInstant StartTime;
    NYql::TIssues Issues;
    NYdb::EStatus Status = NYdb::EStatus::SUCCESS;
    NYdb::NQuery::EExecStatus ExecStatus = NYdb::NQuery::EExecStatus::Unspecified;
    Ydb::TableStats::QueryStats QueryStats;
};

std::unique_ptr<NActors::IActor> CreateStatusTrackerActor(const TRunActorParams& params,
                                                          const TActorId& parent,
                                                          const TActorId& connector,
                                                          const TActorId& pinger,
                                                          const NYdb::TOperation::TOperationId& operationId,
                                                          const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TStatusTrackerActor>(params, parent, connector, pinger, operationId, queryCounters);
}

}
