#include "base_compute_actor.h"
#include "stopper_actor.h"

#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/retry_actor.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/common/utils.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/dq/actors/dq.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>


#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Stopper] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Stopper] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Stopper] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Stopper] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Stopper] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TStopperActor : public TBaseComputeActor<TStopperActor> {
public:

    using TBase = TBaseComputeActor<TStopperActor>;

    enum ERequestType {
        RT_CANCEL_OPERATION,
        RT_GET_OPERATION,
        RT_PING,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("CancelOperation") },
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

    TStopperActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const TActorId& pinger, const NYdb::TOperation::TOperationId& operationId, std::unique_ptr<IPlanStatProcessor>&& processor, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBase(queryCounters, "Stopper")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , Pinger(pinger)
        , OperationId(operationId)
        , Builder(params.Config.GetCommon(), std::move(processor))
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_STOPPER_ACTOR";

    void Start() {
        LOG_I("Start stopper actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        Become(&TStopperActor::StateFunc);
        Register(new TRetryActor<TEvYdbCompute::TEvCancelOperationRequest, TEvYdbCompute::TEvCancelOperationResponse, NYdb::TOperation::TOperationId>(Counters.GetCounters(ERequestType::RT_CANCEL_OPERATION), SelfId(), Connector, OperationId));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCancelOperationResponse, Handle);
        hFunc(TEvYdbCompute::TEvGetOperationResponse, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

    void Handle(const TEvYdbCompute::TEvCancelOperationResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS && response.Status != NYdb::EStatus::NOT_FOUND && response.Status != NYdb::EStatus::PRECONDITION_FAILED) {
            LOG_E("Can't cancel operation: " << response.Issues.ToOneLineString());
            Failed(response.Status, response.Issues);
            return;
        }

        if (response.Status == NYdb::EStatus::NOT_FOUND) {
            LOG_I("Operation successfully canceled and already removed");
            Complete();
            return;
        }

        LOG_I("Operation successfully canceled: " << response.Status);
        Register(new TRetryActor<TEvYdbCompute::TEvGetOperationRequest, TEvYdbCompute::TEvGetOperationResponse, NYdb::TOperation::TOperationId>(Counters.GetCounters(ERequestType::RT_GET_OPERATION), SelfId(), Connector, OperationId));
    }

    void Handle(const TEvYdbCompute::TEvGetOperationResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS && response.Status != NYdb::EStatus::NOT_FOUND) {
            LOG_E("Can't get operation: " << response.Issues.ToOneLineString());
            Failed(response.Status, response.Issues);
            return;
        }

        if (response.Status == NYdb::EStatus::NOT_FOUND) {
            LOG_I("Operation has been already removed");
            Complete();
            return;
        }

        auto statusCode = NYql::NDq::YdbStatusToDqStatus(response.StatusCode);
        LOG_I("Operation successfully fetched, Status: " << response.Status << ", StatusCode: " << NYql::NDqProto::StatusIds::StatusCode_Name(statusCode) << " Issues: " << response.Issues.ToOneLineString());

        StartTime = TInstant::Now();
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();

        Fq::Private::PingTaskRequest pingTaskRequest = Builder.Build(response.QueryStats, response.Issues, FederatedQuery::QueryMeta::ABORTING_BY_USER, statusCode);
        if (Builder.Issues) {
            LOG_W(Builder.Issues.ToOneLineString());
        }
        Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest));
    }

    void Handle(const TEvents::TEvForwardPingResponse::TPtr& ev) {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Dec();
        pingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());

        if (ev.Get()->Get()->Success) {
            pingCounters->Ok->Inc();
            LOG_I("Information about the status of operation is updated");
        } else {
            pingCounters->Error->Inc();
            LOG_E("Error updating information about the status of operation");
        }
        Complete();
    }

    void Failed(NYdb::EStatus status, NYql::TIssues issues) {
        Send(Parent, new TEvYdbCompute::TEvStopperResponse(issues, status));
        FailedAndPassAway();
    }

    void Complete() {
        Send(Parent, new TEvYdbCompute::TEvStopperResponse({}, NYdb::EStatus::SUCCESS));
        CompleteAndPassAway();
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Connector;
    TActorId Pinger;
    NYdb::TOperation::TOperationId OperationId;
    PingTaskRequestBuilder Builder;
    TCounters Counters;
    TInstant StartTime;
};

std::unique_ptr<NActors::IActor> CreateStopperActor(const TRunActorParams& params,
                                                    const TActorId& parent,
                                                    const TActorId& connector,
                                                    const TActorId& pinger,
                                                    const NYdb::TOperation::TOperationId& operationId,
                                                    std::unique_ptr<IPlanStatProcessor>&& processor,
                                                    const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TStopperActor>(params, parent, connector, pinger, operationId, std::move(processor), queryCounters);
}

}
