#include "base_compute_actor.h"

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/time_util.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Finalizer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Finalizer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Finalizer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Finalizer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Finalizer] QueryId: " << Params.QueryId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TFinalizerActor : public TBaseComputeActor<TFinalizerActor> {
public:
    enum ERequestType {
        RT_PING,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
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

    TFinalizerActor(const TRunActorParams& params, const TActorId& parent, const TActorId& pinger, NYdb::NQuery::EExecStatus execStatus, FederatedQuery::QueryMeta::ComputeStatus status, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "Finalizer")
        , Params(params)
        , Parent(parent)
        , Pinger(pinger)
        , ExecStatus(execStatus)
        , Status(status)
        , Counters(GetStepCountersSubgroup())
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "FQ_FINALIZER_ACTOR";

    void Start() {
        LOG_I("Start finalizer actor. Compute status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Status));
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Inc();
        Become(&TFinalizerActor::StateFunc);
        if (IsResignQuery()) {
            SendResignQuery();
        } else {
            SendFinalPing();
        }

    }

    FederatedQuery::QueryMeta::ComputeStatus GetFinalStatus() const {
        switch (ExecStatus) {
            case NYdb::NQuery::EExecStatus::Completed:
                return ::FederatedQuery::QueryMeta::COMPLETED;
            case NYdb::NQuery::EExecStatus::Unspecified:
            case NYdb::NQuery::EExecStatus::Starting:
            case NYdb::NQuery::EExecStatus::Aborted:
            case NYdb::NQuery::EExecStatus::Canceled:
            case NYdb::NQuery::EExecStatus::Failed:
            break;
        }

        switch (Status) {
            case FederatedQuery::QueryMeta::COMPLETING:
            case FederatedQuery::QueryMeta::COMPLETED:
                return FederatedQuery::QueryMeta::COMPLETED;
            case FederatedQuery::QueryMeta::ABORTING_BY_USER:
            case FederatedQuery::QueryMeta::ABORTED_BY_USER:
                return FederatedQuery::QueryMeta::ABORTED_BY_USER;
            case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
            case FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM:
                return FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM;
            case FederatedQuery::QueryMeta::STARTING:
            case FederatedQuery::QueryMeta::FAILED:
            case FederatedQuery::QueryMeta::RESUMING:
            case FederatedQuery::QueryMeta::FAILING:
            case FederatedQuery::QueryMeta::RUNNING:
            case FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED:
            case FederatedQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
            case FederatedQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
                return FederatedQuery::QueryMeta::FAILED;
            case FederatedQuery::QueryMeta::PAUSING:
            case FederatedQuery::QueryMeta::PAUSED:
                return FederatedQuery::QueryMeta::PAUSED;
        }
    }

    bool IsResignQuery() const {
        switch (ExecStatus) {
            case NYdb::NQuery::EExecStatus::Completed:
                return false;
            case NYdb::NQuery::EExecStatus::Unspecified:
            case NYdb::NQuery::EExecStatus::Starting:
            case NYdb::NQuery::EExecStatus::Aborted:
            case NYdb::NQuery::EExecStatus::Canceled:
            case NYdb::NQuery::EExecStatus::Failed:
            break;
        }

        switch (Status) {
            case FederatedQuery::QueryMeta::COMPLETING:
            case FederatedQuery::QueryMeta::COMPLETED:
            case FederatedQuery::QueryMeta::ABORTING_BY_USER:
            case FederatedQuery::QueryMeta::ABORTED_BY_USER:
            case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
            case FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM:
            case FederatedQuery::QueryMeta::STARTING:
            case FederatedQuery::QueryMeta::FAILED:
            case FederatedQuery::QueryMeta::RESUMING:
            case FederatedQuery::QueryMeta::FAILING:
            case FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED:
            case FederatedQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
            case FederatedQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
            case FederatedQuery::QueryMeta::PAUSING:
            case FederatedQuery::QueryMeta::PAUSED:
                return false;
            case FederatedQuery::QueryMeta::RUNNING:
                return true;
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

    void Handle(const TEvents::TEvForwardPingResponse::TPtr& ev) {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Dec();
        pingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Success) {
            pingCounters->Ok->Inc();
            LOG_I("Query moved to terminal state ");
            Send(Parent, new TEvYdbCompute::TEvFinalizerResponse({}, NYdb::EStatus::SUCCESS));
            CompleteAndPassAway();
        } else {
            pingCounters->Error->Inc();
            LOG_E("Error moving the query to the terminal state");
            Send(Parent, new TEvYdbCompute::TEvFinalizerResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Error moving the query to the terminal state"}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
        }
    }

    void SendResignQuery() {
        Fq::Private::PingTaskRequest pingTaskRequest;
        pingTaskRequest.set_resign_query(true);
        Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest, true));
    }

    void SendFinalPing() {
        Fq::Private::PingTaskRequest pingTaskRequest;
        if (ExecStatus != NYdb::NQuery::EExecStatus::Completed && Status != FederatedQuery::QueryMeta::COMPLETING) {
            pingTaskRequest.mutable_result_id()->set_value("");
        }
        pingTaskRequest.set_status(GetFinalStatus());
        *pingTaskRequest.mutable_finished_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
        Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest, true));
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Pinger;
    NYdb::NQuery::EExecStatus ExecStatus;
    FederatedQuery::QueryMeta::ComputeStatus Status;
    
    TCounters Counters;
    TInstant StartTime;
};

std::unique_ptr<NActors::IActor> CreateFinalizerActor(const TRunActorParams& params,
                                                      const TActorId& parent,
                                                      const TActorId& pinger,
                                                      NYdb::NQuery::EExecStatus execStatus,
                                                      FederatedQuery::QueryMeta::ComputeStatus status,
                                                      const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TFinalizerActor>(params, parent, pinger, execStatus, status, queryCounters);
}

}
