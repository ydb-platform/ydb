#include "base_compute_actor.h"

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/ydb/control_plane/compute_database_control_plane_service.h>
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

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Initializer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Initializer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Initializer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Initializer] QueryId: " << Params.QueryId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [Initializer] QueryId: " << Params.QueryId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TInitializerActor : public TBaseComputeActor<TInitializerActor> {
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

    TInitializerActor(const TRunActorParams& params, const TActorId& parent, const TActorId& pinger, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "Initializer")
        , Params(params)
        , Parent(parent)
        , Pinger(pinger)
        , Counters(GetStepCountersSubgroup())
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "FQ_INITIALIZER_ACTOR";

    void Start() {
        LOG_I("Start initializer actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        if (!Params.RequestStartedAt) {
            Become(&TInitializerActor::StateFunc);
            Send(NFq::ComputeDatabaseControlPlaneServiceActorId(), new TEvYdbCompute::TEvCpuQuotaRequest(Params.Scope.ToString(), Params.Deadline));
        } else {
            LOG_I("Query has been initialized (did nothing)");
            Send(Parent, new TEvYdbCompute::TEvInitializerResponse({}, NYdb::EStatus::SUCCESS));
            CompleteAndPassAway();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCpuQuotaResponse, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
    )

    void Handle(TEvYdbCompute::TEvCpuQuotaResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status == NYdb::EStatus::SUCCESS) {
            auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
            pingCounters->InFly->Inc();
            Fq::Private::PingTaskRequest pingTaskRequest;
            pingTaskRequest.set_current_load(response.CurrentLoad);
            *pingTaskRequest.mutable_started_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
            Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest));
        } else {
            Send(Parent, new TEvYdbCompute::TEvInitializerResponse(response.Issues, response.Status));
            FailedAndPassAway();
        }
    }

    void Handle(const TEvents::TEvForwardPingResponse::TPtr& ev) {
        auto pingCounters = Counters.GetCounters(ERequestType::RT_PING);
        pingCounters->InFly->Dec();
        pingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev.Get()->Get()->Success) {
            pingCounters->Ok->Inc();
            LOG_I("Query has been initialized");
            Send(Parent, new TEvYdbCompute::TEvInitializerResponse({}, NYdb::EStatus::SUCCESS));
            CompleteAndPassAway();
        } else {
            pingCounters->Error->Inc();
            LOG_E("Error initialization query");
            Send(Parent, new TEvYdbCompute::TEvInitializerResponse(NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Error moving the query to the terminal state"}}, NYdb::EStatus::INTERNAL_ERROR));
            FailedAndPassAway();
        }
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Pinger;
    TCounters Counters;
    TInstant StartTime;
};

std::unique_ptr<NActors::IActor> CreateInitializerActor(const TRunActorParams& params,
                                                        const TActorId& parent,
                                                        const TActorId& pinger,
                                                        const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TInitializerActor>(params, parent, pinger, queryCounters);
}

}
