#include "base_compute_actor.h"
#include "resources_cleaner_actor.h"

#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/compute/common/metrics.h>
#include <ydb/core/fq/libs/compute/common/retry_actor.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>


#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResourcesCleaner] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResourcesCleaner] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResourcesCleaner] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResourcesCleaner] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ResourcesCleaner] QueryId: " << Params.QueryId << " OperationId: " << ProtoToString(OperationId) << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TResourcesCleanerActor : public TBaseComputeActor<TResourcesCleanerActor> {
public:
    enum ERequestType {
        RT_FORGET_OPERATION,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("ForgetOperation") }
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

    TResourcesCleanerActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const NYdb::TOperation::TOperationId& operationId, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "ResourcesCleaner")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , OperationId(operationId)
        , Counters(GetStepCountersSubgroup())
        , BackoffTimer(20, 1000)
    {}

    static constexpr char ActorName[] = "FQ_RESOURCES_CLEANER_ACTOR";

    void SendForgetOperation(const TDuration& delay = TDuration::Zero()) {
        Register(new TRetryActor<TEvYdbCompute::TEvForgetOperationRequest, TEvYdbCompute::TEvForgetOperationResponse, NYdb::TOperation::TOperationId>(Counters.GetCounters(ERequestType::RT_FORGET_OPERATION), delay, SelfId(), Connector, OperationId));
    }

    void Start() {
        LOG_I("Start resources cleaner actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        Become(&TResourcesCleanerActor::StateFunc);
        SendForgetOperation();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvForgetOperationResponse, Handle);
    )

    void Handle(const TEvYdbCompute::TEvForgetOperationResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status == NYdb::EStatus::TIMEOUT || response.Status == NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED) {
            LOG_I("Operation partly forgotten, will be retried: " << response.Status);
            SendForgetOperation(TDuration::MilliSeconds(BackoffTimer.NextBackoffMs()));
            return;
        }
        if (response.Status != NYdb::EStatus::SUCCESS && response.Status != NYdb::EStatus::NOT_FOUND) {
            LOG_E("Can't forget operation: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvYdbCompute::TEvResourcesCleanerResponse(ev->Get()->Issues, ev->Get()->Status));
            FailedAndPassAway();
            return;
        }
        LOG_I("Operation successfully forgotten " << response.Status);
        Send(Parent, new TEvYdbCompute::TEvResourcesCleanerResponse({}, NYdb::EStatus::SUCCESS));
        CompleteAndPassAway();
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Connector;
    NYdb::TOperation::TOperationId OperationId;
    TCounters Counters;
    NKikimr::TBackoffTimer BackoffTimer;
};

std::unique_ptr<NActors::IActor> CreateResourcesCleanerActor(const TRunActorParams& params,
                                                             const TActorId& parent,
                                                             const TActorId& connector,
                                                             const NYdb::TOperation::TOperationId& operationId,
                                                             const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TResourcesCleanerActor>(params, parent, connector, operationId, queryCounters);
}

}
