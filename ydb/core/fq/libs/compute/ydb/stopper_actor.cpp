#include "base_compute_actor.h"
#include "resources_cleaner_actor.h"

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
    enum ERequestType {
        RT_CANCEL_OPERATION,
        RT_MAX
    };

    class TCounters: public virtual TThrRefBase {
        std::array<TComputeRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TComputeRequestCountersPtr>({
            { MakeIntrusive<TComputeRequestCounters>("CancelOperation") }
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

    TStopperActor(const TRunActorParams& params, const TActorId& parent, const TActorId& connector, const NYdb::TOperation::TOperationId& operationId, const ::NYql::NCommon::TServiceCounters& queryCounters)
        : TBaseComputeActor(queryCounters, "Stopper")
        , Params(params)
        , Parent(parent)
        , Connector(connector)
        , OperationId(operationId)
        , Counters(GetStepCountersSubgroup())
    {}

    static constexpr char ActorName[] = "FQ_STOPPER_ACTOR";

    void Start() {
        LOG_I("Start stopper actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        Become(&TStopperActor::StateFunc);
        Register(new TRetryActor<TEvPrivate::TEvCancelOperationRequest, TEvPrivate::TEvCancelOperationResponse, NYdb::TOperation::TOperationId>(Counters.GetCounters(ERequestType::RT_CANCEL_OPERATION), SelfId(), Connector, OperationId));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCancelOperationResponse, Handle);
    )

    void Handle(const TEvPrivate::TEvCancelOperationResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_E("Can't cancel operation: " << ev->Get()->Issues.ToOneLineString());
            Send(Parent, new TEvPrivate::TEvStopperResponse(ev->Get()->Issues, ev->Get()->Status));
            FailedAndPassAway();
            return;
        }
        LOG_I("Operation successfully canceled");
        Send(Parent, new TEvPrivate::TEvStopperResponse({}, NYdb::EStatus::SUCCESS));
        CompleteAndPassAway();
    }

private:
    TRunActorParams Params;
    TActorId Parent;
    TActorId Connector;
    NYdb::TOperation::TOperationId OperationId;
    TCounters Counters;
};

std::unique_ptr<NActors::IActor> CreateStopperActor(const TRunActorParams& params,
                                                    const TActorId& parent,
                                                    const TActorId& connector,
                                                    const NYdb::TOperation::TOperationId& operationId,
                                                    const ::NYql::NCommon::TServiceCounters& queryCounters) {
    return std::make_unique<TStopperActor>(params, parent, connector, operationId, queryCounters);
}

}
