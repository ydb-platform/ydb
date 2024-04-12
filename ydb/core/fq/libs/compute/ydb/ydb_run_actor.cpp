#include "ydb_run_actor.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/time_util.h>

#include <util/string/split.h>
#include <util/system/hostname.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <ydb/core/fq/libs/compute/common/pinger.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/fq/libs/private_client/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>


#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] QueryId: " << Params.QueryId << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] QueryId: " << Params.QueryId << " " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] QueryId: " << Params.QueryId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] QueryId: " << Params.QueryId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] QueryId: " << Params.QueryId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NFq;

class TYdbRunActor : public NActors::TActorBootstrapped<TYdbRunActor> {
public:
    explicit TYdbRunActor(
        const TActorId& fetcherId,
        const ::NYql::NCommon::TServiceCounters& queryCounters,
        TRunActorParams&& params,
        const IActorFactory::TPtr& actorFactory)
        : FetcherId(fetcherId)
        , Params(std::move(params))
        , CreatedAt(Params.CreatedAt)
        , QueryCounters(queryCounters)
        , ActorFactory(actorFactory)
    {}

    static constexpr char ActorName[] = "YDB_RUN_ACTOR";

    void Bootstrap() {
        LOG_I("Boostrap. " << Params);
        Pinger = Register(ActorFactory->CreatePinger(SelfId()).release());
        Connector = Register(ActorFactory->CreateConnector().release());
        Become(&TYdbRunActor::StateFunc);
        Run();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvInitializerResponse, Handle);
        hFunc(TEvYdbCompute::TEvExecuterResponse, Handle);
        hFunc(TEvYdbCompute::TEvStatusTrackerResponse, Handle);
        hFunc(TEvYdbCompute::TEvResultWriterResponse, Handle);
        hFunc(TEvYdbCompute::TEvResourcesCleanerResponse, Handle);
        hFunc(TEvYdbCompute::TEvFinalizerResponse, Handle);
        hFunc(TEvYdbCompute::TEvStopperResponse, Handle);
        hFunc(TEvents::TEvQueryActionResult, Handle);
    )

    void Handle(const TEvYdbCompute::TEvInitializerResponse::TPtr& ev) {
        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_I("InitializerResponse (failed). Issues: " << response.Issues.ToOneLineString());
            ResignAndPassAway(response.Issues);
            return;
        }

        LOG_I("InitializerResponse (success)");
        Register(ActorFactory->CreateExecuter(SelfId(), Connector, Pinger).release());
    }

    void Handle(const TEvYdbCompute::TEvExecuterResponse::TPtr& ev) {
        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_I("ExecuterResponse (failed). Issues: " << response.Issues.ToOneLineString());
            ResignAndPassAway(response.Issues);
            return;
        }
        Params.ExecutionId = response.ExecutionId;
        Params.OperationId = response.OperationId;
        LOG_I("ExecuterResponse (success). ExecutionId: " << Params.ExecutionId << " OperationId: " << ProtoToString(Params.OperationId));
        Register(ActorFactory->CreateStatusTracker(SelfId(), Connector, Pinger, Params.OperationId).release());
    }

    void Handle(const TEvYdbCompute::TEvStatusTrackerResponse::TPtr& ev) {
        if (CancelOperationIsRunning("StatusTrackerResponse (aborting). ")) {
            return;
        }

        auto& response = *ev->Get();
        if (response.Status == NYdb::EStatus::NOT_FOUND) { // FAILING / ABORTING_BY_USER / ABORTING_BY_SYSTEM
            LOG_I("StatusTrackerResponse (not found). Status: " << response.Status << " Issues: " << response.Issues.ToOneLineString());
            CreateFinalizer(Params.Status);
            return;
        }

        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_I("StatusTrackerResponse (failed). Status: " << response.Status << " Issues: " << response.Issues.ToOneLineString());
            ResignAndPassAway(response.Issues);
            return;
        }

        if (!OperationIsFailing() || response.ExecStatus != NYdb::NQuery::EExecStatus::Completed) {
            ExecStatus = response.ExecStatus;
            Params.Status = response.ComputeStatus;
        }

        LOG_I("StatusTrackerResponse (success) " << response.Status << " ExecStatus: " << static_cast<int>(response.ExecStatus) << " Issues: " << response.Issues.ToOneLineString());
        if (ExecStatus == NYdb::NQuery::EExecStatus::Completed) {
            Register(ActorFactory->CreateResultWriter(SelfId(), Connector, Pinger, Params.OperationId, true).release());
        } else {
            CreateResourcesCleaner();
        }
    }

    void Handle(const TEvYdbCompute::TEvResultWriterResponse::TPtr& ev) {
        if (CancelOperationIsRunning("ResultWriterResponse (aborting). ")) {
            return;
        }

        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_I("ResultWriterResponse (failed). Status: " << response.Status << " Issues: " << response.Issues.ToOneLineString());
            ResignAndPassAway(response.Issues);
            return;
        }
        LOG_I("ResultWriterResponse (success) " << response.Status << " Issues: " << response.Issues.ToOneLineString());
        CreateResourcesCleaner();
    }

    void Handle(const TEvYdbCompute::TEvResourcesCleanerResponse::TPtr& ev) {
        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS && response.Status != NYdb::EStatus::UNSUPPORTED) {
            LOG_I("ResourcesCleanerResponse (failed). Status: " << response.Status << " Issues: " << response.Issues.ToOneLineString());
            ResignAndPassAway(response.Issues);
            return;
        }
        LOG_I("ResourcesCleanerResponse (success) " << response.Status << " Issues: " << response.Issues.ToOneLineString());
        CreateFinalizer(IsAborted ? FederatedQuery::QueryMeta::ABORTING_BY_USER : Params.Status);
    }

    void Handle(const TEvYdbCompute::TEvFinalizerResponse::TPtr ev) {
        // Pinger is no longer available at this place.
        // The query can be restarted only after the expiration of lease in case of error
        auto& response = *ev->Get();
        LOG_I("FinalizerResponse ( " << (response.Status == NYdb::EStatus::SUCCESS ? "success" : "failed") << " ) " << response.Status << " Issues: " << response.Issues.ToOneLineString());
        FinishAndPassAway();
    }

    void Handle(TEvents::TEvQueryActionResult::TPtr& ev) {
        LOG_I("QueryActionResult: " << FederatedQuery::QueryAction_Name(ev->Get()->Action));
        // Start cancel operation only when StatusTracker or ResultWriter is running
        if (Params.OperationId.GetKind() != Ydb::TOperationId::UNUSED && !IsAborted && !FinalizationStarted) {
            IsAborted = true;
            Register(ActorFactory->CreateStopper(SelfId(), Connector, Pinger, Params.OperationId).release());
        }
    }

    void Handle(const TEvYdbCompute::TEvStopperResponse::TPtr& ev) {
        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            LOG_I("StopperResponse (failed). Status: " << response.Status << " Issues: " << response.Issues.ToOneLineString());
            ResignAndPassAway(response.Issues);
            return;
        }
        LOG_I("StopperResponse (success) " << response.Status << " Issues: " << response.Issues.ToOneLineString());
        CreateResourcesCleaner();
    }

    void Run() { // recover points
        switch (Params.Status) {
        case FederatedQuery::QueryMeta::STARTING:
            Register(ActorFactory->CreateInitializer(SelfId(), Pinger).release());
            break;
        case FederatedQuery::QueryMeta::RUNNING:
            if (Params.OperationId.GetKind() == Ydb::TOperationId::UNUSED) {
                Register(ActorFactory->CreateExecuter(SelfId(), Connector, Pinger).release()); // restart query
            } else {
                Register(ActorFactory->CreateStatusTracker(SelfId(), Connector, Pinger, Params.OperationId).release());
            }
            break;
        case FederatedQuery::QueryMeta::COMPLETING:
            if (Params.OperationId.GetKind() != Ydb::TOperationId::UNUSED) {
                Register(ActorFactory->CreateResultWriter(SelfId(), Connector, Pinger, Params.OperationId, false).release());
            } else {
                CreateFinalizer(Params.Status);
            }
            break;
        case FederatedQuery::QueryMeta::FAILING:
        case FederatedQuery::QueryMeta::ABORTING_BY_USER:
        case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
            if (Params.OperationId.GetKind() != Ydb::TOperationId::UNUSED) {
                Register(ActorFactory->CreateStatusTracker(SelfId(), Connector, Pinger, Params.OperationId).release());
            } else {
                CreateFinalizer(Params.Status);
            }
            break;
        default:
            break;
        }
    }

    void ResignAndPassAway(const NYql::TIssues& issues) {
        Send(FetcherId, new NActors::TEvents::TEvPoisonTaken());
        Fq::Private::PingTaskRequest pingTaskRequest;
        NYql::IssuesToMessage(issues, pingTaskRequest.mutable_transient_issues());
        pingTaskRequest.set_resign_query(true);
        // We consider any problems with requests to the external system as unavailability.
        // This is necessary for correct retries
        pingTaskRequest.set_status_code(NYql::NDqProto::StatusIds::UNAVAILABLE);
        Send(Pinger, new TEvents::TEvForwardPingRequest(pingTaskRequest, true));
        FinishAndPassAway();
    }

    void FinishAndPassAway() {
        Send(FetcherId, new NActors::TEvents::TEvPoisonTaken());
        Send(Connector, new NActors::TEvents::TEvPoisonPill());
        PassAway();
    }

    void CreateResourcesCleaner() {
        FinalizationStarted = true;
        Register(ActorFactory->CreateResourcesCleaner(SelfId(), Connector, Params.OperationId).release());
    }

    void CreateFinalizer(FederatedQuery::QueryMeta::ComputeStatus status) {
        FinalizationStarted = true;
        Register(ActorFactory->CreateFinalizer(Params, SelfId(), Pinger, ExecStatus, status).release());
    }

    bool CancelOperationIsRunning(const TString& stage) const {
        if (!IsAborted) {
            return false;
        }

        LOG_I(stage << "Stop task execution, cancel operation now is running");
        return true;
    }

    bool OperationIsFailing() const {
        return Params.Status == FederatedQuery::QueryMeta::FAILING
            || Params.Status == FederatedQuery::QueryMeta::ABORTING_BY_USER
            || Params.Status == FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM;
    }

private:
    bool IsAborted = false;
    bool FinalizationStarted = false;
    TActorId FetcherId;
    NYdb::NQuery::EExecStatus ExecStatus = NYdb::NQuery::EExecStatus::Unspecified;
    TRunActorParams Params;
    TInstant CreatedAt;
    NYql::TIssues TransientIssues;
    ::NYql::NCommon::TServiceCounters QueryCounters;
    TActorId Pinger;
    TActorId Connector;
    IActorFactory::TPtr ActorFactory;
};

IActor* CreateYdbRunActor(
    const NActors::TActorId& fetcherId,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    TRunActorParams&& params,
    const IActorFactory::TPtr& actorFactory
) {
    return new TYdbRunActor(fetcherId, serviceCounters, std::move(params), actorFactory);
}

} /* NFq */
