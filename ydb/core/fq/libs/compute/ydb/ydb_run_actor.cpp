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
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FQ_RUN_ACTOR


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
        YDB_LOG_INFO("[ydb] Boostrap.",
            {"QueryId", Params.QueryId},
            {"Params", Params});
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
            YDB_LOG_INFO("[ydb] InitializerResponse (failed).",
                {"QueryId", Params.QueryId},
                {"Issues", response.Issues.ToOneLineString()});
            ResignAndPassAway(response.Issues);
            return;
        }

        YDB_LOG_INFO("[ydb] InitializerResponse (success)",
            {"QueryId", Params.QueryId});
        Register(ActorFactory->CreateExecuter(SelfId(), Connector, Pinger).release());
    }

    void Handle(const TEvYdbCompute::TEvExecuterResponse::TPtr& ev) {
        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            YDB_LOG_INFO("[ydb] ExecuterResponse (failed).",
                {"QueryId", Params.QueryId},
                {"Issues", response.Issues.ToOneLineString()});
            ResignAndPassAway(response.Issues);
            return;
        }
        Params.ExecutionId = response.ExecutionId;
        Params.OperationId = response.OperationId;
        YDB_LOG_INFO("[ydb] ExecuterResponse (success).",
            {"QueryId", Params.QueryId},
            {"ExecutionId", Params.ExecutionId},
            {"OperationId", Params.OperationId.ToString()});
        Register(ActorFactory->CreateStatusTracker(SelfId(), Connector, Pinger, Params.OperationId).release());
    }

    void Handle(const TEvYdbCompute::TEvStatusTrackerResponse::TPtr& ev) {
        if (CancelOperationIsRunning("StatusTrackerResponse (aborting). ")) {
            return;
        }

        auto& response = *ev->Get();
        if (response.Status == NYdb::EStatus::NOT_FOUND) { // FAILING / ABORTING_BY_USER / ABORTING_BY_SYSTEM
            YDB_LOG_INFO("[ydb] StatusTrackerResponse (not found).",
                {"QueryId", Params.QueryId},
                {"Status", response.Status},
                {"Issues", response.Issues.ToOneLineString()});
            CreateFinalizer(Params.Status);
            return;
        }

        if (response.Status != NYdb::EStatus::SUCCESS) {
            YDB_LOG_INFO("[ydb] StatusTrackerResponse (failed).",
                {"QueryId", Params.QueryId},
                {"Status", response.Status},
                {"Issues", response.Issues.ToOneLineString()});
            ResignAndPassAway(response.Issues);
            return;
        }

        if (!OperationIsFailing() || response.ExecStatus != NYdb::NQuery::EExecStatus::Completed) {
            ExecStatus = response.ExecStatus;
            Params.Status = response.ComputeStatus;
        }

        YDB_LOG_INFO("[ydb] StatusTrackerResponse (success)",
            {"QueryId", Params.QueryId},
            {"Status", response.Status},
            {"ExecStatus", static_cast<int>(response.ExecStatus)},
            {"Issues", response.Issues.ToOneLineString()});
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
            YDB_LOG_INFO("[ydb] ResultWriterResponse (failed).",
                {"QueryId", Params.QueryId},
                {"Status", response.Status},
                {"Issues", response.Issues.ToOneLineString()});
            ResignAndPassAway(response.Issues);
            return;
        }
        YDB_LOG_INFO("[ydb] ResultWriterResponse (success)",
            {"QueryId", Params.QueryId},
            {"Status", response.Status},
            {"Issues", response.Issues.ToOneLineString()});
        CreateResourcesCleaner();
    }

    void Handle(const TEvYdbCompute::TEvResourcesCleanerResponse::TPtr& ev) {
        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS && response.Status != NYdb::EStatus::UNSUPPORTED) {
            YDB_LOG_INFO("[ydb] ResourcesCleanerResponse (failed).",
                {"QueryId", Params.QueryId},
                {"Status", response.Status},
                {"Issues", response.Issues.ToOneLineString()});
            ResignAndPassAway(response.Issues);
            return;
        }
        YDB_LOG_INFO("[ydb] ResourcesCleanerResponse (success)",
            {"QueryId", Params.QueryId},
            {"Status", response.Status},
            {"Issues", response.Issues.ToOneLineString()});
        CreateFinalizer(IsAborted ? FederatedQuery::QueryMeta::ABORTING_BY_USER : Params.Status);
    }

    void Handle(const TEvYdbCompute::TEvFinalizerResponse::TPtr ev) {
        // Pinger is no longer available at this place.
        // The query can be restarted only after the expiration of lease in case of error
        auto& response = *ev->Get();
        YDB_LOG_INFO("[ydb] FinalizerResponse (",
            {"QueryId", Params.QueryId},
            {"#_num_0", (response.Status == NYdb::EStatus::SUCCESS ? "success" : "failed")},
            {"Status", response.Status},
            {"Issues", response.Issues.ToOneLineString()});
        FinishAndPassAway();
    }

    void Handle(TEvents::TEvQueryActionResult::TPtr& ev) {
        YDB_LOG_INFO("[ydb]",
            {"QueryId", Params.QueryId},
            {"QueryActionResult", FederatedQuery::QueryAction_Name(ev->Get()->Action)});
        // Start cancel operation only when StatusTracker or ResultWriter is running
        if (Params.OperationId.GetKind() != NKikimr::NOperationId::TOperationId::UNUSED && !IsAborted && !FinalizationStarted) {
            IsAborted = true;
            Register(ActorFactory->CreateStopper(SelfId(), Connector, Pinger, Params.OperationId).release());
        }
    }

    void Handle(const TEvYdbCompute::TEvStopperResponse::TPtr& ev) {
        auto& response = *ev->Get();
        if (response.Status != NYdb::EStatus::SUCCESS) {
            YDB_LOG_INFO("[ydb] StopperResponse (failed).",
                {"QueryId", Params.QueryId},
                {"Status", response.Status},
                {"Issues", response.Issues.ToOneLineString()});
            ResignAndPassAway(response.Issues);
            return;
        }
        YDB_LOG_INFO("[ydb] StopperResponse (success)",
            {"QueryId", Params.QueryId},
            {"Status", response.Status},
            {"Issues", response.Issues.ToOneLineString()});
        CreateResourcesCleaner();
    }

    void Run() { // recover points
        switch (Params.Status) {
        case FederatedQuery::QueryMeta::STARTING:
            Register(ActorFactory->CreateInitializer(SelfId(), Pinger).release());
            break;
        case FederatedQuery::QueryMeta::RUNNING:
            if (Params.OperationId.GetKind() == NKikimr::NOperationId::TOperationId::UNUSED) {
                Register(ActorFactory->CreateExecuter(SelfId(), Connector, Pinger).release()); // restart query
            } else {
                Register(ActorFactory->CreateStatusTracker(SelfId(), Connector, Pinger, Params.OperationId).release());
            }
            break;
        case FederatedQuery::QueryMeta::COMPLETING:
            if (Params.OperationId.GetKind() != NKikimr::NOperationId::TOperationId::UNUSED) {
                Register(ActorFactory->CreateResultWriter(SelfId(), Connector, Pinger, Params.OperationId, false).release());
            } else {
                CreateFinalizer(Params.Status);
            }
            break;
        case FederatedQuery::QueryMeta::FAILING:
        case FederatedQuery::QueryMeta::ABORTING_BY_USER:
        case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
            if (Params.OperationId.GetKind() != NKikimr::NOperationId::TOperationId::UNUSED) {
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

        YDB_LOG_INFO("[ydb] Stop task execution, cancel operation now is running",
            {"QueryId", Params.QueryId},
            {"stage", stage});
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
