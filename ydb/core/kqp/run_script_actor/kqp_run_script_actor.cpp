#include "kqp_run_script_actor.h"
#include "kqp_run_script_actor_impl.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <exception>
#include <forward_list>

#define LOG_T(stream) LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_D(stream) LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_I(stream) LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_N(stream) LOG_NOTICE_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_W(stream) LOG_WARN_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_E(stream) LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);

namespace NKikimr::NKqp {

namespace {

using namespace NPrivate;

class TRunScriptActor final : public TActorBootstrapped<TRunScriptActor>, IActorExceptionHandler {
    struct TSessionState {
        bool WaitCreation = false;
        bool SessionOpen = false;

        void Close(const TActorIdentity& actor, const TScriptExecutionContext& ctx) {
            if (!SessionOpen) {
                return;
            }

            auto ev = std::make_unique<TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(ctx.UserRequestContext->SessionId);
            actor.Send(MakeKqpProxyID(actor.NodeId()), ev.release());
            SessionOpen = false;
        }
    };

    struct TActorState {
        bool WaitStop = false;
        TActorId Id;

        void Stop(const TActorIdentity& actor) {
            if (!Id || WaitStop) {
                return;
            }

            actor.Send(Id, new TEvents::TEvPoison());
            WaitStop = true;
        }
    };

public:
    static constexpr char ActorName[] = "KQP_RUN_SCRIPT_ACTOR";

    TRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, TKqpRunScriptActorSettings&& settings, NKikimrConfig::TQueryServiceConfig queryServiceConfig)
        : Ctx(CreateExecutionContext(request, settings, queryServiceConfig))
        , QueryServiceConfig(queryServiceConfig)
        , QueryRequest(CreateQueryRequest(request, settings, queryServiceConfig, *Ctx))
    {}

    void Bootstrap() {
        LOG_I("Bootstrap, StreamingDisposition: " << (Ctx->UserRequestContext->StreamingDisposition ? Ctx->UserRequestContext->StreamingDisposition->DebugString() : "null"));
        Become(&TThis::StateFuncCreating);
    }

private:
    static TScriptExecutionContext::TPtr CreateExecutionContext(const NKikimrKqp::TEvQueryRequest& request, const TKqpRunScriptActorSettings& settings, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig) {
        const auto& traceId = request.GetTraceId();
        auto userRequestContext = MakeIntrusive<TUserRequestContext>(
            traceId,
            settings.Database,
            /* SessionId*/ "", // Will be set after session creation
            settings.ExecutionId,
            settings.CustomerSuppliedId ? settings.CustomerSuppliedId : traceId,
            TActorId{} // Will be set in actor bootstrap
        );
        userRequestContext->IsStreamingQuery = settings.SaveQueryPhysicalGraph;
        userRequestContext->CheckpointId = settings.CheckpointId;
        userRequestContext->StreamingQueryPath = settings.StreamingQueryPath;
        userRequestContext->StreamingDisposition = settings.StreamingDisposition;

        return std::make_shared<TScriptExecutionContext>(TScriptExecutionContext{
            .UserRequestContext = std::move(userRequestContext),
            .Counters = settings.Counters,
            .LeaseGeneration = settings.LeaseGeneration,
            .LeaseDuration = settings.LeaseDuration,
            .ResultsTtl = settings.ResultsTtl,
            .Timeout = GetQueryTimeout(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT, request.GetRequest().GetTimeoutMs(), {}, queryServiceConfig, settings.DisableDefaultTimeout),
        });
    }

    static std::unique_ptr<TEvKqp::TEvQueryRequest> CreateQueryRequest(const NKikimrKqp::TEvQueryRequest& request, const TKqpRunScriptActorSettings& settings, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, const TScriptExecutionContext& ctx) {
        auto ev = std::make_unique<TEvKqp::TEvQueryRequest>();
        ev->Record = request;
        ev->SetSaveQueryPhysicalGraph(settings.SaveQueryPhysicalGraph);
        ev->SetDisableDefaultTimeout(settings.DisableDefaultTimeout);
        ev->SetUserRequestContext(ctx.UserRequestContext);

        if (settings.PhysicalGraph) {
            ev->SetQueryPhysicalGraph(std::move(*settings.PhysicalGraph));
        }

        if (request.GetRequest().GetCollectStats() >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL) {
            ev->SetProgressStatsPeriod(settings.ProgressStatsPeriod ? settings.ProgressStatsPeriod : TDuration::MilliSeconds(queryServiceConfig.GetProgressStatsPeriodMs()));
        }

        ev->SetGeneration(settings.LeaseGeneration);
        return ev;
    }

    bool OnUnhandledException(const std::exception& e) final {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << e.what());
        return true;
    }

    // Wait notification that script execution metadata was stored into `script_executions` table
    STRICT_STFUNC(StateFuncCreating,
        sFunc(TEvents::TEvWakeup, HandleCreatingFinished);
        sFunc(TEvents::TEvPoison, HandleCreatingFailed);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, HandleCancellation);
        hFunc(TEvCheckAliveRequest, HandleCheckAlive);
    )

    void HandleCreatingFinished() {
        if (FinishInfo.IsFinished()) {
            LOG_N("Script execution metadata saved after failure, continue finishing");
            Finish(); // Continue finishing
            return;
        }

        LOG_I("Script execution metadata saved, creating new session");
        Become(&TThis::StateFuncInitialize);

        ScriptLeaseWatcherActor.Id = RegisterWithSameMailbox(CreateScriptLeaseWatcherActor(Ctx));
        LOG_I("Started ScriptLeaseWatcherActor: " << ScriptLeaseWatcherActor.Id);

        auto ev = std::make_unique<TEvKqp::TEvCreateSessionRequest>();
        ev->Record.SetTraceId(Ctx->UserRequestContext->TraceId);
        ev->Record.MutableRequest()->SetDatabase(Ctx->UserRequestContext->Database);
        Send(MakeKqpProxyID(SelfId().NodeId()), ev.release());
        SessionState.WaitCreation = true;
    }

    void HandleCreatingFailed() {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Failed to save script execution entry");
    }

    void HandleCancellation(TEvKqp::TEvCancelScriptExecutionRequest::TPtr& ev) {
        LOG_I("Got cancel request: " << ev->Sender);

        CancelRequests.emplace_front(std::move(ev));

        if (!FinishInfo.IsSuccess()) {
            Finish(Ydb::StatusIds::CANCELLED);
        }
    }

    void HandleCheckAlive(TEvCheckAliveRequest::TPtr& ev) {
        LOG_W("Lease was expired in database, checker actor: " << ev->Sender);
        Send(ev->Sender, new TEvCheckAliveResponse());
    }

    // Create new kqp session
    STRICT_STFUNC(StateFuncInitialize,
        hFunc(TEvKqp::TEvCreateSessionResponse, HandleCreateSession);
        hFunc(TEvRunScriptPrivate::TEvScriptLeaseWatcherFinished, HandleLeaseWatcherFinished);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, HandleCancellation);
        hFunc(TEvCheckAliveRequest, HandleCheckAlive);
    )

    void HandleCreateSession(TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        SessionState.WaitCreation = false;

        const auto& record = ev->Get()->Record;
        if (const auto status = record.GetYdbStatus(); status != Ydb::StatusIds::SUCCESS) {
            const auto resourceExhausted = record.GetResourceExhausted();
            LOG_E("Create new session failed: " << status << ", resource exhausted: " << resourceExhausted);

            auto error = TStringBuilder() << "Create new session failed with " << status;

            if (resourceExhausted) {
                Finish(Ydb::StatusIds::OVERLOADED, error << " (resource exhausted)");
            } else {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, error);
            }

            return;
        }

        SessionState.SessionOpen = true;
        const auto& session = record.GetResponse();
        Ctx->UserRequestContext->SessionId = session.GetSessionId();
        QueryRequest->Record.MutableRequest()->SetSessionId(Ctx->UserRequestContext->SessionId);

        if (FinishInfo.IsFinished()) {
            LOG_N("Session created after finish, continue finishing");
            Finish();
            return;
        }

        if (session.GetNodeId() != SelfId().NodeId()) {
            LOG_E("New session started on unexpected node: " << session.GetNodeId());
            Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Session created on wrong node " << session.GetNodeId() << ", expected local session on node " << SelfId().NodeId());
            return;
        }

        Become(&TThis::StateFuncExecute);

        const auto& physicalGraph = QueryRequest->GetQueryPhysicalGraph();
        ScriptResultHandlerActor.Id = RegisterWithSameMailbox(CreateScriptResultHandlerActor(Ctx, physicalGraph ? std::optional(*physicalGraph) : std::nullopt, QueryServiceConfig));
        LOG_D("Started ScriptResultHandlerActor: " << ScriptResultHandlerActor.Id << ", starting query, has physical graph: " << (physicalGraph ? "YES" : "NO"));

        Ctx->UserRequestContext->RunScriptActorId = ScriptResultHandlerActor.Id;
        ActorIdToProto(ScriptResultHandlerActor.Id, QueryRequest->Record.MutableRequestActorId());
        Send(MakeKqpProxyID(SelfId().NodeId()), QueryRequest.release());
    }

    void HandleLeaseWatcherFinished(TEvRunScriptPrivate::TEvScriptLeaseWatcherFinished::TPtr& ev) {
        ScriptLeaseWatcherActor.Id = {};

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS || !FinishInfo.IsFinished()) {
            const auto& issues = ev->Get()->Issues;
            LOG_E("Got lease watcher finished: " << ev->Sender << " with status " << status << ", issues: " << issues.ToOneLineString());
            Finish(status, AddRootIssue("Script lease watcher error", issues));
        } else {
            LOG_I("Got lease watcher finished: " << ev->Sender);
            Finish();
        }
    }

    // Wait query execution
    STRICT_STFUNC(StateFuncExecute,
        IgnoreFunc(TEvKqp::TEvCloseSessionResponse);
        hFunc(TEvKqp::TEvQueryResponse, HandleExecute);
        hFunc(TEvRunScriptPrivate::TEvScriptLeaseWatcherFinished, HandleLeaseWatcherFinished);
        hFunc(TEvRunScriptPrivate::TEvScriptResultHandlerFinished, HandleResultHandlerFinished);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, HandleCancellation);
        hFunc(TEvCheckAliveRequest, HandleCheckAlive);
    )

    void HandleExecute(TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (!ScriptResultHandlerActor.Id) {
            const auto& record = ev->Get()->Record;
            const auto& response = record.GetResponse();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(response.GetQueryIssues(), issues);
            LOG_W("Ignored query response from " << ev->Sender << ", execution already finished, status: " << record.GetYdbStatus() << ", issues: " << issues.ToOneLineString());
            return;
        }

        LOG_D("Forward query response from " << ev->Sender << " to result handler");
        Forward(ev, ScriptResultHandlerActor.Id);
    }

    void HandleResultHandlerFinished(TEvRunScriptPrivate::TEvScriptResultHandlerFinished::TPtr& ev) {
        ScriptResultHandlerActor.Id = {};

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Got result handler finished: " << ev->Sender << " with status " << status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        } else {
            LOG_I("Got result handler finished: " << ev->Sender);
        }

        ExecutionInfo = std::move(ev->Get()->Info);
        Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
    }

    // Wait query finalization
    STRICT_STFUNC(StateFuncFinalize,
        IgnoreFunc(TEvKqp::TEvCloseSessionResponse);
        IgnoreFunc(TEvKqp::TEvQueryResponse); // Ignored because result handler either not started or already finished
        hFunc(TEvKqp::TEvCreateSessionResponse, HandleCreateSession);
        hFunc(TEvScriptExecutionFinished, HandleFinalize);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, HandleCancellation);
        hFunc(TEvCheckAliveRequest, HandleCheckAlive);
    )

    void HandleFinalize(TEvScriptExecutionFinished::TPtr& ev) {
        auto guard = PassAwayGuard();

        const auto status = ev->Get()->Status;
        const auto& issues = ev->Get()->Issues;
        const auto& info = ev->Get()->Info;
        if (status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Got finalize: " << ev->Sender << " with status " << status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        } else {
            LOG_I("Got finalize: " << ev->Sender << ", already finished: " << info.AlreadyStopped << ", execution entry exists: " << info.ExecutionEntryExists);
        }

        const auto alreadyStopped = info.AlreadyStopped || FinishInfo.IsSuccess();

        for (auto& request : CancelRequests) {
            Send(request->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(status, {
                .ExecutionEntryExists = info.ExecutionEntryExists,
                .AlreadyStopped = alreadyStopped,
            }, issues), /* flags */ 0, request->Cookie);
        }
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void Finish(const Ydb::StatusIds::StatusCode status, const TString& message) {
        Finish(status, {NYql::TIssue(message)});
    }

    void Finish(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Finish with error " << status << ", issues: " << issues.ToOneLineString());
        } else if (!FinishInfo.IsFailed()) {
            LOG_I("Finish successfully");
        }

        FinishInfo.Update(status, std::move(issues));

        if (ScriptResultHandlerActor.Id) {
            LOG_D("Stop script result handler " << ScriptResultHandlerActor.Id);
            ScriptResultHandlerActor.Stop(SelfId());
            return;
        }

        if (SessionState.WaitCreation) {
            LOG_D("Wait for session creation before exit");
            return;
        }

        if (SessionState.SessionOpen) {
            LOG_D("Close session");
            SessionState.Close(SelfId(), *Ctx);
        }

        if (ScriptLeaseWatcherActor.Id) {
            LOG_D("Stop script lease watcher " << ScriptLeaseWatcherActor.Id);
            ScriptLeaseWatcherActor.Stop(SelfId());
            return;
        }

        if (!WaitFinalizationRequest) {
            LOG_I("Start script execution finalization");
            Become(&TThis::StateFuncFinalize);

            const auto cancelledByUser = !CancelRequests.empty();
            if (FinishInfo.IsFailed() && cancelledByUser) {
                NYql::TIssue cancelIssue("Request was canceled by user");
                cancelIssue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
                FinishInfo.Issues.AddIssue(cancelIssue);
                FinishInfo.Status = Ydb::StatusIds::CANCELLED;
            }

            const auto finalizationStatus = FinishInfo.IsSuccess() || cancelledByUser
                ? EFinalizationStatus::FS_COMMIT
                : EFinalizationStatus::FS_ROLLBACK;

            auto execStatus = Ydb::Query::EXEC_STATUS_COMPLETED;
            if ((FinishInfo.IsFailed() && cancelledByUser) || *FinishInfo.Status == Ydb::StatusIds::CANCELLED) {
                execStatus = Ydb::Query::EXEC_STATUS_CANCELLED;
            } else if (FinishInfo.IsFailed()) {
                execStatus = Ydb::Query::EXEC_STATUS_FAILED;
            }

            auto scriptFinalizeRequest = std::make_unique<TEvScriptFinalizeRequest>(
                finalizationStatus, Ctx->UserRequestContext->CurrentExecutionId, Ctx->UserRequestContext->Database,
                *FinishInfo.Status, execStatus, FinishInfo.Issues, std::move(ExecutionInfo.QueryStats),
                std::move(ExecutionInfo.QueryPlan), std::move(ExecutionInfo.QueryAst), Ctx->LeaseGeneration, cancelledByUser
            );
            Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), scriptFinalizeRequest.release());
            WaitFinalizationRequest = true;
        } else {
            LOG_N("Skip finish with error " << *FinishInfo.Status << ", issues: " << FinishInfo.Issues.ToOneLineString() << ", already waiting finalization");
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << ActorName << "] " << SelfId() << ". Ctx: " << *Ctx->UserRequestContext << ". LeaseGeneration: " << Ctx->LeaseGeneration << ". ";
    }

    const TScriptExecutionContext::TPtr Ctx;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    std::unique_ptr<TEvKqp::TEvQueryRequest> QueryRequest;
    TFinishInfo FinishInfo;
    TExecutionInfo ExecutionInfo;
    TSessionState SessionState;
    TActorState ScriptLeaseWatcherActor;
    TActorState ScriptResultHandlerActor;
    std::forward_list<TEvKqp::TEvCancelScriptExecutionRequest::TPtr> CancelRequests;
    bool WaitFinalizationRequest = false;
};

} // namespace

IActor* CreateRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, TKqpRunScriptActorSettings&& settings, NKikimrConfig::TQueryServiceConfig queryServiceConfig) {
    return new TRunScriptActor(request, std::move(settings), std::move(queryServiceConfig));
}

} // namespace NKikimr::NKqp
