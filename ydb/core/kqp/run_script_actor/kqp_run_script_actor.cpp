#include "kqp_run_script_actor.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/proto/result_set_meta.pb.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/size_literals.h>

#include <forward_list>

#define LOG_T(stream) LOG_TRACE_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". " << "Ctx: " << *UserRequestContext << ". " << stream);
#define LOG_D(stream) LOG_DEBUG_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". " << "Ctx: " << *UserRequestContext << ". " << stream);
#define LOG_I(stream) LOG_INFO_S (NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". " << "Ctx: " << *UserRequestContext << ". " << stream);
#define LOG_W(stream) LOG_WARN_S (NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". " << "Ctx: " << *UserRequestContext << ". " << stream);
#define LOG_E(stream) LOG_ERROR_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". " << "Ctx: " << *UserRequestContext << ". " << stream);

namespace NKikimr::NKqp {

namespace {

constexpr ui32 LEASE_UPDATE_FREQUENCY = 2;

constexpr ui64 MIN_SAVE_RESULT_BATCH_SIZE = 5_MB;
constexpr i32 MIN_SAVE_RESULT_BATCH_ROWS = 5000;
constexpr ui64 RUN_SCRIPT_ACTOR_BUFFER_SIZE = 40_MB;

class TRunScriptActor : public NActors::TActorBootstrapped<TRunScriptActor> {
    enum class ERunState {
        Created,
        Running,
        Cancelling,
        Cancelled,
        Finishing,
        Finished,
    };

    enum EWakeUp {
        RunEvent,
        UpdateLeaseEvent,
    };

    struct TResultSetInfo {
        bool Truncated = false;
        ui64 RowCount = 0;
        ui64 ByteCount = 0;
        NJson::TJsonValue* Meta;

        ui64 FirstRowId = 0;
        ui64 AccumulatedSize = 0;
        Ydb::ResultSet PendingResult;
    };

    struct TPendingAck {
        TActorId ReplyActorId;
        THolder<TEvKqpExecuter::TEvStreamDataAck> AckEvent;
    };

public:
    TRunScriptActor(const TString& executionId, const NKikimrKqp::TEvQueryRequest& request, const TString& database, ui64 leaseGeneration, TDuration leaseDuration, TDuration resultsTtl, NKikimrConfig::TQueryServiceConfig&& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : ExecutionId(executionId)
        , Request(request)
        , Database(database)
        , LeaseGeneration(leaseGeneration)
        , LeaseDuration(leaseDuration)
        , ResultsTtl(resultsTtl)
        , QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {
        UserRequestContext = MakeIntrusive<TUserRequestContext>(Request.GetTraceId(), Database, "", ExecutionId, Request.GetTraceId());
    }

    static constexpr char ActorName[] = "KQP_RUN_SCRIPT_ACTOR";

    void Bootstrap() {
        Become(&TRunScriptActor::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(NActors::TEvents::TEvPoison, Handle);
        hFunc(TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(TEvKqpExecuter::TEvExecuterProgress, Handle);
        hFunc(TEvKqp::TEvQueryResponse, Handle);
        hFunc(TEvKqp::TEvCreateSessionResponse, Handle);
        IgnoreFunc(TEvKqp::TEvCloseSessionResponse);
        hFunc(TEvKqp::TEvCancelQueryResponse, Handle);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, Handle);
        hFunc(TEvScriptExecutionFinished, Handle);
        hFunc(TEvScriptLeaseUpdateResponse, Handle);
        hFunc(TEvSaveScriptResultMetaFinished, Handle);
        hFunc(TEvSaveScriptResultFinished, Handle);
        hFunc(TEvCheckAliveRequest, Handle);
    )

    void SendToKqpProxy(THolder<NActors::IEventBase> ev) {
        if (!Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release())) {
            Issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error"));
            Finish(Ydb::StatusIds::INTERNAL_ERROR);
        }
    }

    void CreateSession() {
        auto ev = MakeHolder<TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Request.GetRequest().GetDatabase());

        SendToKqpProxy(std::move(ev));
    }

    void Handle(TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        const auto& resp = ev->Get()->Record;
        if (resp.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            if (resp.GetResourceExhausted()) {
                Finish(Ydb::StatusIds::OVERLOADED);
            } else {
                Finish(Ydb::StatusIds::INTERNAL_ERROR);
            }
        } else {
            SessionId = resp.GetResponse().GetSessionId();
            UserRequestContext->SessionId = SessionId;

            if (RunState == ERunState::Running) {
                Start();
            } else {
                CloseSession();
            }
        }
    }

    void CloseSession() {
        if (SessionId) {
            auto ev = MakeHolder<TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(SessionId);

            Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
            SessionId.clear();
        }
    }

    void Start() {
        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();
        ev->Record = Request;
        ev->Record.MutableRequest()->SetSessionId(SessionId);
        ev->SetUserRequestContext(UserRequestContext);
        if (ev->Record.GetRequest().GetCollectStats() >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL) {
            ev->SetProgressStatsPeriod(TDuration::MilliSeconds(QueryServiceConfig.GetProgressStatsPeriodMs()));
        }

        NActors::ActorIdToProto(SelfId(), ev->Record.MutableRequestActorId());

        LOG_I("Start Script Execution");
        SendToKqpProxy(std::move(ev));
    }

    void Handle(TEvCheckAliveRequest::TPtr& ev) {
        LOG_W("Lease was expired in database"
            << ", execution id: " << ExecutionId
            << ", saved final status: " << FinalStatusIsSaved
            << ", wait finalization request: " << WaitFinalizationRequest
            << ", is executing: " << IsExecuting()
            << ", current status: " << Status);

        Send(ev->Sender, new TEvCheckAliveResponse());
    }

    void RunLeaseUpdater() {
        Register(CreateScriptLeaseUpdateActor(SelfId(), Database, ExecutionId, LeaseDuration, Counters));
        LeaseUpdateQueryRunning = true;
        Counters->ReportRunActorLeaseUpdateBacklog(TInstant::Now() - LeaseUpdateScheduleTime);
    }

    bool LeaseUpdateRequired() const {
        return IsExecuting() || SaveResultMetaInflight || SaveResultInflight;
    }

    // TODO: remove this after there will be a normal way to store results and generate execution id
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
        case EWakeUp::RunEvent:
            LeaseUpdateScheduleTime = TInstant::Now();
            Schedule(LeaseDuration / LEASE_UPDATE_FREQUENCY, new NActors::TEvents::TEvWakeup(EWakeUp::UpdateLeaseEvent));
            RunState = ERunState::Running;
            CreateSession();
            break;

        case EWakeUp::UpdateLeaseEvent:
            if (LeaseUpdateRequired() && !FinalStatusIsSaved) {
                RunLeaseUpdater();
            }
            break;
        }
    }

    void TerminateActorExecution(Ydb::StatusIds::StatusCode replyStatus, const NYql::TIssues& replyIssues) {
        for (auto& req : CancelRequests) {
            Send(req->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(replyStatus, replyIssues));
        }
        PassAway();
    }

    void RunScriptExecutionFinisher() {
        if (!FinalStatusIsSaved) {
            FinalStatusIsSaved = true;
            WaitFinalizationRequest = true;
            RunState = IsExecuting() ? ERunState::Finishing : RunState;

            auto scriptFinalizeRequest = std::make_unique<TEvScriptFinalizeRequest>(
                GetFinalizationStatusFromRunState(), ExecutionId, Database, Status, GetExecStatusFromStatusCode(Status),
                Issues, std::move(QueryStats), std::move(QueryPlan), std::move(QueryAst), LeaseGeneration
            );
            Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), scriptFinalizeRequest.release());
            return;
        }

        if (!WaitFinalizationRequest && RunState != ERunState::Cancelled && RunState != ERunState::Finished) {
            RunState = ERunState::Finished;
            TerminateActorExecution(Ydb::StatusIds::PRECONDITION_FAILED, { NYql::TIssue("Already finished") });
        }
    }

    void Handle(TEvScriptLeaseUpdateResponse::TPtr& ev) {
        LeaseUpdateQueryRunning = false;

        if (!ev->Get()->ExecutionEntryExists) {
            FinalStatusIsSaved = true;
            if (RunState == ERunState::Running) {
                CancelRunningQuery();
            }
        }

        if (FinishAfterLeaseUpdate) {
            RunScriptExecutionFinisher();
        } else if (IsExecuting() && ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(ev->Get()->Status);
        }

        if (LeaseUpdateRequired()) {
            TInstant leaseUpdateTime = ev->Get()->CurrentDeadline - LeaseDuration / LEASE_UPDATE_FREQUENCY;
            LeaseUpdateScheduleTime = TInstant::Now();
            if (TInstant::Now() >= leaseUpdateTime) {
                RunLeaseUpdater();
            } else {
                Schedule(leaseUpdateTime, new NActors::TEvents::TEvWakeup(EWakeUp::UpdateLeaseEvent));
            }
        }
    }

    // Event in case of error in registering script in database
    // Just pass away, because we have not started execution.
    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        Y_ABORT_UNLESS(RunState == ERunState::Created);
        PassAway();
    }

    void SendStreamDataResponse() {
        if (PendingAcks.empty()) {
            return;
        }

        if (PendingResultSetsSize > RUN_SCRIPT_ACTOR_BUFFER_SIZE) {
            // Try to save any pending result
            SaveResult();
        }

        if (PendingResultSetsSize <= RUN_SCRIPT_ACTOR_BUFFER_SIZE) {
            while (!PendingAcks.empty()) {
                auto response = std::move(PendingAcks.front());
                PendingAcks.pop();

                LOG_D("Send stream data ack"
                    << ", seqNo: " << response.AckEvent->Record.GetSeqNo()
                    << ", to: " << response.ReplyActorId);

                Send(response.ReplyActorId, response.AckEvent.Release());
            }
        }
    }

    void SaveResult(size_t resultSetId) {
        if (SaveResultInflight) {
            return;
        }

        if (!ExpireAt && ResultsTtl > TDuration::Zero()) {
            ExpireAt = TInstant::Now() + ResultsTtl;
        }

        auto& resultSetInfo = ResultSetInfos[resultSetId];
        Register(CreateSaveScriptExecutionResultActor(SelfId(), Database, ExecutionId, resultSetId, ExpireAt, resultSetInfo.FirstRowId, resultSetInfo.AccumulatedSize, std::move(resultSetInfo.PendingResult)));
        SaveResultInflight++;
        PendingResultSetsSize -= resultSetInfo.ByteCount - resultSetInfo.AccumulatedSize;
        resultSetInfo.FirstRowId = resultSetInfo.RowCount;
        resultSetInfo.AccumulatedSize = resultSetInfo.ByteCount;
        resultSetInfo.PendingResult = Ydb::ResultSet();
    }

    void SaveResult() {
        for (size_t resultSetId = 0; resultSetId < ResultSetInfos.size(); ++resultSetId) {
            if (ResultSetInfos[resultSetId].PendingResult.rows_size()) {
                SaveResult(resultSetId);
                break;
            }
        }
    }

    void Handle(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        if (RunState != ERunState::Running) {
            return;
        }
        auto resp = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(QueryServiceConfig.GetScriptResultSizeLimit()
                                 ? QueryServiceConfig.GetScriptResultSizeLimit() > std::numeric_limits<i64>::max() 
                                    ? std::numeric_limits<i64>::max()
                                    : static_cast<i64>(QueryServiceConfig.GetScriptResultSizeLimit())
                                 : std::numeric_limits<i64>::max());

        LOG_D("Compute stream data"
            << ", seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", queryResultIndex: " << ev->Get()->Record.GetQueryResultIndex()
            << ", from: " << ev->Sender);

        auto resultSetIndex = ev->Get()->Record.GetQueryResultIndex();

        if (resultSetIndex >= ResultSetInfos.size()) {
            // we don't know result set count, so just accept all of them
            // it's possible to have several result sets per script
            // they can arrive in any order and may be missed for some indices
            ResultSetInfos.resize(resultSetIndex + 1);
        }

        auto& resultSetInfo = ResultSetInfos[resultSetIndex];
        if (IsExecuting() && !resultSetInfo.Truncated) {
            auto& rowCount = resultSetInfo.RowCount;
            auto& byteCount = resultSetInfo.ByteCount;

            for (auto& row : *ev->Get()->Record.MutableResultSet()->mutable_rows()) {
                if (QueryServiceConfig.GetScriptResultRowsLimit() && rowCount + 1 > QueryServiceConfig.GetScriptResultRowsLimit()) {
                    resultSetInfo.Truncated = true;
                    break;
                }

                auto serializedSize = row.ByteSizeLong();
                if (QueryServiceConfig.GetScriptResultSizeLimit() && byteCount + serializedSize > QueryServiceConfig.GetScriptResultSizeLimit()) {
                    resultSetInfo.Truncated = true;
                    break;
                }

                rowCount++;
                byteCount += serializedSize;
                PendingResultSetsSize += serializedSize;
                *resultSetInfo.PendingResult.add_rows() = std::move(row);
            }

            bool newResultSet = resultSetInfo.Meta == nullptr;
            if (newResultSet || resultSetInfo.Truncated) {
                Ydb::Query::Internal::ResultSetMeta meta;
                if (newResultSet) {
                    *meta.mutable_columns() = ev->Get()->Record.GetResultSet().columns();
                }
                if (resultSetInfo.Truncated) {
                    meta.set_truncated(true);
                }

                NJson::TJsonValue* value;
                if (newResultSet) {
                    value = &ResultSetMetas[resultSetIndex];
                    resultSetInfo.Meta = value;
                } else {
                    value = resultSetInfo.Meta;
                }
                NProtobufJson::Proto2Json(meta, *value, NProtobufJson::TProto2JsonConfig());

                // can't save meta when previous request is not completed for TLI reasons
                if (SaveResultMetaInflight) {
                    PendingResultMeta = true;
                } else {
                    SaveResultMeta();
                    SaveResultMetaInflight++;
                }
            }

            if (ShouldSaveResult(resultSetInfo)) {
                SaveResult(resultSetIndex);
            }
        }

        PendingAcks.push({.ReplyActorId = ev->Sender, .AckEvent = std::move(resp)});
        SendStreamDataResponse();
    }

    void SaveResultMeta() {
        NJsonWriter::TBuf sout;
        sout.WriteJsonValue(&ResultSetMetas);
        Register(
            CreateSaveScriptExecutionResultMetaActor(SelfId(), Database, ExecutionId, sout.Str())
        );
    }

    void Handle(TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        Register(
            CreateScriptProgressActor(ExecutionId, Database, ev->Get()->Record.GetQueryPlan(), "")
        );
    }

    void Handle(TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (RunState != ERunState::Running) {
            return;
        }
        auto& record = ev->Get()->Record.GetRef();

        const auto& issueMessage = record.GetResponse().GetQueryIssues();
        NYql::IssuesFromMessage(issueMessage, Issues);

        if (record.GetResponse().HasQueryPlan()) {
            QueryPlan = record.GetResponse().GetQueryPlan();
        }

        if (record.GetResponse().HasQueryStats()) {
            QueryStats = record.GetResponse().GetQueryStats();
        }

        if (record.GetResponse().HasQueryAst()) {
            QueryAst = record.GetResponse().GetQueryAst();
        }

        Finish(record.GetYdbStatus());
    }

    void Handle(TEvKqp::TEvCancelScriptExecutionRequest::TPtr& ev) {
        switch (RunState) {
        case ERunState::Created:
            CancelRequests.emplace_front(std::move(ev));
            Finish(Ydb::StatusIds::CANCELLED, ERunState::Cancelling);
            break;
        case ERunState::Running:
            CancelRequests.emplace_front(std::move(ev));
            CancelRunningQuery();
            break;
        case ERunState::Cancelling:
            CancelRequests.emplace_front(std::move(ev));
            break;
        case ERunState::Cancelled:
            Send(ev->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(Ydb::StatusIds::PRECONDITION_FAILED, "Already cancelled"));
            break;
        case ERunState::Finishing:
            CancelRequests.emplace_front(std::move(ev));
            break;
        case ERunState::Finished:
            Send(ev->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(Ydb::StatusIds::PRECONDITION_FAILED, "Already finished"));
            break;
        }
    }

    void CancelRunningQuery() {
        if (SessionId.empty()) {
            Finish(Ydb::StatusIds::CANCELLED, ERunState::Cancelling);
        } else {
            RunState = ERunState::Cancelling;
            auto ev = MakeHolder<TEvKqp::TEvCancelQueryRequest>();
            ev->Record.MutableRequest()->SetSessionId(SessionId);

            Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        }
    }

    void Handle(TEvKqp::TEvCancelQueryResponse::TPtr&) {
        Finish(Ydb::StatusIds::CANCELLED, ERunState::Cancelling);
    }

    void Handle(TEvScriptExecutionFinished::TPtr& ev) {
        WaitFinalizationRequest = false;

        if (RunState == ERunState::Cancelling) {
            RunState = ERunState::Cancelled;
        }

        if (RunState == ERunState::Finishing) {
            RunState = ERunState::Finished;
        }

        Ydb::StatusIds::StatusCode status = ev->Get()->Status;
        NYql::TIssues issues = std::move(ev->Get()->Issues);
        if (RunState == ERunState::Finished) {
            status = Ydb::StatusIds::PRECONDITION_FAILED;
            issues.Clear();
            issues.AddIssue("Already finished");
        }
        TerminateActorExecution(status, issues);
    }

    void Handle(TEvSaveScriptResultMetaFinished::TPtr& ev) {
        if (PendingResultMeta) {
            PendingResultMeta = false;
            SaveResultMeta();
            return;
        }

        SaveResultMetaInflight--;
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS && (Status == Ydb::StatusIds::SUCCESS || Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)) {
            Status = ev->Get()->Status;
            Issues.AddIssues(ev->Get()->Issues);
        }
        CheckInflight();
    }

    void Handle(TEvSaveScriptResultFinished::TPtr& ev) {
        SaveResultInflight--;
        if (Status == Ydb::StatusIds::SUCCESS || Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
            if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
                Status = ev->Get()->Status;
                Issues.AddIssues(ev->Get()->Issues);
            } else {
                for (size_t resultSetId = 0; resultSetId < ResultSetInfos.size(); ++resultSetId) {
                    if (ShouldSaveResult(ResultSetInfos[resultSetId])) {
                        SaveResult(resultSetId);
                        break;
                    }
                }
            }
        }
        SendStreamDataResponse();
        CheckInflight();
    }

    static Ydb::Query::ExecStatus GetExecStatusFromStatusCode(Ydb::StatusIds::StatusCode status) {
        if (status == Ydb::StatusIds::SUCCESS) {
            return Ydb::Query::EXEC_STATUS_COMPLETED;
        } else if (status == Ydb::StatusIds::CANCELLED) {
            return Ydb::Query::EXEC_STATUS_CANCELLED;
        } else {
            return Ydb::Query::EXEC_STATUS_FAILED;
        }
    }

    EFinalizationStatus GetFinalizationStatusFromRunState() const {
        if ((Status == Ydb::StatusIds::SUCCESS || Status == Ydb::StatusIds::CANCELLED) && !IsExecuting()) {
            return EFinalizationStatus::FS_COMMIT;
        }
        return EFinalizationStatus::FS_ROLLBACK;
    }

    void CheckInflight() {
        if (Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED || (Status == Ydb::StatusIds::SUCCESS && RunState == ERunState::Finishing && (SaveResultMetaInflight || SaveResultInflight))) {
            // waiting for script completion
            return;
        }

        if (PendingResultSetsSize) {
            // Complete results saving
            SaveResult();
            return;
        }

        if (!LeaseUpdateQueryRunning) {
            RunScriptExecutionFinisher();
        } else {
            FinishAfterLeaseUpdate = true;
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status, ERunState runState = ERunState::Finishing) {
        RunState = runState;
        Status = status;

        // if query has no results, save empty json array
        if (ResultSetInfos.empty()) {
            ResultSetMetas.SetType(NJson::JSON_ARRAY);
            SaveResultMeta();
            SaveResultMetaInflight++;
        } else {
            CheckInflight();
        }
    }

    void PassAway() override {
        CloseSession();
        NActors::TActorBootstrapped<TRunScriptActor>::PassAway();
    }

    bool IsExecuting() const {
        return RunState != ERunState::Finished
            && RunState != ERunState::Finishing
            && RunState != ERunState::Cancelled
            && RunState != ERunState::Cancelling;
    }

    static bool ShouldSaveResult(TResultSetInfo& resultInfo) {
        if (!resultInfo.PendingResult.rows_size()) {
            return false;
        }
        return resultInfo.Truncated || resultInfo.PendingResult.rows_size() >= MIN_SAVE_RESULT_BATCH_ROWS || resultInfo.ByteCount - resultInfo.AccumulatedSize >= MIN_SAVE_RESULT_BATCH_SIZE;
    }

private:
    const TString ExecutionId;
    NKikimrKqp::TEvQueryRequest Request;
    const TString Database;
    const ui64 LeaseGeneration;
    const TDuration LeaseDuration;
    const TDuration ResultsTtl;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    TIntrusivePtr<TKqpCounters> Counters;
    TString SessionId;
    TInstant LeaseUpdateScheduleTime;
    bool LeaseUpdateQueryRunning = false;
    bool FinalStatusIsSaved = false;
    bool FinishAfterLeaseUpdate = false;
    bool WaitFinalizationRequest = false;
    ERunState RunState = ERunState::Created;
    std::forward_list<TEvKqp::TEvCancelScriptExecutionRequest::TPtr> CancelRequests;

    // Result
    NYql::TIssues Issues;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;

    // Result
    std::vector<TResultSetInfo> ResultSetInfos;
    std::queue<TPendingAck> PendingAcks;
    TMaybe<TInstant> ExpireAt;
    NJson::TJsonValue ResultSetMetas;
    ui32 SaveResultInflight = 0;
    ui32 SaveResultMetaInflight = 0;
    bool PendingResultMeta = false;
    ui64 PendingResultSetsSize = 0;
    std::optional<TString> QueryPlan;
    std::optional<TString> QueryAst;
    std::optional<NKqpProto::TKqpStatsQuery> QueryStats;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
};

} // namespace

NActors::IActor* CreateRunScriptActor(const TString& executionId, const NKikimrKqp::TEvQueryRequest& request, const TString& database, ui64 leaseGeneration, TDuration leaseDuration, TDuration resultsTtl, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TRunScriptActor(executionId, request, database, leaseGeneration, leaseDuration, resultsTtl, std::move(queryServiceConfig), counters);
}

} // namespace NKikimr::NKqp
