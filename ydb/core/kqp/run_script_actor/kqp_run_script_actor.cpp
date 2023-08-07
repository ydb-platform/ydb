#include "kqp_run_script_actor.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/size_literals.h>

#include <forward_list>

#define LOG_T(stream) LOG_TRACE_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, SelfId() << " " << stream);
#define LOG_D(stream) LOG_DEBUG_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, SelfId() << " " << stream);
#define LOG_I(stream) LOG_INFO_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, SelfId() << " " << stream);
#define LOG_W(stream) LOG_WARN_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, SelfId() << " " << stream);
#define LOG_E(stream) LOG_ERROR_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, SelfId() << " " << stream);

namespace NKikimr::NKqp {

namespace {

constexpr ui64 RESULT_SIZE_LIMIT = 10_MB;
constexpr int RESULT_ROWS_LIMIT = 1000;
constexpr ui32 LEASE_UPDATE_FREQUENCY = 2;

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

public:
    TRunScriptActor(const TString& executionId, const NKikimrKqp::TEvQueryRequest& request, const TString& database, ui64 leaseGeneration, TDuration leaseDuration)
        : ExecutionId(executionId)
        , Request(request)
        , Database(database)
        , LeaseGeneration(leaseGeneration)
        , LeaseDuration(leaseDuration)
    {}

    static constexpr char ActorName[] = "KQP_RUN_SCRIPT_ACTOR";

    void Bootstrap() {
        Become(&TRunScriptActor::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(NActors::TEvents::TEvPoison, Handle);
        hFunc(TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(TEvKqp::TEvQueryResponse, Handle);
        hFunc(TEvKqp::TEvCreateSessionResponse, Handle);
        IgnoreFunc(TEvKqp::TEvCloseSessionResponse);
        hFunc(TEvKqp::TEvCancelQueryResponse, Handle);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, Handle);
        hFunc(TEvScriptExecutionFinished, Handle);
        hFunc(TEvScriptLeaseUpdateResponse, Handle);
        hFunc(TEvSaveScriptResultMetaFinished, Handle);
        hFunc(TEvSaveScriptResultFinished, Handle);
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

        NActors::ActorIdToProto(SelfId(), ev->Record.MutableRequestActorId());

        SendToKqpProxy(std::move(ev));
    }

    // TODO: remove this after there will be a normal way to store results and generate execution id
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
        case EWakeUp::RunEvent:
            Schedule(LeaseDuration / LEASE_UPDATE_FREQUENCY, new NActors::TEvents::TEvWakeup(EWakeUp::UpdateLeaseEvent));
            RunState = ERunState::Running;
            CreateSession();
            break;

        case EWakeUp::UpdateLeaseEvent:
            if (RunState == ERunState::Cancelled || RunState == ERunState::Cancelling || RunState == ERunState::Finished || RunState == ERunState::Finishing) {
                break;
            }

            if (!LeaseUpdateQueryRunning && !FinalStatusIsSaved) {
                Register(CreateScriptLeaseUpdateActor(SelfId(), Database, ExecutionId, LeaseDuration));
                LeaseUpdateQueryRunning = true;
            }
            Schedule(LeaseDuration / LEASE_UPDATE_FREQUENCY, new NActors::TEvents::TEvWakeup(EWakeUp::UpdateLeaseEvent));
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
            Register(CreateScriptExecutionFinisher(ExecutionId, Database, LeaseGeneration, Status, GetExecStatusFromStatusCode(Status),
                                                     Issues, std::move(QueryStats), std::move(QueryPlan), std::move(QueryAst)));
            return;
        }
        
        if (RunState != ERunState::Cancelled && RunState != ERunState::Finished) {
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
    }

    // Event in case of error in registering script in database
    // Just pass away, because we have not started execution.
    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        Y_VERIFY(RunState == ERunState::Created);
        PassAway();
    }

    void Handle(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        if (RunState != ERunState::Running) {
            return;
        }
        auto resp = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(RESULT_SIZE_LIMIT);

        LOG_D("Send stream data ack"
            << ", seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", queryResultIndex: " << ev->Get()->Record.GetQueryResultIndex()
            << ", to: " << ev->Sender);

        Send(ev->Sender, resp.Release());

        auto resultSetIndex = ev->Get()->Record.GetQueryResultIndex();

        if (resultSetIndex >= ExpireAt.size()) {
            // we don't know result set count, so just accept all of them
            // it's possible to have several result sets per script
            // they can arrive in any order and may be missed for some indices 
            ResultSetRowCount.resize(resultSetIndex + 1);
            ResultSetByteCount.resize(resultSetIndex + 1);
            Truncated.resize(resultSetIndex + 1);
            ExpireAt.resize(resultSetIndex + 1);
        }

        if (ExpireAt[resultSetIndex] == TInstant::Zero()) {
            ExpireAt[resultSetIndex] = TInstant::Now() + TDuration::Days(1);
        }

        if (IsExecuting() && !Truncated[resultSetIndex]) {
            auto& rowCount = ResultSetRowCount[resultSetIndex];
            auto& byteCount = ResultSetByteCount[resultSetIndex];
            auto firstRow = rowCount;
            std::vector<TString> serializedRows;

            for (const auto& row : ev->Get()->Record.GetResultSet().rows()) {
                if (rowCount > RESULT_ROWS_LIMIT) {
                    Truncated[resultSetIndex] = true;
                    break;
                }

                auto serializedSize = row.ByteSizeLong();
                if (byteCount + serializedSize > RESULT_SIZE_LIMIT) {
                    Truncated[resultSetIndex] = true;
                    break;
                }

                rowCount++;
                byteCount += serializedSize;
                serializedRows.push_back(row.SerializeAsString());
            }

            if (firstRow == 0 || Truncated[resultSetIndex]) {
                Ydb::Query::ResultSetMeta meta;
                *meta.mutable_columns() = ev->Get()->Record.GetResultSet().columns();
                meta.set_truncated(Truncated[resultSetIndex]);

                NJson::TJsonValue* value;
                if (firstRow == 0) {
                    value = &ResultSetMetas[resultSetIndex];
                    ResultSetMetaArray.push_back(value);
                } else {
                    value = ResultSetMetaArray[resultSetIndex];
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

            if (!serializedRows.empty()) {
                Register(
                    CreateSaveScriptExecutionResultActor(SelfId(), Database, ExecutionId, resultSetIndex, ExpireAt[resultSetIndex], firstRow, std::move(serializedRows))
                );
                SaveResultInflight++;
            }
        }
    }

    void SaveResultMeta() {
        NJsonWriter::TBuf sout;
        sout.WriteJsonValue(&ResultSetMetas);
        Register(
            CreateSaveScriptExecutionResultMetaActor(SelfId(), Database, ExecutionId, sout.Str())
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
        if (Status == Ydb::StatusIds::SUCCESS && ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Status = ev->Get()->Status;
            Issues.AddIssues(ev->Get()->Issues);
        }
        CheckInflight();
    }

    void Handle(TEvSaveScriptResultFinished::TPtr& ev) {
        SaveResultInflight--;
        if (Status == Ydb::StatusIds::SUCCESS && ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Status = ev->Get()->Status;
            Issues.AddIssues(ev->Get()->Issues);
        }
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


    void CheckInflight() {
        if (Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED || (Status == Ydb::StatusIds::SUCCESS && RunState == ERunState::Finishing && (SaveResultMetaInflight || SaveResultInflight))) {
            // waiting for script completion
            return;
        }

        if (!LeaseUpdateQueryRunning) {
            RunScriptExecutionFinisher();
        } else {
            FinishAfterLeaseUpdate = true;
        }

        if (RunState == ERunState::Cancelling) {
            Issues.AddIssue("Script execution is cancelled");
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status, ERunState runState = ERunState::Finishing) {
        RunState = runState;
        Status = status;

        // if query has no results, save empty json array
        if (ResultSetMetaArray.empty()) {
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

private:
    const TString ExecutionId;
    const NKikimrKqp::TEvQueryRequest Request;
    const TString Database;
    const ui64 LeaseGeneration;
    const TDuration LeaseDuration;
    TString SessionId;
    bool LeaseUpdateQueryRunning = false;
    bool FinalStatusIsSaved = false;
    bool FinishAfterLeaseUpdate = false;
    ERunState RunState = ERunState::Created;
    std::forward_list<TEvKqp::TEvCancelScriptExecutionRequest::TPtr> CancelRequests;

    // Result
    NYql::TIssues Issues;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;

    // Result
    std::vector<ui64> ResultSetRowCount;
    std::vector<ui64> ResultSetByteCount;
    std::vector<bool> Truncated;
    std::vector<TInstant> ExpireAt;
    std::vector<NJson::TJsonValue*> ResultSetMetaArray;
    NJson::TJsonValue ResultSetMetas;
    ui32 SaveResultInflight = 0;
    ui32 SaveResultMetaInflight = 0;
    bool PendingResultMeta = false;
    TMaybe<TString> QueryPlan;
    TMaybe<TString> QueryAst;
    TMaybe<NKqpProto::TKqpStatsQuery> QueryStats;
};

} // namespace

NActors::IActor* CreateRunScriptActor(const TString& executionId, const NKikimrKqp::TEvQueryRequest& request, const TString& database, ui64 leaseGeneration, TDuration leaseDuration) {
    return new TRunScriptActor(executionId, request, database, leaseGeneration, leaseDuration);
}

} // namespace NKikimr::NKqp
