#include "kqp_run_script_actor.h"

#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/protos/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

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

class TRunScriptActor : public NActors::TActorBootstrapped<TRunScriptActor> {
    enum class ERunState {
        Created,
        Running,
        Cancelling,
        Cancelled,
        Finished,
    };
public:
    TRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, const TString& database, ui64 leaseGeneration)
        : Request(request)
        , Database(database)
        , LeaseGeneration(leaseGeneration)
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
        hFunc(TEvKqp::TEvFetchScriptResultsRequest, Handle);
        hFunc(TEvKqp::TEvCreateSessionResponse, Handle);
        IgnoreFunc(TEvKqp::TEvCloseSessionResponse);
        hFunc(TEvKqp::TEvCancelQueryResponse, Handle);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, Handle);
        hFunc(TEvScriptExecutionFinished, Handle);
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
    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        if (RunState == ERunState::Created) {
            RunState = ERunState::Running;
            CreateSession();
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
            << ", to: " << ev->Sender);

        Send(ev->Sender, resp.Release());

        if (!IsFinished() && !IsTruncated()) {
            MergeResultSet(ev);
        }
    }

    void Handle(TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (RunState != ERunState::Running) {
            return;
        }
        auto& record = ev->Get()->Record.GetRef();

        const auto& issueMessage = record.GetResponse().GetQueryIssues();
        NYql::IssuesFromMessage(issueMessage, Issues);

        Finish(record.GetYdbStatus());
    }

    void Handle(TEvKqp::TEvFetchScriptResultsRequest::TPtr& ev) {
        auto resp = MakeHolder<TEvKqp::TEvFetchScriptResultsResponse>();
        if (!IsFinished()) {
            if (RunState == ERunState::Created || RunState == ERunState::Running) {
                resp->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
                resp->Record.AddIssues()->set_message("Results are not ready");
            } else if (RunState == ERunState::Cancelled || RunState == ERunState::Cancelling) {
                resp->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
                resp->Record.AddIssues()->set_message("Script execution is cancelled");
            }
        } else {
            if (!ResultSets.empty()) {
                resp->Record.SetResultSetIndex(0);
                resp->Record.MutableResultSet()->mutable_columns()->CopyFrom(ResultSets[0].columns());

                const ui64 rowsOffset = ev->Get()->Record.GetRowsOffset();
                const ui64 rowsLimit = ev->Get()->Record.GetRowsLimit();
                ui64 rowsAdded = 0;
                for (i64 row = static_cast<i64>(rowsOffset); row < ResultSets[0].rows_size(); ++row) {
                    if (rowsAdded >= rowsLimit) {
                        resp->Record.MutableResultSet()->set_truncated(true);
                        break;
                    }
                    resp->Record.MutableResultSet()->add_rows()->CopyFrom(ResultSets[0].rows(row));
                }
            }
            resp->Record.SetStatus(Status);
            for (const auto& issue : Issues) {
                auto item = resp->Record.add_issues();
                NYql::IssueToMessage(issue, item);
            }
        }
        Send(ev->Sender, std::move(resp));
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
            for (auto& req : CancelRequests) {
                Send(req->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(ev->Get()->Status, ev->Get()->Issues));
            }
            CancelRequests.clear();
        }
    }

    void MergeResultSet(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        if (ResultSets.empty()) {
            ResultSets.emplace_back(ev->Get()->Record.GetResultSet());
            return;
        }
        if (ResultSets[0].columns_size() != ev->Get()->Record.GetResultSet().columns_size()) {
            Issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error"));
            Finish(Ydb::StatusIds::INTERNAL_ERROR);
            return;
        }
        size_t rowsAdded = 0;
        for (auto& row : *ev->Get()->Record.MutableResultSet()->mutable_rows()) {
            ResultSets[0].add_rows()->Swap(&row);
            ++rowsAdded;
            if (ResultSets[0].rows_size() >= RESULT_ROWS_LIMIT) {
                break;
            }
        }
        if (ev->Get()->Record.GetResultSet().truncated() || ResultSets[0].rows_size() >= RESULT_ROWS_LIMIT || ResultSets[0].ByteSizeLong() >= RESULT_SIZE_LIMIT) {
            ResultSets[0].set_truncated(true);
        }
        LOG_D("Received partial result. Rows added: " << rowsAdded << ". Truncated: " << IsTruncated());
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

    void Finish(Ydb::StatusIds::StatusCode status, ERunState runState = ERunState::Finished) {
        RunState = runState;
        Status = status;
        Register(CreateScriptExecutionFinisher(ActorIdToScriptExecutionId(SelfId()), Database, LeaseGeneration, status, GetExecStatusFromStatusCode(status), Issues));
        if (RunState == ERunState::Cancelling) {
            Issues.AddIssue("Script execution is cancelled");
            ResultSets.clear();
        }
        CloseSession();
    }

    void PassAway() override {
        CloseSession();
        NActors::TActorBootstrapped<TRunScriptActor>::PassAway();
    }

    bool IsFinished() const {
        return RunState == ERunState::Finished;
    }

    bool IsTruncated() const {
        return !ResultSets.empty() && ResultSets[0].truncated();
    }

private:
    const NKikimrKqp::TEvQueryRequest Request;
    const TString Database;
    const ui64 LeaseGeneration;
    TString SessionId;
    ERunState RunState = ERunState::Created;
    std::forward_list<TEvKqp::TEvCancelScriptExecutionRequest::TPtr> CancelRequests;

    // Result
    NYql::TIssues Issues;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    std::vector<Ydb::ResultSet> ResultSets;
};

} // namespace

NActors::IActor* CreateRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, const TString& database, ui64 leaseGeneration) {
    return new TRunScriptActor(request, database, leaseGeneration);
}

} // namespace NKikimr::NKqp
