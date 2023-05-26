#include "kqp_run_script_actor.h"

#include <ydb/core/base/kikimr_issue.h>
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
        hFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
        hFunc(NKqp::TEvKqp::TEvFetchScriptResultsRequest, Handle);
    )

    void Start() {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record = Request;

        NActors::ActorIdToProto(SelfId(), ev->Record.MutableRequestActorId());

        if (!Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release())) {
            Issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error"));
            Finish(Ydb::StatusIds::INTERNAL_ERROR);
        }
    }

    // TODO: remove this after there will be a normal way to store results and generate execution id
    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        Start();
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
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

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        auto& record = ev->Get()->Record.GetRef();

        const auto& issueMessage = record.GetResponse().GetQueryIssues();
        NYql::IssuesFromMessage(issueMessage, Issues);

        Finish(record.GetYdbStatus());
    }

    void Handle(NKqp::TEvKqp::TEvFetchScriptResultsRequest::TPtr& ev) {
        auto resp = MakeHolder<NKqp::TEvKqp::TEvFetchScriptResultsResponse>();
        if (!IsFinished()) {
            resp->Record.SetStatus(Ydb::StatusIds::BAD_REQUEST);
            resp->Record.AddIssues()->set_message("Results are not ready");
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

    void MergeResultSet(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
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
        } else {
            return Ydb::Query::EXEC_STATUS_FAILED;
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status) {
        Status = status;
        Register(CreateScriptExecutionFinisher(SelfId().ToString() /*executionId*/, Database, LeaseGeneration, status, GetExecStatusFromStatusCode(status), Issues));
    }

    bool IsFinished() const {
        return Status != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    }

    bool IsTruncated() const {
        return !ResultSets.empty() && ResultSets[0].truncated();
    }

private:
    const NKikimrKqp::TEvQueryRequest Request;
    const TString Database;
    const ui64 LeaseGeneration;

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
