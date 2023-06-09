#include "service_query.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/public/api/protos/draft/ydb_query.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>

namespace NKikimr::NGRpcService {

namespace {

using namespace NActors;

using TEvFetchScriptResultsRequest = TGrpcRequestNoOperationCall<Ydb::Query::FetchScriptResultsRequest,
    Ydb::Query::FetchScriptResultsResponse>;

constexpr i64 MAX_ROWS_LIMIT = 1000;

class TFetchScriptResultsRPC : public TActorBootstrapped<TFetchScriptResultsRPC> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TFetchScriptResultsRPC(TEvFetchScriptResultsRequest* request)
        : Request_(request)
    {}

    void Bootstrap() {
        const auto* req = Request_->GetProtoRequest();
        if (!req) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, "Internal error");
            return;
        }

        const TString& executionId = req->execution_id();
        NActors::TActorId runScriptActor;
        if (!NKqp::ScriptExecutionIdToActorId(executionId, runScriptActor)) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Incorrect execution id");
            return;
        }

        if (req->rows_limit() <= 0) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Invalid rows limit");
            return;
        }

        if (req->rows_offset() < 0) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Invalid rows offset");
            return;
        }

        if (req->rows_limit() > MAX_ROWS_LIMIT) {
            Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Rows limit is too large. Values <= " << MAX_ROWS_LIMIT << " are allowed");
            return;
        }

        auto ev = MakeHolder<NKqp::TEvKqp::TEvFetchScriptResultsRequest>();
        ev->Record.SetRowsOffset(req->rows_offset());
        ev->Record.SetRowsLimit(req->rows_limit());

        ui64 flags = IEventHandle::FlagTrackDelivery;
        if (runScriptActor.NodeId() != SelfId().NodeId()) {
            flags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = runScriptActor.NodeId();
        }
        Send(runScriptActor, std::move(ev), flags);

        Become(&TFetchScriptResultsRPC::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NKqp::TEvKqp::TEvFetchScriptResultsResponse, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
    )

    void Handle(NKqp::TEvKqp::TEvFetchScriptResultsResponse::TPtr& ev) {
        Ydb::Query::FetchScriptResultsResponse resp;
        resp.set_status(ev->Get()->Record.GetStatus());
        resp.mutable_issues()->Swap(ev->Get()->Record.MutableIssues());
        resp.set_result_set_index(static_cast<i64>(ev->Get()->Record.GetResultSetIndex()));
        if (ev->Get()->Record.HasResultSet()) {
            resp.mutable_result_set()->Swap(ev->Get()->Record.MutableResultSet());
        }
        Reply(resp.status(), std::move(resp));
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown) {
            Reply(Ydb::StatusIds::NOT_FOUND, "No such execution");
        } else {
            Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver fetch request to destination");
        }
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        Reply(Ydb::StatusIds::UNAVAILABLE, "Failed to deliver fetch request to destination");
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }
        TActorBootstrapped<TFetchScriptResultsRPC>::PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, Ydb::Query::FetchScriptResultsResponse&& result, const NYql::TIssues& issues = {}) {
        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::RPC_REQUEST, "Fetch script results, status: "
            << Ydb::StatusIds::StatusCode_Name(status) << (issues ? ". Issues: " : "") << issues.ToOneLineString());

        for (const auto& issue : issues) {
            auto item = result.add_issues();
            NYql::IssueToMessage(issue, item);
        }

        result.set_status(status);

        TString serializedResult;
        Y_PROTOBUF_SUPPRESS_NODISCARD result.SerializeToString(&serializedResult);

        Request_->SendSerializedResult(std::move(serializedResult), status);

        PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        Ydb::Query::FetchScriptResultsResponse result;
        Reply(status, std::move(result), issues);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& errorText) {
        NYql::TIssues issues;
        issues.AddIssue(errorText);
        Reply(status, issues);
    }

private:
    std::unique_ptr<TEvFetchScriptResultsRequest> Request_;
    TMaybe<ui32> SubscribedOnSession;
};

} // namespace

void DoFetchScriptResults(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    Y_UNUSED(f);
    auto* req = dynamic_cast<TEvFetchScriptResultsRequest*>(p.release());
    Y_VERIFY(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    TActivationContext::AsActorContext().Register(new TFetchScriptResultsRPC(req));
}

} // namespace NKikimr::NGRpcService
