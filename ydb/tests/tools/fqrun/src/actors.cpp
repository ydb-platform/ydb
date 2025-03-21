#include "actors.h"
#include "common.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

using namespace NKikimrRun;

namespace NFqRun {

namespace {

class TRunQueryActor : public NActors::TActorBootstrapped<TRunQueryActor> {
public:
    TRunQueryActor(TQueryRequest request, NThreading::TPromise<TQueryResponse> promise)
        : Scope(request.Event->Scope)
        , UserSid(request.Event->User)
        , Token(request.Event->Token)
        , PingPeriod(request.PingPeriod)
        , Request(std::move(request.Event))
        , Promise(promise)
    {}

    void Bootstrap() {
        Send(NFq::ControlPlaneProxyActorId(), std::move(Request));

        Become(&TRunQueryActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NFq::TEvControlPlaneProxy::TEvCreateQueryResponse, Handle);
        hFunc(NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse, Handle);
        sFunc(NActors::TEvents::TEvWakeup, DescribeQuery);
    )

    void Handle(NFq::TEvControlPlaneProxy::TEvCreateQueryResponse::TPtr& ev) {
        if (ev->Get()->Issues) {
            Fail(FederatedQuery::QueryMeta::FAILED, {GroupIssues(NYql::TIssue("Failed to create request"), ev->Get()->Issues)});
            return;
        }

        QueryId = ev->Get()->Result.query_id();
        DescribeQuery();
    }

    void Handle(NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse::TPtr& ev) {
        if (ev->Get()->Issues) {
            Fail(FederatedQuery::QueryMeta::FAILED, {GroupIssues(NYql::TIssue("Failed to describe request"), ev->Get()->Issues)});
            return;
        }

        const auto& result = ev->Get()->Result.query();
        const auto status = result.meta().status();
        if (!IsFinalStatus(status)) {
            Schedule(PingPeriod, new NActors::TEvents::TEvWakeup());
            return;
        }

        TQueryResponse response;
        response.Status = status;
        NYql::IssuesFromMessage(result.issue(), response.Issues);
        NYql::IssuesFromMessage(result.transient_issue(), response.TransientIssues);
        Promise.SetValue(response);
        PassAway();
    }

    void DescribeQuery() const {
        FederatedQuery::DescribeQueryRequest request;
        request.set_query_id(QueryId);

        Send(NFq::ControlPlaneProxyActorId(), new NFq::TEvControlPlaneProxy::TEvDescribeQueryRequest(Scope, request, UserSid, Token, TVector<TString>{}));
    }

private:
    void Fail(FederatedQuery::QueryMeta::ComputeStatus status, NYql::TIssues issues) {
        Promise.SetValue({
            .Status = status,
            .Issues = std::move(issues)
        });
        PassAway();
    }

private:
    const TString Scope;
    const TString UserSid;
    const TString Token;
    const TDuration PingPeriod;

    std::unique_ptr<NFq::TEvControlPlaneProxy::TEvCreateQueryRequest> Request;
    NThreading::TPromise<TQueryResponse> Promise;
    TString QueryId;
};

class TAsyncQueryRunnerActor : public TAsyncQueryRunnerActorBase<TQueryRequest, TQueryResponse> {
    using TBase = TAsyncQueryRunnerActorBase<TQueryRequest, TQueryResponse>;

public:
    TAsyncQueryRunnerActor(const TAsyncQueriesSettings& settings)
        : TBase(settings)
    {}

protected:
    void RunQuery(TQueryRequest&& request, NThreading::TPromise<TQueryResponse> promise) override {
        Register(new TRunQueryActor(std::move(request), promise));
    }
};

}  // anonymous namespace

bool TQueryResponse::IsSuccess() const {
    return Status == FederatedQuery::QueryMeta::COMPLETED;
}

TString TQueryResponse::GetStatus() const {
    return FederatedQuery::QueryMeta::ComputeStatus_Name(Status);
}

TString TQueryResponse::GetError() const {
    NYql::TIssues issues = Issues;
    if (TransientIssues) {
        issues.AddIssue(GroupIssues(NYql::TIssue("Transient issues"), TransientIssues));
    }
    return issues.ToString();
}

NActors::IActor* CreateAsyncQueryRunnerActor(const TAsyncQueriesSettings& settings) {
    return new TAsyncQueryRunnerActor(settings);
}

}  // namespace NFqRun
