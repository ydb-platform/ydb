#include "rpc_calls.h"
#include "grpc_request_proxy.h"

#include <ydb/core/base/path.h>

namespace NKikimr {
namespace NGRpcService {

template <>
void FillYdbStatus(Ydb::PersQueue::V1::StreamingWriteServerMessage& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    resp.set_status(status);
    NYql::IssuesToMessage(issues, resp.mutable_issues());
}

template <>
void FillYdbStatus(Ydb::PersQueue::V1::StreamingReadServerMessage& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    resp.set_status(status);
    NYql::IssuesToMessage(issues, resp.mutable_issues());
}

template <>
void FillYdbStatus(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    resp.set_status(status);
    NYql::IssuesToMessage(issues, resp.mutable_issues());
}

template <>
void FillYdbStatus(Ydb::Topic::StreamWriteMessage::FromServer& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    resp.set_status(status);
    NYql::IssuesToMessage(issues, resp.mutable_issues());
}

template <>
void FillYdbStatus(Ydb::Topic::StreamReadMessage::FromServer& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    resp.set_status(status);
    NYql::IssuesToMessage(issues, resp.mutable_issues());
}

template <>
void FillYdbStatus(Ydb::Topic::StreamDirectReadMessage::FromServer& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    resp.set_status(status);
    NYql::IssuesToMessage(issues, resp.mutable_issues());
}

template <>
void FillYdbStatus(Draft::Dummy::PingResponse& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    Y_UNUSED(resp);
    Y_UNUSED(issues);
    Y_UNUSED(status);
}

template <>
void FillYdbStatus(Ydb::Coordination::SessionResponse& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    auto* failure = resp.mutable_failure();
    failure->set_status(status);
    NYql::IssuesToMessage(issues, failure->mutable_issues());
}

std::pair<TString, TString> SplitPath(const TMaybe<TString>& database, const TString& path) {
    std::pair<TString, TString> pathPair;
    TString error;

    if (!TrySplitPathByDb(path, database.GetOrElse(TString()), pathPair, error)) {
        ythrow yexception() << error;
    }

    return pathPair;
}

std::pair<TString, TString> SplitPath(const TString& path) {
    auto splitPos = path.find_last_of('/');
    if (splitPos == path.npos || splitPos + 1 == path.size()) {
        ythrow yexception() << "wrong path format '" << path << "'" ;
    }
    return {path.substr(0, splitPos), path.substr(splitPos + 1)};
}

void RefreshTokenSendRequest(const TActorContext& ctx, IEventBase* refreshTokenRequest) {
    ctx.Send(CreateGRpcRequestProxyId(), refreshTokenRequest);
}

void RefreshTokenReplyUnauthenticated(TActorId recipient, TActorId sender, NYql::TIssues&& issues) {
    TActivationContext::Send(new IEventHandle(recipient, sender,
        new TGRpcRequestProxy::TEvRefreshTokenResponse{false, nullptr, false, std::move(issues)}
    ));
}

void RefreshTokenReplyUnavailable(TActorId recipient, NYql::TIssues&& issues) {
    const TActorContext& ctx = TActivationContext::AsActorContext();
    ctx.Send(recipient,
        new TGRpcRequestProxy::TEvRefreshTokenResponse{false, nullptr, true, std::move(issues)}
    );
}

} // namespace NGRpcService
} // namespace NKikimr
