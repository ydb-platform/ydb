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

void RefreshToken(const TString& token, const TString& database, const TActorContext& ctx, TActorId from) {
    ctx.Send(CreateGRpcRequestProxyId(), new TRefreshTokenImpl(token, database, from));
}

void TRefreshTokenImpl::ReplyUnauthenticated(const TString&) {
    TActivationContext::Send(new IEventHandle(From_, TActorId(),
        new TGRpcRequestProxy::TEvRefreshTokenResponse
            { false, nullptr, false, IssueManager_.GetIssues()}));
}

void TRefreshTokenImpl::ReplyUnavaliable() {
    const TActorContext& ctx = TActivationContext::AsActorContext();
    ctx.Send(From_,
        new TGRpcRequestProxy::TEvRefreshTokenResponse
            { false, nullptr, true, IssueManager_.GetIssues()});
}

} // namespace NGRpcService
} // namespace NKikimr
