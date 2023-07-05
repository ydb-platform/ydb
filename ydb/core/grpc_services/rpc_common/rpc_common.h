#pragma once


#include <ydb/core/base/path.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/proto_duration.h>
#include "ydb/core/grpc_services/grpc_request_proxy.h"

namespace NKikimr {
namespace NGRpcService {
class IRequestCtx;

template<typename TEv>
inline void SetRlPath(TEv& ev, const IRequestCtx& ctx) {
    if (const auto& path = ctx.GetRlPath()) {
        auto rl = ev->Record.MutableRlPath();
        rl->SetCoordinationNode(path->CoordinationNode);
        rl->SetResourcePath(path->ResourcePath);
    }
}

template<typename TEv>
inline void SetAuthToken(TEv& ev, const IRequestCtx& ctx) {
    if (ctx.GetSerializedToken()) {
        ev->Record.SetUserToken(ctx.GetSerializedToken());
    }
}

template<typename TEv>
inline void SetDatabase(TEv& ev, const IRequestCtx& ctx) {
    // Empty database in case of absent header
    ev->Record.MutableRequest()->SetDatabase(CanonizePath(ctx.GetDatabaseName().GetOrElse("")));
}

inline void SetDatabase(TEvTxUserProxy::TEvProposeTransaction* ev, const IRequestCtx& ctx) {
    // Empty database in case of absent header
    ev->Record.SetDatabaseName(CanonizePath(ctx.GetDatabaseName().GetOrElse("")));
}

inline void SetDatabase(TEvTxUserProxy::TEvNavigate* ev, const IRequestCtx& ctx) {
    // Empty database in case of absent header
    ev->Record.SetDatabaseName(CanonizePath(ctx.GetDatabaseName().GetOrElse("")));
}

inline void SetRequestType(TEvTxUserProxy::TEvProposeTransaction* ev, const IRequestCtx& ctx) {
    ev->Record.SetRequestType(ctx.GetRequestType().GetOrElse(""));
}

inline void SetPeerName(TEvTxUserProxy::TEvProposeTransaction* ev, const IRequestCtx& ctx) {
    ev->Record.SetPeerName(ctx.GetPeerName());
}

inline bool CheckSession(const TString& sessionId, IRequestCtxBase* ctx) {
    static const auto err = TString("Empty session id");
    if (sessionId.empty()) {
        ctx->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, err));
        return false;
    }

    return true;
}

inline bool CheckQuery(const TString& query, IRequestCtxBase* ctx) {
    static const auto err = TString("Empty query text");
    if (query.empty()) {
        ctx->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, err));
        return false;
    }

    return true;
}

} // namespace NGRpcService
} // namespace NKikimr
