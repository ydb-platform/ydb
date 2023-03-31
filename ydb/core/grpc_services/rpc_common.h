#pragma once

#include "grpc_request_proxy.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/proto_duration.h>

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

} // namespace NGRpcService
} // namespace NKikimr
