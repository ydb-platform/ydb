#pragma once
#include "defs.h"

#include <util/generic/variant.h>
#include <ydb/core/grpc_services/base/base.h>

#include <functional>

namespace NActors {
    struct TActorId;
}

class TDuration;

namespace Ydb {
namespace RateLimiter {
    class AcquireResourceResponse;
    class AcquireResourceRequest;
}
}

namespace NKikimr {

namespace NGRpcService {
    class IRequestCtxBase;
}

namespace NRpcService {

struct TRlFullPath {
    TString CoordinationNode;
    TString ResourcePath;
    TString DatabaseName;
    TString Token;
};

// Acquire `acquireUnits`. The rpc actor will be registered using same mailbox.
TActorId RateLimiterAcquireUseSameMailbox(
    const TRlFullPath& fullPath,
    ui64 required,
    const TDuration& duration,
    std::function<void()>&& onSuccess,
    std::function<void()>&& onTimeout,
    const TActorContext& ctx);

TActorId RateLimiterAcquireUseSameMailbox(
    Ydb::RateLimiter::AcquireResourceRequest&& request,
    const TString& database,
    const TString& token,
    std::function<void(Ydb::RateLimiter::AcquireResourceResponse resp)>&& cb, const TActorContext &ctx);

TActorId RateLimiterAcquireUseSameMailbox(const NGRpcService::IRequestCtxBase& reqCtx,
    ui64 required,
    const TDuration& duration,
    std::function<void()>&& onSuccess,
    std::function<void()>&& onFail,
    const NActors::TActorContext &ctx);

struct TRlConfig {
    struct TOnReqAction {
        ui64 Required;
    };

    struct TOnRespAction {
    };

    using TActions = std::variant<TOnReqAction, TOnRespAction>;

    TRlConfig(const TString& coordinationNodeKey, const TString& resourceKey, const TVector<TActions>& actions)
        : CoordinationNodeKey(coordinationNodeKey)
        , ResourceKey(resourceKey)
        , Actions(actions)
    {}

    const TString CoordinationNodeKey;
    // The key in the database attribute map to find actual 'resource path'
    const TString ResourceKey;
    const TVector<TActions> Actions;
};

enum class Actions {
    OnReq,
    OnResp
};

TMaybe<TRlPath> Match(const TRlConfig& rlConfig, const THashMap<TString, TString>& attrs);
TVector<std::pair<Actions, Ydb::RateLimiter::AcquireResourceRequest>> MakeRequests(
    const TRlConfig& rlConfig, const TRlPath& rlPath);

} // namespace NRpcService
} // namespace NKikimr
