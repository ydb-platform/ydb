#include "local_rate_limiter.h"
#include "service_ratelimiter_events.h"
#include "rpc_common/rpc_common.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

namespace NKikimr {
namespace NRpcService {

TActorId RateLimiterAcquireUseSameMailbox(
    const TRlFullPath& fullPath,
    ui64 required,
    const TDuration& duration,
    std::function<void()>&& onSuccess,
    std::function<void()>&& onTimeout,
    const TActorContext& ctx)
{
    auto cb = [onSuccess{std::move(onSuccess)}, onTimeout{std::move(onTimeout)}]
        (const Ydb::RateLimiter::AcquireResourceResponse& resp)
    {
        switch (resp.operation().status()) {
            case Ydb::StatusIds::SUCCESS:
                onSuccess();
            break;
            case Ydb::StatusIds::TIMEOUT:
            case Ydb::StatusIds::CANCELLED:
                onTimeout();
            break;
            default:
                // Fallback
                onSuccess();
         }
    };

    Ydb::RateLimiter::AcquireResourceRequest request;
    SetDuration(duration * 10, *request.mutable_operation_params()->mutable_operation_timeout());
    SetDuration(duration, *request.mutable_operation_params()->mutable_cancel_after());
    request.set_coordination_node_path(fullPath.CoordinationNode);
    request.set_resource_path(fullPath.ResourcePath);
    request.set_required(required);
    return RateLimiterAcquireUseSameMailbox(
        std::move(request),
        fullPath.DatabaseName,
        fullPath.Token,
        std::move(cb),
        ctx);
}

TActorId RateLimiterAcquireUseSameMailbox(
    Ydb::RateLimiter::AcquireResourceRequest&& request,
    const TString& database,
    const TString& token,
    std::function<void(Ydb::RateLimiter::AcquireResourceResponse resp)>&& cb,
    const TActorContext& ctx)
{
    return DoLocalRpcSameMailbox<NKikimr::NGRpcService::TEvAcquireRateLimiterResource>(
        std::move(request), std::move(cb), database, token, ctx);
}

TActorId RateLimiterAcquireUseSameMailbox(
    const NGRpcService::IRequestCtxBase& reqCtx,
    ui64 required,
    const TDuration& duration,
    std::function<void()>&& onSuccess,
    std::function<void()>&& onTimeout,
    const TActorContext& ctx)
{
    if (const auto maybeRlPath = reqCtx.GetRlPath()) {
        auto cb = [onSuccess{std::move(onSuccess)}, onTimeout{std::move(onTimeout)}]
            (const Ydb::RateLimiter::AcquireResourceResponse& resp)
        {
            switch (resp.operation().status()) {
                case Ydb::StatusIds::SUCCESS:
                    onSuccess();
                break;
                case Ydb::StatusIds::TIMEOUT:
                case Ydb::StatusIds::CANCELLED:
                    onTimeout();
                break;
                default:
                    // Fallback
                    onSuccess();
             }
        };

        const auto& rlPath = maybeRlPath.GetRef();
        Ydb::RateLimiter::AcquireResourceRequest request;
        SetDuration(duration * 10, *request.mutable_operation_params()->mutable_operation_timeout());
        SetDuration(duration, *request.mutable_operation_params()->mutable_cancel_after());
        request.set_coordination_node_path(rlPath.CoordinationNode);
        request.set_resource_path(rlPath.ResourcePath);
        request.set_required(required);
        return RateLimiterAcquireUseSameMailbox(
            std::move(request),
            reqCtx.GetDatabaseName().GetOrElse(""),
            reqCtx.GetSerializedToken(),
            std::move(cb),
            ctx);
    }
    return {};
}

static void Fill(const TRlConfig::TOnReqAction& action,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    TVector <std::pair<Actions, Ydb::RateLimiter::AcquireResourceRequest>>& res)
{
    Ydb::RateLimiter::AcquireResourceRequest request;
    request.set_coordination_node_path(coordinationNodePath);
    request.set_resource_path(resourcePath);
    request.set_required(action.Required);

    res.push_back({Actions::OnReq, request});
}

static void Fill(const TRlConfig::TOnRespAction&,
    const TString& coordinationNodePath,
    const TString& resourcePath,
    TVector <std::pair<Actions, Ydb::RateLimiter::AcquireResourceRequest>>& res)
{
    Ydb::RateLimiter::AcquireResourceRequest request;
    request.set_coordination_node_path(coordinationNodePath);
    request.set_resource_path(resourcePath);

    res.push_back({Actions::OnResp, request});
}

TMaybe<TRlPath> Match(const TRlConfig& rlConfig, const THashMap<TString, TString>& attrs) {
    const auto coordinationNodeIt = attrs.find(rlConfig.CoordinationNodeKey);
    if (coordinationNodeIt == attrs.end()) {
        return {};
    }

    const auto rlResourcePathIt = attrs.find(rlConfig.ResourceKey);
    if (rlResourcePathIt == attrs.end()) {
        return {};
    }

    return TRlPath{coordinationNodeIt->second, rlResourcePathIt->second};
}

TVector<std::pair<Actions, Ydb::RateLimiter::AcquireResourceRequest>> MakeRequests(
    const TRlConfig& rlConfig, const TRlPath& rlPath)
{
    TVector <std::pair<Actions, Ydb::RateLimiter::AcquireResourceRequest>> result;
    result.reserve(rlConfig.Actions.size());

    for (auto& action : rlConfig.Actions) {
        auto f = [&](const auto& item) {
            Fill(item, rlPath.CoordinationNode, rlPath.ResourcePath, result);
        };
        std::visit(f, action);
    }

    return result;
}

} // namespace NRpcService
} // namespace NKikimr
