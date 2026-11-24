#define INCLUDE_YDB_INTERNAL_H
#include "grpc_connections.h"

#include <library/cpp/threading/future/core/coroutine_traits.h>
#include <util/random/random.h>

#include <charconv>

namespace NYdb::inline Dev {

void TGRpcConnectionsImpl::StartEndpointWatch(TDbDriverStatePtr state) {
    if (auto context = CreateContext()) {
        state->EndpointWatchContext = context;
        WatchEndpoints(state, std::move(context));
    } else {
        state->EndpointPool.PublishWatchResult({{}, MakeClientStoppedStatus()});
        state->SignalDiscoveryCompleted();
    }
}

NThreading::TFuture<void> TGRpcConnectionsImpl::WatchEndpoints(std::weak_ptr<TDbDriverState> weak,
    IQueueClientContextPtr context) {
    std::optional<std::uint64_t> generation;
    bool installedLegacy = false;
    unsigned failures = 0;
    while (!context->IsCancelled()) {
        auto state = weak.lock();
        if (!state) {
            co_return;
        }
        TRpcRequestSettings settings;
        settings.Header.emplace_back("x-ydb-discovery-long-poll", "1");
        if (generation) {
            settings.Header.emplace_back("x-ydb-if-route-generation", std::to_string(*generation));
            // A managed worker may hold the request for 30 seconds before forwarding it.
            settings.Deadline = TDeadline::AfterDuration(std::chrono::seconds(45));
        }
        TDuration delay;
        TListEndpointsResult result;
        auto update = installedLegacy && !generation
            ? TEndpointPool::ERouteUpdate::LegacyRefresh : TEndpointPool::ERouteUpdate::Refresh;
        try {
            auto call = ListEndpoints(state, std::move(settings), context);
            state.reset();
            result = co_await call;
            if (result.DiscoveryStatus.Ok()) {
                const auto& metadata = result.DiscoveryStatus.Metadata;
                const auto [it, end] = metadata.equal_range("x-ydb-route-generation");
                if (it == end && !generation) {
                    installedLegacy |= !result.Result.endpoints().empty();
                    delay = TDuration::Minutes(1); // Unmodified worker. Probe again on the next normal refresh.
                } else {
                    std::uint64_t next = 0;
                    bool valid = false;
                    if (it != end && std::next(it) == end) {
                        const auto& value = it->second;
                        auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), next);
                        valid = ec == std::errc{} && ptr == value.data() + value.size() && (!generation || next >= *generation);
                    }
                    if (valid) {
                        if ((!generation && installedLegacy) || (generation && next > *generation)) {
                            update = TEndpointPool::ERouteUpdate::Switch;
                        }
                        generation = next;
                    } else {
                        result = {{}, TPlainStatus(EStatus::UNAVAILABLE, "Invalid or stale discovery route generation")};
                    }
                }
            }
        } catch (const std::exception& error) {
            result = {{}, TPlainStatus(EStatus::CLIENT_INTERNAL_ERROR, error.what())};
        }
        state = weak.lock();
        if (!state) {
            co_return;
        }
        if (context->IsCancelled()) {
            break;
        }
        if (!result.DiscoveryStatus.Ok() || result.Result.endpoints().empty()) {
            failures = std::min(6u, failures + 1);
            const auto ms = 100u << failures;
            delay = TDuration::MilliSeconds(ms / 2 + RandomNumber<unsigned>(ms / 2));
        } else {
            failures = 0;
        }
        state->EndpointPool.PublishWatchResult(std::move(result), update, generation);
        state->SignalDiscoveryCompleted();
        state.reset();
        if (delay && !(co_await ScheduleFuture(delay, context))) {
            break;
        }
    }
    if (auto state = weak.lock()) {
        state->EndpointPool.PublishWatchResult({{}, MakeClientStoppedStatus()});
        state->SignalDiscoveryCompleted();
    }
}

} // namespace NYdb
