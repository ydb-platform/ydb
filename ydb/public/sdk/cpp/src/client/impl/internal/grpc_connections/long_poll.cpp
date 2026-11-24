#define INCLUDE_YDB_INTERNAL_H
#include "grpc_connections.h"

#include <library/cpp/threading/future/core/coroutine_traits.h>
#include <util/random/random.h>

#include <algorithm>
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
    std::string endpointVersion;
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
        }
        if (!endpointVersion.empty()) {
            settings.Header.emplace_back("x-ydb-if-endpoint-version", endpointVersion);
        }
        if (generation || !endpointVersion.empty()) {
            // YDB or a managed worker may hold the request for 30 seconds before forwarding it.
            settings.Deadline = TDeadline::AfterDuration(std::chrono::seconds(45));
        }
        TDuration delay;
        TListEndpointsResult result;
        auto update = installedLegacy && !generation
            ? TEndpointPool::ERouteUpdate::LegacyRefresh : TEndpointPool::ERouteUpdate::Refresh;
        const auto started = std::chrono::steady_clock::now();
        try {
            auto call = ListEndpoints(state, std::move(settings), context);
            state.reset();
            result = co_await call;
            if (result.DiscoveryStatus.Ok()) {
                const auto& metadata = result.DiscoveryStatus.Metadata;
                const auto [it, end] = metadata.equal_range("x-ydb-route-generation");
                const auto previousGeneration = generation;
                if (it != end || generation) {
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
                if (result.DiscoveryStatus.Ok() && !result.Result.endpoints().empty()) {
                    const auto [version, last] = metadata.equal_range("x-ydb-endpoint-version");
                    std::string nextVersion;
                    if (version != last && std::next(version) == last) {
                        const auto& value = version->second;
                        if (!value.empty() && value.size() <= 256 && std::all_of(value.begin(), value.end(), [](unsigned char c) {
                                return c >= 0x21 && c <= 0x7e && c != ',';
                            })) {
                            nextVersion = value;
                        }
                    }
                    installedLegacy |= !generation;
                    if (!generation && nextVersion.empty()) {
                        delay = TDuration::Minutes(1);
                    } else if ((generation || !endpointVersion.empty()) && previousGeneration == generation
                            && nextVersion == endpointVersion) {
                        // An intermediary can echo an unchanged snapshot immediately.
                        // Limit its request rate while leaving normal held replies unaffected.
                        const auto elapsed = std::chrono::steady_clock::now() - started;
                        if (elapsed < std::chrono::seconds(1)) {
                            delay = TDuration::MilliSeconds(1000 - std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
                        }
                    }
                    endpointVersion = std::move(nextVersion);
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
