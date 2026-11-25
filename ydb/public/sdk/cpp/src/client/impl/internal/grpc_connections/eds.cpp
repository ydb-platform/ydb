#define INCLUDE_YDB_INTERNAL_H
#include "grpc_connections.h"

#include <ydb/public/api/grpc/draft/xds/codec/codec.h>
#include <ydb/public/api/grpc/draft/xds/eds.grpc.pb.h>

#include <library/cpp/threading/future/core/coroutine_traits.h>
#include <util/generic/guid.h>
#include <util/random/random.h>

#include <charconv>

namespace NYdb::inline Dev {
namespace {

using TEdsService = envoy::service::endpoint::v3::YdbDraft::EndpointDiscoveryService;
using TRequest = Ydb::Xds::DiscoveryRequest;
using TResponse = Ydb::Xds::DiscoveryResponse;
using TStream = NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>::TPtr;
using TGrpcStatus = NYdbGrpc::TGrpcStatus;

constexpr char RouteMode[] = "x-ydb-route-mode";
constexpr char RouteGeneration[] = "x-ydb-route-generation";
constexpr char MinimumGeneration[] = "x-ydb-min-route-generation";
constexpr char RequireCurrent[] = "x-ydb-require-current-route";

template <class TMetadata>
std::optional<std::uint64_t> ReadGeneration(const TMetadata& metadata) {
    if (metadata.count(RouteGeneration) != 1) {
        return {};
    }
    const auto& value = metadata.find(RouteGeneration)->second;
    std::uint64_t generation = 0;
    auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), generation);
    if (error != std::errc{} || end != value.data() + value.size()) {
        return {};
    }
    return generation;
}

// Only an explicit managed initial header opts a route into generation checks.
// Once observed, missing, duplicated or older metadata cannot release the gate.
struct TRoute {
    std::optional<std::uint64_t> Floor;
    bool RequireCurrentRoute = false;

    template <class TMetadata>
    bool Accept(const TMetadata& metadata) {
        if (!metadata.count(RouteMode) && !metadata.count(RouteGeneration)) {
            return !Floor;
        }
        const auto generation = ReadGeneration(metadata);
        if (!generation || (Floor && *generation < *Floor) || metadata.count(RouteMode) != 1
            || metadata.find(RouteMode)->second != "managed") {
            return false;
        }
        Floor = generation;
        return true;
    }

    TRpcRequestSettings Settings() const {
        TRpcRequestSettings settings;
        settings.EndpointPolicy = TRpcRequestSettings::TEndpointPolicy::UseDiscoveryEndpoint;
        settings.IncludeObservabilityInBuildInfo = true;
        if (Floor) {
            settings.Header.emplace_back(MinimumGeneration, std::to_string(*Floor));
        }
        if (RequireCurrentRoute) {
            settings.Header.emplace_back(RequireCurrent, "1");
        }
        return settings;
    }
};

TRequest Subscription(const std::string& database) {
    TRequest request;
    request.set_type_url(NEds::TypeUrl);
    request.add_resource_names(database);
    return request;
}

auto Complete(NThreading::TPromise<TGrpcStatus> promise) {
    return [promise = std::move(promise)](TGrpcStatus&& status) mutable {
        promise.SetValue(std::move(status));
    };
}

TPlainStatus ToWatchStatus(const TGrpcStatus& status, const std::string& endpoint) {
    if (status.GRpcStatusCode == grpc::StatusCode::PERMISSION_DENIED) {
        return TPlainStatus(EStatus::UNAUTHORIZED, status.Msg);
    }
    return TPlainStatus(status, endpoint);
}

} // namespace

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
    TRoute route;
    const auto nodeId = CreateGuidAsString();
    auto unsupportedUntil = TInstant::Zero();
    unsigned failures = 0;
    auto publish = [&](TListEndpointsResult result) {
        if (auto state = weak.lock()) {
            state->EndpointPool.PublishWatchResult(std::move(result));
            state->SignalDiscoveryCompleted();
        }
    };

    while (!context->IsCancelled()) {
        auto state = weak.lock();
        if (!state) {
            co_return;
        }
        TDuration delay;
        try {
            if (TInstant::Now() < unsupportedUntil) {
                auto call = ListEndpoints(state, route.Settings());
                state.reset();
                auto result = co_await call;
                if (result.DiscoveryStatus.Ok() && !route.Accept(result.DiscoveryStatus.Metadata)) {
                    result = {{}, TPlainStatus(EStatus::UNAVAILABLE, "Unary discovery returned a stale or missing route generation")};
                }
                if (result.DiscoveryStatus.Ok()) {
                    route.RequireCurrentRoute = false;
                    failures = 0;
                    delay = TDuration::Minutes(1);
                }
                publish(std::move(result));
            } else {
                const auto database = state->Database;
                const auto endpoint = state->DiscoveryEndpoint;
                auto attempt = context->CreateContext();
                auto timer = attempt ? attempt->CreateContext() : nullptr;
                if (!timer) {
                    break;
                }
                // Limit a stalled handshake/first assignment, leaving an established watch idle indefinitely.
                ScheduleCallback(TDuration::Seconds(10), [attempt](bool fired) { if (fired) attempt->Cancel(); }, timer);
                auto connected = NThreading::NewPromise<std::pair<TPlainStatus, TStream>>();
                StartBidirectionalStream<TEdsService, TRequest, TResponse>(
                    [connected](TPlainStatus status, TStream stream) mutable {
                        connected.SetValue(std::make_pair(std::move(status), std::move(stream)));
                    }, &TEdsService::Stub::AsyncStreamEndpoints, state, route.Settings(), attempt);
                state.reset();
                auto [status, stream] = co_await connected.GetFuture();
                std::unordered_multimap<std::string, std::string> initial;
                TGrpcStatus terminal;
                bool receivedResponse = false;
                bool installed = false;
                if (stream) {
                    auto request = Subscription(database);
                    request.mutable_node()->set_id(nodeId);
                    stream->Write(std::move(request));
                    auto headers = NThreading::NewPromise<TGrpcStatus>();
                    stream->ReadInitialMetadata(&initial, Complete(headers));
                    terminal = co_await headers.GetFuture();
                    std::string acceptedVersion;
                    while (terminal.Ok() && !context->IsCancelled()) {
                        TResponse response;
                        auto read = NThreading::NewPromise<TGrpcStatus>();
                        stream->Read(&response, Complete(read));
                        terminal = co_await read.GetFuture();
                        if (!terminal.Ok()) {
                            break;
                        }
                        receivedResponse = true;
                        TListEndpointsResult snapshot;
                        TString error;
                        auto reply = Subscription(database);
                        reply.set_response_nonce(response.nonce());
                        if (!NEds::Decode(response, TString(database), snapshot.Result, error)) {
                            reply.mutable_error_detail()->set_code(grpc::StatusCode::INVALID_ARGUMENT);
                            reply.mutable_error_detail()->set_message(error);
                        } else if (!route.Accept(initial)) {
                            terminal = TGrpcStatus(grpc::StatusCode::UNAVAILABLE, "EDS returned a stale or missing route generation");
                            break;
                        } else if (auto owner = weak.lock()) {
                            snapshot = MutateDiscovery(std::move(snapshot), owner.get());
                            if (!snapshot.DiscoveryStatus.Ok()) {
                                reply.mutable_error_detail()->set_code(grpc::StatusCode::INVALID_ARGUMENT);
                                reply.mutable_error_detail()->set_message(snapshot.DiscoveryStatus.Issues.ToString());
                            } else {
                                publish(std::move(snapshot));
                                acceptedVersion = response.version_info();
                                installed = true;
                                route.RequireCurrentRoute = false;
                                failures = 0;
                                timer->Cancel();
                            }
                        } else {
                            break;
                        }
                        reply.set_version_info(acceptedVersion);
                        auto ack = NThreading::NewPromise<TGrpcStatus>();
                        stream->Write(std::move(reply), Complete(ack));
                        terminal = co_await ack.GetFuture();
                    }
                    status = ToWatchStatus(terminal, endpoint);
                    stream->Cancel();
                }
                timer->Cancel();
                attempt->Cancel();
                if (context->IsCancelled()) {
                    break;
                }
                if (!receivedResponse && status.Status == EStatus::CLIENT_CALL_UNIMPLEMENTED && route.Accept(initial)) {
                    unsupportedUntil = TInstant::Now() + TDuration::Minutes(5);
                    continue;
                }
                if (installed || route.Floor) {
                    auto cut = ReadGeneration(terminal.ServerTrailingMetadata);
                    if (route.Floor && terminal.GRpcStatusCode == grpc::StatusCode::UNAVAILABLE && cut && *cut > *route.Floor) {
                        route.Floor = cut;
                        route.RequireCurrentRoute = false;
                    } else {
                        route.RequireCurrentRoute = true;
                    }
                }
                if (installed) {
                    if (auto owner = weak.lock()) {
                        owner->EndpointPool.PauseWatch();
                    }
                } else {
                    publish({{}, std::move(status)});
                }
            }
        } catch (const std::exception& error) {
            publish({{}, TPlainStatus(EStatus::CLIENT_INTERNAL_ERROR, error.what())});
        }
        state.reset();
        if (!delay && failures++ != 0) {
            const auto maxDelay = 100u << std::min(failures - 1, 6u);
            delay = TDuration::MilliSeconds(maxDelay / 2 + RandomNumber<unsigned>(maxDelay / 2));
        }
        if (delay && !(co_await ScheduleFuture(delay, context))) {
            break;
        }
    }
    publish({{}, MakeClientStoppedStatus()});
}

} // namespace NYdb
