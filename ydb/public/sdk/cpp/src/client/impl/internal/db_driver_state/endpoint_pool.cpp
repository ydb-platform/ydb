#define INCLUDE_YDB_INTERNAL_H
#include "endpoint_pool.h"

#include <library/cpp/threading/future/core/coroutine_traits.h>

namespace NYdb::inline Dev {

TEndpointPool::TEndpointPool(TListEndpointsResultProvider&& provider, IInternalClient* client)
    : Provider_(std::move(provider))
    , Client_(client)
    , LastUpdateTime_(TInstant::Zero().MicroSeconds())
    , BalancingPolicy_(client->GetBalancingSettings())
{}

void TEndpointPool::EnableWatch() {
    UsesWatch_ = true;
    DiscoveryPromise_ = NThreading::NewPromise<TEndpointUpdateResult>();
}

void TEndpointPool::PublishWatchResult(TListEndpointsResult result, ERouteUpdate routeUpdate) {
    {
        std::lock_guard guard(Mutex_);
        if (!DiscoveryPromise_.Initialized()) {
            DiscoveryPromise_ = NThreading::NewPromise<TEndpointUpdateResult>();
        }
    }
    if (routeUpdate == ERouteUpdate::Switch) {
        // Replica-local node IDs and even host:port pairs can be reused.
        Install({});
    }
    TEndpointUpdateResult update;
    if (result.DiscoveryStatus.Ok() && result.Result.endpoints().empty()) {
        result.DiscoveryStatus = TPlainStatus(EStatus::UNAVAILABLE, "Discovery returned an empty endpoint assignment");
    }
    update.DiscoveryStatus = std::move(result.DiscoveryStatus);
    if (update.DiscoveryStatus.Ok() || routeUpdate != ERouteUpdate::LegacyRefresh) {
        update.Removed = Install(update.DiscoveryStatus.Ok() ? result.Result : Ydb::Discovery::ListEndpointsResult{});
    }
    NThreading::TPromise<TEndpointUpdateResult> promise;
    {
        std::lock_guard guard(Mutex_);
        WatchStatus_ = update.DiscoveryStatus;
        DiscoveryPromise_.Swap(promise);
    }
    if (promise.Initialized()) {
        promise.SetValue(std::move(update));
    }
}

TEndpointPool::~TEndpointPool() {
    if (UsesWatch_) {
        return;
    }
    try {
        NThreading::TFuture<TEndpointUpdateResult> future;
        {
            std::lock_guard guard(Mutex_);
            if (DiscoveryPromise_.Initialized()) {
               future = DiscoveryPromise_.GetFuture();
            }
        }
        if (future.Initialized()) {
            future.Wait();
        }
    } catch (...) {
        Y_ABORT("Unexpected exception from endpoint pool dtor");
    }
}

std::pair<NThreading::TFuture<TEndpointUpdateResult>, bool> TEndpointPool::UpdateAsync() {
    NThreading::TFuture<TEndpointUpdateResult> future;
    {
        std::lock_guard guard(Mutex_);
        if (UsesWatch_) {
            return {DiscoveryPromise_.Initialized() ? DiscoveryPromise_.GetFuture()
                : NThreading::MakeFuture(TEndpointUpdateResult{{}, WatchStatus_}), false};
        }
        if (DiscoveryPromise_.Initialized()) {
            return {DiscoveryPromise_.GetFuture(), false};
        } else {
            DiscoveryPromise_ = NThreading::NewPromise<TEndpointUpdateResult>();
            future = DiscoveryPromise_.GetFuture();
        }
    }
    auto handler = [this](const TAsyncListEndpointsResult& future) {
        TListEndpointsResult result = future.GetValue();
        std::vector<std::string> removed;
        if (result.DiscoveryStatus.Status == EStatus::SUCCESS) {
            removed = Install(result.Result);
        }
        NThreading::TPromise<TEndpointUpdateResult> promise;
        {
            std::lock_guard guard(Mutex_);
            DiscoveryPromise_.Swap(promise);
        }
        promise.SetValue({std::move(removed), result.DiscoveryStatus});
    };

    Provider_().Subscribe(handler);
    return {future, true};
}

std::vector<std::string> TEndpointPool::Install(const Ydb::Discovery::ListEndpointsResult& result) {
    std::vector<TEndpointRecord> records;
    // Is used to convert float to integer load factor
    // same integer values will be selected randomly.
    const float multiplicator = 10.0;
    std::string selfLocation = result.self_location();
    std::unordered_map<std::string, Ydb::Bridge::PileState> pileStates;
    for (const auto& pile : result.pile_states()) {
        pileStates[pile.pile_name()] = pile;
    }

    for (const auto& endpoint : result.endpoints()) {
        std::int32_t loadFactor = static_cast<std::int32_t>(multiplicator * std::min(LoadMax, std::max(LoadMin, endpoint.load_factor())));
        std::uint64_t nodeId = endpoint.node_id();
        std::string location = endpoint.location();
        if (!IsPreferredEndpoint(endpoint, selfLocation, pileStates)) {
            // Location mismatch, shift this endpoint
            loadFactor += GetLocalityShift();
        }

        std::string sslTargetNameOverride = endpoint.ssl_target_name_override();
        auto getIpSslTargetNameOverride = [&]() -> std::string {
            if (!sslTargetNameOverride.empty()) {
                return sslTargetNameOverride;
            }
            if (endpoint.ssl()) {
                return endpoint.address();
            }
            return std::string();
        };

        bool addDefault = true;
        for (const auto& addr : endpoint.ip_v6()) {
            if (addr.empty()) {
                continue;
            }
            TStringBuilder endpointBuilder;
            endpointBuilder << "ipv6:";
            if (addr[0] != '[') {
                endpointBuilder << "[";
            }
            endpointBuilder << addr;
            if (addr[addr.size()-1] != ']') {
                endpointBuilder << "]";
            }
            endpointBuilder << ":" << endpoint.port();
            std::string endpointString = std::move(endpointBuilder);
            records.emplace_back(std::move(endpointString), loadFactor, getIpSslTargetNameOverride(), nodeId, location);
            addDefault = false;
        }
        for (const auto& addr : endpoint.ip_v4()) {
            if (addr.empty()) {
                continue;
            }
            std::string endpointString =
                TStringBuilder()
                    << "ipv4:"
                    << addr
                    << ":"
                    << endpoint.port();
            records.emplace_back(std::move(endpointString), loadFactor, getIpSslTargetNameOverride(), nodeId, location);
            addDefault = false;
        }
        if (addDefault) {
            std::string endpointString =
                TStringBuilder()
                    << endpoint.address()
                    << ":"
                    << endpoint.port();
            records.emplace_back(std::move(endpointString), loadFactor, std::move(sslTargetNameOverride), nodeId, std::move(location));
        }
    }
    if (UsesWatch_) {
        // The subscription serializes installations. Retire stale dispatches
        // before SetNewState notifies sessions or publishes new endpoints.
        decltype(WatchAdmissions_) next;
        for (auto& record : records) {
            auto& admission = next[record.Endpoint];
            if (!admission) {
                auto it = WatchAdmissions_.find(record.Endpoint);
                admission = it == WatchAdmissions_.end() ? std::make_shared<NYdbGrpc::TEndpointAdmission>() : it->second;
            }
            record.Admission = admission;
        }
        std::vector<std::string> removed;
        for (auto& [endpoint, admission] : WatchAdmissions_) {
            if (!next.contains(endpoint)) {
                admission->Retire();
                removed.push_back(endpoint);
            }
        }
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
        Client_->DeleteChannels(removed, *this);
#endif
        WatchAdmissions_.swap(next);
    }
    LastUpdateTime_ = TInstant::Now().MicroSeconds();
    return Elector_.SetNewState(std::move(records));
}

TEndpointRecord TEndpointPool::GetEndpoint(const TEndpointKey& preferredEndpoint, bool onlyPreferred) const {
    return Elector_.GetEndpoint(preferredEndpoint, onlyPreferred);
}

TDuration TEndpointPool::TimeSinceLastUpdate() const {
    auto now = TInstant::Now().MicroSeconds();
    return TDuration::MicroSeconds(now - LastUpdateTime_.load());
}

void TEndpointPool::BanEndpoint(const std::string& endpoint) {
    Elector_.PessimizeEndpoint(endpoint);
}

void TEndpointPool::BanNodeId(std::uint64_t nodeId) {
    Elector_.PessimizeNode(nodeId);
}

int TEndpointPool::GetPessimizationRatio() {
    return Elector_.GetPessimizationRatio();
}

bool TEndpointPool::LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag) {
    return Elector_.LinkObjToEndpoint(endpoint, obj, tag);
}

void TEndpointPool::ForEachEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    return Elector_.ForEachEndpoint(cb, 0, std::numeric_limits<std::int32_t>::max(), tag);
}

void TEndpointPool::ForEachLocalEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    return Elector_.ForEachEndpoint(cb, 0, GetLocalityShift() - 1, tag);
}

void TEndpointPool::ForEachForeignEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    return Elector_.ForEachEndpoint(cb, GetLocalityShift(), std::numeric_limits<std::int32_t>::max() - 1, tag);
}

TBalancingPolicy::TImpl::EPolicyType TEndpointPool::GetBalancingPolicyType() const {
    return BalancingPolicy_.PolicyType;
}

void TEndpointPool::SetStatCollector(NSdkStats::TStatCollector& statCollector) {
    if (!statCollector.IsCollecting())
        return;
    Elector_.SetStatCollector(statCollector.GetEndpointElectorStatCollector());
    StatCollector_ = &statCollector;
}

constexpr std::int32_t TEndpointPool::GetLocalityShift() {
    return LoadMax * Multiplicator;
}

bool TEndpointPool::IsPreferredEndpoint(const Ydb::Discovery::EndpointInfo& endpoint,
                                        const std::string& selfLocation,
                                        const std::unordered_map<std::string, Ydb::Bridge::PileState>& pileStates) const {
    switch (BalancingPolicy_.PolicyType) {
        case TBalancingPolicy::TImpl::EPolicyType::UseAllNodes:
            return true;
        case TBalancingPolicy::TImpl::EPolicyType::UsePreferableLocation:
            return endpoint.location() == BalancingPolicy_.Location.value_or(selfLocation);
        case TBalancingPolicy::TImpl::EPolicyType::UsePreferablePileState:
            if (auto it = pileStates.find(endpoint.bridge_pile_name()); it != pileStates.end()) {
                return GetPileState(it->second.state()) == BalancingPolicy_.PileState;
            }
            return true;
    }
    return true;
}

EPileState TEndpointPool::GetPileState(const Ydb::Bridge::PileState::State& state) const {
    switch (state) {
        case Ydb::Bridge::PileState::PRIMARY:
            return EPileState::PRIMARY;
        case Ydb::Bridge::PileState::PROMOTED:
            return EPileState::PROMOTED;
        case Ydb::Bridge::PileState::SYNCHRONIZED:
            return EPileState::SYNCHRONIZED;
        case Ydb::Bridge::PileState::NOT_SYNCHRONIZED:
            return EPileState::NOT_SYNCHRONIZED;
        case Ydb::Bridge::PileState::SUSPENDED:
            return EPileState::SUSPENDED;
        case Ydb::Bridge::PileState::DISCONNECTED:
            return EPileState::DISCONNECTED;
        case Ydb::Bridge::PileState::INACTIVE:
            return EPileState::INACTIVE;
        case Ydb::Bridge::PileState::HOT_STANDBY:
            return EPileState::HOT_STANDBY;
        case Ydb::Bridge::PileState::UNSPECIFIED:
        case Ydb::Bridge::PileState_State_PileState_State_INT_MIN_SENTINEL_DO_NOT_USE_:
        case Ydb::Bridge::PileState_State_PileState_State_INT_MAX_SENTINEL_DO_NOT_USE_:
            return EPileState::UNSPECIFIED;
    }
}

} // namespace NYdb
