#define INCLUDE_YDB_INTERNAL_H
#include "endpoint_pool.h"

namespace NYdb::inline Dev {

TEndpointPool::TEndpointPool(TListEndpointsResultProvider&& provider, const IInternalClient* client)
    : Provider_(provider)
    , LastUpdateTime_(TInstant::Zero().MicroSeconds())
    , BalancingPolicy_(client->GetBalancingSettings())
{}

TEndpointPool::~TEndpointPool() {
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
            std::vector<TEndpointRecord> records;
            // Is used to convert float to integer load factor
            // same integer values will be selected randomly.
            const float multiplicator = 10.0;
            std::string selfLocation = result.Result.self_location();
            std::unordered_map<std::string, Ydb::Bridge::PileState> pileStates;
            for (const auto& pile : result.Result.pile_states()) {
                pileStates[pile.pile_name()] = pile;
            }

            for (const auto& endpoint : result.Result.endpoints()) {
                std::int32_t loadFactor = static_cast<std::int32_t>(multiplicator * std::min(LoadMax, std::max(LoadMin, endpoint.load_factor())));
                std::uint64_t nodeId = endpoint.node_id();
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
                    records.emplace_back(std::move(endpointString), loadFactor, getIpSslTargetNameOverride(), nodeId);
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
                    records.emplace_back(std::move(endpointString), loadFactor, getIpSslTargetNameOverride(), nodeId);
                    addDefault = false;
                }
                if (addDefault) {
                    std::string endpointString =
                        TStringBuilder()
                            << endpoint.address()
                            << ":"
                            << endpoint.port();
                    records.emplace_back(std::move(endpointString), loadFactor, std::move(sslTargetNameOverride), nodeId);
                }
            }
            LastUpdateTime_ = TInstant::Now().MicroSeconds();
            removed = Elector_.SetNewState(std::move(records));
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
        case Ydb::Bridge::PileState::UNSPECIFIED:
        case Ydb::Bridge::PileState_State_PileState_State_INT_MIN_SENTINEL_DO_NOT_USE_:
        case Ydb::Bridge::PileState_State_PileState_State_INT_MAX_SENTINEL_DO_NOT_USE_:
            return EPileState::UNSPECIFIED;
    }
}

} // namespace NYdb
