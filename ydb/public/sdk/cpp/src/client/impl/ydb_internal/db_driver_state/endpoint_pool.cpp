#define INCLUDE_YDB_INTERNAL_H
#include "endpoint_pool.h"

namespace NYdb::inline V3 {

using std::string;
using std::vector;

TEndpointPool::TEndpointPool(TListEndpointsResultProvider&& provider, const IInternalClient* client)
    : Provider_(provider)
    , LastUpdateTime_(TInstant::Zero().MicroSeconds())
    , BalancingSettings_(client->GetBalancingSettings())
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
        vector<string> removed;
        if (result.DiscoveryStatus.Status == EStatus::SUCCESS) {
            vector<TEndpointRecord> records;
            // Is used to convert float to integer load factor
            // same integer values will be selected randomly.
            const float multiplicator = 10.0;
            const auto& preferredLocation = GetPreferredLocation(result.Result.self_location());
            for (const auto& endpoint : result.Result.endpoints()) {
                i32 loadFactor = (i32)(multiplicator * Min(LoadMax, Max(LoadMin, endpoint.load_factor())));
                ui64 nodeId = endpoint.node_id();
                if (BalancingSettings_.Policy != EBalancingPolicy::UseAllNodes) {
                    if (endpoint.location() != preferredLocation) {
                        // Location missmatch, shift this endpoint
                        loadFactor += GetLocalityShift();
                    }
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

void TEndpointPool::BanEndpoint(const string& endpoint) {
    Elector_.PessimizeEndpoint(endpoint);
}

int TEndpointPool::GetPessimizationRatio() {
    return Elector_.GetPessimizationRatio();
}

bool TEndpointPool::LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag) {
    return Elector_.LinkObjToEndpoint(endpoint, obj, tag);
}

void TEndpointPool::ForEachEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    return Elector_.ForEachEndpoint(cb, 0, Max<i32>(), tag);
}

void TEndpointPool::ForEachLocalEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    return Elector_.ForEachEndpoint(cb, 0, GetLocalityShift() - 1, tag);
}

void TEndpointPool::ForEachForeignEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    return Elector_.ForEachEndpoint(cb, GetLocalityShift(), Max<i32>() - 1, tag);
}

EBalancingPolicy TEndpointPool::GetBalancingPolicy() const {
    return BalancingSettings_.Policy;
}

void TEndpointPool::SetStatCollector(NSdkStats::TStatCollector& statCollector) {
    if (!statCollector.IsCollecting())
        return;
    Elector_.SetStatCollector(statCollector.GetEndpointElectorStatCollector());
    StatCollector_ = &statCollector;
}

constexpr i32 TEndpointPool::GetLocalityShift() {
    return LoadMax * Multiplicator;
}

string TEndpointPool::GetPreferredLocation(const string& selfLocation) {
    switch (BalancingSettings_.Policy) {
        case EBalancingPolicy::UseAllNodes:
            return {};
        case EBalancingPolicy::UsePreferableLocation:
            if (BalancingSettings_.PolicyParams.empty()) {
                return selfLocation;
            } else {
                return BalancingSettings_.PolicyParams;
            }
    }
    return {};
}

} // namespace NYdb
