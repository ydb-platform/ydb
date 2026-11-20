#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/balancing_policies.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_client/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/src/client/impl/endpoints/endpoints.h>

#include <library/cpp/threading/future/future.h>

#include <mutex>

namespace NYdb::inline Dev {

struct TListEndpointsResult {
    Ydb::Discovery::ListEndpointsResult Result;
    TPlainStatus DiscoveryStatus;
};

using TAsyncListEndpointsResult = NThreading::TFuture<TListEndpointsResult>;
using TListEndpointsResultProvider = std::function<TAsyncListEndpointsResult()>;

struct TEndpointUpdateResult {
    std::vector<std::string> Removed;
    TPlainStatus DiscoveryStatus;
};

class TEndpointPool {
public:
    enum class EUpdateReason { Regular, TransportFailure };

    TEndpointPool(TListEndpointsResultProvider&& provider, IInternalClient* client);
    ~TEndpointPool();
    std::pair<NThreading::TFuture<TEndpointUpdateResult>, bool> UpdateAsync(EUpdateReason reason = EUpdateReason::Regular);
    NThreading::TFuture<TEndpointUpdateResult> GetPendingTransportUpdate();
    TEndpointRecord GetEndpoint(const TEndpointKey& preferredEndpoint, bool onlyPreferred = false) const;
    TDuration TimeSinceLastUpdate() const;
    void BanEndpoint(const std::string& endpoint);
    void BanNodeId(std::uint64_t nodeId);
    int GetPessimizationRatio();
    bool LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag);
    void ForEachEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachLocalEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachForeignEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    TBalancingPolicy::TImpl::EPolicyType GetBalancingPolicyType() const;
    // TODO: Remove this mess
    void SetStatCollector(NSdkStats::TStatCollector& statCollector);
    static constexpr std::int32_t GetLocalityShift();

private:
    NThreading::TFuture<void> RunUpdates();
    std::vector<std::string> Install(const Ydb::Discovery::ListEndpointsResult& result, TEndpointElectorSafe::EPenalties penalties);
    bool IsPreferredEndpoint(const Ydb::Discovery::EndpointInfo& endpoint,
                             const std::string& selfLocation,
                             const std::unordered_map<std::string, Ydb::Bridge::PileState>& pileStates) const;
    EPileState GetPileState(const Ydb::Bridge::PileState::State& state) const;

private:
    TListEndpointsResultProvider Provider_;
    IInternalClient* const Client_;
    std::mutex Mutex_;
    TEndpointElectorSafe Elector_;
    NThreading::TPromise<TEndpointUpdateResult> DiscoveryPromise_;
    std::atomic_bool TransportUpdatePending_ = false;
    std::uint64_t TransportGeneration_ = 0;
    TDeadline NextTransportUpdate_;
    std::atomic_uint64_t LastUpdateTime_;
    const TBalancingPolicy::TImpl BalancingPolicy_;

    NSdkStats::TStatCollector* StatCollector_ = nullptr;

    // Max, min load factor returned by discovery service
    static constexpr float LoadMax = 100.0;
    static constexpr float LoadMin = -100.0;
    // Is used to convert float to integer load factor
    // same integer values will be selected randomly.
    static constexpr float Multiplicator = 10.0;
};

} // namespace NYdb
