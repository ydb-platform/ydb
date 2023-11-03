#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_client/client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_endpoints/endpoints.h>

#include <library/cpp/threading/future/future.h>

#include <mutex>

namespace NYdb {

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
    TEndpointPool(TListEndpointsResultProvider&& provider, const IInternalClient* client);
    ~TEndpointPool();
    std::pair<NThreading::TFuture<TEndpointUpdateResult>, bool> UpdateAsync();
    TEndpointRecord GetEndpoint(const TEndpointKey& preferredEndpoint, bool onlyPreferred = false) const;
    TDuration TimeSinceLastUpdate() const;
    void BanEndpoint(const std::string& endpoint);
    int GetPessimizationRatio();
    bool LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag);
    void ForEachEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachLocalEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachForeignEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    EBalancingPolicy GetBalancingPolicy() const;
    // TODO: Remove this mess
    void SetStatCollector(NSdkStats::TStatCollector& statCollector);
    static constexpr i32 GetLocalityShift();

private:
    std::string GetPreferredLocation(const std::string& selfLocation);

private:
    TListEndpointsResultProvider Provider_;
    std::mutex Mutex_;
    TEndpointElectorSafe Elector_;
    NThreading::TPromise<TEndpointUpdateResult> DiscoveryPromise_;
    std::atomic_uint64_t LastUpdateTime_;
    const TBalancingSettings BalancingSettings_;

    NSdkStats::TStatCollector* StatCollector_ = nullptr;

    // Max, min load factor returned by discovery service
    static constexpr float LoadMax = 100.0;
    static constexpr float LoadMin = -100.0;
    // Is used to convert float to integer load factor
    // same integer values will be selected randomly.
    static constexpr float Multiplicator = 10.0;
};

} // namespace NYdb
