#pragma once

#include "endpoint_pool.h"

#include <src/client/impl/ydb_internal/internal_header.h>

#include <src/client/impl/ydb_internal/internal_client/client.h>
#include <ydb-cpp-sdk/client/common_client/ssl_credentials.h>
#include <src/client/types/core_facility/core_facility.h>

namespace NYdb::inline V3 {

class ICredentialsProvider;
class ICredentialsProviderFactory;

// Represents state of driver for one particular database
class TDbDriverState
    : public std::enable_shared_from_this<TDbDriverState>
    , public ICoreFacility
{
public:
    enum class ENotifyType : size_t {
        STOP = 0,
        COUNT = 1 // types count
    };

    using TCb = std::function<NThreading::TFuture<void>()>;
    using TPtr = std::shared_ptr<TDbDriverState>;

    TDbDriverState(
        const std::string& database,
        const std::string& discoveryEndpoint,
        EDiscoveryMode discoveryMode,
        const TSslCredentials& sslCredentials,
        IInternalClient* client
    );

    NThreading::TFuture<void> DiscoveryCompleted() const;

    void SignalDiscoveryCompleted();

    void AddPeriodicTask(TPeriodicCb&& cb, TDuration period) override;

    void AddCb(TCb&& cb, ENotifyType type);
    void ForEachEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachLocalEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    void ForEachForeignEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const;
    EBalancingPolicy GetBalancingPolicy() const;
    std::string GetEndpoint() const;
    void SetCredentialsProvider(std::shared_ptr<ICredentialsProvider> credentialsProvider);

    const std::string Database;
    const std::string DiscoveryEndpoint;
    const EDiscoveryMode DiscoveryMode;
    const TSslCredentials SslCredentials;
    std::shared_ptr<ICredentialsProvider> CredentialsProvider;
    IInternalClient* Client;
    TEndpointPool EndpointPool;
    // StopCb allow client to subscribe for notifications from lower layer
    std::mutex NotifyCbsLock;
    std::array<std::vector<TCb>, static_cast<size_t>(ENotifyType::COUNT)> NotifyCbs;
#ifndef YDB_GRPC_UNSECURE_AUTH
    std::shared_ptr<grpc::CallCredentials> CallCredentials;
#endif
    // Status of last discovery call, used in sync mode, coresponding mutex
    std::shared_mutex LastDiscoveryStatusRWLock;
    TPlainStatus LastDiscoveryStatus;
    NSdkStats::TStatCollector StatCollector;
    TLog Log;
    NThreading::TPromise<void> DiscoveryCompletedPromise;
};

// Tracker allows to get driver state by database and credentials
class TDbDriverStateTracker {
    using TStateKey = std::tuple<std::string, std::string, std::string, EDiscoveryMode, TSslCredentials>;
    struct TStateKeyHash {
        size_t operator()(const TStateKey& k) const noexcept {
            THash<std::string> strHash;
            const size_t h0 = strHash(std::get<0>(k));
            const size_t h1 = strHash(std::get<1>(k));
            const size_t h2 = strHash(std::get<2>(k));
            const auto& sslCredentials = std::get<4>(k);
            const size_t h3 = (static_cast<size_t>(std::get<3>(k)) << 1) + static_cast<size_t>(sslCredentials.IsEnabled);
            const size_t h5 = strHash(sslCredentials.CaCert);
            const size_t h6 = strHash(sslCredentials.Cert);
            return (h0 ^ h1 ^ h2 ^ h3 ^ h5 ^ h6);
        }
    };
public:
    TDbDriverStateTracker(IInternalClient* client);
    TDbDriverState::TPtr GetDriverState(
        std::string database,
        std::string DiscoveryEndpoint,
        EDiscoveryMode discoveryMode,
        const TSslCredentials& sslCredentials,
        std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory
    );
    NThreading::TFuture<void> SendNotification(
        TDbDriverState::ENotifyType type);
    void SetMetricRegistry(::NMonitoring::TMetricRegistry *sensorsRegistry);
private:
    IInternalClient* DiscoveryClient_;
    std::unordered_map<TStateKey, std::weak_ptr<TDbDriverState>, TStateKeyHash> States_;
    std::shared_mutex Lock_;
};

using TDbDriverStatePtr = TDbDriverState::TPtr;

} // namespace NYdb
