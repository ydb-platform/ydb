#include "raw_client.h"

#include <yt/cpp/mapreduce/client/client.h>
#include <yt/cpp/mapreduce/client/init.h>

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionCacheKey
{
public:
    TMaybe<TString> JobProxySocketPath;
    TString ClusterUrl;
    TMaybe<TString> RpcProxyRole;
    TMaybe<TString> ProxyAddress;

    // N.B. we want to compute hash of this struct, so we use sorted map.
    TMap<TString, TString> ProxyUrlAliasingRules;

    bool IsHeavy = false;

public:
    TConnectionCacheKey() = default;

    TConnectionCacheKey(const TClientContext& context, bool isHeavy = false)
        : JobProxySocketPath(context.JobProxySocketPath)
        , ClusterUrl(context.ServerName)
        , RpcProxyRole(context.RpcProxyRole)
        , ProxyAddress(context.ProxyAddress)
        , ProxyUrlAliasingRules(context.Config->ProxyUrlAliasingRules.begin(), context.Config->ProxyUrlAliasingRules.end())
        , IsHeavy(isHeavy)
    { }

    bool operator==(const TConnectionCacheKey& other) const = default;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail

template <>
struct THash<NYT::NDetail::TConnectionCacheKey>
{
    size_t operator()(const NYT::NDetail::TConnectionCacheKey& key) const
    {
        using NYT::HashCombine;
        size_t result = 0;
        HashCombine(result, key.JobProxySocketPath);
        HashCombine(result, key.ClusterUrl);
        HashCombine(result, key.RpcProxyRole);
        HashCombine(result, key.ProxyAddress);
        for (const auto& [k, v] : key.ProxyUrlAliasingRules) {
            HashCombine(result, k);
            HashCombine(result, v);
        }
        HashCombine(result, key.IsHeavy);
        return result;
    }
};

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

NApi::IConnectionPtr GetOrCreateConnection(const TConnectionCacheKey& key)
{
    static TMutex lock;
    static THashMap<TConnectionCacheKey, TWeakPtr<NApi::IConnection>> cache;
    static size_t maxCacheSize = 64;

    auto g = Guard(lock);

    auto it = cache.find(key);
    if (it != cache.end()) {
        auto connection = it->second.Lock();
        if (connection) {
            return connection;
        }
    }

    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();
    if (key.JobProxySocketPath) {
        connectionConfig->ProxyUnixDomainSocket = *key.JobProxySocketPath;
        connectionConfig->EnableProxyDiscovery = false;
    } else {
        connectionConfig->ClusterUrl = key.ClusterUrl;
    }
    if (key.RpcProxyRole) {
        connectionConfig->ProxyRole = *key.RpcProxyRole;
    }
    if (key.ProxyAddress) {
        connectionConfig->ProxyAddresses = {*key.ProxyAddress};
    }

    for (const auto& [clusterName, url] : key.ProxyUrlAliasingRules) {
        connectionConfig->ProxyUrlAliasingRules.emplace(clusterName, url);
    }

    auto connection = NApi::NRpcProxy::CreateConnection(connectionConfig);

    if (it != cache.end()) {
        it->second = connection;
    } else if (cache.size() < maxCacheSize) {
        // N.B. once cache size exceeds limit we disable caching.
        // We believe such cases are rare but if it is the case we don't want to leak memory
        // (memory is leaked since we never clear the cache).
        cache.emplace(key, connection);
    } else {
        YT_LOG_WARNING("Cannot cache IConnection since connection cache reached maximum size");
    }

    return connection;
}

static NApi::IClientPtr CreateApiClientImpl(const TClientContext& context, bool isHeavy)
{
    auto key = TConnectionCacheKey(context, isHeavy);
    auto connection = GetOrCreateConnection(key);

    NApi::TClientOptions clientOptions;
    clientOptions.Token = context.Token;
    if (context.ServiceTicketAuth) {
        clientOptions.ServiceTicketAuth = context.ServiceTicketAuth->Ptr;
    }
    if (context.ImpersonationUser) {
        clientOptions.User = *context.ImpersonationUser;
    }
    if (context.JobProxySocketPath) {
        clientOptions.MultiproxyTargetCluster = context.MultiproxyTargetCluster;
    }

    return connection->CreateClient(clientOptions);
}

TApiClients CreateApiClients(const TClientContext& context)
{
    return {
        .Light = CreateApiClientImpl(context, /*isHeavy*/ false),
        .Heavy = CreateApiClientImpl(context, /*isHeavy*/ true),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateRpcClient(
    const TString& serverName,
    const TCreateClientOptions& options)
{
    auto context = NDetail::CreateClientContext(serverName, options);

    auto globalTxId = GetGuid(context.Config->GlobalTxId);

    auto retryConfigProvider = options.RetryConfigProvider_;
    if (!retryConfigProvider) {
        retryConfigProvider = CreateDefaultRetryConfigProvider();
    }

    NDetail::EnsureInitialized();

    auto rawClient = MakeIntrusive<NDetail::TRpcRawClient>(
        NDetail::CreateApiClients(context),
        context.Config);

    return new NDetail::TClient(
        std::move(rawClient),
        context,
        globalTxId,
        CreateDefaultClientRetryPolicy(retryConfigProvider, context.Config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
