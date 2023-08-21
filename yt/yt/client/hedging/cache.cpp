#include "cache.h"
#include "options.h"
#include "rpc.h"

#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

TConfig MakeClusterConfig(const TClustersConfig& clustersConfig, TStringBuf clusterUrl)
{
    auto [cluster, proxyRole] = ExtractClusterAndProxyRole(clusterUrl);
    auto it = clustersConfig.GetClusterConfigs().find(cluster);
    TConfig config = (it != clustersConfig.GetClusterConfigs().end()) ?
        it->second : clustersConfig.GetDefaultConfig();
    config.SetClusterName(ToString(cluster));
    if (!proxyRole.empty()) {
        config.SetProxyRole(ToString(proxyRole));
    }
    return config;
}

namespace {

class TClientsCache
    : public IClientsCache
{
public:
    TClientsCache(const TClustersConfig& config, const NApi::TClientOptions& options)
        : ClustersConfig_(config)
        , Options_(options)
    {}

    NApi::IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        {
            auto guard = ReaderGuard(Lock_);
            auto clientIt = Clients_.find(clusterUrl);
            if (clientIt != Clients_.end()) {
                return clientIt->second;
            }
        }

        auto client = CreateClient(MakeClusterConfig(ClustersConfig_, clusterUrl), Options_);

        auto guard = WriterGuard(Lock_);
        return Clients_.try_emplace(clusterUrl, client).first->second;
    }

private:
    TClustersConfig ClustersConfig_;
    NApi::TClientOptions Options_;
    NThreading::TReaderWriterSpinLock Lock_;
    THashMap<TString, NApi::IClientPtr> Clients_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IClientsCachePtr CreateClientsCache(const TClustersConfig& config, const NApi::TClientOptions& options)
{
    return New<TClientsCache>(config, options);
}

IClientsCachePtr CreateClientsCache(const TConfig& config, const NApi::TClientOptions& options)
{
    TClustersConfig clustersConfig;
    *clustersConfig.MutableDefaultConfig() = config;
    return CreateClientsCache(clustersConfig, options);
}

IClientsCachePtr CreateClientsCache(const TConfig& config)
{
    return CreateClientsCache(config, GetClientOpsFromEnvStatic());
}

IClientsCachePtr CreateClientsCache(const NApi::TClientOptions& options)
{
    return CreateClientsCache(TClustersConfig{}, options);
}

IClientsCachePtr CreateClientsCache()
{
    return CreateClientsCache(GetClientOpsFromEnvStatic());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
