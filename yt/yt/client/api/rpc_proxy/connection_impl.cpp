#include "connection_impl.h"
#include "discovery_service_proxy.h"
#include "connection_impl.h"
#include "client_impl.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/net/local_address.h>
#include <yt/yt/core/net/address.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/roaming_channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/dynamic_channel_pool.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/peer_discovery.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/service_discovery/service_discovery.h>

#include <yt/yt/build/ya_version.h>

#include <util/system/env.h>

namespace NYT::NApi::NRpcProxy {

using namespace NBus;
using namespace NRpc;
using namespace NNet;
using namespace NHttp;
using namespace NHttps;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NServiceDiscovery;

////////////////////////////////////////////////////////////////////////////////

static const std::string ProxyUrlCanonicalHttpPrefix = "http://";
static const std::string ProxyUrlCanonicalHttpsPrefix = "https://";
static const std::string ProxyUrlCanonicalSuffix = ".yt.yandex.net";

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, std::string> ParseProxyUrlAliasingRules(const TString& envConfig)
{
    if (envConfig.empty()) {
        return {};
    }
    return ConvertTo<THashMap<std::string, std::string>>(TYsonString(envConfig));
}

void ApplyProxyUrlAliasingRules(std::string& url, const std::optional<THashMap<std::string, std::string>>& proxyUrlAliasingRules)
{
    static const auto rulesFromEnv = ParseProxyUrlAliasingRules(GetEnv("YT_PROXY_URL_ALIASING_CONFIG"));

    const auto& rules = proxyUrlAliasingRules.value_or(rulesFromEnv);

    if (auto ruleIt = rules.find(url); ruleIt != rules.end()) {
        url = ruleIt->second;
    }
}

std::string NormalizeHttpProxyUrl(std::string url, const std::optional<THashMap<std::string, std::string>>& proxyUrlAliasingRules)
{
    ApplyProxyUrlAliasingRules(url, proxyUrlAliasingRules);

    if (url.find('.') == TString::npos &&
        url.find(':') == TString::npos &&
        url.find("localhost") == TString::npos)
    {
        url.append(ProxyUrlCanonicalSuffix);
    }

    if (!url.starts_with(ProxyUrlCanonicalHttpPrefix) && !url.starts_with(ProxyUrlCanonicalHttpsPrefix)) {
        url = ProxyUrlCanonicalHttpPrefix + url;
    }

    return url;
}

namespace {

bool IsProxyUrlSecure(const std::string& url)
{
    return url.starts_with(ProxyUrlCanonicalHttpsPrefix);
}

std::string MakeConnectionLoggingTag(const TConnectionConfigPtr& config, TGuid connectionId)
{
    TStringBuilder builder;
    TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
    if (config->ClusterUrl) {
        delimitedBuilder->AppendFormat("ClusterUrl: %v", *config->ClusterUrl);
    }
    if (config->ProxyRole) {
        delimitedBuilder->AppendFormat("ProxyRole: %v", *config->ProxyRole);
    }
    delimitedBuilder->AppendFormat("ConnectionId: %v", connectionId);
    return builder.Flush();
}

TString MakeEndpointDescription(const TConnectionConfigPtr& config, TGuid connectionId)
{
    return Format("Rpc{%v}", MakeConnectionLoggingTag(config, connectionId));
}

IAttributeDictionaryPtr MakeErrorAttributes(const TConnectionConfigPtr& config)
{
    auto attributes = CreateEphemeralAttributes();
    if (config->ProxyEndpoints) {
        attributes->Set("endpoint_set_cluster", config->ProxyEndpoints->Cluster);
        attributes->Set("endpoint_set_id", config->ProxyEndpoints->EndpointSetId);
    }
    if (config->ClusterUrl) {
        attributes->Set("cluster_url", config->ClusterUrl);
    }
    attributes->Set("proxy_role", config->ProxyRole.value_or(DefaultRpcProxyRole));
    return attributes;
}

IAttributeDictionaryPtr MakeEndpointAttributes(const TConnectionConfigPtr& config, TGuid connectionId)
{
    auto attributes = MakeErrorAttributes(config);
    attributes->Set("rpc_proxy", true);
    attributes->Set("connection_id", connectionId);
    return attributes;
}

TString MakeConnectionClusterId(const TConnectionConfigPtr& config)
{
    if (config->ClusterName) {
        return Format("Rpc(Name=%v)", *config->ClusterName);
    } else if (config->ClusterUrl) {
        return Format("Rpc(Url=%v)", *config->ClusterUrl);
    } else {
        return Format("Rpc(ProxyAddresses=%v)", config->ProxyAddresses);
    }
}

class TProxyChannelProvider
    : public IRoamingChannelProvider
{
public:
    TProxyChannelProvider(
        TConnectionConfigPtr config,
        TGuid connectionId,
        TDynamicChannelPoolPtr pool,
        bool sticky)
        : Pool_(std::move(pool))
        , Sticky_(sticky)
        , EndpointDescription_(MakeEndpointDescription(config, connectionId))
        , EndpointAttributes_(MakeEndpointAttributes(config, connectionId))
    { }

    const std::string& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        if (Sticky_) {
            auto guard = Guard(SpinLock_);
            if (!Channel_) {
                Channel_ = Pool_->GetRandomChannel();
            }
            return Channel_;
        } else {
            return Pool_->GetRandomChannel();
        }
    }

    void Terminate(const TError& /*error*/) override
    { }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

private:
    const TDynamicChannelPoolPtr Pool_;
    const bool Sticky_;
    const TGuid ConnectionId_;

    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TFuture<IChannelPtr> Channel_;
};

TConnectionConfigPtr GetPostprocessedConfigAndValidate(TConnectionConfigPtr config)
{
    config->Postprocess();
    ValidateConnectionConfig(config);
    return config;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TConnection::TConnection(TConnectionConfigPtr config, TConnectionOptions options)
    : Config_(GetPostprocessedConfigAndValidate(std::move(config)))
    , ConnectionId_(TGuid::Create())
    , LoggingTag_(MakeConnectionLoggingTag(Config_, ConnectionId_))
    , ClusterId_(MakeConnectionClusterId(Config_))
    , Logger(RpcProxyClientLogger().WithRawTag(LoggingTag_))
    , ChannelFactory_(Config_->ProxyUnixDomainSocket
        ? NRpc::NBus::CreateUdsBusChannelFactory(Config_->BusClient)
        : NRpc::NBus::CreateTcpBusChannelFactory(Config_->BusClient))
    , CachingChannelFactory_(CreateCachingChannelFactory(
        ChannelFactory_,
        Config_->IdleChannelTtl))
    , ChannelPool_(New<TDynamicChannelPool>(
        Config_->DynamicChannelPool,
        ChannelFactory_,
        MakeEndpointDescription(Config_, ConnectionId_),
        MakeEndpointAttributes(Config_, ConnectionId_),
        TApiServiceProxy::GetDescriptor().ServiceName,
        CreateDefaultPeerDiscovery()))
    , ActionQueue_(options.ConnectionInvoker ? nullptr : New<TActionQueue>("RpcProxyConn"))
    , ConnectionInvoker_(options.ConnectionInvoker ? options.ConnectionInvoker : ActionQueue_->GetInvoker())
    , UpdateProxyListBackoffStrategy_(TExponentialBackoffOptions{
        .InvocationCount = std::numeric_limits<int>::max(),
        .MinBackoff = Config_->ProxyListRetryPeriod,
        .MaxBackoff = Config_->MaxProxyListRetryPeriod,
    })
    , ServiceDiscovery_(Config_->EnableProxyDiscovery && Config_->ProxyEndpoints
        ? NRpc::TDispatcher::Get()->GetServiceDiscovery()
        : nullptr)
{
    if (Config_->EnableProxyDiscovery && Config_->ProxyEndpoints && !ServiceDiscovery_) {
        ChannelPool_->SetPeerDiscoveryError(TError("No Service Discovery is configured"));
        return;
    }

    if (Config_->ProxyAddresses) {
        ChannelPool_->SetPeers(*Config_->ProxyAddresses);
    }

    if (Config_->ProxyUnixDomainSocket) {
        ChannelPool_->SetPeers({*Config_->ProxyUnixDomainSocket});
    }
}

TConnection::~TConnection()
{
    RunNoExcept([&] {
        Terminate();
    });
}

IChannelPtr TConnection::CreateChannel(bool sticky)
{
    auto provider = New<TProxyChannelProvider>(
        Config_,
        ConnectionId_,
        ChannelPool_,
        sticky);
    return CreateRoamingChannel(std::move(provider));
}

IChannelPtr TConnection::CreateChannelByAddress(const std::string& address)
{
    return CachingChannelFactory_->CreateChannel(address);
}

TClusterTag TConnection::GetClusterTag() const
{
    THROW_ERROR_EXCEPTION_UNLESS(Config_->ClusterTag,
        "Cluster tag is not specified in connection config; please set \"cluster_tag\"");
    return *Config_->ClusterTag;
}

const std::string& TConnection::GetLoggingTag() const
{
    return LoggingTag_;
}

const std::string& TConnection::GetClusterId() const
{
    return ClusterId_;
}

const std::optional<std::string>& TConnection::GetClusterName() const
{
    return Config_->ClusterName;
}

bool TConnection::IsSameCluster(const IConnectionPtr& other) const
{
    // NB: Cluster tag is not defined for RPC proxy connection
    // so we use some best-effort logic here.
    return GetClusterId() == other->GetClusterId();
}

IInvokerPtr TConnection::GetInvoker()
{
    return ConnectionInvoker_;
}

NApi::IClientPtr TConnection::CreateClient(const TClientOptions& options)
{
    if (options.Token) {
        DiscoveryToken_.Store(*options.Token);
    }

    if (Config_->EnableProxyDiscovery && (Config_->ClusterUrl || Config_->ProxyEndpoints)) {
        if (!ProxyListUpdateStarted_.exchange(true)) {
            ScheduleProxyListUpdate(TDuration::Zero());
        }
    }

    return New<TClient>(this, options);
}

NHiveClient::ITransactionParticipantPtr TConnection::CreateTransactionParticipant(
    NHiveClient::TCellId /*cellId*/,
    const TTransactionParticipantOptions& /*options*/)
{
    YT_UNIMPLEMENTED();
}

void TConnection::ClearMetadataCaches()
{ }

void TConnection::Terminate()
{
    YT_LOG_DEBUG("Terminating connection");
    Terminated_ = true;
    ChannelPool_->Terminate(TError("Connection terminated"));
}

bool TConnection::IsTerminated() const
{
    return Terminated_;
}

const TConnectionConfigPtr& TConnection::GetConfig()
{
    return Config_;
}

std::vector<std::string> TConnection::DiscoverProxiesViaHttp()
{
    auto correlationId = TGuid::Create();

    try {
        YT_LOG_DEBUG("Updating proxy list via HTTP (CorrelationId: %v)", correlationId);

        auto poller = TTcpDispatcher::Get()->GetXferPoller();
        auto headers = New<THeaders>();
        SetUserAgent(headers, GetRpcUserAgent());
        if (auto token = DiscoveryToken_.Load(); !token.empty()) {
            headers->Add("Authorization", "OAuth " + token);
        }
        headers->Add("X-YT-Correlation-Id", ToString(correlationId));
        headers->Add("X-YT-Header-Format", "<format=text>yson");
        headers->Add(
            "X-YT-Parameters",
            BuildYsonStringFluently(EYsonFormat::Text)
                .BeginMap()
                    .Item("output_format")
                    .BeginAttributes()
                        .Item("format").Value("text")
                    .EndAttributes()
                    .Value("yson")
                    .OptionalItem("role", Config_->ProxyRole)
                    .OptionalItem("address_type", Config_->ProxyAddressType)
                    .OptionalItem("network_name", Config_->ProxyNetworkName)
                .EndMap().ToString());

        auto url = NormalizeHttpProxyUrl(*Config_->ClusterUrl) + "/api/v4/discover_proxies";
        auto client = IsProxyUrlSecure(*Config_->ClusterUrl)
            ? NHttps::CreateClient(Config_->HttpsClient, std::move(poller))
            : NHttp::CreateClient(Config_->HttpClient, std::move(poller));
        // TODO(babenko): switch to std::string
        auto rsp = WaitFor(client->Get(TString(url), headers))
            .ValueOrThrow();

        if (rsp->GetStatusCode() != EStatusCode::OK) {
            THROW_ERROR_EXCEPTION("HTTP proxy discovery request returned an error")
                << TErrorAttribute("correlation_id", correlationId)
                << TErrorAttribute("status_code", rsp->GetStatusCode())
                << ParseYTError(rsp);
        }

        auto body = rsp->ReadAll();
        YT_LOG_DEBUG("Received proxy list via HTTP (CorrelationId: %v)", correlationId);

        auto node = ConvertTo<INodePtr>(TYsonString(ToString(body)));
        node = node->AsMap()->FindChild("proxies");
        return ConvertTo<std::vector<std::string>>(node);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error discovering RPC proxies via HTTP")
            << TErrorAttribute("correlation_id", correlationId)
            << ex;
    }
}

std::vector<std::string> TConnection::DiscoverProxiesViaServiceDiscovery()
{
    YT_LOG_DEBUG("Updating proxy list via Service Discovery");

    if (!ServiceDiscovery_) {
        THROW_ERROR_EXCEPTION("No service discovery configured");
    }

    const auto& clusters = Config_->ProxyEndpoints->Clusters;
    std::vector<TFuture<TEndpointSet>> asyncEndpointSets;
    asyncEndpointSets.reserve(clusters.size());
    for (const auto& cluster : clusters) {
        asyncEndpointSets.push_back(ServiceDiscovery_->ResolveEndpoints(
            cluster,
            Config_->ProxyEndpoints->EndpointSetId));
    }

    auto endpointSets = WaitFor(AllSet(asyncEndpointSets))
        .ValueOrThrow();

    std::vector<std::string> allAddresses;
    std::vector<TError> errors;
    for (int i = 0; i < std::ssize(endpointSets); ++i) {
        if (!endpointSets[i].IsOK()) {
            errors.push_back(endpointSets[i]);
            YT_LOG_WARNING(
                endpointSets[i],
                "Could not resolve endpoints from cluster (Cluster: %v, EndpointSetId: %v)",
                clusters[i],
                Config_->ProxyEndpoints->EndpointSetId);
            continue;
        }

        auto addresses = AddressesFromEndpointSet(endpointSets[i].Value());
        allAddresses.insert(allAddresses.end(), addresses.begin(), addresses.end());
    }

    if (errors.size() == endpointSets.size()) {
        THROW_ERROR_EXCEPTION("Error discovering RPC proxies via Service Discovery") << errors;
    }

    return allAddresses;
}

void TConnection::ScheduleProxyListUpdate(TDuration delay)
{
    TDelayedExecutor::Submit(
        BIND(&TConnection::OnProxyListUpdate, MakeWeak(this)),
        delay,
        GetInvoker());
}

void TConnection::OnProxyListUpdate()
{
    if (Terminated_.load()) {
        return;
    }

    try {
        YT_LOG_DEBUG("Updating proxy list");

        auto proxies = [&] {
            if (Config_->ProxyEndpoints) {
                return DiscoverProxiesViaServiceDiscovery();
            } else if (Config_->ClusterUrl) {
                return DiscoverProxiesViaHttp();
            } else {
                YT_ABORT();
            }
        }();

        if (proxies.empty()) {
            THROW_ERROR_EXCEPTION("Proxy list is empty");
        }

        ChannelPool_->SetPeers(proxies);

        UpdateProxyListBackoffStrategy_.Restart();

        ScheduleProxyListUpdate(
            TPeriodicExecutorOptions::WithJitter(Config_->ProxyListUpdatePeriod)
                .GenerateDelay());
    } catch (const std::exception& ex) {
        UpdateProxyListBackoffStrategy_.Next();
        int attempt = UpdateProxyListBackoffStrategy_.GetInvocationIndex() % Config_->MaxProxyListUpdateAttempts;
        if (attempt == 0) {
            ChannelPool_->SetPeerDiscoveryError(TError(ex) << *MakeErrorAttributes(Config_));
        }

        auto backoff = UpdateProxyListBackoffStrategy_.GetBackoff();
        YT_LOG_WARNING(ex, "Error updating proxy list, backing off and retrying (Backoff: %v)",
            backoff);
        ScheduleProxyListUpdate(backoff);
    }
}

NYson::TYsonString TConnection::GetConfigYson() const
{
    return ConvertToYsonString(Config_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
