#include "raw_client.h"

#include <yt/cpp/mapreduce/client/client.h>
#include <yt/cpp/mapreduce/client/init.h>

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

NYT::NApi::IClientPtr CreateApiClient(const TClientContext& context)
{
    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();
    if (context.JobProxySocketPath) {
        connectionConfig->ProxyUnixDomainSocket = *context.JobProxySocketPath;
    } else {
        connectionConfig->ClusterUrl = context.ServerName;
    }
    if (context.RpcProxyRole) {
        connectionConfig->ProxyRole = *context.RpcProxyRole;
    }
    if (context.ProxyAddress) {
        connectionConfig->ProxyAddresses = {*context.ProxyAddress};
    }

    THashMap<std::string, std::string> proxyUrlAliasingRules;
    for (const auto& [clusterName, url] : context.Config->ProxyUrlAliasingRules) {
        proxyUrlAliasingRules.emplace(clusterName, url);
    }

    connectionConfig->ProxyUrlAliasingRules = std::move(proxyUrlAliasingRules);

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

    auto connection = NApi::NRpcProxy::CreateConnection(connectionConfig);
    return connection->CreateClient(clientOptions);
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
        NDetail::CreateApiClient(context),
        context.Config);

    return new NDetail::TClient(
        std::move(rawClient),
        context,
        globalTxId,
        CreateDefaultClientRetryPolicy(retryConfigProvider, context.Config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
