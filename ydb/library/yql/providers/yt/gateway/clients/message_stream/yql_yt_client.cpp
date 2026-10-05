#include "yql_yt_client.h"
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/ytree/convert.h>
#include <util/generic/yexception.h>

namespace NYql {
NYT::NApi::IClientPtr CreateYtClient(const TString& endpoint, const TString& token) {

    // Strip scheme if present ("http://host:port" → "host:port")
    TString rpcAddress = endpoint;
    if (rpcAddress.StartsWith("http://")) {
        rpcAddress = rpcAddress.substr(7);
    } else if (rpcAddress.StartsWith("https://")) {
        rpcAddress = rpcAddress.substr(8);
    }

    // Use ProxyAddresses to connect directly to the RPC proxy, bypassing HTTP discovery.
    // Also set EnableProxyDiscovery=false to prevent the client from trying to
    // discover proxies via HTTP (/api/v4/discover_proxies) — which would fail in
    // Docker-based test environments where internal RPC addresses are not reachable.
    auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ProxyAddresses = std::make_optional<std::vector<std::string>>(
        std::vector<std::string>{std::string(rpcAddress.data(), rpcAddress.size())});
    connectionConfig->EnableProxyDiscovery = false;

    auto connection = NYT::NApi::NRpcProxy::CreateConnection(connectionConfig);

    NYT::NApi::TClientOptions options;
    options.Token = token;
    auto client = connection->CreateClient(options);

    return client;
}

NThreading::TFuture<bool> IsYtQueue(const NYT::NApi::IClientPtr& client, const TString& path) {
    auto promise = NThreading::NewPromise<bool>();
    client->GetNode(path + "/@").Subscribe(BIND([client, promise](const NYT::TErrorOr<NYT::NYson::TYsonString>& result) mutable {
        try {
            const auto attrs = NYT::NYTree::ConvertToNode(result.ValueOrThrow())->AsMap();
            Y_ENSURE(NYT::NYTree::ConvertTo<std::string>(attrs->GetChildOrThrow("type")) == "table", "YT object is not a table");
            promise.SetValue(attrs->FindChildValue<bool>("dynamic").value_or(false) && !attrs->FindChildValue<bool>("sorted").value_or(false));
        } catch (...) {
            promise.SetException(std::current_exception());
        }
    }));
    return promise.GetFuture();
}

}
