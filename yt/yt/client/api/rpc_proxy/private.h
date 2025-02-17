#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

// Parameters specific to sticky transactions.
// The separate structure wrapped in |std::optional| helps to differentiate
// between sticky and non-sticky transactions.
struct TStickyTransactionParameters
{
    // Empty if not supported.
    TString ProxyAddress;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConnection)
DECLARE_REFCOUNTED_CLASS(TClientBase)
DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TTransaction)

YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyClientLogger, "RpcProxyClient");

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, std::string> ParseProxyUrlAliasingRules(const TString& envConfig);
void ApplyProxyUrlAliasingRules(
    std::string& url,
    const std::optional<THashMap<std::string, std::string>>& proxyUrlAliasingRules = {});
std::string NormalizeHttpProxyUrl(
    std::string url,
    const std::optional<THashMap<std::string, std::string>>& proxyUrlAliasingRules = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
