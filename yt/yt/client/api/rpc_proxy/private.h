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

inline const NLogging::TLogger RpcProxyClientLogger("RpcProxyClient");

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TString> ParseProxyUrlAliasingRules(TString envConfig);
void ApplyProxyUrlAliasingRules(TString& url);
TString NormalizeHttpProxyUrl(TString url);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
