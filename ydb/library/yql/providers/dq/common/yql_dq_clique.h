#pragma once

#include <functional>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql {

class TGatewaysConfig;

struct TDqCliqueRef {
    TString YtCluster;
    TString CliqueName;
};

struct TDqYtClusterBinding {
    TString Proxy;
    TString Token;
};

using TDqYtClusterResolver = std::function<TMaybe<TDqYtClusterBinding>(const TString& ytClusterShortcut)>;

TDqCliqueRef ParseDqCliqueRef(const TString& value);

TDqYtClusterResolver MakeYtClusterResolver(const TGatewaysConfig* gatewaysConfig);

TDqYtClusterBinding ResolveYtClusterBindingOrThrow(
    const TDqYtClusterResolver& resolveYtCluster,
    const TString& ytClusterShortcut);

// Validates pragma dq.Clique value: format and YT cluster shortcut in gateways config.
void ValidateDqCliqueYtCluster(const TGatewaysConfig* gatewaysConfig, const TString& cliqueValue);

} // namespace NYql
