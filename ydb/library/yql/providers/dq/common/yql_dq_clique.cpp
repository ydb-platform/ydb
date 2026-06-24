#include "yql_dq_clique.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>

namespace NYql {

namespace {

constexpr const char* CliqueValueHint =
    "pragma dq.Clique value must have format <yt_cluster>.<clique_name>: "
    "exactly one dot separating non-empty yt_cluster (without dots) and clique_name";

[[noreturn]] void ThrowInvalidClique(const TString& value) {
    throw yexception() << CliqueValueHint << ", got: \"" << value << "\"";
}

} // namespace

TDqCliqueRef ParseDqCliqueRef(const TString& value) {
    TStringBuf buf(value);
    const auto ytCluster = buf.NextTok('.');
    const auto cliqueName = buf.NextTok('.');
    if (!buf.empty() || ytCluster.empty() || cliqueName.empty()) {
        ThrowInvalidClique(value);
    }
    return TDqCliqueRef{
        .YtCluster = TString(ytCluster),
        .CliqueName = TString(cliqueName),
    };
}

TDqYtClusterResolver MakeYtClusterResolver(const TGatewaysConfig* gatewaysConfig) {
    return [gatewaysConfig](const TString& clusterShortcut) -> TMaybe<TDqYtClusterBinding> {
        if (!gatewaysConfig || !gatewaysConfig->HasYt()) {
            return Nothing();
        }
        for (const auto& mapping : gatewaysConfig->GetYt().GetClusterMapping()) {
            if (mapping.GetName() == clusterShortcut) {
                TDqYtClusterBinding binding;
                binding.Proxy = mapping.GetCluster();
                binding.Token = mapping.GetYTToken();
                return binding;
            }
        }
        return Nothing();
    };
}

TDqYtClusterBinding ResolveYtClusterBindingOrThrow(
    const TDqYtClusterResolver& resolveYtCluster,
    const TString& ytClusterShortcut)
{
    const auto ytBinding = resolveYtCluster(ytClusterShortcut);
    if (!ytBinding) {
        throw yexception()
            << "Unknown YT cluster shortcut \"" << ytClusterShortcut
            << "\" from pragma dq.Clique; check gateways Yt.ClusterMapping";
    }
    if (ytBinding->Proxy.empty()) {
        throw yexception() << "YT cluster \"" << ytClusterShortcut << "\" has empty proxy in gateways config";
    }
    return *ytBinding;
}

void ValidateDqCliqueYtCluster(const TGatewaysConfig* gatewaysConfig, const TString& cliqueValue) {
    const auto cliqueRef = ParseDqCliqueRef(cliqueValue);
    const auto resolveYtCluster = MakeYtClusterResolver(gatewaysConfig);
    ResolveYtClusterBindingOrThrow(resolveYtCluster, cliqueRef.YtCluster);
}

} // namespace NYql
