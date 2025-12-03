#include "gateways_utils.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

void TGatewaySQLFlags::CollectAllTo(THashSet<TString>& target) const {
    target.insert(begin(Unconditional), end(Unconditional));
    target.insert(begin(Activated), end(Activated));
}

THashSet<TString> TGatewaySQLFlags::All() const {
    THashSet<TString> all(Unconditional.size() + Activated.size());
    CollectAllTo(all);
    return all;
}

void TGatewaySQLFlags::ExtendWith(const TGatewaySQLFlags& flags) {
    Unconditional.insert(begin(flags.Unconditional), end(flags.Unconditional));
    Activated.insert(begin(flags.Activated), end(flags.Activated));
}

TGatewaySQLFlags TGatewaySQLFlags::From(const TGatewaysConfig& config, const TActivator& isActive) {
    if (!config.HasSqlCore()) {
        return {};
    }

    TGatewaySQLFlags flags;

    {
        const auto& simple = config.GetSqlCore().GetTranslationFlags();
        flags.Unconditional.insert(begin(simple), end(simple));
    }

    for (const auto& flag : config.GetSqlCore().GetExtendedTranslationFlags()) {
        const auto& name = flag.GetName();
        YQL_ENSURE(flag.GetArgs().empty(), "Expected an empty SQL flag args");

        if (!flag.HasActivation()) {
            flags.Unconditional.emplace(name);
        } else if (isActive(flag.GetActivation())) {
            flags.Activated.emplace(name);
        }
    }

    return flags;
}

// We are in a testing environment, all features should be turned on.
TGatewaySQLFlags TGatewaySQLFlags::FromTesting(const TGatewaysConfig& config) {
    return From(config, [](const TActivationPercentage&) { return true; });
}

void GetClusterMappingFromGateways(const NYql::TGatewaysConfig& gateways, THashMap<TString, TString>& clusterMapping) {
    clusterMapping.clear();
    clusterMapping["pg_catalog"] = PgProviderName;
    clusterMapping["information_schema"] = PgProviderName;
    if (gateways.HasYt()) {
        AddClusters(gateways.GetYt().GetClusterMapping(),
                    TString{YtProviderName},
                    &clusterMapping);
    }
    if (gateways.HasClickHouse()) {
        AddClusters(gateways.GetClickHouse().GetClusterMapping(),
                    TString{ClickHouseProviderName},
                    &clusterMapping);
    }
    if (gateways.HasS3()) {
        AddClusters(gateways.GetS3().GetClusterMapping(),
                    TString{S3ProviderName},
                    &clusterMapping);
    }
    if (gateways.HasYdb() && !gateways.HasKikimr()) {
        AddClusters(gateways.GetYdb().GetClusterMapping(),
                    TString{YdbProviderName},
                    &clusterMapping);
    }
}

} // namespace NYql
