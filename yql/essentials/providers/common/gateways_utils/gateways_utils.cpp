#include "gateways_utils.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <util/generic/maybe.h>

namespace NYql {

void TGatewaySQLFlags::Set(const TString& flag, TVector<TString> args) {
    All_[flag] = std::move(args);
}

void TGatewaySQLFlags::ExtendWith(const TGatewaySQLFlags& flags) {
    Activated_.insert(begin(flags.Activated_), end(flags.Activated_));
    All_.insert(begin(flags.All_), end(flags.All_));
}

NSQLTranslation::TExtendedSqlFlags TGatewaySQLFlags::ToMap(
    NSQLTranslation::TExtendedSqlFlags map,
    bool areOnlyActivated) const {
    for (const auto& [flag, values] : All_) {
        if (areOnlyActivated && !Activated_.contains(flag)) {
            continue;
        }

        map[flag] = values;
    }
    return map;
}

TGatewaySQLFlags TGatewaySQLFlags::From(const TGatewaysConfig& config, const TActivator& isActive) {
    if (!config.HasSqlCore()) {
        return {};
    }

    TGatewaySQLFlags flags;

    {
        const auto& simple = config.GetSqlCore().GetTranslationFlags();
        for (const auto& flag : simple) {
            flags.Set(flag);
        }
    }

    for (const auto& flag : config.GetSqlCore().GetExtendedTranslationFlags()) {
        const auto& name = flag.GetName();

        if (!flag.HasActivation()) {
            // Unconditionally enable
        } else if (isActive(flag.GetActivation())) {
            flags.Activated_.emplace(name);
        } else {
            continue;
        }

        TVector<TString> args(Reserve(flag.GetArgs().size()));
        for (const auto& arg : flag.GetArgs()) {
            args.emplace_back(arg);
        }

        flags.Set(name, std::move(args));
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
    if (!gateways.HasYdb() && gateways.HasKikimr()) {
        AddClusters(gateways.GetKikimr().GetClusterMapping(),
                    TString{KikimrProviderName},
                    &clusterMapping);
    }
}

} // namespace NYql
