#include "gateways_utils.h"

#include <yql/essentials/providers/common/activation/yql_activation.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/proto/static_gateways_config.pb.h>
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
    const NConfig::TActivationGroupRegistry activationGroups(config);

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
        } else if (isActive(activationGroups.Resolve(flag.GetActivation()))) {
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

void SyncWithStaticGateways(TStaticGatewaysConfig& staticGateways, TGatewaysConfig& gateways) {
    if (gateways.HasYt()) {
        if (!staticGateways.GetYt().HasMrJobBin() && gateways.GetYt().HasMrJobBin()) {
            staticGateways.MutableYt()->SetMrJobBin(gateways.GetYt().GetMrJobBin());
        }
        if (!staticGateways.GetYt().HasMrJobUdfsDir() && gateways.GetYt().HasMrJobUdfsDir()) {
            staticGateways.MutableYt()->SetMrJobUdfsDir(gateways.GetYt().GetMrJobUdfsDir());
        }
        if (!staticGateways.GetYt().HasYtDebugLogFile() && gateways.GetYt().HasYtDebugLogFile()) {
            staticGateways.MutableYt()->SetYtDebugLogFile(gateways.GetYt().GetYtDebugLogFile());
        }
        if (!staticGateways.GetYt().HasMrJobBinMd5() && gateways.GetYt().HasMrJobBinMd5()) {
            staticGateways.MutableYt()->SetMrJobBinMd5(gateways.GetYt().GetMrJobBinMd5());
        }
        if (staticGateways.GetYt().MrJobSystemLibsWithMd5Size() == 0 && gateways.GetYt().MrJobSystemLibsWithMd5Size() != 0) {
            auto* staticSysLibs = staticGateways.MutableYt()->MutableMrJobSystemLibsWithMd5();
            for (const auto& entry : gateways.GetYt().GetMrJobSystemLibsWithMd5()) {
                TStaticFileWithMd5 staticEntry;
                staticEntry.SetFile(entry.GetFile());
                staticEntry.SetMd5(entry.GetMd5());
                staticSysLibs->Add(std::move(staticEntry));
            }
        }
    }

    if (gateways.HasRtmr()) {
        if (!staticGateways.GetRtmr().HasYqlRtmrDynLib() && gateways.GetRtmr().HasYqlRtmrDynLib()) {
            staticGateways.MutableRtmr()->SetYqlRtmrDynLib(gateways.GetRtmr().GetYqlRtmrDynLib());
        }

        if (staticGateways.GetRtmr().ArtifactsSize() == 0 && gateways.GetRtmr().ArtifactsSize() != 0) {
            auto* staticArtifacts = staticGateways.MutableRtmr()->MutableArtifacts();
            for (const auto& entry : gateways.GetRtmr().GetArtifacts()) {
                staticArtifacts->Add(TString(entry));
            }
        }
    }

    // remove all static settings from dynamic config
    if (gateways.HasYt()) {
        gateways.MutableYt()->ClearMrJobBin();
        gateways.MutableYt()->ClearYtDebugLogFile();
        gateways.MutableYt()->ClearMrJobBinMd5();
        gateways.MutableYt()->ClearMrJobSystemLibsWithMd5();
    }

    if (gateways.HasRtmr()) {
        gateways.MutableRtmr()->ClearYqlRtmrDynLib();
        gateways.MutableRtmr()->ClearArtifacts();
    }
}

} // namespace NYql
