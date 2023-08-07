#pragma once

#include <ydb/core/fq/libs/config/protos/compute.pb.h>

#include <util/digest/multi.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NFq {

class TComputeConfig {
public:
    explicit TComputeConfig(const NFq::NConfig::TComputeConfig& computeConfig)
        : ComputeConfig(computeConfig)
        , DefaultCompute(ComputeConfig.GetDefaultCompute() != NFq::NConfig::EComputeType::UNKNOWN
                             ? ComputeConfig.GetDefaultCompute()
                             : NFq::NConfig::EComputeType::IN_PLACE) {
        for (const auto& mapping : ComputeConfig.GetComputeMapping()) {
            if (mapping.HasActivation()) {
                auto percentage = mapping.GetActivation().GetPercentage();
                Y_ENSURE(percentage == 0 || percentage == 100,
                         "Activation percentage must be either 0 or 100");
            }
        }
    }

    NFq::NConfig::EComputeType GetComputeType(const FederatedQuery::QueryContent::QueryType queryType, const TString& scope) const {
        for (const auto& mapping : ComputeConfig.GetComputeMapping()) {
            if (mapping.GetQueryType() == queryType) {
                if (mapping.HasActivation()) {
                    const auto& activation    = mapping.GetActivation();
                    const auto& includeScopes = activation.GetIncludeScopes();
                    const auto& excludeScopes = activation.GetExcludeScopes();
                    auto isActivatedCase1 =
                        activation.GetPercentage() == 0 &&
                        Find(includeScopes, scope) == includeScopes.end();
                    auto isActivatedCase2 =
                        activation.GetPercentage() == 100 &&
                        Find(excludeScopes, scope) != excludeScopes.end();
                    if (isActivatedCase1 || isActivatedCase2) {
                        return mapping.GetCompute();
                    }
                }
                return mapping.GetCompute();
            }
        }

        return DefaultCompute;
    }

    NFq::NConfig::TYdbStorageConfig GetConnection(const TString& scope) const {
        const auto& controlPlane = ComputeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
                return {};
            case NConfig::TYdbComputeControlPlane::kSingle:
                return controlPlane.GetSingle().GetConnection();
            case NConfig::TYdbComputeControlPlane::kCms:
                return GetConnection(scope, controlPlane.GetCms().GetDatabaseMapping());
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                return GetConnection(scope, controlPlane.GetYdbcp().GetDatabaseMapping());
        }
    }

    NFq::NConfig::TYdbStorageConfig GetConnection(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto it = databaseMapping.GetScopeToComputeDatabase().find(scope);
        if (it != databaseMapping.GetScopeToComputeDatabase().end()) {
            return it->second.GetConnection();
        }
        return databaseMapping.GetCommon().empty()
                   ? NFq::NConfig::TYdbStorageConfig{}
                   : databaseMapping
                         .GetCommon(MultiHash(scope) % databaseMapping.GetCommon().size())
                         .GetConnection();
    }

    bool YdbComputeControlPlaneEnabled(const TString& scope) const {
        return ComputeConfig.GetYdb().GetEnable() &&
               ComputeConfig.GetYdb().GetControlPlane().GetEnable() &&
               (GetComputeType(FederatedQuery::QueryContent::ANALYTICS, scope) ==
                    NFq::NConfig::EComputeType::YDB ||
                GetComputeType(FederatedQuery::QueryContent::STREAMING, scope) ==
                    NFq::NConfig::EComputeType::YDB);
    }

    const NFq::NConfig::TComputeConfig& GetProto() const {
        return ComputeConfig;
    }

private:
    NFq::NConfig::TComputeConfig ComputeConfig;
    NFq::NConfig::EComputeType DefaultCompute;
};

} /* NFq */
