#pragma once

#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>

#include <util/digest/multi.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NFq {

class TComputeConfig {
public:
    explicit TComputeConfig()
        : TComputeConfig({})
    {}

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
            if (mapping.GetQueryType() != queryType) {
                continue;
            }
            if (!mapping.HasActivation()) {
                return mapping.GetCompute();
            }
            const auto& activation    = mapping.GetActivation();
            const auto& includeScopes = activation.GetIncludeScopes();
            const auto& excludeScopes = activation.GetExcludeScopes();
            auto isActivatedCase1 =
                activation.GetPercentage() == 0 &&
                Find(includeScopes, scope) != includeScopes.end();
            auto isActivatedCase2 =
                activation.GetPercentage() == 100 &&
                Find(excludeScopes, scope) == excludeScopes.end();
            if (isActivatedCase1 || isActivatedCase2) {
                return mapping.GetCompute();
            }
        }

        return DefaultCompute;
    }

    TVector<TString> GetPinTenantNames(FederatedQuery::QueryContent::QueryType queryType, const TString& scope) const {
        NFq::NConfig::EComputeType computeType = GetComputeType(queryType, scope);
        switch (computeType) {
            case NFq::NConfig::EComputeType::YDB:
                return TVector<TString>{ComputeConfig.GetYdb().GetPinTenantName().begin(), ComputeConfig.GetYdb().GetPinTenantName().end()};
            case NFq::NConfig::EComputeType::IN_PLACE:
            case NFq::NConfig::EComputeType::UNKNOWN:
            case NFq::NConfig::EComputeType_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NFq::NConfig::EComputeType_INT_MAX_SENTINEL_DO_NOT_USE_:
                return TVector<TString>{};
        }
    }

    TVector<TString> GetExternalSourcesAccessSIDs(const TString& scope) const {
        const auto& controlPlane = ComputeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
                return {};
            case NConfig::TYdbComputeControlPlane::kSingle:
            {
                const auto& ids = controlPlane.GetSingle().GetAccessConfig().GetExternalSourcesAccessSID();
                return TVector<TString>{ids.begin(), ids.end()};
            }
            case NConfig::TYdbComputeControlPlane::kCms:
                return GetExternalSourcesAccessSIDs(scope, controlPlane.GetCms().GetDatabaseMapping());
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                return GetExternalSourcesAccessSIDs(scope, controlPlane.GetYdbcp().GetDatabaseMapping());
        }
    }

    TVector<TString> GetExternalSourcesAccessSIDs(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        const auto& protoExternalSourcesAccessSIDs = GetComputeDatabaseConfig(scope, databaseMapping).GetAccessConfig().GetExternalSourcesAccessSID();
        return TVector<TString>{protoExternalSourcesAccessSIDs.begin(), protoExternalSourcesAccessSIDs.end()};
    }

    NFq::NConfig::TYdbStorageConfig GetControlPlaneConnection(const TString& scope) const {
        const auto& controlPlane = ComputeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
                return {};
            case NConfig::TYdbComputeControlPlane::kSingle:
                return controlPlane.GetSingle().GetConnection();
            case NConfig::TYdbComputeControlPlane::kCms:
                return GetControlPlaneConnection(scope, controlPlane.GetCms().GetDatabaseMapping());
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                return GetControlPlaneConnection(scope, controlPlane.GetYdbcp().GetDatabaseMapping());
        }
    }

    NFq::NConfig::TYdbStorageConfig GetControlPlaneConnection(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto computeDatabaseConfig = GetComputeDatabaseConfig(scope, databaseMapping);
        return computeDatabaseConfig.GetControlPlaneConnection();
    }

    NFq::NConfig::TYdbStorageConfig GetExecutionConnection(const TString& scope) const {
        const auto& controlPlane = ComputeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
                return {};
            case NConfig::TYdbComputeControlPlane::kSingle:
                return controlPlane.GetSingle().GetConnection();
            case NConfig::TYdbComputeControlPlane::kCms:
                return GetExecutionConnection(scope, controlPlane.GetCms().GetDatabaseMapping());
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                return GetExecutionConnection(scope, controlPlane.GetYdbcp().GetDatabaseMapping());
        }
    }

    NFq::NConfig::TYdbStorageConfig GetSchemeConnection(const TString& scope) const {
        const auto& controlPlane = ComputeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
                return {};
            case NConfig::TYdbComputeControlPlane::kSingle:
                return controlPlane.GetSingle().GetConnection();
            case NConfig::TYdbComputeControlPlane::kCms:
                return GetSchemeConnection(scope, controlPlane.GetCms().GetDatabaseMapping());
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                return GetSchemeConnection(scope, controlPlane.GetYdbcp().GetDatabaseMapping());
        }
    }

    NFq::NConfig::TYdbStorageConfig GetSchemeConnection(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto it = databaseMapping.GetScopeToComputeDatabase().find(scope);
        if (it != databaseMapping.GetScopeToComputeDatabase().end()) {
            return it->second.HasSchemeConnection() ? it->second.GetSchemeConnection() : it->second.GetExecutionConnection(); // TODO: for backward compatibility, cleanup it after migration
        }

        if (databaseMapping.GetCommon().empty()) {
            return NFq::NConfig::TYdbStorageConfig{};
        }

        auto config = databaseMapping.GetCommon(MultiHash(scope) % databaseMapping.GetCommon().size());
        return config.HasSchemeConnection() ? config.GetSchemeConnection() : config.GetExecutionConnection(); // TODO: for backward compatibility, cleanup it after migration
    }

    NFq::NConfig::TYdbStorageConfig GetExecutionConnection(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto computeDatabaseConfig = GetComputeDatabaseConfig(scope, databaseMapping);
        return computeDatabaseConfig.GetExecutionConnection();
    }

    NFq::NConfig::TWorkloadManagerConfig GetWorkloadManagerConfig(const TString& scope) const {
        const auto& controlPlane = ComputeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
                return {};
            case NConfig::TYdbComputeControlPlane::kSingle:
                if (controlPlane.GetSingle().HasWorkloadManagerConfig()) {
                    return controlPlane.GetSingle().GetWorkloadManagerConfig();
                }
                return controlPlane.GetDefaultWorkloadManagerConfig();
            case NConfig::TYdbComputeControlPlane::kCms:
                return GetWorkloadManagerConfig(scope, controlPlane.GetCms().GetDatabaseMapping());
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                return GetWorkloadManagerConfig(scope, controlPlane.GetYdbcp().GetDatabaseMapping());
        }
    }

    NFq::NConfig::TWorkloadManagerConfig GetWorkloadManagerConfig(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto computeDatabaseConfig = GetComputeDatabaseConfig(scope, databaseMapping);
        if (computeDatabaseConfig.HasWorkloadManagerConfig()) {
            return computeDatabaseConfig.GetWorkloadManagerConfig();
        }
        return ComputeConfig.GetYdb().GetControlPlane().GetDefaultWorkloadManagerConfig();
    }

    NFq::NConfig::TComputeDatabaseConfig GetComputeDatabaseConfig(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto it = databaseMapping.GetScopeToComputeDatabase().find(scope);
        if (it != databaseMapping.GetScopeToComputeDatabase().end()) {
            return it->second;
        }
        if (databaseMapping.GetCommon().empty()) {
            return NFq::NConfig::TComputeDatabaseConfig{};
        }
        return databaseMapping.GetCommon(MultiHash(scope) % databaseMapping.GetCommon().size());
    }

    bool YdbComputeControlPlaneEnabled(const TString& scope) const {
        return YdbComputeControlPlaneEnabled(scope, FederatedQuery::QueryContent::ANALYTICS) ||  
                YdbComputeControlPlaneEnabled(scope, FederatedQuery::QueryContent::STREAMING);
    }

    bool YdbComputeControlPlaneEnabled(const TString& scope, FederatedQuery::QueryContent::QueryType queryType) const {
        if (queryType == FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
            return YdbComputeControlPlaneEnabled(scope);
        }
        return ComputeConfig.GetYdb().GetEnable() &&
               ComputeConfig.GetYdb().GetControlPlane().GetEnable() &&
               GetComputeType(queryType, scope) == NFq::NConfig::EComputeType::YDB;
    }

    bool IsYDBSchemaOperationsEnabled(
        const TString& scope,
        const FederatedQuery::ConnectionSetting::ConnectionCase& connectionCase) const {
        return IsConnectionCaseEnabled(connectionCase) &&
               YdbComputeControlPlaneEnabled(scope);
    }

    bool IsYDBSchemaOperationsEnabled(
        const TString& scope,
        const FederatedQuery::BindingSetting::BindingCase& bindingCase) const {
        return IsBindingCaseEnabled(bindingCase) &&
               YdbComputeControlPlaneEnabled(scope);
    }

    const NFq::NConfig::TComputeConfig& GetProto() const {
        return ComputeConfig;
    }

    bool IsBindingCaseEnabled(
        const FederatedQuery::BindingSetting::BindingCase& bindingCase) const {
        switch (bindingCase) {
            case FederatedQuery::BindingSetting::kObjectStorage:
                return true;
            case FederatedQuery::BindingSetting::kDataStreams:
            case FederatedQuery::BindingSetting::BINDING_NOT_SET:
                return false;
        }
    }

    // This function shows which external data sources are currently supported by the open-source YDB
    // and which ones are not yet supported.
    bool IsConnectionCaseEnabled(
        const FederatedQuery::ConnectionSetting::ConnectionCase& connectionCase) const {
        switch (connectionCase) {
            case FederatedQuery::ConnectionSetting::kObjectStorage:
            case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            case FederatedQuery::ConnectionSetting::kGreenplumCluster:
            case FederatedQuery::ConnectionSetting::kMonitoring:
            case FederatedQuery::ConnectionSetting::kMysqlCluster:
            case FederatedQuery::ConnectionSetting::kYdbDatabase:
            case FederatedQuery::ConnectionSetting::kLogging:
            case FederatedQuery::ConnectionSetting::kIceberg:
                return true;
            case FederatedQuery::ConnectionSetting::kDataStreams:
            case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
                return false;
        }
    }

    [[nodiscard]] bool IsReplaceIfExistsSyntaxSupported() const {
      return ComputeConfig.GetSupportedComputeYdbFeatures().GetReplaceIfExists();
    }

private:
    NFq::NConfig::TComputeConfig ComputeConfig;
    NFq::NConfig::EComputeType DefaultCompute;
};

} /* NFq */
