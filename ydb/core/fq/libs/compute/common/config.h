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
        auto it = databaseMapping.GetScopeToComputeDatabase().find(scope);
        if (it != databaseMapping.GetScopeToComputeDatabase().end()) {
            return it->second.GetControlPlaneConnection();
        }
        return databaseMapping.GetCommon().empty()
                   ? NFq::NConfig::TYdbStorageConfig{}
                   : databaseMapping
                         .GetCommon(MultiHash(scope) % databaseMapping.GetCommon().size())
                         .GetControlPlaneConnection();
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

    NFq::NConfig::TYdbStorageConfig GetExecutionConnection(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto it = databaseMapping.GetScopeToComputeDatabase().find(scope);
        if (it != databaseMapping.GetScopeToComputeDatabase().end()) {
            return it->second.GetExecutionConnection();
        }
        return databaseMapping.GetCommon().empty()
                   ? NFq::NConfig::TYdbStorageConfig{}
                   : databaseMapping
                         .GetCommon(MultiHash(scope) % databaseMapping.GetCommon().size())
                         .GetExecutionConnection();
    }

    NFq::NConfig::TWorkloadManagerConfig GetWorkloadManagerConfig(const TString& scope) {
        const auto& controlPlane = ComputeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
                return {};
            case NConfig::TYdbComputeControlPlane::kSingle:
                return controlPlane.GetSingle().GetWorkloadManagerConfig();
            case NConfig::TYdbComputeControlPlane::kCms:
                return GetWorkloadManagerConfig(scope, controlPlane.GetCms().GetDatabaseMapping());
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                return GetWorkloadManagerConfig(scope, controlPlane.GetYdbcp().GetDatabaseMapping());
        }
    }

    NFq::NConfig::TWorkloadManagerConfig GetWorkloadManagerConfig(const TString& scope, const ::NFq::NConfig::TDatabaseMapping& databaseMapping) const {
        auto it = databaseMapping.GetScopeToComputeDatabase().find(scope);
        if (it != databaseMapping.GetScopeToComputeDatabase().end()) {
            return it->second.GetWorkloadManagerConfig();
        }
        return databaseMapping.GetCommon().empty()
                   ? NFq::NConfig::TWorkloadManagerConfig{}
                   : databaseMapping
                         .GetCommon(MultiHash(scope) % databaseMapping.GetCommon().size())
                         .GetWorkloadManagerConfig();
    }

    bool YdbComputeControlPlaneEnabled(const TString& scope) const {
        return ComputeConfig.GetYdb().GetEnable() &&
               ComputeConfig.GetYdb().GetControlPlane().GetEnable() &&
               (GetComputeType(FederatedQuery::QueryContent::ANALYTICS, scope) ==
                    NFq::NConfig::EComputeType::YDB ||
                GetComputeType(FederatedQuery::QueryContent::STREAMING, scope) ==
                    NFq::NConfig::EComputeType::YDB);
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

    bool IsConnectionCaseEnabled(
        const FederatedQuery::ConnectionSetting::ConnectionCase& connectionCase) const {
        switch (connectionCase) {
            case FederatedQuery::ConnectionSetting::kObjectStorage:
            case FederatedQuery::ConnectionSetting::kClickhouseCluster:
            case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
            case FederatedQuery::ConnectionSetting::kGreenplumCluster:
            case FederatedQuery::ConnectionSetting::kMysqlCluster:
            case FederatedQuery::ConnectionSetting::kYdbDatabase:
                return true;
            case FederatedQuery::ConnectionSetting::kDataStreams:
            case FederatedQuery::ConnectionSetting::kMonitoring:
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
