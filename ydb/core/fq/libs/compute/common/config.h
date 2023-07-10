#pragma once

#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>

#include <util/generic/algorithm.h>

#include <util/digest/multi.h>

namespace NFq {

class TComputeConfig {
public:
    explicit TComputeConfig(const NFq::NConfig::TComputeConfig& computeConfig)
        : ComputeConfig(computeConfig)
    {}

    NFq::NConfig::EComputeType GetComputeType(const Fq::Private::GetTaskResult::Task& task) const {
        for (const auto& rule: ComputeConfig.GetComputeMapping().GetRule()) {
            const bool isMatched = AllOf(rule.GetKey(),
                [&task](const auto& key) {
                    switch (key.key_case()) {
                        case NConfig::TComputeMappingRuleKey::kQueryType:
                            return key.GetQueryType() == task.query_type();
                        case NConfig::TComputeMappingRuleKey::KEY_NOT_SET:
                            return false;
                    }
                });
            if (isMatched) {
                return rule.GetCompute();
            }
        }
        return NFq::NConfig::EComputeType::IN_PLACE;
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
        return databaseMapping.GetCommon().empty() ? NFq::NConfig::TYdbStorageConfig{} : databaseMapping.GetCommon(MultiHash(scope) % databaseMapping.GetCommon().size()).GetConnection();
    }

    bool YdbComputeControlPlaneEnabled() const {
        return ComputeConfig.GetYdb().GetEnable() && ComputeConfig.GetYdb().GetControlPlane().GetEnable();
    }

private:
    NFq::NConfig::TComputeConfig ComputeConfig;
};

} /* NFq */
