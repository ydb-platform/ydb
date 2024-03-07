#pragma once

#include <util/digest/multi.h>

#include <ydb/core/fq/libs/config/protos/compute.pb.h>


namespace NFq {

struct TComputeMapping {
    using TPtr = std::shared_ptr<TComputeMapping>;
    virtual ui32 GetAvailableCommontTenantCount() = 0;
    virtual std::optional<ui32> MapScopeToCommonTenant(const TString& scope) = 0;
};

struct TNullComputeMapping final : NFq::TComputeMapping {
    ui32 GetAvailableCommontTenantCount() final {
        return 0;
    }
    
    std::optional<ui32> MapScopeToCommonTenant(const TString&) final {
        return std::nullopt;
    }
};

struct TFixedComputeMapping final : NFq::TComputeMapping {
    TFixedComputeMapping(const NConfig::TComputeConfig& computeConfig) {
        const auto& controlPlane = computeConfig.GetYdb().GetControlPlane();
        switch (controlPlane.type_case()) {
            case NConfig::TYdbComputeControlPlane::TYPE_NOT_SET:
            case NConfig::TYdbComputeControlPlane::kSingle:
                TenantCount = 1;
                break;
            case NConfig::TYdbComputeControlPlane::kCms:
                TenantCount = controlPlane.GetCms().GetDatabaseMapping().GetCommon().size();
                break;
            case NConfig::TYdbComputeControlPlane::kYdbcp:
                TenantCount = controlPlane.GetYdbcp().GetDatabaseMapping().GetCommon().size();
                break;
        }
    }

    ui32 GetAvailableCommontTenantCount() final {
        return TenantCount;
    }

    std::optional<ui32> MapScopeToCommonTenant(const TString& scope) final {
        switch (TenantCount) {
        case 0:
            return std::nullopt;
        case 1:
            return 0; // no need to hash
        default:
            return MultiHash(scope) % TenantCount;
        }
    }

    ui32 TenantCount = 0;
};

struct TComputeMappingHolder {
    using TPtr = std::shared_ptr<TComputeMappingHolder>;
    NFq::TComputeMapping::TPtr Mapping;
};

} // namespace NFq
