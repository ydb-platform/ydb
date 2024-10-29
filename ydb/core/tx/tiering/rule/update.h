#pragma once

#include <ydb/core/tx/schemeshard/operations/metadata/behaviour.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieringRuleValidator final: public NSchemeShard::NOperations::IMetadataUpdateValidator {
public:
    TSchemeConclusionStatus ValidatePath() const override;
    TSchemeConclusionStatus ValidateProperties(const NSchemeShard::IMetadataObjectProperties::TPtr object) const override;
    TSchemeConclusionStatus ValidateAlter(
        const NSchemeShard::IMetadataObjectProperties::TPtr object, const NKikimrSchemeOp::TMetadataObjectProperties& request) const override;
    TSchemeConclusionStatus ValidateDrop(const NSchemeShard::IMetadataObjectProperties::TPtr object) const override;

    using IMetadataUpdateValidator::IMetadataUpdateValidator;
};

class TTieringRuleUpdateBehaviour final: public NSchemeShard::NOperations::IMetadataUpdateBehaviour {
private:
    inline static constexpr NKikimrSchemeOp::EPathType PathType = NKikimrSchemeOp::EPathTypeTieringRule;
    inline static constexpr NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase PropertiesImplCase =
        NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase::kTieringRule;

    inline static const TFactoryByPropertiesImpl::TRegistrator<TTieringRuleUpdateBehaviour> RegistratorByPropertiesImpl = PropertiesImplCase;
    inline static const TFactoryByPath::TRegistrator<TTieringRuleUpdateBehaviour> RegistratorByPath = PathType;

public:
    NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase GetPropertiesImplCase() const {
        return PropertiesImplCase;
    }
    NKikimrSchemeOp::EPathType GetObjectPathType() const {
        return PathType;
    }
    NSchemeShard::ESimpleCounters GetCounterType() const {
        return NSchemeShard::COUNTER_TIERING_RULE_COUNT;
    }

    TTieringRuleValidator::IMetadataUpdateValidator::TPtr MakeValidator(
        const NSchemeShard::TPath& parent, const TString& objectName, const NSchemeShard::TOperationContext& ctx) {
        return std::make_shared<TTieringRuleValidator>(parent, objectName, ctx);
    }

    using IMetadataUpdateBehaviour::IMetadataUpdateBehaviour;
};

}   // namespace NKikimr::NColumnShard::NTiers
