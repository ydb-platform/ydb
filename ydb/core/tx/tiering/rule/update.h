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
    inline static const NKikimrSchemeOp::EPathType PathType = NKikimrSchemeOp::EPathTypeTieringRule;
    inline static const NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase PropertiesImplCase =
        NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase::kTieringRule;

public:
    static TFactoryByPropertiesImpl::TRegistrator<TTieringRuleUpdateBehaviour> RegistratorByPropertiesImpl;
    static TFactoryByPath::TRegistrator<TTieringRuleUpdateBehaviour> RegistratorByPath;

    NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase GetPropertiesImplCase() const {
        return PropertiesImplCase;
    }
    NKikimrSchemeOp::EPathType GetObjectPathType() const {
        return PathType;
    }

    TTieringRuleValidator::IMetadataUpdateValidator::TPtr MakeValidator(
        const NSchemeShard::TPath& parent, const TString& objectName, const NSchemeShard::TOperationContext& ctx) {
        return std::make_shared<TTieringRuleValidator>(parent, objectName, ctx);
    }

    using IMetadataUpdateBehaviour::IMetadataUpdateBehaviour;
};

}   // namespace NKikimr::NColumnShard::NTiers
