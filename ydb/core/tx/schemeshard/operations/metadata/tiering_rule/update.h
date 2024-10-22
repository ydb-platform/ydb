#pragma once

#include <ydb/core/tx/schemeshard/operations/metadata/abstract/behaviour.h>

namespace NKikimr::NSchemeShard::NOperations {

class TTieringRuleValidator final: public IMetadataUpdateValidator {
public:
    TSchemeConclusionStatus ValidatePath() const override;
    TSchemeConclusionStatus ValidateObject(const TMetadataObjectInfo::TPtr object) const override;
    TSchemeConclusionStatus ValidateAlter(
        const TMetadataObjectInfo::TPtr object, const NKikimrSchemeOp::TMetadataObjectProperties& request) const override;
    TSchemeConclusionStatus ValidateDrop(const TMetadataObjectInfo::TPtr object) const override;

    using IMetadataUpdateValidator::IMetadataUpdateValidator;
};

class TTieringRuleCallbacks final: public IMetadataUpdateCallbacks {
public:
    using IMetadataUpdateCallbacks::IMetadataUpdateCallbacks;
};

class TTieringRuleUpdateBehaviour final: public IMetadataUpdateBehaviour {
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

    IMetadataUpdateValidator::TPtr MakeValidator(const TPath& parent, const TString& objectName, const TOperationContext& ctx) {
        return std::make_shared<TTieringRuleValidator>(parent, objectName, ctx);
    }
    IMetadataUpdateCallbacks::TPtr MakeCallbacks(TUpdateStartContext& ctx) {
        return std::make_shared<TTieringRuleCallbacks>(ctx);
    }

    using IMetadataUpdateBehaviour::IMetadataUpdateBehaviour;
};

}   // namespace NKikimr::NSchemeShard::NOperations
