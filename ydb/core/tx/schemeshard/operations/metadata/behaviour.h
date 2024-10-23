#pragma once

#include "properties.h"

#include <ydb/core/tx/schemeshard/operations/abstract/context.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

namespace NKikimr::NSchemeShard::NOperations {

class IMetadataUpdateValidator {
protected:
    const TPath Parent;
    const TString Name;
    const TOperationContext& Ctx;

protected:
    IMetadataUpdateValidator(const TPath& parent, const TString& objectName, const TOperationContext& ctx)
        : Parent(parent)
        , Name(objectName)
        , Ctx(ctx) {
    }

public:
    using TPtr = std::shared_ptr<IMetadataUpdateValidator>;

    using TSchemeConclusionStatus =
        TConclusionSpecialStatus<NKikimrScheme::EStatus, NKikimrScheme::StatusSuccess, NKikimrScheme::StatusInvalidParameter>;

    virtual TSchemeConclusionStatus ValidatePath() const = 0;
    virtual TSchemeConclusionStatus ValidateProperties(const IMetadataObjectProperties::TPtr object) const = 0;
    virtual TSchemeConclusionStatus ValidateAlter(
        const IMetadataObjectProperties::TPtr object, const NKikimrSchemeOp::TMetadataObjectProperties& request) const = 0;
    virtual TSchemeConclusionStatus ValidateDrop(const IMetadataObjectProperties::TPtr object) const = 0;

    virtual ~IMetadataUpdateValidator() = default;
};

class IMetadataUpdateBehaviour {
public:
    using TFactoryByPropertiesImpl = NObjectFactory::TObjectFactory<IMetadataUpdateBehaviour, NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase>;
    using TFactoryByPath = NObjectFactory::TObjectFactory<IMetadataUpdateBehaviour, NKikimrSchemeOp::EPathType>;
    using TPtr = std::shared_ptr<IMetadataUpdateBehaviour>;

    virtual NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase GetPropertiesImplCase() const = 0;
    virtual NKikimrSchemeOp::EPathType GetObjectPathType() const = 0;

    virtual IMetadataUpdateValidator::TPtr MakeValidator(const TPath& parent, const TString& objectName, const TOperationContext& ctx) = 0;

    virtual ~IMetadataUpdateBehaviour() = default;
};

}   // namespace NKikimr::NSchemeShard::NOperations
