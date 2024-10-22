#pragma once

#include "info.h"

#include <ydb/core/tx/schemeshard/operations/abstract/context.h>

namespace NKikimr::NSchemeShard::NOperations {

class IMetadataUpdateCallbacks {
protected:
    TUpdateStartContext& Ctx;

protected:
    IMetadataUpdateCallbacks(TUpdateStartContext& ctx)
        : Ctx(ctx) {
    }

public:
    using TPtr = std::shared_ptr<IMetadataUpdateCallbacks>;

    virtual void OnCreated(const TMetadataObjectInfo::TPtr /*object*/) {
    }
    virtual void OnAltered(const TMetadataObjectInfo::TPtr /*oldObject*/, const TMetadataObjectInfo::TPtr /*result*/) {
    }
    virtual void OnDropProposed(const TMetadataObjectInfo::TPtr /*object*/) {
    }

    virtual ~IMetadataUpdateCallbacks() = default;
};

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
    virtual TSchemeConclusionStatus ValidateObject(const TMetadataObjectInfo::TPtr object) const = 0;
    virtual TSchemeConclusionStatus ValidateAlter(
        const TMetadataObjectInfo::TPtr object, const NKikimrSchemeOp::TMetadataObjectProperties& request) const = 0;
    virtual TSchemeConclusionStatus ValidateDrop(const TMetadataObjectInfo::TPtr object) const = 0;

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
    virtual IMetadataUpdateCallbacks::TPtr MakeCallbacks(TUpdateStartContext& ctx) = 0;

    virtual ~IMetadataUpdateBehaviour() = default;
};

}   // namespace NKikimr::NSchemeShard::NOperations
