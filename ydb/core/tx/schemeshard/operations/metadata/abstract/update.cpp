#include "update.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOperations {

void TMetadataUpdateBase::PersistObject(const TPathId& pathId, const TMetadataObjectInfo::TPtr& object, const TUpdateStartContext& context) {
    context.GetSSOperationContext()->SS->MetadataObjects[pathId] = object;
    context.GetSSOperationContext()->SS->PersistMetadataObject(*context.GetDB(), pathId, object);
}

TConclusionStatus TMetadataUpdateCreate::DoInitializeImpl(const TUpdateInitializationContext& context) {
    const auto& modification = context.GetModification();
    const auto& request = modification->GetCreateMetadataObject();

    Behaviour.reset(IMetadataUpdateBehaviour::TFactoryByPropertiesImpl::Construct(request.GetProperties().GetPropertiesImplCase()));
    if (!Behaviour) {
        return TConclusionStatus::Fail("Updates are not supported for given object");
    }
    const auto validator = Behaviour->MakeValidator(
        TPath::Resolve(modification->GetWorkingDir(), context.GetSSOperationContext()->SS), request.GetName(), *context.GetSSOperationContext());

    if (context.OriginalEntityExists()) {
        return TConclusionStatus::Fail("Object already exists");
    }

    Result = TMetadataObjectInfo::Create(Behaviour->GetObjectPathType());
    Result->SetAlterVersion(1);
    if (!Result->DeserializePropertiesFromProto(request.GetProperties())) {
        return TConclusionStatus::Fail("Cannot parse object properties");
    }

    if (auto status = validator->ValidateObject(Result); status.IsFail()) {
        return TConclusionStatus::Fail(status.GetErrorMessage());
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TMetadataUpdateCreate::DoStart(const TUpdateStartContext& context) {
    PersistObject(context.GetObjectPath()->Base()->PathId, Result, context);
    // Not implemented: counters
    // context.GetSSOperationContext()->SS->TabletCounters->Simple()[COUNTER_TIERING_RULE_COUNT].Add(1);
    return TConclusionStatus::Success();
}

TConclusionStatus TMetadataUpdateAlter::DoInitializeImpl(const TUpdateInitializationContext& context) {
    const auto& modification = context.GetModification();
    const auto& request = modification->GetCreateMetadataObject();
    const auto& originalEntity = context.GetOriginalEntityAsVerified<TMetadataEntity>();

    Behaviour.reset(IMetadataUpdateBehaviour::TFactoryByPropertiesImpl::Construct(request.GetProperties().GetPropertiesImplCase()));
    if (!Behaviour) {
        return TConclusionStatus::Fail("Updates are not supported for given object");
    }
    const auto validator = Behaviour->MakeValidator(
        TPath::Resolve(modification->GetWorkingDir(), context.GetSSOperationContext()->SS), request.GetName(), *context.GetSSOperationContext());

    if (auto status = validator->ValidateAlter(originalEntity.GetObjectInfo(), request.GetProperties()); status.IsFail()) {
        return TConclusionStatus::Fail(status.GetErrorMessage());
    }

    Result = TMetadataObjectInfo::Create(Behaviour->GetObjectPathType());
    Result->SetAlterVersion(originalEntity.GetObjectInfo()->GetAlterVersion() + 1);
    if (!Result->ApplyPatch(request.GetProperties())) {
        return TConclusionStatus::Fail("Cannot parse object properties");
    }

    if (auto status = validator->ValidateObject(Result); status.IsFail()) {
        return TConclusionStatus::Fail(status.GetErrorMessage());
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TMetadataUpdateAlter::DoStart(const TUpdateStartContext& context) {
    PersistObject(context.GetObjectPath()->Base()->PathId, Result, context);
    return TConclusionStatus::Success();
}

TConclusionStatus TMetadataUpdateDrop::DoInitializeImpl(const TUpdateInitializationContext& context) {
    const auto& modification = context.GetModification();
    const auto& request = modification->GetDrop();
    const auto& originalEntity = context.GetOriginalEntityAsVerified<TMetadataEntity>();
    const TPath objectPath = TPath::Resolve(modification->GetWorkingDir(), context.GetSSOperationContext()->SS);

    if (!objectPath.IsResolved()) {
        return TConclusionStatus::Fail("Object not found at " + objectPath.PathString());
    }

    Behaviour.reset(IMetadataUpdateBehaviour::TFactoryByPath::Construct(objectPath->PathType));
    if (!Behaviour) {
        return TConclusionStatus::Fail("Updates are not supported for given object");
    }
    const auto validator = Behaviour->MakeValidator(objectPath, request.GetName(), *context.GetSSOperationContext());

    if (auto status = validator->ValidateDrop(originalEntity.GetObjectInfo()); status.IsFail()) {
        return TConclusionStatus::Fail(status.GetErrorMessage());
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TMetadataUpdateDrop::DoStart(const TUpdateStartContext& context) {
    context.GetSSOperationContext()->MemChanges.GrabMetadataObject(context.GetSSOperationContext()->SS, context.GetObjectPath()->Base()->PathId);
    return TConclusionStatus::Success();
}

TConclusionStatus TMetadataUpdateDrop::DoFinish(const TUpdateFinishContext& context) {
    // Not implemented: counters
    // context.GetSSOperationContext()->SS->TabletCounters->Simple()[COUNTER_TIERING_RULE_COUNT].Sub(1);
    context.GetSSOperationContext()->SS->PersistRemoveMetadataObject(*context.GetDB(), context.GetObjectPath()->Base()->PathId);
    return TConclusionStatus::Success();
}

NKikimrSchemeOp::TModifyScheme TMetadataUpdateDrop::RestoreRequest(const TPath& path) {
    AFL_VERIFY(path.IsResolved());
    NKikimrSchemeOp::TModifyScheme request;

    request.SetWorkingDir(path.Parent().PathString());
    request.MutableDrop()->SetName(path.Base()->Name);

    return request;
}

}   // namespace NKikimr::NSchemeShard::NOperations
