#include "object.h"
#include "update.h"

namespace NKikimr::NSchemeShard::NOperations {

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoCreateUpdate(const TUpdateInitializationContext& context) const {
    std::shared_ptr<ISSEntityUpdate> update;
    switch (context.GetModification()->GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateMetadataObject:
            update.reset(TMetadataUpdateCreate::TFactory::Construct(
                context.GetModification()->GetCreateMetadataObject().GetProperties().GetPropertiesImplCase()));
            break;
        case NKikimrSchemeOp::ESchemeOpAlterMetadataObject:
            update.reset(TMetadataUpdateAlter::TFactory::Construct(
                context.GetModification()->GetCreateMetadataObject().GetProperties().GetPropertiesImplCase()));
            break;
        case NKikimrSchemeOp::ESchemeOpDropMetadataObject:
            update = GetDropUpdate();
            break;
        default:
            return TConclusionStatus::Fail("Not a metadata operation");
    }
    AFL_VERIFY(update);
    if (auto status = update->Initialize(context); status.IsFail()) {
        return status;
    }
    return update;
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoRestoreUpdate(const TUpdateRestoreContext& /*context*/) const {
    return TConclusionStatus::Fail("Restoring updates is not supported for metadata objects");
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::RestoreDropUpdate(const TUpdateRestoreContext& context) const {
    auto update = GetDropUpdate();
    AFL_VERIFY(update);
    NKikimrSchemeOp::TModifyScheme emptyRequest;
    TUpdateInitializationContext initializationContext(
        &context.GetOriginalEntity(), context.GetSSOperationContext(), &emptyRequest, context.GetTxId());
    if (auto status = update->Initialize(initializationContext); status.IsFail()) {
        return status;
    }
    return update;
}

TConclusion<std::shared_ptr<TMetadataEntity>> TMetadataEntity::GetEntity(TOperationContext& context, const TPath& path) {
    auto entity = TMetadataEntity::TFactory::MakeHolder(path->PathType, path->PathId);
    if (!entity) {
        return TConclusionStatus::Fail("Not a metadata object at " + path.PathString());
    }
    if (auto status = entity->Initialize({&context}); status.IsFail()) {
        return status;
    }
    return std::shared_ptr<TMetadataEntity>(entity.Release());
}

}   // namespace NKikimr::NSchemeShard::NOperations
