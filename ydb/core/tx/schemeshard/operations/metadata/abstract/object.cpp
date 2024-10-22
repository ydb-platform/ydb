#include "object.h"
#include "update.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOperations {

[[nodiscard]] TConclusionStatus TMetadataEntity::DoInitialize(const TEntityInitializationContext& context) {
    auto* object = context.GetSSOperationContext()->SS->MetadataObjects.FindPtr(GetPathId());
    if (!object) {
        return TConclusionStatus::Fail("Object not found");
    }
    ObjectInfo = *object;
    return TConclusionStatus::Success();
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoCreateUpdate(const TUpdateInitializationContext& context) const {
    std::shared_ptr<ISSEntityUpdate> update;
    switch (context.GetModification()->GetOperationType()) {
        case NKikimrSchemeOp::ESchemeOpCreateMetadataObject:
            update = std::make_shared<TMetadataUpdateCreate>();
            break;
        case NKikimrSchemeOp::ESchemeOpAlterMetadataObject:
            update = std::make_shared<TMetadataUpdateAlter>();
            break;
        case NKikimrSchemeOp::ESchemeOpDropMetadataObject:
            update = std::make_shared<TMetadataUpdateDrop>();
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

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoRestoreUpdate(const TUpdateRestoreContext& context) const {
    auto findTxState = context.GetSSOperationContext()->SS->TxInFlight.FindPtr(context.GetOperationId());
    AFL_VERIFY(findTxState);
    if (findTxState->TxType != TTxState::TxDropMetadataObject) {
        Y_ABORT("Only restoration of drop updates is supported");
    }

    auto update = std::make_shared<TMetadataUpdateDrop>();
    AFL_VERIFY(update);

    TPath objectPath = TPath::Init(context.GetOriginalEntityAsVerified<TMetadataEntity>().GetPathId(), context.GetSSOperationContext()->SS);
    NKikimrSchemeOp::TModifyScheme request = TMetadataUpdateDrop::RestoreRequest(objectPath);

    TUpdateInitializationContext initializationContext(
        &context.GetOriginalEntity(), context.GetSSOperationContext(), &request, context.GetTxId());
    if (auto status = update->Initialize(initializationContext); status.IsFail()) {
        return status;
    }
    return update;
}

// TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::RestoreDropUpdate(const TUpdateRestoreContext& context) const {
//     auto update = GetDropUpdate();
//     AFL_VERIFY(update);
//     NKikimrSchemeOp::TModifyScheme emptyRequest;
//     TUpdateInitializationContext initializationContext(
//         &context.GetOriginalEntity(), context.GetSSOperationContext(), &emptyRequest, context.GetTxId());
//     if (auto status = update->Initialize(initializationContext); status.IsFail()) {
//         return status;
//     }
//     return update;
// }

// IMetadataObjectInfo::TPtr CreateObjectInfo(const TPathId& pathId, const NKikimrSchemeOp::EPathType pathType) {
//     auto entity = TMetadataEntity::TFactory::MakeHolder(pathType, pathId);
//     return entity->DoCreateObjectInfo();
// }

}   // namespace NKikimr::NSchemeShard::NOperations
