#include "object.h"
#include "update.h"

#include <ydb/core/tx/schemeshard/operations/metadata/tiering_rule/object.h>

namespace NKikimr::NSchemeShard::NOperations {

std::shared_ptr<ISSEntity> TMetadataEntity::MakeEntity(const TPath& path) {
    switch (path->PathType) {
        case NKikimrSchemeOp::EPathTypeTieringRule:
            return std::make_shared<TTieringRuleEntity>(path->PathId);
        default:
            return nullptr;
    }
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoCreateUpdate(const TUpdateInitializationContext& context) const {
    auto update = TMetadataUpdate::MakeUpdate(*context.GetModification());
    if (auto status = update->Initialize(context); status.IsFail()) {
        return status;
    }
    return update;
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TMetadataEntity::DoRestoreUpdate(const TUpdateRestoreContext& /*context*/) const {
    return TConclusionStatus::Fail("Restoring updates is not supported for metadata objects");
}

TConclusion<std::shared_ptr<ISSEntity>> TMetadataEntity::GetEntity(TOperationContext& context, const TPath& path) {
    auto entity = MakeEntity(path);
    if (!entity) {
        return TConclusionStatus::Fail("Unexpected object type at " + path.PathString());
    }
    if (auto status = entity->Initialize({&context}); status.IsFail()) {
        return status;
    }
    return entity;
}

}   // namespace NKikimr::NSchemeShard::NOperations
