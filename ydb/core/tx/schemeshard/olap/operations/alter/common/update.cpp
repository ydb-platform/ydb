#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard__operation_iface.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TColumnTableUpdate::DoStart(const TUpdateStartContext& context) {
    auto conclusion = DoStartImpl(context);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    const auto pathId = context.GetObjectPath()->Base()->PathId;
    auto tableInfo = context.GetSSOperationContext()->SS->ColumnTables.TakeVerified(pathId);
    context.GetSSOperationContext()->SS->PersistColumnTableAlter(*context.GetDB(), pathId, *GetTargetTableInfoVerified());
    tableInfo->AlterData = GetTargetTableInfoVerified();
    return TConclusionStatus::Success();
}

TConclusionStatus TColumnTableUpdate::DoFinish(const TUpdateFinishContext& context) {
    auto conclusion = DoFinishImpl(context);
    if (conclusion.IsFail()) {
        return conclusion;
    }

    const auto pathId = context.GetObjectPath()->Base()->PathId;
    auto tableInfo = context.GetSSOperationContext()->SS->ColumnTables.TakeAlterVerified(pathId);
    context.GetSSOperationContext()->SS->PersistColumnTableAlterRemove(*context.GetDB(), pathId);
    context.GetSSOperationContext()->SS->PersistColumnTable(*context.GetDB(), pathId, *tableInfo);
    return TConclusionStatus::Success();
}

}
