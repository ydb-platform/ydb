#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

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

    {
        THashSet<TString> oldDataSources = tableInfo->GetUsedTiers();
        THashSet<TString> newDataSources = GetTargetTableInfoVerified()->GetUsedTiers();
        for (const auto& tier : oldDataSources) {
            if (!newDataSources.contains(tier)) {
                auto tierPath = TPath::Resolve(tier, context.GetSSOperationContext()->SS);
                AFL_VERIFY(tierPath.IsResolved())("path", tier);
                context.GetSSOperationContext()->SS->PersistRemoveExternalDataSourceReference(*context.GetDB(), tierPath->PathId, pathId);
            }
        }
        for (const auto& tier : newDataSources) {
            if (!oldDataSources.contains(tier)) {
                auto tierPath = TPath::Resolve(tier, context.GetSSOperationContext()->SS);
                AFL_VERIFY(tierPath.IsResolved())("path", tier);
                context.GetSSOperationContext()->SS->PersistExternalDataSourceReference(
                    *context.GetDB(), tierPath->PathId, TPath::Init(pathId, context.GetSSOperationContext()->SS));
            }
        }
    }

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