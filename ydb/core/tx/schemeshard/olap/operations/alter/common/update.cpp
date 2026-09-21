#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TColumnTableUpdate::DoStart(const TUpdateStartContext& context) {
    auto conclusion = DoStartImpl(context);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    const auto pathId = context.GetObjectPath()->Base()->PathId;
    auto* ssContext = context.GetSSOperationContext();
    ssContext->MemChanges.GrabColumnTable(ssContext->SS, pathId);
    auto tableInfo = ssContext->SS->ColumnTables.TakeVerified(pathId);
    tableInfo->AlterData = GetTargetTableInfoVerified();

    {
        THashSet<TString> oldDataSources = tableInfo->GetUsedTiers();
        THashSet<TString> newDataSources = GetTargetTableInfoVerified()->GetUsedTiers();
        for (const auto& tier : oldDataSources) {
            if (!newDataSources.contains(tier)) {
                auto tierPath = TPath::Resolve(tier, ssContext->SS);
                AFL_VERIFY(tierPath.IsResolved())("path", tier);
                ssContext->MemChanges.GrabExternalDataSource(ssContext->SS, tierPath->PathId);
                ssContext->SS->RemoveExternalDataSourceReference(tierPath->PathId, pathId);
                ssContext->DbChanges.PersistExternalDataSource(tierPath->PathId);
            }
        }
        for (const auto& tier : newDataSources) {
            if (!oldDataSources.contains(tier)) {
                auto tierPath = TPath::Resolve(tier, ssContext->SS);
                AFL_VERIFY(tierPath.IsResolved())("path", tier);
                ssContext->MemChanges.GrabExternalDataSource(ssContext->SS, tierPath->PathId);
                ssContext->SS->AddExternalDataSourceReference(
                    tierPath->PathId, TPath::Init(pathId, ssContext->SS));
                ssContext->DbChanges.PersistExternalDataSource(tierPath->PathId);
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
    auto* ssContext = context.GetSSOperationContext();
    ssContext->MemChanges.GrabColumnTable(ssContext->SS, pathId);
    auto tableInfo = ssContext->SS->ColumnTables.TakeAlterVerified(pathId);
    ssContext->DbChanges.PersistColumnTableAlterRemove(pathId);
    ssContext->DbChanges.PersistColumnTable(pathId);
    Y_UNUSED(tableInfo);
    return TConclusionStatus::Success();
}

}