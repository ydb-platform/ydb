#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/rule/object.h>

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

bool TColumnTableUpdate::ValidateTtlSettings(const NKikimrSchemeOp::TColumnDataLifeCycle& ttl, const TOlapSchema& schema,
    const TUpdateInitializationContext& context, IErrorCollector& errors) {
    if (!schema.ValidateTtlSettings(ttl, errors)) {
        return false;
    }

    if (const TString& tieringId = ttl.GetUseTiering()) {
        const TPath path =
            TPath::Resolve(NColumnShard::NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath(), context.GetSSOperationContext()->SS)
                .Dive(ttl.GetUseTiering());
        {
            TPath::TChecker checks = path.Check();
            checks.NotEmpty().NotUnderDomainUpgrade().IsAtLocalSchemeShard().IsResolved().NotDeleted().IsTieringRule().NotUnderOperation();
            if (!checks) {
                errors.AddError(checks.GetStatus(), checks.GetError());
            }
        }

        const auto* tieringRule = context.GetSSOperationContext()->SS->TieringRules.FindPtr(path.Base()->PathId);
        AFL_VERIFY(tieringRule)("name", tieringId);
        if (!schema.ValidateTieringColumn(tieringRule->Get()->DefaultColumn, errors)) {
            return false;
        }
    }

    return true;
}
}   // namespace NKikimr::NSchemeShard::NOlap::NAlter
