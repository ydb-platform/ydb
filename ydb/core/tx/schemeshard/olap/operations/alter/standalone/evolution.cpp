#include "evolution.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TStandaloneSchemaEvolution::DoInitialize(const TEvolutionInitializationContext& context) {
    TColumnTableInfo::TPtr tableInfo = context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(GetPathId());
    if (!tableInfo->IsStandalone()) {
        return TConclusionStatus::Fail("incorrect path object - not standalone");
    }
    Original = std::make_shared<TStandaloneTable>(GetPathId(), tableInfo);
    Target = std::make_shared<TStandaloneTable>(GetPathId(), tableInfo->AlterData);
    {
        NKikimrSchemeOp::TModifyScheme txSchema;
        AFL_VERIFY(!!tableInfo->AlterData->AlterBody);
        *txSchema.MutableAlterColumnTable() = *tableInfo->AlterData->AlterBody;
        txSchema.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
        Update = std::make_shared<TStandaloneSchemaUpdate>(Original);
        TUpdateInitializationContext uContext(context.GetSSOperationContext(), &txSchema);
        auto conclusion = Update->Initialize(uContext);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TStandaloneSchemaEvolution::DoStartEvolution(const TEvolutionStartContext& context) {
    auto table = context.GetSSOperationContext()->SS->ColumnTables.TakeVerified(GetPathId());
    table->AlterData = Target->GetTableInfoPtrVerified();
    context.GetSSOperationContext()->SS->PersistColumnTableAlter(*context.GetDB(), GetPathId(), Target->GetTableInfoVerified());
    return TConclusionStatus::Success();
}

}