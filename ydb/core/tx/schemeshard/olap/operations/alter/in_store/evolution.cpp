#include "evolution.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TInStoreSchemaEvolution::DoInitialize(const TEvolutionInitializationContext& context) {
    TColumnTableInfo::TPtr tableInfo = context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(GetPathId());
    AFL_VERIFY(!tableInfo->IsStandalone());
    TEntityInitializationContext eContext(context.GetSSOperationContext());
    Original = std::make_shared<TInStoreTable>(GetPathId(), tableInfo, eContext);
    Target = std::make_shared<TInStoreTable>(GetPathId(), tableInfo->AlterData, eContext);
    Update = std::make_shared<TInStoreSchemaUpdate>(Original, Target);
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TInStoreSchemaEvolution::DoStartEvolution(const TEvolutionStartContext& context) {
    const auto storePathId = Original->GetTableInfo()->GetOlapStorePathIdVerified();
    TPath storePath = TPath::Init(storePathId, context.GetSSOperationContext()->SS);

    Y_ABORT_UNLESS(Original->GetStoreInfo()->ColumnTables.contains((*context.GetObjectPath())->PathId));
    Original->GetStoreInfo()->ColumnTablesUnderOperation.insert((*context.GetObjectPath())->PathId);

    // Sequentially chain operations in the same olap store
    if (context.GetSSOperationContext()->SS->Operations.contains(storePath.Base()->LastTxId)) {
        context.GetSSOperationContext()->OnComplete.Dependence(storePath.Base()->LastTxId, (*context.GetObjectPath())->LastTxId);
    }
    storePath.Base()->LastTxId = (*context.GetObjectPath())->LastTxId;
    context.GetSSOperationContext()->SS->PersistLastTxId(*context.GetDB(), storePath.Base());
    context.GetSSOperationContext()->SS->PersistColumnTableAlter(*context.GetDB(), GetPathId(), *Target->GetTableInfo());
    auto table = context.GetSSOperationContext()->SS->ColumnTables.TakeVerified(GetPathId());
    table->AlterData = Target->GetTableInfoPtrVerified();
    return TConclusionStatus::Success();
}

}