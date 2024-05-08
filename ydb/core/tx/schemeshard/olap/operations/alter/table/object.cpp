#include "object.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TSSEntityColumnTable::DoInitialize(const TEntityInitializationContext& context) {
    TableInfo = context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(GetPathId());
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TSSEntityColumnTable::DoStartUpdate(TEvolutions&& evolutions, const TStartUpdateContext& /*context*/) {
    AFL_VERIFY(!!TableInfo);
    TableInfo->SetEvolutions(std::move(evolutions));
    return TConclusionStatus::Success();
}

void TSSEntityColumnTable::DoPersist(const TDBWriteContext& dbContext) const {
    AFL_VERIFY(!!TableInfo);
    dbContext.GetSSOperationContext()->SS->PersistColumnTable(*dbContext.GetDB(), GetPathId(), *TableInfo);
    if (TableInfo->IsInModification()) {
        dbContext.GetSSOperationContext()->SS->PersistColumnTableAlter(*dbContext.GetDB(), GetPathId(), *TableInfo);
    } else {
        dbContext.GetSSOperationContext()->SS->PersistColumnTableAlterRemove(*dbContext.GetDB(), GetPathId());
    }
}

}