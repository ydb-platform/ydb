#include "update.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TInStoreTableUpdate::DoStartImpl(const TUpdateStartContext& context) {
    const auto& inStoreTable = GetTargetEntityAsVerified<TInStoreTable>();

    auto tableInfo = GetTargetTableInfoVerified();
    const auto storePathId = tableInfo->GetOlapStorePathIdVerified();
    TPath storePath = TPath::Init(storePathId, context.GetSSOperationContext()->SS);

    Y_ABORT_UNLESS(inStoreTable.GetStoreInfo()->ColumnTables.contains((*context.GetObjectPath())->PathId));
    inStoreTable.GetStoreInfo()->ColumnTablesUnderOperation.insert((*context.GetObjectPath())->PathId);

    // Sequentially chain operations in the same olap store
    if (context.GetSSOperationContext()->SS->Operations.contains(storePath.Base()->LastTxId)) {
        context.GetSSOperationContext()->OnComplete.Dependence(storePath.Base()->LastTxId, (*context.GetObjectPath())->LastTxId);
    }
    storePath.Base()->LastTxId = (*context.GetObjectPath())->LastTxId;
    context.GetSSOperationContext()->SS->PersistLastTxId(*context.GetDB(), storePath.Base());
    return DoStartInStoreImpl(context);
}

}