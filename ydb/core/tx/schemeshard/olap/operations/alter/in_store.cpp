#include "in_store.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

bool TInStoreTableInfoConstructor::DoInitialize(const TTablesStorage::TTableReadGuard& originalTableInfo, const TInitializationContext& iContext, IErrorCollector& errors) {
    const auto storePathId = originalTableInfo->GetOlapStorePathIdVerified();
    TPath storePath = TPath::Init(storePathId, iContext.GetSSOperationContext()->SS);
    {
        TPath::TChecker checks = storePath.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .IsOlapStore()
            .NotUnderOperation();

        if (!checks) {
            errors.AddError(checks.GetError());
            return false;
        }
    }

    Y_ABORT_UNLESS(iContext.GetSSOperationContext()->SS->OlapStores.contains(storePathId));
    StoreInfo = iContext.GetSSOperationContext()->SS->OlapStores.at(storePathId);
    return true;
}

bool TInStoreTableInfoConstructor::DoCustomStartToSS(const TTablesStorage::TTableReadGuard& tableInfo, TStartSSContext& context, IErrorCollector& /*errors*/) {
    const auto storePathId = tableInfo->GetOlapStorePathIdVerified();
    TPath storePath = TPath::Init(storePathId, context.GetSSOperationContext()->SS);

    Y_ABORT_UNLESS(StoreInfo->ColumnTables.contains((*context.GetObjectPath())->PathId));
    StoreInfo->ColumnTablesUnderOperation.insert((*context.GetObjectPath())->PathId);

    // Sequentially chain operations in the same olap store
    if (context.GetSSOperationContext()->SS->Operations.contains(storePath.Base()->LastTxId)) {
        context.GetSSOperationContext()->OnComplete.Dependence(storePath.Base()->LastTxId, (*context.GetObjectPath())->LastTxId);
    }
    storePath.Base()->LastTxId = (*context.GetObjectPath())->LastTxId;
    context.GetSSOperationContext()->SS->PersistLastTxId(*context.GetDB(), storePath.Base());
    return true;
}

const NKikimrSchemeOp::TColumnTableSchema* TInStoreTableInfoConstructor::GetTableSchema(const TTablesStorage::TTableExtractedGuard& tableInfo, IErrorCollector& errors) const {
    if (!StoreInfo->SchemaPresets.count(tableInfo->Description.GetSchemaPresetId())) {
        errors.AddError(NKikimrScheme::StatusSchemeError, "No preset for in-store column table");
        return nullptr;
    }

    auto& preset = StoreInfo->SchemaPresets.at(tableInfo->Description.GetSchemaPresetId());
    auto& presetProto = StoreInfo->GetDescription().GetSchemaPresets(preset.GetProtoIndex());
    if (!presetProto.HasSchema()) {
        errors.AddError(NKikimrScheme::StatusSchemeError, "No schema in preset for in-store column table");
        return nullptr;
    }
    return &presetProto.GetSchema();
}

}