#include "object.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusion<std::shared_ptr<ISSEntityUpdate>> TColumnTableEntity::DoRestoreUpdate(const TUpdateRestoreContext& context) const {
    auto& ssContext = *context.GetSSOperationContext();
    auto tableInfo = ssContext.SS->ColumnTables.GetVerifiedPtr(GetPathId());
    if (!tableInfo) {
        return TConclusionStatus::Fail("object not exists");
    }
    if (!tableInfo->AlterData) {
        return TConclusionStatus::Fail("object not in update");
    }
    if (!tableInfo->AlterData->AlterBody) {
        return TConclusionStatus::Fail("object is incorrect in update");
    }
    NKikimrSchemeOp::TModifyScheme mScheme;
    *mScheme.MutableAlterColumnTable() = *tableInfo->AlterData->AlterBody;
    mScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
    TUpdateInitializationContext uContext(&context.GetOriginalEntity(), context.GetSSOperationContext(), &mScheme, context.GetTxId());
    return DoCreateUpdateImpl(uContext);
}

TConclusionStatus TColumnTableEntity::DoInitialize(const TEntityInitializationContext& context) {
    TableInfo = context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(GetPathId());
    return DoInitializeImpl(context);
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TColumnTableEntity::DoCreateUpdate(const TUpdateInitializationContext& context) const {
    if (GetTableInfo()->AlterVersion == 0) {
        return NKikimr::TConclusionStatus::Fail("Table is not created yet");
    }
    if (GetTableInfo()->AlterData) {
        return NKikimr::TConclusionStatus::Fail("There's another Alter in flight");
    }
    return DoCreateUpdateImpl(context);
}

}