#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> DropColumnTableWithLocalIndexes(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    const TString& parentPathStr = tx.GetWorkingDir();
    const TString& tableName = tx.GetDrop().GetName();

    TPath tablePath = TPath::Resolve(parentPathStr, context.SS).Dive(tableName);
    if (!tablePath.IsResolved() || tablePath.IsDeleted() || !tablePath->IsColumnTable()) {
        result.push_back(CreateDropColumnTable(NextPartId(nextId, result), tx));
        return result;
    }

    // First, drop the table itself - this will mark it as under operation
    result.push_back(CreateDropColumnTable(NextPartId(nextId, result), tx));

    // Then, drop local index children after the table is marked as under operation
    for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
        TPath childPath = tablePath.Child(childName);
        if (!childPath.IsResolved() || !childPath.Base()->IsTableIndex() || childPath.Base()->Dropped()) {
            continue;
        }

        if (!context.SS->Indexes.contains(childPathId)) {
            continue;
        }

        auto indexInfo = context.SS->Indexes.at(childPathId);
        if (!TTableIndexInfo::IsLocalIndex(indexInfo->Type)) {
            continue;
        }

        // Create sub-operation to drop the local index
        auto dropIndexScheme = TransactionTemplate(
            parentPathStr + "/" + tableName,
            NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
        dropIndexScheme.SetInternal(true);
        dropIndexScheme.MutableDrop()->SetName(childName);

        result.push_back(CreateDropLocalIndex(NextPartId(nextId, result), dropIndexScheme));
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
