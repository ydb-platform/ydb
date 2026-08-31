#include <ydb/core/tx/schemeshard/schemeshard__affected_paths_traits.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/core/tx/schemeshard/olap/operations/local_index_helpers.h>

namespace NKikimr::NSchemeShard {

using TAffectedESchemeOpDropColumnTable = TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpDropColumnTable>(
    TAffectedESchemeOpDropColumnTable,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // DropColumnTableWithLocalIndexes discovers and cascades over local-index children
    // (tablePath.Base()->GetChildren()) at execution time, so the drop is not just the
    // named table -- it is a cascade rooted there.
    const auto& drop = tx.GetDrop();
    return DeclareCascadeTargetByIdOrName(context.SS, tx.GetWorkingDir(), drop.GetName(),
        drop.HasId() ? drop.GetId() : 0);
}

} // namespace NOperation

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

        result.push_back(CreateDropColumnTableLocalIndex(NextPartId(nextId, result), dropIndexScheme));
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
