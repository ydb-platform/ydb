#include "schemeshard__index_shadow_data_repair.h"
#include "schemeshard__operation_common.h"
#include "index/index_build_info.h"

#include <ydb/core/base/table_index.h>

#include <vector>

namespace NKikimr::NSchemeShard {

TIndexShadowDataRepairCheck CheckIndexShadowDataRepair(TSchemeShard* ss, TPathId pathId) {
    using EStatus = EIndexShadowDataRepairStatus;

    const auto path = TPath::Init(pathId, ss);
    if (!path.IsResolved() || path.IsDeleted()) {
        return {EStatus::Done, "Implementation table no longer exists"};
    }
    if (!path.Base()->IsTable() || path.LeafName() != NTableIndex::ImplTable) {
        return {EStatus::Invalid, "Expected an index implementation table"};
    }

    const auto indexPath = path.Parent();
    if (!indexPath.IsResolved() || !indexPath.Base()->IsTableIndex()) {
        return {EStatus::Invalid, "Expected a table index parent"};
    }
    const auto* childId = indexPath.Base()->FindChild(NTableIndex::ImplTable);
    if (!childId || *childId != pathId) {
        return {EStatus::Invalid, "Implementation table is no longer the index's current child"};
    }
    const auto index = ss->Indexes.find(indexPath.Base()->PathId);
    if (index == ss->Indexes.end() || index->second->Type != NKikimrSchemeOp::EIndexTypeGlobalUnique) {
        return {EStatus::Invalid, "Expected a global unique index"};
    }
    const auto table = ss->Tables.find(pathId);
    if (table == ss->Tables.end()) {
        return {EStatus::Invalid, "Implementation table metadata is missing"};
    }
    if (!table->second->PartitionConfig().HasShadowData()) {
        return {EStatus::Done, "ShadowData is already cleared"};
    }

    const auto basePath = indexPath.Parent();
    if (!basePath.IsResolved() || !basePath.Base()->IsTable()) {
        return {EStatus::Invalid, "Expected an indexed table"};
    }
    for (const auto& current : {basePath, indexPath, path}) {
        if (current.Base()->IsMigrated()) {
            return {EStatus::Done, "Path has migrated to another SchemeShard"};
        }
        const auto checks = current.Check();
        checks.NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .NotDeleted()
            .NotUnderDeleting()
            .NotUnderOperation();
        if (!checks) {
            return {EStatus::Deferred, checks.GetError()};
        }
        const auto currentId = current.Base()->PathId;
        if (ss->LockedPaths.contains(currentId)) {
            return {EStatus::Deferred, "Path is locked"};
        }
        const auto snapshots = ss->TablesWithSnapshots.find(currentId);
        if (snapshots != ss->TablesWithSnapshots.end() && !snapshots->second.empty()) {
            return {EStatus::Deferred, "Table has an active snapshot"};
        }
        const auto currentTable = ss->Tables.find(currentId);
        if (currentTable != ss->Tables.end()
            && (currentTable->second->AlterData || !currentTable->second->GetSplitOpsInFlight().empty()))
        {
            return {EStatus::Deferred, "Table has a pending alteration or split/merge"};
        }
    }
    if (index->second->AlterData) {
        return {EStatus::Deferred, "Index has a pending alteration"};
    }
    if (index->second->State != NKikimrSchemeOp::EIndexStateReady) {
        return {EStatus::Deferred, "Index is not ready"};
    }
    for (const auto& [_, build] : ss->IndexBuilds) {
        if (build->TablePathId == basePath.Base()->PathId && !build->IsFinished()) {
            return {EStatus::Deferred, "Index build has not finished"};
        }
    }
    return {EStatus::Ready, {}};
}

ISubOperation::TPtr CreateRepairIndexShadowData(TOperationId id, const TTxTransaction& tx, TOperationContext& context) {
    const auto& alter = tx.GetAlterTable();
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    alter.GetReflection()->ListFields(alter, &fields);
    if (!tx.GetInternal() || !tx.HasAlterTable() || !alter.HasPathId() || fields.size() != 1) {
        return CreateReject(id, NKikimrScheme::StatusInvalidParameter,
            "ShadowData repair requires an internal request containing only AlterTable.PathId");
    }

    const auto pathId = TPathId::FromProto(alter.GetPathId());
    const auto check = CheckIndexShadowDataRepair(context.SS, pathId);
    switch (check.Status) {
        case EIndexShadowDataRepairStatus::Done:
            return CreateReject(id, NKikimrScheme::StatusAlreadyExists, check.Reason);
        case EIndexShadowDataRepairStatus::Deferred:
            return CreateReject(id, NKikimrScheme::StatusMultipleModifications, check.Reason);
        case EIndexShadowDataRepairStatus::Invalid:
            return CreateReject(id, NKikimrScheme::StatusInvalidParameter, check.Reason);
        case EIndexShadowDataRepairStatus::Ready:
            break;
    }

    const auto path = TPath::Init(pathId, context.SS);
    auto repair = TransactionTemplate(path.Parent().PathString(), NKikimrSchemeOp::ESchemeOpRepairIndexShadowData);
    repair.SetInternal(true);
    *repair.MutableAlterTable()->MutablePathId() = alter.GetPathId();
    auto* config = repair.MutableAlterTable()->MutablePartitionConfig();
    config->SetShadowData(false);
    *config->MutableCompactionPolicy() = context.SS->Tables.at(pathId)->PartitionConfig().GetCompactionPolicy();
    config->MutableCompactionPolicy()->SetKeepEraseMarkers(false);
    return CreateFinalizeBuildIndexImplTable(id, repair);
}

} // namespace NKikimr::NSchemeShard
