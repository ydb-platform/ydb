#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_path_element.h>

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <yql/essentials/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NSchemeShard {
namespace {

ISubOperation::TPtr FinalizeIndexImplTable(TOperationContext& context, const TPath& index, const TOperationId& partId, const TString& name, const TPathId& pathId, const NKikimrSchemeOp::TLockGuard& lockGuard,
        const NKikimrSchemeOp::TVectorIndexKmeansTreeDescription* vectorDescription, const TString& embeddingColumn) {
    TPath implTable = index.Child(name);
    {
        // To safely fill the TransactionTemplate below, we need to check if the table is valid.
        const auto checks = implTable.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .IsInsideTableIndexPath()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            return CreateReject(partId, checks.GetStatus(), checks.GetError());
        }
    }

    Y_ABORT_UNLESS(implTable->PathId == pathId);
    Y_ABORT_UNLESS(implTable.LeafName() == name);

    TTableInfo::TPtr table = context.SS->Tables.at(pathId);
    auto transaction = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexImplTable);

    auto operation = transaction.MutableAlterTable();
    operation->SetName(name);
    operation->MutablePartitionConfig()->MutableCompactionPolicy()->CopyFrom(table->PartitionConfig().GetCompactionPolicy());
    operation->MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(false);
    operation->MutablePartitionConfig()->SetShadowData(false);

    // For a vector index posting table, hand the shard the settings it needs to
    // build an in-memory HNSW index: this alter is the moment the uploaded
    // posting data becomes readable there, and the shard has no other source
    // for the metric/dimension or for the embedding column name. Skip when the
    // settings are still incomplete (e.g. vector dimension autodetect did not
    // resolve), matching how the build scans guard the settings they send.
    if (vectorDescription && !embeddingColumn.empty()) {
        TString unused;
        if (NKikimr::NKMeans::ValidateSettings(vectorDescription->GetSettings().settings(), unused)) {
            *operation->MutableVectorIndexKmeansTreeDescription() = *vectorDescription;
            operation->SetVectorIndexEmbeddingColumn(embeddingColumn);
            const TPath mainTable = index.Parent();
            mainTable.Base()->PathId.ToProto(operation->MutableVectorIndexTablePathId());
            operation->SetVectorIndexTablePath(mainTable.PathString());
            index.Base()->PathId.ToProto(operation->MutableVectorIndexPathId());
            operation->SetVectorIndexPath(index.PathString());
            for (const auto& [columnId, column] : table->Columns) {
                if (column.Name == embeddingColumn) {
                    operation->SetVectorIndexEmbeddingColumnId(columnId);
                    break;
                }
            }
        }
    }
    LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "FinalizeIndexImplTable HNSW: " << name
            << ", haveVectorDesc# " << (vectorDescription != nullptr)
            << ", embeddingColumn# '" << embeddingColumn << "'"
            << ", attached# " << operation->HasVectorIndexEmbeddingColumnId());

    if (implTable.IsLocked()) { // implTables for some type of indexes may be locked during build
        *transaction.MutableLockGuard() = lockGuard;
    }

    return CreateFinalizeBuildIndexImplTable(partId, transaction);
}

ISubOperation::TPtr DropIndexImplTable(const TPath& index, const TOperationId& nextId, const TOperationId& partId, const TString& name, const TPathId& pathId, const NKikimrSchemeOp::TLockGuard& lockGuard, bool& rejected) {
    TPath implTable = index.Child(name);
    {
        const auto checks = implTable.Check();
        checks.NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .IsInsideTableIndexPath()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            rejected = true;
            return CreateReject(nextId, checks.GetStatus(), checks.GetError());
        }
    }

    Y_ABORT_UNLESS(implTable->PathId == pathId);
    Y_ABORT_UNLESS(implTable.LeafName() == name);

    rejected = false;
    auto transaction = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
    if (implTable.IsLocked()) {
        // because some impl tables may be not locked, do not pass lock guard for them
        // otherwise `CheckLocks` check would fail
        *transaction.MutableLockGuard() = lockGuard;
    }
    auto operation = transaction.MutableDrop();
    operation->SetName(name);
    return CreateDropTable(partId, transaction);
}

}

TVector<ISubOperation::TPtr> ApplyBuildIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild);

    auto config = tx.GetApplyIndexBuild();
    TString tablePath = config.GetTablePath();
    TString indexName = config.GetIndexName();

    TPath table = TPath::Resolve(tablePath, context.SS);
    {
        // To safely fill the TransactionTemplate below, we need to check if the table is valid.
        const auto checks = table.Check();
        checks
            .IsAtLocalSchemeShard()
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    TVector<ISubOperation::TPtr> result;
    {
        auto finalize = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable);
        *finalize.MutableLockGuard() = tx.GetLockGuard();
        auto op = finalize.MutableFinalizeBuildIndexMainTable();
        op->SetTableName(table.LeafName());
        op->SetSnapshotTxId(config.GetSnapshotTxId());
        op->SetBuildIndexId(config.GetBuildIndexId());
        if (!indexName.empty()) {
            TPath index = table.Child(indexName);
            index.Base()->PathId.ToProto(op->MutableOutcome()->MutableApply()->MutableIndexPathId());
        }

        result.push_back(CreateFinalizeBuildIndexMainTable(NextPartId(nextId, result), finalize));
    }

    // IsBuildIndex()
    if (!indexName.empty()) {
        TPath index = table.Child(indexName);
        auto tableIndexAltering = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex);
        *tableIndexAltering.MutableLockGuard() = tx.GetLockGuard();
        auto alterIndex = tableIndexAltering.MutableAlterTableIndex();
        alterIndex->SetName(index.LeafName());
        alterIndex->SetState(NKikimrSchemeOp::EIndexState::EIndexStateReady);

        if (config.HasVectorIndexKmeansTreeDescription()) {
            *alterIndex->MutableVectorIndexKmeansTreeDescription() = config.GetVectorIndexKmeansTreeDescription();
        }

        result.push_back(CreateAlterTableIndex(NextPartId(nextId, result), tableIndexAltering));
    }

    // IsBuildIndex()
    if (!indexName.empty()) {
        TPath index = table.Child(indexName);
        Y_ABORT_UNLESS(index.Base()->GetChildren().size() >= 1);

        // The embedding column keeps its original base table name. Recover it
        // from the active build when available, or from the persistent index
        // schema once the transient build record has been removed. Both the
        // name and settings are needed for the eager posting-table HNSW build.
        TString embeddingColumn;
        if (config.HasVectorIndexKmeansTreeDescription()) {
            const auto buildIt = context.SS->IndexBuilds.find(TIndexBuildId(config.GetBuildIndexId()));
            if (buildIt != context.SS->IndexBuilds.end() && !buildIt->second->IndexColumns.empty()) {
                embeddingColumn = buildIt->second->IndexColumns.back();
            }

            // ApplyBuildIndex may run after the transient build record has
            // already been removed. The index schema is persistent and is the
            // authoritative fallback; without it the posting-table alter lacks
            // VectorIndexEmbeddingColumn, the eager build is silently skipped,
            // and the first query starts an extremely expensive lazy rebuild.
            if (embeddingColumn.empty()) {
                auto indexIt = context.SS->Indexes.find(index.Base()->PathId);
                if (indexIt != context.SS->Indexes.end()) {
                    const auto& indexInfo = indexIt->second->AlterData
                        ? indexIt->second->AlterData
                        : indexIt->second;
                    if (!indexInfo->IndexKeys.empty()) {
                        embeddingColumn = indexInfo->IndexKeys.back();
                    }
                }
            }
        }
        for (auto& indexChildItems : index.Base()->GetChildren()) {
            const auto& indexImplTableName = indexChildItems.first;
            const auto partId = NextPartId(nextId, result);
            if (NTableIndex::IsBuildImplTable(indexImplTableName)) {
                bool rejected = false;
                auto op = DropIndexImplTable(index, nextId, partId, indexImplTableName, indexChildItems.second, tx.GetLockGuard(), rejected);
                if (rejected) {
                    return {std::move(op)};
                }
                result.push_back(std::move(op));
            } else if (context.SS->TablesWithSnapshots.contains(indexChildItems.second)) {
                auto finalize = TransactionTemplate(index.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable);
                *finalize.MutableLockGuard() = tx.GetLockGuard();
                auto op = finalize.MutableFinalizeBuildIndexMainTable();
                op->SetTableName(indexImplTableName);
                op->SetSnapshotTxId(ui64(context.SS->TablesWithSnapshots.at(indexChildItems.second)));
                op->SetBuildIndexId(config.GetBuildIndexId());
                result.push_back(CreateFinalizeBuildIndexMainTable(partId, finalize));
            } else {
                // Only the posting table holds the vectors that get indexed.
                const bool isPostingTable = (indexImplTableName == NTableIndex::NKMeans::PostingTable);
                result.push_back(FinalizeIndexImplTable(context, index, partId, indexImplTableName, indexChildItems.second, tx.GetLockGuard(),
                    isPostingTable && config.HasVectorIndexKmeansTreeDescription() ? &config.GetVectorIndexKmeansTreeDescription() : nullptr,
                    isPostingTable ? embeddingColumn : TString{}));
            }
        }
    }

    return result;
}

TVector<ISubOperation::TPtr> CancelBuildIndex(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild);

    auto config = tx.GetCancelIndexBuild();
    TString tablePath = config.GetTablePath();
    TString indexName = config.GetIndexName();

    TPath table = TPath::Resolve(tablePath, context.SS);
    {
        // To safely fill the TransactionTemplate below, we need to check if the table is valid.
        const auto checks = table.Check();
        checks
            .IsAtLocalSchemeShard()
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            .NotUnderOperation();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    TVector<ISubOperation::TPtr> result;

    {
        auto finalize = TransactionTemplate(table.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable);
        *finalize.MutableLockGuard() = tx.GetLockGuard();

        auto op = finalize.MutableFinalizeBuildIndexMainTable();
        op->SetTableName(table.LeafName());
        op->SetSnapshotTxId(config.GetSnapshotTxId());
        op->SetBuildIndexId(config.GetBuildIndexId());

        // IsBuildIndex()
        if (!indexName.empty()) {
            TPath index = table.Child(indexName);
            index.Base()->PathId.ToProto(op->MutableOutcome()->MutableCancel()->MutableIndexPathId());
        }

        result.push_back(CreateFinalizeBuildIndexMainTable(NextPartId(nextId, result), finalize));
    }

    // IsBuildIndex()
    if (!indexName.empty()) {
        TPath index = table.Child(indexName);
        auto tableIndexDropping = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
        auto operation = tableIndexDropping.MutableDrop();
        operation->SetName(index.Base()->Name);

        result.push_back(CreateDropTableIndex(NextPartId(nextId, result), tableIndexDropping));

        Y_ABORT_UNLESS(index.Base()->GetChildren().size() >= 1);
        for (auto& indexChildItems : index.Base()->GetChildren()) {
            const auto partId = NextPartId(nextId, result);
            bool rejected = false;
            auto op = DropIndexImplTable(index, nextId, partId, indexChildItems.first, indexChildItems.second, tx.GetLockGuard(), rejected);
            if (rejected) {
                return {std::move(op)};
            }
            result.push_back(std::move(op));
        }
    }

    return result;
}

ISubOperation::TPtr DropBuildColumn(TOperationId id, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnBuild);

    auto config = tx.GetDropColumnBuild();

    const TPath tablePath = TPath::Resolve(config.GetSettings().GetTable(), context.SS);
    {
        // To safely fill the TransactionTemplate below, we need to check if the table is valid.
        const auto checks = tablePath.Check();
        checks
            .IsAtLocalSchemeShard()
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsTable()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            return CreateReject(id, checks.GetStatus(), checks.GetError());
        }
    }

    auto mainTableAlter = TransactionTemplate(tablePath.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);
    *mainTableAlter.MutableLockGuard() = tx.GetLockGuard();

    auto op = mainTableAlter.MutableAlterTable();
    op->SetName(tablePath.LeafName());

    for (const auto& col : config.GetSettings().Getcolumn()) {
        auto colInfo = op->AddDropColumns();
        colInfo->SetName(col.GetColumnName());
    }

    return CreateAlterTable(id, mainTableAlter);
}

}
}
