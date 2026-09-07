#include "schemeshard_operation_registry.h"

#include "schemeshard__operation.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {
namespace NOperationFactories {

using TParts = TSchemeOperationParts;
using TTxTransaction = NKikimrSchemeOp::TModifyScheme;

TParts MakeMkDir(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateMkDir(op.NextPartId(), tx)};
}

TParts MakeRmDir(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateRmDir(op.NextPartId(), tx)};
}

TParts MakeModifyACL(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateModifyACL(op.NextPartId(), tx)};
}

TParts MakeAlterUserAttributes(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterUserAttrs(op.NextPartId(), tx)};
}

TParts MakeForceDropUnsafe(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateForceDropUnsafe(op.NextPartId(), tx)};
}

TParts MakeCreateTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    if (tx.GetCreateTable().HasCopyFromTable()) {
        return CreateCopyTable(op.NextPartId(), tx, context);
    }
    return {CreateNewTable(op.NextPartId(), tx)};
}

TParts MakeAlterTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateConsistentAlterTable(op.NextPartId(), tx, context);
}

TParts MakeSplitMergeTablePartitions(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateSplitMerge(op.NextPartId(), tx)};
}

TParts MakeBackup(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateBackup(op.NextPartId(), tx)};
}

TParts MakeRestore(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateRestore(op.NextPartId(), tx)};
}

TParts MakeDropTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateDropIndexedTable(op.NextPartId(), tx, context);
}

TParts MakeCreateIndexedTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateIndexedTable(op.NextPartId(), tx, context);
}

TParts MakeCreateConsistentCopyTables(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateConsistentCopyTables(op.NextPartId(), tx, context);
}

TParts MakeCreateRtmrVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewRTMR(op.NextPartId(), tx)};
}

TParts MakeCreateColumnStore(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewOlapStore(op.NextPartId(), tx)};
}

TParts MakeAlterColumnStore(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterOlapStore(op.NextPartId(), tx)};
}

TParts MakeDropColumnStore(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropOlapStore(op.NextPartId(), tx)};
}

TParts MakeCreateColumnTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    if (tx.GetCreateColumnTable().HasCopyFromTable()) {
        return {CreateReadOnlyCopyColumnTable(op.NextPartId(), tx)};
    }
    return CreateColumnTableWithLocalIndexes(op.NextPartId(), tx, context);
}

TParts MakeAlterColumnTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return AlterColumnTableWithLocalIndexes(op.NextPartId(), tx, context);
}

TParts MakeDropColumnTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return DropColumnTableWithLocalIndexes(op.NextPartId(), tx, context);
}

TParts MakeCreatePersQueueGroup(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewPQ(op.NextPartId(), tx)};
}

TParts MakeAlterPersQueueGroup(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterPQ(op.NextPartId(), tx)};
}

TParts MakeDropPersQueueGroup(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropPQ(op.NextPartId(), tx)};
}

TParts MakeCreateSolomonVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewSolomon(op.NextPartId(), tx)};
}

TParts MakeAlterSolomonVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterSolomon(op.NextPartId(), tx)};
}

TParts MakeDropSolomonVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropSolomon(op.NextPartId(), tx)};
}

TParts MakeCreateSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateSubDomain(op.NextPartId(), tx)};
}

TParts MakeAlterSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateCompatibleSubdomainAlter(op.NextPartId(), tx, context);
}

TParts MakeDropSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropSubdomain(op.NextPartId(), tx)};
}

TParts MakeForceDropSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return {CreateCompatibleSubdomainDrop(context.SS, op.NextPartId(), tx)};
}

TParts MakeCreateExtSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateExtSubDomain(op.NextPartId(), tx)};
}

TParts MakeAlterExtSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateCompatibleAlterExtSubDomain(op.NextPartId(), tx, context);
}

TParts MakeForceDropExtSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateForceDropExtSubDomain(op.NextPartId(), tx)};
}

TParts MakeCreateKesus(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewKesus(op.NextPartId(), tx)};
}

TParts MakeAlterKesus(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterKesus(op.NextPartId(), tx)};
}

TParts MakeDropKesus(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropKesus(op.NextPartId(), tx)};
}

TParts MakeUpgradeSubDomain(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateUpgradeSubDomain(op.NextPartId(), tx)};
}

TParts MakeUpgradeSubDomainDecision(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateUpgradeSubDomainDecision(op.NextPartId(), tx)};
}

TParts MakeCreateColumnBuild(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return {CreateBuildColumn(op.NextPartId(), tx, context)};
}

TParts MakeDropColumnBuild(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return {DropBuildColumn(op.NextPartId(), tx, context)};
}

TParts MakeCreateIndexBuild(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateBuildIndex(op.NextPartId(), tx, context);
}

TParts MakeCreateLock(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateLock(op.NextPartId(), tx)};
}

TParts MakeDropLock(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {DropLock(op.NextPartId(), tx)};
}

TParts MakeCreateBlockStoreVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewBSV(op.NextPartId(), tx)};
}

TParts MakeAssignBlockStoreVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAssignBSV(op.NextPartId(), tx)};
}

TParts MakeAlterBlockStoreVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterBSV(op.NextPartId(), tx)};
}

TParts MakeDropBlockStoreVolume(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropBSV(op.NextPartId(), tx)};
}

TParts MakeCreateFileStore(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewFileStore(op.NextPartId(), tx)};
}

TParts MakeAlterFileStore(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterFileStore(op.NextPartId(), tx)};
}

TParts MakeDropFileStore(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropFileStore(op.NextPartId(), tx)};
}

TParts MakeAlterLogin(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterLogin(op.NextPartId(), tx)};
}

TParts MakeCreateSequence(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewSequence(op.NextPartId(), tx)};
}

TParts MakeAlterSequence(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterSequence(op.NextPartId(), tx)};
}

TParts MakeDropSequence(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropSequence(op.NextPartId(), tx)};
}

TParts MakeApplyIndexBuild(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return ApplyBuildIndex(op.NextPartId(), tx, context);
}

TParts MakeInitiateBuildIndexImplTable(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    THashSet<TString> localSequences;
    for (const auto& col : tx.GetCreateTable().GetColumns()) {
        if (col.HasDefaultFromSequence()) {
            localSequences.insert(col.GetDefaultFromSequence());
        }
    }
    return {CreateInitializeBuildIndexImplTable(op.NextPartId(), tx, localSequences)};
}

TParts MakePrepareIndexValidation(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreatePrepareIndexValidation(op.NextPartId(), tx)};
}

TParts MakeCancelIndexBuild(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CancelBuildIndex(op.NextPartId(), tx, context);
}

TParts MakeDropIndex(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateDropIndex(op.NextPartId(), tx, context);
}

TParts MakeCreateCdcStream(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateNewCdcStream(op.NextPartId(), tx, context);
}

TParts MakeAlterCdcStream(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateAlterCdcStream(op.NextPartId(), tx, context);
}

TParts MakeDropCdcStream(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateDropCdcStream(op.NextPartId(), tx, context);
}

TParts MakeRotateCdcStream(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateRotateCdcStream(op.NextPartId(), tx, context);
}

TParts MakeMoveTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateConsistentMoveTable(op.NextPartId(), tx, context);
}

TParts MakeMoveTableIndex(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateMoveTableIndex(op.NextPartId(), tx)};
}

TParts MakeMoveIndex(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    const auto& moving = tx.GetMoveIndex();
    TPath tablePath = TPath::Resolve(moving.GetTablePath(), context.SS);
    if (tablePath.IsResolved() && !tablePath.IsDeleted() && tablePath->IsColumnTable()) {
        return CreateConsistentMoveLocalIndex(op.NextPartId(), tx, context);
    }
    return CreateConsistentMoveIndex(op.NextPartId(), tx, context);
}

TParts MakeMoveSequence(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateMoveSequence(op.NextPartId(), tx)};
}

TParts MakeCreateReplication(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewReplication(op.NextPartId(), tx)};
}

TParts MakeAlterReplication(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterReplication(op.NextPartId(), tx)};
}

TParts MakeDropReplication(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropReplication(op.NextPartId(), tx, false)};
}

TParts MakeDropReplicationCascade(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropReplication(op.NextPartId(), tx, true)};
}

TParts MakeCreateTransfer(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewTransfer(op.NextPartId(), tx)};
}

TParts MakeAlterTransfer(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterTransfer(op.NextPartId(), tx)};
}

TParts MakeDropTransfer(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropTransfer(op.NextPartId(), tx, false)};
}

TParts MakeDropTransferCascade(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropTransfer(op.NextPartId(), tx, true)};
}

TParts MakeCreateBlobDepot(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewBlobDepot(op.NextPartId(), tx)};
}

TParts MakeAlterBlobDepot(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterBlobDepot(op.NextPartId(), tx)};
}

TParts MakeDropBlobDepot(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropBlobDepot(op.NextPartId(), tx)};
}

TParts MakeCreateExternalTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateNewExternalTable(op.NextPartId(), tx, context);
}

TParts MakeDropExternalTable(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropExternalTable(op.NextPartId(), tx)};
}

TParts MakeCreateExternalDataSource(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateNewExternalDataSource(op.NextPartId(), tx, context);
}

TParts MakeDropExternalDataSource(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropExternalDataSource(op.NextPartId(), tx)};
}

TParts MakeCreateView(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewView(op.NextPartId(), tx)};
}

TParts MakeDropView(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropView(op.NextPartId(), tx)};
}

TParts MakeCreateContinuousBackup(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateNewContinuousBackup(op.NextPartId(), tx, context);
}

TParts MakeAlterContinuousBackup(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateAlterContinuousBackup(op.NextPartId(), tx, context);
}

TParts MakeDropContinuousBackup(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateDropContinuousBackup(op.NextPartId(), tx, context);
}

TParts MakeCreateResourcePool(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewResourcePool(op.NextPartId(), tx)};
}

TParts MakeDropResourcePool(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropResourcePool(op.NextPartId(), tx)};
}

TParts MakeAlterResourcePool(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterResourcePool(op.NextPartId(), tx)};
}

TParts MakeRestoreMultipleIncrementalBackups(
        const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateRestoreMultipleIncrementalBackups(op.NextPartId(), tx, context);
}

TParts MakeCreateBackupCollection(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewBackupCollection(op.NextPartId(), tx)};
}

TParts MakeDropBackupCollection(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateDropBackupCollectionCascade(op.NextPartId(), tx, context);
}

TParts MakeBackupBackupCollection(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateBackupBackupCollection(op.NextPartId(), tx, context);
}

TParts MakeBackupIncrementalBackupCollection(
        const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateBackupIncrementalBackupCollection(op.NextPartId(), tx, context);
}

TParts MakeCreateFullBackupOp(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewFullBackupOp(op.NextPartId(), tx)};
}

TParts MakeRestoreBackupCollection(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateRestoreBackupCollection(op.NextPartId(), tx, context);
}

TParts MakeCreateLongIncrementalRestoreOp(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateLongIncrementalRestoreOpControlPlane(op.NextPartId(), tx)};
}

TParts MakeCreateSysView(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewSysView(op.NextPartId(), tx)};
}

TParts MakeDropSysView(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropSysView(op.NextPartId(), tx)};
}

TParts MakeChangePathState(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateChangePathState(op.NextPartId(), tx, context);
}

TParts MakeIncrementalRestoreLockTargets(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateIncrementalRestoreLockTargets(op.NextPartId(), tx, context);
}

TParts MakeIncrementalRestoreUnlockTargets(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateIncrementalRestoreUnlockTargets(op.NextPartId(), tx, context);
}

TParts MakeIncrementalRestoreFinalize(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateIncrementalRestoreFinalize(op.NextPartId(), tx)};
}

TParts MakeCreateSecret(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return {CreateNewSecret(op.NextPartId(), tx, context)};
}

TParts MakeAlterSecret(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterSecret(op.NextPartId(), tx)};
}

TParts MakeDropSecret(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropSecret(op.NextPartId(), tx)};
}

TParts MakeCreateStreamingQuery(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return {CreateNewStreamingQuery(op.NextPartId(), tx, context)};
}

TParts MakeDropStreamingQuery(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropStreamingQuery(op.NextPartId(), tx)};
}

TParts MakeAlterStreamingQuery(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateAlterStreamingQuery(op.NextPartId(), tx)};
}

TParts MakeTruncateTable(const TOperation& op, const TTxTransaction& tx, TOperationContext& context) {
    return CreateConsistentTruncateTable(op.NextPartId(), tx, context);
}

TParts MakeCreateTestShardSet(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateNewTestShardSet(op.NextPartId(), tx)};
}

TParts MakeDropTestShardSet(const TOperation& op, const TTxTransaction& tx, TOperationContext&) {
    return {CreateDropTestShardSet(op.NextPartId(), tx)};
}

} // namespace NOperationFactories

TSchemeOperationParts MakeRegisteredOperationParts(
        const TOperation& op, const NKikimrSchemeOp::TModifyScheme& tx, TOperationContext& context) {
    const auto* entry = FindSchemeOperation(tx.GetOperationType());
    Y_ABORT_UNLESS(entry);
    if (entry->Factory) {
        return entry->Factory(op, tx, context);
    }
    if (entry->Support == ESchemeOperationSupport::Internal) {
        Y_ABORT("%s", entry->Reason);
    }
    if (entry->Support == ESchemeOperationSupport::Deprecated) {
        Y_ABORT("impossible");
    }
    AbortUnimplementedSchemeOperation(entry->Type);
}

} // namespace NKikimr::NSchemeShard
