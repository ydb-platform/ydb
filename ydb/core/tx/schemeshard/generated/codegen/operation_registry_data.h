#pragma once

#define SCHEME_OPERATIONS(OP) \
OP(ESchemeOpMkDir, Implemented, return {CreateMkDir(op.NextPartId(), tx)};) \
OP(ESchemeOpRmDir, Implemented, return {CreateRmDir(op.NextPartId(), tx)};) \
OP(ESchemeOpModifyACL, Implemented, return {CreateModifyACL(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterUserAttributes, Implemented, return {CreateAlterUserAttrs(op.NextPartId(), tx)};) \
OP(ESchemeOpForceDropUnsafe, Implemented, return {CreateForceDropUnsafe(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateTable, Implemented, \
    if (tx.GetCreateTable().HasCopyFromTable()) { \
        return CreateCopyTable(op.NextPartId(), tx, context); \
    } \
    return {CreateNewTable(op.NextPartId(), tx)}; \
) \
OP(ESchemeOpAlterTable, Implemented, return CreateConsistentAlterTable(op.NextPartId(), tx, context);) \
OP(ESchemeOpSplitMergeTablePartitions, Implemented, return {CreateSplitMerge(op.NextPartId(), tx)};) \
OP(ESchemeOpBackup, Implemented, return {CreateBackup(op.NextPartId(), tx)};) \
OP(ESchemeOpRestore, Implemented, return {CreateRestore(op.NextPartId(), tx)};) \
OP(ESchemeOpDropTable, Implemented, return CreateDropIndexedTable(op.NextPartId(), tx, context);) \
OP(ESchemeOpCreateIndexedTable, Implemented, return CreateIndexedTable(op.NextPartId(), tx, context);) \
OP(ESchemeOpCreateTableIndex, Internal, "is handled as part of ESchemeOpCreateIndexedTable") \
OP(ESchemeOpDropTableIndex, Internal, "is handled as part of ESchemeOpDropTable") \
OP(ESchemeOpCreateConsistentCopyTables, Implemented, \
    return CreateConsistentCopyTables(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpCreateRtmrVolume, Implemented, return {CreateNewRTMR(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateColumnStore, Implemented, return {CreateNewOlapStore(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterColumnStore, Implemented, return {CreateAlterOlapStore(op.NextPartId(), tx)};) \
OP(ESchemeOpDropColumnStore, Implemented, return {CreateDropOlapStore(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateColumnTable, Implemented, \
    if (tx.GetCreateColumnTable().HasCopyFromTable()) { \
        return {CreateReadOnlyCopyColumnTable(op.NextPartId(), tx)}; \
    } \
    return CreateColumnTableWithLocalIndexes(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpAlterColumnTable, Implemented, return AlterColumnTableWithLocalIndexes(op.NextPartId(), tx, context);) \
OP(ESchemeOpDropColumnTable, Implemented, return DropColumnTableWithLocalIndexes(op.NextPartId(), tx, context);) \
OP(ESchemeOpCreatePersQueueGroup, Implemented, return {CreateNewPQ(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterPersQueueGroup, Implemented, return {CreateAlterPQ(op.NextPartId(), tx)};) \
OP(ESchemeOpDropPersQueueGroup, Implemented, return {CreateDropPQ(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateSolomonVolume, Implemented, return {CreateNewSolomon(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterSolomonVolume, Implemented, return {CreateAlterSolomon(op.NextPartId(), tx)};) \
OP(ESchemeOpDropSolomonVolume, Implemented, return {CreateDropSolomon(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateSubDomain, Implemented, return {CreateSubDomain(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterSubDomain, Implemented, return CreateCompatibleSubdomainAlter(op.NextPartId(), tx, context);) \
OP(ESchemeOpDropSubDomain, Implemented, return {CreateDropSubdomain(op.NextPartId(), tx)};) \
OP(ESchemeOpForceDropSubDomain, Implemented, \
    return {CreateCompatibleSubdomainDrop(context.SS, op.NextPartId(), tx)}; \
) \
OP(ESchemeOpCreateExtSubDomain, Implemented, return {CreateExtSubDomain(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterExtSubDomain, Implemented, \
    return CreateCompatibleAlterExtSubDomain(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpAlterExtSubDomainCreateHive, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpForceDropExtSubDomain, Implemented, return {CreateForceDropExtSubDomain(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateKesus, Implemented, return {CreateNewKesus(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterKesus, Implemented, return {CreateAlterKesus(op.NextPartId(), tx)};) \
OP(ESchemeOpDropKesus, Implemented, return {CreateDropKesus(op.NextPartId(), tx)};) \
OP(ESchemeOpUpgradeSubDomain, Implemented, return {CreateUpgradeSubDomain(op.NextPartId(), tx)};) \
OP(ESchemeOpUpgradeSubDomainDecision, Implemented, return {CreateUpgradeSubDomainDecision(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateColumnBuild, Implemented, return {CreateBuildColumn(op.NextPartId(), tx, context)};) \
OP(ESchemeOpDropColumnBuild, Implemented, return {DropBuildColumn(op.NextPartId(), tx, context)};) \
OP(ESchemeOpCreateIndexBuild, Implemented, return CreateBuildIndex(op.NextPartId(), tx, context);) \
OP(ESchemeOpCreateLock, Implemented, return {CreateLock(op.NextPartId(), tx)};) \
OP(ESchemeOpDropLock, Implemented, return {DropLock(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateBlockStoreVolume, Implemented, return {CreateNewBSV(op.NextPartId(), tx)};) \
OP(ESchemeOpAssignBlockStoreVolume, Implemented, return {CreateAssignBSV(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterBlockStoreVolume, Implemented, return {CreateAlterBSV(op.NextPartId(), tx)};) \
OP(ESchemeOpDropBlockStoreVolume, Implemented, return {CreateDropBSV(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateFileStore, Implemented, return {CreateNewFileStore(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterFileStore, Implemented, return {CreateAlterFileStore(op.NextPartId(), tx)};) \
OP(ESchemeOpDropFileStore, Implemented, return {CreateDropFileStore(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterLogin, Implemented, return {CreateAlterLogin(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateSequence, Implemented, return {CreateNewSequence(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterSequence, Implemented, return {CreateAlterSequence(op.NextPartId(), tx)};) \
OP(ESchemeOpDropSequence, Implemented, return {CreateDropSequence(op.NextPartId(), tx)};) \
OP(ESchemeOpApplyIndexBuild, Implemented, return ApplyBuildIndex(op.NextPartId(), tx, context);) \
OP(ESchemeOpAlterTableIndex, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpInitiateBuildIndexImplTable, Implemented, \
    THashSet<TString> localSequences; \
    for (const auto& col : tx.GetCreateTable().GetColumns()) { \
        if (col.HasDefaultFromSequence()) { \
            localSequences.insert(col.GetDefaultFromSequence()); \
        } \
    } \
    return {CreateInitializeBuildIndexImplTable(op.NextPartId(), tx, localSequences)}; \
) \
OP(ESchemeOpFinalizeBuildIndexImplTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpInitiateBuildIndexMainTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpFinalizeBuildIndexMainTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpPrepareIndexValidation, Implemented, return {CreatePrepareIndexValidation(op.NextPartId(), tx)};) \
OP(ESchemeOpCancelIndexBuild, Implemented, return CancelBuildIndex(op.NextPartId(), tx, context);) \
OP(ESchemeOpDropIndex, Implemented, return CreateDropIndex(op.NextPartId(), tx, context);) \
OP(ESchemeOpDropTableIndexAtMainTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpCreateCdcStream, Implemented, return CreateNewCdcStream(op.NextPartId(), tx, context);) \
OP(ESchemeOpCreateCdcStreamImpl, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpCreateCdcStreamAtTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpAlterCdcStream, Implemented, return CreateAlterCdcStream(op.NextPartId(), tx, context);) \
OP(ESchemeOpAlterCdcStreamImpl, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpAlterCdcStreamAtTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpDropCdcStream, Implemented, return CreateDropCdcStream(op.NextPartId(), tx, context);) \
OP(ESchemeOpDropCdcStreamImpl, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpDropCdcStreamAtTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpRotateCdcStream, Implemented, return CreateRotateCdcStream(op.NextPartId(), tx, context);) \
OP(ESchemeOpRotateCdcStreamImpl, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpRotateCdcStreamAtTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOp_DEPRECATED_35, Deprecated) \
OP(ESchemeOpMoveTable, Implemented, return CreateConsistentMoveTable(op.NextPartId(), tx, context);) \
OP(ESchemeOpMoveTableIndex, Implemented, return {CreateMoveTableIndex(op.NextPartId(), tx)};) \
OP(ESchemeOpMoveIndex, Implemented, \
    const auto& moving = tx.GetMoveIndex(); \
    TPath tablePath = TPath::Resolve(moving.GetTablePath(), context.SS); \
    if (tablePath.IsResolved() && !tablePath.IsDeleted() && tablePath->IsColumnTable()) { \
        return CreateConsistentMoveLocalIndex(op.NextPartId(), tx, context); \
    } \
    return CreateConsistentMoveIndex(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpMoveSequence, Implemented, return {CreateMoveSequence(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateReplication, Implemented, return {CreateNewReplication(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterReplication, Implemented, return {CreateAlterReplication(op.NextPartId(), tx)};) \
OP(ESchemeOpDropReplication, Implemented, return {CreateDropReplication(op.NextPartId(), tx, false)};) \
OP(ESchemeOpDropReplicationCascade, Implemented, return {CreateDropReplication(op.NextPartId(), tx, true)};) \
OP(ESchemeOpCreateTransfer, Implemented, return {CreateNewTransfer(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterTransfer, Implemented, return {CreateAlterTransfer(op.NextPartId(), tx)};) \
OP(ESchemeOpDropTransfer, Implemented, return {CreateDropTransfer(op.NextPartId(), tx, false)};) \
OP(ESchemeOpDropTransferCascade, Implemented, return {CreateDropTransfer(op.NextPartId(), tx, true)};) \
OP(ESchemeOpCreateBlobDepot, Implemented, return {CreateNewBlobDepot(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterBlobDepot, Stub, return {CreateAlterBlobDepot(op.NextPartId(), tx)};) \
OP(ESchemeOpDropBlobDepot, Stub, return {CreateDropBlobDepot(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateExternalTable, Implemented, return CreateNewExternalTable(op.NextPartId(), tx, context);) \
OP(ESchemeOpDropExternalTable, Implemented, return {CreateDropExternalTable(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterExternalTable, Unsupported) \
OP(ESchemeOpCreateExternalDataSource, Implemented, \
    return CreateNewExternalDataSource(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpDropExternalDataSource, Implemented, return {CreateDropExternalDataSource(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterExternalDataSource, Unsupported) \
OP(ESchemeOpCreateView, Implemented, return {CreateNewView(op.NextPartId(), tx)};) \
OP(ESchemeOpDropView, Implemented, return {CreateDropView(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterView, Unsupported) \
OP(ESchemeOpCreateContinuousBackup, Implemented, return CreateNewContinuousBackup(op.NextPartId(), tx, context);) \
OP(ESchemeOpAlterContinuousBackup, Implemented, return CreateAlterContinuousBackup(op.NextPartId(), tx, context);) \
OP(ESchemeOpDropContinuousBackup, Implemented, return CreateDropContinuousBackup(op.NextPartId(), tx, context);) \
OP(ESchemeOpCreateResourcePool, Implemented, return {CreateNewResourcePool(op.NextPartId(), tx)};) \
OP(ESchemeOpDropResourcePool, Implemented, return {CreateDropResourcePool(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterResourcePool, Implemented, return {CreateAlterResourcePool(op.NextPartId(), tx)};) \
OP(ESchemeOpRestoreMultipleIncrementalBackups, Retired, \
    return CreateRestoreMultipleIncrementalBackups(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpRestoreIncrementalBackupAtTable, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpCreateBackupCollection, Implemented, return {CreateNewBackupCollection(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterBackupCollection, Unsupported) \
OP(ESchemeOpDropBackupCollection, Implemented, \
    return CreateDropBackupCollectionCascade(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpBackupBackupCollection, Implemented, \
    return CreateBackupBackupCollection(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpBackupIncrementalBackupCollection, Implemented, \
    return CreateBackupIncrementalBackupCollection(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpCreateLongIncrementalBackupOp, Internal, "multipart operations are handled before, also they require transaction details") \
OP(ESchemeOpCreateFullBackupOp, Implemented, \
    return {CreateNewFullBackupOp(op.NextPartId(), tx)}; \
) \
OP(ESchemeOpRestoreBackupCollection, Implemented, \
    return CreateRestoreBackupCollection(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpCreateLongIncrementalRestoreOp, Implemented, \
    return {CreateLongIncrementalRestoreOpControlPlane(op.NextPartId(), tx)}; \
) \
OP(ESchemeOpCreateSysView, Implemented, return {CreateNewSysView(op.NextPartId(), tx)};) \
OP(ESchemeOpDropSysView, Implemented, return {CreateDropSysView(op.NextPartId(), tx)};) \
OP(ESchemeOpChangePathState, Implemented, return CreateChangePathState(op.NextPartId(), tx, context);) \
OP(ESchemeOpIncrementalRestoreLockTargets, Implemented, \
    return CreateIncrementalRestoreLockTargets(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpIncrementalRestoreUnlockTargets, Implemented, \
    return CreateIncrementalRestoreUnlockTargets(op.NextPartId(), tx, context); \
) \
OP(ESchemeOpIncrementalRestoreFinalize, Implemented, \
    return {CreateIncrementalRestoreFinalize(op.NextPartId(), tx)}; \
) \
OP(ESchemeOpCreateSecret, Implemented, return {CreateNewSecret(op.NextPartId(), tx, context)};) \
OP(ESchemeOpAlterSecret, Implemented, return {CreateAlterSecret(op.NextPartId(), tx)};) \
OP(ESchemeOpDropSecret, Implemented, return {CreateDropSecret(op.NextPartId(), tx)};) \
OP(ESchemeOpCreateStreamingQuery, Implemented, return {CreateNewStreamingQuery(op.NextPartId(), tx, context)};) \
OP(ESchemeOpDropStreamingQuery, Implemented, return {CreateDropStreamingQuery(op.NextPartId(), tx)};) \
OP(ESchemeOpAlterStreamingQuery, Implemented, return {CreateAlterStreamingQuery(op.NextPartId(), tx)};) \
OP(ESchemeOpTruncateTable, Implemented, return CreateConsistentTruncateTable(op.NextPartId(), tx, context);) \
OP(ESchemeOpCreateTestShardSet, Implemented, return {CreateNewTestShardSet(op.NextPartId(), tx)};) \
OP(ESchemeOpDropTestShardSet, Implemented, return {CreateDropTestShardSet(op.NextPartId(), tx)};)

#define SCHEME_OPERATION_RECOVERY(UNSUPPORTED, TRANSIENT) \
UNSUPPORTED(TxAlterView, ESchemeOpAlterView) \
UNSUPPORTED(TxAlterBackupCollection, ESchemeOpAlterBackupCollection) \
TRANSIENT(TxCreateContinuousBackup) \
TRANSIENT(TxAlterContinuousBackup) \
TRANSIENT(TxDropContinuousBackup)
