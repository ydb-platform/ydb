SCHEME_OP_IMPLEMENTED(ESchemeOpMkDir, return {CreateMkDir(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpRmDir, return {CreateRmDir(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpModifyACL, return {CreateModifyACL(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterUserAttributes, return {CreateAlterUserAttrs(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpForceDropUnsafe, return {CreateForceDropUnsafe(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateTable,
    if (tx.GetCreateTable().HasCopyFromTable()) {
        return CreateCopyTable(op.NextPartId(), tx, context); // Copy indexes table as well as common table
    }
    return {CreateNewTable(op.NextPartId(), tx)};
)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterTable, return CreateConsistentAlterTable(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpSplitMergeTablePartitions, return {CreateSplitMerge(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpBackup, return {CreateBackup(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpRestore, return {CreateRestore(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropTable, return CreateDropIndexedTable(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateIndexedTable, return CreateIndexedTable(op.NextPartId(), tx, context);)
SCHEME_OP_INTERNAL(ESchemeOpCreateTableIndex, "is handled as part of ESchemeOpCreateIndexedTable")
SCHEME_OP_INTERNAL(ESchemeOpDropTableIndex, "is handled as part of ESchemeOpDropTable")
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateConsistentCopyTables,
    return CreateConsistentCopyTables(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateRtmrVolume, return {CreateNewRTMR(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateColumnStore, return {CreateNewOlapStore(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterColumnStore, return {CreateAlterOlapStore(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropColumnStore, return {CreateDropOlapStore(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateColumnTable,
    if (tx.GetCreateColumnTable().HasCopyFromTable()) {
        return {CreateReadOnlyCopyColumnTable(op.NextPartId(), tx)};
    }
    return CreateColumnTableWithLocalIndexes(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterColumnTable, return AlterColumnTableWithLocalIndexes(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropColumnTable, return DropColumnTableWithLocalIndexes(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreatePersQueueGroup, return {CreateNewPQ(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterPersQueueGroup, return {CreateAlterPQ(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropPersQueueGroup, return {CreateDropPQ(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateSolomonVolume, return {CreateNewSolomon(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterSolomonVolume, return {CreateAlterSolomon(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropSolomonVolume, return {CreateDropSolomon(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateSubDomain, return {CreateSubDomain(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterSubDomain, return CreateCompatibleSubdomainAlter(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropSubDomain, return {CreateDropSubdomain(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpForceDropSubDomain,
    return {CreateCompatibleSubdomainDrop(context.SS, op.NextPartId(), tx)};
)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateExtSubDomain, return {CreateExtSubDomain(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterExtSubDomain,
    return CreateCompatibleAlterExtSubDomain(op.NextPartId(), tx, context);
)
SCHEME_OP_INTERNAL(ESchemeOpAlterExtSubDomainCreateHive, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpForceDropExtSubDomain, return {CreateForceDropExtSubDomain(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateKesus, return {CreateNewKesus(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterKesus, return {CreateAlterKesus(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropKesus, return {CreateDropKesus(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpUpgradeSubDomain, return {CreateUpgradeSubDomain(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpUpgradeSubDomainDecision, return {CreateUpgradeSubDomainDecision(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateColumnBuild, return {CreateBuildColumn(op.NextPartId(), tx, context)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropColumnBuild, return {DropBuildColumn(op.NextPartId(), tx, context)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateIndexBuild, return CreateBuildIndex(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateLock, return {CreateLock(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropLock, return {DropLock(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateBlockStoreVolume, return {CreateNewBSV(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAssignBlockStoreVolume, return {CreateAssignBSV(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterBlockStoreVolume, return {CreateAlterBSV(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropBlockStoreVolume, return {CreateDropBSV(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateFileStore, return {CreateNewFileStore(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterFileStore, return {CreateAlterFileStore(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropFileStore, return {CreateDropFileStore(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterLogin, return {CreateAlterLogin(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateSequence, return {CreateNewSequence(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterSequence, return {CreateAlterSequence(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropSequence, return {CreateDropSequence(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpApplyIndexBuild, return ApplyBuildIndex(op.NextPartId(), tx, context);)
SCHEME_OP_INTERNAL(ESchemeOpAlterTableIndex, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpInitiateBuildIndexImplTable,
    THashSet<TString> localSequences;
    for (const auto& col : tx.GetCreateTable().GetColumns()) {
        if (col.HasDefaultFromSequence()) {
            localSequences.insert(col.GetDefaultFromSequence());
        }
    }
    return {CreateInitializeBuildIndexImplTable(op.NextPartId(), tx, localSequences)};
)
SCHEME_OP_INTERNAL(ESchemeOpFinalizeBuildIndexImplTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_INTERNAL(ESchemeOpInitiateBuildIndexMainTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_INTERNAL(ESchemeOpFinalizeBuildIndexMainTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpPrepareIndexValidation, return {CreatePrepareIndexValidation(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCancelIndexBuild, return CancelBuildIndex(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropIndex, return CreateDropIndex(op.NextPartId(), tx, context);)
SCHEME_OP_INTERNAL(ESchemeOpDropTableIndexAtMainTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateCdcStream, return CreateNewCdcStream(op.NextPartId(), tx, context);)
SCHEME_OP_INTERNAL(ESchemeOpCreateCdcStreamImpl, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_INTERNAL(ESchemeOpCreateCdcStreamAtTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterCdcStream, return CreateAlterCdcStream(op.NextPartId(), tx, context);)
SCHEME_OP_INTERNAL(ESchemeOpAlterCdcStreamImpl, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_INTERNAL(ESchemeOpAlterCdcStreamAtTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpDropCdcStream, return CreateDropCdcStream(op.NextPartId(), tx, context);)
SCHEME_OP_INTERNAL(ESchemeOpDropCdcStreamImpl, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_INTERNAL(ESchemeOpDropCdcStreamAtTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpRotateCdcStream, return CreateRotateCdcStream(op.NextPartId(), tx, context);)
SCHEME_OP_INTERNAL(ESchemeOpRotateCdcStreamImpl, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_INTERNAL(ESchemeOpRotateCdcStreamAtTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_DEPRECATED(ESchemeOp_DEPRECATED_35)
SCHEME_OP_IMPLEMENTED(ESchemeOpMoveTable, return CreateConsistentMoveTable(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpMoveTableIndex, return {CreateMoveTableIndex(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpMoveIndex,
    const auto& moving = tx.GetMoveIndex();
    TPath tablePath = TPath::Resolve(moving.GetTablePath(), context.SS);
    if (tablePath.IsResolved() && !tablePath.IsDeleted() && tablePath->IsColumnTable()) {
        return CreateConsistentMoveLocalIndex(op.NextPartId(), tx, context);
    }
    return CreateConsistentMoveIndex(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpMoveSequence, return {CreateMoveSequence(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateReplication, return {CreateNewReplication(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterReplication, return {CreateAlterReplication(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropReplication, return {CreateDropReplication(op.NextPartId(), tx, false)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropReplicationCascade, return {CreateDropReplication(op.NextPartId(), tx, true)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateTransfer, return {CreateNewTransfer(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterTransfer, return {CreateAlterTransfer(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropTransfer, return {CreateDropTransfer(op.NextPartId(), tx, false)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropTransferCascade, return {CreateDropTransfer(op.NextPartId(), tx, true)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateBlobDepot, return {CreateNewBlobDepot(op.NextPartId(), tx)};)
SCHEME_OP_STUB(ESchemeOpAlterBlobDepot, return {CreateAlterBlobDepot(op.NextPartId(), tx)};)
SCHEME_OP_STUB(ESchemeOpDropBlobDepot, return {CreateDropBlobDepot(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateExternalTable, return CreateNewExternalTable(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropExternalTable, return {CreateDropExternalTable(op.NextPartId(), tx)};)
SCHEME_OP_UNSUPPORTED(ESchemeOpAlterExternalTable)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateExternalDataSource,
    return CreateNewExternalDataSource(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropExternalDataSource, return {CreateDropExternalDataSource(op.NextPartId(), tx)};)
SCHEME_OP_UNSUPPORTED(ESchemeOpAlterExternalDataSource)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateView, return {CreateNewView(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropView, return {CreateDropView(op.NextPartId(), tx)};)
SCHEME_OP_UNSUPPORTED(ESchemeOpAlterView)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateContinuousBackup, return CreateNewContinuousBackup(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterContinuousBackup, return CreateAlterContinuousBackup(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropContinuousBackup, return CreateDropContinuousBackup(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateResourcePool, return {CreateNewResourcePool(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropResourcePool, return {CreateDropResourcePool(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterResourcePool, return {CreateAlterResourcePool(op.NextPartId(), tx)};)
SCHEME_OP_RETIRED(ESchemeOpRestoreMultipleIncrementalBackups,
    return CreateRestoreMultipleIncrementalBackups(op.NextPartId(), tx, context);
)
SCHEME_OP_INTERNAL(ESchemeOpRestoreIncrementalBackupAtTable, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateBackupCollection, return {CreateNewBackupCollection(op.NextPartId(), tx)};)
SCHEME_OP_UNSUPPORTED(ESchemeOpAlterBackupCollection)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropBackupCollection,
    return CreateDropBackupCollectionCascade(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpBackupBackupCollection,
    return CreateBackupBackupCollection(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpBackupIncrementalBackupCollection,
    return CreateBackupIncrementalBackupCollection(op.NextPartId(), tx, context);
)
SCHEME_OP_INTERNAL(ESchemeOpCreateLongIncrementalBackupOp, "multipart operations are handled before, also they require transaction details")
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateFullBackupOp,
    // Internal control op only - not valid for direct user submission via TModifyScheme.
    return {CreateNewFullBackupOp(op.NextPartId(), tx)};
)
SCHEME_OP_IMPLEMENTED(ESchemeOpRestoreBackupCollection,
    return CreateRestoreBackupCollection(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateLongIncrementalRestoreOp,
    return {CreateLongIncrementalRestoreOpControlPlane(op.NextPartId(), tx)};
)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateSysView, return {CreateNewSysView(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropSysView, return {CreateDropSysView(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpChangePathState, return CreateChangePathState(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpIncrementalRestoreLockTargets,
    return CreateIncrementalRestoreLockTargets(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpIncrementalRestoreUnlockTargets,
    return CreateIncrementalRestoreUnlockTargets(op.NextPartId(), tx, context);
)
SCHEME_OP_IMPLEMENTED(ESchemeOpIncrementalRestoreFinalize,
    return {CreateIncrementalRestoreFinalize(op.NextPartId(), tx)};
)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateSecret, return {CreateNewSecret(op.NextPartId(), tx, context)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterSecret, return {CreateAlterSecret(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropSecret, return {CreateDropSecret(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateStreamingQuery, return {CreateNewStreamingQuery(op.NextPartId(), tx, context)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropStreamingQuery, return {CreateDropStreamingQuery(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpAlterStreamingQuery, return {CreateAlterStreamingQuery(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpTruncateTable, return CreateConsistentTruncateTable(op.NextPartId(), tx, context);)
SCHEME_OP_IMPLEMENTED(ESchemeOpCreateTestShardSet, return {CreateNewTestShardSet(op.NextPartId(), tx)};)
SCHEME_OP_IMPLEMENTED(ESchemeOpDropTestShardSet, return {CreateDropTestShardSet(op.NextPartId(), tx)};)
SCHEME_OP_UNSUPPORTED_TX(TxAlterView, ESchemeOpAlterView)
SCHEME_OP_UNSUPPORTED_TX(TxAlterBackupCollection, ESchemeOpAlterBackupCollection)
SCHEME_OP_TRANSIENT_TX(TxCreateContinuousBackup)
SCHEME_OP_TRANSIENT_TX(TxAlterContinuousBackup)
SCHEME_OP_TRANSIENT_TX(TxDropContinuousBackup)
