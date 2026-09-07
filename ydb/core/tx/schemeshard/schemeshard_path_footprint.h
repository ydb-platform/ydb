#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>

#include <util/generic/deque.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

enum class EPathRefKind {
    LeafUnderWorkingDir,
    // Relative to WorkingDir unless absolute.
    PathUnderWorkingDir,
    // Always relative to WorkingDir, even with a leading slash (TSplitChildTag).
    PathUnderWorkingDirSplit,
    Absolute,
    LeafUnderSibling,
    ById,
    // Runtime-derived paths; Value is empty.
    Implicit,
};

enum class EPathRefRole {
    Target,
    Source,
    Parent,
    Dependency,
};

// Columns: enum name, display template, protobuf field, default kind, default role.
// Templates use {i}, {j}, and {key} for Index, SubIndex, and MapKey.
// Protobuf names are empty for synthetic and ID fields.
// Extraction may override kind and role for individual operations.
#define SCHEMESHARD_PATH_FIELDS(X) \
    X(Drop_Id, "Drop.Id", "", ById, Target) \
    X(Drop_Name, "Drop.Name", "NKikimrSchemeOp.TDrop.Name", LeafUnderWorkingDir, Target) \
    X(MkDir_Name, "MkDir.Name", "NKikimrSchemeOp.TMkDir.Name", LeafUnderWorkingDir, Target) \
    X(CreateTable_Name, "CreateTable.Name", "NKikimrSchemeOp.TTableDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(CreateTable_CopyFromTable, "CreateTable.CopyFromTable", \
        "NKikimrSchemeOp.TTableDescription.CopyFromTable", Absolute, Source) \
    X(CreatePersQueueGroup_Name, "CreatePersQueueGroup.Name", \
        "NKikimrSchemeOp.TPersQueueGroupDescription.Name", LeafUnderWorkingDir, Target) \
    X(Implicit_DropTable_Children, "DropTable.<indexes,cdcStreams,implTables>", "", \
        Implicit, Dependency) \
    X(AlterTable_PathId, "AlterTable.PathId", "", ById, Target) \
    X(AlterTable_Id_Deprecated, "AlterTable.Id_Deprecated", "", ById, Target) \
    X(AlterTable_Name, "AlterTable.Name", "NKikimrSchemeOp.TTableDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(AlterTable_Column_DefaultFromSequence, "AlterTable.Columns[{i}].DefaultFromSequence", \
        "NKikimrSchemeOp.TColumnDescription.DefaultFromSequence", LeafUnderSibling, Dependency) \
    X(AlterPersQueueGroup_PathId, "AlterPersQueueGroup.PathId", "", ById, Target) \
    X(AlterPersQueueGroup_Name, "AlterPersQueueGroup.Name", \
        "NKikimrSchemeOp.TPersQueueGroupDescription.Name", LeafUnderWorkingDir, Target) \
    X(AlterPersQueueGroup_IncrementalBackup_DstPath, \
        "AlterPersQueueGroup.PQTabletConfig.OffloadConfig.IncrementalBackup.DstPath", \
        "NKikimrPQ.TOffloadConfig.TIncrementalBackup.DstPath", Absolute, Dependency) \
    X(ModifyACL_Name, "ModifyACL.Name", "NKikimrSchemeOp.TModifyACL.Name", \
        LeafUnderWorkingDir, Target) \
    X(SplitMergeTablePartitions_TableLocalId, "SplitMergeTablePartitions.TableLocalId", "", \
        ById, Target) \
    X(SplitMergeTablePartitions_TablePath, "SplitMergeTablePartitions.TablePath", \
        "NKikimrSchemeOp.TSplitMergeTablePartitions.TablePath", Absolute, Target) \
    X(Backup_TableName, "Backup.TableName", "NKikimrSchemeOp.TBackupTask.TableName", \
        LeafUnderWorkingDir, Target) \
    /* CanBackupTable inspects children to reject global indexes. */ \
    X(Implicit_Backup_TableChildren, "Backup.<tableChildren>", "", Implicit, Dependency) \
    X(SubDomain_Name, "SubDomain.Name", "NKikimrSubDomains.TSubDomainSettings.Name", \
        LeafUnderWorkingDir, Target) \
    X(CreateRtmrVolume_Name, "CreateRtmrVolume.Name", \
        "NKikimrSchemeOp.TRtmrVolumeDescription.Name", LeafUnderWorkingDir, Target) \
    X(CreateBlockStoreVolume_Name, "CreateBlockStoreVolume.Name", \
        "NKikimrSchemeOp.TBlockStoreVolumeDescription.Name", LeafUnderWorkingDir, Target) \
    X(AlterBlockStoreVolume_PathId, "AlterBlockStoreVolume.PathId", "", ById, Target) \
    X(AlterBlockStoreVolume_Name, "AlterBlockStoreVolume.Name", \
        "NKikimrSchemeOp.TBlockStoreVolumeDescription.Name", LeafUnderWorkingDir, Target) \
    X(AssignBlockStoreVolume_Name, "AssignBlockStoreVolume.Name", \
        "NKikimrSchemeOp.TBlockStoreAssignOp.Name", LeafUnderWorkingDir, Target) \
    X(Kesus_Name, "Kesus.Name", "NKikimrSchemeOp.TKesusDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(Implicit_ForceDropSubDomain_Subtree, "ForceDropSubDomain.<subtree>", "", \
        Implicit, Dependency) \
    X(CreateSolomonVolume_Name, "CreateSolomonVolume.Name", \
        "NKikimrSchemeOp.TCreateSolomonVolume.Name", LeafUnderWorkingDir, Target) \
    X(AlterSolomonVolume_Name, "AlterSolomonVolume.Name", \
        "NKikimrSchemeOp.TAlterSolomonVolume.Name", LeafUnderWorkingDir, Target) \
    X(AlterUserAttributes_PathName, "AlterUserAttributes.PathName", \
        "NKikimrSchemeOp.TAlterUserAttributes.PathName", PathUnderWorkingDir, Target) \
    X(Implicit_ForceDropExtSubDomain_Subtree, "ForceDropExtSubDomain.<subtree>", "", \
        Implicit, Dependency) \
    X(Implicit_ForceDropUnsafe_Subtree, "ForceDropUnsafe.<subtree>", "", Implicit, Dependency) \
    X(CreateIndexedTable_TableDescription_Name, "CreateIndexedTable.TableDescription.Name", \
        "NKikimrSchemeOp.TTableDescription.Name", LeafUnderWorkingDir, Target) \
    X(CreateIndexedTable_IndexDescription_Name, "CreateIndexedTable.IndexDescription[{i}].Name", \
        "NKikimrSchemeOp.TIndexCreationConfig.Name", LeafUnderSibling, Dependency) \
    X(CreateIndexedTable_SequenceDescription_Name, \
        "CreateIndexedTable.SequenceDescription[{i}].Name", \
        "NKikimrSchemeOp.TSequenceDescription.Name", LeafUnderSibling, Dependency) \
    X(Implicit_CreateIndexedTable_IndexImplTables, "CreateIndexedTable.<indexImplTables>", "", \
        Implicit, Dependency) \
    X(CreateTableIndex_Name, "CreateTableIndex.Name", \
        "NKikimrSchemeOp.TIndexCreationConfig.Name", LeafUnderWorkingDir, Target) \
    X(CopyTables_Item_SrcPath, "CreateConsistentCopyTables.CopyTableDescriptions[{i}].SrcPath", \
        "NKikimrSchemeOp.TCopyTableConfig.SrcPath", Absolute, Source) \
    X(CopyTables_Item_DstPath, "CreateConsistentCopyTables.CopyTableDescriptions[{i}].DstPath", \
        "NKikimrSchemeOp.TCopyTableConfig.DstPath", Absolute, Target) \
    X(CopyTables_Item_CreateSrcCdc_StreamName, \
        "CreateConsistentCopyTables.CopyTableDescriptions[{i}]" \
            ".CreateSrcCdcStream.StreamDescription.Name", \
        "NKikimrSchemeOp.TCdcStreamDescription.Name", LeafUnderSibling, Dependency) \
    X(CopyTables_Item_DropSrcCdc_StreamName, \
        "CreateConsistentCopyTables.CopyTableDescriptions[{i}].DropSrcCdcStream.StreamName[{j}]", \
        "NKikimrSchemeOp.TDropCdcStream.StreamName", LeafUnderSibling, Dependency) \
    X(CopyTables_Item_IndexImplCdc_StreamName, \
        "CreateConsistentCopyTables.CopyTableDescriptions[{i}]" \
            ".IndexImplTableCdcStreams[{key}].StreamDescription.Name", \
        "NKikimrSchemeOp.TCdcStreamDescription.Name", LeafUnderSibling, Dependency) \
    X(CopyTables_Item_IndexImplDropCdc_StreamName, \
        "CreateConsistentCopyTables.CopyTableDescriptions[{i}]" \
            ".IndexImplTableDropCdcStreams[{key}].StreamName[{j}]", \
        "NKikimrSchemeOp.TDropCdcStream.StreamName", LeafUnderSibling, Dependency) \
    X(Implicit_CopyTables_Item_Children, \
        "CreateConsistentCopyTables.CopyTableDescriptions[{i}].<indexes,implTables,sequences>", "", \
        Implicit, Dependency) \
    X(UpgradeSubDomain_Name, "UpgradeSubDomain.Name", \
        "NKikimrSchemeOp.TUpgradeSubDomain.Name", LeafUnderWorkingDir, Target) \
    X(InitiateIndexBuild_Table, "InitiateIndexBuild.Table", \
        "NKikimrSchemeOp.TIndexBuildConfig.Table", Absolute, Parent) \
    X(InitiateIndexBuild_Index_Name, "InitiateIndexBuild.Index.Name", \
        "NKikimrSchemeOp.TIndexCreationConfig.Name", LeafUnderSibling, Target) \
    X(Implicit_InitiateIndexBuild_IndexImplTables, "InitiateIndexBuild.<indexImplTables>", "", \
        Implicit, Dependency) \
    X(InitiateBuildIndexMainTable_TableName, "InitiateBuildIndexMainTable.TableName", \
        "NKikimrSchemeOp.TInitiateBuildIndexMainTable.TableName", LeafUnderWorkingDir, Target) \
    X(PrepareIndexValidation_TableName, "PrepareIndexValidation.TableName", \
        "NKikimrSchemeOp.TPrepareIndexValidation.TableName", LeafUnderWorkingDir, Target) \
    X(LockConfig_Name, "LockConfig.Name", "NKikimrSchemeOp.TLockConfig.Name", \
        LeafUnderWorkingDir, Target) \
    X(ApplyIndexBuild_TablePath, "ApplyIndexBuild.TablePath", \
        "NKikimrSchemeOp.TIndexBuildControl.TablePath", Absolute, Parent) \
    X(ApplyIndexBuild_IndexName, "ApplyIndexBuild.IndexName", \
        "NKikimrSchemeOp.TIndexBuildControl.IndexName", LeafUnderSibling, Target) \
    X(FinalizeBuildIndexMainTable_TableName, "FinalizeBuildIndexMainTable.TableName", \
        "NKikimrSchemeOp.TFinalizeBuildIndexMainTable.TableName", LeafUnderWorkingDir, Target) \
    X(AlterTableIndex_Name, "AlterTableIndex.Name", \
        "NKikimrSchemeOp.TIndexAlteringConfig.Name", LeafUnderWorkingDir, Target) \
    X(DropIndex_TableName, "DropIndex.TableName", "NKikimrSchemeOp.TDropIndex.TableName", \
        PathUnderWorkingDir, Parent) \
    X(DropIndex_IndexName, "DropIndex.IndexName", "NKikimrSchemeOp.TDropIndex.IndexName", \
        LeafUnderSibling, Target) \
    X(Implicit_DropIndex_IndexImplTables, "DropIndex.<indexImplTables>", "", \
        Implicit, Dependency) \
    X(CancelIndexBuild_TablePath, "CancelIndexBuild.TablePath", \
        "NKikimrSchemeOp.TIndexBuildControl.TablePath", Absolute, Parent) \
    X(CancelIndexBuild_IndexName, "CancelIndexBuild.IndexName", \
        "NKikimrSchemeOp.TIndexBuildControl.IndexName", LeafUnderSibling, Target) \
    X(CreateFileStore_Name, "CreateFileStore.Name", \
        "NKikimrSchemeOp.TFileStoreDescription.Name", LeafUnderWorkingDir, Target) \
    X(AlterFileStore_Name, "AlterFileStore.Name", \
        "NKikimrSchemeOp.TFileStoreDescription.Name", LeafUnderWorkingDir, Target) \
    X(Restore_TableName, "Restore.TableName", "NKikimrSchemeOp.TRestoreTask.TableName", \
        LeafUnderWorkingDir, Target) \
    X(Implicit_Restore_TableChildren, "Restore.<tableChildren>", "", Implicit, Dependency) \
    X(CreateColumnStore_Name, "CreateColumnStore.Name", \
        "NKikimrSchemeOp.TColumnStoreDescription.Name", LeafUnderWorkingDir, Target) \
    X(AlterColumnStore_Name, "AlterColumnStore.Name", \
        "NKikimrSchemeOp.TAlterColumnStore.Name", LeafUnderWorkingDir, Target) \
    X(Implicit_DropColumnStore_ColumnTables, "DropColumnStore.<columnTables>", "", \
        Implicit, Dependency) \
    X(CreateColumnTable_Name, "CreateColumnTable.Name", \
        "NKikimrSchemeOp.TColumnTableDescription.Name", LeafUnderWorkingDir, Target) \
    X(CreateColumnTable_CopyFromTable, "CreateColumnTable.CopyFromTable", \
        "NKikimrSchemeOp.TColumnTableDescription.CopyFromTable", Absolute, Source) \
    /* TTL Storage names an external data source by absolute path. */ \
    X(CreateColumnTable_TierStorage, \
        "CreateColumnTable.TtlSettings.Enabled.Tiers[{i}].EvictToExternalStorage.Storage", \
        "NKikimrSchemeOp.TTTLSettings.TEvictionToExternalStorageSettings.Storage", \
        Absolute, Dependency) \
    X(AlterColumnTable_Name, "AlterColumnTable.Name", \
        "NKikimrSchemeOp.TAlterColumnTable.Name", LeafUnderWorkingDir, Target) \
    X(AlterColumnTable_TierStorage, \
        "AlterColumnTable.AlterTtlSettings.Enabled.Tiers[{i}].EvictToExternalStorage.Storage", \
        "NKikimrSchemeOp.TTTLSettings.TEvictionToExternalStorageSettings.Storage", \
        Absolute, Dependency) \
    /* Previous TTL tiers come from stored state. */ \
    X(Implicit_AlterColumnTable_DroppedTiers, "AlterColumnTable.<droppedTierStorages>", "", \
        Implicit, Dependency) \
    /* CDC AtTable and Impl parts override the resolution defaults. */ \
    X(CreateCdcStream_TableName, "CreateCdcStream.TableName", \
        "NKikimrSchemeOp.TCreateCdcStream.TableName", PathUnderWorkingDir, Parent) \
    X(CreateCdcStream_StreamDescription_Name, "CreateCdcStream.StreamDescription.Name", \
        "NKikimrSchemeOp.TCdcStreamDescription.Name", LeafUnderSibling, Target) \
    X(Implicit_CreateCdcStream_PqGroupUnderStream, "CreateCdcStream.<pqGroupUnderStream>", "", \
        Implicit, Dependency) \
    X(AlterCdcStream_TableName, "AlterCdcStream.TableName", \
        "NKikimrSchemeOp.TAlterCdcStream.TableName", PathUnderWorkingDir, Parent) \
    X(AlterCdcStream_StreamName, "AlterCdcStream.StreamName", \
        "NKikimrSchemeOp.TAlterCdcStream.StreamName", LeafUnderSibling, Target) \
    X(DropCdcStream_TableName, "DropCdcStream.TableName", \
        "NKikimrSchemeOp.TDropCdcStream.TableName", PathUnderWorkingDir, Parent) \
    X(DropCdcStream_StreamName, "DropCdcStream.StreamName[{i}]", \
        "NKikimrSchemeOp.TDropCdcStream.StreamName", LeafUnderSibling, Target) \
    X(RotateCdcStream_TableName, "RotateCdcStream.TableName", \
        "NKikimrSchemeOp.TRotateCdcStream.TableName", PathUnderWorkingDir, Parent) \
    X(RotateCdcStream_OldStreamName, "RotateCdcStream.OldStreamName", \
        "NKikimrSchemeOp.TRotateCdcStream.OldStreamName", LeafUnderSibling, Source) \
    X(RotateCdcStream_NewStream_Name, "RotateCdcStream.NewStream.StreamDescription.Name", \
        "NKikimrSchemeOp.TCdcStreamDescription.Name", LeafUnderSibling, Target) \
    X(MoveTable_SrcPath, "MoveTable.SrcPath", "NKikimrSchemeOp.TMove.SrcPath", Absolute, Source) \
    X(MoveTable_DstPath, "MoveTable.DstPath", "NKikimrSchemeOp.TMove.DstPath", Absolute, Target) \
    X(Implicit_MoveTable_Children, "MoveTable.<indexes,implTables,cdcStreams>", "", \
        Implicit, Dependency) \
    X(MoveTableIndex_SrcPath, "MoveTableIndex.SrcPath", "NKikimrSchemeOp.TMove.SrcPath", \
        Absolute, Source) \
    X(MoveTableIndex_DstPath, "MoveTableIndex.DstPath", "NKikimrSchemeOp.TMove.DstPath", \
        Absolute, Target) \
    X(Implicit_MoveTableIndex_Children, "MoveTableIndex.<indexImplTables,sequences>", "", \
        Implicit, Dependency) \
    X(MoveSequence_SrcPath, "MoveSequence.SrcPath", "NKikimrSchemeOp.TMove.SrcPath", \
        Absolute, Source) \
    X(MoveSequence_DstPath, "MoveSequence.DstPath", "NKikimrSchemeOp.TMove.DstPath", \
        Absolute, Target) \
    X(Sequence_Name, "Sequence.Name", "NKikimrSchemeOp.TSequenceDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(CopySequence_CopyFrom, "CopySequence.CopyFrom", "NKikimrSchemeOp.TCopySequence.CopyFrom", \
        Absolute, Source) \
    X(Replication_Name, "Replication.Name", "NKikimrSchemeOp.TReplicationDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(Replication_TransferTarget_DstPath, "Replication.Config.TransferSpecific.Target.DstPath", \
        "NKikimrReplication.TReplicationConfig.TTransferSpecific.TTarget.DstPath", \
        Absolute, Dependency) \
    X(Replication_TransferTarget_DirectoryPath, \
        "Replication.Config.TransferSpecific.Target.DirectoryPath", \
        "NKikimrReplication.TReplicationConfig.TTransferSpecific.TTarget.DirectoryPath", \
        Absolute, Dependency) \
    X(Replication_SpecificTarget_DstPath, "Replication.Config.Specific.Targets[{i}].DstPath", \
        "NKikimrReplication.TReplicationConfig.TTargetSpecific.TTarget.DstPath", \
        Absolute, Dependency) \
    X(Replication_AlterTransfer_DirectoryPath, "Replication.AlterTransfer.DirectoryPath", \
        "NKikimrSchemeOp.TReplicationDescription.TAlterTransfer.DirectoryPath", \
        Absolute, Dependency) \
    X(AlterReplication_PathId, "AlterReplication.PathId", "", ById, Target) \
    X(AlterReplication_Name, "AlterReplication.Name", \
        "NKikimrSchemeOp.TReplicationDescription.Name", LeafUnderWorkingDir, Target) \
    X(AlterReplication_TransferTarget_DstPath, \
        "AlterReplication.Config.TransferSpecific.Target.DstPath", \
        "NKikimrReplication.TReplicationConfig.TTransferSpecific.TTarget.DstPath", \
        Absolute, Dependency) \
    X(AlterReplication_TransferTarget_DirectoryPath, \
        "AlterReplication.Config.TransferSpecific.Target.DirectoryPath", \
        "NKikimrReplication.TReplicationConfig.TTransferSpecific.TTarget.DirectoryPath", \
        Absolute, Dependency) \
    X(AlterReplication_SpecificTarget_DstPath, \
        "AlterReplication.Config.Specific.Targets[{i}].DstPath", \
        "NKikimrReplication.TReplicationConfig.TTargetSpecific.TTarget.DstPath", \
        Absolute, Dependency) \
    X(AlterReplication_AlterTransfer_DirectoryPath, \
        "AlterReplication.AlterTransfer.DirectoryPath", \
        "NKikimrSchemeOp.TReplicationDescription.TAlterTransfer.DirectoryPath", \
        Absolute, Dependency) \
    X(BlobDepot_Name, "BlobDepot.Name", "NKikimrSchemeOp.TBlobDepotDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(MoveIndex_TablePath, "MoveIndex.TablePath", "NKikimrSchemeOp.TMoveIndex.TablePath", \
        Absolute, Parent) \
    X(MoveIndex_SrcPath, "MoveIndex.SrcPath", "NKikimrSchemeOp.TMoveIndex.SrcPath", \
        LeafUnderSibling, Source) \
    X(MoveIndex_DstPath, "MoveIndex.DstPath", "NKikimrSchemeOp.TMoveIndex.DstPath", \
        LeafUnderSibling, Target) \
    X(Implicit_MoveIndex_IndexImplTables, "MoveIndex.<indexImplTables>", "", \
        Implicit, Dependency) \
    X(CreateExternalTable_Name, "CreateExternalTable.Name", \
        "NKikimrSchemeOp.TExternalTableDescription.Name", LeafUnderWorkingDir, Target) \
    X(CreateExternalTable_DataSourcePath, "CreateExternalTable.DataSourcePath", \
        "NKikimrSchemeOp.TExternalTableDescription.DataSourcePath", Absolute, Dependency) \
    X(CreateExternalDataSource_Name, "CreateExternalDataSource.Name", \
        "NKikimrSchemeOp.TExternalDataSourceDescription.Name", LeafUnderWorkingDir, Target) \
    X(InitiateColumnBuild_Table, "InitiateColumnBuild.Table", \
        "NKikimrIndexBuilder.TColumnBuildSettings.Table", Absolute, Target) \
    X(DropColumnBuild_Settings_Table, "DropColumnBuild.Settings.Table", \
        "NKikimrIndexBuilder.TColumnBuildSettings.Table", Absolute, Target) \
    X(CreateView_Name, "CreateView.Name", "NKikimrSchemeOp.TViewDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(CreateContinuousBackup_TableName, "CreateContinuousBackup.TableName", \
        "NKikimrSchemeOp.TCreateContinuousBackup.TableName", LeafUnderWorkingDir, Target) \
    X(CreateContinuousBackup_StreamName, \
        "CreateContinuousBackup.ContinuousBackupDescription.StreamName", \
        "NKikimrSchemeOp.TContinuousBackupDescription.StreamName", LeafUnderSibling, Target) \
    X(Implicit_CreateContinuousBackup_CdcStream, "CreateContinuousBackup.<cdcStream>", "", \
        Implicit, Dependency) \
    X(AlterContinuousBackup_TableName, "AlterContinuousBackup.TableName", \
        "NKikimrSchemeOp.TAlterContinuousBackup.TableName", PathUnderWorkingDirSplit, Target) \
    X(AlterContinuousBackup_TakeIncrementalBackup_DstPath, \
        "AlterContinuousBackup.TakeIncrementalBackup.DstPath", \
        "NKikimrSchemeOp.TAlterContinuousBackup.TTakeIncrementalBackup.DstPath", \
        PathUnderWorkingDirSplit, Target) \
    X(AlterContinuousBackup_TakeIncrementalBackup_DstStreamPath, \
        "AlterContinuousBackup.TakeIncrementalBackup.DstStreamPath", \
        "NKikimrSchemeOp.TAlterContinuousBackup.TTakeIncrementalBackup.DstStreamPath", \
        LeafUnderSibling, Target) \
    X(Implicit_AlterContinuousBackup_IncrementalBackupTable, \
        "AlterContinuousBackup.<incrementalBackupTable>", "", Implicit, Dependency) \
    X(DropContinuousBackup_TableName, "DropContinuousBackup.TableName", \
        "NKikimrSchemeOp.TDropContinuousBackup.TableName", LeafUnderWorkingDir, Target) \
    X(CreateResourcePool_Name, "CreateResourcePool.Name", \
        "NKikimrSchemeOp.TResourcePoolDescription.Name", LeafUnderWorkingDir, Target) \
    X(RestoreMultipleIncrementalBackups_SrcTablePaths, \
        "RestoreMultipleIncrementalBackups.SrcTablePaths[{i}]", \
        "NKikimrSchemeOp.TRestoreMultipleIncrementalBackups.SrcTablePaths", Absolute, Source) \
    X(RestoreMultipleIncrementalBackups_DstTablePath, \
        "RestoreMultipleIncrementalBackups.DstTablePath", \
        "NKikimrSchemeOp.TRestoreMultipleIncrementalBackups.DstTablePath", Absolute, Target) \
    /* Create/Drop accept absolute paths; other backup-collection ops use leaf names. */ \
    X(CreateBackupCollection_Name, "CreateBackupCollection.Name", \
        "NKikimrSchemeOp.TBackupCollectionDescription.Name", PathUnderWorkingDir, Target) \
    X(CreateBackupCollection_Entry_Path, \
        "CreateBackupCollection.ExplicitEntryList.Entries[{i}].Path", \
        "NKikimrSchemeOp.TBackupCollectionDescription.TBackupEntry.Path", Absolute, Dependency) \
    X(AlterBackupCollection_Name, "AlterBackupCollection.Name", \
        "NKikimrSchemeOp.TBackupCollectionDescription.Name", LeafUnderWorkingDir, Target) \
    X(DropBackupCollection_Name, "DropBackupCollection.Name", \
        "NKikimrSchemeOp.TBackupCollectionDescription.Name", PathUnderWorkingDir, Target) \
    X(Implicit_DropBackupCollection_Entries, "DropBackupCollection.<collectionEntries>", "", \
        Implicit, Dependency) \
    X(BackupBackupCollection_Name, "BackupBackupCollection.Name", \
        "NKikimrSchemeOp.TBackupBackupCollection.Name", LeafUnderWorkingDir, Target) \
    X(Implicit_BackupBackupCollection_Entries, "BackupBackupCollection.<collectionEntries>", "", \
        Implicit, Dependency) \
    X(BackupIncrementalBackupCollection_Name, "BackupIncrementalBackupCollection.Name", \
        "NKikimrSchemeOp.TBackupBackupCollection.Name", LeafUnderWorkingDir, Target) \
    X(Implicit_BackupIncrementalBackupCollection_Entries, \
        "BackupIncrementalBackupCollection.<collectionEntries>", "", Implicit, Dependency) \
    /* CreateFullBackupOp uses WorkingDir as the backup collection path. */ \
    X(WorkingDirItself, "<WorkingDir>", "", PathUnderWorkingDir, Target) \
    /* ApplyIf IDs have no name form; replay must strip or rederive them. */ \
    X(ApplyIf_PathId, "ApplyIf[{i}].PathId", "", ById, Dependency) \
    /* Strict ACL checks scan the database subtree when removing a user or group. */ \
    X(Implicit_AlterLogin_AclScan, "AlterLogin.<aclScanSubtree>", "", Implicit, Dependency) \
    X(Implicit_CreateFullBackupOp_Entries, "CreateFullBackupOp.<collectionEntries>", "", \
        Implicit, Dependency) \
    X(RestoreBackupCollection_Name, "RestoreBackupCollection.Name", \
        "NKikimrSchemeOp.TBackupBackupCollection.Name", LeafUnderWorkingDir, Target) \
    X(Implicit_RestoreBackupCollection_Entries, "RestoreBackupCollection.<collectionEntries>", "", \
        Implicit, Dependency) \
    X(CreateSysView_Name, "CreateSysView.Name", "NKikimrSchemeOp.TSysViewDescription.Name", \
        LeafUnderWorkingDir, Target) \
    X(ChangePathState_Path, "ChangePathState.Path", "NKikimrSchemeOp.TChangePathState.Path", \
        PathUnderWorkingDir, Target) \
    X(IncrementalRestoreLockTargets_DstPaths, "IncrementalRestoreLockTargets.DstPaths[{i}]", \
        "NKikimrSchemeOp.TIncrementalRestoreLockTargets.DstPaths", PathUnderWorkingDir, Target) \
    X(IncrementalRestoreLockTargets_SrcPaths, "IncrementalRestoreLockTargets.SrcPaths[{i}]", \
        "NKikimrSchemeOp.TIncrementalRestoreLockTargets.SrcPaths", PathUnderWorkingDir, Source) \
    X(Implicit_IncrementalRestoreFinalize_PersistedState, \
        "IncrementalRestoreFinalize.<persistedRestoreState>", "", Implicit, Target) \
    X(CreateSecret_Name, "CreateSecret.Name", "NKikimrSchemeOp.TSecretSchemaOp.Name", \
        LeafUnderWorkingDir, Target) \
    X(AlterSecret_Name, "AlterSecret.Name", "NKikimrSchemeOp.TSecretSchemaOp.Name", \
        LeafUnderWorkingDir, Target) \
    X(CreateStreamingQuery_Name, "CreateStreamingQuery.Name", \
        "NKikimrSchemeOp.TStreamingQueryDescription.Name", LeafUnderWorkingDir, Target) \
    X(TruncateTable_TableName, "TruncateTable.TableName", \
        "NKikimrSchemeOp.TTruncateTable.TableName", PathUnderWorkingDir, Target) \
    X(CreateTestShardSet_Name, "CreateTestShardSet.Name", \
        "NKikimrSchemeOp.TCreateTestShardSet.Name", LeafUnderWorkingDir, Target)

enum class EPathField : ui16 {
#define SCHEMESHARD_PATH_FIELD_ENUMERATOR(name, tpl, proto, kind, role) name,
    SCHEMESHARD_PATH_FIELDS(SCHEMESHARD_PATH_FIELD_ENUMERATOR)
#undef SCHEMESHARD_PATH_FIELD_ENUMERATOR
    Count
};

// Field template with placeholders unexpanded.
TStringBuf PathFieldName(EPathField field);

TStringBuf PathFieldProtoName(EPathField field);
EPathRefKind PathFieldDefaultKind(EPathField field);
EPathRefRole PathFieldDefaultRole(EPathField field);

// String views borrow from the request or TPathRefs::Owned; both must outlive the ref.
struct TPathRef {
    EPathField Field = EPathField::Count;
    // Index and SubIndex expand {i} and {j}.
    ui32 Index = Max<ui32>();
    ui32 SubIndex = Max<ui32>();
    // Expands {key}.
    TStringBuf MapKey;
    TStringBuf Value;
    // ById: OwnerId == 0 denotes the local SchemeShard.
    ui64 OwnerId = 0;
    ui64 LocalPathId = 0;
    EPathRefKind Kind = EPathRefKind::LeafUnderWorkingDir;
    EPathRefRole Role = EPathRefRole::Target;
    // LeafUnderSibling: raw sibling path.
    TStringBuf BasePath;
    // Index of the Implicit anchor or LeafUnderSibling base; -1 if absent.
    // Used for sibling lookup when BasePath is empty.
    int AnchorIndex = -1;
};

// Expands placeholders into an owning string; allocates.
TString FieldPath(const TPathRef& ref);

struct TPathRefs {
    TVector<TPathRef> Refs;
    // Keeps computed strings stable while refs are appended.
    TDeque<TString> Owned;

    size_t size() const { return Refs.size(); }
    bool empty() const { return Refs.empty(); }
    const TPathRef& operator[](size_t i) const { return Refs[i]; }
    TVector<TPathRef>::const_iterator begin() const { return Refs.begin(); }
    TVector<TPathRef>::const_iterator end() const { return Refs.end(); }
};

// Layer 1: pure, state-free extraction. Covers every EOperationType. Allocates
// nothing per string: every value is a view into tx or into the result itself.
TPathRefs ExtractPathRefs(const NKikimrSchemeOp::TModifyScheme& tx);

// Joins request paths without state lookup, canonization, or existence checks.
// joined contains preceding results in extraction order for sibling lookup.
// Returns empty for ById, Implicit, or a sibling with no base.
TString JoinPathRef(TStringBuf workingDir, const TPathRef& ref,
    const TVector<TString>& joined = {});

// Sorted, unique protobuf names for descriptor coverage tests.
const TVector<TStringBuf>& KnownPathFieldNames();

TStringBuf PathRefKindName(EPathRefKind kind);
TStringBuf PathRefRoleName(EPathRefRole role);

}  // namespace NKikimr::NSchemeShard
