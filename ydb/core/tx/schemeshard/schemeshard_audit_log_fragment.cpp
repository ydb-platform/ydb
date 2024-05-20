#include "schemeshard_audit_log_fragment.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/library/aclib/aclib.h>

#include <util/string/builder.h>

namespace {

TString DefineUserOperationName(const NKikimrSchemeOp::TModifyScheme& tx) {
    NKikimrSchemeOp::EOperationType type = tx.GetOperationType();
    switch (type) {
    // common
    case NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL:
        return "MODIFY ACL";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes:
        return "ALTER USER ATTRIBUTES";
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropUnsafe:
        return "DROP PATH UNSAFE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock:
        return "CREATE LOCK";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropLock:
        return "DROP LOCK";
    // specify ESchemeOpAlterLogin with each separate case
    // it looks a bit out of the scheme, but improve reading of audit logs
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin:
        switch (tx.GetAlterLogin().GetAlterCase()) {
            case NKikimrSchemeOp::TAlterLogin::kCreateUser:
                return "CREATE USER";
            case NKikimrSchemeOp::TAlterLogin::kModifyUser:
                return "MODIFY USER";
            case NKikimrSchemeOp::TAlterLogin::kRemoveUser:
                return "REMOVE USER";
            case NKikimrSchemeOp::TAlterLogin::kCreateGroup:
                return "CREATE GROUP";
            case NKikimrSchemeOp::TAlterLogin::kAddGroupMembership:
                return "ADD GROUP MEMBERSHIP";
            case NKikimrSchemeOp::TAlterLogin::kRemoveGroupMembership:
                return "REMOVE GROUP MEMBERSHIP";
            case NKikimrSchemeOp::TAlterLogin::kRenameGroup:
                return "RENAME GROUP";
            case NKikimrSchemeOp::TAlterLogin::kRemoveGroup:
                return "REMOVE GROUP";
            default:
                Y_ABORT("switch should cover all operation types");
        }
    case NKikimrSchemeOp::EOperationType::ESchemeOp_DEPRECATED_35:
        return "ESchemeOp_DEPRECATED_35";
    // dir
    case NKikimrSchemeOp::EOperationType::ESchemeOpMkDir:
        return "CREATE DIRECTORY";
    case NKikimrSchemeOp::EOperationType::ESchemeOpRmDir:
        return "DROP DIRECTORY";
    // table
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        return "CREATE TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable:
        return "ALTER TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTable:
        return "DROP TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables:
        return "CREATE TABLE COPY FROM";
    case NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions:
        return "ALTER TABLE PARTITIONS";
    case NKikimrSchemeOp::EOperationType::ESchemeOpBackup:
        return "BACKUP TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestore:
        return "RESTORE TABLE";
    // topic
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
        return "CREATE PERSISTENT QUEUE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup:
        return "ALTER PERSISTENT QUEUE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup:
        return "DROP PERSISTENT QUEUE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAllocatePersQueueGroup:
        return "ALLOCATE PERSISTENT QUEUE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDeallocatePersQueueGroup:
        return "DEALLOCATE PERSISTENT QUEUE";
    // database
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
        return "CREATE DATABASE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomainCreateHive:
        return "ALTER DATABASE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropExtSubDomain:
        return "DROP DATABASE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain:
        return "ALTER DATABASE MIGRATE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision:
        return "ALTER DATABASE MIGRATE DECISION";
    // rtmr
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume:
        return "CREATE RTMR VOLUME";
    // blockstore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume:
        return "CREATE BLOCK STORE VOLUME";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlockStoreVolume:
        return "ALTER BLOCK STORE VOLUME";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAssignBlockStoreVolume:
        return "ALTER BLOCK STORE VOLUME ASSIGN";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume:
        return "DROP BLOCK STORE VOLUME";
    // kesus
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus:
        return "CREATE KESUS";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterKesus:
        return "ALTER KESUS";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus:
        return "DROP KESUS";
    // solomon
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume:
        return "CREATE SOLOMON VOLUME";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSolomonVolume:
        return "ALTER SOLOMON VOLUME";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume:
        return "DROP SOLOMON VOLUME";
    // index
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
        return "CREATE TABLE WITH INDEXES";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex:
        return "CREATE INDEX";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex:
        return "DROP INDEX";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild:
        return "BUILD INDEX";
    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable:
        return "ALTER TABLE BUILD INDEX INIT";
    case NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild:
        return "ALTER TABLE BUILD INDEX APPLY";
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable:
        return "ALTER TABLE BUILD INDEX FINISH";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex:
        return "ALTER INDEX";
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexImplTable:
        return "ALTER TABLE BUILD INDEX FINISH";
    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable:
        return "ALTER TABLE BUILD INDEX INIT";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex:
        return "ALTER TABLE DROP INDEX";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndexAtMainTable:
        return "ALTER TABLE DROP INDEX";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild:
        return "ALTER TABLE BUILD INDEX CANCEL";
    // rename
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable:
        return "ALTER TABLE RENAME";
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex:
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex:
        return "ALTER TABLE INDEX RENAME";
    // filestore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore:
        return "CREATE FILE STORE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterFileStore:
        return "ALTER FILE STORE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore:
        return "DROP FILE STORE";
    // columnstore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore:
        return "CREATE COLUMN STORE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore:
        return "ALTER COLUMN STORE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore:
        return "DROP COLUMN STORE";
    // columntable
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable:
        return "CREATE COLUMN TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable:
        return "ALTER COLUMN TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable:
        return "DROP COLUMN TABLE";
    // changefeed
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable:
        return "ALTER TABLE ADD CHANGEFEED";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamAtTable:
        return "ALTER TABLE ALTER CHANGEFEED";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream:
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamAtTable:
        return "ALTER TABLE DROP CHANGEFEED";
    // sequence
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence:
        return "CREATE SEQUENCE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSequence:
        return "ALTER SEQUENCE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence:
        return "DROP SEQUENCE";
    // replication
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateReplication:
        return "CREATE REPLICATION";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterReplication:
        return "ALTER REPLICATION";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication:
        return "DROP REPLICATION";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplicationCascade:
        return "DROP REPLICATION CASCADE";
    // blob depot
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlobDepot:
        return "CREATE BLOB DEPOT";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlobDepot:
        return "ALTER BLOB DEPOT";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlobDepot:
        return "DROP BLOB DEPOT";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable:
        return "CREATE EXTERNAL TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalTable:
        return "DROP EXTERNAL TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalTable:
        return "ALTER EXTERNAL TABLE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource:
        return "CREATE EXTERNAL DATA SOURCE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalDataSource:
        return "DROP EXTERNAL DATA SOURCE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalDataSource:
        return "ALTER EXTERNAL DATA SOURCE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnBuild:
        return "ALTER TABLE ADD COLUMN DEFAULT";
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateView:
        return "CREATE VIEW";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterView:
        return "ALTER VIEW";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropView:
        return "DROP VIEW";
    // continuous backup
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateContinuousBackup:
        return "ALTER TABLE ADD CONTINUOUS BACKUP";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup:
        return "ALTER TABLE ALTER CONTINUOUS BACKUP";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup:
        return "ALTER TABLE DROP CONTINUOUS BACKUP";
    }
    Y_ABORT("switch should cover all operation types");
}

TVector<TString> ExtractChangingPaths(const NKikimrSchemeOp::TModifyScheme& tx) {
    TVector<TString> result;

    switch (tx.GetOperationType()) {
    case NKikimrSchemeOp::EOperationType::ESchemeOpMkDir:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetMkDir().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateTable().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreatePersQueueGroup().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAllocatePersQueueGroup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAllocatePersQueueGroup().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDeallocatePersQueueGroup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDeallocatePersQueueGroup().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterTable().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterPersQueueGroup().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpModifyACL:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetModifyACL().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpRmDir:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetSplitMergeTablePartitions().GetTablePath()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpBackup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetBackup().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSubDomain:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetSubDomain().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSubDomain:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateRtmrVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateRtmrVolume().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlockStoreVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateBlockStoreVolume().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlockStoreVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterBlockStoreVolume().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAssignBlockStoreVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAssignBlockStoreVolume().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetKesus().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropKesus:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropSubDomain:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateSolomonVolume().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterKesus:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetKesus().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetSubDomain().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterUserAttributes:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterUserAttributes().GetPathName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropUnsafe:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexedTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateIndexedTable().GetTableDescription().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTableIndex:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateTableIndex().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables:
        for (const auto& item : tx.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
            result.emplace_back(item.GetDstPath());
        }
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExtSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomain:
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExtSubDomainCreateHive:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetSubDomain().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpForceDropExtSubDomain:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOp_DEPRECATED_35:
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetUpgradeSubDomain().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetUpgradeSubDomain().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild:
        result.emplace_back(NKikimr::JoinPath({tx.GetInitiateIndexBuild().GetTable(), tx.GetInitiateIndexBuild().GetIndex().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexMainTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetInitiateBuildIndexMainTable().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetLockConfig().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild:
        result.emplace_back(NKikimr::JoinPath({tx.GetApplyIndexBuild().GetTablePath(), tx.GetApplyIndexBuild().GetIndexName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexMainTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetFinalizeBuildIndexMainTable().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterTableIndex().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSolomonVolume:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterSolomonVolume().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropLock:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetLockConfig().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpFinalizeBuildIndexImplTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterTable().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateTable().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropIndex:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDropIndex().GetTableName(), tx.GetDropIndex().GetIndexName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndexAtMainTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDropIndex().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild:
        result.emplace_back(NKikimr::JoinPath({tx.GetCancelIndexBuild().GetTablePath(), tx.GetCancelIndexBuild().GetIndexName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFileStore:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateFileStore().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterFileStore:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterFileStore().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropFileStore:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestore:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetRestore().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateColumnStore().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterColumnStore().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterColumnTable().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterColumnTable().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin:
        result.emplace_back(tx.GetWorkingDir());
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateCdcStream().GetTableName(), tx.GetCreateCdcStream().GetStreamDescription().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamImpl:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateCdcStream().GetStreamDescription().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStreamAtTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateCdcStream().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStream:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterCdcStream().GetTableName(), tx.GetAlterCdcStream().GetStreamName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamImpl:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterCdcStream().GetStreamName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterCdcStreamAtTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterCdcStream().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStream:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDropCdcStream().GetTableName(), tx.GetDropCdcStream().GetStreamName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamAtTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDropCdcStream().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable:
        result.emplace_back(tx.GetMoveTable().GetSrcPath());
        result.emplace_back(tx.GetMoveTable().GetDstPath());
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveTableIndex:
        result.emplace_back(tx.GetMoveTableIndex().GetSrcPath());
        result.emplace_back(tx.GetMoveTableIndex().GetDstPath());
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetSequence().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSequence:
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateReplication:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetReplication().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterReplication:
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplication:
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropReplicationCascade:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBlobDepot:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetBlobDepot().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBlobDepot:
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBlobDepot:
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveIndex:
        result.emplace_back(NKikimr::JoinPath({tx.GetMoveIndex().GetTablePath(), tx.GetMoveIndex().GetSrcPath()}));
        result.emplace_back(NKikimr::JoinPath({tx.GetMoveIndex().GetTablePath(), tx.GetMoveIndex().GetDstPath()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateExternalTable().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalTable:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalTable:
        // TODO: unimplemented
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalDataSource:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateExternalDataSource().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropExternalDataSource:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterExternalDataSource:
        // TODO: unimplemented
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnBuild:
        result.emplace_back(tx.GetInitiateColumnBuild().GetTable());
        break;

    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateView:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateView().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropView:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDrop().GetName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterView:
        // TODO: implement
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateContinuousBackup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateContinuousBackup().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetAlterContinuousBackup().GetTableName()}));
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropContinuousBackup:
        result.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetDropContinuousBackup().GetTableName()}));
        break;
    }

    return result;
}

TString ExtractNewOwner(const NKikimrSchemeOp::TModifyScheme& tx) {
    bool hasNewOwner = tx.HasModifyACL() && tx.GetModifyACL().HasNewOwner();
    if (hasNewOwner) {
        return tx.GetModifyACL().GetNewOwner();
    }
    return {};
}

struct TChange {
    TVector<TString> Add;
    TVector<TString> Remove;
};

TChange ExtractACLChange(const NKikimrSchemeOp::TModifyScheme& tx) {
    bool hasACL = tx.HasModifyACL() && tx.GetModifyACL().HasDiffACL();
    if (hasACL) {
        TChange result;

        NACLib::TDiffACL diff(tx.GetModifyACL().GetDiffACL());
        for (const auto& i : diff.GetDiffACE()) {
            auto diffType = static_cast<NACLib::EDiffType>(i.GetDiffType());
            const NACLibProto::TACE& ace = i.GetACE();
            switch (diffType) {
                case NACLib::EDiffType::Add:
                    result.Add.push_back(NACLib::TACL::ToString(ace));
                    break;
                case NACLib::EDiffType::Remove:
                    result.Remove.push_back(NACLib::TACL::ToString(ace));
                    break;
            }
        }

        return result;
    }
    return {};
}

TChange ExtractUserAttrChange(const NKikimrSchemeOp::TModifyScheme& tx) {
    bool hasUserAttrs = tx.HasAlterUserAttributes() && (tx.GetAlterUserAttributes().UserAttributesSize() > 0);
    if (hasUserAttrs) {
        TChange result;
        auto str = TStringBuilder();

        for (const auto& i : tx.GetAlterUserAttributes().GetUserAttributes()) {
            const auto& key = i.GetKey();
            const auto& value = i.GetValue();
            if (value.empty()) {
                result.Remove.push_back(key);
            } else {
                str.clear();
                str << key << ": " << value;
                result.Add.push_back(str);
            }
        }

        return result;
    }
    return {};
}

struct TChangeLogin {
    TString LoginUser;
    TString LoginGroup;
    TString LoginMember;
};

TChangeLogin ExtractLoginChange(const NKikimrSchemeOp::TModifyScheme& tx) {
    if (tx.HasAlterLogin()) {
        TChangeLogin result;
        switch (tx.GetAlterLogin().GetAlterCase()) {
            case NKikimrSchemeOp::TAlterLogin::kCreateUser:
                result.LoginUser = tx.GetAlterLogin().GetCreateUser().GetUser();
                break;
            case NKikimrSchemeOp::TAlterLogin::kModifyUser:
                result.LoginUser = tx.GetAlterLogin().GetModifyUser().GetUser();
                break;
            case NKikimrSchemeOp::TAlterLogin::kRemoveUser:
                result.LoginUser = tx.GetAlterLogin().GetRemoveUser().GetUser();
                break;
            case NKikimrSchemeOp::TAlterLogin::kCreateGroup:
                result.LoginGroup = tx.GetAlterLogin().GetCreateGroup().GetGroup();
                break;
            case NKikimrSchemeOp::TAlterLogin::kAddGroupMembership:
                result.LoginGroup = tx.GetAlterLogin().GetAddGroupMembership().GetGroup();
                result.LoginMember = tx.GetAlterLogin().GetAddGroupMembership().GetMember();
                break;
            case NKikimrSchemeOp::TAlterLogin::kRemoveGroupMembership:
                result.LoginGroup = tx.GetAlterLogin().GetRemoveGroupMembership().GetGroup();
                result.LoginMember = tx.GetAlterLogin().GetRemoveGroupMembership().GetMember();
                break;
            case NKikimrSchemeOp::TAlterLogin::kRenameGroup:
                result.LoginGroup = tx.GetAlterLogin().GetRenameGroup().GetGroup();
                break;
            case NKikimrSchemeOp::TAlterLogin::kRemoveGroup:
                result.LoginGroup = tx.GetAlterLogin().GetRemoveGroup().GetGroup();
                break;
            default:
                Y_ABORT("switch should cover all operation types");
        }
        return result;
    }
    return {};
}

} // anonymous namespace

namespace NKikimr::NSchemeShard {

TAuditLogFragment MakeAuditLogFragment(const NKikimrSchemeOp::TModifyScheme& tx) {
    auto [aclAdd, aclRemove] = ExtractACLChange(tx);
    auto [userAttrsAdd, userAttrsRemove] = ExtractUserAttrChange(tx);
    auto [loginUser, loginGroup, loginMember] = ExtractLoginChange(tx);

    return {
        .Operation = DefineUserOperationName(tx),
        .Paths = ExtractChangingPaths(tx),
        .NewOwner = ExtractNewOwner(tx),
        .ACLAdd = aclAdd,
        .ACLRemove = aclRemove,
        .UserAttrsAdd = userAttrsAdd,
        .UserAttrsRemove = userAttrsRemove,
        .LoginUser = loginUser,
        .LoginGroup = loginGroup,
        .LoginMember = loginMember,
    };
}

}
