#include "schemeshard_audit_log_fragment.h"

#include "schemeshard_path_footprint.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

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
    case NKikimrSchemeOp::EOperationType::ESchemeOpPrepareIndexValidation:
        return "ALTER TABLE BUILD INDEX PUBLISH SHADOW";
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
    case NKikimrSchemeOp::EOperationType::ESchemeOpMoveSequence:
        return "ALTER SEQUENCE RENAME";
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
    case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStream:
    case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamImpl:
    case NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStreamAtTable:
        return "ALTER TABLE ROTATE CHANGEFEED";
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
    // replication
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTransfer:
        return "CREATE TRANSFER";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterTransfer:
        return "ALTER TRANSFER";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTransfer:
        return "DROP TRANSFER";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTransferCascade:
        return "DROP TRANSFER CASCADE";
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
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnBuild:
        return "ALTER TABLE ADD COLUMN CANCEL";
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
    // resource pool
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateResourcePool:
        return "CREATE RESOURCE POOL";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropResourcePool:
        return "DROP RESOURCE POOL";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterResourcePool:
        return "ALTER RESOURCE POOL";
    // incremental backup
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreMultipleIncrementalBackups:
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackupAtTable:
        return "RESTORE";
    // backup collection
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateBackupCollection:
        return "CREATE BACKUP COLLECTION";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterBackupCollection:
        return "ALTER BACKUP COLLECTION";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropBackupCollection:
        return "DROP BACKUP COLLECTION";

    case NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateFullBackupOp:
        return "BACKUP";
    case NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection:
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLongIncrementalBackupOp:
        return "BACKUP INCREMENTAL";
    case NKikimrSchemeOp::EOperationType::ESchemeOpRestoreBackupCollection:
        return "RESTORE";
    // long incremental restore
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateLongIncrementalRestoreOp:
        return "RESTORE INCREMENTAL";
    // system view
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSysView:
        return "CREATE SYSTEM VIEW";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSysView:
        return "DROP SYSTEM VIEW";
    case NKikimrSchemeOp::EOperationType::ESchemeOpChangePathState:
        return "CHANGE PATH STATE";
    case NKikimrSchemeOp::EOperationType::ESchemeOpIncrementalRestoreLockTargets:
        return "INCREMENTAL RESTORE LOCK TARGETS";
    case NKikimrSchemeOp::EOperationType::ESchemeOpIncrementalRestoreUnlockTargets:
        return "INCREMENTAL RESTORE UNLOCK TARGETS";
    case NKikimrSchemeOp::EOperationType::ESchemeOpIncrementalRestoreFinalize:
        return "RESTORE INCREMENTAL FINALIZE";
    // secret
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateSecret:
        return "CREATE SECRET";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterSecret:
        return "ALTER SECRET";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropSecret:
        return "DROP SECRET";
    // streaming query
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateStreamingQuery:
        return "CREATE STREAMING QUERY";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropStreamingQuery:
        return "DROP STREAMING QUERY";
    case NKikimrSchemeOp::EOperationType::ESchemeOpAlterStreamingQuery:
        return "ALTER STREAMING QUERY";
    case NKikimrSchemeOp::EOperationType::ESchemeOpTruncateTable:
        return "TRUNCATE TABLE";
    // test shard set
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTestShardSet:
        return "CREATE TEST SHARD SET";
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTestShardSet:
        return "DROP TEST SHARD SET";
    }
    Y_ABORT("switch should cover all operation types");
}

// The paths an audit record shows for one request.
//
// ExtractPathRefs (schemeshard_path_footprint.h) already knows which protobuf
// field of every operation type carries a path and how Propose() resolves it;
// JoinPathRef turns one such ref into a string using nothing but the request.
// This is the same knowledge the 136-arm switch that used to live here
// duplicated by hand, which is why several operations logged a wrong path or
// none at all.
//
// Only Target and Source refs are reported: "changing paths" means the paths
// the operation changes, plus the ones it reads to produce them. A Parent ref
// (the table a cdc stream hangs off) and a Dependency ref (a replication
// destination, an external data source) are not what the record is about.
//
// A by-id ref and an Implicit ref produce no entry: resolving a path id, or
// enumerating the children a cascade will touch, needs schemeshard state that
// a pure function over TModifyScheme does not have. That is deliberate -- the
// old code logged the bare working dir for an id-addressed request, a path
// that has nothing to do with the target -- and an empty result makes the
// audit line omit the "paths" field entirely.
TVector<TString> ExtractChangingPaths(const NKikimrSchemeOp::TModifyScheme& tx) {
    using namespace NKikimr::NSchemeShard;

    // ESchemeOpAlterLogin resolves no TPath at all: it only checks that the
    // working dir names the login audience, so the extractor emits nothing for
    // it. The audit record has always shown the working dir, and for a login
    // operation that is the whole of what it touches.
    if (tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin) {
        return {tx.GetWorkingDir()};
    }

    // One string per ref, in extraction order, including the ones left out of
    // the result: a sibling leaf whose base is another ref reads it from here.
    TVector<TString> joined;
    TVector<TString> result;
    for (const auto& ref : ExtractPathRefs(tx)) {
        joined.push_back(JoinPathRef(tx.GetWorkingDir(), ref, joined));
        if (joined.back().empty()) {
            continue;
        }
        if (ref.Role == EPathRefRole::Target || ref.Role == EPathRefRole::Source) {
            result.push_back(joined.back());
        }
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
    TVector<TString> LoginUserChange;
};

TChangeLogin ExtractLoginChange(const NKikimrSchemeOp::TModifyScheme& tx) {
    if (tx.HasAlterLogin()) {
        const auto& alter = tx.GetAlterLogin();

        TChangeLogin result;
        switch (tx.GetAlterLogin().GetAlterCase()) {
            case NKikimrSchemeOp::TAlterLogin::kCreateUser: {
                result.LoginUser = alter.GetCreateUser().GetUser();
                break;
            }

            case NKikimrSchemeOp::TAlterLogin::kModifyUser: {
                const auto& modify = alter.GetModifyUser();
                result.LoginUser = modify.GetUser();

                if (modify.HasHashedPassword()) {
                    result.LoginUserChange.push_back("password");
                }

                if (modify.HasCanLogin() && modify.GetCanLogin()) {
                    result.LoginUserChange.push_back("unblocking");
                }

                if (modify.HasCanLogin() && !modify.GetCanLogin()) {
                    result.LoginUserChange.push_back("blocking");
                }

                break;
            }

            case NKikimrSchemeOp::TAlterLogin::kRemoveUser: {
                result.LoginUser = alter.GetRemoveUser().GetUser();
                break;
            }

            case NKikimrSchemeOp::TAlterLogin::kCreateGroup: {
                result.LoginGroup = alter.GetCreateGroup().GetGroup();
                break;
            }

            case NKikimrSchemeOp::TAlterLogin::kAddGroupMembership: {
                result.LoginGroup = alter.GetAddGroupMembership().GetGroup();
                result.LoginMember = alter.GetAddGroupMembership().GetMember();
                break;
            }

            case NKikimrSchemeOp::TAlterLogin::kRemoveGroupMembership: {
                result.LoginGroup = alter.GetRemoveGroupMembership().GetGroup();
                result.LoginMember = alter.GetRemoveGroupMembership().GetMember();
                break;
            }

            case NKikimrSchemeOp::TAlterLogin::kRenameGroup: {
                result.LoginGroup = alter.GetRenameGroup().GetGroup();
                break;
            }

            case NKikimrSchemeOp::TAlterLogin::kRemoveGroup: {
                result.LoginGroup = alter.GetRemoveGroup().GetGroup();
                break;
            }

            default: {
                Y_ABORT("switch should cover all operation types");
            }
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
    auto [loginUser, loginGroup, loginMember, loginUserChange] = ExtractLoginChange(tx);

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
        .LoginUserChange = loginUserChange
    };
}

}
