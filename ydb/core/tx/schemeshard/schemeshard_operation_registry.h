#pragma once

#include <ydb/core/tx/schemeshard/generated/operation_registry.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <array>

namespace NKikimrSchemeOp {
class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

class ISubOperation;
struct TOperation;
struct TOperationContext;

using TSchemeOperationParts = TVector<TIntrusivePtr<ISubOperation>>;
using TSchemeOperationFactory = TSchemeOperationParts(
    const TOperation&, const NKikimrSchemeOp::TModifyScheme&, TOperationContext&);

enum class ESchemeOperationSupport {
    Implemented, // Has a working operation factory.
    Internal, // Handled within another operation.
    Unsupported, // Not implemented or obsolete; dispatch aborts or invokes an incomplete legacy factory.
    Rejected, // Handler returns an explicit unsupported-operation response.
};

namespace NOperationFactories {

TSchemeOperationFactory MakeMkDir;
TSchemeOperationFactory MakeRmDir;
TSchemeOperationFactory MakeModifyACL;
TSchemeOperationFactory MakeAlterUserAttributes;
TSchemeOperationFactory MakeForceDropUnsafe;
TSchemeOperationFactory MakeCreateTable;
TSchemeOperationFactory MakeAlterTable;
TSchemeOperationFactory MakeSplitMergeTablePartitions;
TSchemeOperationFactory MakeBackup;
TSchemeOperationFactory MakeRestore;
TSchemeOperationFactory MakeDropTable;
TSchemeOperationFactory MakeCreateIndexedTable;
TSchemeOperationFactory MakeCreateConsistentCopyTables;
TSchemeOperationFactory MakeCreateRtmrVolume;
TSchemeOperationFactory MakeCreateColumnStore;
TSchemeOperationFactory MakeAlterColumnStore;
TSchemeOperationFactory MakeDropColumnStore;
TSchemeOperationFactory MakeCreateColumnTable;
TSchemeOperationFactory MakeAlterColumnTable;
TSchemeOperationFactory MakeDropColumnTable;
TSchemeOperationFactory MakeCreatePersQueueGroup;
TSchemeOperationFactory MakeAlterPersQueueGroup;
TSchemeOperationFactory MakeDropPersQueueGroup;
TSchemeOperationFactory MakeCreateSolomonVolume;
TSchemeOperationFactory MakeAlterSolomonVolume;
TSchemeOperationFactory MakeDropSolomonVolume;
TSchemeOperationFactory MakeCreateSubDomain;
TSchemeOperationFactory MakeAlterSubDomain;
TSchemeOperationFactory MakeDropSubDomain;
TSchemeOperationFactory MakeForceDropSubDomain;
TSchemeOperationFactory MakeCreateExtSubDomain;
TSchemeOperationFactory MakeAlterExtSubDomain;
TSchemeOperationFactory MakeForceDropExtSubDomain;
TSchemeOperationFactory MakeCreateKesus;
TSchemeOperationFactory MakeAlterKesus;
TSchemeOperationFactory MakeDropKesus;
TSchemeOperationFactory MakeUpgradeSubDomain;
TSchemeOperationFactory MakeUpgradeSubDomainDecision;
TSchemeOperationFactory MakeCreateColumnBuild;
TSchemeOperationFactory MakeDropColumnBuild;
TSchemeOperationFactory MakeCreateIndexBuild;
TSchemeOperationFactory MakeCreateLock;
TSchemeOperationFactory MakeDropLock;
TSchemeOperationFactory MakeCreateBlockStoreVolume;
TSchemeOperationFactory MakeAssignBlockStoreVolume;
TSchemeOperationFactory MakeAlterBlockStoreVolume;
TSchemeOperationFactory MakeDropBlockStoreVolume;
TSchemeOperationFactory MakeCreateFileStore;
TSchemeOperationFactory MakeAlterFileStore;
TSchemeOperationFactory MakeDropFileStore;
TSchemeOperationFactory MakeAlterLogin;
TSchemeOperationFactory MakeCreateSequence;
TSchemeOperationFactory MakeAlterSequence;
TSchemeOperationFactory MakeDropSequence;
TSchemeOperationFactory MakeApplyIndexBuild;
TSchemeOperationFactory MakeInitiateBuildIndexImplTable;
TSchemeOperationFactory MakePrepareIndexValidation;
TSchemeOperationFactory MakeCancelIndexBuild;
TSchemeOperationFactory MakeDropIndex;
TSchemeOperationFactory MakeCreateCdcStream;
TSchemeOperationFactory MakeAlterCdcStream;
TSchemeOperationFactory MakeDropCdcStream;
TSchemeOperationFactory MakeRotateCdcStream;
TSchemeOperationFactory MakeMoveTable;
TSchemeOperationFactory MakeMoveTableIndex;
TSchemeOperationFactory MakeMoveIndex;
TSchemeOperationFactory MakeMoveSequence;
TSchemeOperationFactory MakeCreateReplication;
TSchemeOperationFactory MakeAlterReplication;
TSchemeOperationFactory MakeDropReplication;
TSchemeOperationFactory MakeDropReplicationCascade;
TSchemeOperationFactory MakeCreateTransfer;
TSchemeOperationFactory MakeAlterTransfer;
TSchemeOperationFactory MakeDropTransfer;
TSchemeOperationFactory MakeDropTransferCascade;
TSchemeOperationFactory MakeCreateBlobDepot;
TSchemeOperationFactory MakeAlterBlobDepot;
TSchemeOperationFactory MakeDropBlobDepot;
TSchemeOperationFactory MakeCreateExternalTable;
TSchemeOperationFactory MakeDropExternalTable;
TSchemeOperationFactory MakeCreateExternalDataSource;
TSchemeOperationFactory MakeDropExternalDataSource;
TSchemeOperationFactory MakeCreateView;
TSchemeOperationFactory MakeDropView;
TSchemeOperationFactory MakeCreateContinuousBackup;
TSchemeOperationFactory MakeAlterContinuousBackup;
TSchemeOperationFactory MakeDropContinuousBackup;
TSchemeOperationFactory MakeCreateResourcePool;
TSchemeOperationFactory MakeDropResourcePool;
TSchemeOperationFactory MakeAlterResourcePool;
TSchemeOperationFactory MakeRestoreMultipleIncrementalBackups;
TSchemeOperationFactory MakeCreateBackupCollection;
TSchemeOperationFactory MakeDropBackupCollection;
TSchemeOperationFactory MakeBackupBackupCollection;
TSchemeOperationFactory MakeBackupIncrementalBackupCollection;
TSchemeOperationFactory MakeCreateFullBackupOp;
TSchemeOperationFactory MakeRestoreBackupCollection;
TSchemeOperationFactory MakeCreateLongIncrementalRestoreOp;
TSchemeOperationFactory MakeCreateSysView;
TSchemeOperationFactory MakeDropSysView;
TSchemeOperationFactory MakeChangePathState;
TSchemeOperationFactory MakeIncrementalRestoreLockTargets;
TSchemeOperationFactory MakeIncrementalRestoreUnlockTargets;
TSchemeOperationFactory MakeIncrementalRestoreFinalize;
TSchemeOperationFactory MakeCreateSecret;
TSchemeOperationFactory MakeAlterSecret;
TSchemeOperationFactory MakeDropSecret;
TSchemeOperationFactory MakeCreateStreamingQuery;
TSchemeOperationFactory MakeDropStreamingQuery;
TSchemeOperationFactory MakeAlterStreamingQuery;
TSchemeOperationFactory MakeTruncateTable;
TSchemeOperationFactory MakeCreateTestShardSet;
TSchemeOperationFactory MakeDropTestShardSet;

} // namespace NOperationFactories

struct TSchemeOperationInfo {
    NKikimrSchemeOp::EOperationType Type;
    TSchemeOperationFactory* Factory = nullptr;
    ESchemeOperationSupport Support = ESchemeOperationSupport::Implemented;
    const char* Reason = nullptr;
};

inline constexpr TSchemeOperationInfo SchemeOperations[] = {
    {NKikimrSchemeOp::ESchemeOpMkDir, &NOperationFactories::MakeMkDir},
    {NKikimrSchemeOp::ESchemeOpRmDir, &NOperationFactories::MakeRmDir},
    {NKikimrSchemeOp::ESchemeOpModifyACL, &NOperationFactories::MakeModifyACL},
    {NKikimrSchemeOp::ESchemeOpAlterUserAttributes, &NOperationFactories::MakeAlterUserAttributes},
    {NKikimrSchemeOp::ESchemeOpForceDropUnsafe, &NOperationFactories::MakeForceDropUnsafe},
    {NKikimrSchemeOp::ESchemeOpCreateTable, &NOperationFactories::MakeCreateTable},
    {NKikimrSchemeOp::ESchemeOpAlterTable, &NOperationFactories::MakeAlterTable},
    {NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions, &NOperationFactories::MakeSplitMergeTablePartitions},
    {NKikimrSchemeOp::ESchemeOpBackup, &NOperationFactories::MakeBackup},
    {NKikimrSchemeOp::ESchemeOpRestore, &NOperationFactories::MakeRestore},
    {NKikimrSchemeOp::ESchemeOpDropTable, &NOperationFactories::MakeDropTable},
    {NKikimrSchemeOp::ESchemeOpCreateIndexedTable, &NOperationFactories::MakeCreateIndexedTable},
    {NKikimrSchemeOp::ESchemeOpCreateTableIndex,
        nullptr, ESchemeOperationSupport::Internal, "is handled as part of ESchemeOpCreateIndexedTable"},
    {NKikimrSchemeOp::ESchemeOpDropTableIndex, nullptr, ESchemeOperationSupport::Internal, "is handled as part of ESchemeOpDropTable"},
    {NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables, &NOperationFactories::MakeCreateConsistentCopyTables},
    {NKikimrSchemeOp::ESchemeOpCreateRtmrVolume, &NOperationFactories::MakeCreateRtmrVolume},
    {NKikimrSchemeOp::ESchemeOpCreateColumnStore, &NOperationFactories::MakeCreateColumnStore},
    {NKikimrSchemeOp::ESchemeOpAlterColumnStore, &NOperationFactories::MakeAlterColumnStore},
    {NKikimrSchemeOp::ESchemeOpDropColumnStore, &NOperationFactories::MakeDropColumnStore},
    {NKikimrSchemeOp::ESchemeOpCreateColumnTable, &NOperationFactories::MakeCreateColumnTable},
    {NKikimrSchemeOp::ESchemeOpAlterColumnTable, &NOperationFactories::MakeAlterColumnTable},
    {NKikimrSchemeOp::ESchemeOpDropColumnTable, &NOperationFactories::MakeDropColumnTable},
    {NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup, &NOperationFactories::MakeCreatePersQueueGroup},
    {NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup, &NOperationFactories::MakeAlterPersQueueGroup},
    {NKikimrSchemeOp::ESchemeOpDropPersQueueGroup, &NOperationFactories::MakeDropPersQueueGroup},
    {NKikimrSchemeOp::ESchemeOpCreateSolomonVolume, &NOperationFactories::MakeCreateSolomonVolume},
    {NKikimrSchemeOp::ESchemeOpAlterSolomonVolume, &NOperationFactories::MakeAlterSolomonVolume},
    {NKikimrSchemeOp::ESchemeOpDropSolomonVolume, &NOperationFactories::MakeDropSolomonVolume},
    {NKikimrSchemeOp::ESchemeOpCreateSubDomain, &NOperationFactories::MakeCreateSubDomain},
    {NKikimrSchemeOp::ESchemeOpAlterSubDomain, &NOperationFactories::MakeAlterSubDomain},
    {NKikimrSchemeOp::ESchemeOpDropSubDomain, &NOperationFactories::MakeDropSubDomain},
    {NKikimrSchemeOp::ESchemeOpForceDropSubDomain, &NOperationFactories::MakeForceDropSubDomain},
    {NKikimrSchemeOp::ESchemeOpCreateExtSubDomain, &NOperationFactories::MakeCreateExtSubDomain},
    {NKikimrSchemeOp::ESchemeOpAlterExtSubDomain, &NOperationFactories::MakeAlterExtSubDomain},
    {NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain, &NOperationFactories::MakeForceDropExtSubDomain},
    {NKikimrSchemeOp::ESchemeOpCreateKesus, &NOperationFactories::MakeCreateKesus},
    {NKikimrSchemeOp::ESchemeOpAlterKesus, &NOperationFactories::MakeAlterKesus},
    {NKikimrSchemeOp::ESchemeOpDropKesus, &NOperationFactories::MakeDropKesus},
    {NKikimrSchemeOp::ESchemeOpUpgradeSubDomain, &NOperationFactories::MakeUpgradeSubDomain},
    {NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision, &NOperationFactories::MakeUpgradeSubDomainDecision},
    {NKikimrSchemeOp::ESchemeOpCreateColumnBuild, &NOperationFactories::MakeCreateColumnBuild},
    {NKikimrSchemeOp::ESchemeOpDropColumnBuild, &NOperationFactories::MakeDropColumnBuild},
    {NKikimrSchemeOp::ESchemeOpCreateIndexBuild, &NOperationFactories::MakeCreateIndexBuild},
    {NKikimrSchemeOp::ESchemeOpCreateLock, &NOperationFactories::MakeCreateLock},
    {NKikimrSchemeOp::ESchemeOpDropLock, &NOperationFactories::MakeDropLock},
    {NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume, &NOperationFactories::MakeCreateBlockStoreVolume},
    {NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume, &NOperationFactories::MakeAssignBlockStoreVolume},
    {NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume, &NOperationFactories::MakeAlterBlockStoreVolume},
    {NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume, &NOperationFactories::MakeDropBlockStoreVolume},
    {NKikimrSchemeOp::ESchemeOpCreateFileStore, &NOperationFactories::MakeCreateFileStore},
    {NKikimrSchemeOp::ESchemeOpAlterFileStore, &NOperationFactories::MakeAlterFileStore},
    {NKikimrSchemeOp::ESchemeOpDropFileStore, &NOperationFactories::MakeDropFileStore},
    {NKikimrSchemeOp::ESchemeOpAlterLogin, &NOperationFactories::MakeAlterLogin},
    {NKikimrSchemeOp::ESchemeOpCreateSequence, &NOperationFactories::MakeCreateSequence},
    {NKikimrSchemeOp::ESchemeOpAlterSequence, &NOperationFactories::MakeAlterSequence},
    {NKikimrSchemeOp::ESchemeOpDropSequence, &NOperationFactories::MakeDropSequence},
    {NKikimrSchemeOp::ESchemeOpApplyIndexBuild, &NOperationFactories::MakeApplyIndexBuild},
    {NKikimrSchemeOp::ESchemeOpAlterTableIndex,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable, &NOperationFactories::MakeInitiateBuildIndexImplTable},
    {NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexImplTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpInitiateBuildIndexMainTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexMainTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpPrepareIndexValidation, &NOperationFactories::MakePrepareIndexValidation},
    {NKikimrSchemeOp::ESchemeOpCancelIndexBuild, &NOperationFactories::MakeCancelIndexBuild},
    {NKikimrSchemeOp::ESchemeOpDropIndex, &NOperationFactories::MakeDropIndex},
    {NKikimrSchemeOp::ESchemeOpDropTableIndexAtMainTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpCreateCdcStream, &NOperationFactories::MakeCreateCdcStream},
    {NKikimrSchemeOp::ESchemeOpCreateCdcStreamImpl,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpCreateCdcStreamAtTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpAlterCdcStream, &NOperationFactories::MakeAlterCdcStream},
    {NKikimrSchemeOp::ESchemeOpAlterCdcStreamImpl,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpAlterCdcStreamAtTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpDropCdcStream, &NOperationFactories::MakeDropCdcStream},
    {NKikimrSchemeOp::ESchemeOpDropCdcStreamImpl,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpDropCdcStreamAtTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpRotateCdcStream, &NOperationFactories::MakeRotateCdcStream},
    {NKikimrSchemeOp::ESchemeOpRotateCdcStreamImpl,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpRotateCdcStreamAtTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOp_DEPRECATED_35, nullptr, ESchemeOperationSupport::Unsupported, "impossible"},
    {NKikimrSchemeOp::ESchemeOpMoveTable, &NOperationFactories::MakeMoveTable},
    {NKikimrSchemeOp::ESchemeOpMoveTableIndex, &NOperationFactories::MakeMoveTableIndex},
    {NKikimrSchemeOp::ESchemeOpMoveIndex, &NOperationFactories::MakeMoveIndex},
    {NKikimrSchemeOp::ESchemeOpMoveSequence, &NOperationFactories::MakeMoveSequence},
    {NKikimrSchemeOp::ESchemeOpCreateReplication, &NOperationFactories::MakeCreateReplication},
    {NKikimrSchemeOp::ESchemeOpAlterReplication, &NOperationFactories::MakeAlterReplication},
    {NKikimrSchemeOp::ESchemeOpDropReplication, &NOperationFactories::MakeDropReplication},
    {NKikimrSchemeOp::ESchemeOpDropReplicationCascade, &NOperationFactories::MakeDropReplicationCascade},
    {NKikimrSchemeOp::ESchemeOpCreateTransfer, &NOperationFactories::MakeCreateTransfer},
    {NKikimrSchemeOp::ESchemeOpAlterTransfer, &NOperationFactories::MakeAlterTransfer},
    {NKikimrSchemeOp::ESchemeOpDropTransfer, &NOperationFactories::MakeDropTransfer},
    {NKikimrSchemeOp::ESchemeOpDropTransferCascade, &NOperationFactories::MakeDropTransferCascade},
    {NKikimrSchemeOp::ESchemeOpCreateBlobDepot, &NOperationFactories::MakeCreateBlobDepot},
    {NKikimrSchemeOp::ESchemeOpAlterBlobDepot, &NOperationFactories::MakeAlterBlobDepot, ESchemeOperationSupport::Unsupported},
    {NKikimrSchemeOp::ESchemeOpDropBlobDepot, &NOperationFactories::MakeDropBlobDepot, ESchemeOperationSupport::Unsupported},
    {NKikimrSchemeOp::ESchemeOpCreateExternalTable, &NOperationFactories::MakeCreateExternalTable},
    {NKikimrSchemeOp::ESchemeOpDropExternalTable, &NOperationFactories::MakeDropExternalTable},
    {NKikimrSchemeOp::ESchemeOpAlterExternalTable, nullptr, ESchemeOperationSupport::Unsupported},
    {NKikimrSchemeOp::ESchemeOpCreateExternalDataSource, &NOperationFactories::MakeCreateExternalDataSource},
    {NKikimrSchemeOp::ESchemeOpDropExternalDataSource, &NOperationFactories::MakeDropExternalDataSource},
    {NKikimrSchemeOp::ESchemeOpAlterExternalDataSource, nullptr, ESchemeOperationSupport::Unsupported},
    {NKikimrSchemeOp::ESchemeOpCreateView, &NOperationFactories::MakeCreateView},
    {NKikimrSchemeOp::ESchemeOpDropView, &NOperationFactories::MakeDropView},
    {NKikimrSchemeOp::ESchemeOpAlterView, nullptr, ESchemeOperationSupport::Unsupported},
    {NKikimrSchemeOp::ESchemeOpCreateContinuousBackup, &NOperationFactories::MakeCreateContinuousBackup},
    {NKikimrSchemeOp::ESchemeOpAlterContinuousBackup, &NOperationFactories::MakeAlterContinuousBackup},
    {NKikimrSchemeOp::ESchemeOpDropContinuousBackup, &NOperationFactories::MakeDropContinuousBackup},
    {NKikimrSchemeOp::ESchemeOpCreateResourcePool, &NOperationFactories::MakeCreateResourcePool},
    {NKikimrSchemeOp::ESchemeOpDropResourcePool, &NOperationFactories::MakeDropResourcePool},
    {NKikimrSchemeOp::ESchemeOpAlterResourcePool, &NOperationFactories::MakeAlterResourcePool},
    {NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups,
        &NOperationFactories::MakeRestoreMultipleIncrementalBackups, ESchemeOperationSupport::Rejected},
    {NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpCreateBackupCollection, &NOperationFactories::MakeCreateBackupCollection},
    {NKikimrSchemeOp::ESchemeOpAlterBackupCollection, nullptr, ESchemeOperationSupport::Unsupported},
    {NKikimrSchemeOp::ESchemeOpDropBackupCollection, &NOperationFactories::MakeDropBackupCollection},
    {NKikimrSchemeOp::ESchemeOpBackupBackupCollection, &NOperationFactories::MakeBackupBackupCollection},
    {NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection, &NOperationFactories::MakeBackupIncrementalBackupCollection},
    {NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp,
        nullptr, ESchemeOperationSupport::Internal, "multipart operations are handled before, also they require transaction details"},
    {NKikimrSchemeOp::ESchemeOpCreateFullBackupOp, &NOperationFactories::MakeCreateFullBackupOp},
    {NKikimrSchemeOp::ESchemeOpRestoreBackupCollection, &NOperationFactories::MakeRestoreBackupCollection},
    {NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp, &NOperationFactories::MakeCreateLongIncrementalRestoreOp},
    {NKikimrSchemeOp::ESchemeOpCreateSysView, &NOperationFactories::MakeCreateSysView},
    {NKikimrSchemeOp::ESchemeOpDropSysView, &NOperationFactories::MakeDropSysView},
    {NKikimrSchemeOp::ESchemeOpChangePathState, &NOperationFactories::MakeChangePathState},
    {NKikimrSchemeOp::ESchemeOpIncrementalRestoreLockTargets, &NOperationFactories::MakeIncrementalRestoreLockTargets},
    {NKikimrSchemeOp::ESchemeOpIncrementalRestoreUnlockTargets, &NOperationFactories::MakeIncrementalRestoreUnlockTargets},
    {NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize, &NOperationFactories::MakeIncrementalRestoreFinalize},
    {NKikimrSchemeOp::ESchemeOpCreateSecret, &NOperationFactories::MakeCreateSecret},
    {NKikimrSchemeOp::ESchemeOpAlterSecret, &NOperationFactories::MakeAlterSecret},
    {NKikimrSchemeOp::ESchemeOpDropSecret, &NOperationFactories::MakeDropSecret},
    {NKikimrSchemeOp::ESchemeOpCreateStreamingQuery, &NOperationFactories::MakeCreateStreamingQuery},
    {NKikimrSchemeOp::ESchemeOpDropStreamingQuery, &NOperationFactories::MakeDropStreamingQuery},
    {NKikimrSchemeOp::ESchemeOpAlterStreamingQuery, &NOperationFactories::MakeAlterStreamingQuery},
    {NKikimrSchemeOp::ESchemeOpTruncateTable, &NOperationFactories::MakeTruncateTable},
    {NKikimrSchemeOp::ESchemeOpCreateTestShardSet, &NOperationFactories::MakeCreateTestShardSet},
    {NKikimrSchemeOp::ESchemeOpDropTestShardSet, &NOperationFactories::MakeDropTestShardSet},
};

namespace NOperationRegistry {

constexpr size_t CountEntries(NKikimrSchemeOp::EOperationType type) {
    size_t count = 0;
    for (const auto& entry : SchemeOperations) {
        count += entry.Type == type;
    }
    return count;
}

constexpr bool ValidFactories() {
    for (const auto& entry : SchemeOperations) {
        switch (entry.Support) {
        case ESchemeOperationSupport::Implemented:
        case ESchemeOperationSupport::Rejected:
            if (!entry.Factory || entry.Reason) {
                return false;
            }
            break;
        case ESchemeOperationSupport::Internal:
            if (entry.Factory || !entry.Reason) {
                return false;
            }
            break;
        case ESchemeOperationSupport::Unsupported:
            if (entry.Factory && entry.Reason) {
                return false;
            }
            break;
        default:
            return false;
        }
    }
    return true;
}

static_assert(std::size(SchemeOperations) == SchemeOperationCount, "Operation registry size does not match protobuf");
static_assert(CheckSchemeOperationRegistry<CountEntries>());
static_assert(ValidFactories(), "Operation support status does not match its factory or rejection reason");

inline constexpr auto Lookup = [] {
    std::array<const TSchemeOperationInfo*, NKikimrSchemeOp::EOperationType_ARRAYSIZE> result{};
    for (const auto& entry : SchemeOperations) {
        result[entry.Type] = &entry;
    }
    return result;
}();

} // namespace NOperationRegistry

constexpr const TSchemeOperationInfo* FindSchemeOperation(NKikimrSchemeOp::EOperationType type) {
    const auto index = static_cast<size_t>(type);
    return index < NOperationRegistry::Lookup.size() ? NOperationRegistry::Lookup[index] : nullptr;
}

constexpr ESchemeOperationSupport GetSchemeOperationSupport(NKikimrSchemeOp::EOperationType type) {
    const auto* entry = FindSchemeOperation(type);
    Y_ABORT_UNLESS(entry);
    return entry->Support;
}

[[noreturn]] inline void AbortUnimplementedSchemeOperation(NKikimrSchemeOp::EOperationType type) {
    Y_ABORT("Scheme operation %d is not implemented", static_cast<int>(type));
}

template <NKikimrSchemeOp::EOperationType Type>
[[noreturn]] void AbortUnimplementedSchemeOperation() {
    static_assert(GetSchemeOperationSupport(Type) == ESchemeOperationSupport::Unsupported,
        "Replace the unsupported dispatch when implementing an operation");
    AbortUnimplementedSchemeOperation(Type);
}

TSchemeOperationParts MakeRegisteredOperationParts(
    const TOperation& op, const NKikimrSchemeOp::TModifyScheme& tx, TOperationContext& context);

} // namespace NKikimr::NSchemeShard
