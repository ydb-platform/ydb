#pragma once

#include "schemeshard__op_traits.h"
#include "schemeshard_affected_paths.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/generated/op_type_list.h>

#include <optional>

namespace NKikimr::NSchemeShard {

// Deliberately undefined, so an operation reaching a generated case label without a
// specialization fails to build. The switch alone is not relied on for that guarantee:
// it ends in a default: label, which would route an unenumerated operation to the
// fallback and silently read as exempt. SS_FOR_EACH_OP_TYPE below closes that.
template <NKikimrSchemeOp::EOperationType opType>
struct TAffectedPathsTraits;

struct TAffectedPathsDeclares {
    static constexpr bool Declares = true;
};

struct TAffectedPathsExempt {
    static constexpr bool Declares = false;
};

// Bound to DispatchOp's default: label. Reaching it at runtime would mean the generated
// switch has no case for this operation type; SS_FOR_EACH_OP_TYPE below rules that out
// at compile time, so this exists only to keep the dispatch template well formed.
struct TAffectedPathsUnreachable {
    static constexpr bool Declares = false;
};

#define SS_DECLARES_AFFECTED_PATHS(op)                                              \
    template <>                                                                     \
    struct TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>                \
        : TAffectedPathsDeclares {}

#define SS_EXEMPT_AFFECTED_PATHS(op, why)                                           \
    template <>                                                                     \
    struct TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>                \
        : TAffectedPathsExempt {                                                    \
        static constexpr const char* Why = why;                                     \
    }

SS_DECLARES_AFFECTED_PATHS(ESchemeOpMkDir);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateTable,                       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreatePersQueueGroup,              "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropTable,                         "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropPersQueueGroup);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterTable,                        "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterPersQueueGroup,               "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpModifyACL,                         "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpRmDir);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpSplitMergeTablePartitions,         "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpBackup,                            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateSubDomain,                   "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSubDomain);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateRtmrVolume,                  "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateBlockStoreVolume);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterBlockStoreVolume,             "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAssignBlockStoreVolume,            "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropBlockStoreVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateKesus);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropKesus);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpForceDropSubDomain);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateSolomonVolume,               "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSolomonVolume);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterKesus,                 "declared but unverified; no alter coverage in harness");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterSubDomain,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterUserAttributes,               "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpForceDropUnsafe);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateIndexedTable,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateTableIndex,                  "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateConsistentCopyTables,        "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropTableIndex,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateExtSubDomain,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterExtSubDomain,                 "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpForceDropExtSubDomain);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOp_DEPRECATED_35,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpUpgradeSubDomain,                  "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpUpgradeSubDomainDecision,          "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateIndexBuild,                  "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpInitiateBuildIndexMainTable,       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateLock,                        "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpApplyIndexBuild,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpFinalizeBuildIndexMainTable,       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterTableIndex,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterSolomonVolume,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropLock,                          "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpFinalizeBuildIndexImplTable,       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpInitiateBuildIndexImplTable,       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropIndex,                         "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropTableIndexAtMainTable,         "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCancelIndexBuild,                  "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateFileStore);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterFileStore,                    "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropFileStore);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpRestore,                           "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateColumnStore,                 "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterColumnStore,                  "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropColumnStore,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateColumnTable,                 "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterColumnTable,                  "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropColumnTable,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterLogin,                        "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateCdcStream,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateCdcStreamImpl,               "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateCdcStreamAtTable,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterCdcStream,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterCdcStreamImpl,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterCdcStreamAtTable,             "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropCdcStream,                     "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropCdcStreamImpl,                 "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropCdcStreamAtTable,              "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpRotateCdcStream,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpRotateCdcStreamImpl,               "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpRotateCdcStreamAtTable,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpMoveTable,                         "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpMoveTableIndex,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateSequence,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterSequence,                     "not yet migrated");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSequence);
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateReplication,                 "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterReplication,                  "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropReplicationCascade,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateBlobDepot,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterBlobDepot,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropBlobDepot,                     "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpMoveIndex,                         "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterExtSubDomainCreateHive,       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateExternalTable,               "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropExternalTable,                 "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterExternalTable,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateExternalDataSource,          "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropExternalDataSource,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterExternalDataSource,           "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateColumnBuild,                 "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropColumnBuild,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateView,                        "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterView,                         "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropView,                          "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropReplication,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateContinuousBackup,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterContinuousBackup,             "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropContinuousBackup,              "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateResourcePool,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropResourcePool,                  "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterResourcePool,                 "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpRestoreMultipleIncrementalBackups, "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpRestoreIncrementalBackupAtTable,   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateBackupCollection,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterBackupCollection,             "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropBackupCollection,              "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpBackupBackupCollection,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpBackupIncrementalBackupCollection, "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpRestoreBackupCollection,           "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpMoveSequence,                      "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateTransfer,                    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterTransfer,                     "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropTransfer,                      "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropTransferCascade,               "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateSysView,                     "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropSysView,                       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateLongIncrementalRestoreOp,    "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpChangePathState,                   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpIncrementalRestoreFinalize,        "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateLongIncrementalBackupOp,     "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateSecret,                      "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterSecret,                       "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropSecret,                        "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateStreamingQuery,              "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropStreamingQuery,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpAlterStreamingQuery,               "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpTruncateTable,                     "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpPrepareIndexValidation,            "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpIncrementalRestoreLockTargets,     "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpIncrementalRestoreUnlockTargets,   "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateFullBackupOp,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpCreateTestShardSet,                "not yet migrated");
SS_EXEMPT_AFFECTED_PATHS(ESchemeOpDropTestShardSet,                  "not yet migrated");

#undef SS_DECLARES_AFFECTED_PATHS
#undef SS_EXEMPT_AFFECTED_PATHS

// The switch in DispatchOp ends in a default: label, so a missing case would route to
// TAffectedPathsUnreachable and read as "exempt" rather than failing. This closes that:
// SS_FOR_EACH_OP_TYPE is generated from the enum descriptor and has no default, and
// sizeof on an incomplete type is an error, so an operation added without a declaration
// above breaks the build here rather than being silently skipped at runtime.
#define SS_ASSERT_AFFECTED_PATHS_DECLARED(op)                                       \
    static_assert(sizeof(TAffectedPathsTraits<op>) > 0,                             \
        "operation type " #op " has no affected-paths declaration; add "            \
        "SS_DECLARES_AFFECTED_PATHS or SS_EXEMPT_AFFECTED_PATHS for it");

SS_FOR_EACH_OP_TYPE(SS_ASSERT_AFFECTED_PATHS_DECLARED)

#undef SS_ASSERT_AFFECTED_PATHS_DECLARED

namespace NOperation {

template <class TTraits>
std::optional<TAffectedPaths> GetAffectedPaths(
    TTraits traits, const TTxTransaction& tx, const TOperationContext& context);

} // namespace NOperation

template <class TTraits>
std::optional<TAffectedPaths> GetAffectedPaths(
    TTraits traits, const TTxTransaction& tx, const TOperationContext& context)
{
    if constexpr (TTraits::Declares) {
        return NOperation::GetAffectedPaths(traits, tx, context);
    }
    return std::nullopt;
}

// Instantiation point for the traits template. One must live in the shipped library
// rather than only in tests: with no caller, a missing specialization is never
// diagnosed and the undefined primary guarantees nothing.
bool OperationDeclaresAffectedPaths(const TTxTransaction& tx);

} // namespace NKikimr::NSchemeShard
