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

namespace NOperation {

template <class TTraits>
std::optional<TAffectedPaths> GetAffectedPaths(
    TTraits traits, const TTxTransaction& tx, const TOperationContext& context);

} // namespace NOperation

// The three shapes that cover 95 of the 123 declarations in this table. Writing one of
// these IS the whole task for an ordinary operation -- no .cpp change, no template
// boilerplate. Reach for a hand-written GetAffectedPaths only when the operation genuinely
// does something else (multiple targets, a rename, a path derived from more than a name).
//
// `accessor` is the TModifyScheme getter carrying the request, e.g. GetCreateFoo. It must be
// the same field the suboperation's Propose resolves, and the same one GetTargetName returns
// if the op has one -- a declaration that reads a different field names a different object
// than the operation mutates, which is the exact failure this table exists to prevent.

// Creates a new object named under WorkingDir.
#define SS_DECLARES_CHILD_OF_WORKING_DIR(op, accessor)                              \
    SS_DECLARES_AFFECTED_PATHS(op);                                                 \
    namespace NOperation {                                                          \
    template <>                                                                     \
    inline std::optional<TAffectedPaths>                                            \
    GetAffectedPaths<TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>>(    \
        TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>,                  \
        const TTxTransaction& tx, const TOperationContext&)                         \
    {                                                                               \
        return DeclareChildOfWorkingDir(tx.GetWorkingDir(), tx.accessor().GetName());\
    }                                                                               \
    }

// Acts on an existing object, which the request may name or identify by path id.
#define SS_DECLARES_TARGET_BY_ID_OR_NAME(op, accessor)                              \
    SS_DECLARES_AFFECTED_PATHS(op);                                                 \
    namespace NOperation {                                                          \
    template <>                                                                     \
    inline std::optional<TAffectedPaths>                                            \
    GetAffectedPaths<TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>>(    \
        TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>,                  \
        const TTxTransaction& tx, const TOperationContext& context)                 \
    {                                                                               \
        const auto& request = tx.accessor();                                        \
        return DeclareTargetByIdOrName(context, tx.GetWorkingDir(), request.GetName(),\
            request.HasId() ? request.GetId() : 0);                                 \
    }                                                                               \
    }

// There is no SS_DECLARES_CASCADE_TARGET. It declared the root and gave up on the subtree,
// switching the cross-check off for the whole operation -- which meant the operations with the
// least obvious paths were the only ones nobody checked. All 16 of its users were converted to
// exact enumeration; a cascade drop walks the same ListSubTree the operation itself calls, so
// the set is knowable at propose. Write that shape with DeclareSubTreeByIdOrName, as the drop
// families in schemeshard__affected_paths_other.cpp do.

#define SS_DECLARES_AFFECTED_PATHS(op)                                              \
    template <>                                                                     \
    struct TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>                \
        : TAffectedPathsDeclares {}

// Exemption is a category, not prose. Free text let anything through -- including, during
// this migration, a justification that turned out to be false -- and a reviewer cannot check
// a sentence as easily as they can check "is this factory really Y_ABORT". Pick the one that
// applies; if none does, the operation writes path rows and has to declare them.
#define SS_EXEMPT_AFFECTED_PATHS(op, why)                                           \
    template <>                                                                     \
    struct TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::op>                \
        : TAffectedPathsExempt {                                                    \
        static constexpr const char* Why = why;                                     \
    }

// The factory in schemeshard__operation.cpp is Y_ABORT("TODO: implement").
#define SS_EXEMPT_UNIMPLEMENTED(op)                                                 \
    SS_EXEMPT_AFFECTED_PATHS(op, "unimplemented: the factory is Y_ABORT(TODO: implement)")

// Reachable, but Propose is an unfinished stub that resolves nothing and writes nothing.
#define SS_EXEMPT_UNFINISHED_STUB(op)                                               \
    SS_EXEMPT_AFFECTED_PATHS(op, "Propose is an unfinished stub: sets state and returns")

// Withdrawn: the factory unconditionally CreateRejects, or the op number is retired.
#define SS_EXEMPT_RETIRED(op)                                                       \
    SS_EXEMPT_AFFECTED_PATHS(op, "retired: nothing is ever proposed for this op")

// Genuinely writes no row in Schema::Paths. `what` names the table it writes instead, so the
// claim can be checked against the code rather than taken on trust.
#define SS_EXEMPT_NO_PATH_ROW(op, what)                                             \
    SS_EXEMPT_AFFECTED_PATHS(op, "writes no path row; writes " what)

SS_DECLARES_AFFECTED_PATHS(ESchemeOpMkDir);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreatePersQueueGroup);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropPersQueueGroup);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterPersQueueGroup);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpModifyACL);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpRmDir);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpSplitMergeTablePartitions);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpBackup);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateSubDomain);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSubDomain);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateRtmrVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateBlockStoreVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterBlockStoreVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAssignBlockStoreVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropBlockStoreVolume);
SS_DECLARES_CHILD_OF_WORKING_DIR(ESchemeOpCreateKesus, GetKesus);
SS_DECLARES_TARGET_BY_ID_OR_NAME(ESchemeOpDropKesus, GetDrop);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpForceDropSubDomain);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateSolomonVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSolomonVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterKesus);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterSubDomain);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterUserAttributes);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpForceDropUnsafe);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateIndexedTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateTableIndex);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateConsistentCopyTables);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropTableIndex);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateExtSubDomain);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterExtSubDomain);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpForceDropExtSubDomain);
SS_EXEMPT_RETIRED(ESchemeOp_DEPRECATED_35);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpUpgradeSubDomain);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpUpgradeSubDomainDecision);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateIndexBuild);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpInitiateBuildIndexMainTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateLock);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpApplyIndexBuild);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpFinalizeBuildIndexMainTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterTableIndex);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterSolomonVolume);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropLock);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpFinalizeBuildIndexImplTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpInitiateBuildIndexImplTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropIndex);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropTableIndexAtMainTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCancelIndexBuild);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateFileStore);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterFileStore);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropFileStore);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpRestore);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateColumnStore);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterColumnStore);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropColumnStore);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateColumnTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterColumnTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropColumnTable);
SS_EXEMPT_NO_PATH_ROW(ESchemeOpAlterLogin, "the subdomain security state");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateCdcStream);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateCdcStreamImpl);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateCdcStreamAtTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterCdcStream);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterCdcStreamImpl);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterCdcStreamAtTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropCdcStream);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropCdcStreamImpl);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropCdcStreamAtTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpRotateCdcStream);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpRotateCdcStreamImpl);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpRotateCdcStreamAtTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpMoveTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpMoveTableIndex);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateSequence);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterSequence);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSequence);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateReplication);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterReplication);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropReplicationCascade);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateBlobDepot);
SS_EXEMPT_UNFINISHED_STUB(ESchemeOpAlterBlobDepot);
SS_EXEMPT_UNFINISHED_STUB(ESchemeOpDropBlobDepot);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpMoveIndex);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterExtSubDomainCreateHive);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateExternalTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropExternalTable);
SS_EXEMPT_UNIMPLEMENTED(ESchemeOpAlterExternalTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateExternalDataSource);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropExternalDataSource);
SS_EXEMPT_UNIMPLEMENTED(ESchemeOpAlterExternalDataSource);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateColumnBuild);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropColumnBuild);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateView);
SS_EXEMPT_UNIMPLEMENTED(ESchemeOpAlterView);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropView);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropReplication);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateContinuousBackup);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterContinuousBackup);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropContinuousBackup);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateResourcePool);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropResourcePool);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterResourcePool);
SS_EXEMPT_RETIRED(ESchemeOpRestoreMultipleIncrementalBackups);
SS_EXEMPT_RETIRED(ESchemeOpRestoreIncrementalBackupAtTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateBackupCollection);
SS_EXEMPT_UNIMPLEMENTED(ESchemeOpAlterBackupCollection);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropBackupCollection);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpBackupBackupCollection);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpBackupIncrementalBackupCollection);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpRestoreBackupCollection);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpMoveSequence);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateTransfer);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterTransfer);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropTransfer);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropTransferCascade);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateSysView);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSysView);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateLongIncrementalRestoreOp);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpChangePathState);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpIncrementalRestoreFinalize);
SS_EXEMPT_NO_PATH_ROW(ESchemeOpCreateLongIncrementalBackupOp, "the IncrementalBackups list, keyed by TxId");
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateSecret);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterSecret);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropSecret);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateStreamingQuery);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropStreamingQuery);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpAlterStreamingQuery);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpTruncateTable);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpPrepareIndexValidation);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpIncrementalRestoreLockTargets);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpIncrementalRestoreUnlockTargets);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateFullBackupOp);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpCreateTestShardSet);
SS_DECLARES_AFFECTED_PATHS(ESchemeOpDropTestShardSet);

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
