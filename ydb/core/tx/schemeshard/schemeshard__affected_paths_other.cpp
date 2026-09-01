#include "schemeshard__affected_paths_traits.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"

#include <ydb/core/base/path.h>

// Affected-path declarations for the operations that do not fit the plain
// create/drop/alter families: compound factories that decompose into several
// suboperations, and control ops whose target is named by an absolute path rather
// than by WorkingDir plus a leaf name.
//
// They live here rather than beside their factories because several of those .cpp
// files own more than one operation family and were being edited in parallel. The
// specialization only needs to be in the same library as its dispatch, so nothing
// about the mechanism requires it to sit next to CreateXxx.

namespace NKikimr::NSchemeShard {

namespace {

// A request that names its target by absolute path instead of WorkingDir plus a leaf
// name. The parent goes in for the same reason DeclareChildOfWorkingDir declares
// WorkingDir: creating or removing a child bumps the parent's DirAlterVersion, which is
// a path-row write of its own.
void AddAbsoluteTarget(TAffectedPaths& result, const TString& path,
        TAffectedPath::ERole role = TAffectedPath::ERole::Target)
{
    if (path.empty()) {
        return;
    }
    result.Paths.push_back(TAffectedPath{
        .Role = role,
        .Path = path,
    });
    const TStringBuf parent = ExtractParent(path);
    if (!parent.empty()) {
        result.Paths.push_back(TAffectedPath{
            .Role = TAffectedPath::ERole::Container,
            .Path = TString(parent),
        });
    }
}

// The lock/unlock and finalize control ops carry a repeated field of paths, each of
// which may be absolute or relative to WorkingDir. The suboperation makes exactly this
// distinction (schemeshard__operation_incr_restore_lock_targets.cpp:47-51), so the
// declaration has to make it the same way or it names a different object.
void AddPathList(TAffectedPaths& result, const TString& workingDir,
        const ::google::protobuf::RepeatedPtrField<TString>& paths)
{
    for (const auto& path : paths) {
        if (path.empty()) {
            continue;
        }
        const bool isAbsolute = path[0] == '/';
        result.Paths.push_back(TAffectedPath{
            .Role = TAffectedPath::ERole::Target,
            .Path = isAbsolute ? path : JoinPath({workingDir, path}),
        });
    }
    if (!workingDir.empty()) {
        result.Paths.push_back(TAffectedPath{
            .Role = TAffectedPath::ERole::Container,
            .Path = workingDir,
        });
    }
}

} // namespace

// ---------------------------------------------------------------------------------
// SplitMerge
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpSplitMergeTablePartitions =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpSplitMergeTablePartitions>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpSplitMergeTablePartitions>(
    TAffectedESchemeOpSplitMergeTablePartitions,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // The request may name the table by an absolute path or by a path id, and the path
    // id may carry a foreign owner. DeclareTargetByIdOrName cannot express the second
    // case -- it forces the local owner through MakeLocalId -- so the resolution is
    // spelled out here exactly as TSplitMerge::Propose spells it
    // (schemeshard__operation_split_merge.cpp:815-825, :856-857).
    const auto& info = tx.GetSplitMergeTablePartitions();

    TPathId pathId;
    if (info.HasTableOwnerId() && info.HasTableLocalId()) {
        pathId = TPathId(TOwnerId(info.GetTableOwnerId()), TLocalPathId(info.GetTableLocalId()));
    } else if (info.HasTableLocalId()) {
        pathId = context.SS->MakeLocalId(TLocalPathId(info.GetTableLocalId()));
    }

    const TPath path = pathId
        ? TPath::Init(pathId, context.SS)
        : TPath::Resolve(info.GetTablePath(), context.SS);

    TAffectedPaths result;
    // Deliberately not Unresolved: the suboperation rejects an unresolvable table with
    // its own status, and turning that into StatusPreconditionFailed here would change
    // the error the caller sees. An empty declaration is accurate -- an operation that
    // never proposes writes nothing.
    if (!path.IsResolved()) {
        return result;
    }
    result.Paths.push_back(TAffectedPath{
        .Locator = TAffectedPath::ELocator::ByPathId,
        .Role = TAffectedPath::ERole::Target,
        .Path = path.PathString(),
        .PathId = path.Base()->PathId,
    });
    return result;
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// Backup / Restore of a single table
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpBackup =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpBackup>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpBackup>(
    TAffectedESchemeOpBackup,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // TBackup::GetTableName, resolved as WorkingDir.Dive(name)
    // (schemeshard__operation_backup_restore_common.h:716-717, :748).
    //
    // Not DeclareChildOfWorkingDir: a backup does not create the table it names, so it must
    // not claim the Create/MustWrite that helper asserts. Same paths, honest intent.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetBackup().GetTableName(), 0);
}

} // namespace NOperation

using TAffectedESchemeOpRestore =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpRestore>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpRestore>(
    TAffectedESchemeOpRestore,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // Restores into a table that already exists, so not a create -- see TBackup above.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetRestore().GetTableName(), 0);
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// AssignBlockStoreVolume
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpAssignBlockStoreVolume =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpAssignBlockStoreVolume>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpAssignBlockStoreVolume>(
    TAffectedESchemeOpAssignBlockStoreVolume,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // An assign, not a create: the volume is already there.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetAssignBlockStoreVolume().GetName(), 0);
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// CreateLock
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpCreateLock =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpCreateLock>(
    TAffectedESchemeOpCreateLock,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // A lock is taken on a table that already exists. Nothing gains a child, so the
    // container must not be told it does.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetLockConfig().GetName(), 0);
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// PrepareIndexValidation
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpPrepareIndexValidation =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpPrepareIndexValidation>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpPrepareIndexValidation>(
    TAffectedESchemeOpPrepareIndexValidation,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    return DeclareChildOfWorkingDir(tx.GetWorkingDir(),
        tx.GetPrepareIndexValidation().GetTableName());
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// InitiateBuildIndexImplTable
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpInitiateBuildIndexImplTable =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpInitiateBuildIndexImplTable>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpInitiateBuildIndexImplTable>(
    TAffectedESchemeOpInitiateBuildIndexImplTable,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // A create of the index impl table under the index path, carried in GetCreateTable
    // like every other create-table request (schemeshard__operation.cpp:1631-1638).
    return DeclareChildOfWorkingDir(tx.GetWorkingDir(), tx.GetCreateTable().GetName());
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// Column build
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpCreateColumnBuild =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnBuild>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpCreateColumnBuild>(
    TAffectedESchemeOpCreateColumnBuild,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // Decomposes into a single InitiateBuildIndexMainTable on the named table
    // (index/operation_create_build_index.cpp:21-23, :44-51).
    TAffectedPaths result;
    AddAbsoluteTarget(result, tx.GetInitiateColumnBuild().GetTable());
    return result;
}

} // namespace NOperation

using TAffectedESchemeOpDropColumnBuild =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnBuild>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpDropColumnBuild>(
    TAffectedESchemeOpDropColumnBuild,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // Decomposes into a single AlterTable on the named table
    // (index/operation_apply_build_index.cpp:249-281).
    TAffectedPaths result;
    AddAbsoluteTarget(result, tx.GetDropColumnBuild().GetSettings().GetTable());
    return result;
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// Index build: create / apply / cancel
// ---------------------------------------------------------------------------------

namespace {

// The index build ops all name a main table and, optionally, one index under it, then
// fan out over the index's impl tables. Which impl tables those are is decided by the
// index type inside the CalcXxxImplTableDesc helpers on create, and read from the
// index's existing children on apply/cancel -- so that set is genuinely absent from the
// request. This used to be marked Incomplete for that reason, which was the wrong
// conclusion from a true premise: the fan-out is propose-time, not execution-time, and
// every impl table becomes a CONSTRUCTED PART carrying its own transaction, so admitParts
// (schemeshard__operation.cpp:401) asks each one and widens the set with it.
//   create: index/operation_create_build_index.cpp:215-242 pushes CreateNewTableIndex,
//     CreateInitializeBuildIndexMainTable and one CreateInitializeBuildIndexImplTable per
//     impl table; the impl table names its own path via GetCreateTable().GetName().
//   apply/cancel: index/operation_apply_build_index.cpp:127-172 and :221-241 loop over the
//     index's existing children, and each part -- FinalizeBuildIndexImplTable, DropTable,
//     FinalizeBuildIndexMainTable -- carries the impl table name in its own tx with
//     WorkingDir set to the index path.
// Verified rather than assumed: poisoning the ESchemeOpInitiateBuildIndexImplTable
// declaration alone makes 98 ut_index_build tests abort on
//   "wrote path row it had not declared: /MyRoot/ServerLessDB/Table/index1/indexImplTable"
// which shows both that the cross-check is armed for these ops and that the impl-table
// coverage comes from the part, not from anything this helper needs to say.
TAffectedPaths DeclareIndexBuildRoots(const TString& tablePath, const TString& indexName) {
    TAffectedPaths result;
    AddAbsoluteTarget(result, tablePath);
    if (!indexName.empty() && !tablePath.empty()) {
        result.Paths.push_back(TAffectedPath{
            .Role = TAffectedPath::ERole::Target,
            .Path = JoinPath({tablePath, indexName}),
        });
    }
    return result;
}

} // namespace

using TAffectedESchemeOpCreateIndexBuild =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateIndexBuild>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpCreateIndexBuild>(
    TAffectedESchemeOpCreateIndexBuild,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // index/operation_create_build_index.cpp:101 (table), :109 (index),
    // :224-242 and :244-363 (impl tables, per index type).
    const auto& op = tx.GetInitiateIndexBuild();
    return DeclareIndexBuildRoots(op.GetTable(), op.GetIndex().GetName());
}

} // namespace NOperation

using TAffectedESchemeOpApplyIndexBuild =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpApplyIndexBuild>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpApplyIndexBuild>(
    TAffectedESchemeOpApplyIndexBuild,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // index/operation_apply_build_index.cpp:96 (table), :123 (index),
    // :147-171 (loop over the index's existing children).
    const auto& op = tx.GetApplyIndexBuild();
    return DeclareIndexBuildRoots(op.GetTablePath(), op.GetIndexName());
}

} // namespace NOperation

using TAffectedESchemeOpCancelIndexBuild =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCancelIndexBuild>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpCancelIndexBuild>(
    TAffectedESchemeOpCancelIndexBuild,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // index/operation_apply_build_index.cpp:184 (table), :220 (index),
    // :231-241 (loop over the index's existing children).
    const auto& op = tx.GetCancelIndexBuild();
    return DeclareIndexBuildRoots(op.GetTablePath(), op.GetIndexName());
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// ConsistentCopyTables
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpCreateConsistentCopyTables =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpCreateConsistentCopyTables>(
    TAffectedESchemeOpCreateConsistentCopyTables,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // The request enumerates the table pairs (schemeshard__operation_consistent_copy_
    // tables.cpp:185-187), and each pair then fans out over the source's children --
    // indexes and their impl tables (:282, :343), and sequences (:387, :412) --
    // synthesizing destination paths that are nowhere in the request. That was the
    // stated reason for Incomplete, and it describes the request correctly; it is still
    // the wrong conclusion, because the fan-out happens at propose. Every descendant is
    // pushed as a constructed part -- CreateNewTableIndex at :336/:340, CreateCopyAnyTable
    // at :381, CreateCopySequence at :428 -- and admitParts asks each part for its own
    // declaration, which each one can answer from its own tx.
    //
    // Note this marker was NOT unreachable, contrary to the note at :553 below. That note
    // is right that CreateConsistentCopyTables expands inline for a backup/restore of a
    // collection, so the marker never travels upward there. But schemeshard__operation.cpp
    // :1587 dispatches a genuine top-level ESchemeOpCreateConsistentCopyTables, proposed by
    // grpc_services/rpc_copy_tables.cpp:37, schemeshard_export_flow_proposals.cpp:52 and
    // ut_helpers/helpers.cpp:1009. For those the request-level loop at
    // schemeshard__operation.cpp:300 does read this declaration, and the Incomplete did
    // disarm the check for the whole copy.
    TAffectedPaths result;
    for (const auto& descr : tx.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
        AddAbsoluteTarget(result, descr.GetSrcPath(), TAffectedPath::ERole::Source);
        AddAbsoluteTarget(result, descr.GetDstPath(), TAffectedPath::ERole::Target);
    }
    return result;
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// RotateCdcStream
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpRotateCdcStream =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpRotateCdcStream>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpRotateCdcStream>(
    TAffectedESchemeOpRotateCdcStream,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // All three path rows the rotation writes are named by the request: the table, the
    // stream being retired and the stream replacing it
    // (schemeshard__operation_rotate_cdc_stream.cpp:367, :371-372, :631; names read at
    // :729-731 and joined at :739, :767, :784).
    const auto& op = tx.GetRotateCdcStream();
    const TString tablePath = JoinPath({tx.GetWorkingDir(), op.GetTableName()});

    TAffectedPaths result;
    result.Paths.push_back(TAffectedPath{
        .Role = TAffectedPath::ERole::Target,
        .Path = tablePath,
    });
    result.Paths.push_back(TAffectedPath{
        .Role = TAffectedPath::ERole::Target,
        .Path = JoinPath({tablePath, op.GetOldStreamName()}),
    });
    result.Paths.push_back(TAffectedPath{
        .Role = TAffectedPath::ERole::Target,
        .Path = JoinPath({tablePath, op.GetNewStream().GetStreamDescription().GetName()}),
    });
    result.Paths.push_back(TAffectedPath{
        .Role = TAffectedPath::ERole::Container,
        .Path = tx.GetWorkingDir(),
    });
    return result;
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// TruncateTable
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpTruncateTable =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpTruncateTable>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpTruncateTable>(
    TAffectedESchemeOpTruncateTable,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // DfsOnTableChildrenTree walks the table's whole child tree -- indexes, their impl
    // tables and sequences -- emitting a truncate per node. That walk runs at propose:
    // CreateConsistentTruncateTable (schemeshard__operation_truncate_table.cpp:560) calls it
    // at :589 and it pushes a CreateTruncateTable part per node at :388. Only the root is in
    // the request, but every part is asked for its own declaration, so the tree is covered.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetTruncateTable().GetTableName(), 0);
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// UpgradeSubDomain
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpUpgradeSubDomain =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomain>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpUpgradeSubDomain>(
    TAffectedESchemeOpUpgradeSubDomain,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // The upgrade rewrites every path in the subdomain: ListSubTree feeds PersistLastTxId
    // (schemeshard__operation_upgrade_subdomain.cpp:46, :73), PersistPath (:566, :578)
    // and PersistPathDirAlterVersion (:637, :644, :1045, :1051). The request names only
    // the subdomain root -- but Propose already walks the subtree at :1184, so the
    // declaration can walk it too instead of switching the check off.
    //
    // The path-type check at :1187-:1199 does not narrow that set. It is a precondition:
    // one descendant of a forbidden type rejects the whole operation with
    // StatusPreconditionFailed, so either nothing is written or all of it is.
    //
    // Effect::Alter, includeRoot: an upgrade rewrites paths rather than creating or
    // dropping them, and the root is rewritten along with the rest (:1202 iterates the
    // full set, unlike the type check above it which skips the root).
    return DeclareSubTreeByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetUpgradeSubDomain().GetName(), 0, /*includeRoot=*/true,
        TAffectedPath::EEffect::Alter);
}

} // namespace NOperation

using TAffectedESchemeOpUpgradeSubDomainDecision =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpUpgradeSubDomainDecision>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpUpgradeSubDomainDecision>(
    TAffectedESchemeOpUpgradeSubDomainDecision,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // Commit and Undo both walk the same subtree the upgrade touched
    // (schemeshard__operation_upgrade_subdomain.cpp:847, :993, :1045, :1051).
    //
    // Unlike UpgradeSubDomain, this op's own Propose does not walk -- but the declaration
    // does not need it to. DeclareSubTree runs its own ListSubTree over the in-memory tree
    // at declaration time, and the decision is taken on a subdomain that is mid-upgrade and
    // structurally frozen, so that set is the set the commit or undo rewrites.
    return DeclareSubTreeByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetUpgradeSubDomain().GetName(), 0, /*includeRoot=*/true,
        TAffectedPath::EEffect::Alter);
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// Backup collection: backup / incremental backup / restore
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpBackupBackupCollection =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpBackupBackupCollection>(
    TAffectedESchemeOpBackupBackupCollection,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // The fan-out is propose-time, not execution-time: CreateBackupBackupCollection
    // (schemeshard__operation_backup_backup_collection.cpp:34) walks the description at
    // :85-:119, pushes the control part at :123 and hands the copy set to
    // CreateConsistentCopyTables at :265. So admitParts sees the copy parts and this could
    // in principle be a plain target.
    //
    // It cannot be yet. CreateConsistentCopyTables expands inline into the caller's part
    // list rather than proposing an ESchemeOpCreateConsistentCopyTables part, so nothing
    // from that op's own declaration travels upward here. (That declaration no longer
    // carries an Incomplete anyway -- it was retired once its fan-out was confirmed to be
    // propose-time; but it was never dead weight, since the top-level CopyTables and export
    // paths do consult it. See the note above it.) What admitParts actually asks for a
    // backup or restore is each copy part, whose transaction is ESchemeOpCreateTable
    // with CopyFromTable set -- and that declaration disclaims itself, returning nullopt
    // (schemeshard__operation_create_table.cpp:874). The disclaimer is right for a top-level
    // request, where MakeOperationParts re-dispatches such a tx to CreateCopyTable
    // (schemeshard__operation.cpp:1567) and the path row is written by the parts below it.
    // It is wrong for a part built by CCT, where TCopyTable::Propose itself persists the
    // destination, its parent and the source (schemeshard__operation_copy_table.cpp:819-821).
    // All three are knowable from that part's own tx (WorkingDir + Name + CopyFromTable).
    //
    // The copy half of that is now fixed: the CreateTable declaration no longer disclaims a
    // copy-from-table create, it declares the destination, its parent and the source, so the
    // copy parts cover themselves.
    //
    // Still blocked, on a second and unrelated gap. Retiring this marker now aborts on
    //   /MyRoot/TestTable/19700101000009Z_continuousBackupImpl
    // -- the continuous-backup CDC stream, whose name is minted from Now() inside
    // CreateAlterContinuousBackup (schemeshard__operation_alter_continuous_backup.cpp:176-180)
    // rather than coming from the request. The part that creates it does carry the name in
    // its own tx, but that part is an ESchemeOpCreateCdcStream, which is itself still
    // Incomplete (schemeshard__operation_create_cdc_stream.cpp:944). Retire this with the
    // CDC *AtTable* family, not before.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetBackupBackupCollection().GetName(), 0);
}

} // namespace NOperation

using TAffectedESchemeOpBackupIncrementalBackupCollection =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpBackupIncrementalBackupCollection>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpBackupIncrementalBackupCollection>(
    TAffectedESchemeOpBackupIncrementalBackupCollection,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // The request names only the collection, but CreateBackupIncrementalBackupCollection
    // (schemeshard__operation_backup_incremental_backup_collection.cpp:155) expands it at
    // propose: an AlterContinuousBackup part per description entry (:220), another per index
    // impl table (:287), and the control part at :296. Only the writes land at execution, and
    // every part is asked for its own declaration.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetBackupIncrementalBackupCollection().GetName(), 0);
}

} // namespace NOperation

using TAffectedESchemeOpRestoreBackupCollection =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpRestoreBackupCollection>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpRestoreBackupCollection>(
    TAffectedESchemeOpRestoreBackupCollection,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    // Like BackupBackupCollection above, the fan-out is propose-time: the child scan for the
    // last full backup and the incrementals after it is at
    // schemeshard__operation_restore_backup_collection.cpp:395-:401, the pairing with the
    // description's entries at :414-:430, and the parts come from CreateConsistentCopyTables
    // (:432) and CreateIncrementalBackupPathStateOps (:436).
    //
    // Restore trips both halves of the copy -- the destination is the live table it restores
    // into and the source is the backup-dir copy -- so it needed the CreateTable declaration
    // to stop disclaiming a copy-from-table create. That is now done, and both halves are
    // covered by the copy part that writes them.
    //
    // Held with BackupBackupCollection above for the same second reason: the incremental
    // path reaches the continuous-backup CDC stream whose name is minted from Now(), and the
    // part that creates it is still Incomplete. Retire the pair with the CDC *AtTable* family.
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(),
        tx.GetRestoreBackupCollection().GetName(), 0);
}

} // namespace NOperation

using TAffectedESchemeOpCreateLongIncrementalRestoreOp =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpCreateLongIncrementalRestoreOp>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpCreateLongIncrementalRestoreOp>(
    TAffectedESchemeOpCreateLongIncrementalRestoreOp,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // The control op targets the collection alone; the tables it records in
    // TLongIncrementalRestoreOp are data, not paths it writes
    // (schemeshard__operation_restore_backup_collection.cpp:224, :236-240).
    return DeclareChildOfWorkingDir(tx.GetWorkingDir(),
        tx.GetRestoreBackupCollection().GetName());
}

} // namespace NOperation

// ---------------------------------------------------------------------------------
// Incremental restore: lock / unlock / finalize / change path state
// ---------------------------------------------------------------------------------

using TAffectedESchemeOpChangePathState =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpChangePathState>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpChangePathState>(
    TAffectedESchemeOpChangePathState,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // Joined exactly as the suboperation joins it, and it is a genuine path-row write:
    // schemeshard__operation_change_path_state.cpp:55 and :81.
    return DeclareChildOfWorkingDir(tx.GetWorkingDir(), tx.GetChangePathState().GetPath());
}

} // namespace NOperation

using TAffectedESchemeOpIncrementalRestoreLockTargets =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpIncrementalRestoreLockTargets>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpIncrementalRestoreLockTargets>(
    TAffectedESchemeOpIncrementalRestoreLockTargets,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // One ChangePathState suboperation per listed path, and the list is the whole set
    // (schemeshard__operation_incr_restore_lock_targets.cpp:44-69).
    const auto& targets = tx.GetIncrementalRestoreLockTargets();
    TAffectedPaths result;
    AddPathList(result, tx.GetWorkingDir(), targets.GetDstPaths());
    AddPathList(result, tx.GetWorkingDir(), targets.GetSrcPaths());
    return result;
}

} // namespace NOperation

using TAffectedESchemeOpIncrementalRestoreUnlockTargets =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpIncrementalRestoreUnlockTargets>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpIncrementalRestoreUnlockTargets>(
    TAffectedESchemeOpIncrementalRestoreUnlockTargets,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // Same decomposition as the lock, with the reverse target state.
    const auto& targets = tx.GetIncrementalRestoreLockTargets();
    TAffectedPaths result;
    AddPathList(result, tx.GetWorkingDir(), targets.GetDstPaths());
    AddPathList(result, tx.GetWorkingDir(), targets.GetSrcPaths());
    return result;
}

} // namespace NOperation

using TAffectedESchemeOpIncrementalRestoreFinalize =
    TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpIncrementalRestoreFinalize>;

namespace NOperation {

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpIncrementalRestoreFinalize>(
    TAffectedESchemeOpIncrementalRestoreFinalize,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    Y_UNUSED(context);
    // Both lists are absolute paths and together they are the full set the finalize
    // touches (schemeshard__operation_incremental_restore_finalize.cpp:350, :364).
    const auto& finalize = tx.GetIncrementalRestoreFinalize();
    TAffectedPaths result;
    for (const auto& path : finalize.GetTargetTablePaths()) {
        AddAbsoluteTarget(result, path);
    }
    for (const auto& path : finalize.GetBackupTablePaths()) {
        AddAbsoluteTarget(result, path, TAffectedPath::ERole::Source);
    }
    return result;
}

} // namespace NOperation

} // namespace NKikimr::NSchemeShard
