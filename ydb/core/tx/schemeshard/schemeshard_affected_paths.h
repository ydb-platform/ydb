#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSchemeShard {

struct TAffectedPath {
    enum class ERole : ui8 {
        Target,
        Source,
        Container,
    };

    // Mirrors the suboperation precedence: where a request carries both an id and
    // a name, every subop resolves by id (schemeshard__operation_alter_table.cpp,
    // schemeshard__operation_drop_table.cpp). A declaration that preferred the
    // name would name a different object than the operation mutates.
    enum class ELocator : ui8 {
        ByPath,
        ByPathId,
    };

    // What kind of thing this entry is, for a consumer that filters. A consumer that only
    // wants the schema story reads SchemaEffect and skips the rest; one auditing physical
    // writes needs all three.
    enum class EEffectClass : ui8 {
        SchemaEffect,          // a logical change to the object
        Reference,             // a dependency the DDL names but does not change
        BookkeepingInternal,   // a path-row write with no logical meaning
    };

    // The logical change itself.
    enum class EEffect : ui8 {
        Create,
        Alter,
        Drop,
        MoveFrom,
        MoveTo,
        ChildrenChanged,
    };

    // Intent, NOT outcome. Whether a physical path-row write is expected of this entry.
    enum class EObservation : ui8 {
        MustWrite,      // a write must happen, or the declaration is wrong
        MayWrite,       // a successful no-op is legitimate
        ReferenceOnly,  // no write expected at all
    };

    ELocator Locator = ELocator::ByPath;
    ERole Role = ERole::Target;

    // Absolute, already joined with WorkingDir. SplitIntoTransactions only moves the
    // WorkingDir/Name boundary, so an absolute path survives the auto-mkdir split
    // unchanged and may be declared once, before the split.
    TString Path;
    TPathId PathId;

    // A create names a path that does not exist yet, so resolution must fall back
    // to the parent rather than treating absence as failure.
    bool MustExist = false;

    // Defaults chosen so a declaration that sets none of these keeps the meaning it had
    // before the typed model existed: an unqualified schema change whose path-row write is
    // permitted but not demanded.
    EEffectClass Class = EEffectClass::SchemaEffect;
    EEffect      Effect = EEffect::Alter;

    // Intent only: what the declaration claims should happen. There is deliberately no
    // Outcome (Applied/NotApplied) beside it -- what actually happened is known only after
    // execution, and belongs in memory at completion rather than in the declaration, where
    // it would read as a claim the declarer was never in a position to make.
    EObservation Expect = EObservation::MayWrite;
};

struct TAffectedPaths {
    TVector<TAffectedPath> Paths;

    // The operation touches paths that cannot be enumerated from the request alone
    // (cascade drops, backup-collection expansion). Set this rather than returning a
    // short list that reads as complete.
    bool Incomplete = false;

    // The declaration was attempted and failed -- a named path id did not resolve, say.
    // Distinct from an empty Paths, which means the operation genuinely touches nothing.
    // Collapsing the two is what let the old IsPathlessOp read a failure as "no paths".
    bool Unresolved = false;
};

class TSchemeShard;

// Turns an undeclared path write from a log line into an abort naming the path and the
// Persist* that made it. Set by the schemeshard test environment (TTestEnv), so a suite is
// covered by constructing a test env rather than by remembering an env var or a per-suite
// ya.make line -- and a suite added later is covered without anyone remembering anything.
// Never set in production: the point of it is that it stops the tablet.
inline bool UndeclaredPathTouchIsFatal = false;

// The reverse check's own switch, deliberately separate from UndeclaredPathTouchIsFatal and
// deliberately not set by TTestEnv yet.
//
// The mechanism is wired and proven: poisoning a declaration with a path nobody writes aborts
// naming that path and its txId.
//
// The helper-level half of the audit is done. DeclareTargetByIdOrName used to inherit
// Create/MustWrite wholesale from DeclareChildOfWorkingDir on its by-name branch, demanding a
// path-row write from every by-name alter and drop -- TAlterTable being the extreme case, its
// file contains no path-row Persist* call at all. Both of its branches now say Alter/MayWrite,
// which is the only claim that helper is in a position to make. That took the armed failures
// from 94 to 10.
//
// The remaining 10 are one class and need per-call-site classification rather than a helper
// change: operations that are not creates call DeclareChildOfWorkingDir directly, and so
// assert Create/MustWrite on a container that never gains a child. Named by the armed run:
//
//   AssignBlockStoreVolume, AssignBlockStoreVolumeDuringAlter   -> /MyRoot/BSVolume
//   AlterMigratedIndexTable                                     -> /MyRoot/Tenant/Table/Index
//   AlterIndexTableDirectly, ConsistentCopyAfterDropIndexes,
//   CopyLockedTableForBackup, CopyTableForBackup, OnlineBuild,
//   DefaultStorageConfigTableWithChannelProfileIdBuildIndex,
//   PersistUniqueIndexKeySize-OnCreate-false                    -> /MyRoot
//
// Each wants its own answer -- an assign is not a create, a lock on an existing table does not
// give its parent a child -- and the fix changes what the outbox records, since Effect is part
// of the record. Arm this from TTestEnv in that change; an unarmed check proves nothing.
inline bool UnfulfilledPathDeclarationIsFatal = false;

// The reverse half of the cross-check. ObservePathTouched tests written ⊆ declared, which
// by construction cannot see an over-declaration: a declaration naming a path nobody writes
// passes silently, so the six subtree walks recently added are verified only in the one
// direction the tooling can see. This tests the other direction -- every entry the
// declaration marked MustWrite got a write -- and is answerable only at completion, once
// every phase has had its chance to write.
//
// Takes the flat inputs rather than a TOperation so the semantics are testable without
// standing up a schemeshard, the way the declarations above already are. Returns the first
// unfulfilled path so the caller can name it; MayWrite and ReferenceOnly are skipped, and
// an Incomplete or exempt (nullopt) declaration demands nothing, because neither was ever
// in a position to promise its list was whole.
std::optional<TString> FindUnfulfilledMustWrite(
    const TVector<std::optional<TAffectedPaths>>& declared,
    const THashSet<TString>& observed);

// The common shape: an object named directly under WorkingDir. The container is part of
// it because creating or removing a child bumps the parent's DirAlterVersion, which is a
// path-row write in its own right.
TAffectedPaths DeclareChildOfWorkingDir(const TString& workingDir, const TString& name);

// For requests that may name their target either way. The single implementation of the
// precedence: a local path id wins over the name, matching what every suboperation does
// (TAlterTable, TDropTable). Declaring name-first here is what let the outbox record a
// different object than the operation mutated. Pass localPathId == 0 when absent.
TAffectedPaths DeclareTargetByIdOrName(TSchemeShard* ss, const TString& workingDir,
    const TString& name, ui64 localPathId);

// Overloads taking the context rather than the schemeshard, so a declaration can be written
// as a single line in the traits table. TOperationContext is only forward-declared there;
// binding a reference to an incomplete type is fine, and the member access happens in the
// .cpp where it is complete.
struct TOperationContext;
TAffectedPaths DeclareTargetByIdOrName(const TOperationContext& context,
    const TString& workingDir, const TString& name, ui64 localPathId);
TAffectedPaths DeclareCascadeTargetByIdOrName(const TOperationContext& context,
    const TString& workingDir, const TString& name, ui64 localPathId);

// Every path in the target's subtree, for an operation that writes a row per descendant
// itself rather than fanning out into parts. Uses the same ListSubTree the operations
// already call at propose, so the declaration cannot name a different set than the walk.
//
// includeRoot says whether that walk covers the root: TDropExtSubdomain erases it from the
// set before dropping (schemeshard__operation_drop_extsubdomain.cpp:213) while
// TDropForceUnsafe does not (schemeshard__operation_drop_unsafe.cpp:223).
//
// The effect is the caller's to state and cannot be inferred here: a force drop takes its
// descendants with it, an owner change only alters them. The container of the root is
// declared as well -- all of these operations bump its DirAlterVersion.
TAffectedPaths DeclareSubTree(TSchemeShard* ss, TPathId root, bool includeRoot,
    TAffectedPath::EEffect effect);

// The same, for a request that names its root the way an ordinary target is named. Keeps
// the id-over-name precedence in one place: it is the property that stops a declaration
// naming a different object than the operation mutates, so it must not be re-spelled per
// call site. Pass localPathId == 0 when absent.
TAffectedPaths DeclareSubTreeByIdOrName(TSchemeShard* ss, const TString& workingDir,
    const TString& name, ui64 localPathId, bool includeRoot, TAffectedPath::EEffect effect);

// A drop that takes the target's whole subtree with it. The root is named exactly, as it
// is what the request asked for and what the outbox records, but the descendants are
// walked at execution time and cannot be enumerated here -- hence Incomplete, which turns
// the path cross-check off for the operation rather than letting it report every child.
TAffectedPaths DeclareCascadeTargetByIdOrName(TSchemeShard* ss, const TString& workingDir,
    const TString& name, ui64 localPathId);

} // namespace NKikimr::NSchemeShard
