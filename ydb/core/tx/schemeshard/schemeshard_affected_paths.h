#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

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
