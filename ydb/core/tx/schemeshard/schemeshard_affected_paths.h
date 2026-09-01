#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/hash.h>
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

    // There is deliberately no Incomplete flag. It existed as a migration crutch -- "this
    // operation touches paths I cannot enumerate" -- and it switched the cross-check off for
    // the whole operation, stickily, which meant the 26 operations that most needed checking
    // were the only ones exempt from it. Every one of those justifications turned out to be
    // wrong, in five distinct ways, and removing them found six real defects. Nothing sets it
    // now, so the field is gone rather than left as a hatch someone can take quietly.
    //
    // An operation that genuinely writes no path rows takes an explicit, categorised
    // SS_EXEMPT_AFFECTED_PATHS instead. That is reviewable; "incomplete" was not.

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

// The reverse check's own switch, separate from UndeclaredPathTouchIsFatal. Both are armed by
// TTestEnv, so both are live for every schemeshard suite.
//
// Read this before adding a MustWrite anywhere: as it stands, this check is nearly inert, and
// that is a finding rather than a gap to be filled in by stamping more claims.
//
// The arming pass ran the armed failures from 94 to zero. Not one of them was cleared by
// loosening a claim for convenience; each died to a specific counter-example:
//
//   a by-name alter writes its target   -> TAlterTable has no path-row Persist* at all
//   a subtree walk writes each child    -> a subdomain upgrade marks them migrated in memory
//                                          and persists the root alone (upgrade_subdomain.cpp:566)
//   a force drop rewrites its subtree   -> TSchemeShardTest::ForceDropTwice, where the second
//                                          drop succeeds having written nothing
//   a create bumps its container        -> a vector index's impl table leaves index1 untouched
//
// What survives is one MustWrite in the whole tree: the target of a create, below. Every
// container, subtree entry, alter and drop is MayWrite, and MayWrite is exempt by construction.
// So this check can no longer catch much, and the forward check (written subset-of declared)
// remains the load-bearing half.
//
// The reason it cannot work as specified is structural, not an oversight. A declaration is
// computed from the request; whether a write happens depends on state at execution. That
// information does not exist yet at declaration time, so no amount of care closes the gap --
// which is also why TAffectedPath deliberately carries no Outcome field.
//
// The design that would work is to record what was applied in memory as writes are observed
// and compare committed-against-declared at completion, in the DoCheckDeclarations hook this
// branch already built. Prefer that over adding MustWrite claims the next counter-example
// deletes.
inline bool UnfulfilledPathDeclarationIsFatal = false;

// Test-only sink for the finished plan of a top-level operation.
//
// The union across an operation's parts *is* the plan the requirement asks for -- every
// suboperation of one request shares a single TOperation, so admitParts accumulates them all
// into one DeclaredPathSet. But that set is erased with the operation
// (schemeshard__operation_side_effects.cpp, ss->Operations.erase), so nothing outside the
// tablet can assert on it, and "the plan is complete" stayed an argument rather than a test.
//
// When non-null, each completed operation's declared set is recorded here keyed by txId. A
// test then asserts what a top-level request planned -- including the paths contributed by
// parts it never named itself, which is the whole claim behind fanning-out operations like
// BackupBackupCollection.
//
// Null in production, and read only where the operation already survives the same gate as the
// checks above, so an exempt or knowingly partial declaration never lands here and cannot be
// mistaken for a complete plan.
inline THashMap<ui64, THashSet<TString>>* CompletedPlanSink = nullptr;

// The reverse half of the cross-check. ObservePathTouched tests written ⊆ declared, which
// by construction cannot see an over-declaration: a declaration naming a path nobody writes
// passes silently, so the six subtree walks recently added are verified only in the one
// direction the tooling can see. This tests the other direction -- every entry the
// declaration marked MustWrite got a write -- and is answerable only at completion, once
// every phase has had its chance to write.
//
// Takes the flat inputs rather than a TOperation so the semantics are testable without
// standing up a schemeshard, the way the declarations above already are. Returns the first
// unfulfilled path so the caller can name it; MayWrite and ReferenceOnly are skipped, and an
// exempt (nullopt) declaration demands nothing, because it never promised a list at all.
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

// Same, for a request that carries a full TPathId. Prefer this wherever one is available:
// the ui64 form can only describe a path this schemeshard owns, because it rebuilds the id
// with MakeLocalId, which hardcodes the owner to this tablet. Passing GetLocalId() from a
// wire TPathId silently discards its OwnerId, and a migrated path owned by another
// schemeshard then declares against an id that does not resolve -- which refuses the whole
// operation, since IgniteOperation treats an unresolved declaration as PreconditionFailed.
// Pass a default-constructed TPathId when the request has none.
TAffectedPaths DeclareTargetByIdOrName(TSchemeShard* ss, const TString& workingDir,
    const TString& name, const TPathId& pathId);

// Overloads taking the context rather than the schemeshard, so a declaration can be written
// as a single line in the traits table. TOperationContext is only forward-declared there;
// binding a reference to an incomplete type is fine, and the member access happens in the
// .cpp where it is complete.
struct TOperationContext;
TAffectedPaths DeclareTargetByIdOrName(const TOperationContext& context,
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
// `expect` defaults to MayWrite, and that default is a finding rather than caution. A force
// drop does rewrite every descendant -- the first time. Run it against a subtree that is
// already dropped (TSchemeShardTest::ForceDropTwice) and the operation succeeds having
// written nothing, which is exactly the successful no-op MayWrite exists to describe. The
// same is true of an owner change that sets the owner it already has, and of a subdomain
// upgrade, which only marks descendants migrated in memory (upgrade_subdomain.cpp:566-579).
//
// So MustWrite is far harder to assert statically than the three-way split suggests: a
// declaration cannot know from the request whether the operation will find work to do. Pass
// it only where the write is unconditional.
TAffectedPaths DeclareSubTree(TSchemeShard* ss, TPathId root, bool includeRoot,
    TAffectedPath::EEffect effect,
    TAffectedPath::EObservation expect = TAffectedPath::EObservation::MayWrite);

// The same, for a request that names its root the way an ordinary target is named. Keeps
// the id-over-name precedence in one place: it is the property that stops a declaration
// naming a different object than the operation mutates, so it must not be re-spelled per
// call site. Pass localPathId == 0 when absent.
TAffectedPaths DeclareSubTreeByIdOrName(TSchemeShard* ss, const TString& workingDir,
    const TString& name, ui64 localPathId, bool includeRoot, TAffectedPath::EEffect effect,
    TAffectedPath::EObservation expect = TAffectedPath::EObservation::MayWrite);

} // namespace NKikimr::NSchemeShard
