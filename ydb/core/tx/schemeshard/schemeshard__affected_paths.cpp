#include "schemeshard__dispatch_op.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NSchemeShard {

bool OperationDeclaresAffectedPaths(const TTxTransaction& tx) {
    return DispatchAffectedPaths(tx, [](auto traits) {
        return decltype(traits)::Declares;
    });
}

std::optional<TString> FindUnfulfilledMustWrite(
        const TVector<std::optional<TAffectedPaths>>& declared,
        const THashSet<TString>& observed)
{
    for (const auto& entry : declared) {
        // nullopt is an exempt operation type, not an empty declaration. Incomplete is a
        // declaration that said up front it could not enumerate what it touches -- the
        // observation set is not armed for it at all, so every entry would read as
        // unfulfilled. Both mean the same thing here: no claim to hold anyone to.
        if (!entry || entry->Incomplete) {
            continue;
        }
        for (const auto& affected : entry->Paths) {
            if (affected.Expect != TAffectedPath::EObservation::MustWrite) {
                continue;
            }
            if (!observed.contains(affected.Path)) {
                return affected.Path;
            }
        }
    }
    return std::nullopt;
}

TAffectedPaths DeclareChildOfWorkingDir(const TString& workingDir, const TString& name) {
    // Canonized, not merely joined. WorkingDir arrives from the wire and may carry a
    // trailing slash ("/MyRoot/table/indexByValue/"), which JoinPath would turn into a
    // double slash -- a path that matches nothing and would be recorded in the outbox
    // verbatim. CanonizePath collapses the separators the way TPath::Resolve does.
    const TString target = CanonizePath(JoinPath({workingDir, name}));

    TAffectedPaths result;
    result.Paths.push_back(TAffectedPath{
        .Role = TAffectedPath::ERole::Target,
        .Path = target,
        .Class = TAffectedPath::EEffectClass::SchemaEffect,
        .Effect = TAffectedPath::EEffect::Create,
        .Expect = TAffectedPath::EObservation::MustWrite,
    });
    // Not WorkingDir. A create may carry a relative path rather than a leaf -- MkDir
    // "DirB/DirC" under /MyRoot/DirA -- and then the directory that gains the child is
    // /MyRoot/DirA/DirB, which is what SplitIntoTransactions would rewrite WorkingDir to.
    // Declaring WorkingDir here would name the right container only for a bare leaf, and
    // that is exactly the case the auto-mkdir split does not have to touch.
    //
    // Empty for a target directly under the root ("/" + "MyRoot"), which has no container
    // path to name; pushing it anyway would put an empty string in the outbox record.
    if (const TStringBuf container = ExtractParent(target); !container.empty()) {
        result.Paths.push_back(TAffectedPath{
            .Role = TAffectedPath::ERole::Container,
            .Path = TString(container),
            .Class = TAffectedPath::EEffectClass::SchemaEffect,
            .Effect = TAffectedPath::EEffect::ChildrenChanged,
            // MayWrite, not MustWrite, and this one is counter-intuitive: gaining a child
            // usually bumps the parent's DirAlterVersion, but not always. Creating a vector
            // index's impl table under /MyRoot/Table/index1 leaves that index's row
            // untouched (TVectorIndexTests::ReplaceVectorIndex). E22 again, one level up --
            // even a create cannot promise what its container does.
            .Expect = TAffectedPath::EObservation::MayWrite,
        });
    }
    return result;
}

TAffectedPaths DeclareTargetByIdOrName(TSchemeShard* ss, const TString& workingDir,
        const TString& name, ui64 localPathId)
{
    if (localPathId == 0) {
        // Same paths, different claim. DeclareChildOfWorkingDir describes a create: it
        // stamps the target Create/MustWrite because a create really does write the new row
        // and really does bump its parent. This helper is for an operation acting on an
        // object that already exists, and those do not necessarily write a path row at all --
        // TAlterTable is the extreme case, its whole file contains no path-row Persist call,
        // so demanding a write of /MyRoot/Table is a claim it can never satisfy.
        //
        // Reuse the path arithmetic, restate the intent. MayWrite: the write is permitted,
        // not demanded, which is the only thing this helper is in a position to know.
        TAffectedPaths result = DeclareChildOfWorkingDir(workingDir, name);
        for (auto& affected : result.Paths) {
            affected.Effect = affected.Role == TAffectedPath::ERole::Container
                ? TAffectedPath::EEffect::ChildrenChanged
                : TAffectedPath::EEffect::Alter;
            affected.Expect = TAffectedPath::EObservation::MayWrite;
        }
        return result;
    }

    // WorkingDir is not a path at all on a by-id request, so the container has to come
    // from the resolved target rather than from the transaction.
    // Same resolver the suboperations use, so the declaration cannot name a different
    // object than the operation mutates.
    const TPath target = TPath::ResolveTarget(ss->MakeLocalId(localPathId), workingDir, name, ss);
    if (!target.IsResolved()) {
        TAffectedPaths unresolved;
        unresolved.Unresolved = true;
        return unresolved;
    }

    TAffectedPaths result;
    result.Paths.push_back(TAffectedPath{
        .Locator = TAffectedPath::ELocator::ByPathId,
        .Role = TAffectedPath::ERole::Target,
        .Path = target.PathString(),
        .PathId = target.Base()->PathId,
        .Class = TAffectedPath::EEffectClass::SchemaEffect,
        .Effect = TAffectedPath::EEffect::Alter,
        // MayWrite for the same reason as the by-name branch above: acting on an existing
        // object does not imply rewriting its path row, and several operations that resolve
        // by id write only their own typed table.
        .Expect = TAffectedPath::EObservation::MayWrite,
    });
    if (!target.IsEmpty() && target.Base()->ParentPathId) {
        const TPath parent = TPath::Init(target.Base()->ParentPathId, ss);
        if (parent.IsResolved()) {
            result.Paths.push_back(TAffectedPath{
                .Role = TAffectedPath::ERole::Container,
                .Path = parent.PathString(),
                .Class = TAffectedPath::EEffectClass::SchemaEffect,
                .Effect = TAffectedPath::EEffect::ChildrenChanged,
                // MayWrite, not MustWrite: a plain alter of an existing object does not
                // necessarily bump the parent, so a container no-op is legitimate here.
                .Expect = TAffectedPath::EObservation::MayWrite,
            });
        }
    }
    return result;
}

TAffectedPaths DeclareSubTree(TSchemeShard* ss, TPathId root, bool includeRoot,
        TAffectedPath::EEffect effect, TAffectedPath::EObservation expect)
{
    const TPath rootPath = TPath::Init(root, ss);
    if (!rootPath.IsResolved()) {
        TAffectedPaths unresolved;
        unresolved.Unresolved = true;
        return unresolved;
    }

    TAffectedPaths result;

    // The same walk the operation itself runs at propose -- ExamineTreeVFS over PathsById
    // (schemeshard_impl.cpp:6389), no DB reads -- so this cannot enumerate a different set
    // than the loop that writes the rows.
    for (const TPathId pathId : ss->ListSubTree(root, TlsActivationContext->AsActorContext())) {
        if (pathId == root && !includeRoot) {
            continue;
        }
        const TPath path = TPath::Init(pathId, ss);
        if (!path.IsResolved()) {
            TAffectedPaths unresolved;
            unresolved.Unresolved = true;
            return unresolved;
        }
        result.Paths.push_back(TAffectedPath{
            .Locator = TAffectedPath::ELocator::ByPathId,
            .Role = TAffectedPath::ERole::Target,
            .Path = path.PathString(),
            .PathId = pathId,
            // SchemaEffect, not BookkeepingInternal: a descendant of a force drop is really
            // gone and a descendant of an owner change really has a new owner. The write is
            // about the object, not about the shape of the tree.
            .Class = TAffectedPath::EEffectClass::SchemaEffect,
            .Effect = effect,
            // Usually MustWrite -- the operation's own loop rewrites every one of these
            // rows, which is the whole reason the subtree has to be declared rather than
            // left Incomplete. Not universal: see the note on the declaration.
            .Expect = expect,
        });
    }

    if (rootPath.Base()->ParentPathId) {
        const TPath parent = TPath::Init(rootPath.Base()->ParentPathId, ss);
        if (parent.IsResolved()) {
            result.Paths.push_back(TAffectedPath{
                .Role = TAffectedPath::ERole::Container,
                .Path = parent.PathString(),
                .Class = TAffectedPath::EEffectClass::SchemaEffect,
                .Effect = TAffectedPath::EEffect::ChildrenChanged,
                // Same expectation as the descendants, and for the same reason. Losing or
                // re-owning a child bumps the container's DirAlterVersion in the drops and
                // the owner change (drop_unsafe.cpp:236, drop_extsubdomain.cpp:356,
                // modify_acl.cpp:145) -- but a subdomain upgrade changes no child set, so
                // its container is not written either.
                .Expect = expect,
            });
        }
    }
    return result;
}

TAffectedPaths DeclareSubTreeByIdOrName(TSchemeShard* ss, const TString& workingDir,
        const TString& name, ui64 localPathId, bool includeRoot, TAffectedPath::EEffect effect,
        TAffectedPath::EObservation expect)
{
    // Same precedence as DeclareTargetByIdOrName -- a local path id wins over the name --
    // spelled once here rather than at each call site. Note the test is on the integer:
    // InvalidLocalPathId is Max<ui64>, not zero, so a TPathId built from 0 would read as
    // truthy and send the resolver down the by-id branch to a path that does not exist.
    const TPath target = localPathId
        ? TPath::ResolveTarget(ss->MakeLocalId(localPathId), workingDir, name, ss)
        : TPath::Resolve(CanonizePath(JoinPath({workingDir, name})), ss);

    if (!target.IsResolved()) {
        TAffectedPaths unresolved;
        unresolved.Unresolved = true;
        return unresolved;
    }
    return DeclareSubTree(ss, target.Base()->PathId, includeRoot, effect, expect);
}

TAffectedPaths DeclareCascadeTargetByIdOrName(TSchemeShard* ss, const TString& workingDir,
        const TString& name, ui64 localPathId)
{
    TAffectedPaths result = DeclareTargetByIdOrName(ss, workingDir, name, localPathId);
    // Not set on the unresolved result: that already carries a stronger verdict, and
    // marking it Incomplete as well would only muddy which of the two the caller sees.
    if (!result.Unresolved) {
        result.Incomplete = true;
    }
    return result;
}

TAffectedPaths DeclareTargetByIdOrName(const TOperationContext& context,
        const TString& workingDir, const TString& name, ui64 localPathId)
{
    return DeclareTargetByIdOrName(context.SS, workingDir, name, localPathId);
}

TAffectedPaths DeclareCascadeTargetByIdOrName(const TOperationContext& context,
        const TString& workingDir, const TString& name, ui64 localPathId)
{
    return DeclareCascadeTargetByIdOrName(context.SS, workingDir, name, localPathId);
}

namespace NOperation {

// Suboperation: index/operation_alter_index.cpp (TAlterTableIndex). No pathId field;
// resolves by Name only.
using TAffectedESchemeOpAlterTableIndex = TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpAlterTableIndex>;

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpAlterTableIndex>(
    TAffectedESchemeOpAlterTableIndex,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    const auto& tableIndexAlter = tx.GetAlterTableIndex();
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(), tableIndexAlter.GetName(), 0);
}

// Suboperation: olap/operations/alter_store.cpp (TAlterOlapStore). No pathId field;
// resolves by Name only.
using TAffectedESchemeOpAlterColumnStore = TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnStore>;

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpAlterColumnStore>(
    TAffectedESchemeOpAlterColumnStore,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    const auto& alter = tx.GetAlterColumnStore();
    return DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(), alter.GetName(), 0);
}

// Suboperation: olap/operations/alter_table.cpp (NOlap::NAlter::TAlterColumnTable). No
// pathId field; resolves by Name only, taking the name from AlterColumnTable if present,
// else falling back to the legacy AlterTable proto (used when a row-table alter request
// lands on what is now a column table).
using TAffectedESchemeOpAlterColumnTable = TAffectedPathsTraits<NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable>;

template <>
std::optional<TAffectedPaths> GetAffectedPaths<TAffectedESchemeOpAlterColumnTable>(
    TAffectedESchemeOpAlterColumnTable,
    const TTxTransaction& tx,
    const TOperationContext& context)
{
    const TString& name = tx.HasAlterColumnTable() ? tx.GetAlterColumnTable().GetName() : tx.GetAlterTable().GetName();
    TAffectedPaths result = DeclareTargetByIdOrName(context.SS, tx.GetWorkingDir(), name, 0);
    // An AlterSchema that upserts, drops or moves indexes turns this into
    // AlterColumnTableWithLocalIndexes, which appends a part per index.
    // No Incomplete. This expands into constructed parts, and IgniteOperation asks each
    // part for its own declaration before proposing it, so their paths are covered.
    // Verified rather than assumed: with this removed the schemeshard suites stay green
    // under the cross-check.
    return result;
}

} // namespace NOperation

} // namespace NKikimr::NSchemeShard
