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

TAffectedPaths DeclareChildOfWorkingDir(const TString& workingDir, const TString& name) {
    TAffectedPaths result;
    result.Paths.push_back(TAffectedPath{
        .Role = TAffectedPath::ERole::Target,
        .Path = JoinPath({workingDir, name}),
    });
    result.Paths.push_back(TAffectedPath{
        .Role = TAffectedPath::ERole::Container,
        .Path = workingDir,
    });
    return result;
}

TAffectedPaths DeclareTargetByIdOrName(TSchemeShard* ss, const TString& workingDir,
        const TString& name, ui64 localPathId)
{
    if (localPathId == 0) {
        return DeclareChildOfWorkingDir(workingDir, name);
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
    });
    if (!target.IsEmpty() && target.Base()->ParentPathId) {
        const TPath parent = TPath::Init(target.Base()->ParentPathId, ss);
        if (parent.IsResolved()) {
            result.Paths.push_back(TAffectedPath{
                .Role = TAffectedPath::ERole::Container,
                .Path = parent.PathString(),
            });
        }
    }
    return result;
}

} // namespace NKikimr::NSchemeShard
