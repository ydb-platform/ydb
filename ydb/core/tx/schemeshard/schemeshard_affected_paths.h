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

} // namespace NKikimr::NSchemeShard
