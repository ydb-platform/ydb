#include "schemeshard_database_relative_path.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NSchemeShard {

TConclusion<TDatabaseRelativePath> TDatabaseRelativePath::FromAbsolute(const TString& databaseRoot, const TString& absolutePath) {
    // TrySplitPathByDb rejects a path equal to the database, but the database root maps to "/".
    if (SplitPath(databaseRoot) == SplitPath(absolutePath)) {
        return TDatabaseRelativePath("/");
    }

    // Containment is checked component-wise, so "/Root/Db2" is not inside "/Root/Db".
    std::pair<TString, TString> split;
    TString error;
    if (!TrySplitPathByDb(absolutePath, databaseRoot, split, error)) {
        return TConclusionStatus::Fail(error);
    }

    // The remainder has no leading slash, e.g. "dir/table".
    return TDatabaseRelativePath("/" + split.second);
}

TConclusion<TDatabaseRelativePath> TDatabaseRelativePath::FromWorkingDirAndName(const TString& databaseRoot, const TString& workingDir, const TString& name) {
    return FromAbsolute(databaseRoot, CanonizePath(ChildPath(SplitPath(workingDir), SplitPath(name))));
}

}
