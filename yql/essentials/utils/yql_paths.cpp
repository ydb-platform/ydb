#include "yql_paths.h"

#include <util/folder/pathsplit.h>

namespace NYql {

TString BuildTablePath(TStringBuf prefixPath, TStringBuf path,
                       const std::function<TString(TStringBuf)>& normalizePath) {
    TString normalizedPath;
    if (normalizePath) {
        normalizedPath = normalizePath(path);
        path = normalizedPath;
    }
    if (prefixPath.empty()) {
        return TString(path);
    }
    prefixPath.SkipPrefix("//");

    TPathSplitUnix prefixPathSplit(prefixPath);
    TPathSplitUnix pathSplit(path);

    if (pathSplit.IsAbsolute) {
        return TString(path);
    }

    return prefixPathSplit.AppendMany(pathSplit.begin(), pathSplit.end()).Reconstruct();
}

} // namespace NYql
