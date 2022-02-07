#include "util.h"

#include <util/folder/pathsplit.h>

namespace NYq {

using namespace NYdb;

////////////////////////////////////////////////////////////////////////////////

TString JoinPath(const TString& basePath, const TString& path) {
    if (basePath.empty()) {
        return path;
    }

    TPathSplitUnix prefixPathSplit(basePath);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}

} // namespace NYq
