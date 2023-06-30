#include "cypress.h"

#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ICypressClient::Concatenate(
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options)
{
    TVector<TRichYPath> richSourcePaths;
    richSourcePaths.reserve(sourcePaths.size());
    for (const auto& path : sourcePaths) {
        richSourcePaths.emplace_back(path);
    }
    Concatenate(richSourcePaths, destinationPath, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
