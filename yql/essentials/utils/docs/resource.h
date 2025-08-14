#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NYql::NDocs {

    struct TResourceFilter {
        TString BaseDirectorySuffix;
        TString CutSuffix;
    };

    using TResourcesByRelativePath = THashMap<TString, TString>;

    // Useful when YaTool ALL_RESOURCE_FILES macro is used.
    TResourcesByRelativePath FindResources(const TResourceFilter& filter);

} // namespace NYql::NDocs
