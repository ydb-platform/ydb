#pragma once

#include "markdown.h"
#include "resource.h"

namespace NYql::NDocs {

    using TPages = THashMap<TString, TMarkdownPage>;

    TPages ParsePages(TResourcesByRelativePath resources);

} // namespace NYql::NDocs
