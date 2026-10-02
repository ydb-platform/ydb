#pragma once

#include "markdown.h"
#include "resource.h"

namespace NYql::NDocs {

using TPages = THashMap<TString, TMarkdownPage>;

TPages ParsePages(TResourcesByRelativePath resources);

TPages Resolved(TPages pages, TStringBuf baseURL);

TPages ExtendedSyntaxRemoved(TPages pages);

TPages CodeListingsTagRemoved(TPages pages);

} // namespace NYql::NDocs
