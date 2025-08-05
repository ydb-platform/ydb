#pragma once

#include "link.h"
#include "page.h"

namespace NYql::NDocs {

    TLinks GetLinksFromPages(const TPages& pages);

    TPages Stripped(TPages&& pages, const TLinks& links);

} // namespace NYql::NDocs
