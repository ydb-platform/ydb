#pragma once

#include "ypath_service.h"

#include "attribute_filter.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    NYson::IYsonConsumer* consumer,
    bool stable,
    const TAttributeFilter& attributeFilter = {},
    bool skipEntityMapChildren = false);

void VisitTree(
    INodePtr root,
    NYson::IAsyncYsonConsumer* consumer,
    bool stable,
    const TAttributeFilter& attributeFilter = {},
    bool skipEntityMapChildren = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
