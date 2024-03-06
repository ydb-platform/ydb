#pragma once

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NYson {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPathFilteringMode,
    (Blacklist)
    (Whitelist)
    (WhitelistWithForcedEntities)
    (ForcedEntities)
);

////////////////////////////////////////////////////////////////////////////////

// Creates consumer that wraps underlying consumer providing
// filtering and missing value handling support.
// Several filtering modes are supported:
// - In `Blacklist` mode all nodes inside matched paths are omitted.
// - In `Whitelist` mode all nodes outside matched paths are omitted.
// - In `ForcedEntities` mode an entity node is inserted in case
// no value at given path is consumed.
// - The `WhitelistWithForcedEntities` values are consumed only at provided
// paths and entities are inserted in case of missing values.
// Attribute handling is available in all modes. For `ForcedEntities` modes
// `/@` path stands for inserting empty attribute dictionary, `/@ATTRIBUTE_NAME` for
// inserting the certain attribute.
// Asterisk matching is allowed only for `Blacklist` and `Whitelist` modes.
// Only the `EYsonType::Node` consuming is supported.
std::unique_ptr<NYson::IYsonConsumer> CreateYPathFilteringConsumer(
    NYson::IYsonConsumer* underlying,
    std::vector<NYPath::TYPath> paths,
    EPathFilteringMode mode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
