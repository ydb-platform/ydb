#pragma once

#include "schema.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Exposed for unittests.
struct TConstrainedColumnSchema
    : public TColumnSchema
{
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::string>, Constraint);
};

void Deserialize(TConstrainedColumnSchema& schema, NYson::TYsonPullParserCursor* cursor);
void Deserialize(TConstrainedColumnSchema& schema, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
