#pragma once

#include "schema.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Exposed for unittests.
struct TMaybeDeletedColumnSchema
    : public TColumnSchema
{
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::string>, Constraint);
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, Deleted);

    TDeletedColumn GetDeletedColumnSchema() const;
};

void Deserialize(TMaybeDeletedColumnSchema& schema, NYson::TYsonPullParserCursor* cursor);
void Deserialize(TMaybeDeletedColumnSchema& schema, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
