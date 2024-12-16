#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Create schema that match both two schemas.
TTableSchemaPtr MergeTableSchemas(
    const TTableSchemaPtr& firstSchema,
    const TTableSchemaPtr& secondSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
