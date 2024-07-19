#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableSchemaCompatibilityOptions
{
    bool IgnoreSortOrder = false;
    bool ForbidExtraComputedColumns = true;
    bool IgnoreStableNamesDifference = false;
};

// Validates that values from table with inputSchema also match outputSchema.
//
// Result pair contains following elements:
//   1. Level of compatibility of the given schemas.
//   2. If schemas are fully compatible error is empty otherwise it contains description
//      of incompatibility.
std::pair<ESchemaCompatibility, TError> CheckTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    TTableSchemaCompatibilityOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
