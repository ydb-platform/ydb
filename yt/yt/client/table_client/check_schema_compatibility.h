#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Validates that values from table with inputSchema also match outputSchema.
//
// Result pair contains following elements:
//   1. Level of compatibility of the given schemas.
//   2. If schemas are fully compatible error is empty otherwise it contains description
//      of incompatibility.
std::pair<ESchemaCompatibility, TError> CheckTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    bool ignoreSortOrder,
    bool forbidExtraComputedColumns = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
