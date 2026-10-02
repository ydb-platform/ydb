#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

struct TTypeCompatibilityOptions
{
    bool AllowStructFieldRenaming = false;
    bool AllowStructFieldRemoval = false;

    // Do not deem types incompatible when new type contains unknown removed field names.
    // Without this option, type compatibility relation is not transitive, for example
    // in the following transformation chain
    //   1. [A, B], removed: []
    //   2. [A, B, C], removed: []
    //   3. [A, B], removed: [C]
    // types 1 and 2 are compatible, and so are types 2 and 3, but types 1 and 3 are not.
    bool IgnoreUnknownRemovedFieldNames = true;
};

////////////////////////////////////////////////////////////////////////////////

// Returns pair:
//   1. Inner element that is neither optional nor tagged.
//   2. How many times this element is wrapped into Optional type.
std::pair<NTableClient::TComplexTypeFieldDescriptor, int> UnwrapOptionalAndTagged(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor);

// Returned value is pair with elements
//   1. Compatibility of types.
//   2. If types are NOT FullyCompatible, error contains description of incompatibility.
std::pair<NTableClient::ESchemaCompatibility, TError> CheckTypeCompatibility(
    const NTableClient::TLogicalTypePtr& oldType,
    const NTableClient::TLogicalTypePtr& newType,
    const TTypeCompatibilityOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
