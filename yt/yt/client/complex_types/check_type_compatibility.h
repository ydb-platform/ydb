#pragma once

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NComplexTypes {

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
    const NTableClient::TLogicalTypePtr& newType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
