#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

// NB: Merging erases information about struct field stable names, as well as
// removed struct fields.
NTableClient::TLogicalTypePtr MergeTypes(
    const NTableClient::TLogicalTypePtr& firstType,
    const NTableClient::TLogicalTypePtr& secondType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
