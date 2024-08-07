#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TLogicalTypePtr MergeTypes(
    const NTableClient::TLogicalTypePtr& firstType,
    const NTableClient::TLogicalTypePtr& secondType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
