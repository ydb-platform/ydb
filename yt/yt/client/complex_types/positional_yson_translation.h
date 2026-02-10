#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////
//
// When complex type changes, its positional yson representation stored in
// table chunks can no longer be used as is.

using TPositionalYsonTranslator = std::function<
    NTableClient::TUnversionedValue(NTableClient::TUnversionedValue value)>;

TPositionalYsonTranslator CreatePositionalYsonTranslator(
    const NTableClient::TComplexTypeFieldDescriptor& sourceDescriptor,
    const NTableClient::TComplexTypeFieldDescriptor& targetDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
