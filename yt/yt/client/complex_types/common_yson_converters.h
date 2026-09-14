#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////
//
// Yson converters for types, yson representation of which is identical for
// positional and named modes.
//

using TYsonCursorConverter = std::function<
    void(NYson::TYsonPullParserCursor*, NYson::IYsonConsumer*)>;

using TUnversionedValueConverter = std::function<
    NTableClient::TUnversionedValue(NTableClient::TUnversionedValue)>;

////////////////////////////////////////////////////////////////////////////////

void IdentityYsonCursorConverter(
    NYson::TYsonPullParserCursor* cursor,
    NYson::IYsonConsumer* consumer);

// If element converter converts logical type T -> U, resulting coverter
// converts Optional<T> -> Optional<U>.
TYsonCursorConverter CreateOptionalYsonCursorConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter);

// If element converter converts logical type T -> U, resulting coverter
// converts Optinal<T> -> U.
TYsonCursorConverter CreateUnwrappingOptionalYsonCursorConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter);

// If element converter converts logical type T -> U, resulting coverter
// converts T -> Optional<U>.
TYsonCursorConverter CreateWrappingOptionalYsonCursorConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter);

TYsonCursorConverter CreateListYsonCursorConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    TYsonCursorConverter elementConverter);

TYsonCursorConverter CreateTupleYsonCursorConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    std::vector<TYsonCursorConverter> elementConverters);

TYsonCursorConverter CreateVariantTupleYsonCursorConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    std::vector<TYsonCursorConverter> elementConverters);

////////////////////////////////////////////////////////////////////////////////

TUnversionedValueConverter CreateUnversionedValueConverter(
    TYsonCursorConverter ysonCursorConverter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
