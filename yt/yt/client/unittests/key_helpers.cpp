#include "key_helpers.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow MakeRow(const std::vector<TUnversionedValue>& values)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& value : values) {
        builder.AddValue(value);
    }
    return builder.FinishRow();
}

TOwningKeyBound MakeKeyBound(const std::vector<TUnversionedValue>& values, bool isInclusive, bool isUpper)
{
    return TOwningKeyBound::FromRow(MakeRow(values), isInclusive, isUpper);
}

TComparator MakeComparator(int keyLength)
{
    return TComparator(std::vector<ESortOrder>(keyLength, ESortOrder::Ascending));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
