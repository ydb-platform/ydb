#pragma once

#include <yt/yt/client/table_client/key.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow MakeRow(const std::vector<TUnversionedValue>& values);
TOwningKeyBound MakeKeyBound(const std::vector<TUnversionedValue>& values, bool isInclusive, bool isUpper);
TComparator MakeComparator(int keyLength);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
