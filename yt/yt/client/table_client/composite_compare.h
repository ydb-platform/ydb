#pragma once

#include "public.h"

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

int CompareDoubleValues(double lhs, double rhs);
int CompareYsonValues(NYson::TYsonStringBuf lhs, NYson::TYsonStringBuf rhs);
TFingerprint CompositeFarmHash(NYson::TYsonStringBuf compositeValue);

//! Returns a new yson value containing a truncated expression which can be compared with the original value
//! by using the comparison function above.
//! Returns null if the result is empty, since empty values are not suported in YSON.
//! Effectively, it returns a prefix of the original value up to the first uncomparable item, such as a map or yson attributes,
//! or up until the point when the size limit is hit.
//! The size parameter limits the binary size of the output, i.e. the length of the returned string.
//!
//! NB: The current implementation guarantees that the size of the returned string is not larger then the provided limit. However,
//! this might be hard to maintain and is not something one should rely on. It is better to think of this function as an approximate one.
std::optional<NYson::TYsonString> TruncateYsonValue(NYson::TYsonStringBuf value, i64 size);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
