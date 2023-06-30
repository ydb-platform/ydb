#pragma once

#include "public.h"

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

int CompareCompositeValues(NYson::TYsonStringBuf lhs, NYson::TYsonStringBuf rhs);
TFingerprint CompositeFarmHash(NYson::TYsonStringBuf compositeValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
