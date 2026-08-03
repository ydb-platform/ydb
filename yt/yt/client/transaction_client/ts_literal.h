#pragma once

#include "public.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

// Mostly for tests/benchmarks: spell TTimestamp(123) as 123_ts.
constexpr TTimestamp operator""_ts(unsigned long long value)
{
    return TTimestamp(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
