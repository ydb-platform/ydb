#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxExpressionDepth = 50;
constexpr int MaxWhenThenExpressionCount = 20'000;
constexpr int MaxSubqueryDepth = 50;
constexpr int MaxUnionAllSubqueryCount = 32;
constexpr int MaxConciseStringLength = 16;
constexpr int ElidedStringLength = 8;
constexpr int MaxConciseListLength = 3;
constexpr int ElidedListLength = 2;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, QueryClientLogger, "QueryClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

