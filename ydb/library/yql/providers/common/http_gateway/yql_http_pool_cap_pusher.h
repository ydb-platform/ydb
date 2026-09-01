#pragma once

#include "yql_http_gateway.h"

#include <ydb/library/actors/core/actor.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <functional>

namespace NYql {

using TPoolSharesProvider = std::function<THashMap<NDq::TWorkScope, double>()>;

// Actor that periodically pulls per-pool shares from `provider` and pushes
// derived per-pool caps (MaxHandlers * share) into the gateway via
// IHTTPGateway::UpdatePoolCaps. The `default` pool receives the leftover
// capacity, floored at MaxHandlers * minDefaultFraction.
NActors::IActor* CreateHttpPoolCapPusher(
    TPoolSharesProvider provider,
    IHTTPGateway::TWeakPtr gateway,
    TDuration period,
    size_t maxHandlers,
    double minDefaultFraction);

} // namespace NYql
