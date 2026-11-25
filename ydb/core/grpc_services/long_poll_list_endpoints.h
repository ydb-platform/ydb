#pragma once

#include "rpc_calls.h"

namespace NKikimr::NGRpcService {

// Only opted-in unary calls carry a deadline across board wakeups. The original
// gRPC request stays alive through the base wrapper for the entire exchange.
struct TLongPollListEndpoints final : TEvListEndpointsRequest {
    explicit TLongPollListEndpoints(NYdbGrpc::IRequestContextBase* ctx,
            TInstant holdUntil = TInstant::Now() + TDuration::Seconds(30))
        : TEvListEndpointsRequest(ctx)
        , Transport(ctx)
        , HoldUntil(holdUntil)
    {}

    NYdbGrpc::IRequestContextBase* const Transport;
    const TInstant HoldUntil;
};

} // namespace NKikimr::NGRpcService
