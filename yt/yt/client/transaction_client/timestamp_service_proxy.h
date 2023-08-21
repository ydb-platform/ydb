#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/transaction_client/proto/timestamp_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTimestampServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTimestampServiceProxy, TimestampService);

    DEFINE_RPC_PROXY_METHOD(NProto, GenerateTimestamps,
        .SetMultiplexingBand(NRpc::EMultiplexingBand::Control));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

