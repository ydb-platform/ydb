#pragma once

#include <contrib/libs/grpc/include/grpcpp/resource_quota.h>
#include <contrib/libs/grpc/include/grpcpp/support/channel_arguments.h>

namespace NYdb::NBS::NGrpc {

////////////////////////////////////////////////////////////////////////////////

template <typename TConfig>
grpc::ChannelArguments CreateChannelArguments(const TConfig& config)
{
    grpc::ChannelArguments args;

    ui32 maxMessageSize = config.GetMaxMessageSize();
    if (maxMessageSize) {
        args.SetMaxSendMessageSize(maxMessageSize);
        args.SetMaxReceiveMessageSize(maxMessageSize);
    }

    ui32 memoryQuotaBytes = config.GetMemoryQuotaBytes();
    if (memoryQuotaBytes) {
        grpc::ResourceQuota quota("memory_bound");
        quota.Resize(memoryQuotaBytes);

        args.SetResourceQuota(quota);
    }

    if (auto backoff = config.GetGrpcReconnectBackoff()) {
        args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, backoff.MilliSeconds());
        args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, backoff.MilliSeconds());
        args.SetInt(
            GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS,
            backoff.MilliSeconds());
    }

    return args;
}

}   // namespace NYdb::NBS::NGrpc
