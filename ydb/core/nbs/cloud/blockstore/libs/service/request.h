#pragma once

#include "volume_config.h"

#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TRequestHeaders
{
    TRequestHeaders Clone(TBlockRange64 range) const;

    const TVolumeConfigPtr VolumeConfig;
    const TString ClientId;

    const ui64 RequestId;
    const TBlockRange64 Range;
    const TInstant Timestamp;
};

struct TReadBlocksLocalRequest
{
    TRequestHeaders Headers;
    TGuardedSgList Sglist;

    // Set during execution
    TBlockRange64 RegionRange;
    TBlockRange64 VChunkRange;

    explicit TReadBlocksLocalRequest(TRequestHeaders headers)
        : Headers(std::move(headers))
    {}
};

struct TReadBlocksLocalResponse
{
    NProto::TError Error;
};

struct TWriteBlocksLocalRequest
{
    TRequestHeaders Headers;
    TGuardedSgList Sglist;

    // Set during execution
    TBlockRange64 RegionRange;
    TBlockRange64 VChunkRange;
    ui64 Lsn = 0;
    NProto::TStorageServiceConfig::TWriteMode WriteMode{};
    ui32 PBufferReplyTimeoutMicroseconds{};

    explicit TWriteBlocksLocalRequest(TRequestHeaders headers)
        : Headers(std::move(headers))
    {}
};

struct TWriteBlocksLocalResponse
{
    NProto::TError Error;
};

struct TZeroBlocksLocalRequest
{
    TRequestHeaders Headers;

    explicit TZeroBlocksLocalRequest(TRequestHeaders headers)
        : Headers(std::move(headers))
    {}
};

struct TZeroBlocksLocalResponse
{
    NProto::TError Error;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
struct TRequestTraits
{
    static constexpr bool IsReadRequest()
    {
        return std::is_same_v<TRequest, TReadBlocksLocalRequest>;
    }

    static constexpr bool IsWriteRequest()
    {
        return std::is_same_v<TRequest, TWriteBlocksLocalRequest>;
    }

    static constexpr bool IsReadWriteRequest()
    {
        return IsReadRequest() || IsWriteRequest();
    }
};

////////////////////////////////////////////////////////////////////////////////

enum class EBlockStoreRequest
{
    ReadBlocks = 1,
    WriteBlocks = 2,
    ZeroBlocks = 3,
    MAX
};

ui64 CreateRequestId();

}   // namespace NYdb::NBS::NBlockStore
