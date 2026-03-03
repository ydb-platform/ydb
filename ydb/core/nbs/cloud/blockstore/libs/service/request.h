#pragma once

#include "volume_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TRequestHeaders
{
    const TVolumeConfigPtr VolumeConfig;
    const ui64 RequestId;
    const TString ClientId;
    const TInstant Timestamp;
};

struct TReadBlocksLocalRequest
{
    TRequestHeaders Headers;
    TBlockRange64 Range;
    TGuardedSgList Sglist;

    TReadBlocksLocalRequest(TRequestHeaders headers, TBlockRange64 range)
        : Headers(std::move(headers))
        , Range(range)
    {}
};

struct TReadBlocksLocalResponse
{
    NProto::TError Error;
};

struct TWriteBlocksLocalRequest
{
    TRequestHeaders Headers;
    TBlockRange64 Range;
    TGuardedSgList Sglist;

    TWriteBlocksLocalRequest(TRequestHeaders headers, TBlockRange64 range)
        : Headers(std::move(headers))
        , Range(range)
    {}
};

struct TWriteBlocksLocalResponse
{
    NProto::TError Error;
};

struct TZeroBlocksLocalRequest
{
    TRequestHeaders Headers;
    TBlockRange64 Range;

    TZeroBlocksLocalRequest(TRequestHeaders headers, TBlockRange64 range)
        : Headers(std::move(headers))
        , Range(range)
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
