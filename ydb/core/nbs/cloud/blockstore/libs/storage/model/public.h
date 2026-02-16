#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/public.h>

#include <util/generic/utility.h>
#include <util/generic/ylimits.h>
#include <util/system/defaults.h>

namespace NYdb::NBS::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxBlocksCount = 1024;

constexpr ui64 MaxPartitionBlocksCount = Max<ui32>() - 1;
constexpr ui64 MaxPartitionBlocksCountForMultipartitionVolume = 1u << 31;
constexpr ui64 MaxVolumeBlocksCount = 256_TB / NYdb::NBS::DefaultBlockSize;
// 1 system + 1 log + 1 index + 252 data channel count
constexpr ui32 MaxChannelCount = 255;
constexpr ui32 MaxMergedChannelCount = 248;
// max merged + mixed channel count + fresh channel count
constexpr ui32 MaxDataChannelCount = 252;

constexpr ui32 InvalidCollectPerGenerationCounter = 0xFFFFFFFFul;
constexpr ui32 InvalidBlockIndex = 0xFFFFFFFFul;
constexpr ui16 InvalidBlobOffset = 0xFFFFu;
// used for marking zero blocks
constexpr ui16 ZeroBlobOffset = InvalidBlobOffset - 1;

constexpr ui32 MaxSupportedTabletVersion = 2;

////////////////////////////////////////////////////////////////////////////////

inline ui32 CalculateMaxBlocksInBlob(ui32 maxBlobSize, ui32 blockSize)
{
    return Min(MaxBlocksCount, maxBlobSize / blockSize);
}

////////////////////////////////////////////////////////////////////////////////

enum class EOptimizationMode
{
    OptimizeForLongRanges,
    OptimizeForShortRanges,
};

}   // namespace NYdb::NBS::NBlockStore::NStorage
