#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/fwd.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EPersistResult
{
    Success,
    Cancelled,
};
using TPersistResultFuture = NThreading::TFuture<EPersistResult>;
using TPersistResultPromise = NThreading::TPromise<EPersistResult>;

using TRegionVChunks = TBitMap<VChunkPerRegionCount>;

class ITouchedProvider
{
public:
    virtual ~ITouchedProvider() = default;

    [[nodiscard]] virtual bool Get(ui32 vChunkIndex) const = 0;

    // Returns touched VChunks in the specified region.
    [[nodiscard]] virtual TRegionVChunks GetTouchedVChunks(
        ui32 regionIndex) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class IOracle;
using IOraclePtr = IOracle*;

class TOracleConfig;
using TOracleConfigPtr = std::shared_ptr<TOracleConfig>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
