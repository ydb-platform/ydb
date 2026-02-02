#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/protos/nbs2_load.pb.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TRangeAllocator
{
    NProto::ELoadType LoadType;
    TBlockRange64 Range;

    struct TSubRange
    {
        double Cdf;
        TBlockRange64 Range;
    };
    TVector<TSubRange> SubRanges;
    struct TRequestSize
    {
        double Cdf;
        ui32 MinSize;
        ui32 MaxSize;
    };
    TVector<TRequestSize> RequestSizes;

    ui64 CurrentBlock;

public:
    TRangeAllocator(const NProto::TRangeTest& rangeTest);

    TBlockRange64 AllocateRange();

private:
    void SetupSubRanges(const NProto::TRangeTest& rangeTest);
    void SetupRequestSizes(const NProto::TRangeTest& rangeTest);
};

}   // namespace NCloud::NBlockStore::NLoadTest
