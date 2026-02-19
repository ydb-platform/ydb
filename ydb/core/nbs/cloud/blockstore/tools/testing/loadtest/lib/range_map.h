#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>

#include <util/generic/set.h>
#include <util/generic/maybe.h>
#include <util/random/random.h>

namespace NYdb::NBS::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TRangeMap
{
    struct Greater
    {
        bool operator () (
            const TBlockRange64& a,
            const TBlockRange64& b) const
        {
            return a.Start > b.Start;
        }
    };

    TSet<TBlockRange64, Greater> Blocks;

public:
    TRangeMap() = default;
    TRangeMap(const TBlockRange64& range);
    TRangeMap(const TVector<TBlockRange64>& ranges);

    TMaybe<TBlockRange64> GetBlock(const TBlockRange64& range, bool strict = false);

    void PutBlock(const TBlockRange64& range);

    size_t Size() const;
    bool Empty() const;

    TString DumpRanges() const;
};

}   // namespace NYdb::NBS::NBlockStore::NLoadTest
