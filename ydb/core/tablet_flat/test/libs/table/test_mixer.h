#pragma once

#include <util/generic/utility.h>
#include <util/random/mersenne.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TRow;

    struct TMixerOne {
        ui32 operator()(const TRow&) noexcept
        {
            return 0;
        }
    };

    struct TMixerRnd {
        TMixerRnd(ui32 buckets) 
            : Buckets(buckets) 
        {
        }

        ui32 operator()(const TRow&) noexcept
        {
            return Random.Uniform(Buckets);
        }

    private:
        const ui32 Buckets = 1;
        TMersenne<ui64> Random;
    };

    struct TMixerSeq {
        TMixerSeq(ui32 buckets, ui64 rows)
            : Buckets(buckets)
            , RowsPerBucket(rows / buckets)
            , Skip(rows % buckets)
        {
        }

        ui32 operator()(const TRow&) noexcept
        {
            if (CurrentBucketRemainingRows-- == 0) { // start next bucket with CurrentBucketRemainingRows rows
                ui64 one = (Skip && Skip > Random.Uniform(Buckets) ? 1 : 0);

                CurrentBucketRemainingRows = RowsPerBucket + one - 1, Skip -= one, CurrentBucket++;
            }

            return Min(CurrentBucket, Buckets - 1);
        }

    private:
        const ui32 Buckets;
        ui64 RowsPerBucket;
        ui64 Skip;
        ui64 CurrentBucketRemainingRows = 0;
        ui32 CurrentBucket = Max<ui32>();
        TMersenne<ui64> Random;
    };
}
}
}
