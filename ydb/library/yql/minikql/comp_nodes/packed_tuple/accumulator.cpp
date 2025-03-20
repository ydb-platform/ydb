#include "accumulator.h"
#include "histogram.h"

#include <cstdlib>
#include <ydb/library/yql/utils/simd/simd.h>
#include <arrow/util/bit_util.h>
#include <yql/essentials/utils/prefetch.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

// -----------------------------------------------------------------------
THolder<TAccumulator> TAccumulator::Create(
    const TTupleLayout* layout, ui32 log2Buckets,
    std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& packedTupleBuckets,
    std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& overflowBuckets) {
    // according to https://www.cidrdb.org/cidr2019/papers/p133-zhang-cidr19.pdf
    if (log2Buckets <= 5) // 5, because we have two vectors (packet tuples and overflow) for each bucket and vector of sizes for them and two input vectors <= 64
    {
        return MakeHolder<TAccumulatorImpl>(
            layout, log2Buckets, std::move(packedTupleBuckets), std::move(overflowBuckets));
    }

    #if 0
    if (log2Buckets <= 10 && layout->TotalRowSize <= 256) // tuple is too wide SMB will not help us, because the number of tuples that fit into the buffer will be small
    {
        return MakeHolder<TSMBAccumulatorImpl>(
           layout, log2Buckets, std::move(packedTupleBuckets), std::move(overflowBuckets));
    }
    #endif

    return MakeHolder<TAccumulatorImpl>(
        layout, log2Buckets, std::move(packedTupleBuckets), std::move(overflowBuckets));
}

// -----------------------------------------------------------------------
THolder<TAccumulator> TAccumulator::Create(const TTupleLayout* layout, ui32 log2Buckets) {
    // according to https://www.cidrdb.org/cidr2019/papers/p133-zhang-cidr19.pdf
    if (log2Buckets <= 5) // 5, because we have two vectors (packet tuples and overflow) for each bucket and vector of sizes for them and two input vectors <= 64
    {
        return MakeHolder<TAccumulatorImpl>(layout, log2Buckets);
    }

    #if 0
    if (log2Buckets <= 10 && layout->TotalRowSize <= 256) // tuple is too wide SMB will not help us, because the number of tuples that fit into the buffer will be small
    {
        return MakeHolder<TSMBAccumulatorImpl>(layout, log2Buckets);
    }
    #endif

    return MakeHolder<TAccumulatorImpl>(layout, log2Buckets);
}

// -----------------------------------------------------------------------
TAccumulatorImpl::TAccumulatorImpl(const TTupleLayout* layout, ui32 log2Buckets)
    : NBuckets_(1 << log2Buckets)
    , Layout_(layout)
    , PackedTupleBucketSizes_(NBuckets_, 0)
    , OverflowBucketSizes_(NBuckets_, 0)
    , PackedTupleBuckets_(NBuckets_)
    , OverflowBuckets_(NBuckets_)
{
    // Take leftmost bits for mask
    BitsCount_ = arrow::BitUtil::NumRequiredBits(NBuckets_ - 1);
    PMask_ = ((1 << BitsCount_) - 1) << (32 - BitsCount_);
}

// -----------------------------------------------------------------------
TAccumulatorImpl::TAccumulatorImpl(
    const TTupleLayout* layout, ui32 log2Buckets,
    std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& packedTupleBuckets,
    std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& overflowBuckets
)
    : NBuckets_(1 << log2Buckets)
    , Layout_(layout)
    , PackedTupleBucketSizes_(NBuckets_, 0)
    , OverflowBucketSizes_(NBuckets_, 0)
    , NoAllocations_(true)
    , PackedTupleBuckets_(std::move(packedTupleBuckets))
    , OverflowBuckets_(std::move(overflowBuckets))
{
    Y_ASSERT(NBuckets_ > 0);
    // Take leftmost bits for mask
    BitsCount_ = arrow::BitUtil::NumRequiredBits(NBuckets_ - 1);
    PMask_ = ((1 << BitsCount_) - 1) << (32 - BitsCount_);
}

// -----------------------------------------------------------------------
void TAccumulatorImpl::AddData(const ui8* data, const ui8* overflow, ui32 nItems) {
    const auto layoutTotalRowSize = Layout_->TotalRowSize;

    if (!NoAllocations_) {
        Histogram hist;
        hist.AddData(Layout_, data, nItems, TAccumulator::GetBucketId, PMask_, BitsCount_);
        std::vector<std::pair<ui64, ui64>, TMKQLAllocator<std::pair<ui64, ui64>>> sizes(NBuckets_, {0, 0});
        hist.EstimateSizes(sizes);
        for (ui32 i = 0; i < sizes.size(); ++i) {
            auto [TLsize, Osize] = sizes[i];
            PackedTupleBuckets_[i].resize(PackedTupleBuckets_[i].size() + TLsize);
            OverflowBuckets_[i].resize(OverflowBuckets_[i].size() + Osize);
        }
    }

    for (ui32 i = 0; i < nItems; ++i) {
        const ui8* tuple = data + i * layoutTotalRowSize;
        const ui32 bucketId = GetBucketId(ReadUnaligned<ui32>(tuple), PMask_, BitsCount_);
        NYql::PrefetchForRead(tuple + layoutTotalRowSize * 32);

        ui8* ptStoreAddr = PackedTupleBuckets_[bucketId].data() + PackedTupleBucketSizes_[bucketId];
        NYql::PrefetchForWrite(ptStoreAddr + layoutTotalRowSize);
        ui8* ovStoreAddr = OverflowBuckets_[bucketId].data() + OverflowBucketSizes_[bucketId];

        Layout_->TupleDeepCopy(tuple, overflow, ptStoreAddr, ovStoreAddr, OverflowBucketSizes_[bucketId]);
        PackedTupleBucketSizes_[bucketId] += layoutTotalRowSize;
    }
}

// -----------------------------------------------------------------------
TAccumulatorImpl::TBucketRef TAccumulatorImpl::GetBucket(ui32 bucket) const {
    return {
        &PackedTupleBuckets_[bucket],
        &OverflowBuckets_[bucket],
        static_cast<ui32>(PackedTupleBucketSizes_[bucket] / Layout_->TotalRowSize),
        Layout_
    };
}

// -----------------------------------------------------------------------
void TAccumulatorImpl::Detach(std::vector<TBucket, TMKQLAllocator<TBucket>>& buckets) {
    buckets.clear();
    for (size_t i = 0; i < PackedTupleBuckets_.size(); ++i) {
        ui32 NTuples = static_cast<ui32>(PackedTupleBucketSizes_[i] / Layout_->TotalRowSize);
        buckets.emplace_back(
            std::move(PackedTupleBuckets_[i]), std::move(OverflowBuckets_[i]), NTuples, Layout_);
    }
}


} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
