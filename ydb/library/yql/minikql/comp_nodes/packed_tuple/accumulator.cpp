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
    const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets,
    std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& packedTupleBuckets,
    std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& overflowBuckets)
{
    // according to https://www.cidrdb.org/cidr2019/papers/p133-zhang-cidr19.pdf
    if (log2Buckets <= 6)
    {
        return MakeHolder<TAccumulatorImpl>(
            layout, bitShift, log2Buckets, std::move(packedTupleBuckets), std::move(overflowBuckets));
    }

    if (log2Buckets <= 13 && layout->TotalRowSize <= 128 && layout->VariableColumns.empty()) // tuple is too wide SMB will not help us, because the number of tuples that fit into the buffer will be small
    {
        return MakeHolder<TSMBAccumulatorImpl>(
           layout, bitShift, log2Buckets, std::move(packedTupleBuckets), std::move(overflowBuckets));
    }

    return MakeHolder<TAccumulatorImpl>(
        layout, bitShift, log2Buckets, std::move(packedTupleBuckets), std::move(overflowBuckets));
}

THolder<TAccumulator> TAccumulator::Create(const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets) {
    // according to https://www.cidrdb.org/cidr2019/papers/p133-zhang-cidr19.pdf
    if (log2Buckets <= 6)
    {
        return MakeHolder<TAccumulatorImpl>(layout, bitShift, log2Buckets);
    }

    if (log2Buckets <= 13 && layout->TotalRowSize <= 128 && layout->VariableColumns.empty()) // tuple is too wide SMB will not help us, because the number of tuples that fit into the buffer will be small
    {
        return MakeHolder<TSMBAccumulatorImpl>(layout, bitShift, log2Buckets);
    }

    return MakeHolder<TAccumulatorImpl>(layout, bitShift, log2Buckets);
}

// -----------------------------------------------------------------------
TAccumulatorImpl::TAccumulatorImpl(const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets)
    : NBuckets_(1 << log2Buckets)
    , Layout_(layout)
    , PackedTupleBucketSizes_(NBuckets_, 0)
    , OverflowBucketSizes_(NBuckets_, 0)
    , PackedTupleBuckets_(NBuckets_)
    , OverflowBuckets_(NBuckets_)
{
    Shift_ = sizeof(ui32) * 8 - (log2Buckets + bitShift);
    Mask_ = (1u << log2Buckets) - 1;
}

TAccumulatorImpl::TAccumulatorImpl(
    const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets,
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
    Shift_ = sizeof(ui32) * 8 - (log2Buckets + bitShift);
    Mask_ = (1u << log2Buckets) - 1;
}

void TAccumulatorImpl::AddData(const ui8* data, const ui8* overflow, ui32 nItems) {
    const auto layoutTotalRowSize = Layout_->TotalRowSize;

    if (!NoAllocations_) {
        Histogram hist;
        hist.AddData(Layout_, data, nItems, TAccumulator::GetBucketId, Shift_, Mask_);
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
        const ui32 bucketId = GetBucketId(ReadUnaligned<ui32>(tuple), Shift_, Mask_);
        NYql::PrefetchForRead(tuple + layoutTotalRowSize * 32);

        ui8* ptStoreAddr = PackedTupleBuckets_[bucketId].data() + PackedTupleBucketSizes_[bucketId];
        NYql::PrefetchForWrite(ptStoreAddr + layoutTotalRowSize);
        ui8* ovStoreAddr = OverflowBuckets_[bucketId].data() + OverflowBucketSizes_[bucketId];

        Layout_->TupleDeepCopy(tuple, overflow, ptStoreAddr, ovStoreAddr, OverflowBucketSizes_[bucketId]);
        PackedTupleBucketSizes_[bucketId] += layoutTotalRowSize;
    }
}

TAccumulatorImpl::TBucketRef TAccumulatorImpl::GetBucket(ui32 bucket) const {
    return {
        &PackedTupleBuckets_[bucket],
        &OverflowBuckets_[bucket],
        static_cast<ui32>(PackedTupleBucketSizes_[bucket] / Layout_->TotalRowSize),
        Layout_
    };
}

void TAccumulatorImpl::Detach(std::vector<TBucket, TMKQLAllocator<TBucket>>& buckets) {
    buckets.clear();
    for (size_t i = 0; i < PackedTupleBuckets_.size(); ++i) {
        ui32 NTuples = static_cast<ui32>(PackedTupleBucketSizes_[i] / Layout_->TotalRowSize);
        buckets.emplace_back(
            std::move(PackedTupleBuckets_[i]), std::move(OverflowBuckets_[i]), NTuples, Layout_);
        PackedTupleBuckets_[i].clear();
        PackedTupleBucketSizes_[i] = 0;
    }
}

// -----------------------------------------------------------------------
TSMBAccumulatorImpl::TSMBAccumulatorImpl(const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets)
    : NBuckets_(1 << log2Buckets)
    , Layout_(layout)
    , PackedTupleBucketSizes_(NBuckets_, 0)
    , OverflowBucketSizes_(NBuckets_, 0)
    , PackedTupleBuckets_(NBuckets_)
    , OverflowBuckets_(NBuckets_)
    , PackedTupleSMB_(2 * MB)
    , OverflowSMB_(2 * MB)
{
    Shift_ = sizeof(ui32) * 8 - (log2Buckets + bitShift);
    Mask_ = (1u << log2Buckets) - 1;

    std::memset(PackedTupleSMB_.data(), 0, 2 * MB);
    std::memset(OverflowSMB_.data(), 0, 2 * MB);
}

TSMBAccumulatorImpl::TSMBAccumulatorImpl(
    const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets,
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
    , PackedTupleSMB_(2 * MB + sizeof(ui64) * NBuckets_)
    , OverflowSMB_(2 * MB)
{
    Shift_ = sizeof(ui32) * 8 - (log2Buckets + bitShift);
    Mask_ = (1u << log2Buckets) - 1;

    std::memset(PackedTupleSMB_.data(), 0, 2 * MB + sizeof(ui64) * NBuckets_);
    std::memset(OverflowSMB_.data(), 0, 2 * MB);
}

void TSMBAccumulatorImpl::AddData(const ui8* data, const ui8* overflow, ui32 nItems) {
    const auto layoutTotalRowSize = Layout_->TotalRowSize;
    const ui64 maxBytesPerPartition = 2 * MB / NBuckets_; // in SMB
    const ui64 maxTuplesPerPartition = maxBytesPerPartition / layoutTotalRowSize;

    const ui8 hasVarSized = static_cast<ui8>(Layout_->VariableColumns.size() > 0);
    static const void* dispatch[] = {&&noVarSized, &&varSized}; // accelerate happy path without var sized columns

    if (!NoAllocations_) {
        Histogram hist;
        hist.AddData(Layout_, data, nItems, TAccumulator::GetBucketId, Shift_, Mask_);
        std::vector<std::pair<ui64, ui64>, TMKQLAllocator<std::pair<ui64, ui64>>> sizes(NBuckets_, {0, 0});
        hist.EstimateSizes(sizes);
        for (ui32 i = 0; i < sizes.size(); ++i) {
            auto [TLsize, Osize] = sizes[i];
            PackedTupleBuckets_[i].resize(PackedTupleBuckets_[i].size() + TLsize);
            OverflowBuckets_[i].resize(OverflowBuckets_[i].size() + Osize);
        }
    }

    ui8* ptSmbPartAddr = nullptr;
    ui8* ovSmbPartAddr = nullptr;
    ui8* ptStoreAddr = nullptr;
    ui8* ovStoreAddr = nullptr;
    for (ui32 i = 0; i < nItems; ++i) {
        const ui8* tuple = data + i * layoutTotalRowSize;
        const ui32 bucketId = GetBucketId(ReadUnaligned<ui32>(tuple), Shift_, Mask_);
        NYql::PrefetchForRead(tuple + layoutTotalRowSize * 32);
        auto [nPackedTuples, bytesOverflow] = GetCounters(bucketId, maxBytesPerPartition);

        ptSmbPartAddr = PackedTupleSMB_.data() + bucketId * (maxBytesPerPartition + sizeof(ui64)) + sizeof(ui64); // take bucket in SMB
        if (nPackedTuples == maxTuplesPerPartition) {
            ui8* storeAddr = PackedTupleBuckets_[bucketId].data() + PackedTupleBucketSizes_[bucketId];
            std::memcpy(storeAddr, ptSmbPartAddr, layoutTotalRowSize * nPackedTuples);
            PackedTupleBucketSizes_[bucketId] += layoutTotalRowSize * nPackedTuples;
            nPackedTuples = 0;
        }
        ptStoreAddr = ptSmbPartAddr + nPackedTuples * layoutTotalRowSize; // take last packed tuple

        goto *dispatch[hasVarSized]; // accelerate happy path without var sized columns

    // WARNING: do not use SMB with var sized column, because it works slow. Code here is just for demonstration
    varSized:
        ovSmbPartAddr = OverflowSMB_.data() + bucketId * maxBytesPerPartition; // take bucket in SMB
        if (bytesOverflow + Layout_->GetTupleVarSize(tuple) >= maxTuplesPerPartition) {
            ui8* storeAddr = OverflowBuckets_[bucketId].data() + OverflowBucketSizes_[bucketId];
            std::memcpy(storeAddr, ovSmbPartAddr, bytesOverflow);
            OverflowBucketSizes_[bucketId] += bytesOverflow;
            bytesOverflow = 0;
        }
        ovStoreAddr = ovSmbPartAddr + bytesOverflow; // offset

    noVarSized:
        Layout_->TupleDeepCopy(tuple, overflow, ptStoreAddr, ovStoreAddr, bytesOverflow);
        nPackedTuples++;
        WriteUnaligned<ui64>(ptSmbPartAddr - sizeof(ui64), (bytesOverflow << 32) | nPackedTuples);
        NYql::PrefetchForWrite(ptSmbPartAddr + nPackedTuples * layoutTotalRowSize);
    }

    // flush remaining data in SMB to buckets
    for (ui32 bucketId = 0; bucketId < NBuckets_; ++bucketId) {
        auto [nPackedTuples, bytesOverflow] = GetCounters(bucketId, maxBytesPerPartition);

        ptSmbPartAddr = PackedTupleSMB_.data() + bucketId * (maxBytesPerPartition + sizeof(ui64)) + sizeof(ui64); // take bucket in SMB
        ui8* storeAddr = PackedTupleBuckets_[bucketId].data() + PackedTupleBucketSizes_[bucketId];
        std::memcpy(storeAddr, ptSmbPartAddr, layoutTotalRowSize * nPackedTuples);
        PackedTupleBucketSizes_[bucketId] += layoutTotalRowSize * nPackedTuples;

        ovSmbPartAddr = OverflowSMB_.data() + bucketId * maxBytesPerPartition; // take bucket in SMB
        storeAddr = OverflowBuckets_[bucketId].data() + OverflowBucketSizes_[bucketId];
        std::memcpy(storeAddr, ovSmbPartAddr, bytesOverflow);
        OverflowBucketSizes_[bucketId] += bytesOverflow;

        WriteUnaligned<ui64>(ptSmbPartAddr - sizeof(ui64), 0);
    }
}

std::pair<ui64, ui64> TSMBAccumulatorImpl::GetCounters(ui32 bucket, ui64 maxBytesPerPartition) {
    auto counters = ReadUnaligned<ui64>(PackedTupleSMB_.data() + bucket * (maxBytesPerPartition + sizeof(ui64)));
    ui64 nPackedTuples = counters & 0xFFFFFFFF;
    ui64 bytesOverflow = (counters >> 32) & 0xFFFFFFFF;
    return {nPackedTuples, bytesOverflow};
}

TSMBAccumulatorImpl::TBucketRef TSMBAccumulatorImpl::GetBucket(ui32 bucket) const {
    return {
        &PackedTupleBuckets_[bucket],
        &OverflowBuckets_[bucket],
        static_cast<ui32>(PackedTupleBucketSizes_[bucket] / Layout_->TotalRowSize),
        Layout_
    };
}

void TSMBAccumulatorImpl::Detach(std::vector<TBucket, TMKQLAllocator<TBucket>>& buckets) {
    buckets.clear();
    for (size_t i = 0; i < PackedTupleBuckets_.size(); ++i) {
        ui32 NTuples = static_cast<ui32>(PackedTupleBucketSizes_[i] / Layout_->TotalRowSize);
        buckets.emplace_back(
            std::move(PackedTupleBuckets_[i]), std::move(OverflowBuckets_[i]), NTuples, Layout_);
        PackedTupleBuckets_[i].clear();
        PackedTupleBucketSizes_[i] = 0;
    }
}

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
