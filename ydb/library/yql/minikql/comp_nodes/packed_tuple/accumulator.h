#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/generic/buffer.h>

#include "tuple.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

/*
* Class TAccumulator is used to split provided vector of tuples to buckets using provided key hashes.
*/
class TAccumulator {
public:
    using TBuffer = std::vector<ui8, TMKQLAllocator<ui8>>;

    struct TBucketRef {
        const TBuffer*      PackedTuples;
        const TBuffer*      Overflow;
        ui32                NTuples;    // Count of elements in a bucket
        const TTupleLayout* Layout;     // Layout for packed row (tuple)
    };

    struct TBucket {
        TBuffer             PackedTuples;
        TBuffer             Overflow;
        ui32                NTuples;    // Count of elements in a bucket
        const TTupleLayout* Layout;     // Layout for packed row (tuple)
    };

public:
    virtual ~TAccumulator() = default;

    // Create new accumulator for log2Buckets for given layout using given memory.
    // Accumulator will not allocate any memory
    static THolder<TAccumulator> Create(
        const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets,
        std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& packedTupleBuckets,
        std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& overflowBuckets);

    // Create new accumulator for log2Buckets for given layout.
    // Accumulator will manage memory by itself
    static THolder<TAccumulator> Create(const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets);

    // Add new nItems of data in TTupleLayout representation to accumulator 
    virtual void AddData(const ui8* data, const ui8* overflow, ui32 nItems) = 0;

    // Return bucket reference
    virtual TBucketRef GetBucket(ui32 bucket) const = 0;

    // Detach and return all buckets. Once this method has been called, the accumulator object can no longer be used
    virtual void Detach(std::vector<TBucket, TMKQLAllocator<TBucket>>& buckets) = 0;

    // Get bucket id
    inline static ui32 GetBucketId(ui32 hash, ui32 shift, ui32 mask) {
        return (hash >> shift) & mask;
    }
};

// Simple radix partitioning algorithm using software prefetching technique to improve performance.
// Suitable when number of buckets small (<= 32) or very big (> 1024)
class TAccumulatorImpl: public TAccumulator {
public:
    TAccumulatorImpl(
        const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets,
        std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& packedTupleBuckets,
        std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& overflowBuckets);

    TAccumulatorImpl(const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets);

    ~TAccumulatorImpl() = default;

    void AddData(const ui8* data, const ui8* overflow, ui32 nItems) override;

    TBucketRef GetBucket(ui32 bucket) const override;

    void Detach(std::vector<TBucket, TMKQLAllocator<TBucket>>& buckets) override;

private:
    ui32 NBuckets_{0};                                                   // Number of buckets
    ui32 Shift_{0};                                                      // Mask used for partitioning tuples 
    ui32 Mask_{0};                                                  // Bits count to write buckets number in binary
    const TTupleLayout* Layout_;                                         // Tuple layout
    std::vector<ui64, TMKQLAllocator<ui64>> PackedTupleBucketSizes_;     // Sizes for filled part of packed tuple buckets
    std::vector<ui64, TMKQLAllocator<ui64>> OverflowBucketSizes_;        // Sizes for filled part of overflow buckets
    bool NoAllocations_{false};                                          // Do not allocate any memory for buckets
    std::vector<TBuffer, TMKQLAllocator<TBuffer>> PackedTupleBuckets_;   // Buckets for packed tuples
    std::vector<TBuffer, TMKQLAllocator<TBuffer>> OverflowBuckets_;      // Buckets for overflow buffers
};

// Simple radix partitioning algorithm using software buffer technique to improve performance.
class TSMBAccumulatorImpl: public TAccumulator {
public:
    TSMBAccumulatorImpl(
        const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets,
        std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& packedTupleBuckets,
        std::vector<TBuffer, TMKQLAllocator<TBuffer>>&& overflowBuckets);

    TSMBAccumulatorImpl(const TTupleLayout* layout, ui32 bitShift, ui32 log2Buckets);

    ~TSMBAccumulatorImpl() = default;

    void AddData(const ui8* data, const ui8* overflow, ui32 nItems) override;

    TBucketRef GetBucket(ui32 bucket) const override;

    void Detach(std::vector<TBucket, TMKQLAllocator<TBucket>>& buckets) override;

private:
    static constexpr ui64 MB = 1024 * 1024;

private:
    using THugePageBuffer = std::vector<ui8, TMKQLHugeAllocator<ui8>>;

private:
    std::pair<ui64, ui64> GetCounters(ui32 bucket, ui64 maxBytesPerPartition);

private:
    ui32 NBuckets_{0};                                                   // Number of buckets
    ui32 Shift_{0};                                                      // Mask used for partitioning tuples 
    ui32 Mask_{0};                                                  // Bits count to write buckets number in binary
    const TTupleLayout* Layout_;                                         // Tuple layout
    std::vector<ui64, TMKQLAllocator<ui64>> PackedTupleBucketSizes_;     // Sizes for filled part of packed tuple buckets
    std::vector<ui64, TMKQLAllocator<ui64>> OverflowBucketSizes_;        // Sizes for filled part of overflow buckets
    bool NoAllocations_{false};                                          // Do not allocate any memory for buckets
    std::vector<TBuffer, TMKQLAllocator<TBuffer>> PackedTupleBuckets_;   // Buckets for packed tuples
    std::vector<TBuffer, TMKQLAllocator<TBuffer>> OverflowBuckets_;      // Buckets for overflow buffers
    THugePageBuffer PackedTupleSMB_;                                     // SMB for packed tuples
    THugePageBuffer OverflowSMB_;                                        // SMB for overflow
};


} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
