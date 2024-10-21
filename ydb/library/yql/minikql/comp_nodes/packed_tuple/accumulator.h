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
    static constexpr ui32 DEFAULT_BUCKETS_COUNT = 64;
public:
    struct BucketInfo {
        const ui8*              Data;       // Pointer to start of a bucket (maybe nullptr)
        ui32                    Elements;   // Count of elements in a bucket
        const TTupleLayout*     Layout;     // Layout for packed row (tuple)
    };

public:
    virtual ~TAccumulator() {};

    // Creates new accumulator for nBuckets for given layout
    static THolder<TAccumulator> Create(TTupleLayout* layout, ui32 nBuckets = DEFAULT_BUCKETS_COUNT);

    // Adds new nItems of data in TTupleLayout representation to accumulator 
    virtual void AddData(const ui8* data, ui32 nItems) = 0;

    // Returns bucket info
    virtual BucketInfo GetBucket(ui32 bucket) const = 0;
};


template <typename TTrait>
class TAccumulatorImpl: public TAccumulator {   
public:
    TAccumulatorImpl(TTupleLayout* layout, ui32 nBuckets = DEFAULT_BUCKETS_COUNT);
    ~TAccumulatorImpl();

    void AddData(const ui8* data, ui32 nItems) override;

    BucketInfo GetBucket(ui32 bucket) const override;

private:
    // Save data from first level buffer to the second level buffer.
    // Parameter remainingItems is used to predict how many memory should be allocated.
    // Returns count of L2 elements.
    ui32 Flush(ui32 bucketId, ui32 remainingItems);

private:
    ui32 NBuckets_{0};                                                   // Number of buckets
    TTupleLayout* Layout_;                                               // Tuple layout
    ui8* FirstLevelAccum_{nullptr};                                      // First level small accumulator. Should fit into L1 cache
    std::vector<ui8*, TMKQLAllocator<ui8*>> SecondLevelAccum_;           // Second level accumulator data
    ui32 FirstLevelMemLimit_{0};                                         // Memory limit for first level accumulator
    ui32 FirstLevelBucketSize_{0};                                       // Fixed bucket size of level 1 accumulator
    ui32 TuplesPerFirstLevelBucket_{0};                                  // Fixed bucket tuples number of level 1
    std::vector<ui32, TMKQLAllocator<ui32>> SecondLevelBucketSizes_;     // Fixed bucket sizes for level 2 accumulator
    std::vector<ui32, TMKQLAllocator<ui32>> TuplesPerSecondLevelBucket_; // Fixed bucket tuples number of level 2
    ui32 TotalTuples_{0};                                                // Total tuples number in all buckets and levels
    ui32 MinimalSecondLevelBucketSize_{0};                               // Fixed minimum initial size for second level bucket
    static constexpr double GrowthRate_{1.5};                            // Growth rate for second level buckets
    static constexpr double AddReserveMul_{1.2};                         // Add method reserve size multiplier for second level buckets
};


} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
