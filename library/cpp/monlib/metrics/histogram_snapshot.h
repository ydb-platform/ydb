#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <cmath>
#include <limits>


namespace NMonitoring {

    using TBucketBound = double;
    using TBucketValue = ui64;

    using TBucketBounds = TVector<TBucketBound>;
    using TBucketValues = TVector<TBucketValue>;

    constexpr ui32 HISTOGRAM_MAX_BUCKETS_COUNT = 51;
    constexpr TBucketBound HISTOGRAM_INF_BOUND = std::numeric_limits<TBucketBound>::max();

    ///////////////////////////////////////////////////////////////////////////
    // IHistogramSnapshot
    ///////////////////////////////////////////////////////////////////////////
    class IHistogramSnapshot: public TAtomicRefCount<IHistogramSnapshot> {
    public:
        virtual ~IHistogramSnapshot() = default;

        /**
         * @return buckets count.
         */
        virtual ui32 Count() const = 0;

        /**
         * @return upper bound for the bucket with particular index.
         */
        virtual TBucketBound UpperBound(ui32 index) const = 0;

        /**
         * @return value stored in the bucket with particular index.
         */
        virtual TBucketValue Value(ui32 index) const = 0;
    };

    using IHistogramSnapshotPtr = TIntrusivePtr<IHistogramSnapshot>;

    ///////////////////////////////////////////////////////////////////////////////
    // TLinearHistogramSnapshot
    ///////////////////////////////////////////////////////////////////////////////
    class TLinearHistogramSnapshot: public IHistogramSnapshot {
    public:
        TLinearHistogramSnapshot(
                TBucketBound startValue, TBucketBound bucketWidth, TBucketValues values)
            : StartValue_(startValue)
            , BucketWidth_(bucketWidth)
            , Values_(std::move(values))
        {
        }

        ui32 Count() const override {
            return static_cast<ui32>(Values_.size());
        }

        TBucketBound UpperBound(ui32 index) const override {
            Y_ASSERT(index < Values_.size());
            if (index == Count() - 1) {
                return Max<TBucketBound>();
            }
            return StartValue_ + BucketWidth_ * index;
        }

        TBucketValue Value(ui32 index) const override {
            Y_ASSERT(index < Values_.size());
            return Values_[index];
        }

        ui64 MemorySizeBytes() {
            return sizeof(*this) + Values_.capacity() * sizeof(decltype(Values_)::value_type);
        }

    private:
        TBucketBound StartValue_;
        TBucketBound BucketWidth_;
        TBucketValues Values_;
    };

    ///////////////////////////////////////////////////////////////////////////
    // TExponentialHistogramSnapshot
    ///////////////////////////////////////////////////////////////////////////
    class TExponentialHistogramSnapshot: public IHistogramSnapshot {
    public:
        TExponentialHistogramSnapshot(
                double base, double scale, TBucketValues values)
            : Base_(base)
            , Scale_(scale)
            , Values_(std::move(values))
        {
        }

        ui32 Count() const override {
            return static_cast<ui32>(Values_.size());
        }

        TBucketBound UpperBound(ui32 index) const override {
            Y_ASSERT(index < Values_.size());
            if (index == Values_.size() - 1) {
                return Max<TBucketBound>();
            }
            return std::round(Scale_ * std::pow(Base_, index));
        }

        TBucketValue Value(ui32 index) const override {
            Y_ASSERT(index < Values_.size());
            return Values_[index];
        }

        ui64 MemorySizeBytes() {
            return sizeof(*this) + Values_.capacity() * sizeof(decltype(Values_)::value_type);
        }

    private:
        double Base_;
        double Scale_;
        TBucketValues Values_;
    };

    using TBucket = std::pair<TBucketBound, TBucketValue>;

    ///////////////////////////////////////////////////////////////////////
    // TExplicitHistogramSnapshot
    ///////////////////////////////////////////////////////////////////////
    //
    // Memory layout (single contiguous block):
    //
    //  +------+-----------+--------------+--------+--------+-       -+--------+--------+
    //  | vptr | RefsCount | BucketsCount | Bound1 | Value1 |   ...   | BoundN | ValueN |
    //  +------+-----------+--------------+--------+--------+-       -+--------+--------+
    //
    class alignas(TBucketValue) TExplicitHistogramSnapshot: public IHistogramSnapshot, private TNonCopyable {
    public:
        static TIntrusivePtr<TExplicitHistogramSnapshot> New(ui32 bucketsCount) {
            size_t bucketsSize = bucketsCount * sizeof(TBucket);
            Y_ENSURE(bucketsCount <= HISTOGRAM_MAX_BUCKETS_COUNT, "Cannot allocate a histogram with " << bucketsCount
                << " buckets. Bucket count is limited to " << HISTOGRAM_MAX_BUCKETS_COUNT);

            return new(bucketsSize) TExplicitHistogramSnapshot(bucketsCount);
        }

        TBucket& operator[](ui32 index) noexcept {
            return Bucket(index);
        }

        ui32 Count() const override {
            return BucketsCount_;
        }

        TBucketBound UpperBound(ui32 index) const override {
            return Bucket(index).first;
        }

        TBucketValue Value(ui32 index) const override {
            return Bucket(index).second;
        }

        ui64 MemorySizeBytes() const {
            return sizeof(*this) + BucketsCount_ * sizeof(TBucket);
        }

    private:
        explicit TExplicitHistogramSnapshot(ui32 bucketsCount) noexcept
            : BucketsCount_(bucketsCount)
        {
        }

        static void* operator new(size_t size, size_t bucketsSize) {
            return ::operator new(size + bucketsSize);
        }

        static void operator delete(void* mem) {
            ::operator delete(mem);
        }

        static void operator delete(void* mem, size_t, size_t) {
            // this operator can be called as paired for custom new operator
            ::operator delete(mem);
        }

        TBucket& Bucket(ui32 index) noexcept {
            Y_DEBUG_ABORT_UNLESS(index < BucketsCount_);
            return *(reinterpret_cast<TBucket*>(this + 1) + index);
        }

        const TBucket& Bucket(ui32 index) const noexcept {
            Y_DEBUG_ABORT_UNLESS(index < BucketsCount_);
            return *(reinterpret_cast<const TBucket*>(this + 1) + index);
        }

    private:
        ui32 BucketsCount_;
    };

    static_assert(alignof(TExplicitHistogramSnapshot) == alignof(TBucket),
                  "mismatched alingments of THistogramSnapshot and TBucket");

    IHistogramSnapshotPtr ExplicitHistogramSnapshot(TConstArrayRef<TBucketBound> bounds, TConstArrayRef<TBucketValue> values, bool shrinkBuckets = false);

} // namespace NMonitoring

std::ostream& operator<<(std::ostream& os, const NMonitoring::IHistogramSnapshot& hist);
