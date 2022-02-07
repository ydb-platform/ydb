#pragma once

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <cmath>

namespace NMonitoring {

    constexpr ui32 LOG_HIST_MAX_BUCKETS = 100;

    class TLogHistogramSnapshot: public TAtomicRefCount<TLogHistogramSnapshot> {
    public:
        TLogHistogramSnapshot(double base, ui64 zerosCount, int startPower, TVector<double> buckets)
            : Base_(base)
            , ZerosCount_(zerosCount)
            , StartPower_(startPower)
            , Buckets_(std::move(buckets)) {
        }

        /**
         * @return buckets count.
         */
        ui32 Count() const noexcept {
            return Buckets_.size();
        }

        /**
         * @return upper bound for the bucket with particular index.
         */
        double UpperBound(int index) const noexcept {
            return std::pow(Base_, StartPower_ + index);
        }

        /**
         * @return value stored in the bucket with particular index.
         */
        double Bucket(ui32 index) const noexcept {
            return Buckets_[index];
        }

        /**
         * @return nonpositive values count
         */
        ui64 ZerosCount() const noexcept {
            return ZerosCount_;
        }

        double Base() const noexcept {
            return Base_;
        }

        int StartPower() const noexcept {
            return StartPower_;
        }

        ui64 MemorySizeBytes() const noexcept {
            return sizeof(*this) + Buckets_.capacity() * sizeof(double);
        }

    private:
        double Base_;
        ui64 ZerosCount_;
        int StartPower_;
        TVector<double> Buckets_;
    };

    using TLogHistogramSnapshotPtr = TIntrusivePtr<TLogHistogramSnapshot>;
}

std::ostream& operator<<(std::ostream& os, const NMonitoring::TLogHistogramSnapshot& hist);
