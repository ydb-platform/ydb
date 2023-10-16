#pragma once

#include "log_histogram_snapshot.h"

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/yexception.h>

#include <mutex>
#include <cmath>

namespace NMonitoring {

    class TLogHistogramCollector {
    public:
        static constexpr int DEFAULT_START_POWER = -1;

        explicit TLogHistogramCollector(int startPower = DEFAULT_START_POWER)
                : StartPower_(startPower)
                , CountZero_(0u)
        {}

        void Collect(TLogHistogramSnapshot* logHist) {
            std::lock_guard guard(Mutex_);
            Merge(logHist);
        }

        bool Collect(double value) {
            std::lock_guard guard(Mutex_);
            return CollectDouble(value);
        }

        TLogHistogramSnapshotPtr Snapshot() const {
            std::lock_guard guard(Mutex_);
            return MakeIntrusive<TLogHistogramSnapshot>(BASE, CountZero_, StartPower_, Buckets_);
        }

        void AddZeros(ui64 zerosCount) noexcept {
            std::lock_guard guard(Mutex_);
            CountZero_ += zerosCount;
        }

    private:
        int StartPower_;
        ui64 CountZero_;
        TVector<double> Buckets_;
        mutable std::mutex Mutex_;

        static constexpr size_t MAX_BUCKETS = LOG_HIST_MAX_BUCKETS;
        static constexpr double BASE = 1.5;

    private:
        int EstimateBucketIndex(double value) const {
            return (int) (std::floor(std::log(value) / std::log(BASE)) - StartPower_);
        }

        void CollectPositiveDouble(double value) {
            ssize_t idx = std::floor(std::log(value) / std::log(BASE)) - StartPower_;
            if (idx >= Buckets_.ysize()) {
                idx = ExtendUp(idx);
            } else if (idx <= 0) {
                idx = Max<ssize_t>(0, ExtendDown(idx, 1));
            }
            ++Buckets_[idx];
        }

        bool CollectDouble(double value) {
            if (Y_UNLIKELY(std::isnan(value) || std::isinf(value))) {
                return false;
            }
            if (value <= 0.0) {
                ++CountZero_;
            } else {
                CollectPositiveDouble(value);
            }
            return true;
        }

        void Merge(TLogHistogramSnapshot* logHist) {
            CountZero_ += logHist->ZerosCount();
            const i32 firstIdxBeforeExtend = logHist->StartPower() - StartPower_;
            const i32 lastIdxBeforeExtend = firstIdxBeforeExtend + logHist->Count() - 1;
            if (firstIdxBeforeExtend > Max<i16>() || firstIdxBeforeExtend < Min<i16>()) {
                ythrow yexception() << "i16 overflow on first index";
            }
            if (lastIdxBeforeExtend > Max<i16>() || lastIdxBeforeExtend < Min<i16>()) {
                ythrow yexception() << "i16 overflow on last index";
            }
            i64 firstIdx = ExtendBounds(firstIdxBeforeExtend, lastIdxBeforeExtend, 0).first;
            size_t toMerge = std::min<ui32>(std::max<i64>(-firstIdx, (i64) 0), logHist->Count());
            if (toMerge) {
                for (size_t i = 0; i < toMerge; ++i) {
                    Buckets_[0] += logHist->Bucket(i);
                }
                firstIdx = 0;
            }
            for (size_t i = toMerge; i != logHist->Count(); ++i) {
                Buckets_[firstIdx] += logHist->Bucket(i);
                ++firstIdx;
            }
        }

        int ExtendUp(int expectedIndex) {
            Y_DEBUG_ABORT_UNLESS(expectedIndex >= (int) Buckets_.size());
            const size_t toAdd = expectedIndex - Buckets_.size() + 1;
            const size_t newSize = Buckets_.size() + toAdd;
            if (newSize <= MAX_BUCKETS) {
                Buckets_.resize(newSize, 0.0);
                return expectedIndex;
            }

            const size_t toRemove = newSize - MAX_BUCKETS;
            const size_t actualToRemove = std::min<size_t>(toRemove, Buckets_.size());
            if (actualToRemove > 0) {
                const double firstWeight = std::accumulate(Buckets_.cbegin(), Buckets_.cbegin() + actualToRemove, 0.0);
                Buckets_.erase(Buckets_.cbegin(), Buckets_.cbegin() + actualToRemove);
                if (Buckets_.empty()) {
                    Buckets_.push_back(firstWeight);
                } else {
                    Buckets_[0] = firstWeight;
                }
            }
            Buckets_.resize(MAX_BUCKETS, 0.0);
            StartPower_ += toRemove;
            return expectedIndex - toRemove;
        }

        int ExtendDown(int expectedIndex, int margin) {
            Y_DEBUG_ABORT_UNLESS(expectedIndex <= 0);
            int toAdd = std::min<int>(MAX_BUCKETS - Buckets_.size(), margin - expectedIndex);
            if (toAdd > 0) {
                Buckets_.insert(Buckets_.begin(), toAdd, 0.0);
                StartPower_ -= toAdd;
            }
            return expectedIndex + toAdd;
        }

        std::pair<ssize_t, ssize_t> ExtendBounds(ssize_t startIdx, ssize_t endIdx, ui8 margin) {
            ssize_t realEndIdx;
            ssize_t realStartIdx;
            if (endIdx >= Buckets_.ysize()) {
                Buckets_.reserve(std::max<size_t>(std::min<ui32>(endIdx - startIdx + 1ul, MAX_BUCKETS), 0ul));
                realEndIdx = ExtendUp(endIdx);
                startIdx += realEndIdx - endIdx;
            } else {
                realEndIdx = endIdx;
            }
            if (startIdx < 1) {
                realStartIdx = ExtendDown(startIdx, margin);
                realEndIdx += realStartIdx - startIdx;
            } else {
                realStartIdx = startIdx;
            }
            return std::make_pair(realStartIdx, realEndIdx);
        }
    };

} // namespace NMonitoring
