#pragma once

#include <algorithm>
#include <cassert>
#include <chrono>
#include <vector>

#include <iostream>

namespace NYdb::NTPCC {

// Timer wheel queue with sorted "current" bin
// and unsorted future bins

template <typename T>
class TBinnedTimerQueue {
public:
    using TTimePoint = std::chrono::steady_clock::time_point;

    struct TItem {
        bool operator<(const TItem& other) const {
            return Deadline < other.Deadline;
        }

        bool operator>(const TItem& other) const {
            return Deadline > other.Deadline;
        }

        TTimePoint Deadline;
        T Value;
    };

    struct TBucket {
        template <typename U>
        void AddBack(U&& item) {
            if (!Items.empty()) {
                if (item.Deadline < Items.back().Deadline) {
                    IsSorted = false;
                }
            }
            MinDeadline = std::min(MinDeadline, item.Deadline);
            MaxDeadline = std::max(MaxDeadline, item.Deadline);
            Items.emplace_back(std::forward<U>(item));
        }

        template <typename U>
        void AddSorted(U&& item, size_t validFromPos) {
            if (item.Deadline >= MaxDeadline) {
                AddBack(std::forward<U>(item));
                return;
            }

            MinDeadline = std::min(MinDeadline, item.Deadline);
            MaxDeadline = std::max(MaxDeadline, item.Deadline);

            auto it = std::upper_bound(Items.begin() + validFromPos, Items.end(), item);
            Items.insert(it, std::forward<U>(item));
        }

        void Clear() {
            Items.clear();
            MinDeadline = TTimePoint::max();
            MaxDeadline = TTimePoint::min();
            IsSorted = true;
        }

        size_t Size() const {
            return Items.size();
        }

        void Sort() {
            if (!IsSorted) {
                std::sort(Items.begin(), Items.end());
                IsSorted = true;
            }
        }

        std::vector<TItem> Items;
        TTimePoint MinDeadline = TTimePoint::max();
        TTimePoint MaxDeadline = TTimePoint::min();
        bool IsSorted = true;
    };

    TBinnedTimerQueue(size_t bucketCount = 2, size_t bucketSoftLimit = 100)
        : CurrentBucket(0), CurrentCursor(0)
    {
        Resize(bucketCount, bucketSoftLimit);
    }

    void Resize(size_t bucketCount, size_t bucketSoftLimit) {
        if (Size() != 0) {
            // just sanity check, shouldn't happen
            throw std::runtime_error("Resizing non-empty timer queue");
        }

        Buckets.resize(std::max(size_t(2UL), bucketCount));
        BucketSoftLimit = std::max(size_t(1UL), bucketSoftLimit);
        for (auto& bucket: Buckets) {
            bucket.Items.reserve(BucketSoftLimit * 2);
        }
    }

    template <typename U>
    void Add(std::chrono::milliseconds delay, U&& value, TTimePoint now = std::chrono::steady_clock::now()) {
        auto deadline = now + delay;
        size_t bucket = ComputeBucket(deadline);
        TItem item{deadline, std::forward<U>(value)};

        if (bucket == CurrentBucket) {
            auto& current = Buckets[CurrentBucket];
            current.AddSorted(std::move(item), CurrentCursor);
        } else {
            Buckets[bucket].AddBack(std::move(item));
        }
        ++Size_;
    }

    TTimePoint GetNextDeadline() const {
        if (Size_ == 0) {
            return TTimePoint::max();
        }

        if (auto& current = Buckets[CurrentBucket]; CurrentCursor < current.Items.size()) {
            return current.Items[CurrentCursor].Deadline;
        }
        auto nextBucketIndex = (CurrentBucket + 1) % Buckets.size();
        return Buckets[nextBucketIndex].Items[0].Deadline;
    }

    TItem PopFront() {
        if (Size_ == 0) {
            throw std::runtime_error("Cannot pop from empty timer queue");
        }

        --Size_;
        if (auto& current = Buckets[CurrentBucket]; CurrentCursor < current.Items.size()) {
            return current.Items[CurrentCursor++];
        }

        Advance();
        auto& current = Buckets[CurrentBucket];
        if (current.Items.empty()) {
            throw std::runtime_error("Internal error: Empty bucket after advance");
        }
        return current.Items[CurrentCursor++];
    }

    size_t Size() const {
        return Size_;
    }

    bool Empty() const {
        return Size_ == 0;
    }

    // just for tests
    bool Validate() const {
        // Check current bucket is sorted from CurrentCursor forward
        const auto& current = Buckets[CurrentBucket];
        for (size_t i = CurrentCursor + 1; i < current.Items.size(); ++i) {
            if (current.Items[i - 1] > current.Items[i]) {
                return false;
            }
        }

        // Check each bucket's borders match items inside
        for (size_t i = 0; i < Buckets.size(); ++i) {
            const auto& bucket = Buckets[i];
            if (!bucket.Items.empty()) {
                auto minIt = std::min_element(bucket.Items.begin(), bucket.Items.end());
                auto maxIt = std::max_element(bucket.Items.begin(), bucket.Items.end());
                if (bucket.MinDeadline != minIt->Deadline || bucket.MaxDeadline != maxIt->Deadline) {
                    return false;
                }
            } else {
                if (bucket.MinDeadline != TTimePoint::max() || bucket.MaxDeadline != TTimePoint::min()) {
                    return false;
                }
            }
        }

        // Check bucket border monotonicity
        for (size_t offset = 0; offset < Buckets.size() - 1; ++offset) {
            size_t i = (CurrentBucket + offset) % Buckets.size();
            size_t j = (i + 1) % Buckets.size();
            const auto& a = Buckets[i];
            const auto& b = Buckets[j];
            if (!a.Items.empty() && !b.Items.empty()) {
                if (!(a.MaxDeadline <= b.MinDeadline)) {
                    return false;
                }
            }
        }

        // Check Size_ equals sum of all bucket sizes minus consumed
        size_t computedSize = 0;
        for (size_t i = 0; i < Buckets.size(); ++i) {
            if (i == CurrentBucket) {
                computedSize += Buckets[i].Size() - CurrentCursor;
            } else {
                computedSize += Buckets[i].Size();
            }
        }
        return computedSize == Size_;
    }

    void Dump() const {
        std::cerr << "TimerQueue Dump (CurrentBucket=" << CurrentBucket << "):" << std::endl;
        for (size_t i = 0; i < Buckets.size(); ++i) {
            size_t index = (CurrentBucket + i) % Buckets.size();
            const auto& b = Buckets[index];
            std::cerr << "  [" << index << "] size=" << b.Items.size()
                      << " range=[" << b.MinDeadline.time_since_epoch().count()
                      << ", " << b.MaxDeadline.time_since_epoch().count() << "]";
            size_t limit = std::min<size_t>(5, b.Items.size());
            for (size_t j = 0; j < limit; ++j) {
                std::cerr << " " << b.Items[j].Deadline.time_since_epoch().count() << "," << b.Items[j].Value;
            }
            std::cerr << std::endl;
        }
    }

private:
    void Advance() {
        Buckets[CurrentBucket].Clear();
        CurrentCursor = 0;
        CurrentBucket = (CurrentBucket + 1) % Buckets.size();
        Buckets[CurrentBucket].Sort();
    }

    size_t ComputeBucket(const TTimePoint& deadline) const {
        for (size_t i = 0; i < Buckets.size(); ++i) {
            size_t bucketIndex = (i + CurrentBucket) % Buckets.size();
            const auto& bucket = Buckets[bucketIndex];
            if (deadline <= bucket.MaxDeadline) {
                return bucketIndex;
            }

            size_t nextBucketIndex = (bucketIndex + 1) % Buckets.size();
            auto& nextBucket = Buckets[nextBucketIndex];
            if (deadline <= nextBucket.MinDeadline) {
                // we can insert either to bucket or to nextBucket
                if (bucket.Size() >= BucketSoftLimit) {
                    return nextBucketIndex;
                }
                return bucketIndex;
            }
        }

        // insert to the end
        size_t lastBucketIndex = (CurrentBucket + Buckets.size() - 1) % Buckets.size();
        return lastBucketIndex;
    }

private:
    std::vector<TBucket> Buckets;
    size_t BucketSoftLimit;
    size_t Size_ = 0;
    size_t CurrentBucket = 0;
    size_t CurrentCursor = 0;
};

} // namespace NYdb::NTPCC
