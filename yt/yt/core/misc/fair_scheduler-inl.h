#pragma once
#ifndef FAIR_SCHEDULER_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_scheduler.h"
// For the sake of sane code completion.
#include "fair_scheduler.h"
#endif

#include "heap.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TTask>
class TFairScheduler
    : public IFairScheduler<TTask>
{
public:
    void Enqueue(TTask task, const std::string& user) override
    {
        auto guard = Guard(Lock_);

        auto* bucket = GetOrCreateBucket(user);
        // Insert the bucket into the heap if this is its first task.
        if (!bucket->InHeap) {
            BucketHeap_.push_back(bucket);
            AdjustHeapBack(BucketHeap_.begin(), BucketHeap_.end(), TUserBucketComparer());
            bucket->InHeap = true;
        }
        bucket->Tasks.push(std::move(task));
    }

    std::optional<TTask> TryDequeue() override
    {
        auto guard = Guard(Lock_);

        while (true) {
            if (BucketHeap_.empty()) {
                return std::nullopt;
            }

            auto* bucket = BucketHeap_.front();
            YT_ASSERT(bucket->InHeap);

            auto actualExcessTime = std::max(bucket->ExcessTime, ExcessBaseline_);

            // Account for charged time possibly reordering the heap.
            if (bucket->HeapKey != actualExcessTime) {
                YT_ASSERT(bucket->HeapKey < actualExcessTime);
                bucket->HeapKey = actualExcessTime;
                AdjustHeapFront(BucketHeap_.begin(), BucketHeap_.end(), TUserBucketComparer());
                continue;
            }

            auto& tasks = bucket->Tasks;
            YT_VERIFY(!tasks.empty());

            // Extract the task.
            auto task = std::move(bucket->Tasks.front());
            bucket->Tasks.pop();

            // Remove the bucket from the heap if no tasks are pending.
            if (bucket->Tasks.empty()) {
                ExtractHeap(BucketHeap_.begin(), BucketHeap_.end(), TUserBucketComparer());
                BucketHeap_.pop_back();
                bucket->InHeap = false;
            }

            // Promote the baseline.
            ExcessBaseline_ = actualExcessTime;

            return task;
        }

        YT_ABORT();
    }

    void ChargeUser(const std::string& user, TDuration time) override
    {
        auto guard = Guard(Lock_);

        auto* bucket = GetOrCreateBucket(user);
        // Just charge the bucket, do not reorder it in the heap.
        auto actualExcessTime = std::max(bucket->ExcessTime, ExcessBaseline_);
        bucket->ExcessTime = actualExcessTime + time;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    struct TUserBucket
    {
        explicit TUserBucket(const std::string& userName)
            : UserName(userName)
        { }

        std::string UserName;
        TDuration ExcessTime;
        //! Typically equals ExcessTime; however when a user is charged we just update ExcessTime
        //! and leave HeapKey intact. Upon extracting heap's top we check if its ExcessTime matches its HeapKey
        //! and if not then readjust the heap.
        TDuration HeapKey;
        std::queue<TTask> Tasks;
        bool InHeap = false;
    };

    struct TUserBucketComparer
    {
        bool operator ()(TUserBucket* lhs, TUserBucket* rhs) const
        {
            return lhs->HeapKey < rhs->HeapKey;
        }
    };

    THashMap<std::string, TUserBucket> NameToUserBucket_;
    TDuration ExcessBaseline_;

    //! Min-heap ordered by TUserBucket::ExcessTime.
    //! A bucket is only present here iff it has at least one task.
    std::vector<TUserBucket*> BucketHeap_;

    TUserBucket* GetOrCreateBucket(const std::string& userName)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        auto [it, inserted] = NameToUserBucket_.emplace(userName, TUserBucket(userName));
        return &it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TTask>
IFairSchedulerPtr<TTask> CreateFairScheduler()
{
    return New<TFairScheduler<TTask>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
