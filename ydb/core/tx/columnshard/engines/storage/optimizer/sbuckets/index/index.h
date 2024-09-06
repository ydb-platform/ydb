#pragma once
#include "bucket.h"

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

class TWeightedPortionsBucket {
private:
    std::weak_ptr<TPortionsBucket> Bucket;
    const ui64 Hash;
public:
    operator size_t() const {
        return Hash;
    }

    bool operator==(const TWeightedPortionsBucket& item) const {
        return Bucket.lock().get() == item.Bucket.lock().get();
    }

    TWeightedPortionsBucket(const std::shared_ptr<TPortionsBucket>& bucket)
        : Bucket(bucket)
        , Hash((ui64)bucket.get())
    {
        AFL_VERIFY(!Bucket.expired());
    }

    std::shared_ptr<TPortionsBucket> GetBucketVerified() const {
        AFL_VERIFY(!Bucket.expired());
        return Bucket.lock();
    }
};

class TPortionBuckets {
private:
    const std::shared_ptr<arrow::Schema> PrimaryKeysSchema;
    const std::shared_ptr<IStoragesManager> StoragesManager;
    std::map<TLeftBucketBorder, std::shared_ptr<TPortionsBucket>> Buckets;

    std::map<i64, THashSet<TWeightedPortionsBucket>> BucketsByWeight;
    THashSet<TWeightedPortionsBucket> RatedBuckets;
    TInstant CurrentWeightInstant = TInstant::Now();

    std::shared_ptr<TCounters> Counters;
    std::shared_ptr<IOptimizationLogic> Logic;

    void AddBucketToRating(const std::shared_ptr<TPortionsBucket>& bucket) {
        AFL_VERIFY(RatedBuckets.emplace(bucket).second);
        AFL_VERIFY(BucketsByWeight[bucket->ResetWeight(CurrentWeightInstant)].emplace(bucket).second);

    }

    void RemoveBucketFromRating(const std::shared_ptr<TPortionsBucket>& bucket) {
        AFL_VERIFY(RatedBuckets.erase(bucket));
        auto it = BucketsByWeight.find(bucket->GetWeight());
        AFL_VERIFY(it != BucketsByWeight.end());
        AFL_VERIFY(it->second.erase(bucket));
        if (it->second.empty()) {
            BucketsByWeight.erase(it);
        }
    }

    std::shared_ptr<TPortionsBucket> GetPreviousBucket(const TPortionsBucket& bucket) const {
        auto it = Buckets.find(bucket.GetStart());
        AFL_VERIFY(it != Buckets.end());
        if (it != Buckets.begin()) {
            --it;
            return it->second;
        } else {
            AFL_VERIFY(!bucket.GetStart().HasValue());
            return nullptr;
        }
    }

    std::shared_ptr<TPortionsBucket> GetNextBucket(const TPortionsBucket& bucket) const {
        auto it = Buckets.find(bucket.GetStart());
        AFL_VERIFY(it != Buckets.end());
        ++it;
        if (it != Buckets.end()) {
            return it->second;
        } else {
            AFL_VERIFY(bucket.GetFinish().IsOpen());
            return nullptr;
        }
    }

    std::vector<std::shared_ptr<TPortionsBucket>> GetAffectedBuckets(const TLeftBucketBorder& fromInclude, const TLeftBucketBorder& toInclude) {
        std::vector<std::shared_ptr<TPortionsBucket>> result;
        auto itFrom = Buckets.upper_bound(fromInclude);
        auto itTo = Buckets.upper_bound(toInclude);
        AFL_VERIFY(itFrom != Buckets.begin());
        --itFrom;
        for (auto it = itFrom; it != itTo; ++it) {
            result.emplace_back(it->second);
        }
        return result;
    }

public:
    TPortionBuckets(const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<TCounters>& counters, const std::shared_ptr<IOptimizationLogic>& logic)
        : PrimaryKeysSchema(primaryKeysSchema)
        , StoragesManager(storagesManager)
        , Counters(counters)
        , Logic(logic)
    {
        Buckets.emplace(TLeftBucketBorder(), std::make_shared<TPortionsBucket>(Counters, Logic, TLeftBucketBorder(), TRightBucketBorder()));
        AddBucketToRating(Buckets.begin()->second);
    }

    void ResetLogic(const std::shared_ptr<IOptimizationLogic>& logic) {
        Logic = logic;
        for (auto&& i : Buckets) {
            i.second->ResetLogic(logic);
        }
    }

    std::vector<TTaskDescription> GetTasksDescription() const {
        std::vector<TTaskDescription> result;
        for (auto&& i : Buckets) {
            result.emplace_back(i.second->GetTaskDescription());
        }
        return result;
    }

    bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
        AFL_VERIFY(BucketsByWeight.size());
        AFL_VERIFY(BucketsByWeight.rbegin()->second.size());
        const auto bucket = BucketsByWeight.rbegin()->second.begin()->GetBucketVerified();
        return bucket->IsLocked(dataLocksManager);
    }

    bool IsEmpty() const {
        return Buckets.empty();
    }
    TString DebugString() const {
        return "";
    }
    NJson::TJsonValue SerializeToJson() const {
        return NJson::JSON_NULL;
    }

    void Actualize(const TInstant currentInstant) {
        CurrentWeightInstant = currentInstant;
        AFL_VERIFY(Buckets.size());
        for (auto&& i : Buckets) {
            if (!i.second->NeedActualization(currentInstant)) {
                continue;
            }
            RemoveBucketFromRating(i.second);
            i.second->Actualize(currentInstant);
            AddBucketToRating(i.second);
        }
        AFL_VERIFY(RatedBuckets.size() == Buckets.size());
    }

    i64 GetWeight() const {
        AFL_VERIFY(BucketsByWeight.size());
        return BucketsByWeight.rbegin()->first;
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
        auto buckets = GetAffectedBuckets(portion->IndexKeyStart(), portion->IndexKeyEnd());
        AFL_VERIFY(buckets.size() == 1);
        auto front = buckets.front();
        RemoveBucketFromRating(front);
        front->RemovePortion(portion);
        if (front->IsEmpty()) {
            auto bucket = GetPreviousBucket(*front);
            if (bucket) {
                AFL_VERIFY(Buckets.erase(front->GetStart()));
                RemoveBucketFromRating(bucket);
                bucket->MergeFrom(std::move(*front));
                AddBucketToRating(bucket);
            } else {
                bucket = GetNextBucket(*front);
                if (bucket) {
                    RemoveBucketFromRating(bucket);
                    AFL_VERIFY(Buckets.erase(bucket->GetStart()));
                    front->MergeFrom(std::move(*bucket));
                }
                AddBucketToRating(front);
            }
        } else {
            AddBucketToRating(front);
        }
        AFL_VERIFY(RatedBuckets.size() == Buckets.size());
    }
    void AddPortion(const std::shared_ptr<TPortionInfo>& portion) {
        auto buckets = GetAffectedBuckets(portion->IndexKeyStart(), portion->IndexKeyEnd());
        AFL_VERIFY(buckets.size());
        if (buckets.size() == 1) {
            RemoveBucketFromRating(buckets.front());
            buckets.front()->AddPortion(portion);
            AddBucketToRating(buckets.front());
        } else {
            for (auto&& i : buckets) {
                RemoveBucketFromRating(i);
                AFL_VERIFY(Buckets.erase(i->GetStart()));
            }
            auto front = buckets.front();
            for (ui32 i = 1; i < buckets.size(); ++i) {
                front->MergeFrom(std::move(*buckets[i]));
            }
            auto bucketsNew = front->Split();
            for (auto&& i : bucketsNew) {
                AFL_VERIFY(Buckets.emplace(i->GetStart(), i).second);
                AddBucketToRating(i);
            }
        }
        AFL_VERIFY(RatedBuckets.size() == Buckets.size());
    }

    std::shared_ptr<TColumnEngineChanges> BuildOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        AFL_VERIFY(BucketsByWeight.size());
        AFL_VERIFY(BucketsByWeight.rbegin()->first);
        AFL_VERIFY(BucketsByWeight.rbegin()->second.size());
        const std::shared_ptr<TPortionsBucket> bucketForOptimization = BucketsByWeight.rbegin()->second.begin()->GetBucketVerified();
        auto it = Buckets.find(bucketForOptimization->GetStart());
        AFL_VERIFY(it != Buckets.end());
        ++it;
        return bucketForOptimization->BuildOptimizationTask(granule, locksManager, PrimaryKeysSchema, StoragesManager);
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions() const {
        NArrow::NMerger::TIntervalPositions result;
        for (auto&& i : Buckets) {
            if (!i.first.HasValue()) {
                continue;
            }
            NArrow::NMerger::TSortableBatchPosition posStart(i.first.GetValueVerified().ToBatch(PrimaryKeysSchema), 0, PrimaryKeysSchema->field_names(), {}, false);
            result.AddPosition(std::move(posStart), false);
        }
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
