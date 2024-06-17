#pragma once
#include "common.h"
#include <ydb/core/tx/columnshard/blobs_action/counters/read.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/library/conclusion/status.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {

class TActionReadBlobs {
private:
    THashMap<TBlobRange, TString> Blobs;
public:
    TString DebugString() const;

    TActionReadBlobs() = default;

    TActionReadBlobs(THashMap<TBlobRange, TString>&& blobs)
        : Blobs(std::move(blobs))
    {
        for (auto&& i : Blobs) {
            AFL_VERIFY(i.second.size());
        }
    }

    void Merge(TActionReadBlobs&& item) {
        for (auto&& i : item.Blobs) {
            Add(i.first, std::move(i.second));
        }
    }

    THashMap<TBlobRange, TString>::iterator begin() {
        return Blobs.begin();
    }

    THashMap<TBlobRange, TString>::iterator end() {
        return Blobs.end();
    }

    ui64 GetTotalBlobsSize() const {
        ui64 result = 0;
        for (auto&& i : Blobs) {
            result += i.second.size();
        }
        return result;
    }

    void Add(THashMap<TBlobRange, TString>&& blobs) {
        for (auto&& i : blobs) {
            AFL_VERIFY(i.second.size());
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }

    void Add(const TBlobRange& range, TString&& data) {
        AFL_VERIFY(data.size());
        AFL_VERIFY(Blobs.emplace(range, std::move(data)).second);
    }

    bool Contains(const TBlobRange& bRange) const {
        return Blobs.contains(bRange);
    }

    std::optional<TString> GetBlobRangeOptional(const TBlobRange& bRange) const {
        auto it = Blobs.find(bRange);
        if (it == Blobs.end()) {
            return {};
        }
        return it->second;
    }

    TString Extract(const TBlobRange& bRange) {
        auto it = Blobs.find(bRange);
        AFL_VERIFY(it != Blobs.end());
        TString result = it->second;
        Blobs.erase(it);
        return result;
    }

    bool IsEmpty() const {
        return Blobs.empty();
    }
};

class TBlobsGlueing {
public:
    class TSequentialGluePolicy {
    public:
        bool Glue(TBlobRange& currentRange, const TBlobRange& addRange) const {
            return currentRange.TryGlueWithNext(addRange);
        }
    };

    class TBlobGluePolicy {
    private:
        const ui64 BlobLimitSize = 8LLU << 20;
    public:
        TBlobGluePolicy(const ui64 blobLimitSize)
            : BlobLimitSize(blobLimitSize)
        {
        }

        bool Glue(TBlobRange& currentRange, const TBlobRange& addRange) const {
            return currentRange.TryGlueSameBlob(addRange, BlobLimitSize);
        }
    };

    template <class TGluePolicy>
    static THashMap<TBlobRange, std::vector<TBlobRange>> GroupRanges(std::vector<TBlobRange>&& ranges, const TGluePolicy& policy) {
        std::sort(ranges.begin(), ranges.end());
        THashMap<TBlobRange, std::vector<TBlobRange>> result;
        std::optional<TBlobRange> currentRange;
        std::vector<TBlobRange> currentList;
        for (auto&& br : ranges) {
            if (!currentRange) {
                currentRange = br;
            }
            else if (!policy.Glue(*currentRange, br)) {
                result.emplace(*currentRange, std::move(currentList));
                currentRange = br;
                currentList.clear();
            }
            currentList.emplace_back(br);
        }
        if (currentRange) {
            result.emplace(*currentRange, std::move(currentList));
        }
        return result;
    }

};

class IBlobsReadingAction: public ICommonBlobsAction {
public:
    using TErrorStatus = TConclusionSpecialStatus<NKikimrProto::EReplyStatus, NKikimrProto::EReplyStatus::OK, NKikimrProto::EReplyStatus::ERROR>;
private:
    using TBase = ICommonBlobsAction;

    THashSet<TBlobRange> RangesForRead;
    THashMap<TBlobRange, TString> RangesForResult;
    TMonotonic StartWaitingRanges;
    i32 WaitingRangesCount = 0;
    THashMap<TBlobRange, TString> Replies;
    THashMap<TBlobRange, TErrorStatus> Fails;
    THashMap<TBlobRange, std::vector<TBlobRange>> Groups;
    std::shared_ptr<NBlobOperations::TReadCounters> Counters;
    bool Started = false;
    bool DataExtracted = false;
    YDB_ACCESSOR(bool, IsBackgroundProcess, true);
protected:
    virtual void DoStartReading(THashSet<TBlobRange>&& range) = 0;
    void StartReading(std::vector<TBlobRange>&& ranges);
    virtual THashMap<TBlobRange, std::vector<TBlobRange>> GroupBlobsForOptimization(std::vector<TBlobRange>&& ranges) const = 0;
public:

    const THashMap<TBlobRange, std::vector<TBlobRange>>& GetGroups() const {
        return Groups;
    }

    void Merge(const std::shared_ptr<IBlobsReadingAction>& action) {
        AFL_VERIFY(action);
        AFL_VERIFY(!Started);
        for (auto&& i : action->RangesForResult) {
            RangesForResult.emplace(i.first, i.second);
            auto it = RangesForRead.find(i.first);
            if (it != RangesForRead.end()) {
                RangesForRead.erase(it);
            }
        }
        for (auto&& i : action->RangesForResult) {
            RangesForResult.emplace(i.first, i.second);
        }
        for (auto&& i : action->RangesForRead) {
            if (!RangesForResult.contains(i)) {
                RangesForRead.emplace(i);
            }
        }
    }

    TActionReadBlobs ExtractBlobsData() {
        AFL_VERIFY(Started);
        AFL_VERIFY(IsFinished());
        AFL_VERIFY(!DataExtracted);
        DataExtracted = true;
        auto result = TActionReadBlobs(std::move(Replies));
        RangesForResult.clear();
        Replies.clear();
        return result;
    }

    void SetCounters(std::shared_ptr<NBlobOperations::TReadCounters> counters) {
        Counters = counters;
    }

    IBlobsReadingAction(const TString& storageId)
        : TBase(storageId)
    {

    }

    ui64 GetExpectedBlobsSize() const {
        ui64 result = 0;
        for (auto&& i : RangesForRead) {
            result += i.Size;
        }
        for (auto&& i : RangesForResult) {
            result += i.first.Size;
        }
        return result;
    }

    ui64 GetExpectedBlobsCount() const {
        return RangesForRead.size() + RangesForResult.size();
    }

    void AddRange(const TBlobRange& range, const std::optional<TString>& result = {});

    void Start(const THashSet<TBlobRange>& rangesInProgress);
    void OnReadResult(const TBlobRange& range, const TString& data);
    void OnReadError(const TBlobRange& range, const TErrorStatus& replyStatus);

    bool HasFails() const {
        return Fails.size();
    }

    bool IsFinished() const {
        return WaitingRangesCount == 0;
    }
};

class TReadActionsCollection {
private:
    THashMap<TString, std::shared_ptr<IBlobsReadingAction>> Actions;
public:
    THashMap<TString, std::shared_ptr<IBlobsReadingAction>>::const_iterator begin() const {
        return Actions.begin();
    }

    THashMap<TString, std::shared_ptr<IBlobsReadingAction>>::const_iterator end() const {
        return Actions.end();
    }

    THashMap<TString, std::shared_ptr<IBlobsReadingAction>>::iterator begin() {
        return Actions.begin();
    }

    THashMap<TString, std::shared_ptr<IBlobsReadingAction>>::iterator end() {
        return Actions.end();
    }

    ui32 IsEmpty() const {
        return Actions.empty();
    }

    void Add(const std::shared_ptr<IBlobsReadingAction>& action) {
        auto it = Actions.find(action->GetStorageId());
        if (it == Actions.end()) {
            Actions.emplace(action->GetStorageId(), action);
        } else {
            it->second->Merge(action);
        }
    }

    TReadActionsCollection() = default;

    TReadActionsCollection(const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions) {
        for (auto&& a: actions) {
            Add(a);
        }
    }
};

}
