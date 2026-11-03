#pragma once
#include "common.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/formats/arrow/reader/stats/stats.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/read.h>

#include <ydb/library/conclusion/status.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {

struct TReadStatsContext {
    using EDataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::EDataKind;
    std::shared_ptr<NScanStats::TOperation> Parent;
    EDataKind DataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::unknown;
    std::shared_ptr<NScanStats::TCollector> Collector;

    void Merge(const TReadStatsContext& other) {
        if (!Parent) {
            Parent = other.Parent;
        }
        if (!Collector) {
            Collector = other.Collector;
        }
        if (DataKind == NKqpProto::TColumnShardStepDetails::TStorageRead::unknown ||
            other.DataKind == NKqpProto::TColumnShardStepDetails::TStorageRead::unknown) {
            DataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::unknown;
        } else if (DataKind != other.DataKind) {
            DataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::mixed;
        }
    }
};

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

    std::optional<TString> ExtractOptional(const TBlobRange& bRange) {
        auto it = Blobs.find(bRange);
        if (it == Blobs.end()) {
            return std::nullopt;
        }
        TString result = it->second;
        Blobs.erase(it);
        return result;
    }

    TString ExtractVerified(const TBlobRange& bRange) {
        auto result = ExtractOptional(bRange);
        AFL_VERIFY(result)("range", bRange.ToString());
        return std::move(*result);
    }

    const TString& GetBlobRangeVerified(const TBlobRange& bRange) const {
        auto it = Blobs.find(bRange);
        AFL_VERIFY(it != Blobs.end());
        return it->second;
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
        auto flushCurrent = [&]() {
            AFL_VERIFY(currentRange);
            AFL_VERIFY(currentList.size());
            for (const auto& sub : currentList) {
                AFL_VERIFY(currentRange->GetBlobId() == sub.GetBlobId())("group", currentRange->ToString())("sub", sub.ToString());
                AFL_VERIFY(currentRange->Offset <= sub.Offset)("group", currentRange->ToString())("sub", sub.ToString());
                AFL_VERIFY(static_cast<ui64>(sub.Offset) + sub.GetBlobSize() <= static_cast<ui64>(currentRange->Offset) + currentRange->GetBlobSize())
                ("group", currentRange->ToString())("sub", sub.ToString());
            }
            AFL_VERIFY(result.emplace(*currentRange, std::move(currentList)).second)("group", currentRange->ToString());
            currentList.clear();
        };
        for (auto&& br : ranges) {
            if (!currentRange) {
                currentRange = br;
            } else if (!policy.Glue(*currentRange, br)) {
                flushCurrent();
                currentRange = br;
            }
            currentList.emplace_back(br);
        }
        if (currentRange) {
            flushCurrent();
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
    THashMap<TBlobRange, TReadStatsContext> RangeStats;
    std::shared_ptr<NBlobOperations::TReadCounters> Counters;
    bool Started = false;
    bool DataExtracted = false;
    YDB_ACCESSOR(bool, IsBackgroundProcess, true);
    YDB_ACCESSOR(bool, CacheAfterRead, true);

protected:
    virtual void DoStartReading(THashSet<TBlobRange>&& range) = 0;

    virtual void DoRetryRead(const TBlobRange& range) {
        Y_UNUSED(range);
        AFL_VERIFY(false)("error", "DoRetryRead not implemented for this storage");
    }

    void StartReading(std::vector<TBlobRange>&& ranges);
    virtual THashMap<TBlobRange, std::vector<TBlobRange>> GroupBlobsForOptimization(std::vector<TBlobRange>&& ranges) const = 0;

public:
    void RetryRead(const TBlobRange& range) {
        DoRetryRead(range);
    }

    void OnRetryEnqueue(const TBlobRange& range) const {
        if (Counters) {
            Counters->OnRetryEnqueue(range.Size);
        }
    }

    void OnRetryExecute() const {
        if (Counters) {
            Counters->OnRetryExecute();
        }
    }

    void OnRetryExhausted() const {
        if (Counters) {
            Counters->OnRetryExhausted();
        }
    }

    const THashMap<TBlobRange, std::vector<TBlobRange>>& GetGroups() const {
        return Groups;
    }

    void Merge(const std::shared_ptr<IBlobsReadingAction>& action) {
        AFL_VERIFY(action);
        AFL_VERIFY(!Started);
        if (!RangeStats.empty() || !action->RangeStats.empty()) {
            auto mergeContext = [&](const TBlobRange& range) {
                const auto incoming = action->RangeStats.find(range);
                const auto existing = RangeStats.find(range);
                if (existing != RangeStats.end()) {
                    existing->second.Merge(incoming == action->RangeStats.end() ? TReadStatsContext{} : incoming->second);
                } else if (incoming != action->RangeStats.end()) {
                    auto context = incoming->second;
                    if (RangesForRead.contains(range) || RangesForResult.contains(range)) {
                        context.Merge({});
                    }
                    RangeStats.emplace(range, std::move(context));
                }
            };
            for (const auto& range : action->RangesForRead) {
                mergeContext(range);
            }
            for (const auto& [range, _] : action->RangesForResult) {
                mergeContext(range);
            }
        }
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

    void AddRange(const TBlobRange& range, const std::optional<TString>& result = {},
        TReadStatsContext::EDataKind dataKind = NKqpProto::TColumnShardStepDetails::TStorageRead::unknown);

    TReadStatsContext GetReadStatsContext(const TBlobRange& groupedRange) const;

    virtual void OnReadAttemptResult(const TBlobRange&, std::optional<ui64>, bool) {}

    void Start(const THashSet<TBlobRange>& rangesInProgress);
    void OnReadResult(const TBlobRange& range, const TString& data, const bool fromCache);
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

    std::shared_ptr<IBlobsReadingAction> FindByStorageId(const TString& storageId) const {
        auto it = Actions.find(storageId);
        if (it == Actions.end()) {
            return nullptr;
        }
        return it->second;
    }

    void Add(const std::shared_ptr<IBlobsReadingAction>& action) {
        if (!action->GetExpectedBlobsCount()) {
            return;
        }
        auto it = Actions.find(action->GetStorageId());
        if (it == Actions.end()) {
            Actions.emplace(action->GetStorageId(), action);
        } else {
            it->second->Merge(action);
        }
    }

    TReadActionsCollection() = default;

    TReadActionsCollection(const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions) {
        for (auto&& a : actions) {
            Add(a);
        }
    }
};

}   // namespace NKikimr::NOlap
